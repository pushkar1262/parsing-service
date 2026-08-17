"""The document status machine, and the run records that sit beside it.

The spec asks for `pending → processing → ready → failed`, exposed. That is the
*document's* status and it is what consumers see. But a parse attempt has its own
lifecycle, and conflating the two breaks reprocessing: re-running a parser on a healthy
document would move it to `processing`, and a failed re-run would mark a document
`failed` that has perfectly good content from the previous run. Nothing was wrong with
that document — an optional improvement took it offline and then declared it broken.

So a document has a status and a `current_run_id`; a run has its own status. The
important consequence is a rule, not a state:

    **A reprocess never writes to `documents.status`.**

The document keeps saying `ready` and keeps pointing at the last good artifact until a
new run actually succeeds, at which point one atomic update flips the pointer. A failed
reprocess is a no-op plus a row you can go and read.

That costs nothing extra, because `parse_runs` has to exist anyway: its unique constraint
on `(document_id, content_hash, parser_version, parse_options_hash)` is what turns Kafka's
at-least-once delivery into exactly-once effect.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class DocumentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"
    DELETED = "deleted"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


# Legal document transitions. `ready → processing` is present for a *first* parse that
# somehow re-runs, but a reprocess deliberately does not use it — see the module
# docstring. `failed → processing` is what makes a DLQ replay work after a fix.
_DOCUMENT_TRANSITIONS: dict[DocumentStatus, frozenset[DocumentStatus]] = {
    DocumentStatus.PENDING: frozenset(
        {DocumentStatus.PROCESSING, DocumentStatus.FAILED, DocumentStatus.DELETED}
    ),
    DocumentStatus.PROCESSING: frozenset(
        {DocumentStatus.READY, DocumentStatus.FAILED, DocumentStatus.DELETED}
    ),
    DocumentStatus.READY: frozenset(
        {DocumentStatus.PROCESSING, DocumentStatus.READY, DocumentStatus.DELETED}
    ),
    DocumentStatus.FAILED: frozenset(
        {DocumentStatus.PROCESSING, DocumentStatus.DELETED}
    ),
    # Terminal. A deleted document is never revived; a re-upload is a new document,
    # because reviving one would resurrect content a user asked to have removed.
    DocumentStatus.DELETED: frozenset(),
}

# Statuses a worker may claim from. `ready` is absent on purpose: a document that already
# has content is only re-parsed through the explicit reprocess path, which creates a run
# without touching the document's status.
CLAIMABLE = frozenset({DocumentStatus.PENDING, DocumentStatus.FAILED})


class IllegalTransition(Exception):
    """Refused rather than silently applied.

    A status machine that quietly accepts any assignment is a set of columns, not a
    machine — and the bug it hides is a document going `ready` without an artifact.
    """


def can_transition(current: DocumentStatus, target: DocumentStatus) -> bool:
    return target in _DOCUMENT_TRANSITIONS.get(current, frozenset())


def assert_transition(current: DocumentStatus, target: DocumentStatus) -> None:
    if not can_transition(current, target):
        raise IllegalTransition(f"cannot move a document from {current.value} to {target.value}")


@dataclass
class ParseRun:
    """One attempt to parse one document with one parser version."""

    id: str
    document_id: str
    content_hash: str
    parser_version: str
    parse_options_hash: str = ""
    status: RunStatus = RunStatus.PENDING
    artifact_key: str | None = None
    attempt: int = 1
    lease_expires_at: datetime | None = None
    trace_id: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failure_class: str | None = None
    failure_reason: str | None = None

    @property
    def idempotency_key(self) -> tuple[str, str, str, str]:
        """What makes a redelivery a no-op.

        Parsing is a pure function of these four things, so a run that already succeeded
        for this key has nothing left to do — which is why a replay can be answered from
        the database without touching blob storage or a parser.
        """
        return (
            self.document_id,
            self.content_hash,
            self.parser_version,
            self.parse_options_hash,
        )


@dataclass
class DocumentRecord:
    """The row consumers read, and the one the status API serves."""

    id: str
    # Tenancy is on the row, not derived from the key. Every read is scoped by it, and on
    # Postgres the row-level security policy matches against it — which is why it must be
    # populated at registration rather than backfilled.
    tenant_id: str | None = None
    project_id: str | None = None
    status: DocumentStatus = DocumentStatus.PENDING
    content_hash: str | None = None
    source_uri: str | None = None
    source_bucket: str | None = None
    source_key: str | None = None
    source_version_id: str | None = None
    media_type: str | None = None
    byte_size: int | None = None
    current_run_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    failure_class: str | None = None
    failure_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    deleted_at: datetime | None = None

    @property
    def is_readable(self) -> bool:
        """Whether `/content` can serve something.

        Deliberately *not* `status is READY`. During a reprocess the document is still
        ready and still serving the previous artifact, and the pointer is the honest test
        of whether there is anything to return.
        """
        return (
            self.deleted_at is None
            and self.current_run_id is not None
            and self.status is not DocumentStatus.DELETED
        )
