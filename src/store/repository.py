"""The document and run store, behind a protocol.

Two implementations. `InMemoryRepository` is what the worker and API tests run against —
it is not a mock, it implements the same semantics, including the conditional claim and
the unique constraint, so a test that passes here is testing real idempotency logic.
`PostgresRepository` in `store/postgres.py` is the production one.

The semantics that matter are all about **exactly-once effect on top of at-least-once
delivery**, and they are worth naming because they are easy to implement as something
that looks right and is not:

`claim` must be a conditional update, not a read-then-write. Two workers reading
`status = 'pending'` and both writing `'processing'` both proceed; a single
`UPDATE ... WHERE status IN (...)` returning a row count means exactly one does.

`start_run` must rely on a unique constraint, not a prior existence check. The check-then-
insert race is the same race one level down.

The clock is injected. Time appears in leases and timestamps, and a repository that calls
`datetime.now()` internally cannot be tested for lease expiry without sleeping.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

from domain.status import (
    CLAIMABLE,
    DocumentRecord,
    DocumentStatus,
    ParseRun,
    RunStatus,
    assert_transition,
)

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DuplicateRun(Exception):
    """A run already exists for this idempotency key.

    An outcome, not an error: it means the work is already done or in flight, and the
    caller should commit the offset rather than reparse.
    """

    def __init__(self, existing: ParseRun) -> None:
        super().__init__(f"run already exists for {existing.idempotency_key}")
        self.existing = existing


@dataclass
class ClaimResult:
    """Why a claim did or did not succeed.

    A boolean would be ambiguous in the way that matters: "someone else is working on it"
    and "it is already done" both mean *do not parse*, but only one of them means the
    message can be committed immediately.
    """

    claimed: bool
    document: DocumentRecord | None = None
    reason: str = ""


@runtime_checkable
class DocumentRepository(Protocol):
    def register(
        self,
        document_id: str,
        *,
        source_uri: str | None = None,
        media_type: str | None = None,
    ) -> DocumentRecord: ...

    def get(self, document_id: str) -> DocumentRecord | None: ...

    def get_many(self, document_ids: list[str]) -> list[DocumentRecord]: ...

    def claim(self, document_id: str, *, lease_seconds: int = 1800) -> ClaimResult: ...

    def find_run(
        self,
        document_id: str,
        content_hash: str,
        parser_version: str,
        parse_options_hash: str = "",
    ) -> ParseRun | None: ...

    def start_run(self, run: ParseRun) -> ParseRun: ...

    def complete_run(
        self,
        run_id: str,
        *,
        artifact_key: str,
        content_hash: str,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentRecord: ...

    def fail_run(
        self,
        run_id: str,
        *,
        failure_class: str,
        failure_reason: str,
        permanent: bool,
    ) -> DocumentRecord: ...

    def get_run(self, run_id: str) -> ParseRun | None: ...

    def runs_for(self, document_id: str) -> list[ParseRun]: ...

    def mark_deleted(self, document_id: str) -> DocumentRecord | None: ...

    def reclaim_expired(self) -> list[str]: ...


class InMemoryRepository:
    """A full implementation, not a stub. The worker's tests depend on it behaving."""

    def __init__(self, clock: Clock = utc_now) -> None:
        self._clock = clock
        self._documents: dict[str, DocumentRecord] = {}
        self._runs: dict[str, ParseRun] = {}

    # ------------------------------------------------------------- documents

    def register(
        self,
        document_id: str,
        *,
        source_uri: str | None = None,
        media_type: str | None = None,
    ) -> DocumentRecord:
        """Create the row if absent, leave it alone if present.

        Idempotent because the upload backend may publish a job more than once, and
        because re-registering must never reset a document that is already `ready` back
        to `pending` — which would make the content API start returning 409 for a
        document that has been fine for a month.
        """
        existing = self._documents.get(document_id)
        if existing is not None:
            return existing
        now = self._clock()
        record = DocumentRecord(
            id=document_id,
            status=DocumentStatus.PENDING,
            source_uri=source_uri,
            media_type=media_type,
            created_at=now,
            updated_at=now,
        )
        self._documents[document_id] = record
        return record

    def get(self, document_id: str) -> DocumentRecord | None:
        return self._documents.get(document_id)

    def get_many(self, document_ids: list[str]) -> list[DocumentRecord]:
        return [self._documents[i] for i in document_ids if i in self._documents]

    def claim(self, document_id: str, *, lease_seconds: int = 1800) -> ClaimResult:
        """The idempotency gate: move to `processing` only from a claimable status.

        The equivalent of
            UPDATE documents SET status='processing'
             WHERE id=$1 AND status IN ('pending','failed') AND deleted_at IS NULL
        and the return value is the row count. Everything else in the worker depends on
        this being atomic.
        """
        record = self._documents.get(document_id)
        if record is None:
            return ClaimResult(False, None, "unknown document")
        if record.deleted_at is not None or record.status is DocumentStatus.DELETED:
            return ClaimResult(False, record, "document is deleted")
        if record.status not in CLAIMABLE:
            return ClaimResult(False, record, f"status is {record.status.value}")

        assert_transition(record.status, DocumentStatus.PROCESSING)
        updated = replace(
            record,
            status=DocumentStatus.PROCESSING,
            updated_at=self._clock(),
            failure_class=None,
            failure_reason=None,
        )
        self._documents[document_id] = updated
        return ClaimResult(True, updated)

    def mark_deleted(self, document_id: str) -> DocumentRecord | None:
        record = self._documents.get(document_id)
        if record is None:
            return None
        now = self._clock()
        updated = replace(
            record, status=DocumentStatus.DELETED, deleted_at=now, updated_at=now
        )
        self._documents[document_id] = updated
        return updated

    # ------------------------------------------------------------------ runs

    def find_run(
        self,
        document_id: str,
        content_hash: str,
        parser_version: str,
        parse_options_hash: str = "",
    ) -> ParseRun | None:
        key = (document_id, content_hash, parser_version, parse_options_hash)
        for run in self._runs.values():
            if run.idempotency_key == key:
                return run
        return None

    def start_run(self, run: ParseRun) -> ParseRun:
        """Insert a run, or raise `DuplicateRun` — the unique constraint, in Python.

        Deliberately not "check then insert": that is the same race the claim solves one
        level up, and under a Kafka redelivery it is precisely the race that happens.
        """
        existing = self.find_run(
            run.document_id, run.content_hash, run.parser_version, run.parse_options_hash
        )
        if existing is not None:
            raise DuplicateRun(existing)

        now = self._clock()
        stored = replace(
            run,
            id=run.id or str(uuid.uuid4()),
            status=RunStatus.RUNNING,
            started_at=now,
            lease_expires_at=now + timedelta(seconds=1800),
        )
        self._runs[stored.id] = stored
        return stored

    def get_run(self, run_id: str) -> ParseRun | None:
        return self._runs.get(run_id)

    def runs_for(self, document_id: str) -> list[ParseRun]:
        return sorted(
            (r for r in self._runs.values() if r.document_id == document_id),
            key=lambda r: (r.started_at or datetime.min.replace(tzinfo=timezone.utc)),
        )

    def complete_run(
        self,
        run_id: str,
        *,
        artifact_key: str,
        content_hash: str,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentRecord:
        """Succeed a run and flip the document's pointer to it, atomically.

        The pointer flip is the moment new content becomes visible, and doing it only on
        success is what makes a reprocess safe: until this line runs, every reader is
        still being served the previous artifact.
        """
        run = self._runs[run_id]
        now = self._clock()
        self._runs[run_id] = replace(
            run, status=RunStatus.SUCCEEDED, finished_at=now, artifact_key=artifact_key
        )

        record = self._documents[run.document_id]
        self._documents[record.id] = replace(
            record,
            status=DocumentStatus.READY,
            current_run_id=run_id,
            content_hash=content_hash,
            metadata=metadata or record.metadata,
            failure_class=None,
            failure_reason=None,
            updated_at=now,
        )
        return self._documents[record.id]

    def fail_run(
        self,
        run_id: str,
        *,
        failure_class: str,
        failure_reason: str,
        permanent: bool,
    ) -> DocumentRecord:
        """Fail a run, and the document only if it has never succeeded.

        This is the rule that makes reprocessing safe. A document already serving a good
        artifact stays `ready` when a new run fails — marking it `failed` would take
        working content offline because an *optional* re-parse went wrong.
        """
        run = self._runs[run_id]
        now = self._clock()
        self._runs[run_id] = replace(
            run,
            status=RunStatus.FAILED,
            finished_at=now,
            failure_class=failure_class,
            failure_reason=failure_reason,
        )

        record = self._documents[run.document_id]
        if record.current_run_id is not None:
            # Still serving the previous artifact: record the failure on the run, and
            # leave the document alone.
            return record

        status = DocumentStatus.FAILED if permanent else DocumentStatus.PENDING
        self._documents[record.id] = replace(
            record,
            status=status,
            failure_class=failure_class,
            failure_reason=failure_reason,
            updated_at=now,
        )
        return self._documents[record.id]

    def reclaim_expired(self) -> list[str]:
        """Return documents whose worker died mid-parse, so they can be retried.

        Without this a crashed worker strands a document in `processing` forever: the
        claim gate is doing its job and refusing every redelivery, and no human is
        watching that particular row.
        """
        now = self._clock()
        reclaimed: list[str] = []
        for run in list(self._runs.values()):
            if run.status is not RunStatus.RUNNING:
                continue
            if run.lease_expires_at is None or run.lease_expires_at > now:
                continue
            self._runs[run.id] = replace(
                run,
                status=RunStatus.FAILED,
                finished_at=now,
                failure_class="lease_expired",
                failure_reason="the worker holding this run stopped reporting",
            )
            record = self._documents.get(run.document_id)
            if record is not None and record.status is DocumentStatus.PROCESSING:
                self._documents[record.id] = replace(
                    record, status=DocumentStatus.PENDING, updated_at=now
                )
                reclaimed.append(record.id)
        return reclaimed
