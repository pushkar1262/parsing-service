"""The worker: fetch, parse, store an artifact, announce the outcome.

Stateless. There is no database and no memory between messages — the only durable results
of processing a document are **an object in S3 and an event on Kafka**, and everything the
service used to record in `documents` and `parse_runs` is now upstream's to own.

Three consequences shape this file:

**The artifact key is the idempotency check.** A parse is a pure function of the raw bytes,
the parser version and the parse options, and the key is built from exactly those three
things — so "have we done this already?" is a `HeadObject`, not a row lookup. The conditional
claim, the lease, and the unique constraint that used to answer it are gone with the table.

**A duplicate still emits its event.** The old worker answered a replay from the database
and stayed silent. Silence is no longer safe: an event is the only notification upstream
gets, so a dropped message would leave a document pending forever with republishing unable
to fix it — the second request would find the artifact present and say nothing at all. So a
duplicate re-reads the existing artifact and emits the same completion event again. Costs one
GET, makes replay a working recovery path, and upstream is required to be idempotent anyway.

**Every terminal outcome is announced.** Success goes to `documents.parse.completed`,
permanent failure and exhausted retries to `documents.parse.failed`. A transient failure
mid-tiers announces nothing, because it is still being worked on. If neither event is ever
published, the document is stuck — which is why upstream needs a staleness sweep and why
`publisher=None` is a development-only configuration.

`process` deliberately returns an `Outcome` rather than raising. A worker loop that has to
catch exceptions to decide whether to commit an offset will eventually catch the wrong one
— and "should this be retried?" is a decision with a right answer that belongs where the
cause is known, not in an `except Exception` three frames up.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from domain.document import ParsedDocument
from domain.errors import ServiceError
from domain.status import DocumentStatus
from parse.pipeline import parse_document_full
from parse.registry import Registry, default_registry
from store.artifacts import ArtifactStore, artifact_key
from store.blobs import Storage
from work.queue import (
    TOPIC_COMPLETED,
    TOPIC_DLQ,
    TOPIC_FAILED,
    CompletionEvent,
    FailureEvent,
    Job,
    Message,
    Publisher,
    next_destination,
)


class Disposition(str, Enum):
    """What the loop should do with the message.

    `COMMIT` covers success *and* permanent failure, because both are final: the document
    has its answer announced and redelivering the message would only produce the same
    answer again.
    """

    COMMIT = "commit"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


@dataclass
class Outcome:
    disposition: Disposition
    document_id: str
    run_id: str | None = None
    status: DocumentStatus | None = None
    artifact_key: str | None = None
    failure_class: str | None = None
    failure_reason: str | None = None
    skipped: bool = False
    detail: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.disposition is Disposition.COMMIT and self.failure_class is None


class Worker:
    """One document at a time. Stateless apart from its collaborators."""

    def __init__(
        self,
        *,
        storage: Storage,
        artifacts: ArtifactStore,
        publisher: Publisher | None = None,
        registry: Registry | None = None,
        parser_version: str = "1.0",
        resolve_reference: Callable[[str], str] | None = None,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.storage = storage
        self.artifacts = artifacts
        self.publisher = publisher
        self.registry = registry or default_registry()
        # Stamped on every artifact and part of the key, so cutting a new version is what
        # makes a backfill reparse rather than skip.
        self.parser_version = parser_version
        # A queue message may carry a bare S3 key rather than a full URI, and `parse_ref`
        # would read that as a local filesystem path. Resolution happens here, once, so no
        # entrypoint has to remember to do it.
        self._resolve = resolve_reference or (lambda reference: reference)
        self._on_event = on_event or (lambda name, fields: None)
        self._now = now

    # ------------------------------------------------------------------ main

    def process(self, job: Job) -> Outcome:
        reference = self._resolve(job.reference)

        try:
            fetched = self.storage.fetch(reference)
        except ServiceError as exc:
            # Before any parse: no run id and no content hash, because there are no bytes.
            return self._failed(job, exc, run_id=None, content_hash=None)

        options_hash = _options_hash(job.parse_options)
        key = artifact_key(fetched.content_hash, self.parser_version, options_hash)

        if not job.force:
            duplicate = self._duplicate(job, key)
            if duplicate is not None:
                return duplicate

        run_id = str(uuid.uuid4())
        try:
            output = parse_document_full(
                fetched.data,
                document_id=job.document_id,
                content_hash=fetched.content_hash,
                media_type=fetched.declared_media_type,
                # The uploaded filename, not the S3 key: the key is a UUID, and Markdown
                # and plain text are byte-identical, so the extension is the only signal.
                filename=job.filename or _filename_of(job, fetched),
                source=fetched.source,
                registry=self.registry,
                # Must match the version in `key` above, or the dedup check and the write
                # address two different objects.
                parser_version=self.parser_version,
            )
        except ServiceError as exc:
            return self._failed(
                job, exc, run_id=run_id, content_hash=fetched.content_hash
            )
        except Exception as exc:  # noqa: BLE001 - an unexpected parser bug
            # Transient by default: an unclassified crash is more likely a bug we will
            # fix than a property of the document, and a retry costs less than a document
            # dead-lettered for a reason nobody wrote down.
            return self._failed(
                job,
                _Unclassified(str(exc)),
                run_id=run_id,
                content_hash=fetched.content_hash,
            )

        return self._succeeded(
            job, run_id, output.document, output.page_images, options_hash
        )

    # ------------------------------------------------------------- outcomes

    def _succeeded(
        self,
        job: Job,
        run_id: str,
        document: ParsedDocument,
        page_images: dict[int, bytes],
        options_hash: str,
    ) -> Outcome:
        # The artifact before the announcement, always. An event naming an object that was
        # never written is a 404 for every consumer that reacts to it; an object nobody was
        # told about is invisible, which a replay fixes.
        key = self.artifacts.put(
            document, page_images=page_images, options_hash=options_hash
        )

        metrics = _metrics(document)
        self._emit("job.succeeded", job, run_id=run_id, **metrics)
        self._announce_success(job, run_id, document, key, duplicate=False)

        return Outcome(
            Disposition.COMMIT,
            job.document_id,
            run_id=run_id,
            status=DocumentStatus.READY,
            artifact_key=key,
            metrics=metrics,
        )

    def _duplicate(self, job: Job, key: str) -> Outcome | None:
        """These bytes are already parsed at this version. Re-announce and skip.

        Returns None when there is nothing to reuse, so the caller parses normally. That
        covers the object being absent and the object being unreadable — a truncated or
        half-written artifact should be replaced by a real parse, not served forever
        because a `HeadObject` said something was there.
        """
        try:
            if not self.artifacts.exists(key):
                return None
            document = self.artifacts.get(key)
        except (ServiceError, ValueError):
            # ValueError covers a pydantic ValidationError: the object is there but is not
            # a document, which is a half-written or truncated write. Parse it again rather
            # than serving the corruption forever on the strength of a `HeadObject`.
            return None

        run_id = str(uuid.uuid4())
        metrics = _metrics(document)
        self._emit("job.duplicate", job, run_id=run_id, artifact_key=key, **metrics)
        self._announce_success(job, run_id, document, key, duplicate=True)

        return Outcome(
            Disposition.COMMIT,
            job.document_id,
            run_id=run_id,
            status=DocumentStatus.READY,
            artifact_key=key,
            skipped=True,
            detail="an artifact already exists for these bytes and parser version",
            metrics=metrics,
        )

    def _announce_success(
        self,
        job: Job,
        run_id: str,
        document: ParsedDocument,
        key: str,
        *,
        duplicate: bool,
    ) -> None:
        if self.publisher is None:
            return
        self.publisher.publish(
            Message(
                topic=TOPIC_COMPLETED,
                key=job.document_id,
                value=CompletionEvent(
                    document_id=job.document_id,
                    reference=job.reference,
                    run_id=run_id,
                    content_hash=document.content_hash,
                    artifact_key=key,
                    status=DocumentStatus.READY.value,
                    # Carried from the job, not looked up: this is the tenant the work was
                    # actually scoped to, and it is the only copy of it that survives.
                    tenant_id=job.tenant_id,
                    project_id=job.project_id,
                    metadata=document.metadata.model_dump(mode="json"),
                    warnings=[w.code for w in document.warnings],
                    duplicate=duplicate,
                    completed_at=self._now().isoformat(),
                    event_id=job.event_id,
                    trace_id=job.trace_id,
                ).to_bytes(),
                headers=self._headers(job),
            )
        )

    def _failed(
        self,
        job: Job,
        exc: ServiceError,
        *,
        run_id: str | None,
        content_hash: str | None,
    ) -> Outcome:
        if not exc.transient:
            self._emit("job.failed_permanently", job, failure_class=exc.failure_class)
            self._announce_failure(
                job, exc, run_id=run_id, content_hash=content_hash, permanent=True
            )
            self._to_dead_letter(job, exc)
            return Outcome(
                Disposition.DEAD_LETTER,
                job.document_id,
                run_id=run_id,
                status=DocumentStatus.FAILED,
                failure_class=exc.failure_class,
                failure_reason=str(exc),
            )

        destination = next_destination(job)
        if destination is None:
            # Out of tiers. Terminal in practice, so it is announced — but as a transient
            # failure that ran out of attempts, which is a different thing from a fact
            # about the document and often worth replaying after an infrastructure fix.
            self._emit("job.retries_exhausted", job, failure_class=exc.failure_class)
            self._announce_failure(
                job, exc, run_id=run_id, content_hash=content_hash, permanent=False
            )
            self._to_dead_letter(job, exc, exhausted=True)
            return Outcome(
                Disposition.DEAD_LETTER,
                job.document_id,
                run_id=run_id,
                status=DocumentStatus.FAILED,
                failure_class=exc.failure_class,
                failure_reason=str(exc),
                detail="retries exhausted",
            )

        topic, retry = destination
        if self.publisher is not None:
            self.publisher.publish(
                Message(
                    topic=topic,
                    key=job.document_id,
                    value=retry.to_bytes(),
                    headers={"not_before": retry.not_before.isoformat()},
                )
            )
        # Nothing announced: this document is still being worked on, and telling upstream
        # it failed would show a user a failure that fixes itself ninety seconds later.
        self._emit("job.retrying", job, failure_class=exc.failure_class, topic=topic)
        return Outcome(
            Disposition.RETRY,
            job.document_id,
            run_id=run_id,
            failure_class=exc.failure_class,
            failure_reason=str(exc),
            detail=topic,
        )

    def _announce_failure(
        self,
        job: Job,
        exc: ServiceError,
        *,
        run_id: str | None,
        content_hash: str | None,
        permanent: bool,
    ) -> None:
        if self.publisher is None:
            return
        self.publisher.publish(
            Message(
                topic=TOPIC_FAILED,
                key=job.document_id,
                value=FailureEvent(
                    document_id=job.document_id,
                    reference=job.reference,
                    status=DocumentStatus.FAILED.value,
                    # The original class, not "transient_exhausted": *what* broke is the
                    # useful half, and `permanent` already says whether it can recur.
                    failure_class=exc.failure_class,
                    failure_reason=str(exc),
                    permanent=permanent,
                    attempt=job.attempt,
                    tenant_id=job.tenant_id,
                    project_id=job.project_id,
                    run_id=run_id,
                    content_hash=content_hash,
                    failed_at=self._now().isoformat(),
                    event_id=job.event_id,
                    trace_id=job.trace_id,
                ).to_bytes(),
                headers={
                    **self._headers(job),
                    "failure_class": exc.failure_class,
                    "permanent": "true" if permanent else "false",
                },
            )
        )

    def _to_dead_letter(
        self, job: Job, exc: ServiceError, *, exhausted: bool = False
    ) -> None:
        if self.publisher is None:
            return
        self.publisher.publish(
            Message(
                topic=TOPIC_DLQ,
                key=job.document_id,
                value=job.to_bytes(),
                headers={
                    # Everything needed to replay after a fix, in the headers so it can be
                    # read without deserialising a payload that may be the broken part.
                    "failure_class": (
                        "transient_exhausted" if exhausted else exc.failure_class
                    ),
                    "failure_reason": str(exc),
                    "attempt": str(job.attempt),
                    "trace_id": job.trace_id or "",
                },
            )
        )

    def _headers(self, job: Job) -> dict[str, str]:
        """Correlation context, so a consumer can route or filter without deserialising."""
        return {
            k: v
            for k, v in (
                ("document_id", job.document_id),
                ("tenant_id", job.tenant_id),
                ("trace_id", job.trace_id),
            )
            if v
        }

    def _emit(self, event: str, job: Job, **fields: Any) -> None:
        self._on_event(
            event,
            {
                "document_id": job.document_id,
                "tenant_id": job.tenant_id,
                "project_id": job.project_id,
                "attempt": job.attempt,
                "trace_id": job.trace_id,
                "event_id": job.event_id,
                **fields,
            },
        )


class _Unclassified(ServiceError):
    transient = True
    failure_class = "internal"


def _metrics(document: ParsedDocument) -> dict[str, Any]:
    metadata = document.metadata
    return {
        "format": metadata.format,
        "chars": metadata.char_count,
        "blocks": metadata.block_count,
        "pages": metadata.page_count,
        "ocr_pages": metadata.ocr_page_count,
        # The silent-failure detector: a PDF with a broken font map extracts three
        # characters per page and reports complete success.
        "chars_per_page": (
            metadata.char_count / metadata.page_count if metadata.page_count else None
        ),
        "warnings": [w.code for w in document.warnings],
    }


def _options_hash(options: dict[str, Any]) -> str:
    """A stable fingerprint of the parse options, for the artifact key.

    Sorted, so `{"a":1,"b":2}` and `{"b":2,"a":1}` are the same parse rather than two.
    """
    if not options:
        return ""
    import hashlib
    import json

    encoded = json.dumps(options, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2b(encoded.encode(), digest_size=8).hexdigest()


def _filename_of(job: Job, fetched: Any) -> str | None:
    key = getattr(fetched.ref, "key", None) or job.reference
    return key.rsplit("/", 1)[-1] if key else None
