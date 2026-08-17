"""The worker: fetch, parse, persist, and only then acknowledge.

Everything here is arranged around one rule, and the rest follows from it:

    **The offset is committed last, after the database commit.**

Crash before it and the message is redelivered — caught by the claim gate or the run's
unique constraint, both of which answer "already done" without reparsing. Crash after it
and the work is durable. Get the order backwards and a crash between the commit and the
write silently drops a document: nothing errors, nothing retries, and the document sits in
`pending` until somebody notices it was never processed.

`process` deliberately returns an `Outcome` rather than raising. A worker loop that has to
catch exceptions to decide whether to commit an offset will eventually catch the wrong one
— and "should this be retried?" is a decision with a right answer that belongs where the
cause is known, not in an `except Exception` three frames up.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from domain.document import ParsedDocument
from domain.errors import ServiceError
from domain.status import DocumentStatus, ParseRun
from parse.pipeline import parse_document_full
from parse.registry import Registry, default_registry
from store.artifacts import ArtifactStore
from store.blobs import Storage
from store.repository import ClaimResult, DocumentRepository, DuplicateRun
from work.queue import (
    TOPIC_COMPLETED,
    TOPIC_DLQ,
    Job,
    Message,
    Publisher,
    next_destination,
)


class Disposition(str, Enum):
    """What the loop should do with the message.

    `COMMIT` covers success *and* permanent failure, because both are final: the document
    has its answer recorded and redelivering the message would only produce the same
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
        repository: DocumentRepository,
        storage: Storage,
        artifacts: ArtifactStore,
        publisher: Publisher | None = None,
        registry: Registry | None = None,
        parser_version: str = "1.0",
        resolve_reference: Callable[[str], str] | None = None,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.repository = repository
        self.storage = storage
        self.artifacts = artifacts
        self.publisher = publisher
        self.registry = registry or default_registry()
        # Stamped on every run and part of the idempotency key, so cutting a new version
        # is what makes a backfill reparse rather than skip.
        self.parser_version = parser_version
        # A queue message may carry a bare S3 key rather than a full URI, and `parse_ref`
        # would read that as a local filesystem path. Resolution happens here, once, so no
        # entrypoint has to remember to do it.
        self._resolve = resolve_reference or (lambda reference: reference)
        self._on_event = on_event or (lambda name, fields: None)

    # ------------------------------------------------------------------ main

    def process(self, job: Job) -> Outcome:
        reference = self._resolve(job.reference)
        self.repository.register(
            job.document_id, source_uri=reference, media_type=job.media_type
        )

        claim = self.repository.claim(job.document_id)
        if not claim.claimed:
            return self._already_handled(job, claim)

        try:
            fetched = self.storage.fetch(reference)
        except ServiceError as exc:
            return self._failed(job, None, exc)

        options_hash = _options_hash(job.parse_options)
        existing = self.repository.find_run(
            job.document_id, fetched.content_hash, self.parser_version, options_hash
        )
        if existing is not None and not job.force:
            # A replay of work already done. Answering from the database rather than
            # reparsing is what makes at-least-once delivery cheap instead of merely
            # correct.
            self._emit("job.duplicate", job, run_id=existing.id)
            return Outcome(
                Disposition.COMMIT,
                job.document_id,
                run_id=existing.id,
                skipped=True,
                detail="a run already exists for these bytes and parser version",
            )

        try:
            run = self.repository.start_run(
                ParseRun(
                    id=str(uuid.uuid4()),
                    document_id=job.document_id,
                    content_hash=fetched.content_hash,
                    parser_version=self.parser_version,
                    parse_options_hash=options_hash,
                    attempt=job.attempt,
                    trace_id=job.trace_id,
                )
            )
        except DuplicateRun as duplicate:
            # Lost the race to a concurrent delivery. Not an error: the other worker owns
            # this one, and committing lets it get on with it.
            return Outcome(
                Disposition.COMMIT,
                job.document_id,
                run_id=duplicate.existing.id,
                skipped=True,
                detail="another worker started this run first",
            )

        try:
            output = parse_document_full(
                fetched.data,
                document_id=job.document_id,
                content_hash=fetched.content_hash,
                media_type=fetched.declared_media_type,
                filename=_filename_of(job, fetched),
                source=fetched.source,
                registry=self.registry,
            )
        except ServiceError as exc:
            return self._failed(job, run.id, exc)
        except Exception as exc:  # noqa: BLE001 - an unexpected parser bug
            # Transient by default: an unclassified crash is more likely a bug we will
            # fix than a property of the document, and a retry costs less than a document
            # dead-lettered for a reason nobody wrote down.
            return self._failed(job, run.id, _Unclassified(str(exc)))

        return self._succeeded(job, run, output.document, output.page_images)

    # ------------------------------------------------------------- outcomes

    def _succeeded(
        self,
        job: Job,
        run: ParseRun,
        document: ParsedDocument,
        page_images: dict[int, bytes],
    ) -> Outcome:
        key = self.artifacts.put(document, page_images=page_images)

        # Database last, after the artifact exists. A row pointing at an object that was
        # never written is a 500 on the content API; an object with no row is an orphan a
        # sweeper cleans up.
        record = self.repository.complete_run(
            run.id,
            artifact_key=key,
            content_hash=document.content_hash,
            metadata=document.metadata.model_dump(mode="json"),
        )

        metrics = {
            "format": document.metadata.format,
            "chars": document.metadata.char_count,
            "blocks": document.metadata.block_count,
            "pages": document.metadata.page_count,
            "ocr_pages": document.metadata.ocr_page_count,
            # The silent-failure detector: a PDF with a broken font map extracts three
            # characters per page and reports complete success.
            "chars_per_page": (
                document.metadata.char_count / document.metadata.page_count
                if document.metadata.page_count
                else None
            ),
            "warnings": [w.code for w in document.warnings],
        }
        self._emit("job.succeeded", job, run_id=run.id, **metrics)

        if self.publisher is not None:
            self.publisher.publish(
                Message(
                    topic=TOPIC_COMPLETED,
                    key=job.document_id,
                    value=Job(
                        document_id=job.document_id,
                        reference=job.reference,
                        trace_id=job.trace_id,
                    ).to_bytes(),
                )
            )

        return Outcome(
            Disposition.COMMIT,
            job.document_id,
            run_id=run.id,
            status=record.status,
            artifact_key=key,
            metrics=metrics,
        )

    def _failed(self, job: Job, run_id: str | None, exc: ServiceError) -> Outcome:
        if run_id is not None:
            record = self.repository.fail_run(
                run_id,
                failure_class=exc.failure_class,
                failure_reason=str(exc),
                permanent=not exc.transient,
            )
        else:
            record = self._fail_document(job.document_id, exc)

        if not exc.transient:
            self._emit(
                "job.failed_permanently", job, failure_class=exc.failure_class
            )
            self._to_dead_letter(job, exc)
            return Outcome(
                Disposition.DEAD_LETTER,
                job.document_id,
                run_id=run_id,
                status=record.status if record else None,
                failure_class=exc.failure_class,
                failure_reason=str(exc),
            )

        destination = next_destination(job)
        if destination is None:
            self._emit("job.retries_exhausted", job, failure_class=exc.failure_class)
            self._to_dead_letter(job, exc, exhausted=True)
            if run_id is not None:
                self.repository.fail_run(
                    run_id,
                    failure_class="transient_exhausted",
                    failure_reason=str(exc),
                    permanent=True,
                )
            return Outcome(
                Disposition.DEAD_LETTER,
                job.document_id,
                run_id=run_id,
                failure_class="transient_exhausted",
                failure_reason=str(exc),
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
        self._emit("job.retrying", job, failure_class=exc.failure_class, topic=topic)
        return Outcome(
            Disposition.RETRY,
            job.document_id,
            run_id=run_id,
            failure_class=exc.failure_class,
            failure_reason=str(exc),
            detail=topic,
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
                    # Everything needed to replay after a fix, without going back to the
                    # database to work out what happened.
                    "failure_class": (
                        "transient_exhausted" if exhausted else exc.failure_class
                    ),
                    "failure_reason": str(exc),
                    "attempt": str(job.attempt),
                    "trace_id": job.trace_id or "",
                },
            )
        )

    def _already_handled(self, job: Job, claim: ClaimResult) -> Outcome:
        """A claim that failed is usually good news, and never a reason to retry.

        Already `ready`, already `processing`, or deleted: in every case reparsing would
        be wrong, so the message is committed and the reason recorded.
        """
        self._emit("job.not_claimed", job, reason=claim.reason)
        return Outcome(
            Disposition.COMMIT,
            job.document_id,
            status=claim.document.status if claim.document else None,
            skipped=True,
            detail=claim.reason,
        )

    def _fail_document(self, document_id: str, exc: ServiceError):
        record = self.repository.get(document_id)
        if record is None:
            return None
        run = self.repository.start_run(
            ParseRun(
                id=str(uuid.uuid4()),
                document_id=document_id,
                content_hash=f"unfetched:{document_id}",
                parser_version=self.parser_version,
            )
        )
        return self.repository.fail_run(
            run.id,
            failure_class=exc.failure_class,
            failure_reason=str(exc),
            permanent=not exc.transient,
        )

    def _emit(self, event: str, job: Job, **fields: Any) -> None:
        self._on_event(
            event,
            {
                "document_id": job.document_id,
                "attempt": job.attempt,
                "trace_id": job.trace_id,
                **fields,
            },
        )


class _Unclassified(ServiceError):
    transient = True
    failure_class = "internal"


def _options_hash(options: dict[str, Any]) -> str:
    """A stable fingerprint of the parse options, for the idempotency key.

    Sorted, so `{"a":1,"b":2}` and `{"b":2,"a":1}` are the same run rather than two.
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
