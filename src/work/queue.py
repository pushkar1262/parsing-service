"""Jobs, and where a failed one goes next.

Kafka has no delayed delivery, so retries use **tiered retry topics**: a job that failed
transiently is republished to `parse.retry.30s` with a `not_before` header, and the
consumer for that topic pauses the partition until the time arrives rather than sleeping
inside the poll loop. Sleeping is what gets a consumer evicted from its group.

Only transient failures walk the tiers. Everything in the permanent set goes straight to
the dead-letter queue with `status=failed` — retrying an unsupported format four times is
pure cost with a guaranteed outcome, and it delays the permanent answer by 35 minutes.

The queue is a protocol with an in-memory implementation, so the worker's ordering
guarantees — claim, then work, then commit — are tested without a broker.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

TOPIC_REQUESTED = "documents.parse.requested"
TOPIC_COMPLETED = "documents.parse.completed"
TOPIC_FAILED = "documents.parse.failed"
TOPIC_DELETED = "documents.deleted"
TOPIC_DLQ = "documents.parse.dlq"

# Exponential with a cap, as separate topics because Kafka cannot delay a message.
RETRY_TIERS: tuple[tuple[str, int], ...] = (
    ("documents.parse.retry.30s", 30),
    ("documents.parse.retry.5m", 300),
    ("documents.parse.retry.30m", 1800),
)


@dataclass
class Job:
    """One document to process.

    `reference` rather than bytes: the queue carries a pointer and the worker fetches,
    because a 90 MB PDF has no business inside a Kafka message and because the raw file
    must stay retrievable for reprocessing long after the message is gone.

    `tenant_id` is not decoration, and it matters more now than when there was a database to
    hold it. It arrives on the upload event, travels through the job, and leaves on the
    outcome event — which is the only place it exists once this service commits the offset.
    Drop it anywhere along that path and upstream receives a result it cannot attribute to
    anyone, so it can neither store it nor serve it.
    """

    document_id: str
    reference: str
    tenant_id: str | None = None
    project_id: str | None = None
    media_type: str | None = None
    # The name the user uploaded under. Worth carrying separately from the S3 key: the key
    # is a UUID, and Markdown and plain text are byte-identical, so the extension here is
    # the only thing that tells them apart.
    filename: str | None = None
    # The size the producer saw — a cheap early reject before transferring anything.
    size: int | None = None
    # The producer's id for this event, carried into logs so one upload can be traced
    # across two services without correlating on timestamps.
    event_id: str | None = None
    parse_options: dict[str, Any] = field(default_factory=dict)
    attempt: int = 1
    force: bool = False
    trace_id: str | None = None
    not_before: datetime | None = None

    def to_bytes(self) -> bytes:
        payload = asdict(self)
        if self.not_before is not None:
            payload["not_before"] = self.not_before.isoformat()
        return json.dumps(payload).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> Job:
        """Accept both shapes on the wire.

        The upload backend publishes its own event schema (`s3_bucket`/`s3_key`); a retry
        or DLQ replay is this class's own serialisation. One consumer group reads both, so
        it has to read both — and dispatching on a field rather than on the topic means a
        replayed DLQ message works wherever it is published.
        """
        payload = json.loads(raw.decode("utf-8"))
        if "reference" not in payload:
            return cls.from_event(payload)
        if payload.get("not_before"):
            payload["not_before"] = datetime.fromisoformat(payload["not_before"])
        # Forward compatibility: a producer adding a field must not stop this consumer.
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in payload.items() if k in known})

    @classmethod
    def from_event(cls, payload: dict[str, Any]) -> Job:
        """Map the upload backend's event onto a job.

        Strict about the fields that cannot be guessed. A missing `s3_key` produces a job
        that fails looking like a storage problem; a missing `tenant_id` produces an outcome
        event upstream cannot attribute to any tenant, and so cannot act on. All of them are
        refused here, where the message is still in hand and can be dead-lettered with a
        reason naming the field.
        """
        missing = [
            key
            for key in ("document_id", "s3_bucket", "s3_key", "tenant_id")
            if not str(payload.get(key) or "").strip()
        ]
        if missing:
            raise MalformedEvent(
                f"event is missing required field(s): {', '.join(missing)}"
            )

        bucket = str(payload["s3_bucket"]).strip()
        key = str(payload["s3_key"]).strip().lstrip("/")
        size = str(payload.get("size") or "")
        # `force` and `parse_options` are read here, not only from this service's own
        # serialisation, because reprocessing is driven from upstream now. Without them a
        # reprocess arrives looking identical to the original request, finds the artifact
        # already in place, and is skipped — a silent no-op rather than a re-parse.
        options = payload.get("parse_options")
        return cls(
            document_id=str(payload["document_id"]).strip(),
            # Fully qualified, so no prefix resolution is needed or attempted: the producer
            # said exactly which bucket and key, and that is more trustworthy than a
            # convention this service would have to keep in step with.
            reference=f"s3://{bucket}/{key}",
            tenant_id=_optional(payload.get("tenant_id")),
            project_id=_optional(payload.get("project_id")),
            media_type=_optional(payload.get("content_type")),
            filename=_optional(payload.get("filename")) or key.rsplit("/", 1)[-1],
            size=int(size) if size.isdigit() else None,
            event_id=_optional(payload.get("event_id")),
            trace_id=_optional(payload.get("trace_id"))
            or _optional(payload.get("event_id")),
            parse_options=options if isinstance(options, dict) else {},
            force=bool(payload.get("force")),
        )

    @property
    def key(self) -> str:
        """The partition key.

        `document_id`, so every job for one document lands on one partition and is
        consumed by one member of the group. Concurrent processing of the same document
        becomes structurally impossible rather than merely unlikely — which is the
        foundation the claim gate builds on.
        """
        return self.document_id


@dataclass
class CompletionEvent:
    """A parse finished. Published on `documents.parse.completed`.

    Deliberately not a `Job`. A job describes work still to do — `attempt`, `force`,
    `not_before`, `parse_options` — and none of that means anything once the work is done,
    while everything a consumer wants (`run_id`, `artifact_key`, the metadata) is absent
    from it. Reusing the inbound shape here produced an event carrying four fields that
    were noise and none of the ones that let a consumer act.

    **`tenant_id` is what makes the event usable at all.** A consumer reacting to this
    calls the content API, which requires `X-Tenant-Id` and answers a mismatch with 404,
    not 403. An event without it can only be acted on by a consumer that already knew the
    tenant — which is the polling this event exists to remove.

    `metadata` is carried so the common case needs no round-trip: a consumer can see
    `page_count`, `char_count` and `ocr_applied` and decide whether it even wants the
    content. The status API stays the source of truth; this is a notification, not a
    replacement for it.
    """

    document_id: str
    reference: str
    run_id: str
    content_hash: str
    artifact_key: str
    status: str
    tenant_id: str | None = None
    project_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    # Warning *codes*, not prose: a consumer branches on `ocr_low_confidence`, and the
    # human-readable message belongs in the artifact where it will not be parsed.
    warnings: list[str] = field(default_factory=list)
    # True when this document's bytes were already parsed at this parser version and the
    # existing artifact was reused. The event is still emitted — see `Worker._duplicate`.
    duplicate: bool = False
    completed_at: str | None = None
    event_id: str | None = None
    trace_id: str | None = None

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> CompletionEvent:
        payload = json.loads(raw.decode("utf-8"))
        # Same forward compatibility as `Job`: this service adding a field must not stop
        # a consumer that was deployed before it.
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in payload.items() if k in known})

    @property
    def key(self) -> str:
        """`document_id`, matching the requested topic — so a consumer joining the two
        streams sees both partitioned the same way."""
        return self.document_id


@dataclass
class FailureEvent:
    """A parse will not succeed. Published on `documents.parse.failed`.

    Emitted only on a **terminal** outcome: a permanent failure, or transient retries
    exhausted. An intermediate transient failure is still being worked on and stays inside
    the retry tiers — publishing it would have a document flapping into `failed` and back
    while the tiers are still running, and upstream would show a user a failure that fixes
    itself ninety seconds later.

    This event is the reason the database can be removed. Without it a permanent failure
    leaves no trace anywhere except a dead-letter message nobody consumes: the document
    would sit in `pending` forever, and "stuck" and "failed for a nameable reason" would be
    indistinguishable from upstream.
    """

    document_id: str
    reference: str
    status: str
    failure_class: str
    failure_reason: str
    # False means the failure is transient and the retry tiers are exhausted. Worth
    # separating: a permanent failure is a fact about the document, an exhausted one is
    # usually a fact about our infrastructure and may be worth replaying after a fix.
    permanent: bool
    attempt: int = 1
    tenant_id: str | None = None
    project_id: str | None = None
    # Null when the failure happened before parsing began — a fetch that never returned
    # bytes has no hash and no run.
    run_id: str | None = None
    content_hash: str | None = None
    failed_at: str | None = None
    event_id: str | None = None
    trace_id: str | None = None

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> FailureEvent:
        payload = json.loads(raw.decode("utf-8"))
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in payload.items() if k in known})

    @property
    def key(self) -> str:
        return self.document_id


@dataclass
class Message:
    topic: str
    key: str
    value: bytes
    headers: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class Publisher(Protocol):
    def publish(self, message: Message) -> None: ...


def next_destination(job: Job) -> tuple[str, Job] | None:
    """Where a transiently-failed job goes, or None when the tiers are exhausted.

    Returning None rather than the DLQ topic keeps the *decision* here and the *action*
    at the call site, where the document's status also has to change — those two must not
    drift apart.
    """
    if job.attempt > len(RETRY_TIERS):
        return None
    topic, delay = RETRY_TIERS[job.attempt - 1]
    return topic, Job(
        **{
            **asdict(job),
            "attempt": job.attempt + 1,
            "not_before": datetime.now(timezone.utc) + timedelta(seconds=delay),
        }
    )


class InMemoryQueue:
    """A queue and a publisher in one, for tests and for the local runner.

    Records everything published so a test can assert *where* a failure was routed, which
    is the part of retry handling that is easy to get subtly wrong and impossible to
    notice in production until the DLQ is empty and documents are quietly looping.
    """

    def __init__(self) -> None:
        self.topics: dict[str, list[Message]] = {}

    def publish(self, message: Message) -> None:
        self.topics.setdefault(message.topic, []).append(message)

    def submit(self, job: Job, topic: str = TOPIC_REQUESTED) -> None:
        self.publish(Message(topic=topic, key=job.key, value=job.to_bytes()))

    def drain(self, topic: str = TOPIC_REQUESTED) -> list[Job]:
        messages = self.topics.pop(topic, [])
        return [Job.from_bytes(m.value) for m in messages]

    def jobs_in(self, topic: str) -> list[Job]:
        return [Job.from_bytes(m.value) for m in self.topics.get(topic, [])]

    def events_in(self, topic: str) -> list[CompletionEvent]:
        return [CompletionEvent.from_bytes(m.value) for m in self.topics.get(topic, [])]

    def failures_in(self, topic: str = TOPIC_FAILED) -> list[FailureEvent]:
        return [FailureEvent.from_bytes(m.value) for m in self.topics.get(topic, [])]

    def count(self, topic: str) -> int:
        return len(self.topics.get(topic, []))


class MalformedEvent(Exception):
    """The message cannot be turned into a job.

    Permanent by nature: republishing an event with no `s3_key` produces the same event, so
    the consumer dead-letters it rather than walking the retry tiers.
    """


def _optional(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
