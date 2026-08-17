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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

TOPIC_REQUESTED = "documents.parse.requested"
TOPIC_COMPLETED = "documents.parse.completed"
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
    """

    document_id: str
    reference: str
    media_type: str | None = None
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
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("not_before"):
            payload["not_before"] = datetime.fromisoformat(payload["not_before"])
        return cls(**payload)

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

    def count(self, topic: str) -> int:
        return len(self.topics.get(topic, []))
