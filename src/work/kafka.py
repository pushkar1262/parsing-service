"""The Kafka adapter, and the four settings that decide whether it works.

Broker-agnostic by design: this talks the Kafka protocol through `confluent-kafka`, so it
runs against Kafka, Redpanda or MSK without a code change. `docker-compose.yml` uses
Redpanda locally — same protocol, one binary, no JVM.

Most of this file is configuration, and that is the honest proportion: the consume loop
is twenty lines, and the settings around it are what separate a worker that processes
documents from one that reprocesses the same document forever.

**`max.poll.interval.ms` is the one that bites.** A 20-minute OCR job exceeds the 5-minute
default, the broker decides the consumer is dead, the group rebalances, and another worker
starts the same document *while the first is still running*. It produces duplicate work
and rebalance storms, and running more workers makes it worse rather than better, because
the eviction disturbs the whole group. So: `max.poll.records = 1`, a poll interval above
the worst-case document, and a hard per-document timeout strictly below it.

**`enable.auto.commit = false`**, because the offset must be committed after the database
write, and auto-commit commits on a timer that knows nothing about whether the work
finished.

**The partition key is `document_id`**, so all jobs for one document land on one partition
and are consumed by one member of the group. That is what makes concurrent processing of
the same document structurally impossible rather than merely unlikely, and every
idempotency guarantee downstream leans on it.

**Retry topics are paused rather than slept on.** Kafka has no delayed delivery, so a
retry job carries a `not_before` header; when the consumer sees one that is not yet due it
*pauses the partition* and returns. Sleeping inside the poll loop is exactly what triggers
the eviction described above — the fix for one problem being the cause of the other is why
this is written down here rather than discovered later.

> Not executed in the environment this was written in: there is no broker here. The
> worker logic it drives is covered by `tests/test_worker.py` against `InMemoryQueue`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from work.queue import RETRY_TIERS, TOPIC_DLQ, TOPIC_REQUESTED, Job, MalformedEvent, Message
from work.worker import Disposition, Outcome, Worker

log = logging.getLogger(__name__)

# Above the worst-case document, and paired with a hard per-document timeout below it.
# Thirty minutes is chosen for a 500-page scan going through OCR.
MAX_POLL_INTERVAL_MS = 30 * 60 * 1000
DOCUMENT_TIMEOUT_S = 25 * 60


@dataclass
class KafkaConfig:
    # `localhost:19092` for the Redpanda in docker-compose.yml; the broker's own
    # advertised address in a cluster.
    bootstrap_servers: str
    group_id: str = "parsing-service"
    topics: tuple[str, ...] = (TOPIC_REQUESTED, *(t for t, _ in RETRY_TIERS))
    client_id: str = "parsing-worker"
    extra: dict[str, Any] = field(default_factory=dict)

    def consumer_settings(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": self.client_id,
            # Commit explicitly, after the database write. See the module docstring.
            "enable.auto.commit": False,
            # One document per poll, so the interval below covers exactly one document
            # rather than a batch whose worst case is unbounded.
            "max.poll.interval.ms": MAX_POLL_INTERVAL_MS,
            "max.poll.records": 1,
            # Start at the beginning for a new group: a document published before this
            # consumer existed still needs parsing.
            "auto.offset.reset": "earliest",
            **self.extra,
        }

    def producer_settings(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": f"{self.client_id}-producer",
            # A retry or DLQ message that is lost defeats the entire retry design, so
            # durability is worth the latency here.
            "acks": "all",
            "enable.idempotence": True,
            **self.extra,
        }


class KafkaPublisher:
    """Publishes retries, dead letters and completion events."""

    def __init__(self, config: KafkaConfig, *, producer: Any | None = None) -> None:
        self._config = config
        self._producer = producer

    def _handle(self) -> Any:
        if self._producer is None:
            from confluent_kafka import Producer

            self._producer = Producer(self._config.producer_settings())
        return self._producer

    def publish(self, message: Message) -> None:
        self._handle().produce(
            topic=message.topic,
            key=message.key.encode("utf-8"),
            value=message.value,
            headers=[(k, v.encode("utf-8")) for k, v in message.headers.items()],
        )
        # Flush per message rather than batching. A retry that is still sitting in a
        # producer buffer when the process dies is a document that stops retrying, and
        # throughput is bounded by parsing rather than by publishing anyway.
        self._handle().flush(10)


class KafkaWorkerLoop:
    """Poll, process, commit — in that order, always."""

    def __init__(
        self,
        config: KafkaConfig,
        worker: Worker,
        *,
        consumer: Any | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._config = config
        self._worker = worker
        self._consumer = consumer
        self._now = now
        self._running = False

    def _handle(self) -> Any:
        if self._consumer is None:
            from confluent_kafka import Consumer

            self._consumer = Consumer(self._config.consumer_settings())
            self._consumer.subscribe(list(self._config.topics))
        return self._consumer

    def run(self, *, max_messages: int | None = None) -> int:
        """The loop. `max_messages` exists so a test or a drain job can bound it."""
        consumer = self._handle()
        self._running = True
        handled = 0

        while self._running and (max_messages is None or handled < max_messages):
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                log.warning("kafka error: %s", message.error())
                continue

            if not self._due(message, consumer):
                continue

            try:
                job = Job.from_bytes(message.value())
            except (MalformedEvent, ValueError, TypeError) as exc:
                # Permanent by nature: republishing an event with no s3_key produces the
                # same event. Dead-letter with the reason and commit, rather than blocking
                # the partition on a message that can never succeed.
                log.error(
                    "malformed event",
                    extra={"detail": str(exc), "topic": message.topic()},
                )
                self._dead_letter(message, str(exc))
                consumer.commit(message=message, asynchronous=False)
                handled += 1
                continue

            outcome = self.handle(job)
            # Commit last. Everything in the worker's design assumes this ordering.
            consumer.commit(message=message, asynchronous=False)
            handled += 1
            log.info(
                "processed document",
                extra={
                    "document_id": job.document_id,
                    "disposition": outcome.disposition.value,
                    "run_id": outcome.run_id,
                    "failure_class": outcome.failure_class,
                    "trace_id": job.trace_id,
                },
            )
        return handled

    def handle(self, job: Job) -> Outcome:
        outcome = self._worker.process(job)
        if outcome.disposition is Disposition.RETRY:
            # The worker already published to the next tier; the offset is still
            # committed, because this message is done with — its successor lives on
            # another topic.
            log.info("routed to %s", outcome.detail)
        return outcome

    def _due(self, message: Any, consumer: Any) -> bool:
        """Honour `not_before` by pausing the partition rather than sleeping.

        Sleeping here is what gets the consumer evicted from the group, which is the
        failure this whole file is arranged to avoid.
        """
        headers = dict(message.headers() or [])
        raw = headers.get("not_before")
        if not raw:
            return True
        when = datetime.fromisoformat(
            raw.decode("utf-8") if isinstance(raw, bytes) else raw
        )
        if when <= self._now():
            return True

        from confluent_kafka import TopicPartition

        partition = TopicPartition(message.topic(), message.partition(), message.offset())
        consumer.seek(partition)
        consumer.pause([partition])
        log.debug("paused %s until %s", message.topic(), when.isoformat())
        return False

    def _dead_letter(self, message: Any, reason: str) -> None:
        """Route a message we cannot even parse, preserving the original bytes.

        The payload is kept verbatim rather than re-serialised: whatever is wrong with it is
        exactly what somebody will need to look at.
        """
        publisher = getattr(self._worker, "publisher", None)
        if publisher is None:
            return
        key = message.key()
        publisher.publish(
            Message(
                topic=TOPIC_DLQ,
                key=(key.decode("utf-8", "replace") if key else "unknown"),
                value=message.value(),
                headers={
                    "failure_class": "malformed_event",
                    "failure_reason": reason,
                    "source_topic": message.topic(),
                },
            )
        )

    def stop(self) -> None:
        self._running = False
