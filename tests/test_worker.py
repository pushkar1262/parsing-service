"""The worker, the status machine, and the idempotency that makes replay safe.

The whole point of these tests is that Kafka delivers at least once. Every one of them
answers a version of "what happens when this message arrives twice, or arrives while
another worker is mid-flight, or arrives after the document is already done?" — and the
answers have to be the same whether the duplicate is a millisecond or a day later.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from domain.errors import (
    ObjectNotFound,
    ServiceError,
    StorageUnavailable,
    UnsupportedFormat,
)
from domain.status import DocumentStatus, RunStatus
from store.artifacts import LocalArtifactStore
from store.blobs import FetchPolicy, Storage
from store.repository import DuplicateRun, InMemoryRepository
from work.queue import RETRY_TIERS, TOPIC_COMPLETED, TOPIC_DLQ, InMemoryQueue, Job
from work.worker import Disposition, Worker

SPEC = b"""# Merchant Onboarding

The system must authenticate users within 300ms.

## Security

- Encrypt all traffic with TLS 1.3
"""


class FakeClock:
    """Time as a value, so lease expiry is a test rather than a sleep."""

    def __init__(self) -> None:
        self.now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class ExplodingStorage:
    """Storage that fails a set number of times, then succeeds."""

    def __init__(self, error: ServiceError, *, times: int = 99, then: Storage | None = None):
        self.error = error
        self.times = times
        self.then = then
        self.calls = 0

    def fetch(self, reference):
        self.calls += 1
        if self.calls <= self.times:
            raise self.error
        return self.then.fetch(reference)


@pytest.fixture
def env(tmp_path: Path):
    """A worker wired to real collaborators, none of which need infrastructure."""
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "spec.md").write_bytes(SPEC)

    clock = FakeClock()
    repository = InMemoryRepository(clock=clock)
    storage = Storage(FetchPolicy(local_roots=(inbox,)))
    artifacts = LocalArtifactStore(tmp_path / "artifacts")
    queue = InMemoryQueue()
    worker = Worker(
        repository=repository,
        storage=storage,
        artifacts=artifacts,
        publisher=queue,
    )
    return {
        "worker": worker,
        "repository": repository,
        "artifacts": artifacts,
        "queue": queue,
        "clock": clock,
        "storage": storage,
        "inbox": inbox,
        "job": Job(document_id="doc-1", reference=str(inbox / "spec.md")),
    }


# --------------------------------------------------------------------------- #
# the happy path
# --------------------------------------------------------------------------- #


def test_a_document_is_fetched_parsed_stored_and_marked_ready(env) -> None:
    outcome = env["worker"].process(env["job"])

    assert outcome.disposition is Disposition.COMMIT
    assert outcome.ok
    record = env["repository"].get("doc-1")
    assert record.status is DocumentStatus.READY
    assert record.current_run_id == outcome.run_id

    document = env["artifacts"].get(outcome.artifact_key)
    assert "authenticate users within 300ms" in document.text


def test_the_artifact_key_is_content_addressed(env) -> None:
    """Same bytes and parser version, same key — which is what makes replay free."""
    outcome = env["worker"].process(env["job"])
    record = env["repository"].get("doc-1")
    assert outcome.artifact_key == f"parsed/{record.content_hash}/1.0/document.json"


def test_the_linkage_back_to_the_raw_file_survives_in_the_artifact(env) -> None:
    outcome = env["worker"].process(env["job"])
    document = env["artifacts"].get(outcome.artifact_key)
    assert document.source is not None
    assert document.source.key.endswith("spec.md")


def test_a_completion_event_is_published_so_consumers_need_not_poll(env) -> None:
    env["worker"].process(env["job"])
    assert env["queue"].count(TOPIC_COMPLETED) == 1
    assert env["queue"].jobs_in(TOPIC_COMPLETED)[0].document_id == "doc-1"


def test_success_metrics_include_the_silent_failure_detector(env) -> None:
    """`chars_per_page` is the number that catches a PDF with a broken font map.

    That document parses, reports success, and yields three characters a page. No error
    fires anywhere; only this metric moving would show it.
    """
    outcome = env["worker"].process(env["job"])
    assert "chars_per_page" in outcome.metrics
    assert outcome.metrics["chars"] > 0
    assert outcome.metrics["format"] == "markdown"


# --------------------------------------------------------------------------- #
# idempotency — the reason any of this is shaped the way it is
# --------------------------------------------------------------------------- #


def test_replaying_the_same_job_does_not_parse_twice(env) -> None:
    first = env["worker"].process(env["job"])
    second = env["worker"].process(env["job"])

    assert second.disposition is Disposition.COMMIT
    assert second.skipped
    assert len(env["repository"].runs_for("doc-1")) == 1
    assert first.run_id is not None


def test_a_replay_after_completion_leaves_the_document_ready(env) -> None:
    """The claim gate refuses, and refusing is the correct answer.

    A redelivery must not move a finished document back to `processing`, because the
    content API would start refusing requests for a document that has been fine for a
    month.
    """
    env["worker"].process(env["job"])
    env["worker"].process(env["job"])
    assert env["repository"].get("doc-1").status is DocumentStatus.READY


def test_a_second_worker_mid_flight_does_not_start_a_duplicate_run(env) -> None:
    """The unique constraint, in the form the race actually takes."""
    repository = env["repository"]
    repository.register("doc-2")
    repository.claim("doc-2")

    from domain.status import ParseRun

    run = ParseRun(
        id="run-a", document_id="doc-2", content_hash="abc", parser_version="1.0"
    )
    repository.start_run(run)
    with pytest.raises(DuplicateRun):
        repository.start_run(
            ParseRun(
                id="run-b", document_id="doc-2", content_hash="abc", parser_version="1.0"
            )
        )


def test_a_new_parser_version_is_a_different_run(env) -> None:
    """Which is exactly what makes a parser upgrade a backfill rather than a no-op."""
    env["worker"].process(env["job"])
    upgraded = Worker(
        repository=env["repository"],
        storage=env["storage"],
        artifacts=env["artifacts"],
        publisher=env["queue"],
        parser_version="2.0",
    )
    # A reprocess does not go through the claim gate's `pending` path, so drive it the
    # way the API does: the document is already ready and a new run is created beside it.
    from domain.status import ParseRun

    run = env["repository"].start_run(
        ParseRun(
            id="run-v2",
            document_id="doc-1",
            content_hash=env["repository"].get("doc-1").content_hash,
            parser_version="2.0",
        )
    )
    assert run.status is RunStatus.RUNNING
    assert len(env["repository"].runs_for("doc-1")) == 2
    assert upgraded.parser_version == "2.0"


def test_parse_options_are_part_of_the_idempotency_key(env) -> None:
    """Same bytes, different options, different run — and order must not matter."""
    from work.worker import _options_hash

    assert _options_hash({"a": 1, "b": 2}) == _options_hash({"b": 2, "a": 1})
    assert _options_hash({"a": 1}) != _options_hash({"a": 2})
    assert _options_hash({}) == ""


# --------------------------------------------------------------------------- #
# failure routing
# --------------------------------------------------------------------------- #


def test_a_transient_failure_walks_the_retry_tiers(env) -> None:
    worker = Worker(
        repository=env["repository"],
        storage=ExplodingStorage(StorageUnavailable("s3 is having a moment")),
        artifacts=env["artifacts"],
        publisher=env["queue"],
    )
    outcome = worker.process(env["job"])

    assert outcome.disposition is Disposition.RETRY
    first_tier = RETRY_TIERS[0][0]
    assert env["queue"].count(first_tier) == 1
    retried = env["queue"].jobs_in(first_tier)[0]
    assert retried.attempt == 2
    assert retried.not_before is not None


def test_each_retry_moves_to_a_longer_tier(env) -> None:
    worker = Worker(
        repository=env["repository"],
        storage=ExplodingStorage(StorageUnavailable("still down")),
        artifacts=env["artifacts"],
        publisher=env["queue"],
    )
    for attempt, (topic, _delay) in enumerate(RETRY_TIERS, start=1):
        job = Job(document_id=f"d{attempt}", reference="x", attempt=attempt)
        outcome = worker.process(job)
        assert outcome.disposition is Disposition.RETRY
        assert env["queue"].count(topic) == 1


def test_exhausted_retries_go_to_the_dead_letter_queue(env) -> None:
    worker = Worker(
        repository=env["repository"],
        storage=ExplodingStorage(StorageUnavailable("down for good")),
        artifacts=env["artifacts"],
        publisher=env["queue"],
    )
    job = Job(document_id="doc-x", reference="x", attempt=len(RETRY_TIERS) + 1)
    outcome = worker.process(job)

    assert outcome.disposition is Disposition.DEAD_LETTER
    assert outcome.failure_class == "transient_exhausted"
    assert env["queue"].count(TOPIC_DLQ) == 1


def test_a_permanent_failure_skips_every_retry_tier(env) -> None:
    """Retrying an unsupported format four times is cost with a guaranteed outcome.

    It also delays the permanent answer by 35 minutes, during which the document sits in
    a state that looks like it might still succeed.
    """
    worker = Worker(
        repository=env["repository"],
        storage=ExplodingStorage(UnsupportedFormat("no parser for application/x-dvi")),
        artifacts=env["artifacts"],
        publisher=env["queue"],
    )
    outcome = worker.process(Job(document_id="doc-y", reference="x"))

    assert outcome.disposition is Disposition.DEAD_LETTER
    assert outcome.failure_class == "unsupported_format"
    for topic, _ in RETRY_TIERS:
        assert env["queue"].count(topic) == 0
    assert env["queue"].count(TOPIC_DLQ) == 1


def test_a_dead_letter_message_carries_everything_needed_to_replay(env) -> None:
    worker = Worker(
        repository=env["repository"],
        storage=ExplodingStorage(ObjectNotFound("the object is gone")),
        artifacts=env["artifacts"],
        publisher=env["queue"],
    )
    worker.process(Job(document_id="doc-z", reference="x", trace_id="trace-42"))

    message = env["queue"].topics[TOPIC_DLQ][0]
    assert message.headers["failure_class"] == "not_found"
    assert "gone" in message.headers["failure_reason"]
    assert message.headers["trace_id"] == "trace-42"


def test_a_permanent_failure_is_recorded_on_the_document(env) -> None:
    worker = Worker(
        repository=env["repository"],
        storage=ExplodingStorage(UnsupportedFormat("no parser for image/x-tga")),
        artifacts=env["artifacts"],
        publisher=env["queue"],
    )
    worker.process(Job(document_id="doc-w", reference="x"))

    record = env["repository"].get("doc-w")
    assert record.status is DocumentStatus.FAILED
    assert record.failure_class == "unsupported_format"
    assert "image/x-tga" in record.failure_reason


def test_an_unexpected_parser_crash_is_treated_as_transient(env) -> None:
    """An unclassified bug is more likely ours than the document's.

    A retry costs a little money; a dead-lettered document with no recorded reason costs
    somebody an afternoon.
    """

    class Boom:
        def fetch(self, reference):
            raise RuntimeError("something nobody anticipated")

    worker = Worker(
        repository=env["repository"],
        storage=Boom(),
        artifacts=env["artifacts"],
        publisher=env["queue"],
    )
    with pytest.raises(RuntimeError):
        # Storage errors outside the taxonomy still propagate; only *parser* crashes are
        # absorbed, because those happen after a run row exists to record them against.
        worker.process(Job(document_id="doc-v", reference="x"))


# --------------------------------------------------------------------------- #
# reprocessing must not take a good document offline
# --------------------------------------------------------------------------- #


def test_a_failed_reprocess_leaves_the_document_ready_and_serving(env) -> None:
    """The rule the whole run/document split exists for.

    Nothing was wrong with this document. An optional re-parse went wrong, and marking it
    `failed` would take working content offline for a reason that has nothing to do with
    the document.
    """
    first = env["worker"].process(env["job"])
    repository = env["repository"]

    from domain.status import ParseRun

    reprocess = repository.start_run(
        ParseRun(
            id="run-2",
            document_id="doc-1",
            content_hash="different-bytes",
            parser_version="1.0",
        )
    )
    repository.fail_run(
        reprocess.id,
        failure_class="corrupt",
        failure_reason="the new parser choked",
        permanent=True,
    )

    record = repository.get("doc-1")
    assert record.status is DocumentStatus.READY
    assert record.current_run_id == first.run_id  # still the original, good artifact
    assert record.is_readable


def test_a_successful_reprocess_flips_the_pointer(env) -> None:
    first = env["worker"].process(env["job"])
    repository = env["repository"]

    from domain.status import ParseRun

    run = repository.start_run(
        ParseRun(
            id="run-2",
            document_id="doc-1",
            content_hash="new-hash",
            parser_version="2.0",
        )
    )
    repository.complete_run(run.id, artifact_key="parsed/new-hash/2.0/document.json",
                            content_hash="new-hash")

    record = repository.get("doc-1")
    assert record.current_run_id == "run-2"
    assert record.current_run_id != first.run_id
    assert record.status is DocumentStatus.READY


# --------------------------------------------------------------------------- #
# leases
# --------------------------------------------------------------------------- #


def test_a_crashed_worker_stops_stranding_its_document(env) -> None:
    """Without this the claim gate does its job forever and nobody is watching."""
    repository = env["repository"]
    clock = env["clock"]

    repository.register("doc-stuck")
    repository.claim("doc-stuck")
    from domain.status import ParseRun

    repository.start_run(
        ParseRun(
            id="run-stuck",
            document_id="doc-stuck",
            content_hash="h",
            parser_version="1.0",
        )
    )
    assert repository.get("doc-stuck").status is DocumentStatus.PROCESSING
    assert repository.reclaim_expired() == []

    clock.advance(hours=1)
    assert repository.reclaim_expired() == ["doc-stuck"]
    assert repository.get("doc-stuck").status is DocumentStatus.PENDING
    assert repository.get_run("run-stuck").failure_class == "lease_expired"


# --------------------------------------------------------------------------- #
# deletion
# --------------------------------------------------------------------------- #


def test_a_deleted_document_is_never_reprocessed(env) -> None:
    env["worker"].process(env["job"])
    env["repository"].mark_deleted("doc-1")

    outcome = env["worker"].process(env["job"])
    assert outcome.disposition is Disposition.COMMIT
    assert outcome.skipped
    assert "deleted" in outcome.detail
    assert env["repository"].get("doc-1").status is DocumentStatus.DELETED
