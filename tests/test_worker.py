"""The worker: at-least-once delivery, and the events that are now the only output.

The whole point of these tests is that Kafka delivers at least once and this service keeps
no state. Every one of them answers a version of "what happens when this message arrives
twice?" — and since there is no longer a database, the answer has to come from the artifact
store and from what gets published.

The rule that replaced the status machine: **a terminal outcome is always announced.** A
document nobody hears about is a document stuck in `pending` upstream forever, so the tests
below assert on published events at least as hard as they assert on return values.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from domain.errors import (
    ServiceError,
    StorageUnavailable,
    UnsupportedFormat,
)
from domain.status import DocumentStatus
from store.artifacts import LocalArtifactStore, artifact_key
from store.blobs import FetchPolicy, Storage
from work.queue import (
    RETRY_TIERS,
    TOPIC_COMPLETED,
    TOPIC_DLQ,
    TOPIC_FAILED,
    InMemoryQueue,
    Job,
)
from work.worker import Disposition, Worker

SPEC = b"""# Merchant Onboarding

The system must authenticate users within 300ms.

## Security

- Encrypt all traffic with TLS 1.3
"""

TENANT = "11111111-1111-1111-1111-111111111111"
PROJECT = "22222222-2222-2222-2222-222222222222"


class FakeClock:
    """Time as a value, so a retry delay is a test rather than a sleep."""

    def __init__(self) -> None:
        self.now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class ExplodingStorage:
    """Storage that fails a set number of times, then succeeds."""

    def __init__(
        self, error: ServiceError, *, times: int = 99, then: Storage | None = None
    ):
        self.error = error
        self.times = times
        self.then = then
        self.calls = 0

    def fetch(self, reference):
        self.calls += 1
        if self.calls <= self.times:
            raise self.error
        return self.then.fetch(reference)


class CountingRegistry:
    """Wraps a registry to count how many times a parse actually happened."""

    def __init__(self, inner) -> None:
        self.inner = inner
        self.parses = 0

    def get(self, *args, **kwargs):
        self.parses += 1
        return self.inner.get(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.inner, name)


@pytest.fixture
def env(tmp_path: Path):
    """A worker wired to real collaborators, none of which need infrastructure."""
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "spec.md").write_bytes(SPEC)

    clock = FakeClock()
    storage = Storage(FetchPolicy(local_roots=(inbox,)))
    artifacts = LocalArtifactStore(tmp_path / "artifacts")
    queue = InMemoryQueue()

    def make(**overrides) -> Worker:
        kwargs = {
            "storage": storage,
            "artifacts": artifacts,
            "publisher": queue,
            "now": clock,
        }
        kwargs.update(overrides)
        return Worker(**kwargs)

    return {
        "make": make,
        "worker": make(),
        "artifacts": artifacts,
        "queue": queue,
        "clock": clock,
        "storage": storage,
        "inbox": inbox,
        # Tenant and project are set because a real upload event always carries them —
        # `Job.from_event` refuses one without a tenant.
        "job": Job(
            document_id="doc-1",
            reference=str(inbox / "spec.md"),
            tenant_id=TENANT,
            project_id=PROJECT,
        ),
    }


# --------------------------------------------------------------------------- #
# the happy path
# --------------------------------------------------------------------------- #


def test_a_document_is_fetched_parsed_and_stored(env) -> None:
    outcome = env["worker"].process(env["job"])

    assert outcome.disposition is Disposition.COMMIT
    assert outcome.ok
    assert outcome.status is DocumentStatus.READY
    document = env["artifacts"].get(outcome.artifact_key)
    assert "authenticate users within 300ms" in document.text


def test_the_artifact_key_is_content_addressed(env) -> None:
    """Same bytes and parser version, same key — which is what makes replay free."""
    outcome = env["worker"].process(env["job"])
    document = env["artifacts"].get(outcome.artifact_key)
    assert outcome.artifact_key == f"parsed/{document.content_hash}/1.0/document.json"


def test_the_linkage_back_to_the_raw_file_survives_in_the_artifact(env) -> None:
    outcome = env["worker"].process(env["job"])
    document = env["artifacts"].get(outcome.artifact_key)
    assert document.source is not None
    assert document.source.key.endswith("spec.md")


# --------------------------------------------------------------------------- #
# the completion event — the only thing upstream hears
# --------------------------------------------------------------------------- #


def test_a_completion_event_is_published_so_consumers_need_not_poll(env) -> None:
    outcome = env["worker"].process(env["job"])
    assert env["queue"].count(TOPIC_COMPLETED) == 1

    event = env["queue"].events_in(TOPIC_COMPLETED)[0]
    assert event.document_id == "doc-1"
    assert event.key == "doc-1"
    # Everything needed to fetch the content, without a status call first.
    assert event.run_id == outcome.run_id
    assert event.artifact_key == outcome.artifact_key
    assert event.status == "ready"
    assert event.duplicate is False
    assert event.completed_at == "2026-08-17T12:00:00+00:00"


def test_the_completion_event_carries_the_tenant_it_was_scoped_to(env) -> None:
    """Without this the event is unactionable, and there is no row to recover it from.

    A consumer reacting to it has to scope its own write by tenant. The event is the only
    place that tenant still exists once this service has committed the offset.
    """
    env["worker"].process(env["job"])
    event = env["queue"].events_in(TOPIC_COMPLETED)[0]
    assert event.tenant_id == TENANT
    assert event.project_id == PROJECT

    message = env["queue"].topics[TOPIC_COMPLETED][0]
    assert message.headers["tenant_id"] == TENANT
    assert message.headers["document_id"] == "doc-1"


def test_the_completion_event_reports_metadata_and_warnings(env) -> None:
    """So a consumer can judge the parse before deciding to fetch 200 KB of text."""
    env["worker"].process(env["job"])
    event = env["queue"].events_in(TOPIC_COMPLETED)[0]
    assert event.metadata["char_count"] > 0
    assert event.metadata["block_count"] > 0
    assert event.metadata["parser_version"] == "1.0"
    assert event.warnings == []


def test_an_unknown_field_on_a_completion_event_does_not_break_a_consumer(env) -> None:
    """Forward compatibility, same as `Job`: we will add fields to this."""
    import json

    from work.queue import CompletionEvent

    env["worker"].process(env["job"])
    raw = json.loads(env["queue"].topics[TOPIC_COMPLETED][0].value)
    raw["a_field_from_a_later_version"] = "ignored"
    event = CompletionEvent.from_bytes(json.dumps(raw).encode("utf-8"))
    assert event.document_id == "doc-1"


def test_success_metrics_include_the_silent_failure_detector(env) -> None:
    """`chars_per_page` is the number that catches a PDF with a broken font map.

    That document parses, reports success, and yields three characters a page. No error
    fires anywhere; only this metric moving would show it.
    """
    outcome = env["worker"].process(env["job"])
    assert "chars_per_page" in outcome.metrics
    assert outcome.metrics["chars"] > 0


# --------------------------------------------------------------------------- #
# idempotency, now answered by the artifact store
# --------------------------------------------------------------------------- #


def test_replaying_the_same_job_does_not_parse_twice(env) -> None:
    registry = CountingRegistry(env["worker"].registry)
    worker = env["make"](registry=registry)

    first = worker.process(env["job"])
    second = worker.process(env["job"])

    assert registry.parses == 1
    assert second.skipped
    assert second.artifact_key == first.artifact_key
    assert second.disposition is Disposition.COMMIT


def test_a_replay_re_announces_rather_than_going_silent(env) -> None:
    """The behaviour the database removal turns from an optimisation into a requirement.

    The old worker answered a replay from the database and published nothing. With events as
    the only output, silence makes a lost message unrecoverable: republishing the request
    would find the artifact present and say nothing at all, leaving the document pending
    forever. So a duplicate re-emits, flagged as one.
    """
    env["worker"].process(env["job"])
    env["worker"].process(env["job"])

    events = env["queue"].events_in(TOPIC_COMPLETED)
    assert len(events) == 2
    assert [e.duplicate for e in events] == [False, True]
    # The re-announcement is complete, not a stub: a consumer that missed the first one
    # must be able to act on the second alone.
    assert events[1].artifact_key == events[0].artifact_key
    assert events[1].content_hash == events[0].content_hash
    assert events[1].tenant_id == TENANT
    assert events[1].metadata == events[0].metadata


def test_force_re_parses_past_the_idempotency_gate(env) -> None:
    """What a reprocess needs after a parser bug is fixed without cutting a version."""
    registry = CountingRegistry(env["worker"].registry)
    worker = env["make"](registry=registry)

    worker.process(env["job"])
    forced = Job(**{**vars(env["job"]), "force": True})
    worker.process(forced)

    assert registry.parses == 2


def test_a_new_parser_version_is_a_different_artifact(env) -> None:
    """What makes a backfill on a new version reparse rather than skip."""
    first = env["worker"].process(env["job"])
    second = env["make"](parser_version="2.0").process(env["job"])

    assert first.artifact_key != second.artifact_key
    assert "/1.0/" in first.artifact_key
    assert "/2.0/" in second.artifact_key
    assert not second.skipped


def test_parse_options_are_part_of_the_artifact_key(env) -> None:
    """Otherwise a differing-options parse is skipped as already-done, or overwrites it.

    The options never used to reach the key — only the database's idempotency row — so the
    second parse below would have been silently skipped once the row went away.
    """
    plain = env["worker"].process(env["job"])
    with_options = env["worker"].process(
        Job(**{**vars(env["job"]), "parse_options": {"extract_tables": False}})
    )

    assert plain.artifact_key != with_options.artifact_key
    assert not with_options.skipped
    # The default-options key keeps its original shape, so artifacts written before options
    # existed are still found.
    assert plain.artifact_key.endswith("/1.0/document.json")


def test_a_half_written_artifact_is_replaced_rather_than_served(env) -> None:
    """`exists` said yes and the read failed. Parse it properly instead of trusting it."""
    outcome = env["worker"].process(env["job"])
    document = env["artifacts"].get(outcome.artifact_key)
    key = artifact_key(document.content_hash, "1.0")
    (Path(env["artifacts"].root) / key).write_bytes(b"{ truncated")

    registry = CountingRegistry(env["worker"].registry)
    again = env["make"](registry=registry).process(env["job"])

    assert registry.parses == 1
    assert not again.skipped
    assert env["artifacts"].get(again.artifact_key).text


# --------------------------------------------------------------------------- #
# transient failures
# --------------------------------------------------------------------------- #


def test_a_transient_failure_walks_the_retry_tiers(env) -> None:
    worker = env["make"](storage=ExplodingStorage(StorageUnavailable("s3 is down")))
    outcome = worker.process(env["job"])

    assert outcome.disposition is Disposition.RETRY
    assert env["queue"].count(RETRY_TIERS[0][0]) == 1
    assert env["queue"].count(TOPIC_DLQ) == 0


def test_an_intermediate_transient_failure_announces_nothing(env) -> None:
    """It is still being worked on. Telling upstream it failed would show a user a failure
    that fixes itself thirty seconds later."""
    worker = env["make"](storage=ExplodingStorage(StorageUnavailable("s3 is down")))
    worker.process(env["job"])

    assert env["queue"].count(TOPIC_FAILED) == 0
    assert env["queue"].count(TOPIC_COMPLETED) == 0


def test_each_retry_moves_to_a_longer_tier(env) -> None:
    worker = env["make"](storage=ExplodingStorage(StorageUnavailable("s3 is down")))
    for attempt, (topic, _delay) in enumerate(RETRY_TIERS, start=1):
        worker.process(Job(**{**vars(env["job"]), "attempt": attempt}))
        assert env["queue"].count(topic) == 1


def test_exhausted_retries_are_announced_as_a_transient_failure(env) -> None:
    worker = env["make"](storage=ExplodingStorage(StorageUnavailable("s3 is down")))
    outcome = worker.process(
        Job(**{**vars(env["job"]), "attempt": len(RETRY_TIERS) + 1})
    )

    assert outcome.disposition is Disposition.DEAD_LETTER
    assert env["queue"].count(TOPIC_DLQ) == 1

    failure = env["queue"].failures_in()[0]
    # `permanent: false` is the distinction that matters: this is our infrastructure
    # having failed repeatedly, not a fact about the document, and it may be worth
    # replaying after a fix.
    assert failure.permanent is False
    assert failure.failure_class == "storage_unavailable"
    assert failure.attempt == len(RETRY_TIERS) + 1


# --------------------------------------------------------------------------- #
# permanent failures
# --------------------------------------------------------------------------- #


def test_a_permanent_failure_skips_every_retry_tier(env) -> None:
    worker = env["make"](storage=ExplodingStorage(UnsupportedFormat("a .dwg file")))
    outcome = worker.process(env["job"])

    assert outcome.disposition is Disposition.DEAD_LETTER
    assert env["queue"].count(TOPIC_DLQ) == 1
    for topic, _ in RETRY_TIERS:
        assert env["queue"].count(topic) == 0


def test_a_permanent_failure_is_announced_with_its_reason(env) -> None:
    """The event that replaced `documents.failure_class`. Without it a permanent failure
    leaves no trace but a dead-letter message nobody consumes."""
    worker = env["make"](storage=ExplodingStorage(UnsupportedFormat("a .dwg file")))
    worker.process(env["job"])

    assert env["queue"].count(TOPIC_FAILED) == 1
    failure = env["queue"].failures_in()[0]
    assert failure.document_id == "doc-1"
    assert failure.status == "failed"
    assert failure.failure_class == "unsupported_format"
    assert "dwg" in failure.failure_reason
    assert failure.permanent is True
    assert failure.tenant_id == TENANT
    assert failure.project_id == PROJECT
    assert failure.failed_at == "2026-08-17T12:00:00+00:00"
    # No bytes were ever fetched, so there is nothing to hash and no run happened.
    assert failure.content_hash is None
    assert failure.run_id is None

    headers = env["queue"].topics[TOPIC_FAILED][0].headers
    assert headers["failure_class"] == "unsupported_format"
    assert headers["permanent"] == "true"
    assert headers["tenant_id"] == TENANT


def test_a_failure_after_fetching_reports_the_hash_it_got_that_far_with(env) -> None:
    """Distinguishes "could not read the file" from "read it and could not parse it"."""

    class BrokenRegistry:
        def get(self, *args, **kwargs):
            raise UnsupportedFormat("no parser for this")

        def media_types(self):
            return ()

    worker = env["make"](registry=BrokenRegistry())
    worker.process(env["job"])

    failure = env["queue"].failures_in()[0]
    assert failure.content_hash is not None
    assert failure.run_id is not None


def test_a_dead_letter_message_carries_everything_needed_to_replay(env) -> None:
    worker = env["make"](storage=ExplodingStorage(UnsupportedFormat("a .dwg file")))
    worker.process(env["job"])

    message = env["queue"].topics[TOPIC_DLQ][0]
    assert message.key == "doc-1"
    assert message.headers["failure_class"] == "unsupported_format"
    assert "dwg" in message.headers["failure_reason"]
    replayed = Job.from_bytes(message.value)
    assert replayed.document_id == "doc-1"
    assert replayed.tenant_id == TENANT


def test_an_unexpected_parser_crash_is_treated_as_transient(env) -> None:
    """An unclassified crash is more likely a bug we will fix than a property of the file."""

    class CrashingRegistry:
        def get(self, *args, **kwargs):
            raise RuntimeError("index out of range")

        def media_types(self):
            return ()

    worker = env["make"](registry=CrashingRegistry())
    outcome = worker.process(env["job"])

    assert outcome.disposition is Disposition.RETRY
    assert outcome.failure_class == "internal"
    assert env["queue"].count(TOPIC_FAILED) == 0


# --------------------------------------------------------------------------- #
# no publisher
# --------------------------------------------------------------------------- #


def test_without_a_publisher_the_work_still_happens(env) -> None:
    """Development-only, and it loses every outcome. Worth having work rather than crash,
    because `--document` exists to test S3 wiring with no broker in reach."""
    worker = env["make"](publisher=None)
    outcome = worker.process(env["job"])

    assert outcome.ok
    assert env["artifacts"].get(outcome.artifact_key).text
