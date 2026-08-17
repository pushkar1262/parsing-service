"""The upload event, and where the tenant goes now that there is no database.

The backend's event schema has to map onto a job exactly: a wrong `s3_key` join or a dropped
`tenant_id` fails later in a way that looks like a storage problem.

Isolation itself is no longer testable here, and that is the point of the change rather than
a gap in it. There is no store to scope, no row-level security to enforce and no tenant
header to check — the tenant arrives on the event, travels through the job, and leaves on the
outcome event, which is the only place it still exists once the offset is committed. What
these tests pin is that it survives that journey; upstream owns enforcing it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from store.artifacts import LocalArtifactStore
from store.blobs import FetchPolicy, Storage
from work.queue import (
    TOPIC_COMPLETED,
    TOPIC_FAILED,
    InMemoryQueue,
    Job,
    MalformedEvent,
)
from work.worker import Worker

TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "22222222-2222-2222-2222-222222222222"
PROJECT = "0c9d0601-d77c-4e09-9e58-2beebefd16eb"

EVENT = {
    "event_id": "e1d2feeb-1d7d-4737-82e0-c115f8ff83db",
    "document_id": "c98f59f8-da78-4de0-8a68-cc2a903bf33a",
    "tenant_id": TENANT_A,
    "project_id": PROJECT,
    "s3_bucket": "eos-s3",
    "s3_key": f"uploads/{TENANT_A}/{PROJECT}/c98f59f8-da78-4de0-8a68-cc2a903bf33a.txt",
    "filename": "sample.txt",
    "content_type": "text/plain",
    "size": 77,
}


# --------------------------------------------------------------------------- #
# the upload event
# --------------------------------------------------------------------------- #


def test_the_upload_event_maps_onto_a_job() -> None:
    job = Job.from_bytes(json.dumps(EVENT).encode())
    assert job.document_id == EVENT["document_id"]
    assert job.reference == f"s3://eos-s3/{EVENT['s3_key']}"
    assert job.tenant_id == TENANT_A
    assert job.project_id == PROJECT
    assert job.media_type == "text/plain"
    assert job.filename == "sample.txt"
    assert job.size == 77
    assert job.event_id == EVENT["event_id"]


def test_the_reference_is_fully_qualified_so_no_prefix_is_guessed() -> None:
    """The producer named the bucket and key; that beats a convention we keep in step."""
    job = Job.from_event(EVENT)
    assert job.reference.startswith("s3://eos-s3/uploads/")
    assert "://" in job.reference


def test_the_event_id_becomes_the_trace_id_when_none_is_given() -> None:
    """So one upload can be followed across two services without matching timestamps."""
    assert Job.from_event(EVENT).trace_id == EVENT["event_id"]


def test_an_explicit_trace_id_wins() -> None:
    job = Job.from_event({**EVENT, "trace_id": "abc123"})
    assert job.trace_id == "abc123"


def test_the_partition_key_is_the_document_id() -> None:
    """What makes concurrent processing of one document structurally impossible."""
    assert Job.from_event(EVENT).key == EVENT["document_id"]


@pytest.mark.parametrize("missing", ["document_id", "s3_bucket", "s3_key", "tenant_id"])
def test_an_event_missing_a_required_field_is_refused_by_name(missing: str) -> None:
    """Refused here, where the message is in hand and can be dead-lettered with a reason.

    A missing tenant_id in particular either leaks across tenants or trips an RLS check
    with a message that says nothing about the cause.
    """
    payload = {k: v for k, v in EVENT.items() if k != missing}
    with pytest.raises(MalformedEvent, match=missing):
        Job.from_event(payload)


def test_a_key_with_a_leading_slash_does_not_double_up() -> None:
    job = Job.from_event({**EVENT, "s3_key": "/uploads/a/b.txt"})
    assert job.reference == "s3://eos-s3/uploads/a/b.txt"


def test_the_filename_falls_back_to_the_key_when_absent() -> None:
    """The extension is the only thing separating Markdown from plain text."""
    job = Job.from_event({**EVENT, "filename": ""})
    assert job.filename == "c98f59f8-da78-4de0-8a68-cc2a903bf33a.txt"


def test_an_unknown_field_does_not_stop_the_consumer() -> None:
    """A producer adding a field must not require a coordinated deploy."""
    job = Job.from_bytes(json.dumps({**EVENT, "checksum": "abc", "version": 3}).encode())
    assert job.tenant_id == TENANT_A


def test_an_internal_retry_message_still_round_trips() -> None:
    """One consumer group reads the upload topic and the retry topics."""
    original = Job.from_event(EVENT)
    assert Job.from_bytes(original.to_bytes()).reference == original.reference


def test_force_and_parse_options_survive_the_event_mapping() -> None:
    """Reprocessing is driven from upstream now, and it is driven by these two fields.

    They used to be readable only from this service's own serialisation, so a reprocess
    published as an upload event arrived looking identical to the original request. With
    dedup keyed on whether the artifact already exists, that is a silent no-op rather than a
    re-parse — the failure mode being that nothing at all appears to happen.
    """
    job = Job.from_event({**EVENT, "force": True, "parse_options": {"ocr": False}})
    assert job.force is True
    assert job.parse_options == {"ocr": False}


def test_the_ordinary_event_is_not_a_forced_one() -> None:
    job = Job.from_event(EVENT)
    assert job.force is False
    assert job.parse_options == {}


def test_a_malformed_parse_options_is_ignored_rather_than_fatal() -> None:
    """A producer bug here should not dead-letter a document that is otherwise fine."""
    job = Job.from_event({**EVENT, "parse_options": "not a mapping"})
    assert job.parse_options == {}


# --------------------------------------------------------------------------- #
# the tenant's journey: event → job → outcome event
# --------------------------------------------------------------------------- #


@pytest.fixture
def env(tmp_path: Path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.md").write_bytes(b"# Tenant A\n\nA must authenticate within 300ms.\n")
    queue = InMemoryQueue()
    worker = Worker(
        storage=Storage(FetchPolicy(local_roots=(inbox,))),
        artifacts=LocalArtifactStore(tmp_path / "artifacts"),
        publisher=queue,
    )
    return worker, queue, inbox


def test_the_tenant_reaches_the_completion_event_unchanged(env) -> None:
    worker, queue, inbox = env
    job = Job.from_event({**EVENT, "s3_key": "ignored"})
    outcome = worker.process(
        Job(**{**vars(job), "reference": str(inbox / "a.md")})
    )
    assert outcome.ok

    event = queue.events_in(TOPIC_COMPLETED)[0]
    assert event.tenant_id == TENANT_A
    assert event.project_id == PROJECT
    assert event.event_id == EVENT["event_id"]


def test_the_tenant_reaches_the_failure_event_too(env) -> None:
    """The case that matters more: a failure has no artifact, so this event is the only
    record that anything happened to this document at all."""
    worker, queue, _inbox = env
    job = Job.from_event(EVENT)
    outcome = worker.process(
        Job(**{**vars(job), "reference": "/nonexistent/nowhere.txt"})
    )
    assert not outcome.ok

    failure = queue.failures_in(TOPIC_FAILED)[0]
    assert failure.tenant_id == TENANT_A
    assert failure.project_id == PROJECT
    assert failure.permanent is True
