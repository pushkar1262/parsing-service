"""The upload event, and tenant isolation.

Two things are being pinned here. First, that the backend's event schema maps onto a job
correctly — a wrong `s3_key` join or a dropped `tenant_id` fails later in a way that looks
like a storage problem. Second, that one tenant cannot read another's documents, tested
through the API rather than asserted about a Postgres policy nobody can exercise without a
database.

The Postgres policies in `002_tenancy.sql` are the real enforcement in production. These
tests cover the layer above them, which is what keeps working if someone ever points
`DATABASE_URL` at the table owner and silently disables every policy.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="the API is an optional extra")
pytest.importorskip("httpx", reason="TestClient needs httpx")

from fastapi.testclient import TestClient

from api.app import Services, create_app
from store.artifacts import LocalArtifactStore
from store.blobs import FetchPolicy, Storage
from store.repository import InMemoryRepository
from work.queue import InMemoryQueue, Job, MalformedEvent
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


# --------------------------------------------------------------------------- #
# isolation
# --------------------------------------------------------------------------- #


@pytest.fixture
def api(tmp_path: Path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.md").write_bytes(b"# Tenant A\n\nA must authenticate within 300ms.\n")
    (inbox / "b.md").write_bytes(b"# Tenant B\n\nB must encrypt everything.\n")

    repository = InMemoryRepository()
    artifacts = LocalArtifactStore(tmp_path / "artifacts")
    worker = Worker(
        repository=repository,
        storage=Storage(FetchPolicy(local_roots=(inbox,))),
        artifacts=artifacts,
        publisher=InMemoryQueue(),
    )
    for doc, tenant, name in (("doc-a", TENANT_A, "a.md"), ("doc-b", TENANT_B, "b.md")):
        outcome = worker.process(
            Job(
                document_id=doc,
                reference=str(inbox / name),
                tenant_id=tenant,
                project_id=PROJECT,
            )
        )
        assert outcome.ok

    app = create_app(Services(repository=repository, artifacts=artifacts))
    return TestClient(app), repository


def test_a_document_reports_its_tenant_and_project(api) -> None:
    client, _ = api
    body = client.get("/v1/documents/doc-a", headers={"X-Tenant-Id": TENANT_A}).json()
    assert body["tenant_id"] == TENANT_A
    assert body["project_id"] == PROJECT


def test_one_tenant_cannot_read_anothers_document(api) -> None:
    """404 rather than 403: telling a caller the document exists is itself a disclosure."""
    client, _ = api
    assert client.get("/v1/documents/doc-b", headers={"X-Tenant-Id": TENANT_A}).status_code == 404
    assert client.get("/v1/documents/doc-a", headers={"X-Tenant-Id": TENANT_B}).status_code == 404


def test_content_is_scoped_too_not_just_status(api) -> None:
    """The status check being scoped is no use if the content endpoint is not."""
    client, _ = api
    assert (
        client.get("/v1/documents/doc-b/content", headers={"X-Tenant-Id": TENANT_A}).status_code
        == 404
    )
    assert (
        client.get("/v1/documents/doc-b/text", headers={"X-Tenant-Id": TENANT_A}).status_code
        == 404
    )


def test_locate_cannot_be_used_to_read_across_tenants(api) -> None:
    """Otherwise quote lookup becomes an oracle for another tenant's content."""
    client, _ = api
    response = client.post(
        "/v1/documents/doc-b/locate",
        json={"quotes": ["must encrypt everything"]},
        headers={"X-Tenant-Id": TENANT_A},
    )
    assert response.status_code == 404


def test_batch_status_filters_rather_than_failing(api) -> None:
    """A set containing another tenant's id returns only what the caller may see."""
    client, _ = api
    body = client.get(
        "/v1/documents?ids=doc-a,doc-b", headers={"X-Tenant-Id": TENANT_A}
    ).json()
    assert [b["document_id"] for b in body] == ["doc-a"]


def test_deleting_across_tenants_is_refused(api) -> None:
    client, repository = api
    assert (
        client.delete("/v1/documents/doc-b", headers={"X-Tenant-Id": TENANT_A}).status_code
        == 404
    )
    assert repository.get("doc-b").deleted_at is None


def test_reprocess_across_tenants_is_refused(api) -> None:
    client, _ = api
    assert (
        client.post(
            "/v1/documents/doc-b/reprocess", headers={"X-Tenant-Id": TENANT_A}
        ).status_code
        == 404
    )


def test_a_reprocess_job_carries_the_documents_own_tenant(api) -> None:
    """Taken from the row, never from the request, so it cannot be relabelled."""
    client, repository = api
    queue = InMemoryQueue()
    app = create_app(
        Services(
            repository=repository,
            artifacts=LocalArtifactStore(Path("/tmp/unused")),
            publisher=queue,
        )
    )
    TestClient(app).post(
        "/v1/documents/doc-a/reprocess", headers={"X-Tenant-Id": TENANT_A}
    )
    from work.queue import TOPIC_REQUESTED

    job = queue.jobs_in(TOPIC_REQUESTED)[0]
    assert job.tenant_id == TENANT_A
    assert job.project_id == PROJECT


def test_without_a_header_reads_are_unscoped_by_default(api) -> None:
    """Which is what keeps a single-tenant deployment working."""
    client, _ = api
    assert client.get("/v1/documents/doc-a").status_code == 200
    assert client.get("/v1/documents/doc-b").status_code == 200


def test_a_deployment_can_require_the_tenant_header(api) -> None:
    """For a genuinely multi-tenant deployment: fail rather than serve broadly."""
    _, repository = api
    app = create_app(
        Services(repository=repository, artifacts=LocalArtifactStore(Path("/tmp/unused"))),
        require_tenant=True,
    )
    client = TestClient(app)
    assert client.get("/v1/documents/doc-a").status_code == 400
    assert (
        client.get("/v1/documents/doc-a", headers={"X-Tenant-Id": TENANT_A}).status_code
        == 200
    )


def test_the_worker_records_tenancy_from_the_event(api) -> None:
    _, repository = api
    record = repository.get("doc-a")
    assert record.tenant_id == TENANT_A
    assert record.project_id == PROJECT
