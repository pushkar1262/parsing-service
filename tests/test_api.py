"""The HTTP surface, end to end, against real collaborators.

Nothing is mocked here: a real worker parses a real file into a real artifact store, and
the API serves it. The only thing missing is infrastructure — the repository is in-memory
and the queue is a list — so these tests exercise the actual request paths a consumer
will use.
"""

from __future__ import annotations

from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="the API is an optional extra")
pytest.importorskip("httpx", reason="TestClient needs httpx")

from fastapi.testclient import TestClient

from api.app import Services, create_app
from domain.status import DocumentStatus, ParseRun
from store.artifacts import LocalArtifactStore
from store.blobs import FetchPolicy, Storage
from store.repository import InMemoryRepository
from work.queue import TOPIC_REQUESTED, InMemoryQueue, Job
from work.worker import Worker

SPEC = b"""# Merchant Onboarding

The system must authenticate users within 300ms.

## Security

- Encrypt all traffic with TLS 1.3
- Rotate API keys every 90 days
"""


@pytest.fixture
def api(tmp_path: Path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "spec.md").write_bytes(SPEC)

    repository = InMemoryRepository()
    artifacts = LocalArtifactStore(tmp_path / "artifacts")
    queue = InMemoryQueue()
    worker = Worker(
        repository=repository,
        storage=Storage(FetchPolicy(local_roots=(inbox,))),
        artifacts=artifacts,
        publisher=queue,
    )
    outcome = worker.process(
        Job(document_id="doc-1", reference=str(inbox / "spec.md"))
    )
    assert outcome.ok

    # A second document that failed, so the not-ready paths are real rather than staged.
    repository.register("doc-broken")
    repository.claim("doc-broken")
    run = repository.start_run(
        ParseRun(
            id="run-b",
            document_id="doc-broken",
            content_hash="h",
            parser_version="1.0",
        )
    )
    repository.fail_run(
        run.id,
        failure_class="unsupported_format",
        failure_reason="no parser for application/x-dvi",
        permanent=True,
    )

    app = create_app(
        Services(repository=repository, artifacts=artifacts, publisher=queue)
    )
    return {
        "client": TestClient(app),
        "repository": repository,
        "queue": queue,
        "artifacts": artifacts,
    }


# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #


def test_status_reports_a_ready_document(api) -> None:
    body = api["client"].get("/v1/documents/doc-1").json()
    assert body["status"] == "ready"
    assert body["readable"] is True
    assert body["content_hash"]
    assert body["metadata"]["format"] == "markdown"


def test_status_reports_a_failure_with_its_reason(api) -> None:
    body = api["client"].get("/v1/documents/doc-broken").json()
    assert body["status"] == "failed"
    assert body["readable"] is False
    assert body["failure_class"] == "unsupported_format"
    assert "x-dvi" in body["failure_reason"]


def test_an_unknown_document_is_404(api) -> None:
    assert api["client"].get("/v1/documents/nope").status_code == 404


def test_batch_status_answers_a_document_set_in_one_call(api) -> None:
    """The planning service runs one agent per document over a set."""
    response = api["client"].get("/v1/documents?ids=doc-1,doc-broken")
    assert response.status_code == 200
    assert {b["document_id"] for b in response.json()} == {"doc-1", "doc-broken"}


def test_batch_status_skips_unknown_ids_rather_than_failing(api) -> None:
    """One deleted document in a set should not blind the caller to the rest."""
    body = api["client"].get("/v1/documents?ids=doc-1,ghost").json()
    assert [b["document_id"] for b in body] == ["doc-1"]


def test_batch_status_refuses_an_unbounded_request(api) -> None:
    ids = ",".join(f"d{i}" for i in range(501))
    assert api["client"].get(f"/v1/documents?ids={ids}").status_code == 400


# --------------------------------------------------------------------------- #
# content
# --------------------------------------------------------------------------- #


def test_content_returns_the_artifact(api) -> None:
    body = api["client"].get("/v1/documents/doc-1/content").json()
    assert "authenticate users within 300ms" in body["text"]
    assert body["blocks"]
    assert body["metadata"]["heading_count"] == 2


def test_text_returns_the_canonical_string_alone(api) -> None:
    """What `ExtractRequest.document` takes today, so adoption needs no restructuring."""
    response = api["client"].get("/v1/documents/doc-1/text")
    assert response.status_code == 200
    assert response.text.startswith("# Merchant Onboarding")


def test_include_can_drop_blocks_for_a_smaller_payload(api) -> None:
    body = api["client"].get("/v1/documents/doc-1/content?include=text").json()
    assert "text" in body
    assert "blocks" not in body


def test_content_for_a_failed_document_is_409_not_an_empty_document(api) -> None:
    """An empty 200 is indistinguishable from a document that says nothing.

    The planning service would extract zero requirements and report, correctly from what
    it was given, that the document contains none.
    """
    response = api["client"].get("/v1/documents/doc-broken/content")
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["status"] == "failed"
    assert detail["failure_class"] == "unsupported_format"


def test_the_etag_makes_a_repeat_read_free(api) -> None:
    first = api["client"].get("/v1/documents/doc-1/content")
    etag = first.headers["etag"]
    assert etag

    second = api["client"].get(
        "/v1/documents/doc-1/content", headers={"If-None-Match": etag}
    )
    assert second.status_code == 304


def test_the_etag_changes_when_a_reprocess_lands(api) -> None:
    """It is `content_hash:parser_version`, so new content invalidates it."""
    before = api["client"].get("/v1/documents/doc-1/content").headers["etag"]

    repository = api["repository"]
    run = repository.start_run(
        ParseRun(
            id="run-2",
            document_id="doc-1",
            content_hash="different",
            parser_version="2.0",
        )
    )
    # Point at a real artifact so the read succeeds.
    document = api["artifacts"].get(
        repository.get_run(repository.get("doc-1").current_run_id).artifact_key
    )
    document.content_hash = "different"
    document.metadata.parser_version = "2.0"
    key = api["artifacts"].put(document)
    repository.complete_run(run.id, artifact_key=key, content_hash="different")

    after = api["client"].get("/v1/documents/doc-1/content").headers["etag"]
    assert after != before


# --------------------------------------------------------------------------- #
# locate
# --------------------------------------------------------------------------- #


def test_locate_resolves_quotes_to_spans_and_blocks(api) -> None:
    response = api["client"].post(
        "/v1/documents/doc-1/locate",
        json={"quotes": ["authenticate users within 300ms", "biometric login"]},
    )
    assert response.status_code == 200
    results = response.json()["results"]
    assert results[0]["found"] is True
    assert results[0]["match"] == "exact"
    assert results[0]["block_id"]
    assert results[1]["found"] is False


def test_locate_snaps_a_near_miss_to_real_source_text(api) -> None:
    """One imperfect quote should not discard a whole extraction."""
    response = api["client"].post(
        "/v1/documents/doc-1/locate",
        json={"quotes": ["Rotate API keys every 90 dayz"]},
    )
    result = response.json()["results"][0]
    assert result["match"] == "snapped"
    assert result["text"] == "Rotate API keys every 90 days"


def test_locate_requires_at_least_one_quote(api) -> None:
    assert api["client"].post("/v1/documents/doc-1/locate", json={"quotes": []}).status_code == 422


# --------------------------------------------------------------------------- #
# reprocess, delete, history
# --------------------------------------------------------------------------- #


def test_reprocess_publishes_a_job_and_leaves_the_document_ready(api) -> None:
    """The rule the run/document split exists for, at the API level."""
    response = api["client"].post("/v1/documents/doc-1/reprocess")
    assert response.status_code == 202

    assert api["queue"].count(TOPIC_REQUESTED) == 1
    assert api["queue"].jobs_in(TOPIC_REQUESTED)[0].document_id == "doc-1"
    # Still ready, still serving.
    assert api["repository"].get("doc-1").status is DocumentStatus.READY
    assert api["client"].get("/v1/documents/doc-1/content").status_code == 200


def test_reprocess_can_force_past_the_idempotency_gate(api) -> None:
    """What you want after fixing a parser bug without cutting a version."""
    api["client"].post("/v1/documents/doc-1/reprocess", json={"force": True})
    assert api["queue"].jobs_in(TOPIC_REQUESTED)[0].force is True


def test_runs_history_is_exposed(api) -> None:
    body = api["client"].get("/v1/documents/doc-1/runs").json()
    assert len(body) == 1
    assert body[0]["status"] == "succeeded"
    assert body[0]["artifact_key"]


def test_delete_makes_the_document_gone_immediately(api) -> None:
    assert api["client"].delete("/v1/documents/doc-1").status_code == 202
    # 410, not 404: "deliberately removed" and "never existed" are different answers,
    # and only one of them should stop a client retrying.
    assert api["client"].get("/v1/documents/doc-1").status_code == 410
    assert api["client"].get("/v1/documents/doc-1/content").status_code == 410


def test_deleting_an_unknown_document_is_404(api) -> None:
    assert api["client"].delete("/v1/documents/ghost").status_code == 404


# --------------------------------------------------------------------------- #
# health
# --------------------------------------------------------------------------- #


def test_health_and_readiness(api) -> None:
    assert api["client"].get("/healthz").json() == {"ok": True}
    assert api["client"].get("/readyz").json() == {"ok": True}
