"""The HTTP surface, end to end, against real collaborators.

Nothing is mocked: a real worker parses a real file into a real artifact store, and the API
serves it back. The only thing missing is infrastructure, so these tests exercise the actual
request paths a consumer will use.

The API is now content-addressed — `content_hash` plus parser version is the whole address,
because that is exactly what the artifact key is built from. Status, batch status, run
history, reprocess and delete are not here: they moved upstream with the database that
answered them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="the API is an optional extra")
pytest.importorskip("httpx", reason="TestClient needs httpx")

from fastapi.testclient import TestClient

from api.app import Services, create_app
from store.artifacts import LocalArtifactStore
from store.blobs import FetchPolicy, Storage
from work.queue import InMemoryQueue, Job
from work.worker import Worker

SPEC = b"""# Merchant Onboarding

The system must authenticate users within 300ms.

## Security

- Encrypt all traffic with TLS 1.3
- Rotate API keys every 90 days
"""

ABSENT = "0" * 64


@pytest.fixture
def api(tmp_path: Path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "spec.md").write_bytes(SPEC)

    artifacts = LocalArtifactStore(tmp_path / "artifacts")
    queue = InMemoryQueue()
    worker = Worker(
        storage=Storage(FetchPolicy(local_roots=(inbox,))),
        artifacts=artifacts,
        publisher=queue,
    )
    outcome = worker.process(
        Job(
            document_id="doc-1",
            reference=str(inbox / "spec.md"),
            tenant_id="11111111-1111-1111-1111-111111111111",
        )
    )
    assert outcome.ok

    # The hash is what a consumer gets on the completion event, so that is what the tests
    # address the content with.
    event = queue.events_in("documents.parse.completed")[0]

    app = create_app(Services(artifacts=artifacts, parser_version="1.0"))
    return {
        "client": TestClient(app),
        "hash": event.content_hash,
        "artifacts": artifacts,
        "queue": queue,
    }


# --------------------------------------------------------------------------- #
# content
# --------------------------------------------------------------------------- #


def test_content_returns_the_artifact(api) -> None:
    body = api["client"].get(f"/v1/content/{api['hash']}").json()
    assert "authenticate users within 300ms" in body["text"]
    assert body["blocks"]
    assert body["content_hash"] == api["hash"]


def test_text_returns_the_canonical_string_alone(api) -> None:
    response = api["client"].get(f"/v1/content/{api['hash']}/text")
    assert response.headers["content-type"].startswith("text/plain")
    assert "Rotate API keys every 90 days" in response.text


def test_include_can_drop_blocks_for_a_smaller_payload(api) -> None:
    body = api["client"].get(f"/v1/content/{api['hash']}?include=text").json()
    assert "text" in body
    assert "blocks" not in body


def test_unparsed_content_is_404_naming_the_coordinates(api) -> None:
    """Never parsed and parsed-at-another-version are both "not here" to a caller, so the
    body names both coordinates rather than leaving the difference undiagnosable."""
    response = api["client"].get(f"/v1/content/{ABSENT}")
    assert response.status_code == 404
    assert ABSENT in response.json()["detail"]
    assert "1.0" in response.json()["detail"]


def test_an_older_parser_version_can_still_be_read(api) -> None:
    """A consumer pinned mid-rollout must not have its reads start 404ing."""
    ok = api["client"].get(f"/v1/content/{api['hash']}?parser_version=1.0")
    missing = api["client"].get(f"/v1/content/{api['hash']}?parser_version=9.9")
    assert ok.status_code == 200
    assert missing.status_code == 404


# --------------------------------------------------------------------------- #
# etags
# --------------------------------------------------------------------------- #


def test_the_etag_makes_a_repeat_read_free(api) -> None:
    first = api["client"].get(f"/v1/content/{api['hash']}")
    etag = first.headers["etag"]
    assert etag

    again = api["client"].get(
        f"/v1/content/{api['hash']}", headers={"If-None-Match": etag}
    )
    assert again.status_code == 304


def test_the_etag_is_derived_from_the_address_not_the_body(api) -> None:
    """Content-addressed means the ETag can be computed without reading the object, which
    is what makes a 304 genuinely free rather than a read followed by a discard."""
    etag = api["client"].get(f"/v1/content/{api['hash']}").headers["etag"]
    assert api["hash"] in etag
    assert "1.0" in etag

    other = api["client"].get(
        f"/v1/content/{api['hash']}?parser_version=9.9",
        headers={"If-None-Match": etag},
    )
    # A different version is a different address, so the stale ETag must not match it.
    assert other.status_code == 404


# --------------------------------------------------------------------------- #
# locate
# --------------------------------------------------------------------------- #


def test_locate_resolves_quotes_to_spans_and_blocks(api) -> None:
    body = (
        api["client"]
        .post(
            f"/v1/content/{api['hash']}/locate",
            json={"quotes": ["Rotate API keys every 90 days"]},
        )
        .json()
    )
    result = body["results"][0]
    assert result["found"] is True
    assert result["match"] == "exact"
    assert result["span"][0] < result["span"][1]
    assert result["block_id"]


def test_locate_snaps_a_near_miss_to_real_source_text(api) -> None:
    """One imperfect quote must not discard a whole extraction, and nothing absent from the
    document is ever returned."""
    body = (
        api["client"]
        .post(
            f"/v1/content/{api['hash']}/locate",
            json={"quotes": ["Rotate API keys every 90 dayz"]},
        )
        .json()
    )
    result = body["results"][0]
    assert result["match"] == "snapped"
    assert result["text"] == "Rotate API keys every 90 days"
    assert result["similarity"] < 1.0


def test_locate_reports_an_absent_quote_rather_than_inventing_one(api) -> None:
    body = (
        api["client"]
        .post(
            f"/v1/content/{api['hash']}/locate",
            json={"quotes": ["support for SAML single sign-on"]},
        )
        .json()
    )
    assert body["results"][0]["found"] is False
    assert body["results"][0]["span"] is None


def test_locate_requires_at_least_one_quote(api) -> None:
    response = api["client"].post(
        f"/v1/content/{api['hash']}/locate", json={"quotes": []}
    )
    assert response.status_code == 422


def test_locate_on_unparsed_content_is_404(api) -> None:
    response = api["client"].post(
        f"/v1/content/{ABSENT}/locate", json={"quotes": ["anything"]}
    )
    assert response.status_code == 404


# --------------------------------------------------------------------------- #
# the endpoints that moved upstream
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", "/v1/documents/doc-1"),
        ("get", "/v1/documents"),
        ("get", "/v1/documents/doc-1/runs"),
        ("post", "/v1/documents/doc-1/reprocess"),
        ("delete", "/v1/documents/doc-1"),
    ],
)
def test_the_stateful_routes_are_gone(api, method: str, path: str) -> None:
    """Explicit, so a caller still pointing here fails loudly rather than being quietly
    served something plausible. These now live upstream, which owns the documents table."""
    response = getattr(api["client"], method)(path)
    assert response.status_code == 404


def test_no_tenant_header_is_accepted_or_required(api) -> None:
    """A header this service cannot verify would look like an authorisation check while
    being none, which is worse than plainly having no check at all. Tenancy is enforced
    upstream; this app must not be publicly exposed."""
    scoped = api["client"].get(
        f"/v1/content/{api['hash']}",
        headers={"X-Tenant-Id": "99999999-9999-9999-9999-999999999999"},
    )
    unscoped = api["client"].get(f"/v1/content/{api['hash']}")
    assert scoped.status_code == unscoped.status_code == 200


# --------------------------------------------------------------------------- #
# probes
# --------------------------------------------------------------------------- #


def test_health_and_readiness(api) -> None:
    assert api["client"].get("/healthz").json() == {"ok": True}
    assert api["client"].get("/readyz").json() == {"ok": True}
