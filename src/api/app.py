"""The read API: parsed content, addressed by what it is rather than by which document.

There is no database behind this app. A parse is a pure function of the raw bytes, the
parser version and the parse options, and the artifact key is built from exactly those — so
`content_hash` **is** the address, and no lookup is needed to turn a document into content.
Status, batch status, run history, reprocess and delete all moved upstream with the table
that answered them.

Two things follow from being content-addressed, and the second is a security property, not
a convenience:

**Every response is immutable, so ETags are free and always correct.** The same hash and
parser version can only ever name the same bytes. A repeat read is a 304, and the planning
service reads the same documents repeatedly across a run.

**A `content_hash` is a bearer capability.** Anyone holding one can read that artifact, and
two tenants who upload the same file share one object, so a hash is not scoped to a tenant
and this app cannot check that a caller is entitled to it — there is no row to check against.
Tenant enforcement lives upstream, where the documents table records who owns what.

    Do not expose this app publicly. It must sit on an internal network with upstream
    proxying content reads, or behind a gateway that has already resolved the caller's
    document to this hash. An unguessable hash is not access control: hashes travel in
    events, logs and traces, and none of those are secrets.

`X-Tenant-Id` is deliberately *not* accepted here. Taking a tenant header this app cannot
verify would look like an authorisation check while being none — worse than plainly having
no check, because it invites a caller to rely on it. See `ARCHITECTURE.md` for the
tenant-namespaced-keys alternative, which trades cross-tenant dedup for keys that are
scoped by construction.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import PlainTextResponse, RedirectResponse
from pydantic import BaseModel, Field

from domain.document import ParsedDocument
from domain.errors import ObjectNotFound, ServiceError
from domain.locate import Locator
from store.artifacts import ArtifactStore, artifact_key

API_PREFIX = "/v1"


class Services:
    """What the app needs, injected rather than constructed.

    Constructed at startup and overridden wholesale in tests, so the API can be exercised
    against real collaborators with nothing running.
    """

    def __init__(
        self,
        *,
        artifacts: ArtifactStore,
        parser_version: str = "1.0",
        presign: Any | None = None,
    ) -> None:
        self.artifacts = artifacts
        self.parser_version = parser_version
        self.presign = presign


def get_services(request: Request) -> Services:
    return request.app.state.services


Svc = Annotated[Services, Depends(get_services)]

# The three coordinates of an artifact. `parser_version` defaults to the deployment's
# current version so an ordinary caller passes only the hash; it is a parameter at all
# because a consumer pinned to an older version must be able to keep reading it during a
# rollout.
Version = Annotated[
    str | None,
    Query(description="Parser version. Defaults to this deployment's current version."),
]
Options = Annotated[
    str,
    Query(
        description="Parse-options fingerprint, as published on the completion event. "
        "Empty for the default options, which is almost always the case."
    ),
]


class LocateRequest(BaseModel):
    quotes: list[str] = Field(min_length=1, max_length=200)


def create_app(services: Services | None = None) -> FastAPI:
    app = FastAPI(
        title="parsing-service",
        version="0.2.0",
        summary="Parsed document content, addressed by content hash.",
    )
    if services is not None:
        app.state.services = services

    # ---------------------------------------------------------------- content

    @app.get(f"{API_PREFIX}/content/{{content_hash}}")
    def content(
        content_hash: str,
        response: Response,
        request: Request,
        svc: Svc,
        parser_version: Version = None,
        options: Options = "",
        include: str = Query(
            default="text,blocks,pages",
            description="comma-separated: text, blocks, pages, tables",
        ),
    ):
        version = parser_version or svc.parser_version
        etag = _etag(content_hash, version, options)

        if request.headers.get("if-none-match") == etag:
            # Immutable per hash and version, so a repeat read never re-reads S3.
            return Response(status_code=304, headers={"ETag": etag})

        document = _load(svc, content_hash, version, options)
        wanted = {part.strip() for part in include.split(",") if part.strip()}
        payload = document.model_dump(mode="json")
        if "blocks" not in wanted:
            payload.pop("blocks", None)
        elif "tables" not in wanted:
            for block in payload.get("blocks", []):
                block.pop("table", None)
        if "text" not in wanted:
            payload.pop("text", None)
        if "pages" not in wanted:
            payload.pop("pages", None)

        response.headers["ETag"] = etag
        return payload

    @app.get(f"{API_PREFIX}/content/{{content_hash}}/text", response_class=PlainTextResponse)
    def text(
        content_hash: str,
        response: Response,
        svc: Svc,
        parser_version: Version = None,
        options: Options = "",
    ):
        """The canonical text alone.

        Exists because it is what the consumer takes today: `ExtractRequest.document` in
        the planning service is a plain `str`. It should be able to adopt this API without
        restructuring, and move to `/content` when it wants sections and tables.
        """
        version = parser_version or svc.parser_version
        document = _load(svc, content_hash, version, options)
        response.headers["ETag"] = _etag(content_hash, version, options)
        return document.text

    @app.post(f"{API_PREFIX}/content/{{content_hash}}/locate")
    def locate(
        content_hash: str,
        body: LocateRequest,
        svc: Svc,
        parser_version: Version = None,
        options: Options = "",
    ):
        """Resolve quotes to spans, pages and blocks.

        The half of the planning service's `verbatim_quotes` that belongs on this side:
        finding a string in a document is text mechanics and needs the canonical text, its
        normalisation and its OCR provenance — all of which live here. Deciding what to do
        when a quote is missing stays there, where the model and the repair loop are.
        """
        version = parser_version or svc.parser_version
        document = _load(svc, content_hash, version, options)
        locator = Locator(document)
        return {
            "content_hash": content_hash,
            "document_id": document.document_id,
            "results": [vars(locator.locate(q)) for q in body.quotes],
        }

    @app.get(f"{API_PREFIX}/content/{{content_hash}}/pages/{{number}}/image")
    def page_image(
        content_hash: str,
        number: int,
        svc: Svc,
        parser_version: Version = None,
        options: Options = "",
    ):
        """The vision fallback: `extract` declares `requires: [json_schema, vision]`."""
        version = parser_version or svc.parser_version
        document = _load(svc, content_hash, version, options)
        page = next((p for p in document.pages if p.number == number), None)
        if page is None or not page.image_key:
            raise HTTPException(404, f"no image for page {number}")
        if svc.presign is not None:
            return RedirectResponse(svc.presign(page.image_key), status_code=302)
        return Response(
            content=svc.artifacts.get_bytes(page.image_key), media_type="image/png"
        )

    # ------------------------------------------------------------------ probes

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/readyz")
    def readyz(svc: Svc):
        """Ready means the artifact store answers.

        Deliberately not a read of a known key: there may not be one yet, and a probe that
        fails on an empty bucket takes a healthy deployment out of service on its first day.
        """
        try:
            svc.artifacts.exists("readiness-probe/does-not-exist")
        except ServiceError as exc:
            raise HTTPException(503, f"artifact store unavailable: {exc}") from exc
        return {"ok": True}

    return app


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _load(
    svc: Services, content_hash: str, parser_version: str, options: str
) -> ParsedDocument:
    """Fetch the artifact, or 404.

    A missing object means one of two things and the caller cannot act differently on
    either: never parsed, or parsed at a different version. Both are "not here", and the
    404 body names the coordinates so the difference is diagnosable from the response.
    """
    key = artifact_key(content_hash, parser_version, options)
    try:
        return svc.artifacts.get(key)
    except ObjectNotFound as exc:
        raise HTTPException(
            404,
            f"no parsed content for content_hash={content_hash} "
            f"parser_version={parser_version}",
        ) from exc
    except ServiceError as exc:
        # Storage is unreachable rather than empty. A 502 tells the caller to retry; a 404
        # would have them conclude the document does not exist and stop asking.
        raise HTTPException(502, f"artifact store unavailable: {exc}") from exc


def _etag(content_hash: str, parser_version: str, options: str) -> str:
    """Strong, because these three coordinates can only ever name one set of bytes."""
    suffix = f":{options}" if options else ""
    return f'"{content_hash}:{parser_version}{suffix}"'
