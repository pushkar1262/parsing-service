"""The read API. The only way anything reaches document content.

Design notes that are not obvious from the routes:

**`/content` serves from `current_run_id`, not from `status`.** During a reprocess the
document is still `ready` and still serving the previous artifact, so keying the decision
on the pointer is what makes reprocessing invisible to a consumer.

**`409`, never `200` with empty text.** A document that is not ready yet gets its current
status back with a 409. Returning an empty document would be indistinguishable from a
document that genuinely says nothing, and the planning service would faithfully report
that it contains no requirements.

**ETag is `content_hash:parser_version`.** Parsed content is immutable per run, so every
re-read after the first is a free 304. This matters more than it looks: the planning
service reads the same documents repeatedly across a run.

**Batch status exists because consumers work in document sets.** The planning service's
`extract` runs "one agent per document, in parallel" over a set; making it issue N status
calls to start is a needless N+1 across a service boundary.

End-user authentication is deliberately absent — that is the gateway's job, and this API
accepts no user identity. Service-to-service authentication is still required and belongs
in front of this app (mTLS, or a signed service token in a middleware).
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel, Field

from domain.errors import ObjectNotFound
from domain.locate import Locator
from domain.status import DocumentRecord, DocumentStatus
from store.artifacts import ArtifactStore
from store.repository import DocumentRepository
from work.queue import TOPIC_REQUESTED, Job, Message, Publisher

API_PREFIX = "/v1"


class Services:
    """What the app needs, injected rather than constructed.

    Constructed at startup and overridden wholesale in tests, so the API can be exercised
    against real collaborators with no database, broker or object store running.
    """

    def __init__(
        self,
        *,
        repository: DocumentRepository,
        artifacts: ArtifactStore,
        publisher: Publisher | None = None,
        parser_version: str = "1.0",
        presign: Any | None = None,
    ) -> None:
        self.repository = repository
        self.artifacts = artifacts
        self.publisher = publisher
        self.parser_version = parser_version
        self.presign = presign


def get_services(request: Request) -> Services:
    return request.app.state.services


# The Annotated form rather than `svc: Services = Depends(...)`: it is FastAPI's current
# idiom, and it keeps a callable out of a default argument where it would be evaluated
# once at import and shared.
Svc = Annotated[Services, Depends(get_services)]


# --------------------------------------------------------------------------- #
# wire models
# --------------------------------------------------------------------------- #


class StatusResponse(BaseModel):
    document_id: str
    status: DocumentStatus
    content_hash: str | None = None
    current_run_id: str | None = None
    readable: bool = Field(
        description="Whether /content can serve something right now. Not the same as "
        "status == ready: a document being reprocessed is still serving its previous "
        "artifact."
    )
    media_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    failure_class: str | None = None
    failure_reason: str | None = None
    created_at: Any = None
    updated_at: Any = None


class LocateRequest(BaseModel):
    quotes: list[str] = Field(min_length=1, max_length=200)


class ReprocessRequest(BaseModel):
    parser_version: str | None = None
    parse_options: dict[str, Any] = Field(default_factory=dict)
    force: bool = Field(
        default=False,
        description="Re-run even when a run already exists for the same bytes and "
        "parser version. What you want after fixing a parser bug without cutting a "
        "new version.",
    )


def _status(record: DocumentRecord) -> StatusResponse:
    return StatusResponse(
        document_id=record.id,
        status=record.status,
        content_hash=record.content_hash,
        current_run_id=record.current_run_id,
        readable=record.is_readable,
        media_type=record.media_type,
        metadata=record.metadata,
        failure_class=record.failure_class,
        failure_reason=record.failure_reason,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def create_app(services: Services | None = None) -> FastAPI:
    app = FastAPI(
        title="parsing-service",
        version="0.1.0",
        summary="Structured document content for the planning service.",
    )
    if services is not None:
        app.state.services = services

    # ----------------------------------------------------------------- status

    @app.get(f"{API_PREFIX}/documents/{{document_id}}", response_model=StatusResponse)
    def status(document_id: str, svc: Svc):
        return _status(_require(svc, document_id))

    @app.get(f"{API_PREFIX}/documents", response_model=list[StatusResponse])
    def batch_status(
        svc: Svc,
        ids: str = Query(description="comma-separated document ids"),
    ):
        """One call for a document set, rather than N across a service boundary.

        Unknown ids are simply absent from the response rather than raising: a set where
        one document was deleted should still tell the caller about the rest.
        """
        wanted = [i.strip() for i in ids.split(",") if i.strip()]
        if not wanted:
            raise HTTPException(400, "no document ids given")
        if len(wanted) > 500:
            raise HTTPException(400, "at most 500 ids per request")
        return [_status(r) for r in svc.repository.get_many(wanted)]

    # ---------------------------------------------------------------- content

    @app.get(f"{API_PREFIX}/documents/{{document_id}}/content")
    def content(
        document_id: str,
        response: Response,
        request: Request,
        svc: Svc,
        include: str = Query(
            default="text,blocks,pages",
            description="comma-separated: text, blocks, pages, tables",
        ),
    ):
        record = _require(svc, document_id)
        document = _load(svc, record)
        etag = _etag(record, svc)

        if request.headers.get("if-none-match") == etag:
            # Parsed content is immutable per run, so a repeat read is free.
            return Response(status_code=304, headers={"ETag": etag})

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

    @app.get(f"{API_PREFIX}/documents/{{document_id}}/text", response_class=PlainTextResponse)
    def text(document_id: str, response: Response, svc: Svc):
        """The canonical text alone.

        Exists because it is what the consumer takes today: `ExtractRequest.document` in
        the planning service is a plain `str`. It should be able to adopt this API
        without restructuring, and move to `/content` when it wants sections and tables.
        """
        record = _require(svc, document_id)
        document = _load(svc, record)
        response.headers["ETag"] = _etag(record, svc)
        return document.text

    @app.post(f"{API_PREFIX}/documents/{{document_id}}/locate")
    def locate(
        document_id: str, body: LocateRequest, svc: Svc
    ):
        """Resolve quotes to spans, pages and blocks.

        The half of the planning service's `verbatim_quotes` that belongs on this side:
        finding a string in a document is text mechanics and needs the canonical text, its
        normalisation and its OCR provenance — all of which live here. Deciding what to do
        when a quote is missing stays there, where the model and the repair loop are.
        """
        record = _require(svc, document_id)
        document = _load(svc, record)
        locator = Locator(document)
        return {
            "document_id": document_id,
            "results": [vars(locator.locate(q)) for q in body.quotes],
        }

    @app.get(f"{API_PREFIX}/documents/{{document_id}}/pages/{{number}}/image")
    def page_image(
        document_id: str, number: int, svc: Svc
    ):
        """The vision fallback: `extract` declares `requires: [json_schema, vision]`."""
        record = _require(svc, document_id)
        document = _load(svc, record)
        page = next((p for p in document.pages if p.number == number), None)
        if page is None or not page.image_key:
            raise HTTPException(404, f"no image for page {number}")
        if svc.presign is not None:
            return RedirectResponse(svc.presign(page.image_key), status_code=302)
        return Response(
            content=svc.artifacts.get_bytes(page.image_key), media_type="image/png"
        )

    # ---------------------------------------------------------------- history

    @app.get(f"{API_PREFIX}/documents/{{document_id}}/runs")
    def runs(document_id: str, svc: Svc):
        _require(svc, document_id)
        return [vars(r) | {"status": r.status.value} for r in svc.repository.runs_for(document_id)]

    # ------------------------------------------------------------ write paths

    @app.post(f"{API_PREFIX}/documents/{{document_id}}/reprocess", status_code=202)
    def reprocess(
        document_id: str,
        svc: Svc,
        body: ReprocessRequest | None = None,
    ):
        """Re-run parsing on the existing raw file, without a re-upload.

        Publishes to the same topic first-time parsing uses, so both share one code path.
        The document's status is deliberately untouched: it keeps serving the previous
        artifact until the new run succeeds.
        """
        record = _require(svc, document_id)
        if record.source_uri is None:
            raise HTTPException(409, "this document has no recorded source reference")
        if svc.publisher is None:
            raise HTTPException(503, "no queue is configured on this instance")

        body = body or ReprocessRequest()
        job = Job(
            document_id=document_id,
            reference=record.source_uri,
            media_type=record.media_type,
            parse_options=body.parse_options,
            force=body.force,
        )
        svc.publisher.publish(
            Message(topic=TOPIC_REQUESTED, key=document_id, value=job.to_bytes())
        )
        return {"document_id": document_id, "accepted": True, "status": record.status}

    @app.delete(f"{API_PREFIX}/documents/{{document_id}}", status_code=202)
    def delete(document_id: str, svc: Svc):
        """Tombstone now, clean up asynchronously.

        Returns 202 rather than 204 because the derived data (artifact, page images,
        future embeddings) is removed by a cleanup worker. The document stops being
        readable immediately, which is the part the caller asked for.
        """
        record = svc.repository.get(document_id)
        if record is None:
            raise HTTPException(404, f"no document {document_id}")
        svc.repository.mark_deleted(document_id)
        return {"document_id": document_id, "accepted": True}

    # ----------------------------------------------------------------- health

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/readyz")
    def readyz(svc: Svc):
        """Ready means dependencies answer, not merely that the process is up."""
        try:
            svc.repository.get("__readiness_probe__")
        except Exception as exc:  # noqa: BLE001 - any failure means not ready
            return JSONResponse({"ok": False, "detail": str(exc)}, status_code=503)
        return {"ok": True}

    return app


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _require(svc: Services, document_id: str) -> DocumentRecord:
    record = svc.repository.get(document_id)
    if record is None:
        raise HTTPException(404, f"no document {document_id}")
    if record.status is DocumentStatus.DELETED or record.deleted_at is not None:
        # 410 rather than 404: the difference between "never existed" and "deliberately
        # removed" is worth telling a caller, and it stops a client retrying forever.
        raise HTTPException(410, f"document {document_id} was deleted")
    return record


def _load(svc: Services, record: DocumentRecord):
    if not record.is_readable:
        raise HTTPException(
            409,
            {
                "document_id": record.id,
                "status": record.status.value,
                "detail": "this document has no parsed content yet",
                "failure_class": record.failure_class,
                "failure_reason": record.failure_reason,
            },
        )
    run = svc.repository.get_run(record.current_run_id)
    if run is None or not run.artifact_key:
        raise HTTPException(500, "the current run has no artifact")
    try:
        return svc.artifacts.get(run.artifact_key)
    except ObjectNotFound as exc:
        raise HTTPException(500, f"the artifact for this document is missing: {exc}") from exc


def _etag(record: DocumentRecord, svc: Services) -> str:
    run = svc.repository.get_run(record.current_run_id) if record.current_run_id else None
    version = run.parser_version if run else svc.parser_version
    return f'"{record.content_hash}:{version}"'
