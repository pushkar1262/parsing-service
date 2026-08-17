"""The API entrypoint.

    uvicorn api.main:app --app-dir src --port 8000

Built from the same `Settings` the worker uses, so the two cannot disagree about which
bucket holds the artifacts — a mismatch there produces a 500 on every content request with
a perfectly healthy database behind it.

`presign` is wired in when S3 is configured, so `/pages/{n}/image` redirects rather than
streaming megabytes of PNG through this process. The URL is short-lived: a page image is
document content, and a long-lived link to it is an unauthenticated copy of that content.
"""

from __future__ import annotations

import logging

from api.app import Services, create_app
from config import Settings
from work.main import build_publisher, build_repository, configure_logging

log = logging.getLogger("parsing.api")

# How long a page-image URL stays valid. Long enough for a model to fetch it, short enough
# that a leaked link is not a lasting hole.
PRESIGN_TTL_SECONDS = 300


def build_services(settings: Settings | None = None) -> Services:
    settings = settings or Settings.from_env()
    settings.require_s3()
    s3 = settings.s3_client()

    def presign(key: str) -> str:
        return s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": settings.artifact_bucket,
                "Key": f"{settings.artifact_prefix}{key}",
            },
            ExpiresIn=PRESIGN_TTL_SECONDS,
        )

    return Services(
        repository=build_repository(settings),
        artifacts=settings.artifact_store(s3_client=s3),
        publisher=build_publisher(settings),
        parser_version=settings.parser_version,
        presign=presign,
    )


def build_app():
    configure_logging()
    settings = Settings.from_env()
    log.info("api starting", extra=settings.describe())
    return create_app(build_services(settings))


app = build_app()
