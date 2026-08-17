"""The worker entrypoint: consume events, fetch from S3, parse, persist.

    python -m work.main                      # consume until stopped
    python -m work.main --once               # one message, then exit
    python -m work.main --check              # validate config and connectivity only
    python -m work.main --document d-1 --reference raw/d-1/spec.pdf   # one document, no queue

`--check` is the mode worth using first. It resolves configuration, constructs every
collaborator and does a `HeadBucket` — so a missing variable, a wrong region or a bad key
surfaces here rather than as a dead-lettered document twenty minutes later.

Logs are JSON. A parse failure is only debuggable if `document_id`, `content_hash`,
`format` and `trace_id` are on the same line as the error, and human-readable log lines
lose that the moment anything aggregates them.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from typing import Any

from config import ConfigError, Settings
from domain.errors import ServiceError
from store.repository import InMemoryRepository
from work.queue import Job
from work.worker import Worker

log = logging.getLogger("parsing.worker")


class JsonFormatter(logging.Formatter):
    """One line, one event, machine-readable.

    The extra fields are the point: a message that says "parse failed" without the
    document id is a message that costs somebody a database query to act on.
    """

    _BUILTIN = frozenset(
        vars(logging.LogRecord("", 0, "", 0, "", None, None)).keys()
    ) | {"message", "asctime", "taskName"}

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in self._BUILTIN and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())


def build_repository(settings: Settings):
    """Postgres when configured, in-memory otherwise.

    The fallback is for local runs, and it says so loudly: an in-memory repository loses
    every document's status when the process exits, which is fine for a demo and a
    catastrophe in production if nobody noticed the warning.
    """
    if not settings.database_url:
        log.warning(
            "no DATABASE_URL set; using an in-memory repository that forgets everything "
            "on exit. Set DATABASE_URL for anything but a local run."
        )
        return InMemoryRepository()

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ConfigError(
            f"DATABASE_URL is set but psycopg is not installed; "
            f"install parsing-service[db] ({exc})"
        ) from exc

    from store.postgres import PostgresRepository

    repository = PostgresRepository(psycopg.connect(settings.database_url))
    repository.migrate()
    return repository


def build_publisher(settings: Settings):
    if not settings.kafka_bootstrap_servers:
        log.warning(
            "no KAFKA_BOOTSTRAP_SERVERS set; retries and dead letters will not be "
            "published, so a transient failure is simply lost"
        )
        return None
    from work.kafka import KafkaConfig, KafkaPublisher

    return KafkaPublisher(
        KafkaConfig(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
        )
    )


def build_worker(settings: Settings, *, repository=None, publisher=None) -> Worker:
    settings.require_s3()
    s3 = settings.s3_client()
    return Worker(
        repository=repository if repository is not None else build_repository(settings),
        storage=settings.storage(s3_client=s3),
        artifacts=settings.artifact_store(s3_client=s3),
        publisher=publisher if publisher is not None else build_publisher(settings),
        registry=settings.registry(),
        parser_version=settings.parser_version,
        # The reason a job can carry a bare key: resolution happens once, here.
        resolve_reference=settings.resolve_reference,
        on_event=lambda event, fields: log.info(event, extra=fields),
    )


def check(settings: Settings) -> int:
    """Validate configuration and reach every dependency, without consuming anything."""
    print(json.dumps(settings.describe(), indent=2, default=str))

    try:
        settings.require_s3()
    except ConfigError as exc:
        print(f"\nconfig: FAIL — {exc}", file=sys.stderr)
        return 2

    ok = True
    try:
        settings.s3_client().head_bucket(Bucket=settings.s3_bucket)
        print(f"\ns3       : ok — s3://{settings.s3_bucket}/{settings.s3_prefix}")
    except Exception as exc:  # noqa: BLE001 - report, do not crash the check
        print(f"\ns3       : FAIL — {exc}", file=sys.stderr)
        ok = False

    backend = settings.ocr_backend()
    print(f"ocr      : {backend.name}" + ("" if backend.available() else " (unavailable)"))

    formats = settings.registry().media_types()
    print(f"parsers  : {len(formats)} media types")

    if settings.database_url:
        try:
            build_repository(settings)
            print("database : ok")
        except Exception as exc:  # noqa: BLE001
            print(f"database : FAIL — {exc}", file=sys.stderr)
            ok = False
    else:
        print("database : not configured (in-memory)")

    print(
        "queue    : "
        + (settings.kafka_bootstrap_servers or "not configured (no retries or DLQ)")
    )
    return 0 if ok else 1


def run_one(settings: Settings, document_id: str, reference: str) -> int:
    """Process a single document without a broker. The fastest way to test S3 wiring."""
    worker = build_worker(settings)
    outcome = worker.process(Job(document_id=document_id, reference=reference))
    print(
        json.dumps(
            {
                "document_id": outcome.document_id,
                "disposition": outcome.disposition.value,
                "status": outcome.status.value if outcome.status else None,
                "run_id": outcome.run_id,
                "artifact_key": outcome.artifact_key,
                "skipped": outcome.skipped,
                "detail": outcome.detail,
                "failure_class": outcome.failure_class,
                "failure_reason": outcome.failure_reason,
                "metrics": outcome.metrics,
            },
            indent=2,
            default=str,
        )
    )
    return 0 if outcome.ok or outcome.skipped else 1


def consume(settings: Settings, *, once: bool = False) -> int:
    if not settings.kafka_bootstrap_servers:
        raise ConfigError(
            "KAFKA_BOOTSTRAP_SERVERS is not set, so there is nothing to consume from. "
            "Use --document/--reference to process one document directly."
        )
    from work.kafka import KafkaConfig, KafkaWorkerLoop

    config = KafkaConfig(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
    )
    loop = KafkaWorkerLoop(config, build_worker(settings))

    # SIGTERM must stop the loop rather than kill it. Mid-document, the pod is going away
    # in 30 seconds either way — but an uncommitted offset means a clean redelivery, and
    # dying between the database write and the commit is the one window worth closing.
    def stop(signum, _frame):
        log.info("stopping", extra={"signal": signum})
        loop.stop()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("worker starting", extra=settings.describe())
    started = time.monotonic()
    handled = loop.run(max_messages=1 if once else None)
    log.info(
        "worker stopped",
        extra={"handled": handled, "seconds": round(time.monotonic() - started, 1)},
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="validate config, then exit")
    parser.add_argument("--once", action="store_true", help="handle one message, then exit")
    parser.add_argument("--document", help="process this document id without a queue")
    parser.add_argument("--reference", help="S3 key, s3:// URI or presigned URL")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    settings = Settings.from_env()

    try:
        if args.check:
            return check(settings)
        if args.document:
            if not args.reference:
                parser.error("--document also needs --reference")
            return run_one(settings, args.document, args.reference)
        return consume(settings, once=args.once)
    except ConfigError as exc:
        log.error("configuration error", extra={"detail": str(exc)})
        return 2
    except ServiceError as exc:
        log.error(
            "failed", extra={"failure_class": exc.failure_class, "detail": str(exc)}
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
