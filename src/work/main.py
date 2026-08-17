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
from pathlib import Path
from typing import Any

from config import ConfigError, Settings, _host_of, _role_of
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

    # Migrate with the privileged role, then serve with the app role. Two connections
    # rather than one because eos_app has no CREATE — and because a service holding DDL
    # privileges it needs once at deploy time is a standing risk for no benefit.
    def connect(url: str, what: str):
        """Turn a connection failure into a sentence, not a traceback.

        A worker that cannot reach its database has a configuration problem, and the useful
        output is which role and which host — not eight frames of psycopg internals with
        the password redacted out of the middle of them.
        """
        try:
            return psycopg.connect(url)
        except psycopg.OperationalError as exc:
            raise ConfigError(
                f"cannot connect to the {what} database as "
                f"{_role_of(url) or 'an unnamed role'} at {_host_of(url)}: "
                f"{str(exc).strip().splitlines()[-1]}"
            ) from exc

    if settings.database_migrate_url:
        migrator = PostgresRepository(connect(settings.database_migrate_url, "migration"))
        try:
            migrator.migrate()
        finally:
            migrator._connection.close()
    repository = PostgresRepository(connect(settings.database_url, "application"))
    if not settings.database_migrate_url:
        # No separate URL: migrate as whoever DATABASE_URL is. Works, and warns, because
        # a role that can run these migrations is usually also a role RLS does not apply to.
        log.warning(
            "no DATABASE_MIGRATE_URL set; running migrations as the application role. "
            "If that role owns the tables, row-level security is not being enforced."
        )
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

    if not check_queue(settings):
        ok = False
    return 0 if ok else 1


def check_queue(settings: Settings) -> bool:
    """Probe the broker properly, because "the string is set" proves nothing.

    Three things go wrong here and only one of them is obvious:

    The **advertised listener** trap. A broker started with
    `advertised.listeners=PLAINTEXT://localhost:9092` answers a metadata request from
    anywhere, then tells the client the broker lives at `localhost` — so bootstrap
    succeeds, topics list fine, and every produce and consume then fails connecting to
    127.0.0.1. It looks like the client is broken. Comparing the bootstrap host against
    what the broker advertises catches it in one line.

    The **missing topic**. Subscribing to a topic nobody publishes to is a worker that
    starts cleanly, logs nothing and processes nothing forever.

    Plain unreachability, which is the case people expect.
    """
    if not settings.kafka_bootstrap_servers:
        print("queue    : not configured (no retries, no dead-letter queue)")
        return True

    try:
        from confluent_kafka.admin import AdminClient
    except ImportError as exc:
        print(f"queue    : FAIL — confluent-kafka is not installed ({exc})", file=sys.stderr)
        return False

    try:
        client = AdminClient(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "socket.timeout.ms": 8000,
            }
        )
        metadata = client.list_topics(timeout=10)
    except Exception as exc:  # noqa: BLE001 - report, do not crash the check
        print(
            f"queue    : FAIL — cannot reach {settings.kafka_bootstrap_servers}: {exc}",
            file=sys.stderr,
        )
        return False

    advertised = sorted({f"{b.host}:{b.port}" for b in metadata.brokers.values()})
    print(f"queue    : reachable at {settings.kafka_bootstrap_servers}")
    print(f"           broker advertises {', '.join(advertised)}")

    ok = True
    bootstrap_hosts = {
        hp.split(":")[0] for hp in settings.kafka_bootstrap_servers.split(",")
    }
    loopback = {"localhost", "127.0.0.1", "::1"}
    if not (bootstrap_hosts & loopback) and any(
        broker.host in loopback for broker in metadata.brokers.values()
    ):
        print(
            "           FAIL — the broker advertises a loopback address, so this client\n"
            "           can read metadata but every produce and consume will connect to\n"
            "           127.0.0.1 and be refused. Fix on the broker:\n"
            "             advertised.listeners=PLAINTEXT://"
            f"{sorted(bootstrap_hosts)[0]}:9092",
            file=sys.stderr,
        )
        ok = False

    topics = sorted(t for t in metadata.topics if not t.startswith("__"))
    wanted = settings.kafka_topic_requested
    if wanted in topics:
        partitions = len(metadata.topics[wanted].partitions)
        print(f"           topic {wanted!r}: {partitions} partition(s)")
        if partitions == 1:
            print(
                "           note — one partition caps the worker pool at one consumer. "
                "Raising it later breaks per-key ordering for in-flight keys."
            )
    else:
        print(
            f"           FAIL — topic {wanted!r} does not exist, so the worker would "
            f"consume nothing.\n"
            f"           Topics on this broker: {', '.join(topics) or '(none)'}\n"
            f"           Set KAFKA_TOPIC_REQUESTED to whichever one the upload backend "
            f"publishes to.",
            file=sys.stderr,
        )
        ok = False
    return ok


def run_one(settings: Settings, job: Job) -> int:
    """Process a single document without a broker. The fastest way to test S3 wiring."""
    worker = build_worker(settings)
    outcome = worker.process(job)
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
    from work.queue import RETRY_TIERS

    config = KafkaConfig(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        topics=(settings.kafka_topic_requested, *(t for t, _ in RETRY_TIERS)),
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


def _job_from_event_arg(value: str) -> Job:
    """Read an upload event from a file or straight from the argument.

    A path first, because a real payload has quotes in it and shell-escaping JSON is how
    people end up debugging their own escaping rather than the service.
    """
    candidate = Path(value)
    raw = candidate.read_bytes() if candidate.is_file() else value.encode("utf-8")
    return Job.from_bytes(raw)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="validate config, then exit")
    parser.add_argument("--once", action="store_true", help="handle one message, then exit")
    parser.add_argument("--document", help="process this document id without a queue")
    parser.add_argument("--reference", help="S3 key, s3:// URI or presigned URL")
    parser.add_argument("--tenant", help="tenant id to scope the run to")
    parser.add_argument("--project", help="project id to record")
    parser.add_argument(
        "--event",
        metavar="JSON|PATH",
        help="an upload event, inline or as a file — the exact payload the backend "
        "publishes, so the mapping can be tested without a broker",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    settings = Settings.from_env()

    try:
        if args.check:
            return check(settings)
        if args.event:
            return run_one(settings, _job_from_event_arg(args.event))
        if args.document:
            if not args.reference:
                parser.error("--document also needs --reference")
            return run_one(
                settings,
                Job(
                    document_id=args.document,
                    reference=args.reference,
                    tenant_id=args.tenant,
                    project_id=args.project,
                ),
            )
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
