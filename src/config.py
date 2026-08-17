"""Configuration from the environment, and the collaborators it builds.

One place where the service learns about the world, so nothing below it reads `os.environ`
and every test can construct its dependencies directly. `Settings.from_env()` is called by
the two entrypoints and by nothing else.

Two rules this module exists to enforce.

**Secrets never leave here as strings anyone can print.** `AWS_SECRET_ACCESS_KEY` is read,
handed to the boto3 client, and otherwise unreachable: `describe()` is what logging and the
`/readyz` payload use, and it masks. A credential in a log line is a credential in a log
aggregator, in a backup of that aggregator, and in whatever has read access to either.

**A bare reference is resolved to the configured bucket, not to the filesystem.**
`parse_ref` treats a string with no scheme as a local path, which is right for the CLI and
the tests and wrong for a queue message. So a job carrying `raw/doc-42/spec.pdf` becomes
`s3://{bucket}/{prefix}raw/doc-42/spec.pdf` here, at the boundary, rather than being
guessed at further in.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Read `.env` from the service directory, then the workspace root — the same order the
# sibling planning-service uses, so a single shared file works for both.
_ENV_CANDIDATES = (Path(__file__).resolve().parents[1] / ".env", Path(__file__).resolve().parents[2] / ".env")

_TRUE = frozenset({"1", "true", "yes", "on"})


def load_env(*, override: bool = False) -> list[Path]:
    """Load `.env` files if python-dotenv is available. Returns what was loaded.

    Optional rather than required: in a container the variables are already in the
    environment, and a missing `.env` there is normal rather than an error.
    """
    loaded: list[Path] = []
    try:
        from dotenv import load_dotenv
    except ImportError:  # pragma: no cover - environment dependent
        return loaded
    for path in _ENV_CANDIDATES:
        if path.is_file():
            load_dotenv(path, override=override)
            loaded.append(path)
    return loaded


def _flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().lower() in _TRUE


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _csv(name: str) -> tuple[str, ...]:
    raw = os.environ.get(name, "")
    return tuple(part.strip() for part in raw.split(",") if part.strip())


class ConfigError(Exception):
    """Refused at startup rather than at the first document.

    A worker that boots with no bucket configured and fails on message one has turned a
    typo into an incident; failing to start is louder and cheaper.
    """


@dataclass(frozen=True)
class Settings:
    # ---- blob storage
    s3_bucket: str = ""
    s3_prefix: str = ""
    artifact_bucket: str = ""
    artifact_prefix: str = ""
    aws_region: str = ""
    aws_access_key_id: str = ""
    aws_secret_access_key: str = field(default="", repr=False)
    aws_endpoint_url: str = ""

    # ---- fetch policy
    max_fetch_bytes: int = 100 * 1024 * 1024
    allow_private_networks: bool = False
    allowed_host_suffixes: tuple[str, ...] = ()
    local_roots: tuple[Path, ...] = ()

    # ---- infrastructure
    database_url: str = ""
    # Migrations need CREATE, which the app role deliberately lacks. Kept separate so the
    # service never runs holding privileges it only needs at deploy time — and so that
    # pointing the app at the table owner, which silently disables every RLS policy, has to
    # be a deliberate act rather than a convenient one.
    database_migrate_url: str = ""
    kafka_bootstrap_servers: str = ""
    kafka_group_id: str = "parsing-service"
    kafka_topic_requested: str = "documents.parse.requested"
    require_tenant_header: bool = False

    # ---- parsing
    parser_version: str = "1.0"
    extract_tables: bool = True
    ocr_enabled: bool = True
    ocr_dpi: int = 200
    ocr_language: str = "eng"

    @classmethod
    def from_env(cls, *, load_dotenv_files: bool = True) -> Settings:
        if load_dotenv_files:
            load_env()
        bucket = os.environ.get("S3_BUCKET", "").strip()
        return cls(
            s3_bucket=bucket,
            # Normalised to end with `/` if present, so joining a key never produces
            # `prefixraw/spec.pdf` or `prefix//raw/spec.pdf`.
            s3_prefix=_normalise_prefix(os.environ.get("S3_PREFIX", "")),
            # Artifacts default to the same bucket. Separating them is worth doing when
            # you want different lifecycle rules — parsed content is cheap to regenerate
            # and can expire aggressively, raw uploads cannot.
            artifact_bucket=os.environ.get("S3_ARTIFACT_BUCKET", "").strip() or bucket,
            artifact_prefix=_normalise_prefix(
                os.environ.get("S3_ARTIFACT_PREFIX", "")
            ),
            aws_region=os.environ.get("AWS_REGION", "").strip(),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "").strip(),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip(),
            aws_endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "").strip(),
            max_fetch_bytes=_int("MAX_FETCH_BYTES", 100 * 1024 * 1024),
            allow_private_networks=_flag("ALLOW_PRIVATE_NETWORKS", False),
            allowed_host_suffixes=_csv("ALLOWED_HOST_SUFFIXES"),
            local_roots=tuple(Path(p) for p in _csv("LOCAL_ROOTS")),
            database_url=os.environ.get("DATABASE_URL", "").strip(),
            database_migrate_url=os.environ.get("DATABASE_MIGRATE_URL", "").strip(),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip(),
            kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "parsing-service").strip(),
            kafka_topic_requested=os.environ.get(
                "KAFKA_TOPIC_REQUESTED", "documents.parse.requested"
            ).strip(),
            require_tenant_header=_flag("REQUIRE_TENANT_HEADER", False),
            parser_version=os.environ.get("PARSER_VERSION", "1.0").strip(),
            extract_tables=_flag("EXTRACT_TABLES", True),
            ocr_enabled=_flag("OCR_ENABLED", True),
            ocr_dpi=_int("OCR_DPI", 200),
            ocr_language=os.environ.get("OCR_LANGUAGE", "eng").strip() or "eng",
        )

    # ------------------------------------------------------------ validation

    def require_s3(self) -> None:
        if not self.s3_bucket:
            raise ConfigError("S3_BUCKET is not set, so no document can be fetched")
        if not self.aws_region:
            raise ConfigError("AWS_REGION is not set")

    @property
    def has_static_credentials(self) -> bool:
        return bool(self.aws_access_key_id and self.aws_secret_access_key)

    # ---------------------------------------------------------- construction

    def s3_client(self) -> Any:
        """A boto3 S3 client.

        Credentials are passed explicitly when present rather than left to boto3's
        implicit chain. The chain is convenient and, when it silently picks up a different
        profile than the one in `.env`, it is the reason someone spends an afternoon on a
        403. Absent static credentials we fall through to the chain deliberately, which is
        what an IAM role on a task or node provides — and is the better production setup.
        """
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ConfigError(
                f"boto3 is not installed; install parsing-service[s3] ({exc})"
            ) from exc

        kwargs: dict[str, Any] = {}
        if self.aws_region:
            kwargs["region_name"] = self.aws_region
        if self.aws_endpoint_url:
            kwargs["endpoint_url"] = self.aws_endpoint_url
        if self.has_static_credentials:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        return boto3.client("s3", **kwargs)

    def fetch_policy(self):
        from store.blobs import FetchPolicy
        from store.net import NetPolicy

        return FetchPolicy(
            max_bytes=self.max_fetch_bytes,
            net=NetPolicy(
                allow_private_networks=self.allow_private_networks,
                allowed_host_suffixes=self.allowed_host_suffixes,
            ),
            local_roots=self.local_roots,
        )

    def storage(self, *, s3_client: Any | None = None):
        from store.blobs import Storage

        return Storage(
            self.fetch_policy(), s3_client=s3_client or (self.s3_client() if self.s3_bucket else None)
        )

    def artifact_store(self, *, s3_client: Any | None = None):
        from store.artifacts import S3ArtifactStore

        return S3ArtifactStore(
            self.artifact_bucket,
            client=s3_client or self.s3_client(),
            prefix=self.artifact_prefix,
        )

    def ocr_backend(self):
        """The configured OCR backend, or the null one.

        Returns `NullOcr` rather than raising when Tesseract is missing: a scanned page
        then gets flagged and warned about instead of taking the worker down, which is the
        right failure for a quality problem.
        """
        from parse.ocr.base import NullOcr

        if not self.ocr_enabled:
            return NullOcr()
        try:
            from parse.ocr.tesseract import TesseractOcr
        except ImportError:  # pragma: no cover - environment dependent
            return NullOcr()
        backend = TesseractOcr(language=self.ocr_language)
        return backend if backend.available() else NullOcr()

    def registry(self):
        from parse.registry import default_registry

        return default_registry(ocr=self.ocr_backend())

    # ------------------------------------------------------------ references

    def resolve_reference(self, reference: str) -> str:
        """Turn whatever a job carries into something `parse_ref` reads correctly.

        A scheme means the sender was explicit and is left alone — that covers
        `s3://bucket/key` and a presigned `https://` URL. A bare key is resolved against
        the configured bucket and prefix, because `parse_ref` would otherwise read it as a
        filesystem path and the worker would go looking on local disk for a document that
        lives in S3.
        """
        candidate = reference.strip()
        if not candidate:
            raise ConfigError("the job carries an empty reference")
        if "://" in candidate:
            return candidate
        if not self.s3_bucket:
            # No bucket configured: leave it alone so a local-development reference still
            # works rather than becoming a confusing `s3:///key`.
            return candidate
        key = candidate.lstrip("/")
        if self.s3_prefix and not key.startswith(self.s3_prefix):
            key = f"{self.s3_prefix}{key}"
        return f"s3://{self.s3_bucket}/{key}"

    # -------------------------------------------------------------- logging

    def describe(self) -> dict[str, Any]:
        """Safe to log. Credentials are reported as present or absent, never echoed."""
        return {
            "s3_bucket": self.s3_bucket or None,
            "s3_prefix": self.s3_prefix or None,
            "artifact_bucket": self.artifact_bucket or None,
            "artifact_prefix": self.artifact_prefix or None,
            "aws_region": self.aws_region or None,
            "aws_endpoint_url": self.aws_endpoint_url or None,
            "aws_credentials": "static" if self.has_static_credentials else "iam-chain",
            "access_key_id": _mask(self.aws_access_key_id),
            "secret_access_key": "***" if self.aws_secret_access_key else None,
            "database": _redact_url(self.database_url),
            "database_migrate": _redact_url(self.database_migrate_url),
            "database_role": _role_of(self.database_url),
            "kafka_bootstrap_servers": self.kafka_bootstrap_servers or None,
            "kafka_group_id": self.kafka_group_id,
            "kafka_topic_requested": self.kafka_topic_requested,
            "require_tenant_header": self.require_tenant_header,
            "parser_version": self.parser_version,
            "max_fetch_bytes": self.max_fetch_bytes,
            "allow_private_networks": self.allow_private_networks,
            "allowed_host_suffixes": list(self.allowed_host_suffixes),
            "extract_tables": self.extract_tables,
            "ocr": {
                "enabled": self.ocr_enabled,
                "dpi": self.ocr_dpi,
                "language": self.ocr_language,
            },
        }


def _normalise_prefix(raw: str) -> str:
    prefix = (raw or "").strip().strip("/")
    return f"{prefix}/" if prefix else ""


def _mask(value: str) -> str | None:
    """Enough to tell two keys apart in a log, not enough to use one."""
    if not value:
        return None
    return f"{value[:4]}…{value[-4:]}" if len(value) > 8 else "***"


def _role_of(url: str) -> str | None:
    """The role the service connects as, surfaced because it decides whether RLS applies.

    Reported on startup so `postgres` in this field is visible immediately rather than
    discovered when one tenant sees another's documents.
    """
    if not url or "://" not in url:
        return None
    _, _, rest = url.partition("://")
    credentials, _, _host = rest.rpartition("@")
    if not credentials:
        return None
    return credentials.split(":", 1)[0] or None


def _host_of(url: str) -> str:
    """host:port from a connection URL, for an error message that names the target."""
    if "://" not in url:
        return url or "an unspecified host"
    _, _, rest = url.partition("://")
    _, _, host = rest.rpartition("@")
    return host.split("/", 1)[0] or "an unspecified host"


def _redact_url(url: str) -> str | None:
    """A database URL carries a password in the middle of it.

    Logged as-is on startup — which is a very normal thing to do — it puts the database
    password in the log aggregator.
    """
    if not url:
        return None
    if "@" not in url:
        return url
    scheme, _, rest = url.partition("://")
    _credentials, _, host = rest.rpartition("@")
    return f"{scheme}://***@{host}" if scheme else f"***@{host}"
