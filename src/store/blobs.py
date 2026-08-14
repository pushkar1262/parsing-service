"""Fetching the raw file, with the caps and guards that make it safe to do.

Three backends behind one call, chosen by the reference's shape. What they share is
more important than what differs:

**The size cap is enforced while reading, never from a declared length.**
`Content-Length` is a claim, and `HeadObject` describes the object as it was a moment
ago. Both are worth checking first as a cheap early reject, but the only limit that
holds is the one counted byte by byte as they arrive — otherwise a lying or absent
length is an out-of-memory in a worker.

**The hash is computed during the download.** `content_hash` names the S3 key of the
parsed artifact and decides whether two uploads are the same document, so it is needed
on every fetch; computing it in the read loop makes it free rather than a second pass
over the bytes.

**Failures are classified, not raised raw.** A 5xx is transient and should walk the
retry tiers; a 404 or an expired signature is permanent and should go straight to the
DLQ with a reason. That decision belongs here, where the cause is known, rather than
in an `except Exception` in the worker.

Bytes are returned in memory rather than streamed to a temp file, bounded by
`max_bytes`. That keeps the parse stage a pure function of bytes — which is what makes
it testable without infrastructure — and 100 MB is a comfortable ceiling for the
documents this service exists to read. A deployment that needs to accept 2 GB scans
should spool to disk here and hand parsers a file, which is a change to this module
only.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass, field, replace
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from domain.document import SourceRef
from domain.errors import (
    AccessDenied,
    FetchTimeout,
    ObjectNotFound,
    ObjectTooLarge,
    StorageUnavailable,
)
from store.net import NetPolicy, validate_url
from store.refs import ObjectRef, parse_ref

_CHUNK = 1 << 20  # 1 MiB
DEFAULT_MAX_BYTES = 100 * 1024 * 1024

# Retryable S3 / HTTP conditions. Everything else from the 4xx family is a problem
# with the request that will not fix itself.
_TRANSIENT_S3_CODES = frozenset(
    {
        "InternalError",
        "ServiceUnavailable",
        "SlowDown",
        "RequestTimeout",
        "RequestTimeTooSkewed",
        "ThrottlingException",
        "TooManyRequestsException",
        "503",
        "500",
    }
)


@dataclass
class Fetched:
    data: bytes
    content_hash: str
    source: SourceRef
    ref: ObjectRef
    declared_media_type: str | None = None


@dataclass(frozen=True)
class FetchPolicy:
    max_bytes: int = DEFAULT_MAX_BYTES
    net: NetPolicy = field(default_factory=NetPolicy)
    # Local reads are jailed to these roots. Empty means local refs are refused
    # outright, which is the right default for a worker: `file:///etc/passwd` should
    # not be a readable document just because the reference parser understands paths.
    local_roots: tuple[Path, ...] = ()


class _NoRedirects(HTTPRedirectHandler):
    """Refuse every redirect.

    Following one would mean connecting to a URL that never passed `validate_url`, so
    a 302 to the metadata address defeats the whole check. Rejecting outright is
    simpler to reason about than re-validating each hop, and a document store that
    needs redirects to serve an object is not a store we should be reading from.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise AccessDenied(
            f"refusing to follow a redirect ({code}) to {newurl!r}; "
            f"the reference should point at the object directly"
        )


def _read_capped(stream: BinaryIO, max_bytes: int, what: str) -> tuple[bytes, str]:
    """Read a stream to bytes, hashing as we go, aborting past the cap.

    The abort has to happen *during* the read. Checking afterwards means the process
    already holds the oversized payload, which is the failure the cap exists to
    prevent.
    """
    hasher = sha256()
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = stream.read(_CHUNK)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ObjectTooLarge(
                f"{what} exceeds the {max_bytes} byte limit (stopped at {total})"
            )
        hasher.update(chunk)
        chunks.append(chunk)
    if total == 0:
        raise ObjectNotFound(f"{what} is empty")
    return b"".join(chunks), hasher.hexdigest()


class Storage:
    """Fetches raw files. One instance per worker; cheap to construct.

    `s3_client` is injectable so tests never touch AWS and so a deployment can supply
    a client configured for a VPC endpoint or an alternative endpoint URL.
    """

    def __init__(
        self,
        policy: FetchPolicy | None = None,
        *,
        s3_client: Any | None = None,
        opener: Any | None = None,
    ) -> None:
        self.policy = policy or FetchPolicy()
        self._s3 = s3_client
        self._opener = opener or build_opener(_NoRedirects)

    # ------------------------------------------------------------------ public

    def fetch(self, reference: str | ObjectRef) -> Fetched:
        ref = reference if isinstance(reference, ObjectRef) else parse_ref(reference)
        if ref.kind == "s3":
            return self._fetch_s3(ref)
        if ref.kind == "http":
            return self._fetch_http(ref)
        return self._fetch_file(ref)

    # ---------------------------------------------------------------------- s3

    def _s3_client(self) -> Any:
        if self._s3 is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - environment dependent
                raise StorageUnavailable(
                    f"boto3 is not installed, so s3:// references cannot be "
                    f"fetched ({exc}); install parsing-service[s3]"
                ) from exc
            self._s3 = boto3.client("s3")
        return self._s3

    def _fetch_s3(self, ref: ObjectRef) -> Fetched:
        client = self._s3_client()
        extra: dict[str, Any] = {"Bucket": ref.bucket, "Key": ref.key}
        if ref.version_id:
            extra["VersionId"] = ref.version_id

        # Cheap early reject: refuse a 3 GB object without transferring it. Not a
        # substitute for the streaming cap, since the object could change underneath.
        try:
            head = client.head_object(**extra)
        except Exception as exc:
            raise self._map_s3_error(exc, ref) from exc

        declared_size = head.get("ContentLength")
        if declared_size is not None and declared_size > self.policy.max_bytes:
            raise ObjectTooLarge(
                f"{ref.describe} declares {declared_size} bytes, over the "
                f"{self.policy.max_bytes} byte limit"
            )

        try:
            response = client.get_object(**extra)
            body = response["Body"]
        except Exception as exc:
            raise self._map_s3_error(exc, ref) from exc

        try:
            data, digest = _read_capped(body, self.policy.max_bytes, ref.describe)
        finally:
            close = getattr(body, "close", None)
            if close:
                close()

        # The response's own VersionId pins exactly which bytes we read, even when the
        # reference did not name a version. Without it, an overwrite makes the linkage
        # ambiguous and a later reprocess could parse different bytes.
        version = response.get("VersionId") or ref.version_id
        return Fetched(
            data=data,
            content_hash=digest,
            ref=replace(ref, version_id=version),
            declared_media_type=_clean_media_type(response.get("ContentType")),
            source=SourceRef(
                bucket=ref.bucket or "",
                key=ref.key or "",
                version_id=version,
                byte_size=len(data),
                media_type=_clean_media_type(response.get("ContentType")),
            ),
        )

    def _map_s3_error(self, exc: Exception, ref: ObjectRef) -> Exception:
        """Turn a boto3 error into a classified failure.

        Done by inspecting the response rather than the exception type because
        botocore raises `ClientError` for everything from a missing key to a throttle,
        and the difference is exactly what decides whether we retry.
        """
        response = getattr(exc, "response", None) or {}
        error = response.get("Error", {})
        code = str(error.get("Code", "")) or type(exc).__name__
        status = str(response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))

        if code in ("NoSuchKey", "NoSuchBucket", "NoSuchVersion", "404") or status == "404":
            return ObjectNotFound(f"{ref.describe} does not exist")
        if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "403"):
            return AccessDenied(
                f"not permitted to read {ref.describe} ({code}); if this is a "
                f"presigned reference the signature may have expired"
            )
        if code in _TRANSIENT_S3_CODES or status.startswith("5"):
            return StorageUnavailable(f"blob storage error reading {ref.describe}: {code}")
        if isinstance(exc, (socket.timeout, TimeoutError)):
            return FetchTimeout(f"timed out reading {ref.describe}")
        # Unrecognised: treat as transient. A wrong retry costs a little money; a
        # wrong DLQ routing loses a document until somebody notices.
        return StorageUnavailable(f"unclassified storage error for {ref.describe}: {exc}")

    # -------------------------------------------------------------------- http

    def _fetch_http(self, ref: ObjectRef) -> Fetched:
        assert ref.url is not None
        validate_url(ref.url, self.policy.net)

        request = Request(ref.url, method="GET")
        try:
            response = self._opener.open(request, timeout=self.policy.net.read_timeout_s)
        except HTTPError as exc:
            raise self._map_http_status(exc.code, ref) from exc
        except TimeoutError as exc:
            raise FetchTimeout(f"timed out fetching {ref.describe}") from exc
        except URLError as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, (TimeoutError, socket.timeout)):
                raise FetchTimeout(f"timed out fetching {ref.describe}") from exc
            raise StorageUnavailable(f"cannot reach {ref.describe}: {reason}") from exc

        with response:
            declared = response.headers.get("Content-Length")
            if declared and declared.isdigit() and int(declared) > self.policy.max_bytes:
                raise ObjectTooLarge(
                    f"{ref.describe} declares {declared} bytes, over the "
                    f"{self.policy.max_bytes} byte limit"
                )
            data, digest = _read_capped(response, self.policy.max_bytes, ref.describe)
            media_type = _clean_media_type(response.headers.get("Content-Type"))

        return Fetched(
            data=data,
            content_hash=digest,
            ref=ref,
            declared_media_type=media_type,
            source=SourceRef(
                bucket=ref.bucket or "",
                key=ref.key or ref.describe,
                version_id=ref.version_id,
                byte_size=len(data),
                media_type=media_type,
            ),
        )

    def _map_http_status(self, status: int, ref: ObjectRef) -> Exception:
        if status == 404:
            return ObjectNotFound(f"{ref.describe} does not exist")
        if status in (401, 403):
            return AccessDenied(
                f"not permitted to read {ref.describe} (HTTP {status}); a presigned "
                f"URL may have expired"
            )
        if status == 408 or status == 429 or status >= 500:
            return StorageUnavailable(f"HTTP {status} fetching {ref.describe}")
        return AccessDenied(f"HTTP {status} fetching {ref.describe}")

    # -------------------------------------------------------------------- file

    def _fetch_file(self, ref: ObjectRef) -> Fetched:
        """Read a local file, jailed to the configured roots.

        The jail is not paranoia about our own code: the reference arrives in a job
        message, and a `file:///etc/shadow` reference should be refused by policy
        rather than by hoping no such message is ever produced. `resolve()` before
        comparing is what makes `../` traversal ineffective.
        """
        assert ref.path is not None
        if not self.policy.local_roots:
            raise AccessDenied(
                "local file references are not enabled; configure "
                "FetchPolicy.local_roots to allow them"
            )

        path = ref.path.expanduser().resolve()
        roots = [root.expanduser().resolve() for root in self.policy.local_roots]
        if not any(path == root or root in path.parents for root in roots):
            raise AccessDenied(
                f"{path} is outside the permitted roots "
                f"({', '.join(str(r) for r in roots)})"
            )
        if not path.is_file():
            raise ObjectNotFound(f"{path} is not a file")
        if path.stat().st_size > self.policy.max_bytes:
            raise ObjectTooLarge(
                f"{path} is {path.stat().st_size} bytes, over the "
                f"{self.policy.max_bytes} byte limit"
            )

        with path.open("rb") as handle:
            data, digest = _read_capped(handle, self.policy.max_bytes, str(path))

        return Fetched(
            data=data,
            content_hash=digest,
            ref=ref,
            declared_media_type=None,
            source=SourceRef(
                bucket="", key=str(path), byte_size=len(data), media_type=None
            ),
        )


def _clean_media_type(value: str | None) -> str | None:
    """Strip parameters, so `text/plain; charset=utf-8` compares as `text/plain`.

    Also drops the placeholder S3 applies to objects uploaded without a type, which is
    a claim of ignorance rather than a claim of being a binary blob — and treating it
    as the latter would produce a misleading mismatch warning on every such upload.
    """
    if not value:
        return None
    cleaned = value.split(";", 1)[0].strip().lower()
    if not cleaned or cleaned == "binary/octet-stream":
        return None
    return cleaned
