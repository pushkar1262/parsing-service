"""Writing the parsed artifact, content-addressed.

    parsed/{content_hash}/{parser_version}/document.json
    parsed/{content_hash}/{parser_version}/pages/{n}.png

Content-addressing is what makes a replay free rather than dangerous. The same bytes and
the same parser version produce the same key *and* the same content, so a duplicate
delivery is an idempotent overwrite — there is no "did this already run?" question to get
wrong, because running it again lands in exactly the same place.

It also makes the artifact shared by construction. Two documents uploaded with identical
bytes have the same `content_hash`, so the second parse writes the object the first one
already wrote. The `documents` row keeps them distinct; the storage does not need to.

The write order inside `put` matters and is the opposite of the intuitive one: page
images first, `document.json` last. The JSON is what the API serves and what names the
images, so it must not become visible until everything it references exists. A crash
halfway leaves orphaned images, which a sweeper reaps; the other order leaves a served
document pointing at images that were never written.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from domain.document import ParsedDocument
from domain.errors import ObjectNotFound, StorageUnavailable

ARTIFACT_NAME = "document.json"


def artifact_prefix(content_hash: str, parser_version: str) -> str:
    return f"parsed/{content_hash}/{parser_version}"


def artifact_key(content_hash: str, parser_version: str) -> str:
    return f"{artifact_prefix(content_hash, parser_version)}/{ARTIFACT_NAME}"


def page_image_key(content_hash: str, parser_version: str, page: int) -> str:
    return f"{artifact_prefix(content_hash, parser_version)}/pages/{page}.png"


@runtime_checkable
class ArtifactStore(Protocol):
    def put(
        self, document: ParsedDocument, *, page_images: dict[int, bytes] | None = None
    ) -> str: ...

    def get(self, key: str) -> ParsedDocument: ...

    def get_bytes(self, key: str) -> bytes: ...

    def delete_prefix(self, prefix: str) -> int: ...

    def exists(self, key: str) -> bool: ...


@dataclass
class _Written:
    key: str
    images: dict[int, str]


class _BaseArtifactStore:
    """The shared write logic; subclasses supply the three storage primitives."""

    def _write(self, key: str, body: bytes) -> None:  # pragma: no cover - abstract
        raise NotImplementedError

    def _read(self, key: str) -> bytes:  # pragma: no cover - abstract
        raise NotImplementedError

    def put(
        self, document: ParsedDocument, *, page_images: dict[int, bytes] | None = None
    ) -> str:
        version = document.metadata.parser_version
        images: dict[int, str] = {}

        # Images before the JSON, so the document never references an object that does
        # not exist yet. A crash between the two leaves orphans, which is recoverable;
        # the other order leaves a served document with broken references, which is not.
        for number, payload in (page_images or {}).items():
            key = page_image_key(document.content_hash, version, number)
            self._write(key, payload)
            images[number] = key

        if images:
            for page in document.pages:
                if page.number in images:
                    page.image_key = images[page.number]

        key = artifact_key(document.content_hash, version)
        self._write(key, document.model_dump_json().encode("utf-8"))
        return key

    def get(self, key: str) -> ParsedDocument:
        return ParsedDocument.model_validate_json(self._read(key).decode("utf-8"))

    def get_bytes(self, key: str) -> bytes:
        return self._read(key)


class LocalArtifactStore(_BaseArtifactStore):
    """Filesystem-backed, for development and for every test in this repo."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def _write(self, key: str, body: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename, so a reader never sees a half-written artifact. The
        # temporary file sits in the same directory to keep the rename atomic.
        temporary = path.with_suffix(path.suffix + ".partial")
        temporary.write_bytes(body)
        temporary.replace(path)

    def _read(self, key: str) -> bytes:
        path = self._path(key)
        if not path.is_file():
            raise ObjectNotFound(f"no artifact at {key}")
        return path.read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def delete_prefix(self, prefix: str) -> int:
        target = self._path(prefix)
        if not target.exists():
            return 0
        removed = 0
        for path in sorted(target.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
                removed += 1
            else:
                path.rmdir()
        target.rmdir()
        return removed


class S3ArtifactStore(_BaseArtifactStore):
    """S3-backed. The client is injected so tests never reach AWS."""

    def __init__(
        self, bucket: str, *, client: Any | None = None, prefix: str = ""
    ) -> None:
        self.bucket = bucket
        # An optional prefix so artifacts can share a bucket with raw uploads without
        # colliding, and so a lifecycle rule can target them separately — parsed content
        # is cheap to regenerate and can expire far more aggressively than a raw file.
        self.prefix = f"{prefix.strip('/')}/" if prefix.strip("/") else ""
        self._client = client

    def _full(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def _s3(self) -> Any:
        if self._client is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - environment dependent
                raise StorageUnavailable(
                    f"boto3 is not installed; install parsing-service[s3] ({exc})"
                ) from exc
            self._client = boto3.client("s3")
        return self._client

    def _write(self, key: str, body: bytes) -> None:
        content_type = "application/json" if key.endswith(".json") else "image/png"
        self._s3().put_object(
            Bucket=self.bucket, Key=self._full(key), Body=body, ContentType=content_type
        )

    def _read(self, key: str) -> bytes:
        try:
            response = self._s3().get_object(Bucket=self.bucket, Key=self._full(key))
        except Exception as exc:
            code = str((getattr(exc, "response", {}) or {}).get("Error", {}).get("Code"))
            if code in ("NoSuchKey", "404"):
                raise ObjectNotFound(f"no artifact at {key}") from exc
            raise StorageUnavailable(f"cannot read {key}: {exc}") from exc
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._s3().head_object(Bucket=self.bucket, Key=self._full(key))
        except Exception:  # noqa: BLE001 - absent or unreachable; caller re-writes either way
            return False
        return True

    def delete_prefix(self, prefix: str) -> int:
        client = self._s3()
        removed = 0
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": self._full(prefix)}
            if token:
                kwargs["ContinuationToken"] = token
            listing = client.list_objects_v2(**kwargs)
            keys = [{"Key": item["Key"]} for item in listing.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})
                removed += len(keys)
            if not listing.get("IsTruncated"):
                break
            token = listing.get("NextContinuationToken")
        return removed


def load_json(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))
