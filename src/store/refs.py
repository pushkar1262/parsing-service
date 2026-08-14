"""Where a document lives, in the several forms a job might state it.

The design says a job carries bucket, key and version id. Reality is that a
user-facing backend which already issues presigned upload URLs will often have a
presigned *download* URL to hand, and would rather pass that than a triple. Both are
supported, and they take different paths on purpose:

- `s3://bucket/key` is fetched with the worker's own IAM role, which is the right
  thing for a service reading its own bucket.
- A presigned `https://` URL is fetched over plain HTTP with no credentials, because
  that is the entire point of a presigned URL — the signature in the query string
  *is* the authorisation, and re-signing it with our own credentials would defeat any
  scoping the backend applied.

Either way we record `bucket`, `key` and `version_id` when they can be recovered, so
the linkage back to the raw file survives for reprocessing even when the URL that
delivered it has long since expired.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qs, unquote, urlsplit

RefKind = Literal["s3", "http", "file"]

# Virtual-hosted style: bucket.s3.region.amazonaws.com, bucket.s3.amazonaws.com,
# and the legacy bucket.s3-region.amazonaws.com.
_VIRTUAL_HOSTED = re.compile(
    r"^(?P<bucket>[a-z0-9.\-]{3,63})\.s3[.-](?:(?P<region>[a-z0-9-]+)\.)?"
    r"amazonaws\.com$",
    re.IGNORECASE,
)
# Path style: s3.region.amazonaws.com/bucket/key
_PATH_STYLE = re.compile(
    r"^s3[.-](?:(?P<region>[a-z0-9-]+)\.)?amazonaws\.com$", re.IGNORECASE
)


@dataclass(frozen=True)
class ObjectRef:
    kind: RefKind
    raw: str
    bucket: str | None = None
    key: str | None = None
    version_id: str | None = None
    url: str | None = None
    path: Path | None = None
    region: str | None = None

    @property
    def describe(self) -> str:
        """A short, log-safe description.

        Presigned URLs are truncated at the query string: the signature is a bearer
        credential for that object, and a full URL in a log line is a credential in a
        log line.
        """
        if self.kind == "s3":
            suffix = f"?versionId={self.version_id}" if self.version_id else ""
            return f"s3://{self.bucket}/{self.key}{suffix}"
        if self.kind == "http" and self.url:
            split = urlsplit(self.url)
            return f"{split.scheme}://{split.netloc}{split.path}"
        return str(self.path)


def parse_ref(ref: str) -> ObjectRef:
    """Turn a reference string into something typed.

    A bare path is treated as a local file rather than rejected, because that is what
    makes the CLI and the tests usable without any of this being mocked.
    """
    text = ref.strip()
    if not text:
        raise ValueError("empty object reference")

    split = urlsplit(text)
    scheme = split.scheme.lower()

    if scheme == "s3":
        if not split.netloc or not split.path.lstrip("/"):
            raise ValueError(f"s3 reference needs a bucket and a key: {ref!r}")
        query = parse_qs(split.query)
        version = query.get("versionId", [None])[0]
        return ObjectRef(
            kind="s3",
            raw=text,
            bucket=split.netloc,
            key=unquote(split.path.lstrip("/")),
            version_id=version,
        )

    if scheme in ("http", "https"):
        bucket, key, region = _s3_parts_from_url(split)
        query = parse_qs(split.query)
        return ObjectRef(
            kind="http",
            raw=text,
            url=text,
            bucket=bucket,
            key=key,
            region=region,
            version_id=query.get("versionId", [None])[0],
        )

    if scheme == "file":
        return ObjectRef(kind="file", raw=text, path=Path(unquote(split.path)))

    if scheme and len(scheme) > 1:
        raise ValueError(f"unsupported reference scheme {scheme!r}: {ref!r}")

    # No scheme, or a single letter (a Windows drive) — a filesystem path.
    return ObjectRef(kind="file", raw=text, path=Path(text))


def _s3_parts_from_url(split) -> tuple[str | None, str | None, str | None]:
    """Recover bucket and key from an S3 URL, so linkage survives the URL expiring.

    Best-effort by design: the URL is still fetched verbatim (the signature depends on
    it), and a non-S3 URL simply yields no bucket. Getting this wrong must never break
    the fetch, only the bookkeeping.
    """
    host = (split.hostname or "").lower()
    path = unquote(split.path.lstrip("/"))

    virtual = _VIRTUAL_HOSTED.match(host)
    if virtual:
        return virtual.group("bucket"), path or None, virtual.group("region")

    if _PATH_STYLE.match(host):
        bucket, _, key = path.partition("/")
        return (bucket or None), (key or None), _PATH_STYLE.match(host).group("region")

    return None, None, None
