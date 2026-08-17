"""Bytes to `ParsedDocument`. The whole parse stage behind one call.

Deliberately pure: bytes in, artifact out, no blob storage, no database, no queue.
That is what makes the golden-corpus tests possible without infrastructure, and it
is what makes replay safe — the same bytes and the same parser version produce a
byte-identical artifact, which is the property the content-addressed S3 key depends
on for idempotency.

The worker wraps this with the parts that *do* touch the world: fetch, validate,
scan, persist, commit.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Any

from domain.document import (
    BlockType,
    DocumentMetadata,
    ParsedDocument,
    SourceRef,
)
from parse.base import ParseResult
from parse.detect import detect
from parse.registry import Registry, default_registry
from parse.serialize import serialize

_METADATA_FIELDS = frozenset(DocumentMetadata.model_fields)


def content_hash_of(data: bytes) -> str:
    """The identity of the bytes, and the linkage back to the raw file.

    sha256 rather than a faster non-cryptographic hash: this value names S3 keys and
    decides whether two uploads are the same document, so collision resistance is
    worth more here than throughput.
    """
    return sha256(data).hexdigest()


def parse_document(
    data: bytes,
    *,
    document_id: str,
    media_type: str | None = None,
    filename: str | None = None,
    content_hash: str | None = None,
    source: SourceRef | None = None,
    registry: Registry | None = None,
) -> ParsedDocument:
    """Parse `data` into the canonical representation.

    `media_type` is optional and, when given, treated as a *hint that is checked*:
    we sniff regardless and use what the bytes say. Trusting a caller-supplied type
    is how a .exe gets handed to the PDF parser.
    """
    registry = registry or default_registry()
    sniffed = detect(data, filename=filename)
    digest = content_hash or content_hash_of(data)

    parser = registry.get(sniffed)
    result = parser.parse(data, filename=filename)

    if media_type and media_type != sniffed:
        result.warn(
            "media_type_mismatch",
            f"caller declared {media_type!r} but the bytes are {sniffed!r}; "
            f"parsed as {sniffed!r}",
        )

    return build_from_result(
        result,
        data=data,
        document_id=document_id,
        fallback_format=sniffed,
        parser_name=parser.name,
        parser_version=parser.version,
        content_hash=digest,
        source=source,
    )


def build_from_result(
    result: ParseResult,
    *,
    data: bytes,
    document_id: str,
    fallback_format: str,
    parser_name: str = "unknown",
    parser_version: str = "0",
    content_hash: str | None = None,
    source: SourceRef | None = None,
) -> ParsedDocument:
    """Serialise a parser's output into the artifact.

    Split out from `parse_document` so a caller holding a `ParseResult` — a test
    exercising one parser directly, or a worker that resolved the parser itself in order
    to configure it — can reach the same artifact without going back through detection
    and registry lookup.
    """
    digest = content_hash or content_hash_of(data)
    serialised = serialize(result.blocks, content_hash=digest, page_meta=result.page_meta)

    metadata = _metadata(
        result.metadata,
        parser_name=parser_name,
        parser_version=parser_version,
        fallback_format=fallback_format,
        text=serialised.text,
        blocks=serialised.blocks,
        page_count=len(serialised.pages) or None,
    )

    return ParsedDocument(
        document_id=document_id,
        content_hash=digest,
        metadata=metadata,
        text=serialised.text,
        blocks=serialised.blocks,
        pages=serialised.pages,
        source=source,
        warnings=result.warnings,
    )


def _metadata(
    supplied: dict[str, Any],
    *,
    parser_name: str,
    parser_version: str,
    fallback_format: str,
    text: str,
    blocks: list[Any],
    page_count: int | None,
) -> DocumentMetadata:
    """Merge a parser's format-specific metadata with the counts we always derive.

    Counts are computed here rather than trusted from the parser so they describe
    the *canonical text* rather than the source file. `char_count` in particular has
    to be `len(text)`: it is the number every offset in the document is bounded by,
    and a parser reporting the source file's length instead would make every
    range check subtly wrong.
    """
    fields = {key: value for key, value in supplied.items() if key in _METADATA_FIELDS}
    fields.setdefault("format", fallback_format)
    fields["parser_name"] = parser_name
    fields["parser_version"] = parser_version
    fields["char_count"] = len(text)
    fields["word_count"] = len(text.split())
    fields["block_count"] = len(blocks)
    fields["heading_count"] = sum(1 for b in blocks if b.type is BlockType.HEADING)
    fields["table_count"] = sum(1 for b in blocks if b.type is BlockType.TABLE)
    if page_count is not None and "page_count" not in fields:
        fields["page_count"] = page_count
    return DocumentMetadata(**fields)
