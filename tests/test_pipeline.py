"""Detection, parser resolution, and the lookups the serving layer is built on.

The theme is that nothing here trusts the caller. A declared content type is a claim
to be checked, an extension is a hint, and a format we cannot handle has to fail as
recorded data rather than as an exception escaping the worker.
"""

from __future__ import annotations

import io
import zipfile

import pytest

from domain.document import BlockType
from parse.base import UnsupportedFormat
from parse.detect import detect
from parse.pipeline import content_hash_of, parse_document
from parse.registry import Registry, default_registry
from parse.text import TextParser
from tests.conftest import RICH, assert_spans_hold, own_line, parse

# --------------------------------------------------------------------------- #
# detection — the bytes decide
# --------------------------------------------------------------------------- #


def test_a_pdf_is_detected_by_its_signature() -> None:
    assert detect(b"%PDF-1.7\n...") == "application/pdf"


def test_images_are_detected_by_signature() -> None:
    assert detect(b"\x89PNG\r\n\x1a\n\x00") == "image/png"
    assert detect(b"\xff\xd8\xff\xe0junk") == "image/jpeg"


def test_a_lying_extension_does_not_win() -> None:
    """The whole reason detection exists: an extension is attacker-controlled."""
    assert detect(b"%PDF-1.4 payload", filename="invoice.txt") == "application/pdf"


def test_markdown_and_plain_text_are_separated_by_extension_only() -> None:
    """They are byte-identical, so the extension is the only available signal.

    It changes nothing about how we parse — only what `metadata.format` records.
    """
    body = b"# Heading\n\nBody text.\n"
    assert detect(body, filename="spec.md") == "text/markdown"
    assert detect(body, filename="spec.txt") == "text/plain"
    assert detect(body) == "text/plain"


def test_a_plain_zip_is_not_mistaken_for_an_office_document() -> None:
    """OOXML detection has to inspect the archive, so it must also reject non-Office zips."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("holiday/photo.txt", "not an office document")
    assert detect(buffer.getvalue()) == "application/zip"


def test_a_truncated_zip_degrades_to_a_type_no_parser_claims() -> None:
    """Better a clean rejection than an obscure failure deep inside a parser."""
    assert detect(b"PK\x03\x04 truncated garbage") == "application/zip"


def test_a_binary_blob_is_not_treated_as_text() -> None:
    assert detect(b"\x00\x01\x02\x03\xfe\xff\x00\x11binary") == "application/octet-stream"


def test_utf16_text_is_still_text_despite_its_nul_bytes() -> None:
    """A NUL byte usually means binary, but UTF-16 is full of them by design."""
    assert detect("Requirements".encode("utf-16")) == "text/plain"


# --------------------------------------------------------------------------- #
# resolution and failure
# --------------------------------------------------------------------------- #


def test_an_unsupported_format_raises_a_permanent_failure() -> None:
    with pytest.raises(UnsupportedFormat):
        parse_document(b"\x00\x01\x02\xfe\xffbinary", document_id="d")


def test_an_unavailable_optional_parser_says_how_to_fix_it() -> None:
    """"Not installed" and "not supported" are different messages to an operator.

    One is fixed in a minute; the other is a feature request. Collapsing them wastes
    somebody's afternoon.
    """
    registry = Registry()
    registry.register(TextParser())
    registry.mark_unavailable("application/pdf", "pypdfium2 is not installed")

    with pytest.raises(UnsupportedFormat, match="not installed"):
        registry.get("application/pdf")
    with pytest.raises(UnsupportedFormat, match="no parser for"):
        registry.get("application/x-dvi")


def test_the_default_registry_always_has_text() -> None:
    """Importing the registry must never fail over an optional dependency."""
    assert default_registry().supports("text/plain")


def test_a_declared_type_that_contradicts_the_bytes_is_recorded_not_obeyed() -> None:
    doc = parse_document(
        b"# Heading\n\nBody.\n",
        document_id="d",
        media_type="application/pdf",
        filename="spec.md",
    )
    assert doc.metadata.format == "markdown"
    assert [w.code for w in doc.warnings] == ["media_type_mismatch"]


# --------------------------------------------------------------------------- #
# identity
# --------------------------------------------------------------------------- #


def test_content_hash_is_the_hash_of_the_raw_bytes() -> None:
    data = RICH.encode("utf-8")
    assert parse_document(data, document_id="d").content_hash == content_hash_of(data)


def test_the_same_bytes_under_two_document_ids_share_a_content_hash() -> None:
    """Content-addressing is about the bytes, not the document identity.

    Two uploads of the same file produce the same parsed artifact, which is what lets
    the S3 key be derived from the hash and makes a replay an idempotent overwrite.
    """
    data = RICH.encode("utf-8")
    first = parse_document(data, document_id="a")
    second = parse_document(data, document_id="b")
    assert first.content_hash == second.content_hash
    assert first.text == second.text


# --------------------------------------------------------------------------- #
# metadata counts describe the canonical text, not the source file
# --------------------------------------------------------------------------- #


def test_char_count_is_the_length_of_the_canonical_text() -> None:
    doc = parse(RICH)
    assert doc.metadata.char_count == len(doc.text)
    assert doc.metadata.word_count == len(doc.text.split())


def test_structural_counts_are_derived_not_trusted() -> None:
    doc = parse(RICH)
    assert doc.metadata.block_count == len(doc.blocks)
    assert doc.metadata.heading_count == 4
    assert doc.metadata.table_count == 1


# --------------------------------------------------------------------------- #
# lookups — what the serving layer exposes
# --------------------------------------------------------------------------- #


def test_block_at_returns_the_innermost_block(rich) -> None:
    """Several blocks contain one offset; the useful answer is the narrowest.

    An offset inside a nested bullet sits within that item, its list, the parent
    item, the outer list, and — by parentage — a heading's section. The innermost is
    the one that names what the text at that position actually is.
    """
    offset = rich.text.index("Notify owners")
    block = rich.block_at(offset)
    assert block is not None
    assert block.type is BlockType.LIST_ITEM
    assert own_line(block) == "Notify owners 7 days before rotation"


def test_block_at_returns_none_outside_the_document(rich) -> None:
    assert rich.block_at(len(rich.text) + 5) is None
    assert rich.block_at(-1) is None


def test_page_at_is_none_when_the_format_has_no_pages(rich) -> None:
    """Honest absence rather than an invented page 1."""
    assert rich.pages == []
    assert rich.page_at(rich.text.index("Notify owners")) is None


def test_heading_path_gives_the_enclosing_section_chain(rich) -> None:
    offset = rich.text.index("Audit log retention")
    block = rich.block_at(offset)
    assert block is not None
    assert rich.heading_path(block.id) == [
        "Payments Platform Requirements",
        "Security",
        "Compliance",
    ]


def test_heading_path_of_a_top_level_block_is_empty() -> None:
    doc = parse("Just a paragraph with no headings at all.")
    assert_spans_hold(doc)
    assert doc.heading_path(doc.blocks[0].id) == []
