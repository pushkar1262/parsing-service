"""Quote lookup — the integration point with the planning service.

Every test here mirrors a case the downstream `verbatim_quotes` validator either
handles by normalising, or gets wrong. The ones it gets wrong are the point.
"""

from __future__ import annotations

from domain.document import BlockType
from domain.locate import Locator, locate
from parse.base import PageInfo, RawBlock
from parse.pipeline import parse_document
from parse.serialize import serialize
from tests.conftest import RICH, parse


def _doc_from(blocks, *, page_meta=None):
    """A ParsedDocument straight from raw blocks, for page-level cases."""
    out = serialize(blocks, content_hash="c0ffee", page_meta=page_meta or {})
    from domain.document import DocumentMetadata, ParsedDocument

    return ParsedDocument(
        document_id="d",
        content_hash="c0ffee",
        metadata=DocumentMetadata(
            format="test", parser_name="test", parser_version="1.0"
        ),
        text=out.text,
        blocks=out.blocks,
        pages=out.pages,
    )


# --------------------------------------------------------------------------- #
# exact matching, and what the offset buys
# --------------------------------------------------------------------------- #


def test_a_verbatim_quote_is_found_exactly() -> None:
    doc = parse(RICH)
    result = Locator(doc).locate("authenticate users within 300ms")
    assert result.found and result.match == "exact"
    assert result.similarity == 1.0
    assert doc.text[result.span[0] : result.span[1]] == "authenticate users within 300ms"


def test_the_returned_text_is_the_source_span_not_the_input() -> None:
    """The distinction that makes snapping safe: `text` always comes from the document."""
    doc = parse(RICH)
    result = Locator(doc).locate("AUTHENTICATE USERS within 300MS")
    assert result.found
    assert result.text == "authenticate users within 300ms"


def test_a_quote_resolves_to_its_block(rich) -> None:
    result = Locator(rich).locate("Rotate API keys every 90 days")
    assert result.found
    block = {b.id: b for b in rich.blocks}[result.block_id]
    assert block.type is BlockType.LIST_ITEM


def test_a_quote_from_a_table_resolves(rich) -> None:
    """Only possible because the table was rendered into the canonical text."""
    result = Locator(rich).locate("Audit log retention")
    assert result.found and result.match == "exact"
    block = {b.id: b for b in rich.blocks}[result.block_id]
    assert block.type is BlockType.TABLE


def test_an_invented_quote_is_not_found() -> None:
    """The hallucination guard. Nothing about this may be softened."""
    doc = parse(RICH)
    result = Locator(doc).locate("The system shall support biometric login")
    assert not result.found
    assert result.match == "none"
    assert result.span is None
    assert "does not appear" in result.reason


def test_a_quote_too_short_to_be_evidence_is_rejected() -> None:
    """A three-character match proves nothing about provenance."""
    result = Locator(parse(RICH)).locate("the")
    assert not result.found
    assert "too short" in result.reason


# --------------------------------------------------------------------------- #
# the normalisation a model's retyping requires
# --------------------------------------------------------------------------- #


def test_reflowed_whitespace_still_matches() -> None:
    """Models collapse and re-break whitespace when asked to copy verbatim."""
    doc = parse(RICH)
    result = Locator(doc).locate("authenticate   users\n  within 300ms")
    assert result.found and result.match == "exact"
    assert result.text == "authenticate users within 300ms"


def test_straightened_punctuation_still_matches() -> None:
    doc = parse("The “platform” team’s scope covers payments only.")
    result = Locator(doc).locate('the "platform" team\'s scope')
    assert result.found and result.match == "exact"
    # The text handed back carries the document's real punctuation, not the model's.
    assert result.text == "The “platform” team’s scope"


def test_an_em_dash_retyped_as_a_hyphen_still_matches() -> None:
    doc = parse("Latency — measured at p99 — must stay under 500ms.")
    result = Locator(doc).locate("Latency - measured at p99 - must stay under 500ms")
    assert result.found and result.match == "exact"


# --------------------------------------------------------------------------- #
# snapping — a near miss returns real source text instead of failing
# --------------------------------------------------------------------------- #


def test_a_near_miss_snaps_to_the_source_span() -> None:
    """The behaviour that stops one bad quote discarding a whole extraction."""
    doc = parse("The service shall authenticate every request before it reaches the gateway.")
    result = Locator(doc).locate("The service shall authenticate every request before it reches the gateway")
    assert result.found
    assert result.match == "snapped"
    assert result.similarity >= 0.90
    # The typo is gone: what comes back is what the document actually says.
    assert "reaches" in result.text
    assert doc.text[result.span[0] : result.span[1]] == result.text


def test_a_snapped_span_never_ends_mid_word() -> None:
    """Alignment stops at the last matching character, which cuts words in half.

    "every 90 dayz" aligns against "every 90 days" only as far as "day" — the
    differing letter falls outside every matching run. Handing that back would store a
    quote ending in a truncated word, which reads as corruption and would fail a later
    exact re-check.
    """
    doc = parse("Rotate API keys every 90 days without exception.")
    result = Locator(doc).locate("Rotate API keys every 90 dayz")
    assert result.match == "snapped"
    assert result.text is not None
    assert result.text.endswith("days")
    assert doc.text[result.span[0] : result.span[1]] == result.text


def test_a_paraphrase_does_not_snap() -> None:
    """Snapping must not become a licence to accept invented requirements."""
    doc = parse("The service shall authenticate every request before it reaches the gateway.")
    result = Locator(doc).locate("Users log in with a password and an OTP code")
    assert not result.found
    assert result.match == "none"


# --------------------------------------------------------------------------- #
# page attribution — resolved, never guessed
# --------------------------------------------------------------------------- #


def test_a_quote_resolves_to_its_page_number() -> None:
    doc = _doc_from(
        [
            RawBlock(type=BlockType.PARAGRAPH, text="Nothing of note here.", page=3),
            RawBlock(
                type=BlockType.PARAGRAPH,
                text="The service shall authenticate users within 300ms.",
                page=4,
            ),
        ]
    )
    result = Locator(doc).locate("authenticate users within 300ms")
    assert result.found
    assert result.page == 4


def test_ocr_provenance_travels_with_the_match() -> None:
    """The flag that resolves the vision/OCR conflict downstream.

    `extract` requires vision, so on a scanned page the model quotes what it *sees*
    while the validator checks our OCR text. A consumer can only loosen strictness
    where it is unjust if we tell it which matches came off a scan.
    """
    doc = _doc_from(
        [RawBlock(type=BlockType.PARAGRAPH, text="Encrypt all traffic in transit.", page=1)],
        page_meta={1: PageInfo(source="ocr", confidence=0.71)},
    )
    result = Locator(doc).locate("Encrypt all traffic in transit")
    assert result.found
    assert result.ocr_applied is True
    assert result.confidence == 0.71


def test_page_is_none_for_a_format_without_pages(rich) -> None:
    result = Locator(rich).locate("Rotate API keys every 90 days")
    assert result.found
    assert result.page is None
    assert result.ocr_applied is False


# --------------------------------------------------------------------------- #
# ambiguity
# --------------------------------------------------------------------------- #


def test_a_repeated_sentence_reports_its_occurrence_count() -> None:
    """Page attribution for a boilerplate line is a coin flip; say so.

    A `verbatim_quotes` built on `in` cannot distinguish "found once" from "found in
    the footer of every page", and silently attributes the quote to the first hit.
    """
    doc = _doc_from(
        [
            RawBlock(type=BlockType.PARAGRAPH, text="Confidential draft only.", page=1),
            RawBlock(type=BlockType.PARAGRAPH, text="Some real content here.", page=1),
            RawBlock(type=BlockType.PARAGRAPH, text="Confidential draft only.", page=2),
        ]
    )
    result = Locator(doc).locate("Confidential draft only")
    assert result.found
    assert result.occurrences == 2
    assert result.page == 1  # the first, and the caller now knows to distrust it


def test_a_unique_quote_reports_one_occurrence(rich) -> None:
    assert Locator(rich).locate("Rotate API keys every 90 days").occurrences == 1


# --------------------------------------------------------------------------- #
# batch
# --------------------------------------------------------------------------- #


def test_a_batch_of_quotes_is_answered_in_order() -> None:
    """One folded document, many quotes — the shape an extraction actually arrives in."""
    doc = parse(RICH)
    results = locate(
        doc,
        [
            "authenticate users within 300ms",
            "The system shall support biometric login",
            "Response times shall not exceed 500ms at p99",
        ],
    )
    assert [r.match for r in results] == ["exact", "none", "exact"]
    assert [r.found for r in results] == [True, False, True]


def test_locating_against_an_empty_document_finds_nothing() -> None:
    doc = parse_document(b"", document_id="d", filename="empty.txt")
    result = Locator(doc).locate("anything at all here")
    assert not result.found
