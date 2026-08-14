"""DOCX parsing, against a document built in code rather than a binary fixture.

Building the .docx here keeps the fixture reviewable — a reader can see exactly what
structure the assertions expect — and keeps the repo free of binary blobs whose
contents nobody can diff. The golden corpus of real-world files comes later, for the
formats where authoring in code is not possible.
"""

from __future__ import annotations

import io

import pytest

from domain.document import BlockType

docx = pytest.importorskip("docx", reason="python-docx is an optional dependency")

from parse.pipeline import parse_document
from tests.conftest import assert_spans_hold, own_line

DOCX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)


def build_docx() -> bytes:
    document = docx.Document()
    document.core_properties.author = "Platform Team"
    document.core_properties.keywords = "payments; security, latency"

    document.add_heading("Payments Platform Requirements", level=1)
    document.add_paragraph("The system must authenticate users within 300ms.")

    document.add_heading("Security", level=2)
    document.add_paragraph("Encrypt all traffic with TLS 1.3", style="List Bullet")
    document.add_paragraph("Rotate API keys every 90 days", style="List Bullet")
    document.add_paragraph(
        "Notify owners 7 days before rotation", style="List Bullet 2"
    )

    document.add_heading("Compliance", level=3)
    table = document.add_table(rows=3, cols=3)
    cells = [
        ["Control", "Standard", "Owner"],
        ["Audit log retention", "SOC 2", "Platform"],
        ["Data residency", "GDPR", "Legal"],
    ]
    for row_index, row in enumerate(cells):
        for col_index, value in enumerate(row):
            table.cell(row_index, col_index).text = value

    document.add_heading("Performance", level=2)
    document.add_paragraph("Response times shall not exceed 500ms at p99.")

    buffer = io.BytesIO()
    document.save(buffer)
    return buffer.getvalue()


@pytest.fixture(scope="module")
def parsed():
    doc = parse_document(build_docx(), document_id="docx-1", filename="spec.docx")
    assert_spans_hold(doc)
    return doc


# --------------------------------------------------------------------------- #
# detection and the invariant
# --------------------------------------------------------------------------- #


def test_a_docx_is_detected_from_its_zip_contents() -> None:
    """A .docx and a .xlsx share magic bytes; only the archive listing separates them."""
    from parse.detect import detect

    assert detect(build_docx(), filename="anything.bin") == DOCX_MEDIA_TYPE


def test_spans_hold_for_a_docx(parsed) -> None:
    assert_spans_hold(parsed)
    assert parsed.metadata.format == "docx"
    assert parsed.metadata.parser_name == "docx"


# --------------------------------------------------------------------------- #
# structure — the reason DOCX is worth doing before PDF
# --------------------------------------------------------------------------- #


def test_heading_levels_come_from_style_names(parsed) -> None:
    headings = [
        (b.depth, b.text.lstrip("# ").strip())
        for b in parsed.blocks
        if b.type is BlockType.HEADING
    ]
    assert headings == [
        (1, "Payments Platform Requirements"),
        (2, "Security"),
        (3, "Compliance"),
        (2, "Performance"),
    ]


def test_the_section_tree_closes_back_up_correctly(parsed) -> None:
    by_id = {b.id: b for b in parsed.blocks}
    headings = {
        b.text.lstrip("# ").strip(): b
        for b in parsed.blocks
        if b.type is BlockType.HEADING
    }
    assert by_id[headings["Compliance"].parent_id] is headings["Security"]
    # `Performance` is an h2 after an h3, so it must reattach to the h1.
    assert by_id[headings["Performance"].parent_id] is headings[
        "Payments Platform Requirements"
    ]


def test_list_nesting_survives_a_style_only_numbering_document(parsed) -> None:
    """The case that would silently flatten if only `w:numPr` were consulted."""
    items = [b for b in parsed.blocks if b.type is BlockType.LIST_ITEM]
    depths = {own_line(b): b.depth for b in items}
    assert depths["Encrypt all traffic with TLS 1.3"] == 1
    assert depths["Rotate API keys every 90 days"] == 1
    assert depths["Notify owners 7 days before rotation"] == 2


def test_consecutive_items_join_one_list_rather_than_one_each(parsed) -> None:
    top_level_lists = [
        b
        for b in parsed.blocks
        if b.type is BlockType.LIST and b.parent_id and b.depth == 0
    ]
    outer = [b for b in top_level_lists if "Encrypt all traffic" in b.text]
    assert len(outer) == 1
    assert "Rotate API keys every 90 days" in outer[0].text


# --------------------------------------------------------------------------- #
# document order — the bug python-docx's API invites
# --------------------------------------------------------------------------- #


def test_the_table_stays_under_the_heading_that_introduces_it(parsed) -> None:
    """`document.paragraphs + document.tables` would move this to the end.

    A requirements table detached from its heading loses the context that made it
    mean anything, so the body XML is walked in document order instead.
    """
    order = [b.text for b in parsed.blocks]
    compliance = next(i for i, t in enumerate(order) if t == "### Compliance")
    table = next(i for i, t in enumerate(order) if t.startswith("| Control |"))
    performance = next(i for i, t in enumerate(order) if t == "## Performance")
    assert compliance < table < performance


def test_table_cells_survive_as_both_text_and_data(parsed) -> None:
    table = next(b for b in parsed.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows[1] == ["Audit log retention", "SOC 2", "Platform"]
    assert "| Audit log retention | SOC 2 | Platform |" in parsed.text


# --------------------------------------------------------------------------- #
# metadata
# --------------------------------------------------------------------------- #


def test_core_properties_are_read_and_empty_strings_are_treated_as_absent(parsed) -> None:
    assert parsed.metadata.author == "Platform Team"
    # Word writes "" rather than omitting; a naive read reports a subject of nothing.
    assert parsed.metadata.subject is None


def test_keywords_split_on_both_separators_word_uses(parsed) -> None:
    assert parsed.metadata.keywords == ["payments", "security", "latency"]


def test_the_title_falls_back_to_the_first_heading(parsed) -> None:
    """No core-property title was set, so the opening heading is the best answer."""
    assert parsed.metadata.title == "Payments Platform Requirements"


def test_page_count_is_none_because_it_cannot_be_known(parsed) -> None:
    """A .docx has no page count without rendering it; None beats a guess."""
    assert parsed.metadata.page_count is None
    assert parsed.pages == []


def test_a_corrupt_docx_raises_a_permanent_failure() -> None:
    from parse.base import CorruptDocument
    from parse.docx import DocxParser

    with pytest.raises(CorruptDocument):
        DocxParser().parse(b"PK\x03\x04 not really a docx")
