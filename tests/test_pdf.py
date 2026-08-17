"""PDF parsing, against real PDFs generated at test time.

reportlab writes the fixtures so the assertions can state exactly what geometry produced
them — which font size, which vertical gap. That matters more here than for any other
format: everything this parser knows is inferred, so a test has to control the input
geometry to say anything meaningful about the inference.
"""

from __future__ import annotations

import pytest

from domain.document import BlockType

reportlab = pytest.importorskip("reportlab", reason="test-only PDF generator")
pytest.importorskip("pypdfium2", reason="pypdfium2 is an optional dependency")

from reportlab.pdfgen import canvas

from parse.pipeline import parse_document
from tests.conftest import assert_spans_hold, own_line

PAGE = (595, 842)


def build_pdf(
    pages: list[list[tuple[str, int, float, float]]],
    *,
    image_pages: frozenset[int] = frozenset(),
) -> bytes:
    """Draw pages of (text, font_size, x, y) so tests control the geometry exactly.

    `image_pages` (1-based) paints a raster over the page, which is what makes a fixture
    resemble a *scan* rather than a blank page. The distinction is load-bearing: thin text
    alone is a short page, thin text plus an image is a scan.
    """
    import io

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=PAGE)
    for number, page in enumerate(pages, start=1):
        if number in image_pages:
            pdf.drawImage(_scan_raster(), 40, 40, width=500, height=700)
        for text, size, x, y in page:
            pdf.setFont("Helvetica", size)
            pdf.drawString(x, y, text)
        pdf.showPage()
    pdf.save()
    return buffer.getvalue()


def _scan_raster():
    """A small grey raster standing in for a scanned page image."""
    from PIL import Image
    from reportlab.lib.utils import ImageReader

    return ImageReader(Image.new("L", (80, 110), color=210))


SPEC = [
    [
        ("Merchant Onboarding Platform", 20, 72, 760),
        ("Acme Payments needs a self-service portal so merchants can", 10, 72, 720),
        ("register and start accepting card payments without support.", 10, 72, 707),
        ("Registration", 14, 72, 660),
        ("The system must let a merchant register with an email address", 10, 72, 630),
        ("and a password.", 10, 72, 617),
        ("• Verify the email before the account becomes active", 10, 86, 580),
        ("• Reject registrations from sanctioned jurisdictions", 10, 86, 567),
    ],
    [
        ("Performance", 14, 72, 760),
        ("The portal shall authenticate users within 300ms at p99.", 10, 72, 730),
    ],
]


@pytest.fixture(scope="module")
def spec():
    doc = parse_document(build_pdf(SPEC), document_id="pdf-1", filename="spec.pdf")
    assert_spans_hold(doc)
    return doc


# --------------------------------------------------------------------------- #
# detection, the invariant, metadata
# --------------------------------------------------------------------------- #


def test_a_pdf_is_detected_and_parsed(spec) -> None:
    assert spec.metadata.format == "pdf"
    assert spec.metadata.parser_name == "pdf"
    assert spec.metadata.page_count == 2
    assert spec.metadata.has_text_layer is True


def test_spans_hold_for_a_pdf(spec) -> None:
    assert_spans_hold(spec)


# --------------------------------------------------------------------------- #
# line joining — the repair the downstream quote check depends on
# --------------------------------------------------------------------------- #


def test_a_wrapped_sentence_becomes_one_quotable_line(spec) -> None:
    """pdfium emits a newline at every rendered line break.

    Left alone, every multi-line requirement would carry a mid-sentence break and no
    model could reproduce it as a quote.
    """
    assert (
        "The system must let a merchant register with an email address and a password."
        in spec.text
    )


def test_a_paragraph_is_not_split_at_every_rendered_line(spec) -> None:
    paragraph = next(
        b
        for b in spec.blocks
        if b.type is BlockType.PARAGRAPH and b.text.startswith("Acme Payments")
    )
    assert "merchants can register and start accepting" in paragraph.text


def test_a_large_vertical_gap_does_start_a_new_paragraph(spec) -> None:
    """The gap between the intro and the next paragraph is a real boundary.

    Without gap detection everything on the page would fuse into one block; with too
    eager a threshold every line would be its own.
    """
    paragraphs = [b.text for b in spec.blocks if b.type is BlockType.PARAGRAPH]
    assert any(p.startswith("Acme Payments") for p in paragraphs)
    assert any(p.startswith("The system must let") for p in paragraphs)


# --------------------------------------------------------------------------- #
# heading inference — relative sizes, ranked into levels
# --------------------------------------------------------------------------- #


def test_headings_are_recovered_from_relative_font_size(spec) -> None:
    headings = [own_line(b) for b in spec.blocks if b.type is BlockType.HEADING]
    assert "Merchant Onboarding Platform" in headings
    assert "Registration" in headings
    assert "Performance" in headings


def test_heading_levels_come_from_ranking_the_sizes(spec) -> None:
    """20pt outranks 14pt, so the title is h1 and the sections are h2.

    Ranking rather than an absolute size table: a document set entirely in 18pt should
    still get a sensible hierarchy.
    """
    levels = {
        own_line(b): b.depth for b in spec.blocks if b.type is BlockType.HEADING
    }
    assert levels["Merchant Onboarding Platform"] == 1
    assert levels["Registration"] == 2
    assert levels["Performance"] == 2


def test_body_text_is_never_promoted_to_a_heading(spec) -> None:
    headings = [own_line(b) for b in spec.blocks if b.type is BlockType.HEADING]
    assert not any(h.startswith("Acme Payments") for h in headings)
    assert not any(h.startswith("The portal shall") for h in headings)


def test_a_uniform_document_yields_no_headings_rather_than_guesses() -> None:
    """With no size variation there is no evidence, so nothing is claimed.

    Degrading to `paragraph` is the honest failure: a mislabelled heading corrupts the
    section tree, an unlabelled one only flattens it.
    """
    flat = build_pdf(
        [
            [
                ("Requirements", 11, 72, 760),
                ("The system shall log every authentication attempt.", 11, 72, 730),
                ("Sessions expire after 30 minutes of inactivity.", 11, 72, 700),
            ]
        ]
    )
    doc = parse_document(flat, document_id="flat", filename="flat.pdf")
    assert_spans_hold(doc)
    assert not [b for b in doc.blocks if b.type is BlockType.HEADING]
    # And no content is lost by declining to structure it.
    assert "log every authentication attempt" in doc.text
    assert "Sessions expire after 30 minutes" in doc.text


def test_the_section_tree_nests_content_under_its_heading(spec) -> None:
    by_id = {b.id: b for b in spec.blocks}
    performance = next(
        b for b in spec.blocks if b.type is BlockType.HEADING and own_line(b) == "Performance"
    )
    latency = next(b for b in spec.blocks if "300ms at p99" in b.text)
    assert by_id[latency.parent_id] is performance
    assert spec.heading_path(latency.id) == [
        "Merchant Onboarding Platform",
        "Performance",
    ]


# --------------------------------------------------------------------------- #
# lists
# --------------------------------------------------------------------------- #


def test_bullet_glyphs_become_list_items(spec) -> None:
    items = [own_line(b) for b in spec.blocks if b.type is BlockType.LIST_ITEM]
    assert "Verify the email before the account becomes active" in items
    assert "Reject registrations from sanctioned jurisdictions" in items


def test_consecutive_bullets_join_one_list(spec) -> None:
    lists = [b for b in spec.blocks if b.type is BlockType.LIST]
    assert len(lists) == 1
    assert "Verify the email" in lists[0].text
    assert "Reject registrations" in lists[0].text


# --------------------------------------------------------------------------- #
# pages — the whole reason page attribution works
# --------------------------------------------------------------------------- #


def test_every_page_gets_a_span_in_the_canonical_text(spec) -> None:
    assert [p.number for p in spec.pages] == [1, 2]
    for page in spec.pages:
        assert page.char_count == page.span[1] - page.span[0]
        assert spec.text[page.span[0] : page.span[1]].strip()


def test_a_quote_resolves_to_the_page_it_was_printed_on(spec) -> None:
    """Page attribution as a lookup, on a real document."""
    from domain.locate import Locator

    result = Locator(spec).locate("authenticate users within 300ms at p99")
    assert result.found
    assert result.page == 2

    intro = Locator(spec).locate("needs a self-service portal")
    assert intro.found
    assert intro.page == 1


def test_a_page_break_starts_a_new_paragraph(spec) -> None:
    """Text either side of a page break is not one sentence."""
    assert not any(
        "without support." in b.text and "Performance" in b.text for b in spec.blocks
    )


# --------------------------------------------------------------------------- #
# the OCR decision, per page
# --------------------------------------------------------------------------- #


def test_a_scanned_page_in_a_digital_document_is_flagged_for_ocr() -> None:
    """A hybrid document: digital pages plus a scanned appendix.

    The page must be reported, not dropped — this warning is also what catches a PDF
    whose font map is broken and which would otherwise report success with no content.
    """
    hybrid = build_pdf(
        [
            [
                ("Requirements", 16, 72, 760),
                ("The service shall retry failed settlements automatically.", 10, 72, 720),
                ("Every retry shall be recorded with a timestamp and reason.", 10, 72, 707),
            ],
            [],  # a scan: an image, no text layer
        ],
        image_pages=frozenset({2}),
    )
    doc = parse_document(hybrid, document_id="hybrid", filename="hybrid.pdf")
    assert_spans_hold(doc)

    needs_ocr = [w for w in doc.warnings if w.code == "page_needs_ocr"]
    assert [w.page for w in needs_ocr] == [2]
    # Page 1 still parsed normally.
    assert "retry failed settlements automatically" in doc.text
    assert doc.metadata.page_count == 2


def test_a_legitimately_short_page_is_not_flagged_for_ocr(spec) -> None:
    """Thin text alone is not evidence of a scan.

    Page 2 of the fixture holds one heading and one sentence — 67 characters, under the
    threshold. Flagging it would cry wolf *and* route a perfectly readable page into the
    OCR lane, which costs 10-100x more to process. What distinguishes a scan is thin text
    plus something drawn.
    """
    assert "page_needs_ocr" not in [w.code for w in spec.warnings]
    assert "The portal shall authenticate users within 300ms at p99." in spec.text


def test_a_document_of_scans_says_so_loudly() -> None:
    """The silent-failure case: success with no content is worse than an error."""
    doc = parse_document(
        build_pdf([[], []], image_pages=frozenset({1, 2})),
        document_id="scan",
        filename="scan.pdf",
    )
    codes = [w.code for w in doc.warnings]
    assert "no_text_extracted" in codes
    assert codes.count("page_needs_ocr") == 2
    assert doc.metadata.has_text_layer is False
    assert doc.text == ""


def test_a_genuinely_blank_page_is_reported_as_blank_not_as_a_scan() -> None:
    """Different problems want different words.

    A blank page is usually a broken export; a scan needs OCR. Reporting both as "needs
    OCR" sends someone to configure Tesseract for a document that has nothing on it.
    """
    doc = parse_document(
        build_pdf([[("Requirements", 14, 72, 760), ("A requirement sentence here.", 10, 72, 700)], []]),
        document_id="blank",
        filename="blank.pdf",
    )
    codes = [w.code for w in doc.warnings]
    assert "blank_page" in codes
    assert "page_needs_ocr" not in codes


# --------------------------------------------------------------------------- #
# tables — the content most worth not losing
# --------------------------------------------------------------------------- #


def build_pdf_with_table() -> bytes:
    """A ruled table between two paragraphs, which is how requirement tables appear."""
    import io

    rows = [
        ["Document", "Required", "Retention"],
        ["Government ID", "Yes", "7 years"],
        ["Proof of address", "Yes", "7 years"],
    ]
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=PAGE)
    pdf.setFont("Helvetica", 16)
    pdf.drawString(72, 780, "Document Requirements")
    pdf.setFont("Helvetica", 10)
    pdf.drawString(72, 750, "The table below lists the documents a merchant must supply.")

    x0, y0, width, height = 72, 640, 150, 20
    for row_index, row in enumerate(rows):
        for col_index, cell in enumerate(row):
            pdf.drawString(
                x0 + col_index * width + 4,
                y0 + (len(rows) - 1 - row_index) * height + 6,
                cell,
            )
    for i in range(len(rows) + 1):
        pdf.line(x0, y0 + i * height, x0 + 3 * width, y0 + i * height)
    for j in range(4):
        pdf.line(x0 + j * width, y0, x0 + j * width, y0 + len(rows) * height)

    pdf.drawString(72, 600, "Retention is measured from the date of account closure.")
    pdf.showPage()
    pdf.save()
    return buffer.getvalue()


@pytest.fixture(scope="module")
def tabular():
    doc = parse_document(
        build_pdf_with_table(), document_id="pdf-table", filename="table.pdf"
    )
    assert_spans_hold(doc)
    return doc


def test_a_ruled_table_becomes_a_table_block(tabular) -> None:
    table = next(b for b in tabular.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows[0] == ["Document", "Required", "Retention"]
    assert table.table.rows[1] == ["Government ID", "Yes", "7 years"]
    assert table.page == 1


def test_the_table_is_rendered_into_the_canonical_text(tabular) -> None:
    """Otherwise a requirement stated in a table row can never be quoted."""
    assert "| Government ID | Yes | 7 years |" in tabular.text


def test_table_cells_do_not_also_appear_as_loose_paragraphs(tabular) -> None:
    """The bug this whole geometry pass exists to prevent.

    pdfium's text layer contains the cell text too, so without removing the lines a
    table's contents appear twice — once structured, once as stray paragraphs — and the
    document's character count roughly doubles across every table.
    """
    paragraphs = [b.text for b in tabular.blocks if b.type is BlockType.PARAGRAPH]
    assert not any("Government ID" in p for p in paragraphs)
    assert not any("7 years" in p for p in paragraphs)
    # And exactly once in the canonical text overall.
    assert tabular.text.count("Government ID") == 1


def test_prose_around_the_table_survives_in_order(tabular) -> None:
    """A table appended after the prose loses the heading that gave it meaning."""
    order = [b.type for b in tabular.blocks]
    types = [t for t in order if t in (BlockType.PARAGRAPH, BlockType.TABLE)]
    assert types == [BlockType.PARAGRAPH, BlockType.TABLE, BlockType.PARAGRAPH]
    assert "The table below lists the documents" in tabular.text
    assert "Retention is measured from the date" in tabular.text


def test_a_table_attaches_to_the_heading_above_it(tabular) -> None:
    table = next(b for b in tabular.blocks if b.type is BlockType.TABLE)
    assert tabular.heading_path(table.id) == ["Document Requirements"]


def test_a_cell_resolves_to_the_table_block(tabular) -> None:
    from domain.locate import Locator

    result = Locator(tabular).locate("Proof of address")
    assert result.found
    block = {b.id: b for b in tabular.blocks}[result.block_id]
    assert block.type is BlockType.TABLE
    assert result.page == 1


def test_table_extraction_can_be_turned_off(tabular) -> None:
    """The escape hatch, since this is a second pass over the file.

    With it off the cells survive as text — structure is lost, content is not.
    """
    from parse.pdf import PdfParser

    result = PdfParser(extract_tables=False).parse(build_pdf_with_table())
    assert not [b for b in result.blocks if b.type is BlockType.TABLE]
    assert any("Government ID" in b.text for b in _walk(result.blocks))


def _walk(blocks):
    for block in blocks:
        yield block
        yield from _walk(block.children)


# --------------------------------------------------------------------------- #
# metadata
# --------------------------------------------------------------------------- #


def test_producer_placeholder_metadata_is_treated_as_absent(spec) -> None:
    """reportlab writes "untitled"/"anonymous"; Word writes "".

    Both mean "no value", and accepting them means a consumer displays "untitled" to a
    human while the fallback to the document's own first heading — which is right here —
    never runs.
    """
    assert spec.metadata.author is None
    assert spec.metadata.title == "Merchant Onboarding Platform"


# --------------------------------------------------------------------------- #
# failure classification
# --------------------------------------------------------------------------- #


def test_a_corrupt_pdf_is_a_permanent_failure() -> None:
    from domain.errors import CorruptDocument
    from parse.pdf import PdfParser

    with pytest.raises(CorruptDocument) as caught:
        PdfParser().parse(b"%PDF-1.7\nnot actually a pdf at all")
    assert caught.value.transient is False
    assert caught.value.failure_class == "corrupt"
