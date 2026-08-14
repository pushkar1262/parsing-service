"""XLSX, HTML and CSV — the secondary formats, and their specific traps.

Each of these has one failure mode that is quiet rather than loud, and that is what most
of these tests are about: a spreadsheet of uncalculated formulas, a web page whose first
two hundred words are a navigation menu, a semicolon-delimited export read as one column.
All three parse "successfully" and produce content nobody can use.
"""

from __future__ import annotations

import io

import pytest

from domain.document import BlockType
from parse.pipeline import parse_document
from tests.conftest import assert_spans_hold, own_line

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


# --------------------------------------------------------------------------- #
# XLSX
# --------------------------------------------------------------------------- #

openpyxl = pytest.importorskip("openpyxl", reason="openpyxl is an optional dependency")


def build_xlsx(sheets: dict[str, list[list]]) -> bytes:
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for name, rows in sheets.items():
        sheet = workbook.create_sheet(title=name)
        for row in rows:
            sheet.append(row)
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


@pytest.fixture(scope="module")
def workbook_doc():
    data = build_xlsx(
        {
            "Controls": [
                ["Control", "Standard", "Retention years"],
                ["Audit log retention", "SOC 2", 7],
                ["Data residency", "GDPR", 10],
            ],
            "Fields": [
                ["Field", "Required"],
                ["merchant_id", True],
                ["trading_name", False],
            ],
        }
    )
    doc = parse_document(data, document_id="xlsx-1", filename="controls.xlsx")
    assert_spans_hold(doc)
    return doc


def test_an_xlsx_is_detected_from_its_archive_contents() -> None:
    from parse.detect import detect

    assert detect(build_xlsx({"S": [["a"]]}), filename="x.bin") == XLSX_MEDIA_TYPE


def test_each_sheet_becomes_a_section_with_a_table(workbook_doc) -> None:
    """The sheet name belongs in the section tree, or a cell loses its context."""
    headings = [own_line(b) for b in workbook_doc.blocks if b.type is BlockType.HEADING]
    assert headings == ["Controls", "Fields"]
    assert sum(1 for b in workbook_doc.blocks if b.type is BlockType.TABLE) == 2


def test_a_cell_is_quotable_through_the_canonical_text(workbook_doc) -> None:
    assert "| Audit log retention | SOC 2 | 7 |" in workbook_doc.text


def test_a_cell_resolves_to_its_sheet(workbook_doc) -> None:
    from domain.locate import Locator

    result = Locator(workbook_doc).locate("Data residency")
    assert result.found
    assert workbook_doc.heading_path(result.block_id) == ["Controls"]


def test_whole_number_floats_read_as_integers(workbook_doc) -> None:
    """Excel stores 7 as 7.0; "7.0 years" in a retention column reads as an error."""
    assert "| 7 |" in workbook_doc.text
    assert "7.0" not in workbook_doc.text


def test_booleans_are_rendered_as_words(workbook_doc) -> None:
    assert "| merchant_id | true |" in workbook_doc.text


def test_excels_over_wide_used_range_is_trimmed() -> None:
    """A used range extending past the data is normal and must not become empty pipes."""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Sparse"
    sheet["A1"] = "Header"
    sheet["A2"] = "Value"
    sheet["H40"] = None  # touched, so the used range grows to H40
    buffer = io.BytesIO()
    workbook.save(buffer)

    doc = parse_document(buffer.getvalue(), document_id="x", filename="s.xlsx")
    assert_spans_hold(doc)
    table = next(b for b in doc.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows == [["Header"], ["Value"]]


def test_an_empty_sheet_is_reported_rather_than_silently_skipped() -> None:
    data = build_xlsx({"Real": [["a", "b"], ["1", "2"]], "Blank": []})
    doc = parse_document(data, document_id="x", filename="s.xlsx")
    assert "empty_sheet" in [w.code for w in doc.warnings]


def test_a_corrupt_xlsx_is_a_permanent_failure() -> None:
    from domain.errors import CorruptDocument
    from parse.xlsx import XlsxParser

    with pytest.raises(CorruptDocument) as caught:
        XlsxParser().parse(b"PK\x03\x04 not a workbook")
    assert caught.value.failure_class == "corrupt"


# --------------------------------------------------------------------------- #
# HTML
# --------------------------------------------------------------------------- #

PAGE = b"""<!doctype html>
<html><head>
  <title>Merchant Onboarding</title>
  <style>.nav { color: red; font-weight: bold; }</style>
  <script>window.analytics = {track: function(){}};</script>
</head>
<body>
  <nav><a href="/">Home</a> <a href="/docs">Docs</a></nav>
  <h1>Merchant Onboarding</h1>
  <p>Merchants must register before accepting payments.</p>
  <h2>Registration</h2>
  <ul>
    <li>Verify the email address</li>
    <li>Reject sanctioned jurisdictions</li>
  </ul>
  <table>
    <tr><th>Document</th><th>Required</th></tr>
    <tr><td>Government ID</td><td>Yes</td></tr>
  </table>
  <footer>Copyright Acme 2026. All rights reserved.</footer>
</body></html>
"""


@pytest.fixture(scope="module")
def page_doc():
    doc = parse_document(PAGE, document_id="html-1", filename="page.html")
    assert_spans_hold(doc)
    return doc


def test_script_and_style_text_never_reaches_the_content(page_doc) -> None:
    """Their text is code. Left in, a CSS rule gets quoted as a requirement."""
    assert "font-weight" not in page_doc.text
    assert "window.analytics" not in page_doc.text


def test_navigation_and_footer_chrome_is_dropped(page_doc) -> None:
    """A saved page is mostly furniture, and a planning step will happily read it."""
    assert "Docs" not in page_doc.text
    assert "All rights reserved" not in page_doc.text


def test_heading_structure_comes_from_the_tags(page_doc) -> None:
    levels = {own_line(b): b.depth for b in page_doc.blocks if b.type is BlockType.HEADING}
    assert levels == {"Merchant Onboarding": 1, "Registration": 2}


def test_lists_and_tables_survive(page_doc) -> None:
    items = [own_line(b) for b in page_doc.blocks if b.type is BlockType.LIST_ITEM]
    assert items == ["Verify the email address", "Reject sanctioned jurisdictions"]
    assert "| Government ID | Yes |" in page_doc.text


def test_the_title_element_wins_over_the_first_heading(page_doc) -> None:
    assert page_doc.metadata.title == "Merchant Onboarding"
    assert page_doc.metadata.format == "html"


def test_two_documents_do_not_leak_structure_into_each_other() -> None:
    """The parser holds a heading stack; it must not be shared between documents."""
    first = parse_document(PAGE, document_id="a", filename="a.html")
    second = parse_document(
        b"<html><body><p>Standalone paragraph.</p></body></html>",
        document_id="b",
        filename="b.html",
    )
    assert_spans_hold(second)
    assert second.blocks[0].type is BlockType.PARAGRAPH
    assert second.blocks[0].parent_id is None
    assert first.metadata.title == "Merchant Onboarding"


# --------------------------------------------------------------------------- #
# CSV
# --------------------------------------------------------------------------- #


def test_a_comma_delimited_export_becomes_a_table() -> None:
    doc = parse_document(
        b"Control,Standard,Owner\nAudit log retention,SOC 2,Platform\n",
        document_id="csv-1",
        filename="controls.csv",
    )
    assert_spans_hold(doc)
    table = next(b for b in doc.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows[1] == ["Audit log retention", "SOC 2", "Platform"]


def test_a_semicolon_delimited_export_is_not_read_as_one_column() -> None:
    """The European export. Assuming a comma fails here, and fails quietly."""
    doc = parse_document(
        b"Control;Standard;Owner\nAudit log retention;SOC 2;Platform\n",
        document_id="csv-2",
        filename="controls.csv",
    )
    assert_spans_hold(doc)
    table = next(b for b in doc.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows[0] == ["Control", "Standard", "Owner"]
    assert "single_column" not in [w.code for w in doc.warnings]


def test_a_tab_delimited_export_works_too() -> None:
    doc = parse_document(
        b"Field\tRequired\nmerchant_id\tYes\n", document_id="c", filename="f.csv"
    )
    table = next(b for b in doc.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows[1] == ["merchant_id", "Yes"]


def test_ragged_rows_are_padded_rather_than_dropped() -> None:
    doc = parse_document(
        b"a,b,c\n1,2\n3,4,5\n", document_id="c", filename="f.csv"
    )
    assert_spans_hold(doc)
    table = next(b for b in doc.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows[1] == ["1", "2", ""]


def test_a_file_with_no_rows_is_a_permanent_failure() -> None:
    from domain.errors import CorruptDocument
    from parse.csv import CsvParser

    with pytest.raises(CorruptDocument):
        CsvParser().parse(b"   \n\n  \n")
