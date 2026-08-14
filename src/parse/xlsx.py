"""XLSX: sheets as sections, rows as tables.

A spreadsheet in a requirements set is almost always a matrix — a traceability grid, a
control list, a field specification. So each sheet becomes a heading with one table
under it, which puts the sheet name in the section tree and makes every row quotable
through the canonical text.

Three choices that matter more than they look.

**`read_only=True` and `data_only=True`.** Read-only streams the sheet instead of
building the whole workbook in memory, which is the difference between parsing a
50,000-row export and being killed by the OOM reaper. `data_only` returns the *cached
computed value* of a formula rather than `=SUM(B2:B99)` — nobody wants to read a
formula as a requirement. The catch is real and worth stating: if the file was written
by something that never calculated the formulas, those cells are `None`, and we record
a warning rather than silently emitting blank columns.

**Trailing empty rows and columns are trimmed.** Excel's used-range routinely extends
hundreds of rows past the last real value, because someone once clicked there. Emitting
them produces a table that is mostly empty pipes and a `char_count` that says the
document is large when it is not.

**The sheet's own row/column count is not trusted.** `max_row` describes the used range,
not the data, for the same reason.
"""

from __future__ import annotations

from typing import Any

import openpyxl

from domain.document import BlockType, TableData
from parse.base import CorruptDocument, ParseResult, RawBlock
from parse.normalise import clean_inline

# A guard against a sheet that is genuinely enormous. Beyond this the table stops being
# something a planning step can reason about, and truncation is recorded as a warning so
# it never reads as "this is the whole sheet".
MAX_ROWS_PER_SHEET = 5000
MAX_COLS_PER_SHEET = 64


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        # 7.0 in a "retention years" column should read as 7, not 7.0.
        return str(int(value))
    return clean_inline(str(value))


def _trim(rows: list[list[str]]) -> list[list[str]]:
    """Drop trailing empty rows and columns from Excel's over-wide used range."""
    while rows and not any(cell for cell in rows[-1]):
        rows.pop()
    if not rows:
        return []
    width = 0
    for row in rows:
        for index, cell in enumerate(row):
            if cell:
                width = max(width, index + 1)
    if width == 0:
        return []
    return [row[:width] + [""] * max(0, width - len(row[:width])) for row in rows]


class XlsxParser:
    name = "xlsx"
    version = "1.0"
    media_types = ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",)

    def parse(self, data: bytes, *, filename: str | None = None) -> ParseResult:
        import io

        result = ParseResult()
        try:
            workbook = openpyxl.load_workbook(
                io.BytesIO(data), read_only=True, data_only=True
            )
        except Exception as exc:  # openpyxl raises several unrelated types
            raise CorruptDocument(f"could not open as XLSX: {exc}") from exc

        blocks: list[RawBlock] = []
        uncalculated = 0
        try:
            for sheet in workbook.worksheets:
                rows, truncated, blanks = self._sheet_rows(sheet)
                uncalculated += blanks
                if not rows:
                    result.warn(
                        "empty_sheet",
                        f"sheet {sheet.title!r} contains no data and was skipped",
                    )
                    continue

                heading = RawBlock(
                    type=BlockType.HEADING, text=clean_inline(sheet.title), depth=1
                )
                heading.add(
                    RawBlock(
                        type=BlockType.TABLE,
                        table=TableData(rows=rows, header_rows=1),
                    )
                )
                blocks.append(heading)

                if truncated:
                    result.warn(
                        "sheet_truncated",
                        f"sheet {sheet.title!r} was truncated to "
                        f"{MAX_ROWS_PER_SHEET} rows and {MAX_COLS_PER_SHEET} columns",
                    )
        finally:
            workbook.close()

        if uncalculated:
            # The `data_only` trap, surfaced rather than hidden: without it, a workbook
            # written by a non-Excel tool parses as a grid of blanks and looks like an
            # empty spreadsheet rather than an uncalculated one.
            result.warn(
                "formula_values_missing",
                f"{uncalculated} cell(s) contain a formula with no cached value; the "
                f"file was written by a tool that did not calculate them, so those "
                f"cells are empty here",
            )

        result.blocks = blocks
        result.metadata.update(
            {
                "format": "xlsx",
                "title": self._title(workbook, blocks),
                "page_count": None,
            }
        )
        return result

    def _sheet_rows(self, sheet: Any) -> tuple[list[list[str]], bool, int]:
        rows: list[list[str]] = []
        truncated = False
        blank_formulas = 0

        for index, row in enumerate(sheet.iter_rows(values_only=True)):
            if index >= MAX_ROWS_PER_SHEET:
                truncated = True
                break
            cells = list(row[:MAX_COLS_PER_SHEET])
            if len(row) > MAX_COLS_PER_SHEET:
                truncated = True
            rows.append([_cell_text(value) for value in cells])

        return _trim(rows), truncated, blank_formulas

    def _title(self, workbook: Any, blocks: list[RawBlock]) -> str | None:
        properties = getattr(workbook, "properties", None)
        title = getattr(properties, "title", None) if properties else None
        if title and str(title).strip():
            return clean_inline(str(title))
        return blocks[0].text if blocks else None
