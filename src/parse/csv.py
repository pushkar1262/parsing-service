"""CSV and TSV: one table, with the delimiter worked out rather than assumed.

Cheap to support and worth supporting, because exported requirement matrices and field
specifications arrive this way constantly.

The only real decision is the delimiter. Assuming a comma fails on European exports,
where the comma is the decimal separator and the delimiter is a semicolon — and it fails
*quietly*, producing a single-column table whose one column contains every field. So the
delimiter is sniffed from the header, and the result is a table rather than a wall of
text.
"""

from __future__ import annotations

import csv as _csv
import io

from domain.document import BlockType, TableData
from parse.base import CorruptDocument, ParseResult, RawBlock
from parse.normalise import clean_inline, decode

MAX_ROWS = 5000
MAX_COLS = 64

_CANDIDATE_DELIMITERS = ",;\t|"


def _sniff(sample: str) -> str:
    """Work out the delimiter, falling back to the most common candidate.

    `csv.Sniffer` is tried first and is usually right, but it raises on short or unusual
    files. The fallback counts candidates in the first line, which handles the
    semicolon-delimited European export that is the case worth getting right.
    """
    try:
        return _csv.Sniffer().sniff(sample, delimiters=_CANDIDATE_DELIMITERS).delimiter
    except _csv.Error:
        first = sample.split("\n", 1)[0]
        counts = {delimiter: first.count(delimiter) for delimiter in _CANDIDATE_DELIMITERS}
        best = max(counts, key=lambda key: counts[key])
        return best if counts[best] else ","


class CsvParser:
    name = "csv"
    version = "1.0"
    media_types = ("text/csv", "text/tab-separated-values")

    def parse(self, data: bytes, *, filename: str | None = None) -> ParseResult:
        text, encoding_warning = decode(data)
        result = ParseResult()
        if encoding_warning:
            result.warn(encoding_warning, "character encoding had to be guessed")

        if not text.strip():
            raise CorruptDocument("the file contains no rows")

        delimiter = _sniff(text[:8192])
        rows: list[list[str]] = []
        truncated = False
        try:
            for index, row in enumerate(_csv.reader(io.StringIO(text), delimiter=delimiter)):
                if index >= MAX_ROWS:
                    truncated = True
                    break
                if len(row) > MAX_COLS:
                    truncated = True
                rows.append([clean_inline(cell) for cell in row[:MAX_COLS]])
        except _csv.Error as exc:
            raise CorruptDocument(f"malformed CSV: {exc}") from exc

        rows = [row for row in rows if any(row)]
        if not rows:
            raise CorruptDocument("the file contains no non-empty rows")

        width = max(len(row) for row in rows)
        rows = [row + [""] * (width - len(row)) for row in rows]

        if truncated:
            result.warn(
                "rows_truncated",
                f"only the first {MAX_ROWS} rows and {MAX_COLS} columns were read",
            )
        if width == 1 and len(rows) > 1:
            # The signal that delimiter detection was wrong. Silently emitting a
            # one-column table is how a requirements matrix becomes an unusable blob.
            result.warn(
                "single_column",
                f"every row parsed as one column using delimiter {delimiter!r}; the "
                f"file may use a delimiter this parser did not recognise",
            )

        result.blocks = [
            RawBlock(type=BlockType.TABLE, table=TableData(rows=rows, header_rows=1))
        ]
        result.metadata.update({"format": "csv", "delimiter": delimiter})
        return result
