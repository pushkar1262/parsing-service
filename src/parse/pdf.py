"""PDF: the format that matters most and tells you least.

A .docx states its structure — "Heading 2" is a heading because it says so. A PDF
states positions. There is no heading, no paragraph and no list in the file; there are
glyphs at coordinates, and everything above that is inference. So this parser is built
to be *honestly uncertain*: it recovers what the geometry supports and degrades to
`paragraph` rather than guessing, because a mislabelled heading corrupts the section
tree while an unlabelled one merely flattens it.

Four decisions worth knowing about.

**Lines come from pdfium's own segmentation; sizes come from the declared font size.**
Walking the character stream in index order and splitting on the newline characters
pdfium inserts gives correct reading order for free — pdfium is good at this, and
reimplementing it from coordinates is a well-known way to get columns wrong. Grouping
glyphs by their `top` coordinate, which looks like the obvious approach, does not work at
all: ascenders and descenders differ per glyph, so "Merchant" alone scatters across three
different tops.

Size is read with `FPDFText_GetFontSize` rather than measured from the glyph box, and the
difference is not cosmetic. Measured by glyph height, "Registration" and "Performance"
set in the same 14pt come out as 8.59 and 7.73 — a 10% gap caused only by the descender
in "g". That made two sibling headings rank as two different levels and quietly nested
one section inside the other. The declared size reports 14.0 for both.

**Paragraphs are found by vertical gaps, not blank lines.** A PDF has no blank lines —
paragraph separation is whitespace between baselines. So the typical line gap is
measured per document and a materially larger gap ends the paragraph. Without this every
rendered line becomes its own paragraph, and a requirement spanning two lines can never
be quoted as one sentence.

**Headings are relative, and levels come from ranking.** A document set in 18pt has no
12pt headings, so size is compared against the document's own body size rather than an
absolute table. Distinct larger sizes are then ranked, so a document with one heading
size gets h1s throughout and one with three gets h1/h2/h3 — instead of a fixed
size→level mapping that is wrong for every document not built from the same template.

**The OCR decision is per page.** Real documents are hybrids: a digital specification
with three scanned appendix pages. A document-level flag either OCRs everything at a
hundred times the cost or silently loses the appendix.
"""

from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_raw

from domain.document import BlockType, PageSource, TableData
from parse.base import CorruptDocument, EncryptedDocument, PageInfo, ParseResult, RawBlock
from parse.normalise import clean_inline, join_wrapped, normalise

# A page yielding fewer than this many characters is a scan, a cover page, or a broken
# font map. All three deserve a closer look, and only a human or OCR can tell them apart.
MIN_CHARS_PER_PAGE = 80

# How much larger than the body a line must be to read as a heading. Conservative on
# purpose: bold body text and figure captions routinely sit a few percent above the
# median, and promoting those is the failure that wrecks the section tree.
HEADING_SIZE_RATIO = 1.18

# A heading is short. Anything longer is a paragraph that happens to be set large.
MAX_HEADING_CHARS = 120

# Font sizes within this fraction of each other are the same tier. Small, because
# declared sizes are usually exact — this only absorbs scaled-text-matrix artifacts
# like 11.9994 where the author wrote 12.
SIZE_TOLERANCE = 0.04

# Multiple of the typical line gap that ends a paragraph.
PARAGRAPH_GAP_RATIO = 1.6

# Glyphs that begin a list item. The dash forms require a trailing space so a
# hyphenated word starting a line is not mistaken for a bullet.
_BULLET_GLYPHS = ("•", "▪", "◦", "‣", "⁃", "·", "", "")
_BULLET_DASHES = ("- ", "– ", "— ", "* ")

# Values PDF producers write to mean "no value". Treating them as real metadata is the
# same bug as trusting Word's empty strings, but louder: a consumer showing `title` to a
# human would display "untitled", and the fallback to the document's own first heading —
# which is usually correct — never gets a chance to run.
_PLACEHOLDER_METADATA = frozenset(
    {
        "untitled",
        "unknown",
        "anonymous",
        "none",
        "null",
        "n/a",
        "na",
        "-",
        "--",
        "()",
        "unspecified",
        "no title",
        "default",
        "microsoft word",
        "document1",
    }
)


@dataclass
class _Line:
    text: str
    size: float
    indent: float
    right: float
    top: float
    bottom: float
    page: int


@dataclass
class _TableRegion:
    """A ruled table, and the area of the page it occupies.

    The geometry is not bookkeeping — it is what lets the lines that make up the table
    be removed from the text flow. Without it the table's contents appear twice: once as
    a table, once as a run of stray paragraphs.
    """

    rows: list[list[str]]
    top: float
    bottom: float
    left: float
    right: float
    page: int

    def contains(self, line: _Line) -> bool:
        centre = (line.top + line.bottom) / 2
        vertical = self.bottom - 1 <= centre <= self.top + 1
        horizontal = line.right >= self.left - 2 and line.indent <= self.right + 2
        return vertical and horizontal


_Element = _Line | _TableRegion


@dataclass
class _Extraction:
    # Lines and tables interleaved in reading order, so a table stays with the heading
    # that introduces it rather than being appended after the prose.
    elements: list[_Element] = field(default_factory=list)
    page_meta: dict[int, PageInfo] = field(default_factory=dict)
    ocr_candidates: list[int] = field(default_factory=list)
    page_count: int = 0

    @property
    def lines(self) -> list[_Line]:
        return [item for item in self.elements if isinstance(item, _Line)]


class PdfParser:
    name = "pdf"
    version = "1.0"
    media_types = ("application/pdf",)

    def __init__(self, *, extract_tables: bool = True) -> None:
        # Table extraction is a second pass over the file with a different library and it
        # is the slow part of parsing a large PDF. On by default because a requirements
        # table read as loose lines is a silent loss of exactly the content that matters
        # most; off for a deployment that has measured the cost and decided against it.
        self.extract_tables = extract_tables

    def parse(self, data: bytes, *, filename: str | None = None) -> ParseResult:
        result = ParseResult()
        document = self._open(data)
        try:
            extraction = self._extract(document, result, data)
            metadata = self._document_metadata(document)
        finally:
            document.close()

        blocks = self._structure(extraction)
        result.blocks = blocks
        result.page_meta = extraction.page_meta

        if not extraction.lines:
            result.warn(
                "no_text_extracted",
                "no text could be extracted from any page; the document is almost "
                "certainly a scan and needs OCR before it carries any content",
            )

        metadata.update(
            {
                "format": "pdf",
                "page_count": extraction.page_count,
                "has_text_layer": bool(extraction.lines),
                "ocr_applied": False,
                "ocr_page_count": 0,
            }
        )
        if "title" not in metadata:
            first_heading = next(
                (b.text for b in blocks if b.type is BlockType.HEADING), None
            )
            if first_heading:
                metadata["title"] = first_heading
        result.metadata.update(metadata)
        return result

    # --------------------------------------------------------------------- open

    def _open(self, data: bytes) -> Any:
        try:
            return pdfium.PdfDocument(data, autoclose=False)
        except pdfium.PdfiumError as exc:
            message = str(exc).lower()
            if "password" in message or "encrypt" in message:
                raise EncryptedDocument(
                    "the PDF is password-protected; upload an unlocked copy"
                ) from exc
            raise CorruptDocument(f"could not open as PDF: {exc}") from exc
        except Exception as exc:  # pdfium raises bare exceptions for some malformed input
            raise CorruptDocument(f"could not open as PDF: {exc}") from exc

    # --------------------------------------------------------------- extraction

    def _extract(self, document: Any, result: ParseResult, data: bytes) -> _Extraction:
        extraction = _Extraction(page_count=len(document))
        if extraction.page_count == 0:
            raise CorruptDocument("the PDF contains no pages")

        tables = self._tables(data, result) if self.extract_tables else {}

        for index in range(extraction.page_count):
            number = index + 1
            page = document[index]
            try:
                textpage = page.get_textpage()
            except pdfium.PdfiumError as exc:
                result.warn("page_unreadable", f"page {number}: {exc}", page=number)
                page.close()
                continue

            try:
                lines = self._page_lines(textpage, number)
                has_images = self._has_images(page)
            finally:
                textpage.close()
                page.close()

            extraction.page_meta[number] = PageInfo(source=PageSource.TEXT_LAYER)
            characters = sum(len(line.text) for line in lines)

            # Thin text alone is not evidence of a scan. A section divider, a title page
            # or a page carrying one requirement is legitimately short, and flagging it
            # would both cry wolf and route a perfectly good page into the OCR lane,
            # which costs 10-100x more to process. What distinguishes a scan is thin text
            # *plus something drawn* — so the image check is what makes the signal usable.
            if characters < MIN_CHARS_PER_PAGE and has_images:
                extraction.ocr_candidates.append(number)
                result.warn(
                    "page_needs_ocr",
                    f"page {number} yielded {characters} characters but contains "
                    f"images; it is probably a scan, or has a text layer that cannot "
                    f"be read",
                    page=number,
                )
            elif not characters and not has_images:
                # Genuinely blank. Worth recording because a run of blank pages usually
                # means a broken export rather than an intentionally empty document.
                result.warn("blank_page", f"page {number} contains nothing", page=number)

            extraction.elements.extend(self._merge(lines, tables.get(number, [])))

        if extraction.ocr_candidates:
            result.metadata["ocr_candidate_pages"] = extraction.ocr_candidates
        return extraction

    def _merge(
        self, lines: list[_Line], tables: list[_TableRegion]
    ) -> list[_Element]:
        """Drop the lines a table is made of, then interleave by position.

        Both halves matter. Keeping the lines would emit every cell twice — once inside
        the table, once as loose paragraphs — and appending tables at the end of the page
        would detach a requirements table from the heading that gives it meaning.
        Ordering by descending vertical position puts everything back in reading order.
        """
        if not tables:
            return list(lines)

        kept: list[_Element] = [
            line for line in lines if not any(table.contains(line) for table in tables)
        ]
        kept.extend(tables)
        kept.sort(key=lambda item: -item.top)
        return kept

    def _tables(
        self, data: bytes, result: ParseResult
    ) -> dict[int, list[_TableRegion]]:
        """Ruled tables per page, via pdfplumber.

        Only *ruled* tables are detected, which is pdfplumber's default and the
        conservative choice: whitespace-aligned columns are genuinely ambiguous, and a
        wrongly-detected table swallows surrounding prose into cells where a planning
        step can no longer read it as sentences.

        This is a second pass over the file and it is the slow part of parsing a large
        PDF, which is why it is a flag rather than unconditional.
        """
        try:
            import pdfplumber
        except ImportError:  # pragma: no cover - environment dependent
            result.warn(
                "tables_not_extracted",
                "pdfplumber is not installed, so any tables in this document were read "
                "as loose lines; install parsing-service[pdf]",
            )
            return {}

        import io

        found: dict[int, list[_TableRegion]] = {}
        try:
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                for index, page in enumerate(pdf.pages):
                    number = index + 1
                    try:
                        regions = self._page_tables(page, number)
                    except Exception as exc:  # noqa: BLE001 - one bad page, not a bad document
                        result.warn(
                            "table_extraction_failed",
                            f"page {number}: {exc}; its tables were read as loose lines",
                            page=number,
                        )
                        continue
                    finally:
                        page.flush_cache()
                    if regions:
                        found[number] = regions
        except Exception as exc:  # noqa: BLE001 - text extraction already succeeded
            # Never fail the document over tables: the text layer is the primary output
            # and losing structure is much better than losing the content entirely.
            result.warn(
                "table_extraction_failed",
                f"table extraction failed for the whole document ({exc}); tables were "
                f"read as loose lines",
            )
            return {}
        return found

    def _page_tables(self, page: Any, number: int) -> list[_TableRegion]:
        height = page.height
        regions: list[_TableRegion] = []
        for table in page.find_tables():
            rows = [
                [clean_inline(cell or "") for cell in row] for row in table.extract()
            ]
            rows = [row for row in rows if any(row)]
            if not rows:
                continue
            left, top, right, bottom = table.bbox
            # pdfplumber measures from the top of the page, pdfium from the bottom.
            regions.append(
                _TableRegion(
                    rows=rows,
                    top=height - top,
                    bottom=height - bottom,
                    left=left,
                    right=right,
                    page=number,
                )
            )
        return regions

    def _has_images(self, page: Any) -> bool:
        """Whether anything is drawn on the page besides text.

        On failure this returns True rather than False. The asymmetry is deliberate: a
        false positive costs one spurious warning, while a false negative silently drops
        a scanned page of requirements out of the document with nothing to indicate it
        ever existed.
        """
        try:
            for _ in page.get_objects(
                filter=(pdfium_raw.FPDF_PAGEOBJ_IMAGE,), max_depth=4
            ):
                return True
        except Exception:  # noqa: BLE001 - raw C traversal; unknown means "assume a scan"
            return True
        return False

    def _page_lines(self, textpage: Any, number: int) -> list[_Line]:
        """Reconstruct lines from the character stream.

        Index order is reading order, and pdfium emits `\\r\\n` between lines, so
        splitting on those gives its line segmentation with none of the column-ordering
        risk of rebuilding lines from coordinates. Glyph boxes are used only for size
        and indentation, which is what they are reliable for.
        """
        try:
            count = textpage.count_chars()
        except pdfium.PdfiumError:
            return []
        if count <= 0:
            return []

        lines: list[_Line] = []
        # char, left, bottom, right, top, font_size
        current: list[tuple[str, float, float, float, float, float]] = []

        def flush() -> None:
            if not current:
                return
            text = "".join(char for char, _, _, _, _, _ in current).strip()
            if text:
                sizes = [size for _, _, _, _, _, size in current if size > 0]
                lines.append(
                    _Line(
                        text=text,
                        size=statistics.median(sizes) if sizes else 0.0,
                        indent=min(left for _, left, _, _, _, _ in current),
                        right=max(right for _, _, _, right, _, _ in current),
                        top=max(top for _, _, _, _, top, _ in current),
                        bottom=min(bottom for _, _, bottom, _, _, _ in current),
                        page=number,
                    )
                )
            current.clear()

        for position in range(count):
            try:
                char = textpage.get_text_range(position, 1)
            except pdfium.PdfiumError:
                continue
            if char in ("\n", "\r"):
                flush()
                continue
            try:
                left, bottom, right, top = textpage.get_charbox(position)
            except (pdfium.PdfiumError, TypeError, IndexError):
                continue
            current.append(
                (char, left, bottom, right, top, _font_size(textpage, position))
            )
        flush()
        return lines

    # ---------------------------------------------------------------- structure

    def _structure(self, extraction: _Extraction) -> list[RawBlock]:
        """Group lines into blocks, and blocks under headings.

        Two passes, because deciding what counts as a heading needs the whole document's
        size distribution and the paragraph gap threshold needs its typical line
        spacing. Neither can be known while reading the first page, which is why a
        single-pass version mislabels exactly the part of the document people read
        first.
        """
        lines = extraction.lines
        if not lines:
            return []

        body_size = self._body_size(lines)
        heading_sizes = self._heading_sizes(lines, body_size)
        gap_threshold = self._gap_threshold(lines)

        root: list[RawBlock] = []
        heading_stack: list[tuple[int, RawBlock]] = []
        paragraph: list[_Line] = []

        def sink() -> list[RawBlock]:
            return heading_stack[-1][1].children if heading_stack else root

        def flush() -> None:
            if not paragraph:
                return
            text = join_wrapped("\n".join(line.text for line in paragraph))
            if text:
                sink().append(
                    RawBlock(type=BlockType.PARAGRAPH, text=text, page=paragraph[0].page)
                )
            paragraph.clear()

        previous: _Line | None = None
        for element in extraction.elements:
            if isinstance(element, _TableRegion):
                # A table ends whatever paragraph preceded it, and attaches to the open
                # heading like any other block.
                flush()
                sink().append(
                    RawBlock(
                        type=BlockType.TABLE,
                        table=TableData(rows=element.rows, header_rows=1),
                        page=element.page,
                    )
                )
                previous = None
                continue

            line = element
            if previous is not None and self._breaks_paragraph(
                previous, line, gap_threshold
            ):
                flush()

            level = self._heading_level(line, body_size, heading_sizes)
            bullet = _bullet_content(line.text)

            if level is not None and bullet is None:
                flush()
                heading = RawBlock(
                    type=BlockType.HEADING,
                    text=clean_inline(line.text),
                    depth=level,
                    page=line.page,
                )
                while heading_stack and heading_stack[-1][0] >= level:
                    heading_stack.pop()
                sink().append(heading)
                heading_stack.append((level, heading))
            elif bullet is not None:
                flush()
                self._add_bullet(bullet, line, sink())
            else:
                paragraph.append(line)
            previous = line

        flush()
        return root

    def _breaks_paragraph(
        self, previous: _Line, current: _Line, gap_threshold: float
    ) -> bool:
        if current.page != previous.page:
            return True
        gap = previous.bottom - current.top
        return gap > gap_threshold

    def _gap_threshold(self, lines: list[_Line]) -> float:
        """The vertical gap that separates paragraphs rather than wrapped lines.

        Derived from the document's own line spacing: a single-spaced document and a
        double-spaced one need different thresholds, and a fixed number in points is
        wrong for both.
        """
        gaps = [
            second.bottom - first.top
            for first, second in pairwise(lines)
            if second.page == first.page
        ]
        positive = [gap for gap in gaps if gap > 0]
        if not positive:
            return float("inf")  # single-line pages: never break on gap
        return statistics.median(positive) * PARAGRAPH_GAP_RATIO + 0.5

    def _body_size(self, lines: list[_Line]) -> float:
        """The most common line size, weighted by how much text is set in it.

        Weighted by characters rather than lines: a title page contributes many short
        large-text lines, and an unweighted mode would conclude the body text is 24pt
        and then find no headings anywhere in the document.
        """
        weights: Counter[float] = Counter()
        for line in lines:
            if line.size > 0:
                weights[round(line.size, 1)] += len(line.text)
        return weights.most_common(1)[0][0] if weights else 0.0

    def _heading_sizes(self, lines: list[_Line], body_size: float) -> list[float]:
        """Distinct heading sizes, largest first, so level equals rank.

        Nearly-equal sizes are clustered rather than compared exactly. Declared font
        sizes are usually clean numbers, but a scaled text matrix produces 11.9994
        where the author wrote 12, and treating that as its own heading level would
        invent a whole extra tier of hierarchy from a rounding artifact.
        """
        if body_size <= 0:
            return []
        candidates = sorted(
            {
                round(line.size, 2)
                for line in lines
                if line.size >= body_size * HEADING_SIZE_RATIO
                and len(line.text) <= MAX_HEADING_CHARS
                and _bullet_content(line.text) is None
            },
            reverse=True,
        )
        clusters: list[float] = []
        for size in candidates:
            if clusters and abs(clusters[-1] - size) <= clusters[-1] * SIZE_TOLERANCE:
                continue  # same tier as the cluster already recorded
            clusters.append(size)
        return clusters[:6]

    def _heading_level(
        self, line: _Line, body_size: float, heading_sizes: list[float]
    ) -> int | None:
        if not heading_sizes or len(line.text) > MAX_HEADING_CHARS:
            return None
        if line.size < body_size * HEADING_SIZE_RATIO:
            return None
        for rank, representative in enumerate(heading_sizes, start=1):
            if abs(line.size - representative) <= representative * SIZE_TOLERANCE:
                return rank
        return None

    def _add_bullet(self, content: str, line: _Line, sink: list[RawBlock]) -> None:
        """Attach to the open list, or start one.

        Consecutive bullets must join a single list; otherwise the structure claims four
        unrelated lists where the document shows one list of four things.
        """
        if sink and sink[-1].type is BlockType.LIST:
            container = sink[-1]
        else:
            container = RawBlock(type=BlockType.LIST, page=line.page)
            sink.append(container)
        container.add(
            RawBlock(
                type=BlockType.LIST_ITEM,
                text=clean_inline(content),
                depth=1,
                page=line.page,
            )
        )

    # ----------------------------------------------------------------- metadata

    def _document_metadata(self, document: Any) -> dict[str, Any]:
        meta: dict[str, Any] = {}
        for source, target in (
            ("Title", "title"),
            ("Author", "author"),
            ("Subject", "subject"),
            ("Producer", "producer"),
        ):
            try:
                value = document.get_metadata_value(source)
            except (pdfium.PdfiumError, AttributeError, KeyError):
                continue
            cleaned = normalise(str(value or "")).strip()
            if cleaned and cleaned.lower() not in _PLACEHOLDER_METADATA:
                meta[target] = cleaned
        return meta


def _font_size(textpage: Any, position: int) -> float:
    """The declared font size of one character.

    Not measured from the glyph box, which was the first attempt and is wrong: glyph
    height depends on which letters a line happens to contain. "Registration" and
    "Performance" set in the same 14pt measure 8.59 and 7.73 by glyph height, because
    one has a descender and the other does not — a 10% difference that made two
    identical heading levels rank as two different ones, quietly nesting a section
    under its sibling.

    `FPDFText_GetFontSize` reports what the PDF actually declares, so both read 14.0.
    """
    try:
        return float(pdfium_raw.FPDFText_GetFontSize(textpage, position))
    except Exception:  # noqa: BLE001 - a raw C call; any failure means "unknown"
        return 0.0


def _bullet_content(text: str) -> str | None:
    """The content of a list-item line, or None if this is not one."""
    for glyph in _BULLET_GLYPHS:
        if glyph and text.startswith(glyph):
            return text[len(glyph) :].strip() or None
    for dash in _BULLET_DASHES:
        if text.startswith(dash):
            return text[len(dash) :].strip() or None
    return None
