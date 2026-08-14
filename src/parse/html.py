"""HTML: structure is stated, but so is a great deal of noise.

HTML is the easiest format to get the *tags* right in and the easiest to get the
*content* wrong in. A saved requirements page is 10% document and 90% navigation,
cookie banner, sidebar and footer. Extracting every text node produces a document whose
first two hundred words are a menu, and a planning step reading that will faithfully
turn "Skip to main content" into a requirement.

So this parser does two things in order: drop the elements that are never content, then
read structure from what remains. `<script>` and `<style>` are the obvious ones and
their *text* is code — leaving it in is how a CSS rule ends up quoted as a requirement.

Built on the standard library's `HTMLParser` rather than a dependency. lxml or
selectolax would be faster and more forgiving of genuinely broken markup, but HTML is a
secondary format here — documents arrive as PDF, DOCX and text — and a stdlib
implementation keeps it dependency-free. If HTML becomes a primary input, swapping the
tokeniser is a change to this module alone.
"""

from __future__ import annotations

from html.parser import HTMLParser as _StdHTMLParser

from domain.document import BlockType, TableData
from parse.base import ParseResult, RawBlock
from parse.normalise import clean_inline, decode

# Elements whose text is never document content.
_DISCARD = frozenset(
    {
        "script",
        "style",
        "noscript",
        "template",
        "svg",
        "head",
        "nav",
        "aside",
        "footer",
        "form",
        "button",
        "iframe",
    }
)

_HEADINGS = {f"h{level}": level for level in range(1, 7)}
_BLOCK_ENDING = frozenset(
    {"p", "div", "section", "article", "br", "li", "tr", "blockquote", "pre"}
)
# Void elements never receive an end tag, so a naive stack would never pop them.
_VOID = frozenset(
    {"br", "img", "hr", "input", "meta", "link", "source", "col", "area", "base"}
)


class _Collector(_StdHTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[RawBlock] = []
        self.title: str | None = None
        self._discard_depth = 0
        self._stack: list[str] = []
        self._buffer: list[str] = []
        self._heading_level: int | None = None
        self._in_title = False
        self._list_stack: list[RawBlock] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._pre = False
        # Per-instance, not class-level: a shared mutable default would leak the
        # heading stack from one parsed document into the next.
        self._open_headings: list[RawBlock] = []

    # ------------------------------------------------------------------ tags

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag in _DISCARD:
            self._discard_depth += 1
            return
        if self._discard_depth:
            return
        if tag not in _VOID:
            self._stack.append(tag)

        if tag == "title":
            self._in_title = True
        elif tag in _HEADINGS:
            self._flush()
            self._heading_level = _HEADINGS[tag]
        elif tag in ("ul", "ol"):
            self._flush()
            container = RawBlock(
                type=BlockType.LIST, attrs={"ordered": tag == "ol"}
            )
            self._attach(container)
            self._list_stack.append(container)
        elif tag == "li":
            self._flush()
        elif tag == "table":
            self._flush()
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag == "pre":
            self._flush()
            self._pre = True
        elif tag == "br":
            self._buffer.append(" ")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _DISCARD:
            self._discard_depth = max(0, self._discard_depth - 1)
            return
        if self._discard_depth:
            return

        if tag == "title":
            self._in_title = False
            self.title = clean_inline("".join(self._buffer)) or None
            self._buffer.clear()
        elif tag in _HEADINGS:
            text = clean_inline("".join(self._buffer))
            self._buffer.clear()
            if text:
                self._attach(
                    RawBlock(
                        type=BlockType.HEADING, text=text, depth=self._heading_level or 1
                    )
                )
            self._heading_level = None
        elif tag == "li":
            text = clean_inline("".join(self._buffer))
            self._buffer.clear()
            if text and self._list_stack:
                self._list_stack[-1].add(
                    RawBlock(
                        type=BlockType.LIST_ITEM,
                        text=text,
                        depth=len(self._list_stack),
                        attrs=dict(self._list_stack[-1].attrs),
                    )
                )
        elif tag in ("ul", "ol"):
            self._buffer.clear()
            if self._list_stack:
                self._list_stack.pop()
        elif tag in ("td", "th") and self._row is not None:
            self._row.append(clean_inline("".join(self._buffer)))
            self._buffer.clear()
        elif tag == "tr" and self._table is not None and self._row is not None:
            if any(self._row):
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            if self._table:
                self._attach(
                    RawBlock(
                        type=BlockType.TABLE,
                        table=TableData(rows=self._table, header_rows=1),
                    )
                )
            self._table = None
        elif tag == "pre":
            text = "".join(self._buffer).strip("\n")
            self._buffer.clear()
            self._pre = False
            if text.strip():
                self._attach(RawBlock(type=BlockType.CODE, text=text))
        elif tag in _BLOCK_ENDING:
            self._flush()

        if tag in self._stack:
            # Pop to the matching tag, which also recovers from unclosed inner elements.
            while self._stack and self._stack.pop() != tag:
                pass

    def handle_data(self, data: str) -> None:
        if self._discard_depth:
            return
        if self._in_title or self._pre or self._buffer or data.strip():
            self._buffer.append(data)

    # --------------------------------------------------------------- assembly

    def _attach(self, block: RawBlock) -> None:
        """Place a block under the innermost open heading, so sections nest.

        Same rule as every other parser: a heading introduces the blocks that follow it
        until a heading at the same or a shallower level closes it.
        """
        if block.type is BlockType.HEADING:
            while self._open_headings and self._open_headings[-1].depth >= block.depth:
                self._open_headings.pop()
            target = (
                self._open_headings[-1].children if self._open_headings else self.blocks
            )
            target.append(block)
            self._open_headings.append(block)
            return
        if self._list_stack and block is not self._list_stack[-1]:
            return
        target = self._open_headings[-1].children if self._open_headings else self.blocks
        target.append(block)

    def _flush(self) -> None:
        if self._table is not None or self._pre:
            return
        text = clean_inline("".join(self._buffer))
        self._buffer.clear()
        if not text or self._heading_level is not None:
            return
        if self._list_stack:
            return
        self._attach(RawBlock(type=BlockType.PARAGRAPH, text=text))

    def finish(self) -> None:
        self._flush()


class HtmlParser:
    name = "html"
    version = "1.0"
    media_types = ("text/html", "application/xhtml+xml")

    def parse(self, data: bytes, *, filename: str | None = None) -> ParseResult:
        text, encoding_warning = decode(data)
        result = ParseResult()
        if encoding_warning:
            result.warn(encoding_warning, "character encoding had to be guessed")

        collector = _Collector()
        collector.feed(text)
        collector.close()
        collector.finish()

        result.blocks = collector.blocks
        result.metadata["format"] = "html"
        title = collector.title or next(
            (b.text for b in collector.blocks if b.type is BlockType.HEADING), None
        )
        if title:
            result.metadata["title"] = title
        return result
