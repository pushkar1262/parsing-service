"""Plain text and Markdown.

Worth building first, and not because it is easy. Markdown is already the shape the
canonical text is rendered *in*, so this parser is the one place where input and
output structure coincide — which makes it the cheapest way to prove the whole
representation round-trips. A pipe table parsed here and re-rendered by
`serialize.render_table` should come back the same, and a heading tree recovered
here should serialise to the same heading tree. Any drift is a real bug in the
contract, caught without a single binary fixture.

Plain text falls out for free: with no Markdown syntax present, every blank-line
separated run becomes a paragraph, which is exactly right.
"""

from __future__ import annotations

import re

from domain.document import BlockType, TableData
from parse.base import ParseResult, RawBlock
from parse.normalise import clean_inline, decode, join_wrapped, normalise

_ATX = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_FENCE = re.compile(r"^\s*(```+|~~~+)\s*([^\s`~]*)\s*$")
_LIST = re.compile(r"^(\s*)([-*+]|\d{1,9}[.)])\s+(.*)$")
_TABLE_RULE = re.compile(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$")
_HR = re.compile(r"^\s*([-*_])(\s*\1){2,}\s*$")
_CELL_SPLIT = re.compile(r"(?<!\\)\|")


def _split_row(line: str) -> list[str]:
    """Split a pipe-table row, honouring the escaping `serialize` applies.

    The inverse of `serialize._escape_cell`, so a table survives a parse/render
    round trip. Without the negative lookbehind an escaped pipe inside a cell would
    split it, silently shifting every following column.
    """
    stripped = line.strip()
    stripped = stripped.removeprefix("|")
    if stripped.endswith("|") and not stripped.endswith("\\|"):
        stripped = stripped[:-1]
    return [cell.replace("\\|", "|").strip() for cell in _CELL_SPLIT.split(stripped)]


class TextParser:
    name = "text"
    version = "1.0"
    media_types = (
        "text/plain",
        "text/markdown",
        "text/x-markdown",
        "application/markdown",
    )

    def parse(self, data: bytes, *, filename: str | None = None) -> ParseResult:
        raw, encoding_warning = decode(data)
        result = ParseResult()
        if encoding_warning:
            result.warn(
                encoding_warning,
                "character encoding could not be determined from the bytes; "
                "text may contain replacement characters",
            )

        lines = normalise(raw).split("\n")

        # Headings own the blocks that follow them, so content can be traced to its
        # section. The stack holds the currently open headings, outermost first.
        heading_stack: list[tuple[int, RawBlock]] = []
        root: list[RawBlock] = []

        def sink() -> list[RawBlock]:
            return heading_stack[-1][1].children if heading_stack else root

        index = 0
        total = len(lines)
        while index < total:
            line = lines[index]

            if not line.strip():
                index += 1
                continue

            fence = _FENCE.match(line)
            if fence:
                block, index = self._code(lines, index, fence)
                sink().append(block)
                continue

            atx = _ATX.match(line)
            if atx:
                level = len(atx.group(1))
                heading = RawBlock(
                    type=BlockType.HEADING,
                    text=clean_inline(atx.group(2)),
                    depth=level,
                )
                # Close every heading at or below this level before opening it, so
                # an h2 following an h3 becomes a sibling rather than a descendant.
                while heading_stack and heading_stack[-1][0] >= level:
                    heading_stack.pop()
                sink().append(heading)
                heading_stack.append((level, heading))
                index += 1
                continue

            if self._is_table(lines, index):
                block, index = self._table(lines, index)
                sink().append(block)
                continue

            if _HR.match(line):
                index += 1  # a horizontal rule carries no content
                continue

            if _LIST.match(line):
                block, index = self._list(lines, index)
                sink().append(block)
                continue

            block, index = self._paragraph(lines, index)
            sink().append(block)

        result.blocks = root
        title = self._title(root)
        if title:
            result.metadata["title"] = title
        result.metadata["format"] = "markdown" if self._has_markup(lines) else "text"
        return result

    # ------------------------------------------------------------------ parts

    def _code(
        self, lines: list[str], index: int, fence: re.Match[str]
    ) -> tuple[RawBlock, int]:
        """A fenced code block, kept verbatim.

        The one place normalisation is skipped: collapsing whitespace inside code
        would destroy the indentation that is the content. Only the fence itself is
        interpreted.
        """
        marker, lang = fence.group(1), fence.group(2)
        index += 1
        body: list[str] = []
        while index < len(lines):
            if lines[index].strip().startswith(marker[0] * len(marker)):
                index += 1
                break
            body.append(lines[index])
            index += 1
        return (
            RawBlock(
                type=BlockType.CODE,
                text="\n".join(body).rstrip(),
                attrs={"lang": lang} if lang else {},
            ),
            index,
        )

    def _is_table(self, lines: list[str], index: int) -> bool:
        return (
            "|" in lines[index]
            and index + 1 < len(lines)
            and _TABLE_RULE.match(lines[index + 1]) is not None
        )

    def _table(self, lines: list[str], index: int) -> tuple[RawBlock, int]:
        header = _split_row(lines[index])
        index += 2  # the header and its rule
        rows = [header]
        while index < len(lines) and "|" in lines[index] and lines[index].strip():
            rows.append(_split_row(lines[index]))
            index += 1
        return (
            RawBlock(type=BlockType.TABLE, table=TableData(rows=rows, header_rows=1)),
            index,
        )

    def _list(self, lines: list[str], index: int) -> tuple[RawBlock, int]:
        """A list, nested by indentation.

        `stack` holds one entry per open nesting level. Indentation is compared
        against the level it opened at rather than against a fixed step, because
        real documents indent by two spaces, four spaces, or a tab, and a fixed step
        would flatten half of them.
        """
        stack: list[tuple[int, RawBlock]] = []
        last_item: RawBlock | None = None
        root: RawBlock | None = None

        while index < len(lines):
            match = _LIST.match(lines[index])
            if match is None:
                # A single blank line inside a list is a loose list, not its end.
                if (
                    not lines[index].strip()
                    and index + 1 < len(lines)
                    and _LIST.match(lines[index + 1])
                ):
                    index += 1
                    continue
                break

            indent = len(match.group(1).expandtabs(4))
            marker, content = match.group(2), match.group(3)
            ordered = marker[0].isdigit()

            if not stack:
                root = RawBlock(type=BlockType.LIST, attrs={"ordered": ordered})
                stack.append((indent, root))
            else:
                while len(stack) > 1 and indent < stack[-1][0]:
                    stack.pop()
                if indent > stack[-1][0] and last_item is not None:
                    nested = RawBlock(type=BlockType.LIST, attrs={"ordered": ordered})
                    last_item.add(nested)
                    stack.append((indent, nested))

            depth = len(stack)
            attrs: dict[str, object] = {"ordered": ordered}
            if ordered:
                attrs["number"] = int(re.sub(r"\D", "", marker) or 1)
            last_item = RawBlock(
                type=BlockType.LIST_ITEM,
                text=clean_inline(content),
                depth=depth,
                attrs=attrs,
            )
            stack[-1][1].add(last_item)
            index += 1

        assert root is not None  # guarded by the _LIST.match at the call site
        return root, index

    def _paragraph(self, lines: list[str], index: int) -> tuple[RawBlock, int]:
        """Consecutive non-blank lines, unwrapped into one line of canonical text.

        `join_wrapped` rather than a plain join, so a hard-wrapped source (every
        exported .txt requirements doc) does not leave mid-sentence line breaks in
        text that a model will be asked to quote from.
        """
        body: list[str] = []
        while index < len(lines) and lines[index].strip():
            line = lines[index]
            if _ATX.match(line) or _LIST.match(line) or _FENCE.match(line):
                break
            if self._is_table(lines, index):
                break
            body.append(line)
            index += 1
        return RawBlock(type=BlockType.PARAGRAPH, text=join_wrapped("\n".join(body))), index

    # --------------------------------------------------------------- metadata

    def _title(self, blocks: list[RawBlock]) -> str | None:
        """The first heading, if the document opens with one.

        Deliberately narrow: a heading buried on page four is a section title, not
        the document's. Better to report no title than the wrong one, since a title
        is one of the few fields a consumer will show to a human without checking.
        """
        for block in blocks:
            if block.type is BlockType.HEADING:
                return block.text or None
            return None
        return None

    def _has_markup(self, lines: list[str]) -> bool:
        return any(
            _ATX.match(line) or _LIST.match(line) or _FENCE.match(line) for line in lines
        )
