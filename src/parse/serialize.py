"""Block tree in, canonical text plus spans out. The one module to guard hardest.

Every character offset in this service is born here. Nothing else may compute a
span, because the only way a span can be trustworthy is if it falls out of
*building* the text — the position where a block's characters were written. The
alternative, searching the finished text for each block, is what produces the
quiet off-by-a-paragraph errors that surface much later as a downstream validator
rejecting honest work.

The canonical text is a deterministic Markdown rendering. Markdown because models
read its structure natively, because substring matching still works on the inner
text of a marked-up block, and because a consumer that ignores `blocks` entirely
still sees headings and tables. Deterministic because the artifact is
content-addressed: same bytes and same parser version must produce the same
output, or replay stops being an idempotent overwrite.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import blake2b
from typing import Any

from domain.document import Block, BlockType, Page, PageSource, TableData
from parse.base import PageInfo, RawBlock

# Blocks whose span covers their children, versus blocks that merely parent them.
#
# A `list` contains its items: slicing a list's span should give you the whole
# list. A `heading` introduces a section: its span is the heading line alone, even
# though the section's blocks record it as their parent. If headings spanned their
# sections, `block.text` on a heading would return the entire chapter, which is not
# what any consumer means by the text of a heading.
_SPANS_CHILDREN = frozenset({BlockType.LIST, BlockType.LIST_ITEM})

# Separator written between a block's children (and between its own line and its
# first child). List structure is single-newline; everything else is a blank line.
_TIGHT = frozenset({BlockType.LIST, BlockType.LIST_ITEM})

BLOCK_SEP = "\n\n"
TIGHT_SEP = "\n"


@dataclass
class Serialized:
    text: str
    blocks: list[Block]
    pages: list[Page]


@dataclass
class _Rec:
    """A block's identity and extent, recorded during the walk."""

    ordinal: int
    type: BlockType
    depth: int
    parent: int | None
    page: int | None
    table: TableData | None
    confidence: float | None
    attrs: dict[str, Any]
    start: int = 0
    end: int = 0


class _Writer:
    """Append-only text builder that knows the current offset.

    Parts are joined once at the end rather than concatenated as we go: a running
    string would make the walk quadratic on large documents, and a 500-page PDF is
    exactly where that would start to hurt.
    """

    __slots__ = ("parts", "pos")

    def __init__(self) -> None:
        self.parts: list[str] = []
        self.pos = 0

    def write(self, s: str) -> None:
        if s:
            self.parts.append(s)
            self.pos += len(s)

    def build(self) -> str:
        return "".join(self.parts)


# --------------------------------------------------------------------------- #
# rendering one block's own line
# --------------------------------------------------------------------------- #


def _escape_cell(value: str) -> str:
    """Make a cell safe for a pipe table without changing what it says.

    A literal pipe would split the cell and a newline would break the row, so both
    are neutralised. Nothing else is touched: escaping more would mean the text a
    consumer quotes from is no longer the text the document contained.
    """
    return value.replace("|", r"\|").replace("\r", " ").replace("\n", " ").strip()


def render_table(table: TableData) -> str:
    """A table as a Markdown pipe table.

    This rendering is not decoration — it is the only reason a requirement stated
    inside a table can ever be quoted and validated. A table that lives solely in
    `Block.table` is invisible to anything reading the canonical text.
    """
    if not table.rows:
        return ""
    width = max(len(row) for row in table.rows)
    padded = [list(row) + [""] * (width - len(row)) for row in table.rows]

    def line(cells: Sequence[str]) -> str:
        return "| " + " | ".join(_escape_cell(c) for c in cells) + " |"

    if table.header_rows >= 1:
        head, body = padded[0], padded[1:]
    else:
        head, body = [""] * width, padded

    rule = "| " + " | ".join(["---"] * width) + " |"
    return "\n".join([line(head), rule, *(line(r) for r in body)])


def _own_text(node: RawBlock) -> str:
    """The Markdown line(s) this block contributes itself, excluding children."""
    if node.type is BlockType.LIST:
        return ""  # a pure container: its items are the text

    if node.type is BlockType.HEADING:
        level = min(max(node.depth, 1), 6)
        return f"{'#' * level} {node.text}".rstrip()

    if node.type is BlockType.LIST_ITEM:
        indent = "  " * max(node.depth - 1, 0)
        if node.attrs.get("ordered"):
            marker = f"{node.attrs.get('number', 1)}. "
        else:
            marker = "- "
        return f"{indent}{marker}{node.text}".rstrip()

    if node.type is BlockType.CODE:
        lang = node.attrs.get("lang") or ""
        return f"```{lang}\n{node.text}\n```"

    if node.type is BlockType.TABLE:
        if node.table is not None:
            return render_table(node.table)
        return node.text

    return node.text


def _child_sep(node_type: BlockType) -> str:
    return TIGHT_SEP if node_type in _TIGHT else BLOCK_SEP


# --------------------------------------------------------------------------- #
# pruning
# --------------------------------------------------------------------------- #


def _prune(nodes: Iterable[RawBlock]) -> list[RawBlock]:
    """Drop blocks that would render to nothing.

    An empty block is not harmless: it would claim a zero-width span, and a
    zero-width span can never contain an offset, so it would sit in `blocks`
    forever as a thing no lookup can ever return. Parsers produce them routinely
    (a `list` whose items all turned out blank), so they are cleaned here rather
    than in every parser.
    """
    kept: list[RawBlock] = []
    for node in nodes:
        node.children = _prune(node.children)
        renders = bool(_own_text(node).strip()) or bool(node.children)
        if renders:
            kept.append(node)
    return kept


# --------------------------------------------------------------------------- #
# the walk
# --------------------------------------------------------------------------- #


def _emit(
    node: RawBlock,
    parent: int | None,
    inherited_page: int | None,
    writer: _Writer,
    recs: list[_Rec],
) -> None:
    ordinal = len(recs)
    page = node.page if node.page is not None else inherited_page
    rec = _Rec(
        ordinal=ordinal,
        type=node.type,
        depth=node.depth,
        parent=parent,
        page=page,
        table=node.table,
        confidence=node.confidence,
        attrs=dict(node.attrs),
    )
    # Reserved before descending, so ordinals follow document order and a parent's
    # ordinal is always lower than its children's.
    recs.append(rec)

    start = writer.pos
    own = _own_text(node)
    writer.write(own)
    own_end = writer.pos

    if node.children:
        sep = _child_sep(node.type)
        if own:
            writer.write(sep)
        for index, child in enumerate(node.children):
            if index:
                writer.write(sep)
            _emit(child, ordinal, page, writer, recs)

    rec.start = start
    rec.end = writer.pos if node.type in _SPANS_CHILDREN else own_end


def _block_id(content_hash: str, ordinal: int) -> str:
    """Deterministic, so a replay produces byte-identical blocks.

    Derived from content and position rather than randomly, because a uuid here
    would make two parses of the same bytes differ and break content-addressing.
    """
    return blake2b(f"{content_hash}:{ordinal}".encode(), digest_size=8).hexdigest()


def _pages(recs: Sequence[_Rec], page_meta: Mapping[int, PageInfo]) -> list[Page]:
    """Page spans, derived from the blocks attributed to each page.

    Computed rather than declared: a page's footprint in the canonical text is
    exactly the extent of the text that came off it, and asking a parser to state
    it would be asking it to compute offsets.
    """
    extents: dict[int, tuple[int, int]] = {}
    for rec in recs:
        if rec.page is None or rec.end <= rec.start:
            continue
        low, high = extents.get(rec.page, (rec.start, rec.end))
        extents[rec.page] = (min(low, rec.start), max(high, rec.end))

    pages: list[Page] = []
    for number in sorted(extents):
        start, end = extents[number]
        info = page_meta.get(number, PageInfo())
        pages.append(
            Page(
                number=number,
                span=(start, end),
                source=info.source or PageSource.TEXT_LAYER,
                image_key=info.image_key,
                char_count=end - start,
                confidence=info.confidence,
            )
        )
    return pages


def serialize(
    nodes: Sequence[RawBlock],
    *,
    content_hash: str,
    page_meta: Mapping[int, PageInfo] | None = None,
) -> Serialized:
    """Render a block tree to canonical text, with a span for every block.

    Guarantees, all of which `tests/test_serialize.py` asserts directly:

    - `block.text == text[block.span[0]:block.span[1]]` for every block;
    - blocks are in document order, and a parent precedes its children;
    - the same tree and `content_hash` always produce identical output.
    """
    pruned = _prune([*nodes])
    writer = _Writer()
    recs: list[_Rec] = []

    for index, node in enumerate(pruned):
        if index:
            writer.write(BLOCK_SEP)
        _emit(node, None, None, writer, recs)

    text = writer.build()
    ids = [_block_id(content_hash, rec.ordinal) for rec in recs]

    blocks = [
        Block(
            id=ids[rec.ordinal],
            type=rec.type,
            depth=rec.depth,
            parent_id=ids[rec.parent] if rec.parent is not None else None,
            page=rec.page,
            span=(rec.start, rec.end),
            text=text[rec.start : rec.end],
            table=rec.table,
            confidence=rec.confidence,
            attrs=rec.attrs,
        )
        for rec in recs
    ]

    return Serialized(text=text, blocks=blocks, pages=_pages(recs, page_meta or {}))
