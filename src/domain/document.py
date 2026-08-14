"""The parsed-document contract. This is the whole point of the service.

One representation for every source format, so the planning service never
branches on file type. Two properties make it worth more than a text blob:

`text` is the **canonical text** — the single string a consumer feeds a model.
`blocks` describe that exact string by character span, so structure and text can
never disagree. Everything downstream that needs to point at a piece of the
document (a page number, a heading, a quote's location) does it with an offset
into `text`, and offsets are only ever produced by `parse.serialize`, never by
matching text back against itself.

Note what is *not* here: `parsed_at`, durations, attempt counts. The artifact is
content-addressed at `parsed/{content_hash}/{parser_version}/`, which only makes
a replay an idempotent overwrite if the same input serialises to the same bytes.
A timestamp inside the payload would break that. Timing and provenance live on
the `parse_runs` row instead.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

SCHEMA_VERSION = "1.0"


class BlockType(str, Enum):
    """What a block *is*, in the vocabulary the planning step benefits from.

    Kept deliberately small. A type earns its place by changing how a consumer
    treats the content, not by describing how the source file drew it: `heading`
    and `list_item` change the shape of a plan, `bold_run` does not.
    """

    HEADING = "heading"
    PARAGRAPH = "paragraph"
    LIST = "list"
    LIST_ITEM = "list_item"
    TABLE = "table"
    CODE = "code"
    CAPTION = "caption"
    FOOTNOTE = "footnote"
    FIGURE = "figure"


class PageSource(str, Enum):
    TEXT_LAYER = "text_layer"
    OCR = "ocr"


class TableData(BaseModel):
    """A table's cells, kept alongside the rendered pipe table in `text`.

    Both representations are required and neither is redundant. Machine consumers
    want `rows`; a requirement quoted out of a table can only be validated if the
    table was also rendered into the canonical text.
    """

    rows: list[list[str]] = Field(default_factory=list)
    header_rows: int = Field(default=1, ge=0)

    @property
    def shape(self) -> tuple[int, int]:
        return len(self.rows), max((len(r) for r in self.rows), default=0)


class Block(BaseModel):
    """One structural unit, addressed by its span into `ParsedDocument.text`.

    `text` duplicates `doc.text[span[0]:span[1]]`. That redundancy is deliberate:
    consumers stay trivial, and `tests/test_serialize.py` asserts the equality
    over every block of every fixture, which is what catches an offset bug in the
    serialiser before it reaches a consumer that would fail mysteriously instead.
    """

    id: str
    type: BlockType
    depth: int = 0
    parent_id: str | None = None
    page: int | None = None
    span: tuple[int, int]
    text: str
    table: TableData | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    attrs: dict[str, Any] = Field(default_factory=dict)

    @property
    def start(self) -> int:
        return self.span[0]

    @property
    def end(self) -> int:
        return self.span[1]

    def contains(self, offset: int) -> bool:
        return self.span[0] <= offset < self.span[1]


class Page(BaseModel):
    """A page's footprint in the canonical text.

    `span` is what makes page attribution a lookup rather than a model output:
    given a quote's match offset, the page whose span contains it *is* the answer.

    Only paginated formats populate this. DOCX has no page count that can be known
    without rendering it, so `pages` is empty there and page attribution honestly
    reports `None` rather than inventing a number.
    """

    number: int = Field(ge=1)
    span: tuple[int, int]
    source: PageSource = PageSource.TEXT_LAYER
    image_key: str | None = None
    char_count: int = 0
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)

    def contains(self, offset: int) -> bool:
        return self.span[0] <= offset < self.span[1]


class SourceRef(BaseModel):
    """The linkage back to the raw file, so anything can be reprocessed later.

    `version_id` matters as much as `key`: if the backend overwrites an object,
    the key alone no longer identifies the bytes that were parsed. Together with
    `content_hash` on the document, the linkage is exact.
    """

    bucket: str
    key: str
    version_id: str | None = None
    byte_size: int | None = None
    media_type: str | None = None


class ParseWarning(BaseModel):
    """Something imperfect that did not justify failing the document.

    These are the difference between "parsed" and "parsed well", and they are the
    first thing to read when a downstream extraction looks thin.
    """

    code: str
    message: str
    page: int | None = None
    block_id: str | None = None


class DocumentMetadata(BaseModel):
    format: str
    parser_name: str
    parser_version: str

    title: str | None = None
    author: str | None = None
    subject: str | None = None
    keywords: list[str] = Field(default_factory=list)
    language: str | None = None

    page_count: int | None = None
    word_count: int = 0
    char_count: int = 0
    block_count: int = 0
    heading_count: int = 0
    table_count: int = 0

    created_at: datetime | None = None
    modified_at: datetime | None = None
    producer: str | None = None

    has_text_layer: bool = True
    ocr_applied: bool = False
    ocr_page_count: int = 0


class ParsedDocument(BaseModel):
    """The artifact. One of these per (document, parser_version, parse_options)."""

    schema_version: str = SCHEMA_VERSION
    document_id: str
    content_hash: str
    metadata: DocumentMetadata
    text: str
    blocks: list[Block] = Field(default_factory=list)
    pages: list[Page] = Field(default_factory=list)
    source: SourceRef | None = None
    warnings: list[ParseWarning] = Field(default_factory=list)

    # ---------------------------------------------------------------- lookups

    def block_at(self, offset: int) -> Block | None:
        """The innermost block containing `offset`.

        Blocks nest, so several can contain one offset — a nested bullet sits inside
        its list, which sits inside the parent item, which sits inside the outer
        list. The narrowest match is the innermost, because two blocks containing the
        same offset are necessarily in an ancestor/descendant relationship: siblings
        never overlap.

        The tie matters. A single-item list has *exactly* the span of its one item,
        since a `list` container contributes no text of its own. Preferring the later
        block on equal width resolves it to the item rather than the wrapper, which is
        the answer a caller asking "what is this text" wants — and it is well defined
        because the serialiser guarantees a parent always precedes its children.
        """
        best: Block | None = None
        for block in self.blocks:
            if not block.contains(offset):
                continue
            if best is None or (block.end - block.start) <= (best.end - best.start):
                best = block
        return best

    def page_at(self, offset: int) -> int | None:
        for page in self.pages:
            if page.contains(offset):
                return page.number
        return None

    def heading_path(self, block_id: str) -> list[str]:
        """Enclosing heading text from outermost to innermost.

        The cheap version of "which section is this in", and the grouping key a
        future chunker would use to keep a chunk inside one section.
        """
        by_id = {b.id: b for b in self.blocks}
        block = by_id.get(block_id)
        path: list[str] = []
        while block is not None:
            if block.type is BlockType.HEADING:
                path.append(block.text.lstrip("# ").strip())
            block = by_id.get(block.parent_id) if block.parent_id else None
        return list(reversed(path))
