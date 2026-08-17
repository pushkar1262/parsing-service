"""What a parser emits, and what it must never do.

A parser's job is to recover *structure* from one format and nothing else. It
emits `RawBlock` trees carrying text, and it does not compute character offsets —
`parse.serialize` owns those, because they must be a byproduct of building the
canonical text rather than the result of matching text against itself. A parser
that computed its own spans would be the one way to reintroduce the drift the
whole design exists to prevent.

`RawBlock` is a plain dataclass rather than a pydantic model on purpose: it is
internal scaffolding, mutated freely while a parser walks its source, and it never
crosses the service boundary. `Block` in `domain.document` is the wire type.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from domain.document import BlockType, PageSource, ParseWarning, TableData
from domain.errors import CorruptDocument, EncryptedDocument, UnsupportedFormat


@dataclass
class RawBlock:
    """A block before it has a span or an id.

    `children` carries hierarchy, and hierarchy means two different things
    depending on the type — see `serialize._SPANS_CHILDREN`. A `list` *contains*
    its items, so its span covers them. A `heading` *introduces* its section, so
    its span is only the heading line while its children still record it as their
    parent. Both are useful; conflating them makes `block.text` unpredictable.
    """

    type: BlockType
    text: str = ""
    depth: int = 0
    page: int | None = None
    table: TableData | None = None
    confidence: float | None = None
    attrs: dict[str, Any] = field(default_factory=dict)
    children: list[RawBlock] = field(default_factory=list)

    def add(self, child: RawBlock) -> RawBlock:
        self.children.append(child)
        return child


@dataclass
class PageInfo:
    """Per-page facts the serialiser cannot derive from block text alone.

    Page *spans* are computed from the blocks attributed to each page; whether
    those characters came from a text layer or from OCR is something only the
    parser knows, and it is the flag that tells a downstream validator how much to
    trust a verbatim quote.
    """

    source: PageSource = PageSource.TEXT_LAYER
    image_key: str | None = None
    confidence: float | None = None


@dataclass
class ParseResult:
    """A parser's complete output, before serialisation.

    `metadata` is a loose dict merged into `DocumentMetadata` rather than the model
    itself, so a parser can supply `title`/`author` when its format has them
    without every parser having to know the full metadata shape.
    """

    blocks: list[RawBlock] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    page_meta: dict[int, PageInfo] = field(default_factory=dict)
    warnings: list[ParseWarning] = field(default_factory=list)
    # Rendered page images, by page number. Not part of the artifact — the worker writes
    # them to blob storage and records the key on `Page.image_key`, because a
    # multi-megabyte PNG has no business inside a JSON document.
    page_images: dict[int, bytes] = field(default_factory=dict)

    def warn(self, code: str, message: str, *, page: int | None = None) -> None:
        self.warnings.append(ParseWarning(code=code, message=message, page=page))


@runtime_checkable
class Parser(Protocol):
    """The seam every format sits behind.

    `parse` takes bytes, not a path: the worker holds the object in memory after
    fetching it from blob storage, and a parser that wanted a filesystem path would
    force a temp file on every document for no reason. Parsers that genuinely need
    a file on disk (LibreOffice conversion) write their own, inside the sandbox.
    """

    name: str
    version: str
    media_types: tuple[str, ...]

    def parse(self, data: bytes, *, filename: str | None = None) -> ParseResult: ...


# Re-exported so a parser only ever imports from `parse.base`, while the taxonomy
# itself lives in `domain.errors` — intake routes on `transient` and `failure_class`,
# and those must mean the same thing for a fetch failure as for a parse failure.
__all__ = [
    "CorruptDocument",
    "EncryptedDocument",
    "PageInfo",
    "ParseResult",
    "Parser",
    "RawBlock",
    "UnsupportedFormat",
]
