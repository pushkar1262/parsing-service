"""Media type to parser, and nothing more clever than that.

Parsers are registered rather than imported eagerly because each one's dependency
is optional. A deployment that never sees a spreadsheet should not need `openpyxl`
installed, and — more importantly — a *missing* dependency must surface as a clean
`unsupported_format` rejection recorded against the document, not as an ImportError
that takes the worker down on an unlucky message.
"""

from __future__ import annotations

from parse.base import Parser, UnsupportedFormat
from parse.csv import CsvParser
from parse.html import HtmlParser
from parse.text import TextParser


class Registry:
    def __init__(self) -> None:
        self._by_media_type: dict[str, Parser] = {}
        self._unavailable: dict[str, str] = {}

    def register(self, parser: Parser) -> None:
        for media_type in parser.media_types:
            self._by_media_type[media_type] = parser

    def mark_unavailable(self, media_type: str, reason: str) -> None:
        """Record a format we would support if a dependency were installed.

        Kept distinct from "never heard of it" so the rejection message can say
        `install parsing-service[docx]` rather than implying we cannot ever read the
        file. This is the difference between an operator fixing it in a minute and
        an operator filing a feature request.
        """
        self._unavailable[media_type] = reason

    def get(self, media_type: str) -> Parser:
        parser = self._by_media_type.get(media_type)
        if parser is not None:
            return parser
        reason = self._unavailable.get(media_type)
        if reason:
            raise UnsupportedFormat(f"{media_type} is not available: {reason}")
        raise UnsupportedFormat(f"no parser for {media_type}")

    def supports(self, media_type: str) -> bool:
        return media_type in self._by_media_type

    def media_types(self) -> list[str]:
        return sorted(self._by_media_type)


def default_registry(*, ocr: object | None = None) -> Registry:
    """Every parser whose dependencies are actually importable.

    The try/except around each optional parser is the whole point: importing this
    module must never fail because of a format this deployment does not handle.
    """
    registry = Registry()
    registry.register(TextParser())
    registry.register(HtmlParser())
    registry.register(CsvParser())

    # Parsers that can use OCR are given the backend rather than constructing one, so a
    # deployment picks its engine once and every format follows.
    _try(registry, "parse.image", "ImageParser", IMAGE_MEDIA_TYPES, "Pillow", "ocr",
         kwargs={"ocr": ocr} if ocr else {})

    _try(
        registry,
        "parse.docx",
        "DocxParser",
        ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",),
        "python-docx",
        "docx",
    )
    _try(
        registry,
        "parse.pdf",
        "PdfParser",
        ("application/pdf",),
        "pypdfium2",
        "pdf",
        kwargs={"ocr": ocr} if ocr else {},
    )
    _try(
        registry,
        "parse.xlsx",
        "XlsxParser",
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",),
        "openpyxl",
        "xlsx",
    )
    return registry


IMAGE_MEDIA_TYPES = (
    "image/png",
    "image/jpeg",
    "image/tiff",
    "image/bmp",
    "image/gif",
    "image/webp",
)


def _try(
    registry: Registry,
    module: str,
    attribute: str,
    media_types: tuple[str, ...],
    package: str,
    extra: str,
    kwargs: dict | None = None,
) -> None:
    """Register a parser, or record why it is unavailable.

    The `except ImportError` is the whole point: importing this module must never fail
    because of a format this deployment does not handle, and an operator seeing
    "install parsing-service[pdf]" fixes it in a minute where an ImportError traceback
    from inside a worker sends them reading our source.
    """
    try:
        imported = __import__(module, fromlist=[attribute])
        parser = getattr(imported, attribute)(**(kwargs or {}))
    except ImportError as exc:  # pragma: no cover - depends on the environment
        for media_type in media_types:
            registry.mark_unavailable(
                media_type,
                f"{package} is not installed ({exc}); "
                f"install parsing-service[{extra}]",
            )
    else:
        registry.register(parser)
