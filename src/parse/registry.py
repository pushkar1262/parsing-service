"""Media type to parser, and nothing more clever than that.

Parsers are registered rather than imported eagerly because each one's dependency
is optional. A deployment that never sees a spreadsheet should not need `openpyxl`
installed, and — more importantly — a *missing* dependency must surface as a clean
`unsupported_format` rejection recorded against the document, not as an ImportError
that takes the worker down on an unlucky message.
"""

from __future__ import annotations

from parse.base import Parser, UnsupportedFormat
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


def default_registry() -> Registry:
    """Every parser whose dependencies are actually importable.

    The try/except around each optional parser is the whole point: importing this
    module must never fail because of a format this deployment does not handle.
    """
    registry = Registry()
    registry.register(TextParser())

    try:
        from parse.docx import DocxParser
    except ImportError as exc:  # pragma: no cover - depends on the environment
        registry.mark_unavailable(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            f"python-docx is not installed ({exc})",
        )
    else:
        registry.register(DocxParser())

    return registry
