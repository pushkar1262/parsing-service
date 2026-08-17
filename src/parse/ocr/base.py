"""The OCR seam.

A protocol rather than a direct Tesseract call, for two reasons that both turned out to
matter. Tesseract is free and adequate for clean scans; AWS Textract and Google Document
AI are dramatically better on scanned *tables* and on photographs of documents, and cost
money per page. Which one a deployment wants is a business decision, not a code decision.

The second reason is testability: OCR needs a native binary that is not present in every
environment, and a service whose page-attribution logic can only be tested where
`tesseract` is installed is a service whose page-attribution logic does not get tested.
Everything except the adapter itself is exercised through a fake.

Coordinates come back in **image pixels with a top-left origin**, because that is what
every OCR engine produces. Converting to PDF points is the caller's job, since only the
caller knows the render scale and the page height.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from domain.errors import OcrUnavailable

__all__ = ["NullOcr", "OcrBackend", "OcrLine", "OcrPage", "OcrUnavailable"]


@dataclass
class OcrLine:
    """One recognised line, with the confidence that makes it auditable.

    `confidence` is the single most useful field here and the reason lines rather than a
    flat string: a page that OCR'd at 0.4 produces text a downstream quote check will
    reject, and the only way anyone can tell that from a genuine hallucination is if the
    confidence travelled with the text.
    """

    text: str
    confidence: float | None = None
    left: float = 0.0
    top: float = 0.0
    right: float = 0.0
    bottom: float = 0.0

    @property
    def height(self) -> float:
        return max(self.bottom - self.top, 0.0)


@dataclass
class OcrPage:
    lines: list[OcrLine] = field(default_factory=list)
    confidence: float | None = None
    engine: str = "unknown"

    @property
    def text(self) -> str:
        return "\n".join(line.text for line in self.lines)


@runtime_checkable
class OcrBackend(Protocol):
    name: str
    version: str

    def available(self) -> bool:
        """Whether this backend can actually run right now.

        Separate from construction so a missing native binary is a *recorded* condition
        rather than an ImportError: a worker that dies because Tesseract is not installed
        has turned a degraded-quality problem into an outage.
        """
        ...

    def read(self, image: bytes, *, dpi: int = 200) -> OcrPage: ...


class NullOcr:
    """The backend used when none is configured.

    It raises rather than returning empty text, because silently producing a page with no
    content is the failure mode this whole service is built to avoid: the document would
    go `ready` with the scanned half missing and nothing to indicate it.
    """

    name = "null"
    version = "1.0"

    def available(self) -> bool:
        return False

    def read(self, image: bytes, *, dpi: int = 200) -> OcrPage:
        raise OcrUnavailable(
            "no OCR backend is configured; install parsing-service[ocr] and the "
            "tesseract binary, or configure a cloud OCR backend"
        )
