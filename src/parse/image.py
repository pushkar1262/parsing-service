"""Images: a photograph or scan uploaded on its own.

Structurally the simplest parser and the one most likely to disappoint, because there is
no text layer to fall back on — whatever OCR returns *is* the document. So the two things
this parser must get right are both about honesty rather than extraction:

An image that OCR cannot read is a **failure**, not an empty document. Returning zero
blocks would mark the document `ready` with no content, and the planning service would
extract nothing from it and report, correctly, that the document states no requirements.
That is the worst possible outcome: a confident wrong answer. So a page with no
recognisable text raises, and the document lands in `failed` with a reason a human can
act on.

Confidence travels with every block. A photo of a whiteboard OCRs at 0.4 and produces text
that no downstream quote check will match; the confidence is the only thing that
distinguishes that from a model inventing quotes.
"""

from __future__ import annotations

from typing import Any

from domain.document import BlockType, PageSource
from domain.errors import CorruptDocument, OcrUnavailable
from parse.base import PageInfo, ParseResult, RawBlock
from parse.ocr.base import NullOcr, OcrBackend

# Below this, OCR output is not worth presenting as document content. It is not a hard
# failure — the text is still returned, with a warning — because a poor scan of a real
# requirements page is still more useful than nothing, provided the reader is told.
LOW_CONFIDENCE = 0.55


class ImageParser:
    name = "image"
    version = "1.0"
    media_types = (
        "image/png",
        "image/jpeg",
        "image/tiff",
        "image/bmp",
        "image/gif",
        "image/webp",
    )

    def __init__(self, *, ocr: OcrBackend | None = None, dpi: int = 200) -> None:
        self.ocr = ocr or NullOcr()
        self.dpi = dpi

    def parse(self, data: bytes, *, filename: str | None = None) -> ParseResult:
        result = ParseResult()

        if not self.ocr.available():
            raise OcrUnavailable(
                "this document is an image and no OCR backend is configured, so it has "
                "no readable content; install parsing-service[ocr] with the tesseract "
                "binary, or configure a cloud OCR backend"
            )

        recognised = self.ocr.read(data, dpi=self.dpi)
        if not recognised.lines:
            raise CorruptDocument(
                "no text could be recognised in this image; if it is a photograph of a "
                "document, a higher-resolution or better-lit capture may work"
            )

        blocks: list[RawBlock] = []
        for line in recognised.lines:
            blocks.append(
                RawBlock(
                    type=BlockType.PARAGRAPH,
                    text=line.text,
                    page=1,
                    confidence=line.confidence,
                )
            )

        if recognised.confidence is not None and recognised.confidence < LOW_CONFIDENCE:
            result.warn(
                "low_ocr_confidence",
                f"the image was recognised at {recognised.confidence:.2f} confidence; "
                f"quotes taken from this document may not match its text exactly",
            )

        result.blocks = blocks
        result.page_meta = {
            1: PageInfo(source=PageSource.OCR, confidence=recognised.confidence)
        }
        # The image *is* the page, so it is also the vision fallback.
        result.page_images[1] = data
        result.metadata.update(self._metadata(data, recognised))
        return result

    def _metadata(self, data: bytes, recognised: Any) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "format": "image",
            "page_count": 1,
            "has_text_layer": False,
            "ocr_applied": True,
            "ocr_page_count": 1,
        }
        # The first recognised line is the closest thing to a title an image has, and
        # only if it is short enough to plausibly be one.
        first = recognised.lines[0].text if recognised.lines else ""
        if 0 < len(first) <= 120:
            meta["title"] = first
        return meta
