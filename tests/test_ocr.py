"""OCR, through a fake backend.

The fake is not a shortcut. OCR needs a native binary that is absent from most CI images,
and the logic worth testing is not Tesseract's recognition — it is *ours*: the coordinate
flip, the confidence propagation, the decision to replace a broken text layer rather than
merge with it, and the refusal to report an unreadable image as an empty document. All of
that is independent of which engine recognised the words.

`tests/test_ocr_tesseract.py` covers the adapter itself and skips without the binary.
"""

from __future__ import annotations

import pytest

from domain.document import BlockType, PageSource
from domain.errors import CorruptDocument, OcrUnavailable
from parse.ocr.base import NullOcr, OcrLine, OcrPage

pytest.importorskip("reportlab", reason="test-only PDF generator")
pytest.importorskip("pypdfium2", reason="pypdfium2 is an optional dependency")

from parse.image import ImageParser
from parse.pdf import PdfParser
from parse.pipeline import build_from_result
from tests.conftest import assert_spans_hold
from tests.test_pdf import build_pdf


class FakeOcr:
    """Returns fixed lines in image-pixel, top-left coordinates — like every engine."""

    name = "fake"
    version = "1.0"

    def __init__(self, page: OcrPage | None = None, *, works: bool = True) -> None:
        self.works = works
        self.calls: list[int] = []
        self.page = page or OcrPage(
            engine="fake",
            confidence=0.91,
            lines=[
                # At 200 dpi the scale is 200/72 ≈ 2.778, so these pixel coordinates
                # correspond to a heading near the top of the page and body text below.
                OcrLine("Scanned Requirements", 0.95, 200, 222, 1000, 264),
                OcrLine("The archive shall retain records for seven years.", 0.88, 200, 340, 1400, 372),
                OcrLine("Access requires two-factor authentication.", 0.90, 200, 388, 1300, 420),
            ],
        )

    def available(self) -> bool:
        return self.works

    def read(self, image: bytes, *, dpi: int = 200) -> OcrPage:
        self.calls.append(len(image))
        return self.page


def _scanned_pdf() -> bytes:
    """A digital page followed by a scanned one."""
    return build_pdf(
        [
            [
                ("Digital Section", 16, 72, 760),
                ("The service shall retry failed settlements automatically.", 10, 72, 720),
                ("Every retry is recorded with a timestamp and a reason.", 10, 72, 707),
            ],
            [],
        ],
        image_pages=frozenset({2}),
    )


def _parse_pdf(parser: PdfParser, data: bytes):
    doc = build_from_result(
        parser.parse(data), data=data, document_id="ocr-1", fallback_format="pdf"
    )
    assert_spans_hold(doc)
    return doc


# --------------------------------------------------------------------------- #
# the OCR pass
# --------------------------------------------------------------------------- #


def test_a_scanned_page_is_read_and_joins_the_document() -> None:
    ocr = FakeOcr()
    doc = _parse_pdf(PdfParser(ocr=ocr), _scanned_pdf())

    assert ocr.calls, "the scanned page should have been rendered and sent to OCR"
    assert "The archive shall retain records for seven years." in doc.text
    # And the digital page is untouched.
    assert "retry failed settlements automatically" in doc.text


def test_only_the_scanned_page_is_sent_to_ocr() -> None:
    """The per-page decision, in the form that costs money.

    OCR is 10-100x slower than reading a text layer. Sending the digital page too would
    be pure waste on every hybrid document.
    """
    ocr = FakeOcr()
    _parse_pdf(PdfParser(ocr=ocr), _scanned_pdf())
    assert len(ocr.calls) == 1


def test_the_page_is_marked_as_ocr_sourced() -> None:
    doc = _parse_pdf(PdfParser(ocr=FakeOcr()), _scanned_pdf())
    pages = {p.number: p for p in doc.pages}
    assert pages[1].source is PageSource.TEXT_LAYER
    assert pages[2].source is PageSource.OCR
    assert pages[2].confidence == pytest.approx(0.91, abs=0.02)


def test_metadata_records_how_many_pages_were_ocrd() -> None:
    doc = _parse_pdf(PdfParser(ocr=FakeOcr()), _scanned_pdf())
    assert doc.metadata.ocr_applied is True
    assert doc.metadata.ocr_page_count == 1
    assert doc.metadata.page_count == 2


def test_confidence_reaches_the_blocks_so_a_validator_can_be_lenient() -> None:
    """The field that separates "OCR misread this" from "the model invented it"."""
    doc = _parse_pdf(PdfParser(ocr=FakeOcr()), _scanned_pdf())
    scanned = [b for b in doc.blocks if b.page == 2 and b.confidence is not None]
    assert scanned, "OCR'd blocks must carry their confidence"
    assert all(0.8 <= b.confidence <= 1.0 for b in scanned)
    # Text-layer blocks carry none, because there is nothing uncertain about them.
    assert all(b.confidence is None for b in doc.blocks if b.page == 1)


def test_a_located_quote_reports_that_it_came_from_ocr() -> None:
    """End to end: the flag the planning service needs to loosen its quote check."""
    from domain.locate import Locator

    doc = _parse_pdf(PdfParser(ocr=FakeOcr()), _scanned_pdf())
    result = Locator(doc).locate("retain records for seven years")
    assert result.found
    assert result.page == 2
    assert result.ocr_applied is True
    assert result.confidence is not None


def test_the_vertical_flip_puts_ocr_lines_in_reading_order() -> None:
    """OCR measures down from the top, PDF measures up from the bottom.

    Skip the flip and the page is inverted: the last line becomes the first, every
    paragraph break lands somewhere else, and nothing about it looks like a bug.
    """
    doc = _parse_pdf(PdfParser(ocr=FakeOcr()), _scanned_pdf())
    page_two = doc.text[doc.pages[1].span[0] : doc.pages[1].span[1]]
    heading_at = page_two.index("Scanned Requirements")
    retention_at = page_two.index("retain records")
    access_at = page_two.index("two-factor")
    assert heading_at < retention_at < access_at


def test_page_images_are_kept_for_the_vision_fallback() -> None:
    """`extract` declares `requires: [json_schema, vision]`; a scan is why."""
    result = PdfParser(ocr=FakeOcr()).parse(_scanned_pdf())
    assert set(result.page_images) == {2}
    assert result.page_images[2].startswith(b"\x89PNG")


def test_rendering_page_images_can_be_turned_off() -> None:
    result = PdfParser(ocr=FakeOcr(), render_page_images=False).parse(_scanned_pdf())
    assert result.page_images == {}


# --------------------------------------------------------------------------- #
# degrading honestly
# --------------------------------------------------------------------------- #


def test_without_a_backend_the_page_is_flagged_rather_than_silently_dropped() -> None:
    doc = _parse_pdf(PdfParser(ocr=NullOcr()), _scanned_pdf())
    assert "page_needs_ocr" in [w.code for w in doc.warnings]
    assert doc.metadata.ocr_applied is False


def test_a_backend_that_recognises_nothing_says_so() -> None:
    ocr = FakeOcr(OcrPage(engine="fake", lines=[]))
    doc = _parse_pdf(PdfParser(ocr=ocr), _scanned_pdf())
    codes = [w.code for w in doc.warnings]
    assert "ocr_found_no_text" in codes
    # The rest of the document still parsed.
    assert "retry failed settlements automatically" in doc.text


def test_an_unavailable_backend_never_fails_the_document() -> None:
    """A missing binary is a quality problem, not an outage."""

    class Broken:
        name, version = "broken", "1.0"

        def available(self) -> bool:
            return True

        def read(self, image: bytes, *, dpi: int = 200):
            raise OcrUnavailable("tesseract vanished")

    doc = _parse_pdf(PdfParser(ocr=Broken()), _scanned_pdf())
    assert "ocr_unavailable" in [w.code for w in doc.warnings]
    assert "retry failed settlements automatically" in doc.text


# --------------------------------------------------------------------------- #
# standalone images
# --------------------------------------------------------------------------- #


def _png() -> bytes:
    import io

    from PIL import Image

    buffer = io.BytesIO()
    Image.new("L", (400, 300), color=230).save(buffer, format="PNG")
    return buffer.getvalue()


def test_an_image_document_is_ocrd_into_blocks() -> None:
    result = ImageParser(ocr=FakeOcr()).parse(_png())
    doc = build_from_result(
        result, data=_png(), document_id="img-1", fallback_format="image/png"
    )
    assert_spans_hold(doc)
    assert "Access requires two-factor authentication." in doc.text
    assert doc.metadata.ocr_applied is True
    assert all(b.confidence is not None for b in doc.blocks if b.type is BlockType.PARAGRAPH)


def test_an_unreadable_image_fails_rather_than_parsing_as_empty() -> None:
    """The worst outcome would be a confident wrong answer.

    Zero blocks would mark the document `ready`, and the planning service would report —
    correctly, from what it was given — that the document states no requirements.
    """
    with pytest.raises(CorruptDocument, match="no text could be recognised"):
        ImageParser(ocr=FakeOcr(OcrPage(engine="fake", lines=[]))).parse(_png())


def test_an_image_with_no_backend_is_a_retryable_failure() -> None:
    """Transient, because configuring OCR makes the same bytes succeed."""
    with pytest.raises(OcrUnavailable) as caught:
        ImageParser(ocr=NullOcr()).parse(_png())
    assert caught.value.transient is True
    assert caught.value.failure_class == "ocr_unavailable"


def test_a_poor_scan_is_warned_about_rather_than_rejected() -> None:
    low = OcrPage(
        engine="fake",
        confidence=0.41,
        lines=[OcrLine("Blurry whiteboard text", 0.41, 10, 10, 300, 40)],
    )
    result = ImageParser(ocr=FakeOcr(low)).parse(_png())
    assert "low_ocr_confidence" in [w.code for w in result.warnings]
    assert result.blocks, "poor text is still better than none, provided it is labelled"
