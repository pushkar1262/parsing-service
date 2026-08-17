"""Tesseract, via pytesseract.

`image_to_data` rather than `image_to_string`, which is the whole point of using this
adapter instead of a one-liner. `image_to_string` returns a flat blob: no confidence, no
coordinates, no line structure. Confidence is what lets a downstream quote check know to
be lenient with this page, and coordinates are what let OCR'd text take part in the same
layout-based structure inference as a digital page.

Words are grouped into lines using Tesseract's own `block_num`/`par_num`/`line_num`,
rather than by clustering on the y coordinate. Tesseract already did that segmentation
during recognition and it knows things about the page we do not.

Note the unit mismatch this adapter hides: Tesseract reports confidence as 0-100 with -1
for "no estimate", while everything downstream expects 0.0-1.0 or None. Leaking -1 into a
`confidence` field would read as "extremely low confidence" rather than "unknown", which
is a meaningfully different thing to tell a validator.
"""

from __future__ import annotations

import io
from collections import defaultdict
from typing import Any

from domain.errors import OcrUnavailable
from parse.normalise import clean_inline
from parse.ocr.base import OcrLine, OcrPage

# Tesseract's own marker for "I have no confidence estimate for this token".
_NO_ESTIMATE = -1

# Below this, a word is noise: scanner speckle and page edges routinely recognise as
# stray punctuation with single-digit confidence, and they land in the middle of otherwise
# clean sentences where they break any attempt to quote them.
MIN_WORD_CONFIDENCE = 30.0


class TesseractOcr:
    name = "tesseract"
    version = "1.0"

    def __init__(self, *, language: str = "eng", config: str = "") -> None:
        self.language = language
        self.config = config
        self._checked: bool | None = None

    def available(self) -> bool:
        """Whether both the Python wrapper and the native binary are present.

        Cached, because this shells out to `tesseract --version` and a worker asks the
        question once per page otherwise.
        """
        if self._checked is not None:
            return self._checked
        try:
            import pytesseract

            pytesseract.get_tesseract_version()
            self._checked = True
        except Exception:  # noqa: BLE001 - missing module or missing binary, same answer
            self._checked = False
        return self._checked

    def read(self, image: bytes, *, dpi: int = 200) -> OcrPage:
        try:
            import pytesseract
            from PIL import Image
        except ImportError as exc:
            raise OcrUnavailable(f"OCR dependencies are not installed: {exc}") from exc

        try:
            with Image.open(io.BytesIO(image)) as handle:
                data = pytesseract.image_to_data(
                    handle,
                    lang=self.language,
                    config=self.config,
                    output_type=pytesseract.Output.DICT,
                )
        except pytesseract.TesseractNotFoundError as exc:
            raise OcrUnavailable(
                f"the tesseract binary is not installed or not on PATH: {exc}"
            ) from exc
        except Exception as exc:
            raise OcrUnavailable(f"OCR failed: {exc}") from exc

        return self._to_page(data)

    def _to_page(self, data: dict[str, Any]) -> OcrPage:
        groups: dict[tuple[int, int, int], list[int]] = defaultdict(list)
        for index, text in enumerate(data.get("text", [])):
            if not str(text).strip():
                continue
            confidence = float(data["conf"][index])
            if confidence != _NO_ESTIMATE and confidence < MIN_WORD_CONFIDENCE:
                continue
            key = (
                int(data["block_num"][index]),
                int(data["par_num"][index]),
                int(data["line_num"][index]),
            )
            groups[key].append(index)

        lines: list[OcrLine] = []
        confidences: list[float] = []
        for key in sorted(groups):
            indices = groups[key]
            words = [str(data["text"][i]).strip() for i in indices]
            scores = [
                float(data["conf"][i])
                for i in indices
                if float(data["conf"][i]) != _NO_ESTIMATE
            ]
            text = clean_inline(" ".join(words))
            if not text:
                continue
            confidences.extend(scores)
            lines.append(
                OcrLine(
                    text=text,
                    # 0-100 to 0.0-1.0, and None rather than -1 when Tesseract declines
                    # to estimate: "unknown" and "very low" mean different things.
                    confidence=(sum(scores) / len(scores) / 100.0) if scores else None,
                    left=min(float(data["left"][i]) for i in indices),
                    top=min(float(data["top"][i]) for i in indices),
                    right=max(
                        float(data["left"][i]) + float(data["width"][i]) for i in indices
                    ),
                    bottom=max(
                        float(data["top"][i]) + float(data["height"][i]) for i in indices
                    ),
                )
            )

        page_confidence = (
            sum(confidences) / len(confidences) / 100.0 if confidences else None
        )
        return OcrPage(lines=lines, confidence=page_confidence, engine=self.name)
