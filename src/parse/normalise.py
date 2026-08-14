"""Text repairs applied once, here, so no consumer has to guess at them.

The planning service's quote validator normalises before comparing, and its
docstring names exactly why: "PDF extraction introduces smart quotes, non-breaking
spaces, ligatures and line-wrap whitespace." Every one of those is an artifact of
*our* stage. Fixing them downstream is guesswork against a lossy string; fixing
them here is a handful of well-understood transforms.

What we deliberately do **not** do: lowercase, or flatten smart quotes to ASCII.
Both are lossy for display and for anything that re-renders the document, and the
downstream normaliser already handles them for comparison. Normalise the artifacts
of extraction, not the author's punctuation.

Every pattern here is written with explicit escapes rather than literal characters,
because the whole point of this module is invisible characters — and a source file
full of them is one no reviewer can check.
"""

from __future__ import annotations

import re
import unicodedata

# Characters that carry no meaning in extracted text but break string matching.
# The soft hyphen (U+00AD) is the important one: it survives NFKC, is invisible, and
# sits mid-word in PDFs, so a quote containing it can never match.
_INVISIBLE = {
    "­": "",  # soft hyphen
    "\u200b": "",  # zero-width space
    "‌": "",  # zero-width non-joiner
    "‍": "",  # zero-width joiner
    "⁠": "",  # word joiner
    "﻿": "",  # BOM appearing as a character
}
_INVISIBLE_TABLE = str.maketrans(_INVISIBLE)

# Control characters, except the tab and newline we handle deliberately.
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

# Horizontal whitespace, including the exotic spaces PDF producers emit.
_WS_RUN = re.compile("[ \t  -   　]+")

# Line and paragraph separators, plus CRLF, folded to \n.
_NEWLINES = re.compile("\r\n?| | ")

# A word broken across a line by a hyphen. Requires a word character on both sides,
# so a genuine trailing hyphen or an em-dash run is left alone.
_WRAP_HYPHEN = re.compile("(\\w)[-‐‑]\\s*\n\\s*(\\w)")


def normalise(text: str) -> str:
    """NFKC, invisible characters out, newlines and control characters tidied.

    NFKC is what turns the ligature "ﬁ" into "fi" and a non-breaking space into a
    plain space — the two artifacts most likely to make a genuine quote look
    invented.
    """
    text = _NEWLINES.sub("\n", text)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(_INVISIBLE_TABLE)
    return _CONTROL.sub("", text)


def collapse_ws(text: str) -> str:
    """Collapse runs of horizontal whitespace, preserving line breaks."""
    return "\n".join(_WS_RUN.sub(" ", line).strip() for line in text.split("\n"))


def clean_inline(text: str) -> str:
    """Normalise a fragment that must end up on one line of the canonical text."""
    return _WS_RUN.sub(" ", normalise(text).replace("\n", " ")).strip()


def join_wrapped(text: str) -> str:
    r"""Undo line wrapping inside a paragraph, de-hyphenating as we go.

    This is the repair that matters most for PDFs, and it cannot be done downstream.
    `"authenti-\ncation"` has to become `"authentication"` before anything tries to
    quote it: once the line structure is gone no normaliser can distinguish that
    from a genuinely hyphenated word, and a model shown the broken form either
    reproduces the break or silently repairs it — failing an exact-match check
    either way.
    """
    text = _WRAP_HYPHEN.sub(r"\1\2", text)
    return _WS_RUN.sub(" ", text.replace("\n", " ")).strip()


def decode(data: bytes) -> tuple[str, str | None]:
    """Bytes to text, plus a warning code when the encoding had to be guessed.

    Order matters. UTF-8 first because it is nearly always right and its multi-byte
    sequences are self-validating. Then the UTF-16 a Windows export produces,
    detectable by BOM. Then cp1252, the encoding that passes for ASCII until someone
    types a curly quote. latin-1 last because it *cannot* fail — it maps every byte
    — so reaching it means we are guessing, and the caller records that rather than
    pretending otherwise.
    """
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return data.decode("utf-16"), None
        except UnicodeDecodeError:
            pass
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return data.decode(encoding), None
        except UnicodeDecodeError:
            continue
    try:
        return data.decode("cp1252"), "encoding_guessed"
    except UnicodeDecodeError:
        return data.decode("latin-1", errors="replace"), "encoding_undetermined"
