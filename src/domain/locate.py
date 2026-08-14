"""Find a quote in a parsed document, and say exactly where it is.

This is the half of the planning service's `verbatim_quotes` that belongs on this
side of the boundary. Its `_normalise` helper exists solely to undo *our* artifacts,
which makes it text mechanics living in a requirements module. The split:

- **Where does this string occur?** — here. We own the canonical text, its
  normalisation, its block spans and its OCR provenance.
- **What do we do when it doesn't?** — there. Reject, repair, or quarantine the
  entry is extraction policy, and it needs the model and the repair loop.

Three things this returns that a local `needle in haystack` cannot:

`span`, because computing a match and discarding its offset throws away the page
number, the block, the ordering and a dedup key — all of it free once you have the
position. A consumer can stop asking the model for a page number it gets wrong.

`snapped`, because rejection is usually the wrong remedy. When a quote is a
near-miss we return the *exact source span* as `text`, so the caller replaces the
model's approximation with real document text instead of failing the run. Nothing
that is not in the document is ever returned, so the hallucination guard is intact —
but one imperfect quote stops discarding a whole extraction.

`occurrences`, because a quote matching six times (a repeated footer, a boilerplate
sentence) makes page attribution a coin flip, and the caller deserves to know that
rather than receive the first match as though it were the only one.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Literal

from domain.document import ParsedDocument

MatchKind = Literal["exact", "snapped", "none"]

# Tokens long enough to be worth indexing on. Four characters skips most function
# words without needing a stopword list, which would have to be per-language.
_TOKEN = re.compile(r"[a-z0-9]{4,}")

# How many candidate blocks a fuzzy pass will score. Bounds the cost on large
# documents; the best match is essentially always among the highest-overlap few.
_MAX_SNAP_CANDIDATES = 40

# Folded for comparison only, never for storage. These are the differences that make
# a genuine quote look invented: a model retyping a sentence reflows the whitespace
# and straightens the punctuation almost every time.
_FOLD = {
    "‘": "'",
    "’": "'",
    "‚": "'",
    "‛": "'",
    "“": '"',
    "”": '"',
    "„": '"',
    "′": "'",
    "″": '"',
    "‐": "-",
    "‑": "-",
    "‒": "-",
    "–": "-",
    "—": "-",
    "―": "-",
    "−": "-",
    "…": "...",
}

# Below this, a "quote" is too short to be evidence of anything: three characters
# appear in every document, so a match proves nothing about provenance.
MIN_QUOTE_CHARS = 8

# How close a near-miss has to be before we trust it enough to snap. High on purpose:
# this decides whether a model's paraphrase gets replaced by real source text, and a
# loose threshold would start aligning quotes to the wrong sentence.
SNAP_THRESHOLD = 0.90


@dataclass
class Located:
    quote: str
    found: bool
    match: MatchKind
    span: tuple[int, int] | None = None
    text: str | None = None
    page: int | None = None
    block_id: str | None = None
    occurrences: int = 0
    similarity: float = 0.0
    ocr_applied: bool = False
    confidence: float | None = None
    reason: str | None = None


def _fold(text: str) -> tuple[str, list[int]]:
    """Fold text for comparison, keeping a map back to original offsets.

    The map is the whole trick. Folding case and collapsing whitespace is what makes
    a model's retyped quote match, but the answer has to be a span into the *original*
    canonical text — so every folded character records the index it came from.

    The 1:1 correspondence is preserved deliberately: a few characters lengthen when
    lowercased (U+0130 becomes two), which would desynchronise the map, so those are
    left unfolded rather than breaking the invariant.
    """
    out: list[str] = []
    origin: list[int] = []
    previous_was_space = False

    for index, char in enumerate(text):
        folded = _FOLD.get(char, char)
        if len(folded) != 1:
            # A multi-character fold (an ellipsis) would break the 1:1 map.
            folded = char
        if folded.isspace():
            if previous_was_space:
                continue
            out.append(" ")
            origin.append(index)
            previous_was_space = True
            continue
        lowered = folded.lower()
        out.append(lowered if len(lowered) == 1 else folded)
        origin.append(index)
        previous_was_space = False

    return "".join(out), origin


def _expand_to_word_bounds(text: str, low: int, high: int) -> tuple[int, int]:
    """Grow a span outward until it stops cutting a word in half.

    Alignment ends at the last character that *matched*, so a quote reading
    "every 90 dayz" against a source reading "every 90 days" aligns only as far as
    "day" — the differing final letter is outside every matching run. Returning that
    hands the caller a quote ending mid-word, which reads as corruption and would fail
    any later exact re-check. Completing the word costs nothing and is always the text
    the document actually contains.
    """
    while low > 0 and text[low - 1].isalnum():
        low -= 1
    while high < len(text) and text[high].isalnum():
        high += 1
    return low, high


def _to_source_span(
    origin: list[int], start: int, end: int, text_length: int
) -> tuple[int, int]:
    """Map a folded [start, end) back onto the original text."""
    source_start = origin[start]
    source_end = origin[end - 1] + 1 if end > start else source_start
    return source_start, min(source_end, text_length)


class Locator:
    """Folds a document once, then answers many quote lookups against it.

    Built per document rather than per quote because folding and indexing are the
    expensive parts and an extraction arrives with forty quotes for one document.
    """

    def __init__(self, document: ParsedDocument) -> None:
        self.document = document
        self.folded, self.origin = _fold(document.text)
        self._own = self._own_extents()
        self._token_index = self._build_token_index()

    def _own_extents(self) -> list[tuple[int, int]]:
        """Each block's *own* folded extent, excluding any nested children.

        This distinction decides whether snapping works at all. A `list_item` spans
        the sub-list beneath it, so scoring a quote against the block's full span
        compares a one-line requirement against that line plus every child bullet —
        which drags the similarity ratio far below threshold and makes a
        single-character typo look like a different sentence entirely.

        A container's own extent is empty (a `list` contributes no text of its own),
        which correctly removes it from consideration.
        """
        first_child: dict[str, int] = {}
        for block in self.document.blocks:
            if block.parent_id is None:
                continue
            previous = first_child.get(block.parent_id)
            if previous is None or block.start < previous:
                first_child[block.parent_id] = block.start

        extents: list[tuple[int, int]] = []
        for block in self.document.blocks:
            # A heading's children are emitted *after* it and lie outside its span,
            # so `min` leaves headings with their full extent.
            own_end = min(block.end, first_child.get(block.id, block.end))
            extents.append(
                (self._folded_index(block.start), self._folded_index(own_end))
            )
        return extents

    def _build_token_index(self) -> dict[str, list[int]]:
        """Token to block indices, so snapping never scans the whole document.

        Without this, a fuzzy pass costs two sequence comparisons per block per quote
        — on a 500-page document with forty quotes that is millions of comparisons to
        protect a single extraction. Sharing a four-character token is a cheap,
        generous prefilter: anything close enough to snap to necessarily shares
        several.
        """
        index: dict[str, list[int]] = {}
        for position, (low, high) in enumerate(self._own):
            for token in set(_TOKEN.findall(self.folded[low:high])):
                index.setdefault(token, []).append(position)
        return index

    # ------------------------------------------------------------------ public

    def locate(self, quote: str) -> Located:
        cleaned = quote.strip()
        if len(cleaned) < MIN_QUOTE_CHARS:
            return Located(
                quote=quote,
                found=False,
                match="none",
                reason=(
                    f"too short to be a citation ({len(cleaned)} chars); "
                    f"quote at least {MIN_QUOTE_CHARS}"
                ),
            )

        needle, _ = _fold(cleaned)
        needle = needle.strip()
        if not needle:
            return Located(quote=quote, found=False, match="none", reason="empty quote")

        exact = self._exact(quote, needle)
        if exact is not None:
            return exact
        return self._snap(quote, needle)

    def locate_all(self, quotes: list[str]) -> list[Located]:
        return [self.locate(quote) for quote in quotes]

    # ----------------------------------------------------------------- matching

    def _exact(self, quote: str, needle: str) -> Located | None:
        first = self.folded.find(needle)
        if first < 0:
            return None

        occurrences = 1
        cursor = self.folded.find(needle, first + 1)
        while cursor >= 0:
            occurrences += 1
            cursor = self.folded.find(needle, cursor + 1)

        span = _to_source_span(
            self.origin, first, first + len(needle), len(self.document.text)
        )
        return self._decorate(
            quote, span, match="exact", similarity=1.0, occurrences=occurrences
        )

    def _snap(self, quote: str, needle: str) -> Located:
        """Align a near-miss to real source text.

        Two-step scoring, because the obvious single-step version does not work. A
        quote is often a clause inside a long paragraph, so comparing it against the
        whole block scores badly no matter how exact the clause is. Instead: align
        first, then score the quote against *only the aligned region*. That handles a
        typo in a short bullet and a verbatim clause inside a long paragraph with the
        same rule, and it is what makes the returned span tight rather than
        block-sized.
        """
        candidates = self._candidates(needle)
        best_ratio = 0.0
        best_span: tuple[int, int] | None = None

        for position in candidates:
            low, high = self._own[position]
            haystack = self.folded[low:high]
            if not haystack.strip():
                continue
            # Too short to contain the quote at all, even allowing for edits.
            if len(haystack) < len(needle) * 0.5:
                continue

            aligned = [
                m
                for m in SequenceMatcher(
                    None, needle, haystack, autojunk=False
                ).get_matching_blocks()
                if m.size
            ]
            if not aligned:
                continue

            region_low, region_high = _expand_to_word_bounds(
                haystack, aligned[0].b, aligned[-1].b + aligned[-1].size
            )
            region = haystack[region_low:region_high]
            # Scored on the expanded region, because that is the span we return. Any
            # other choice reports a similarity for text the caller never receives.
            ratio = SequenceMatcher(None, needle, region, autojunk=False).ratio()

            if ratio >= SNAP_THRESHOLD and ratio > best_ratio:
                best_ratio = ratio
                best_span = _to_source_span(
                    self.origin,
                    low + region_low,
                    low + region_high,
                    len(self.document.text),
                )

        if best_span is None:
            return Located(
                quote=quote,
                found=False,
                match="none",
                reason="does not appear in the document, exactly or approximately",
            )
        return self._decorate(
            quote, best_span, match="snapped", similarity=best_ratio, occurrences=1
        )

    # ------------------------------------------------------------------ helpers

    def _candidates(self, needle: str) -> list[int]:
        """Blocks worth scoring, most promising first.

        Ranked by how many distinct tokens they share with the quote, then capped.
        The cap is what bounds the cost: a quote that shares tokens with hundreds of
        blocks is boilerplate, and the best match is overwhelmingly among the few that
        share the most.
        """
        tokens = set(_TOKEN.findall(needle))
        if not tokens:
            # No token long enough to index on — a very short quote. Scan the start of
            # the document rather than nothing, still bounded.
            return list(range(min(len(self._own), _MAX_SNAP_CANDIDATES)))

        shared: Counter[int] = Counter()
        for token in tokens:
            for position in self._token_index.get(token, ()):
                shared[position] += 1
        return [position for position, _ in shared.most_common(_MAX_SNAP_CANDIDATES)]

    def _folded_index(self, source_offset: int) -> int:
        """The folded index corresponding to a source offset.

        A binary search over `origin`, which is sorted by construction because it is
        built by walking the source text forward.
        """
        low, high = 0, len(self.origin)
        while low < high:
            middle = (low + high) // 2
            if self.origin[middle] < source_offset:
                low = middle + 1
            else:
                high = middle
        return low

    def _decorate(
        self,
        quote: str,
        span: tuple[int, int],
        *,
        match: MatchKind,
        similarity: float,
        occurrences: int,
    ) -> Located:
        """Attach everything the offset makes available for free."""
        start, end = span
        block = self.document.block_at(start)
        page_number = self.document.page_at(start)
        page = next((p for p in self.document.pages if p.number == page_number), None)
        return Located(
            quote=quote,
            found=True,
            match=match,
            span=span,
            text=self.document.text[start:end],
            page=page_number,
            block_id=block.id if block else None,
            occurrences=occurrences,
            similarity=round(similarity, 4),
            ocr_applied=bool(page and page.source.value == "ocr"),
            confidence=(block.confidence if block else None)
            or (page.confidence if page else None),
        )


def locate(document: ParsedDocument, quotes: list[str]) -> list[Located]:
    """Convenience for a one-shot lookup; `Locator` when reusing a document."""
    return Locator(document).locate_all(quotes)
