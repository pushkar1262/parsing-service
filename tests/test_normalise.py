"""The repairs that decide whether an honest quote survives validation downstream.

Each of these corresponds to a named artifact in the planning service's own
normaliser docstring. The point of fixing them here is that a consumer comparing
strings cannot: once a word has been split across a line, nothing downstream can
tell a wrap-hyphen from a real one.
"""

from __future__ import annotations

from parse.normalise import clean_inline, decode, join_wrapped, normalise
from parse.pipeline import parse_document
from tests.conftest import WRAPPED, assert_spans_hold, parse

# --------------------------------------------------------------------------- #
# the repairs, in isolation
# --------------------------------------------------------------------------- #


def test_a_word_wrapped_with_a_hyphen_is_rejoined() -> None:
    assert join_wrapped("authenti-\ncation") == "authentication"


def test_a_hyphenated_compound_survives_a_line_break() -> None:
    """`join_wrapped` must not eat hyphens that belong to the word.

    "single-\\nsign-on" is a wrap inside a genuinely hyphenated term. Rejoining it
    to "singlesign-on" would corrupt the text as surely as leaving the break in, so
    the rule only fires between word characters and keeps the rest intact.
    """
    assert join_wrapped("multi-\ntenant isolation") == "multitenant isolation"
    # A hyphen with whitespace already around it is not a wrap and is left alone.
    assert join_wrapped("opt - in") == "opt - in"


def test_ligatures_are_decomposed_by_nfkc() -> None:
    assert normalise("The ﬁrst and ﬂat cases") == "The first and flat cases"


def test_a_non_breaking_space_becomes_a_plain_space() -> None:
    assert normalise("300\u00a0ms") == "300 ms"


def test_a_soft_hyphen_is_removed_entirely() -> None:
    """The nastiest of the set: invisible, mid-word, and NFKC does not touch it."""
    assert normalise("authen\u00adtication") == "authentication"


def test_zero_width_characters_are_removed() -> None:
    assert normalise("TLS\u200b1.3") == "TLS1.3"


def test_smart_quotes_are_preserved() -> None:
    """We fix our artifacts, not the author's punctuation.

    Flattening these would be lossy for display, and the downstream normaliser
    already folds them for comparison purposes.
    """
    assert normalise("the “platform” team’s scope") == (
        "the “platform” team’s scope"
    )


def test_control_characters_are_stripped_but_newlines_survive() -> None:
    assert normalise("a\x00b\x07\nc") == "ab\nc"


def test_crlf_and_unicode_line_separators_fold_to_newline() -> None:
    assert normalise("a\r\nb\u2028c\u2029d") == "a\nb\nc\nd"


def test_clean_inline_flattens_a_fragment_to_one_line() -> None:
    assert clean_inline("  a\n  b\t\tc  ") == "a b c"


# --------------------------------------------------------------------------- #
# end to end, through a document
# --------------------------------------------------------------------------- #


def test_a_hard_wrapped_paragraph_becomes_one_quotable_line() -> None:
    doc = parse(WRAPPED)
    assert_spans_hold(doc)
    # The requirement reads as one sentence, so a model can quote it as one.
    assert (
        "The service shall authenticate every request before it reaches the "
        "payment gateway." in doc.text
    )
    assert "authenti-" not in doc.text


def test_the_ligature_is_gone_from_the_canonical_text() -> None:
    doc = parse(WRAPPED)
    assert "The first release covers card payments only." in doc.text
    assert "ﬁ" not in doc.text


# --------------------------------------------------------------------------- #
# decoding
# --------------------------------------------------------------------------- #


def test_utf8_decodes_without_a_warning() -> None:
    text, warning = decode("Résumé — 300ms".encode())
    assert text == "Résumé — 300ms"
    assert warning is None


def test_a_utf8_bom_is_not_left_in_the_text() -> None:
    text, warning = decode("\ufeffHeading".encode("utf-8"))
    assert text == "Heading"
    assert warning is None


def test_utf16_is_detected_by_its_bom() -> None:
    text, warning = decode("Ünicode".encode("utf-16"))
    assert text == "Ünicode"
    assert warning is None


def test_cp1252_is_recovered_and_flagged_as_a_guess() -> None:
    """A curly quote in cp1252 is invalid UTF-8, and the fallback must say so.

    The warning is the point: the text is usable, but a consumer comparing quotes
    should know the bytes were interpreted rather than decoded.
    """
    text, warning = decode(b"the \x93platform\x94 team")
    assert text == "the “platform” team"
    assert warning == "encoding_guessed"


def test_a_guessed_encoding_reaches_the_documents_warnings() -> None:
    """Imperfect input is recorded on the artifact, not swallowed.

    These warnings are the first thing to read when a downstream extraction looks
    thinner than the document deserved.
    """
    doc = parse_document(b"the \x93platform\x94 team", document_id="d", filename="a.txt")
    assert [w.code for w in doc.warnings] == ["encoding_guessed"]
