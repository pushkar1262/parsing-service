"""Fixture documents, and the assertion every one of them has to survive.

These are strings rather than binary files on purpose: Markdown is the format whose
input structure matches the canonical output structure, so a text fixture exercises
the whole representation — headings, nesting, lists, tables, spans — with nothing to
check in that a reviewer cannot read in the diff. Binary fixtures come in with the
PDF and DOCX parsers, where there is no other option.
"""

from __future__ import annotations

import pytest

from domain.document import BlockType, ParsedDocument
from parse.pipeline import parse_document

RICH = """\
# Payments Platform Requirements

The system must authenticate users within 300ms.
Sessions expire after 30 minutes of inactivity.

## Security

- Encrypt all traffic with TLS 1.3
- Rotate API keys every 90 days
  - Notify owners 7 days before rotation
- Log every authentication attempt

### Compliance

| Control | Standard | Owner |
| --- | --- | --- |
| Audit log retention | SOC 2 | Platform |
| Data residency | GDPR | Legal |

## Performance

Response times shall not exceed 500ms at p99.

```python
def health():
    return {"ok": True}
```
"""

# A hard-wrapped export with a hyphenated line break and a ligature — the two
# artifacts that make an honest quote fail an exact-match check downstream.
WRAPPED = """\
Requirements

The service shall authenti-
cate every request before it
reaches the payment gateway.

The ﬁrst release covers card payments only.
"""

PLAIN = """\
Just two paragraphs here, with no markup at all.

The second one mentions a 99.9% availability target.
"""

ORDERED = """\
1. Collect the requirements
2. Draft the architecture
3. Review with the team
"""


def parse(text: str, *, document_id: str = "doc-1", filename: str = "spec.md"):
    return parse_document(
        text.encode("utf-8"), document_id=document_id, filename=filename
    )


def own_line(block) -> str:
    """A block's own first rendered line, with any list marker stripped.

    Needed because `list_item` spans the list nested beneath it, so a parent item's
    `text` contains its children's text too. Selecting blocks by `in` therefore
    matches the parent as well as the child, which is a trap worth having exactly one
    helper for rather than rediscovering per test.
    """
    first = block.text.strip().split("\n", 1)[0]
    return first.lstrip("#-*+ ").strip()


def assert_spans_hold(doc: ParsedDocument) -> None:
    """The invariant the whole contract rests on.

    If a block's recorded span does not slice back to its own text, then any
    consumer resolving a quote to a page or a section is reading the wrong part of
    the document — and nothing about the failure looks like a parsing bug from the
    outside. This is asserted over every fixture in every test that builds a
    document.
    """
    for block in doc.blocks:
        assert block.text == doc.text[block.start : block.end], (
            f"block {block.id} ({block.type.value}) span {block.span} does not "
            f"match its text: {block.text!r} != {doc.text[block.start:block.end]!r}"
        )
        assert 0 <= block.start <= block.end <= len(doc.text)

    # Document order, and a parent always recorded before its children.
    seen: set[str] = set()
    for block in doc.blocks:
        if block.parent_id is not None:
            assert block.parent_id in seen, (
                f"block {block.id} precedes its parent {block.parent_id}"
            )
        seen.add(block.id)

    # A container's span must cover its children's; a heading's must not, because a
    # heading introduces its section rather than containing it.
    by_id = {b.id: b for b in doc.blocks}
    for block in doc.blocks:
        if block.parent_id is None:
            continue
        parent = by_id[block.parent_id]
        if parent.type in (BlockType.LIST, BlockType.LIST_ITEM):
            assert parent.start <= block.start and block.end <= parent.end, (
                f"{block.type.value} escapes its {parent.type.value} parent"
            )


@pytest.fixture
def rich() -> ParsedDocument:
    doc = parse(RICH)
    assert_spans_hold(doc)
    return doc
