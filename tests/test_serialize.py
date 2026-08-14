"""The serialiser's guarantees, asserted directly.

Every test here is about the relationship between `text` and `blocks`. A parser bug
loses content and is obvious; a serialiser bug keeps all the content and moves the
offsets, which is invisible until a downstream consumer resolves a quote to the
wrong page. These are the tests that make that class of bug loud.
"""

from __future__ import annotations

from domain.document import BlockType, TableData
from parse.base import PageInfo, RawBlock
from parse.serialize import render_table, serialize
from tests.conftest import ORDERED, PLAIN, RICH, WRAPPED, assert_spans_hold, parse

# --------------------------------------------------------------------------- #
# the invariant
# --------------------------------------------------------------------------- #


def test_spans_slice_back_to_block_text_for_every_fixture() -> None:
    for source in (RICH, WRAPPED, PLAIN, ORDERED):
        assert_spans_hold(parse(source))


def test_empty_input_produces_an_empty_document() -> None:
    doc = parse("")
    assert doc.text == ""
    assert doc.blocks == []
    assert doc.metadata.char_count == 0


def test_whitespace_only_input_produces_no_blocks() -> None:
    doc = parse("   \n\n\t\n   ")
    assert doc.blocks == []


# --------------------------------------------------------------------------- #
# determinism — the property content-addressing depends on
# --------------------------------------------------------------------------- #


def test_the_same_bytes_serialise_identically() -> None:
    first = parse(RICH)
    second = parse(RICH)
    assert first.text == second.text
    assert first.content_hash == second.content_hash
    assert [b.id for b in first.blocks] == [b.id for b in second.blocks]
    assert [b.span for b in first.blocks] == [b.span for b in second.blocks]


def test_block_ids_are_derived_from_content_not_position_alone() -> None:
    """Two different documents must not hand out the same block ids.

    Ids are `hash(content_hash, ordinal)`. If they were ordinal alone they would be
    stable and useless — every document's first block would share an id, and a
    cross-document reference would silently resolve to the wrong block.
    """
    first = parse(RICH, document_id="a")
    other = parse(PLAIN, document_id="b")
    assert not {b.id for b in first.blocks} & {b.id for b in other.blocks}


# --------------------------------------------------------------------------- #
# structure
# --------------------------------------------------------------------------- #


def test_headings_nest_by_level_not_by_document_order(rich) -> None:
    headings = {
        b.text.lstrip("# ").strip(): b for b in rich.blocks if b.type is BlockType.HEADING
    }
    by_id = {b.id: b for b in rich.blocks}

    security = headings["Security"]
    compliance = headings["Compliance"]
    performance = headings["Performance"]

    assert by_id[security.parent_id].text.endswith("Payments Platform Requirements")
    assert by_id[compliance.parent_id] is security
    # `## Performance` follows `### Compliance`, so it must close back up to the h1
    # rather than nesting under the deeper heading that preceded it.
    assert by_id[performance.parent_id].text.endswith("Payments Platform Requirements")


def test_a_headings_span_covers_only_its_own_line(rich) -> None:
    heading = next(b for b in rich.blocks if b.text == "## Security")
    assert rich.text[heading.start : heading.end] == "## Security"
    assert "Encrypt all traffic" not in heading.text


def test_a_lists_span_covers_all_of_its_items(rich) -> None:
    lists = [b for b in rich.blocks if b.type is BlockType.LIST]
    outer = lists[0]
    assert "Encrypt all traffic with TLS 1.3" in outer.text
    assert "Log every authentication attempt" in outer.text
    # And the nested item is inside the outer list's span too.
    assert "Notify owners 7 days before rotation" in outer.text


def test_nested_list_items_carry_increasing_depth(rich) -> None:
    items = [b for b in rich.blocks if b.type is BlockType.LIST_ITEM]
    # Select on the item's own rendered line, not on containment: a `list_item`
    # spans its nested list, so the *parent* item's text contains the child's too.
    nested = next(b for b in items if b.text.strip().startswith("- Notify owners"))
    assert nested.depth == 2
    assert [b.depth for b in items if b.id != nested.id] == [1, 1, 1]

    by_id = {b.id: b for b in rich.blocks}
    inner_list = by_id[nested.parent_id]
    assert inner_list.type is BlockType.LIST
    assert by_id[inner_list.parent_id].text.startswith("- Rotate API keys")


def test_an_items_span_covers_the_list_nested_under_it(rich) -> None:
    """Documenting the containment rule, since it surprises on first reading.

    `list_item` is in `_SPANS_CHILDREN`, so slicing a parent item returns its own
    line *and* the sub-list beneath it. That is what makes "give me this bullet and
    everything under it" a slice, and it is why a test selecting blocks by
    substring has to be careful about which of the two it means.
    """
    parent = next(
        b for b in rich.blocks if b.text.startswith("- Rotate API keys every 90 days")
    )
    assert parent.text == (
        "- Rotate API keys every 90 days\n  - Notify owners 7 days before rotation"
    )
    assert parent.text == rich.text[parent.start : parent.end]


def test_list_items_render_tight_not_blank_line_separated(rich) -> None:
    assert "- Encrypt all traffic with TLS 1.3\n- Rotate API keys every 90 days" in rich.text


def test_ordered_lists_keep_their_numbering() -> None:
    doc = parse(ORDERED)
    assert_spans_hold(doc)
    assert "1. Collect the requirements" in doc.text
    assert "3. Review with the team" in doc.text


def test_code_blocks_keep_their_indentation(rich) -> None:
    code = next(b for b in rich.blocks if b.type is BlockType.CODE)
    assert '    return {"ok": True}' in code.text
    assert code.attrs["lang"] == "python"


# --------------------------------------------------------------------------- #
# tables — the reason a table-borne requirement can be quoted at all
# --------------------------------------------------------------------------- #


def test_tables_are_rendered_into_the_canonical_text(rich) -> None:
    """A table that exists only in `Block.table` is invisible to the consumer.

    The planning service validates quotes against the canonical text, so a
    requirement stated in a table row can only survive validation if the row is
    present in that string.
    """
    assert "Audit log retention" in rich.text
    assert "| Control | Standard | Owner |" in rich.text


def test_tables_keep_their_cells_as_data_too(rich) -> None:
    table = next(b for b in rich.blocks if b.type is BlockType.TABLE)
    assert table.table is not None
    assert table.table.rows[0] == ["Control", "Standard", "Owner"]
    assert table.table.rows[1] == ["Audit log retention", "SOC 2", "Platform"]
    assert table.table.shape == (3, 3)


def test_a_pipe_inside_a_cell_survives_the_round_trip() -> None:
    """Escaping has to be reversible or every following column shifts."""
    table = TableData(rows=[["a", "b"], ["x | y", "z"]])
    rendered = render_table(table)
    doc = parse(rendered)
    assert_spans_hold(doc)
    parsed = next(b for b in doc.blocks if b.type is BlockType.TABLE)
    assert parsed.table is not None
    assert parsed.table.rows[1] == ["x | y", "z"]


def test_a_ragged_table_is_padded_rather_than_dropped() -> None:
    rendered = render_table(TableData(rows=[["a", "b", "c"], ["1"]]))
    assert rendered.splitlines()[-1] == "| 1 |  |  |"


# --------------------------------------------------------------------------- #
# pages — computed from block attribution, never declared
# --------------------------------------------------------------------------- #


def test_page_spans_are_derived_from_the_blocks_on_each_page() -> None:
    blocks = [
        RawBlock(type=BlockType.PARAGRAPH, text="First page sentence.", page=1),
        RawBlock(type=BlockType.PARAGRAPH, text="Also page one.", page=1),
        RawBlock(type=BlockType.PARAGRAPH, text="Second page sentence.", page=2),
    ]
    out = serialize(blocks, content_hash="deadbeef")

    assert [p.number for p in out.pages] == [1, 2]
    first, second = out.pages
    assert out.text[first.span[0] : first.span[1]].startswith("First page sentence.")
    assert out.text[first.span[0] : first.span[1]].endswith("Also page one.")
    assert out.text[second.span[0] : second.span[1]] == "Second page sentence."
    assert first.char_count == first.span[1] - first.span[0]


def test_a_quotes_offset_resolves_to_its_page() -> None:
    """Page attribution as a lookup — the mechanism `/locate` is built on."""
    blocks = [
        RawBlock(type=BlockType.PARAGRAPH, text="Nothing of note here.", page=3),
        RawBlock(
            type=BlockType.PARAGRAPH,
            text="The service shall authenticate users within 300ms.",
            page=4,
        ),
    ]
    out = serialize(blocks, content_hash="cafe")

    quote = "authenticate users within 300ms"
    offset = out.text.index(quote)
    page = next(p.number for p in out.pages if p.span[0] <= offset < p.span[1])
    assert page == 4


def test_ocr_provenance_reaches_the_page_record() -> None:
    """The flag that tells a downstream validator how much to trust a quote."""
    blocks = [RawBlock(type=BlockType.PARAGRAPH, text="Scanned line.", page=1)]
    out = serialize(
        blocks,
        content_hash="beef",
        page_meta={1: PageInfo(source="ocr", image_key="pages/1.webp", confidence=0.72)},
    )
    assert out.pages[0].source.value == "ocr"
    assert out.pages[0].image_key == "pages/1.webp"
    assert out.pages[0].confidence == 0.72


def test_child_blocks_inherit_their_parents_page() -> None:
    listing = RawBlock(type=BlockType.LIST, page=7)
    listing.add(RawBlock(type=BlockType.LIST_ITEM, text="An item", depth=1))
    out = serialize([listing], content_hash="feed")
    assert all(b.page == 7 for b in out.blocks)


# --------------------------------------------------------------------------- #
# pruning
# --------------------------------------------------------------------------- #


def test_blocks_that_would_render_to_nothing_are_dropped() -> None:
    """A zero-width span is a block no lookup can ever return."""
    blocks = [
        RawBlock(type=BlockType.PARAGRAPH, text="Kept."),
        RawBlock(type=BlockType.PARAGRAPH, text="   "),
        RawBlock(type=BlockType.LIST),  # a container whose items all vanished
    ]
    out = serialize(blocks, content_hash="abc")
    assert len(out.blocks) == 1
    assert out.text == "Kept."
