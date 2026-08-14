#!/usr/bin/env python3
"""Parse a file and show what the planning service would receive.

    python examples/parse_file.py spec.docx              # outline + metadata
    python examples/parse_file.py spec.docx --text       # the canonical text
    python examples/parse_file.py spec.docx --json       # the full artifact
    python examples/parse_file.py spec.docx --locate "authenticate users"

No network, no database, no queue: this is `parse.pipeline` on a local file, which
is the same call the worker makes after fetching from blob storage. The `--locate`
mode is the one worth trying on a real document — it shows the quote lookup that
replaces a downstream substring test, with the page and block it resolves to.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from domain.document import BlockType
from domain.locate import Locator
from parse.base import CorruptDocument, UnsupportedFormat
from parse.detect import detect
from parse.pipeline import parse_document

_GLYPH = {
    BlockType.HEADING: "H",
    BlockType.PARAGRAPH: "¶",
    BlockType.LIST: "≡",
    BlockType.LIST_ITEM: "•",
    BlockType.TABLE: "▦",
    BlockType.CODE: "{}",
    BlockType.CAPTION: "c",
    BlockType.FOOTNOTE: "f",
    BlockType.FIGURE: "img",
}


def outline(document) -> None:
    print(f"\n{'span':>13}  {'pg':>3}  type        text")
    print("-" * 78)
    for block in document.blocks:
        first = block.text.strip().split("\n", 1)[0]
        if len(first) > 44:
            first = first[:41] + "…"
        page = str(block.page) if block.page is not None else "-"
        indent = "  " * max(block.depth - 1, 0) if block.type in (
            BlockType.LIST_ITEM,
            BlockType.HEADING,
        ) else ""
        print(
            f"{block.start:6d}-{block.end:<6d} {page:>3}  "
            f"{_GLYPH.get(block.type, '?'):<3} {indent}{first}"
        )


def summary(document) -> None:
    meta = document.metadata
    print(f"document_id   {document.document_id}")
    print(f"content_hash  {document.content_hash[:16]}…")
    print(f"format        {meta.format}  (parser {meta.parser_name}/{meta.parser_version})")
    print(f"title         {meta.title or '-'}")
    print(f"author        {meta.author or '-'}")
    print(f"pages         {meta.page_count if meta.page_count is not None else '-'}")
    print(f"chars/words   {meta.char_count} / {meta.word_count}")
    print(
        f"structure     {meta.block_count} blocks, {meta.heading_count} headings, "
        f"{meta.table_count} tables"
    )
    if meta.ocr_applied:
        print(f"ocr           applied to {meta.ocr_page_count} page(s)")
    for warning in document.warnings:
        where = f" (page {warning.page})" if warning.page else ""
        print(f"warning       [{warning.code}]{where} {warning.message}")


def show_locate(document, quotes: list[str]) -> None:
    locator = Locator(document)
    print()
    for quote in quotes:
        found = locator.locate(quote)
        label = {"exact": "EXACT", "snapped": "SNAPPED", "none": "NOT FOUND"}[found.match]
        print(f"{label:<10} {quote!r}")
        if found.found:
            page = f"page {found.page}" if found.page is not None else "page -"
            print(f"           span {found.span}  {page}  block {found.block_id}")
            print(f"           similarity {found.similarity}  occurrences {found.occurrences}")
            if found.ocr_applied:
                print(f"           from OCR text, confidence {found.confidence}")
            if found.text != quote:
                print(f"           source text: {found.text!r}")
        else:
            print(f"           {found.reason}")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("path", type=Path)
    parser.add_argument("--text", action="store_true", help="print the canonical text")
    parser.add_argument("--json", action="store_true", help="print the full artifact")
    parser.add_argument(
        "--locate", action="append", default=[], metavar="QUOTE",
        help="resolve a quote to its span, page and block (repeatable)",
    )
    args = parser.parse_args()

    if not args.path.is_file():
        print(f"not a file: {args.path}", file=sys.stderr)
        return 2

    data = args.path.read_bytes()
    try:
        document = parse_document(
            data, document_id=args.path.stem, filename=args.path.name
        )
    except UnsupportedFormat as exc:
        print(f"rejected: unsupported_format — {exc}", file=sys.stderr)
        print(f"          bytes sniffed as {detect(data, filename=args.path.name)}", file=sys.stderr)
        return 1
    except CorruptDocument as exc:
        print(f"rejected: corrupt — {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(document.model_dump_json(indent=2))
        return 0

    summary(document)
    if args.locate:
        show_locate(document, args.locate)
    elif args.text:
        print()
        print(document.text)
    else:
        outline(document)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
