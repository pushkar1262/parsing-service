#!/usr/bin/env python3
"""Parse a file and show what the planning service would receive.

    python examples/parse_file.py spec.pdf                       # outline + metadata
    python examples/parse_file.py spec.pdf --text                # the canonical text
    python examples/parse_file.py spec.pdf --json                # the full artifact
    python examples/parse_file.py spec.pdf --locate "authenticate users"

    python examples/parse_file.py s3://acme-uploads/raw/spec.pdf  # via boto3
    python examples/parse_file.py "https://...presigned..."       # via plain HTTP

The same fetch → parse path the worker runs, minus the queue and the database. Local
paths are read through the real `Storage` policy rather than a shortcut, so the jail and
the size cap are exercised here too.

`--locate` is the mode worth trying on a real document: it shows the quote lookup that
replaces a downstream substring test, with the page and block each quote resolves to.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from domain.document import BlockType
from domain.errors import CorruptDocument, ServiceError, UnsupportedFormat
from domain.locate import Locator
from parse.detect import detect
from parse.pipeline import parse_document
from store.blobs import FetchPolicy, Storage
from store.net import NetPolicy
from store.refs import parse_ref

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


def _fetch(reference: str, *, allow_private: bool):
    """Fetch through the same Storage the worker will use.

    Local paths get a jail rooted at the current directory rather than being read
    directly, so the CLI exercises the real policy path instead of a shortcut that
    would let a bug in the jail go unnoticed until production.
    """
    ref = parse_ref(reference)
    roots = (Path.cwd(), Path(reference).expanduser().resolve().parent) if ref.kind == "file" else ()
    policy = FetchPolicy(
        local_roots=roots,
        net=NetPolicy(allow_private_networks=allow_private),
    )
    return Storage(policy).fetch(ref)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "reference",
        help="a local path, an s3://bucket/key reference, or a presigned https URL",
    )
    parser.add_argument("--text", action="store_true", help="print the canonical text")
    parser.add_argument("--json", action="store_true", help="print the full artifact")
    parser.add_argument(
        "--locate", action="append", default=[], metavar="QUOTE",
        help="resolve a quote to its span, page and block (repeatable)",
    )
    parser.add_argument(
        "--allow-private",
        action="store_true",
        help="permit URLs resolving to private addresses (needed behind a VPC endpoint)",
    )
    args = parser.parse_args()

    try:
        fetched = _fetch(args.reference, allow_private=args.allow_private)
    except ServiceError as exc:
        kind = "retryable" if exc.transient else "permanent"
        print(f"fetch failed ({kind}): {exc.failure_class} — {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"bad reference: {exc}", file=sys.stderr)
        return 2

    name = Path(fetched.ref.key or fetched.ref.describe).name
    try:
        document = parse_document(
            fetched.data,
            document_id=Path(name).stem or "document",
            content_hash=fetched.content_hash,
            media_type=fetched.declared_media_type,
            filename=name,
            source=fetched.source,
        )
    except UnsupportedFormat as exc:
        print(f"rejected: unsupported_format — {exc}", file=sys.stderr)
        print(
            f"          bytes sniffed as {detect(fetched.data, filename=name)}",
            file=sys.stderr,
        )
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
