# parsing-service

Turns an uploaded document into structured content the planning service can consume.
Raw files come from S3, jobs from Kafka, and parsed content goes out over HTTP — this
API is the only path from a raw upload to document content.

Full design, including the parts not built yet: [DESIGN.md](DESIGN.md).

```bash
pip install -e ".[dev,docx]"
python -m pytest -q                              # 92 tests, all offline

python examples/parse_file.py spec.docx          # structure outline + metadata
python examples/parse_file.py spec.docx --text   # the canonical text
python examples/parse_file.py spec.docx --json   # the full artifact
python examples/parse_file.py spec.docx --locate "authenticate users within 300ms"
```

## What is built

| Piece | Status |
|---|---|
| `ParsedDocument` contract | ✅ |
| Canonical text + spans (`parse/serialize.py`) | ✅ |
| Normalisation — de-hyphenation, NFKC, invisibles | ✅ |
| Byte-sniffing format detection | ✅ |
| Plain text / Markdown parser | ✅ |
| DOCX parser | ✅ |
| Quote lookup (`domain/locate.py`) | ✅ |
| PDF, OCR, XLSX | ⬜ |
| Postgres + S3 persistence, status machine | ⬜ |
| Kafka intake, retry tiers, DLQ | ⬜ |
| HTTP API | ⬜ |

Everything built so far is pure: bytes in, artifact out, no I/O. That is what lets
the whole parse stage be tested without infrastructure, and it is what makes replay
safe once the worker exists.

## The one idea worth knowing

The output is **one canonical text** plus **blocks that describe it by character
span**. Not two renderings — one string, with structure pointing into it.

```python
from parse.pipeline import parse_document

doc = parse_document(open("spec.docx", "rb").read(), document_id="d-1")

doc.text                       # the string a model is given
doc.blocks[3].span             # (97, 131) — into doc.text
doc.blocks[3].text             # == doc.text[97:131], always
doc.block_at(112)              # the innermost block at that offset
doc.page_at(112)               # the page, or None for unpaginated formats
doc.heading_path(block.id)     # ["Payments Platform Requirements", "Security"]
```

This exists because of a specific downstream constraint.
`planning-service/src/domain/extraction.py` validates that every extracted
requirement's `quote` appears verbatim in the text the model was shown — the check
its own docstring calls "the single most valuable validator in the extraction stage."
Two consequences shape everything here:

**The text is derived from the block tree, never rendered separately.** Spans are
recorded while *building* the string, so structure and text cannot drift. If they
could, quotes validated against one would fail against the other and that validator
would start rejecting honest work. `tests/test_serialize.py` asserts
`block.text == doc.text[block.span[0]:block.span[1]]` over every block of every
fixture.

**Line-wrap repair is our job.** A PDF yielding `"authenti-\ncation"` produces a
quote no model can reproduce and no downstream normaliser can fix. See
`parse/normalise.py`.

## Quote lookup

`domain/locate.py` is the half of that downstream validator which belongs on this
side — finding a string is text mechanics; deciding what to do when it is missing is
extraction policy.

```python
from domain.locate import Locator

Locator(doc).locate("Rotate API keys every 90 dayz")
# match="snapped"  similarity=0.9655  span=(134, 163)
# text="Rotate API keys every 90 days"   ← the real source text, typo dropped
# page=None  block_id="4890…"  occurrences=1  ocr_applied=False
```

Three things a local `quote in text` cannot give you:

- **`span`** → the page number, block, ordering and a dedup key, all free once you
  have the offset. The model never has to be asked for a page number it gets wrong.
- **`snapped`** → a near-miss returns the *exact source span* instead of failing, so
  one imperfect quote stops discarding a whole extraction. Nothing absent from the
  document is ever returned, so the hallucination guard is intact.
- **`occurrences`** → a quote matching six times (a repeated footer) makes page
  attribution a coin flip, and the caller should know rather than receive the first
  hit as though it were the only one.

## Layout

```
src/domain/
  document.py     ParsedDocument, Block, Page, Metadata — the contract
  locate.py       quote → span, page, block, snapped source text
src/parse/
  base.py         RawBlock, Parser protocol, failure taxonomy
  serialize.py    ★ block tree → canonical text + spans
  normalise.py    de-hyphenation, NFKC, invisible characters, decoding
  detect.py       format from magic bytes and zip contents
  text.py         plain text and Markdown
  docx.py         DOCX
  registry.py     media type → parser, tolerant of missing optional deps
  pipeline.py     bytes → ParsedDocument
tests/            92 tests; fixtures are readable strings, not binaries
```

`serialize.py` is the file to guard hardest: it is the only place spans are produced,
and its bugs are the kind that keep all the content while moving the offsets.

## Conventions

Mirrors the sibling planning-service: `src/` layout with `pythonpath = ["src"]`,
pydantic v2 wire models, ruff at 92 columns, tests offline and dependency-free.
Each parser's dependency is optional (`pip install -e ".[pdf]"`), and a missing one
surfaces as a recorded `unsupported_format` rejection rather than an ImportError in a
worker.
