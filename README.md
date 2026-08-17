# parsing-service

Turns an uploaded document into structured content the planning service can consume.

**Stateless.** A job arrives on Kafka, the raw file is fetched from S3, and the parse
produces two things: a content-addressed artifact in S3 and one outcome event back on
Kafka. There is no database here — the document's status, run history and tenancy live
upstream in the service that owns the upload, and `documents.parse.completed` /
`documents.parse.failed` are the only things it hears from this one.

```bash
pip install -e ".[dev,docx,pdf,xlsx,s3,api,kafka]"
cp .env.example .env                      # then fill in S3_BUCKET and AWS_REGION
python -m pytest -q                       # 277 tests, all offline

PYTHONPATH=src python -m work.main --check         # preflight: S3, broker, topics
PYTHONPATH=src python -m work.main                 # consume events and parse
PYTHONPATH=src python -m work.main --once          # one message, then exit
uvicorn api.main:app --app-dir src --port 8000     # serve parsed content (internal only)

python examples/parse_file.py spec.pdf                        # outline + metadata
python examples/parse_file.py spec.pdf --text                 # the canonical text
python examples/parse_file.py spec.pdf --json                 # the full artifact
python examples/parse_file.py spec.pdf --locate "authenticate users"

python examples/parse_file.py s3://acme-uploads/raw/spec.pdf   # via boto3
python examples/parse_file.py "https://...presigned-url..."    # via plain HTTP
```

## What is built

| Piece | Status |
|---|---|
| `ParsedDocument` contract | ✅ |
| Canonical text + spans (`parse/serialize.py`) | ✅ |
| Normalisation — de-hyphenation, NFKC, invisibles | ✅ |
| Byte-sniffing format detection | ✅ |
| Plain text / Markdown · DOCX · **PDF** · **XLSX** · **HTML** · **CSV** | ✅ |
| Quote lookup (`domain/locate.py`) | ✅ |
| **Fetch from S3, presigned URLs, local — with SSRF guards and size caps** | ✅ |
| PDF table extraction (ruled tables, de-duplicated from the text flow) | ✅ |
| OCR — pluggable backend, per-page, confidence to the consumer | ✅ |
| Worker: fetch → parse → store → announce → commit, retry tiers, DLQ | ✅ |
| Content-addressed artifact store (S3 + local) | ✅ |
| Idempotency from the artifact key — no database | ✅ |
| Outcome events: `parse.completed`, `parse.failed` | ✅ verified against a live broker |
| HTTP API — content, text, locate, page images | ✅ |
| Kafka/Redpanda adapter | ✅ verified against a live broker |
| Config from `.env`, worker and API entrypoints | ✅ |
| Metrics endpoint, tracing | ⬜ |

The parse stage stays pure — bytes in, artifact out, no I/O — which is what lets it be
tested without infrastructure and what makes job replay safe. Everything that touches the
world (`src/store/`, `src/work/`) sits outside it.

**Why there is no database.** Parsing is a pure function of the raw bytes, the parser
version and the parse options, and the artifact key is built from exactly those three
things. So "have we already done this?" is a `HeadObject`, not a row — which is what the
claim gate, the lease and the `parse_runs` unique constraint used to answer. Everything
else the tables held was a *query surface*, and a query surface belongs with the service
that owns the documents. `tesseract` is still absent from the environment this was built
in, so OCR runs against a fake backend in tests; Kafka and S3 are both verified live.

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

## The upload event

The backend publishes one message per uploaded file:

```json
{
  "event_id":     "e1d2feeb-…",
  "document_id":  "c98f59f8-…",
  "tenant_id":    "11111111-…",
  "project_id":   "0c9d0601-…",
  "s3_bucket":    "eos-s3",
  "s3_key":       "uploads/{tenant}/{project}/{document}.txt",
  "filename":     "sample.txt",
  "content_type": "text/plain",
  "size":         77
}
```

`Job.from_bytes` reads this **and** this service's own retry/DLQ serialisation, dispatching
on whether `reference` is present — one consumer group reads both topics, and a replayed
DLQ message then works wherever it is published. `document_id` is the partition key, so
every job for one document lands on one partition and concurrent processing of it is
structurally impossible.

Four fields are required (`document_id`, `s3_bucket`, `s3_key`, `tenant_id`) and a message
missing any of them is dead-lettered by name rather than retried — republishing an event
with no `s3_key` produces the same event. Unknown fields are ignored, so the producer can
add one without a coordinated deploy.

`force: true` and `parse_options` are read from this event too, which is what makes a
reprocess work: without `force`, a re-request for bytes already parsed at this version finds
the artifact in place and is answered from it rather than re-parsed.

Try it without a broker:

```bash
PYTHONPATH=src python -m work.main --event event.json
```

`--check` is the preflight worth running first. It resolves the config, does a
`HeadBucket`, and probes the broker — including three failures that otherwise look like a
broken client:

- **the topic does not exist**, so the worker would start cleanly, log nothing, and
  process nothing forever;
- **the broker advertises a loopback address**, so metadata requests succeed while every
  produce and consume connects to `127.0.0.1` and is refused. Fixed on the broker with
  `advertised.listeners=PLAINTEXT://<its-own-ip>:9092`, not in this client;
- **an outcome topic does not exist**, which on a broker with auto-creation disabled means
  every completed or failed parse is published into nothing. Since the event is now the only
  record, that is a document upstream never hears about at all.

## The outcome events

Two topics, and between them they are the entire output of this service besides the artifact.
Both are keyed on `document_id`, matching the requested topic, so a consumer joining the
streams sees them partitioned the same way.

`documents.parse.completed` carries everything needed to act without a round-trip:

```json
{
  "document_id": "ecce67d1-…", "tenant_id": "11111111-…", "project_id": "0c9d0601-…",
  "run_id": "b713ad83-…",
  "content_hash": "dc7720161209a62b…",
  "artifact_key": "parsed/dc7720161209a62b…/1.0/document.json",
  "status": "ready",
  "metadata": { "format": "pdf", "page_count": 2, "char_count": 215, "ocr_applied": false },
  "warnings": [], "duplicate": false,
  "completed_at": "2026-08-17T11:00:56.776983+00:00",
  "event_id": "…", "trace_id": "…"
}
```

`documents.parse.failed` is emitted only on a **terminal** outcome — a permanent failure, or
transient retries exhausted. An intermediate transient failure announces nothing, because it
is still being worked on and flapping a document into `failed` would show a user a failure
that fixes itself thirty seconds later.

```json
{
  "document_id": "e0992264-…", "status": "failed",
  "failure_class": "not_found",
  "failure_reason": "s3://…/definitely-not-here.pdf does not exist",
  "permanent": true, "attempt": 1,
  "run_id": null, "content_hash": null,
  "tenant_id": "11111111-…", "failed_at": "…"
}
```

`permanent: false` means the tiers ran out rather than the document being unparseable —
usually our infrastructure, and often worth replaying after a fix. `run_id` and
`content_hash` are null when the failure happened before any bytes were fetched, which is
what separates "could not read the file" from "read it and could not parse it".

**A duplicate is still announced.** The old worker answered a replay from the database and
published nothing. With events as the only output that silence is unsafe: a lost message
would be unrecoverable, because republishing the request would find the artifact present and
say nothing at all. So a replay re-reads the existing artifact and re-emits the same event
with `duplicate: true`. One GET, and replay becomes a working recovery path.

## Tenancy

`tenant_id` arrives on the upload event, travels through the job, and leaves on the outcome
event — which is the only place it exists once this service commits the offset. Drop it
anywhere along that path and upstream receives a result it cannot attribute to anyone.

**This service enforces nothing**, because it has no store to enforce against. Upstream owns
the documents table and therefore owns tenancy. One consequence is a security property worth
stating plainly:

> Content is addressed by hash, so a `content_hash` is a bearer capability, and two tenants
> who upload the same file share one artifact. The read API cannot check that a caller is
> entitled to a hash — there is no row to check against. Keep it on an internal network with
> upstream proxying reads, or namespace artifact keys per tenant and give up cross-tenant
> dedup. An unguessable hash is not access control: hashes travel in events, logs and traces.

`X-Tenant-Id` is deliberately *not* accepted. A header this service cannot verify would look
like an authorisation check while being none, which is worse than plainly having no check.

## Fetching the raw file

Jobs reference documents as `s3://bucket/key`, as a presigned `https://` URL, or as a
local path. `s3://` uses the worker's IAM role; a presigned URL is fetched with no
credentials, because the signature in the query string *is* the authorisation.

Three properties matter more than the plumbing:

- **The size cap is counted byte by byte**, never taken from `Content-Length`. A declared
  length is a claim, and a lying or absent one would otherwise be an OOM in a worker.
- **Failures are classified.** A 5xx is transient and walks the retry tiers; a 404 or an
  expired signature is permanent and goes to the DLQ with a reason. `ServiceError`
  carries `transient` and `failure_class` so the worker never has to guess.
- **URLs are checked against the addresses they resolve to** — see below.

### The SSRF guard

A URL arriving in a job message traces back to something a user influenced. Without a
guard, `http://169.254.169.254/latest/meta-data/iam/security-credentials/` makes the
worker fetch its *own IAM credentials*, parse them as a document, store them as parsed
content, and serve them over the content API. Credential exfiltration dressed as a text
file. So `store/net.py`:

- resolves the hostname and rejects unless **every** resolved address is public — a
  public-looking name proves nothing, since an attacker controls their own DNS;
- **refuses redirects**, since a 302 to the metadata address defeats a check applied only
  to the original URL;
- allows `https` only, with an optional host allowlist;
- has an explicit `allow_private_networks` escape hatch for deployments behind an S3 VPC
  endpoint, off by default.

The residual DNS-rebinding window is documented in that module rather than papered over.

## Layout

```
src/domain/
  document.py     ParsedDocument, Block, Page, Metadata — the contract
  errors.py       one failure taxonomy: transient vs permanent, failure_class
  locate.py       quote → span, page, block, snapped source text
src/parse/
  base.py         RawBlock, Parser protocol
  serialize.py    ★ block tree → canonical text + spans
  normalise.py    de-hyphenation, NFKC, invisible characters, decoding
  detect.py       format from magic bytes and zip contents
  text.py docx.py pdf.py xlsx.py html.py csv.py
  registry.py     media type → parser, tolerant of missing optional deps
  pipeline.py     bytes → ParsedDocument
src/store/
  refs.py         s3:// · presigned https:// · file:// → a typed reference
  net.py          the SSRF guard
  blobs.py        fetch with size caps, streaming hash, classified failures
  artifacts.py    ★ the content-addressed key — which is also the idempotency check
src/work/
  queue.py        Job · CompletionEvent · FailureEvent · retry tiers
  worker.py       fetch → dedup → parse → store → announce
  kafka.py        the broker adapter
src/api/          content · text · locate · page images, all addressed by hash
tests/            277 tests; fixtures are generated in code, not binary blobs
```

`serialize.py` is the file to guard hardest: it is the only place spans are produced,
and its bugs are the kind that keep all the content while moving the offsets.

`artifacts.py` is second. The key it builds is now load-bearing twice over — it addresses
the object *and* answers "already parsed?" — so a key that does not include everything the
parse depends on means either a re-parse that never happens or one that silently overwrites
a different result. That is why `parse_options` is a segment of it, and why the worker's
`parser_version` overrides each parser's own: if the version in the key and the version in
the metadata could differ, the dedup check would read one key and the write would go to
another, so nothing would ever deduplicate.

## A note on PDF

Everything a PDF parser knows is inferred — the file contains glyphs at coordinates, not
headings. Two things this parser does that are easy to get wrong:

Font size is read with `FPDFText_GetFontSize`, not measured from the glyph box. Measured
by glyph height, "Registration" and "Performance" at the same 14pt come out as 8.59 and
7.73 — a 10% gap caused only by the descender in "g" — which ranks two sibling headings as
two different levels and nests one section inside the other.

A page is flagged for OCR only when it has thin text **and** contains an image. Thin text
alone is a section divider or a short page, and flagging it would both cry wolf and route
a readable page into the OCR lane at 10-100x the cost. A page with neither text nor images
is reported as `blank_page`, because a broken export and a scan need different responses.

Ruled tables are extracted with pdfplumber and **their lines are removed from the text
flow**. That removal is the whole difficulty: pdfium's text layer contains the cell text
too, so keeping both emits every cell twice — once structured, once as stray paragraphs.
The two libraries disagree on coordinates (pdfplumber measures from the top of the page,
pdfium from the bottom), so the table's box is converted before deciding which lines it
covers, and the table is then re-inserted at its own vertical position rather than
appended — a requirements table detached from its heading has lost what made it mean
anything. Only ruled tables are detected; whitespace-aligned columns are ambiguous, and a
wrongly-detected table swallows prose into cells. `PdfParser(extract_tables=False)` turns
the second pass off.

## Conventions

Mirrors the sibling planning-service: `src/` layout with `pythonpath = ["src"]`,
pydantic v2 wire models, ruff at 92 columns, tests offline and dependency-free.
Each parser's dependency is optional (`pip install -e ".[pdf]"`), and a missing one
surfaces as a recorded `unsupported_format` rejection rather than an ImportError in a
worker.
