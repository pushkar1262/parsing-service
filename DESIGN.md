# parsing-service — design

Consumes parse jobs from Kafka, reads raw uploads from S3, and serves structured
document content to the planning service over HTTP. It is the only path from a
raw upload to document content.

---

## 0. The constraint that drives everything

The spec says "normalize everything into one internal representation". Reading
the actual consumer changes what that representation has to be.

[`../planning-service/src/domain/extraction.py`](../planning-service/src/domain/extraction.py)
runs `verbatim_quotes` over every extraction — the validator its own docstring
calls "the single most valuable validator in the extraction stage". Every
requirement the model emits carries a `quote`, and that quote must appear as a
**substring of the exact text the model was shown**. The normaliser there already
apologises for us:

> PDF extraction introduces smart quotes, non-breaking spaces, ligatures and
> line-wrap whitespace.

Three consequences, and they are not negotiable:

1. **There is exactly one canonical text per document, and it is the artifact.**
   Not a debug byproduct. If we serve `blocks` to one consumer and a separately
   rendered `text` to another, quotes validated against one will fail against the
   other, and the planning service's best check starts rejecting honest
   extractions. So: **canonical text is produced by serialising the block tree,
   and every block records its offset span into that string as a side effect of
   serialising.** Offsets are correct by construction, never by a later matching
   pass.

2. **Line-wrap de-hyphenation is our job, not theirs.** A PDF that yields
   `"authenti-\ncation"` produces a quote no model can reproduce and no
   normaliser can repair. Fixing it downstream is impossible; fixing it at parse
   time is a dozen lines.

3. **OCR text quality is a downstream correctness problem, not just a quality
   metric.** A garbled OCR page makes `verbatim_quotes` fail on requirements that
   are genuinely in the document. We must therefore ship `ocr_applied` and a
   per-block `confidence` so the planning service can loosen validation for
   OCR-derived text instead of silently dropping real requirements.

Also note `extract` in
[`../planning-service/config/model_policy.yaml`](../planning-service/config/model_policy.yaml)
declares `requires: [json_schema, vision]`, and `contracts.py` has an `ImagePart`
described as "a page image for the vision fallback". **Page images are part of
our contract**, not an internal detail — we must retain and serve them.

---

## 1. Pipeline

```
Kafka: documents.parse.requested   (key = document_id)
   │
   ├─ 1. claim         conditional UPDATE — the idempotency gate
   ├─ 2. fetch         S3 GetObject by (bucket, key, version_id)
   ├─ 3. validate      size · sniff bytes · integrity · zip-bomb ratio
   ├─ 4. scan          clamd INSTREAM — before any parser sees the bytes
   ├─ 5. detect        real format, per-page text-layer coverage
   ├─ 6. parse         format parser → block tree  (sandboxed)
   ├─ 7. ocr           per-page, only pages that need it   [slow lane]
   ├─ 8. structure     heading inference, list/table normalisation
   ├─ 9. serialise     block tree → canonical text + spans   ★ critical module
   ├─ 10. persist      S3 artifact (content-addressed) → Postgres commit
   └─ 11. emit         documents.parse.completed
```

Steps 2–9 are pure functions of `(bytes, parser_version, parse_options)`. That
purity is what makes replay safe and makes the golden-corpus tests in §9
meaningful.

---

## 2. Intake

### Validation order matters

Cheapest and most protective checks first, so a hostile file is rejected before
anything with a CVE history opens it:

| Check | Method | On failure |
|---|---|---|
| Size cap | `Content-Length` from `HeadObject`, before download | permanent |
| Byte sniff | `libmagic` (`python-magic`); extension is a hint only | permanent |
| Declared vs sniffed | compare to the upload backend's `content_type` | warn, continue |
| Malware | stream to `clamd` INSTREAM | permanent → `quarantined` |
| Zip-bomb ratio | uncompressed/compressed before extracting DOCX/XLSX | permanent |
| Integrity | the format's own parser opens it and reports page/sheet count | permanent |

Sniffing gets us the *candidate* format; only the parser opening it successfully
confirms it. A file that sniffs as PDF and fails `pdfium` load is corrupt or
truncated, and that distinction is worth recording separately from "unsupported".

### Parser sandboxing

DOCX and XLSX are zips of XML, and PDF parsers have a long CVE history. Parsing
untrusted uploads is the highest-risk code in the service:

- no network egress from the parse step (a parser should never fetch anything)
- XML external entity resolution disabled — `defusedxml`, or verify the library
  already hard-disables it. XXE via DOCX is a real, boring exploit path
- read-only filesystem except a per-job tmpdir, with a size cap
- memory and CPU limits, plus a **hard wall-clock timeout per document**
- legacy `.doc`/`.xls`: LibreOffice headless converts to the modern format in the
  same sandbox, or we reject. Never in the worker process

### Rejection is a recorded outcome, not an exception

Every failure path writes `failure_class` and a human-readable `failure_reason`
to the document row before the offset is committed. The worker never dies on bad
input; a crashed worker is a bug, a rejected document is data.

`failure_class ∈ {unsupported_format, corrupt, too_large, malware, encrypted,
ocr_failed, transient_exhausted, internal}` — the first six are permanent and
skip retry entirely.

---

## 3. Parsing and extraction

### Library choices

| Format | Library | Why / licence note |
|---|---|---|
| PDF text + page raster | **`pypdfium2`** | Apache-2.0/BSD-3. Fast, robust |
| PDF tables | `pdfplumber` | MIT, on `pdfminer.six` |
| DOCX | `python-docx` | style names give *real* heading levels |
| XLSX | `openpyxl` (`read_only=True, data_only=True`) | streams; no formula eval |
| PPTX | `python-pptx` | slide → section |
| HTML | `selectolax` | fast, lenient |
| MD / TXT | in-house | Markdown already *is* our target shape |
| Images | OCR backend | |

> **Decided: no PyMuPDF.** It is the best PDF library in Python and it is **AGPL-3.0**.
> Commercial use requires a paid Artifex licence. For a service shipped to
> clients, `pypdfium2` + `pdfplumber` avoids the problem entirely. Worth a
> deliberate decision now rather than a legal review later.

### OCR is a per-page decision, not per-document

Real documents are hybrids — a digital spec with three scanned appendix pages. A
document-level boolean OCRs everything (100× the cost) or nothing (silently loses
the appendix). So:

```
for page in pdf:
    if chars_on_page < MIN_CHARS_PER_PAGE and page_has_images:
        ocr(page)         # slow lane
    else:
        use_text_layer(page)
```

`MIN_CHARS_PER_PAGE` lives in `config/limits.yaml`. Mark each page
`source ∈ {text_layer, ocr}` and carry OCR confidence into the blocks.

**Engine:** Tesseract (`pytesseract`) behind an `OCRBackend` protocol. Escalate
to AWS Textract or Google Document AI when confidence is low or the page has
ruled tables — those services are dramatically better at scanned tables, and the
protocol means it is a config change. Start with Tesseract; keep the seam.

### Structure

Headings are where the value is, and the signal differs by format:
DOCX gives them (style names), Markdown gives them (`#`), PDF does not — it needs
inference from font size, weight, and position relative to the body text
distribution. Get the DOCX/MD path exactly right first; PDF heading inference is
heuristic and should degrade to `paragraph` rather than guess wrongly. A
mislabelled heading corrupts the section tree; an unlabelled one only flattens it.

---

## 4. Internal representation

**Flat block list, tree by `parent_id`.** Not nested. Flat gives stable
addressing, trivial range slicing, no recursion in consumers, sortable offsets,
and it is exactly the shape a future chunker walks. Consumers that want the tree
rebuild it in one pass.

```python
class ParsedDocument(BaseModel):
    schema_version: str            # "1.0" — bump on breaking change
    document_id: str
    content_hash: str              # sha256 of raw bytes
    source: SourceRef              # bucket, key, version_id, byte_size, media_type
    metadata: DocumentMetadata
    text: str                      # THE canonical text — what the model sees
    blocks: list[Block]            # document order
    pages: list[Page]
    warnings: list[Warning]        # non-fatal: 3 pages OCR'd, 1 table unparsed…

class Block(BaseModel):
    id: str                        # deterministic: hash(content_hash, ordinal)
    type: Literal["heading", "paragraph", "list", "list_item", "table",
                  "code", "caption", "footnote", "figure"]
    depth: int                     # heading level / list nesting
    parent_id: str | None
    page: int | None               # page it starts on
    span: tuple[int, int]          # [start, end) into ParsedDocument.text
    text: str                      # == doc.text[start:end], see invariant below
    table: TableData | None        # rows/cells, when type == "table"
    confidence: float | None       # OCR only
    attrs: dict

class Page(BaseModel):
    number: int                    # 1-based
    span: tuple[int, int]          # into ParsedDocument.text
    source: Literal["text_layer", "ocr"]
    image_key: str | None          # S3 key for the vision fallback
    char_count: int
    confidence: float | None
```

### The canonical text is a deterministic Markdown rendering

Headings keep `#` markers, lists keep bullets, tables render as pipe tables.
Three reasons: models read Markdown structure natively; substring matching still
works for the inner text of any marked-up block; and a consumer that ignores
`blocks` entirely still sees the structure. Blocks are joined by `\n\n`.

**Tables must be rendered into the canonical text.** If a table exists only in
`Block.table`, any requirement quoted from a table can never validate. The
structured rows stay in `table` for machine use; the pipe rendering goes in the
text.

### Page numbers by offset lookup, not by asking the model

We deliberately do **not** inject `--- page 4 ---` markers into the canonical
text — a quote spanning a marker would fail to match. `pages[].span` carries the
mapping instead, so `ExtractedRequirement.page` is resolved by finding the page
whose span contains the quote's match offset.

**This is a suggested change on the planning-service side**: it currently asks
the model for `page`, which is a thing models get wrong. Resolving it from the
quote offset after validation is a pure lookup and always right. If they would
rather keep the model doing it, `?page_markers=true` on the content endpoint
injects markers — but the lookup is better.

### Normalisation, applied once, here

NFKC (fixes ligatures), de-hyphenate line-wrapped words, collapse intra-line
whitespace, preserve paragraph breaks, strip repeated headers/footers detected
across pages. We do **not** lowercase and do **not** flatten smart quotes —
that is lossy for display, and the downstream normaliser handles it.

### Invariant test

```python
for b in doc.blocks:
    assert b.text == doc.text[b.span[0]:b.span[1]]
```

`Block.text` duplicates the slice and inflates the payload. Worth it: consumers
stay trivial, and this one assertion over the golden corpus catches every
offset bug in `serialize.py` — the module most able to break the contract
silently.

### RAG-readiness without building RAG

Nothing vector-shaped ships now. But a flat block list with spans, pages, and a
heading hierarchy is precisely a chunker's input: group blocks under their
heading ancestor, emit chunks carrying `(document_id, block_ids, span, page)`.
Provenance survives to citation, and no re-parse is ever needed. When RAG
arrives it is a new table plus a `pgvector` column — see §5.

---

## 5. Persistence — recommendation

**Postgres for state, metadata, and indexes; S3 for the parsed artifact and page
images.**

Split by access pattern. Postgres owns what must be transactional or queryable:
the status machine, run history, document metadata, and the heading index. S3
owns the bulk payload, content-addressed:

```
raw/{document_id}/{...}                                   # owned by upload backend
parsed/{content_hash}/{parser_version}/document.json      # the ParsedDocument
parsed/{content_hash}/{parser_version}/pages/{n}.webp     # vision fallback
```

Content-addressing is what makes replay free: the same bytes and the same parser
version write the same key with the same content, so a duplicate Kafka delivery
is an idempotent overwrite rather than a duplicate row.

**Why not put the payload in Postgres.** A 500-page PDF is ~1 MB of text and
5–10 MB of block JSON. That is TOAST churn on the hottest table in the service
and it bloats every backup. Keep rows small.

**Why Postgres for state:**

- Conditional `UPDATE`/unique constraints are the mechanism that turns Kafka's
  at-least-once delivery into exactly-once *effect* (§6). This is the deciding
  factor.
- `pgvector` is the future RAG story with **no new infrastructure**, and — more
  importantly — chunks and vectors sit in the same database as the document row,
  so the cascade delete in §8 is one transaction instead of a distributed cleanup
  with its own failure modes.
- JSONB covers metadata queries without a migration per field.

**Alternatives considered:**

- **MongoDB** — the nested `ParsedDocument` maps to a document naturally and
  schema drift is painless. Rejected on two concrete points: the 16 MB document
  limit is a real ceiling for large parsed PDFs (pushing us to GridFS, which is
  worse than S3 at the same job), and the FSM guards in §6 want the transactional
  story Postgres gives for free.
- **OpenSearch / Elasticsearch** — right answer for full-text search, wrong one
  for a system of record: no transactions, and near-real-time visibility means a
  status read immediately after a write can be stale, which directly breaks the
  status API. Add later as a *derived* index if search becomes a requirement.
- **DynamoDB** — excellent conditional writes, and idiomatic if going
  fully AWS-serverless. But the 400 KB item cap forces the S3 offload anyway, the
  `list by status` query needs a GSI, and there is no in-database vector story.

Schema sketch:

```sql
documents (
  id uuid primary key,                    -- from the upload backend
  status text not null,                   -- pending|processing|ready|failed|deleted
  content_hash text,
  source_bucket text, source_key text, source_version_id text,
  media_type text, byte_size bigint,
  current_run_id uuid references parse_runs(id),   -- what /content serves
  metadata jsonb,
  failure_class text, failure_reason text,
  created_at, updated_at, deleted_at timestamptz
)

parse_runs (
  id uuid primary key,
  document_id uuid not null references documents(id),
  content_hash text not null,
  parser_version text not null,
  parse_options_hash text not null,
  status text not null,                   -- pending|running|succeeded|failed
  artifact_key text,
  attempt int not null default 1,
  lease_expires_at timestamptz,           -- crashed-worker reclaim
  trace_id text,
  started_at, finished_at timestamptz,
  unique (document_id, content_hash, parser_version, parse_options_hash)
)

blocks (                                  -- index only; payload stays in S3
  run_id uuid, block_id text, type text, depth int, page int,
  char_start int, char_end int, heading_path text,
  primary key (run_id, block_id)
)
```

The `unique` constraint on `parse_runs` is the idempotency key. `blocks` is
optional in v1 — add it when a consumer actually needs "give me the Security
section" server-side.

---

## 6. Lifecycle, state, and idempotency

### Separate document status from run status

> **Decided:** the backend guarantees consumers are only pointed at documents that
> are already `ready`, so the availability argument below is not load-bearing.
> The structure survives anyway, because it makes the design *smaller*: since
> `parse_runs` has to exist regardless — its unique constraint is the idempotency
> key in §6 — the safe behaviour is free. **A reprocess simply does not touch
> `documents.status`.** The row stays `ready` with its existing
> `current_run_id`; progress lives on the run. No `reprocessing` state, no extra
> table beyond the one idempotency already required.

The spec asks for `pending → processing → ready → failed`, exposed. Taken
literally, reprocessing a healthy document moves it to `processing` and any
consumer that checks status before reading content sees a document that is no
longer ready. Reprocessing should not take a good document offline.

So the FSM they asked for is the *document's*, and runs get their own:

```
document:   pending ──▶ processing ──▶ ready ⇄ (reprocess runs beside it)
                            │            │
                            ▼            ▼
                          failed      deleted
run:        pending ──▶ running ──▶ succeeded | failed
```

A reprocess creates a **new run** while `status` stays `ready` and
`current_run_id` keeps pointing at the last good artifact. On success, one
atomic update flips `current_run_id`. On failure, the document is untouched and
still serving. `status` only becomes `failed` when *no* run has ever succeeded.

This costs one extra table and buys zero-downtime reprocessing, full attempt
history, and instant rollback (repoint `current_run_id`). The externally exposed
`status` field stays exactly the four values asked for, so consumers see the
simple machine.

`quarantined` (malware) is exposed as `failed` with `failure_class: malware` —
one fewer state for consumers to branch on, with the nuance preserved in the
reason.

### Idempotency, layer by layer

1. **Kafka key = `document_id`** → all jobs for one document land on one
   partition, consumed by one consumer in the group. Concurrent processing of the
   same document is structurally impossible, not merely unlikely.
2. **Claim by conditional update**, so a redelivery finds nothing to do:
   ```sql
   UPDATE documents SET status='processing', updated_at=now()
    WHERE id=$1 AND status IN ('pending','failed')
      AND (deleted_at IS NULL)
   ```
   Zero rows updated → already processing or already ready → check `parse_runs`
   for a matching succeeded run and commit the offset without reparsing.
3. **`parse_runs` unique constraint** on
   `(document_id, content_hash, parser_version, parse_options_hash)` → a replay
   conflicts, and conflict means "done", not "error".
4. **Content-addressed S3 keys** → the write is an idempotent overwrite.
5. **Commit the Kafka offset last**, after the Postgres commit. Crash before it
   → redelivery, caught by gate 2 or 3. Crash after → the work is durable. This
   ordering is the whole at-least-once story; getting it backwards silently drops
   documents.
6. **Leases** — `lease_expires_at` on the running run lets a sweeper reclaim
   documents from a worker that died mid-parse, instead of stranding them in
   `processing` forever.

### Explicit reprocessing

`POST /v1/documents/{id}/reprocess` produces a new run against the existing raw
file — same `content_hash`, new `parser_version` or `parse_options`. It publishes
to the same topic, so reprocessing and first-time parsing share one code path.
`force: true` bypasses the "already done" gate for the same version, which is
what you want after fixing a parser bug without cutting a version.

A parser upgrade is then a backfill: enumerate documents whose
`current_run.parser_version` is stale, publish reprocess jobs, rate-limited.

---

## 7. Serving

```
GET    /v1/documents/{id}                    status + metadata, no content
GET    /v1/documents?ids=a,b,c               batch status  ← for document sets
GET    /v1/documents/{id}/content            ParsedDocument (?include=text,blocks,tables)
GET    /v1/documents/{id}/text               canonical text only
POST   /v1/documents/{id}/locate             quotes[] → offsets, page, block_id, snapped text
GET    /v1/documents/{id}/pages/{n}/image    302 → presigned S3 URL
POST   /v1/documents/{id}/reprocess          → 202 {run_id}
DELETE /v1/documents/{id}                    → 202
GET    /v1/documents/{id}/runs               attempt history
GET    /healthz  /readyz  /metrics
```

Notes:

- **Batch status exists because the planning service works in document sets** —
  its README shows `RunRef(run_id="doc-set-42")` and `extract` runs "one agent per
  document, in parallel". Making it issue N status calls to start a set is a
  needless N+1 across a service boundary.
- **`/text` exists because that is what the consumer actually takes today** —
  `ExtractRequest.document` in
  [`../planning-service/src/api/app.py`](../planning-service/src/api/app.py) is a
  plain `str`. It should be able to adopt us without restructuring, then move to
  `/content` when it wants sections and tables.
- **ETag = `content_hash:parser_version`**, and honour `If-None-Match`. Parsed
  content is immutable per run, so a 304 makes re-reads free.
- `404` unknown id · `409` with the current status when content is requested for
  a document that is not ready (never a 200 with empty text — that reads as an
  empty document) · `410` for deleted.
- **Service-to-service auth still exists.** End-user auth is the gateway's job,
  but this API must not be openly callable inside the cluster: mTLS or a signed
  service token, and it accepts no user identity.

### `/locate` — why quote matching belongs on this side

`verbatim_quotes` in
[`../planning-service/src/domain/extraction.py`](../planning-service/src/domain/extraction.py)
is the right check in the right service, but half of it is our job. Its
`_normalise` helper exists solely to undo *our* artifacts — smart quotes,
non-breaking spaces, ligatures, line-wrap whitespace — which is text mechanics
living inside a requirements module. Split it by the question each half answers:

- **"Where does this string occur in this document?"** — text mechanics. Ours: we
  own the canonical text, its normalisation, its block spans and its OCR
  confidence.
- **"What do we do when it doesn't occur?"** — extraction policy: reject, repair,
  quarantine the entry. Theirs, unchanged.

```
POST /v1/documents/{id}/locate
  { "quotes": ["authenticate users within 300ms", …] }

→ { "results": [ {
      "quote": "authenticate users within 300ms",
      "found": true,
      "match": "exact" | "snapped" | "none",
      "span": [30, 61],
      "text": "authenticate users within 300ms",   # the exact source span
      "page": 4,
      "block_id": "b7f2…",
      "occurrences": 1,
      "similarity": 1.0,
      "ocr_applied": false,
      "confidence": null
    } ] }
```

Four things this buys them that a local substring test cannot:

1. **`span` instead of a boolean.** `needle not in haystack` computes the match
   and discards the offset. That offset gives `page`, `block_id`, ordering and a
   dedup key for free — and lets `page` come out of the model's output schema
   entirely, one less field to hallucinate.
2. **`match: "snapped"` instead of rejection.** When a quote is a near-miss we
   locate it by similarity and return the *exact source span* as `text`. They
   replace the model's approximation with real source text rather than failing the
   run. The hallucination guard is not weakened — nothing is ever returned that
   is not in the document — but most spurious repair turns disappear. This
   matters because one bad quote currently discards the whole
   `ExtractionResult`: with 40 requirements and a 2% per-quote failure rate, a
   majority of runs pay a full 32k-output retry against a $2.00 budget.
3. **`ocr_applied` + `confidence` resolve a designed-in conflict.** `extract`
   declares `requires: [json_schema, vision]`, so on a scanned page the model
   quotes what it *sees* in the page image while the validator checks our *OCR
   text*. Honest extractions get rejected for OCR errors. These fields let them
   loosen to `snapped` exactly where strictness is unjust, and nowhere else.
4. **`block_id` + `occurrences` close two holes.** `re.sub(r"\s+", " ", …)`
   collapses paragraph breaks, so a quote stitching two unrelated sentences across
   a block boundary validates today. Requiring a single containing block closes
   that, and replaces the `min_length=12` magic number with a real check.
   `occurrences > 1` flags an ambiguous match (repeated boilerplate) where page
   attribution would otherwise be a coin flip.

They keep the validator, the repair loop, and the decision. We keep the text
mechanics, where the text lives.

### Also emit a completion event

`documents.parse.completed` (key = `document_id`) lets the planning service react
instead of poll. The status API stays as the source of truth and the fallback;
the event just removes polling latency for the common path.

---

## 8. Deletion

`DELETE` marks `deleted_at`, emits `documents.deleted`, and returns 202. A
cleanup worker then deletes **derived data before raw data**:

```
1. parsed artifacts + page images (S3)
2. chunks + vectors (Postgres — future)
3. blocks index rows
4. raw file (S3)          ← last
5. tombstone the row: keep id + deleted_at, drop content and metadata
```

The order is the interesting part. Crash after step 4 and you have derived
content still being served for a document the user deleted — the request is
unhonoured. Crash after step 1 and the content is already unreachable (delete
satisfied) with an orphaned raw blob, which a periodic sweeper reaps. Satisfy
intent first, tidy second.

Accept deletion from both the API and a `documents.deleted` topic — the upload
backend owns the raw file and may drive it. Cleanup is idempotent, so both firing
is harmless.

---

## 9. Operational

### Retry and DLQ

Kafka has no native delay, so use **tiered retry topics** with a `not_before`
header: `parse.retry.30s` → `parse.retry.5m` → `parse.retry.30m` → `parse.dlq`.
The consumer pauses the partition until `not_before` rather than sleeping in the
poll loop. Backoff is exponential with jitter.

Only transient classes retry — S3 5xx, timeouts, OCR backend unavailable, DB
connection loss. Everything in the permanent set from §2 goes **straight to the
DLQ with `status=failed`** and never retries. Retrying an unsupported format four
times is pure cost with a guaranteed outcome.

DLQ messages carry the original payload, `failure_class`, the exception, and
`trace_id`, so a DLQ replay after a fix is a re-publish.

### The Kafka trap worth naming

> **Still do this now** — it is four config lines, and it is a *correctness* bug,
> not a throughput one. Running more workers concurrently does not fix it and
> mildly worsens it: the eviction triggers a rebalance across the whole consumer
> group, so a single slow document can disturb every worker in it.

A 20-minute OCR job **exceeds `max.poll.interval.ms` (default 5 min), the
consumer is evicted from the group, the partition rebalances, and another worker
starts the same document** — while the first is still running. This is the
classic Kafka + long-task failure and it produces duplicate work plus rebalance
storms. Configure deliberately:

- `max.poll.records = 1`
- `max.poll.interval.ms` above the worst-case document (e.g. 30 min)
- a hard per-document timeout strictly **below** that value
- `enable.auto.commit = false` — commit explicitly, after the DB commit

### Two lanes, because OCR is 10–100× slower

> **Deferred** — running several workers concurrently buys enough parallel slots
> that head-of-line blocking is tolerable at low volume, and the lane split is
> additive later (a new topic and consumer group; the parse path is unchanged).
> Revisit when p99 queue wait diverges from p50, which is the signal that fast
> documents are sitting behind slow ones rather than behind genuine load.
> Keep `failure_class`/`ocr_applied` on the run from day one so that metric is
> already sliceable when the question comes up.

One queue means a batch of 500-page scans head-of-line blocks every 5-page DOCX
behind it. Split: `parse.requested` (fast lane, text extraction) and
`parse.ocr.requested` (slow lane, CPU-heavy, own consumer group, own worker pool
and scaling policy). A document that needs OCR is handed from the fast lane to
the slow one after detection.

### Metrics

Standard: `parse_duration_seconds{format,ocr}`, `parse_total{format,outcome}`,
`ocr_pages_total`, `dlq_total{failure_class}`, `validation_rejections_total{reason}`,
`bytes_processed_total{format}`, and consumer lag per partition.

The one that matters most, given "parsing is where things silently go wrong":

**`chars_extracted_per_page{format}` as a histogram.** A PDF whose text layer is
a broken font map extracts 3 characters per page and reports *success*. No error
fires, the document goes `ready`, and the planning service extracts nothing from
a document full of requirements. Alert on the p50 of this metric moving by
format — it is the only signal that catches silent extraction failure. Pair it
with `blocks_per_document` and `heading_count`.

Structured JSON logs on every stage with `document_id`, `run_id`, `content_hash`,
`format`, `stage`, `duration_ms`, `trace_id`. Propagate W3C traceparent through
Kafka headers from the upload backend all the way to `parse.completed`, so one
trace covers upload → parse → extraction.

### Golden-corpus regression tests

A fixture set of real documents (digital PDF, scanned PDF, hybrid, DOCX with
deep headings, XLSX, a ligature-heavy PDF, a hyphenation-heavy PDF, a corrupt
file, an encrypted one) with expected char counts, block counts, and heading
counts within tolerance. Run in CI on every parser dependency bump. A library
upgrade that quietly degrades extraction is otherwise invisible until the
planning service produces worse plans, which is far too late to attribute.

### Scaling

- **Request tier** — stateless FastAPI, scales on RPS/CPU.
- **Worker tiers** — scale on Kafka consumer lag (KEDA's Kafka scaler with
  `lagThreshold`). Fast and OCR lanes scale independently.
- **Ceiling:** worker replicas cannot usefully exceed partition count. Choose
  partitions generously up front (24 is cheap) — raising them later breaks
  per-key ordering for in-flight keys, which is exactly the guarantee idempotency
  gate 1 rests on.

---

## 10. Out of scope

Restating, because these are the boundaries that erode first:

- **HTTP upload and presigned URLs** — the user-facing backend. We only ever read
  from S3 by reference.
- **End-user authentication** — gateway/backend. We take a `document_id` and a
  service credential, never a user identity.
- **Requirements, architecture, plans** — the planning service. We do not
  interpret content, and in particular we do not call an LLM. If a change to this
  service needs a model, it probably belongs on the other side of the boundary.

---

## 11. Layout

Mirrors planning-service conventions: `src/` with `pythonpath = ["src"]`,
pydantic v2 wire models, YAML config, FastAPI, offline pytest, ruff at 92.

```
config/
  parsers.yaml            # format → parser, options
  limits.yaml             # size caps, timeouts, OCR thresholds, retry tiers
src/
  domain/
    document.py           # ParsedDocument, Block, Page, Metadata — THE contract
    status.py             # FSM, legal transitions
    errors.py             # transient vs permanent taxonomy
  intake/
    consumer.py           # Kafka, offset discipline, lease renewal
    validate.py           # sniff, size, integrity, zip-bomb
    scan.py               # clamd
    retry.py              # tiered topics, backoff, DLQ
  parse/
    registry.py           # format → Parser
    base.py               # Parser protocol
    pdf.py docx.py xlsx.py pptx.py html.py text.py image.py
    ocr/  base.py tesseract.py
    structure.py          # heading inference, list/table normalisation
    serialize.py          # ★ block tree → canonical text + spans
  store/
    blobs.py repository.py migrations/
  api/
    app.py                # status, content, text, reprocess, delete
  obs/
    metrics.py logging.py tracing.py
tests/
  fixtures/               # golden corpus
  test_serialize.py       # the span invariant
  test_idempotency.py     # replay a job twice, assert one run
```

`serialize.py` is the file to write first and guard hardest — it is the one
module whose bugs are invisible locally and break the consumer's most valuable
validator.

---

## 12. Suggested build order

1. `domain/document.py` + `parse/serialize.py` + the span invariant test. The
   contract, provable, before any I/O.
2. `text.py` and `docx.py` parsers — real structure, no OCR, fast feedback.
3. Postgres schema, repository, FSM with the conditional-update claim.
4. Kafka consumer with offset-last discipline; idempotency replay test.
5. API: status, batch status, `/text`, `/content`.
6. `pdf.py` (text layer only), plus the golden corpus.
7. OCR slow lane, page images, `xlsx.py`.
8. Malware scanning, sandbox hardening, DLQ, metrics, tracing.
9. Delete cascade and the orphan sweeper.

Steps 1–5 are an end-to-end service for DOCX and text, which is enough for the
planning service to integrate against while PDF and OCR land behind it.

---

## 13. Open questions

1. **Volume and size ceiling** — largest document, expected pages/day? It decides
   whether the `blocks` index table is v1 and how many partitions to cut.
2. **Is scanned-document quality a real requirement?** If clients send phone
   photos of whiteboards, Tesseract will not be good enough and Textract should be
   the default rather than the escalation.
3. **Multi-tenancy** — is `document_id` globally unique, or is there a
   `tenant_id` that belongs in every key, row, and metric label? Retrofitting
   tenancy is expensive; adding the column now is free.
4. **Will the planning service adopt `/locate` (§7)?** It is the one item here
   that needs a change on their side. Cheapest useful first step: keep
   `verbatim_quotes` exactly as it is, but have it call `/locate` for the match
   instead of `in`, and drop `page` from `ExtractedRequirement` — same validation
   behaviour, correct page numbers, no schema field for the model to get wrong.
   Snapping and per-entry quarantine can follow separately.
