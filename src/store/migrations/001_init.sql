-- Schema for the document store.
--
-- Two tables and one index table. The shapes are ordinary; the constraints are the
-- interesting part, because they are what turn Kafka's at-least-once delivery into
-- exactly-once effect. Read them before changing anything here.

CREATE TABLE IF NOT EXISTS documents (
    id                text PRIMARY KEY,
    status            text NOT NULL DEFAULT 'pending',
    content_hash      text,
    source_uri        text,
    source_bucket     text,
    source_key        text,
    source_version_id text,
    media_type        text,
    byte_size         bigint,
    -- Which run `/content` serves. The pointer, not the status, is what makes
    -- zero-downtime reprocessing work: it keeps naming the last good artifact until a
    -- new run actually succeeds.
    current_run_id    uuid,
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    failure_class     text,
    failure_reason    text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    deleted_at        timestamptz,

    CONSTRAINT documents_status_valid CHECK (
        status IN ('pending', 'processing', 'ready', 'failed', 'deleted')
    )
);

CREATE TABLE IF NOT EXISTS parse_runs (
    id                 uuid PRIMARY KEY,
    document_id        text NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    content_hash       text NOT NULL,
    parser_version     text NOT NULL,
    parse_options_hash text NOT NULL DEFAULT '',
    status             text NOT NULL DEFAULT 'pending',
    artifact_key       text,
    attempt            int NOT NULL DEFAULT 1,
    lease_expires_at   timestamptz,
    trace_id           text,
    started_at         timestamptz,
    finished_at        timestamptz,
    failure_class      text,
    failure_reason     text,

    CONSTRAINT parse_runs_status_valid CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed')
    ),

    -- THE idempotency key. Parsing is a pure function of these four things, so a
    -- redelivery conflicts here and "conflict" means "already done", not "error".
    -- Removing this constraint does not break any test that does not test replay; it
    -- breaks production the first time a consumer rebalances.
    CONSTRAINT parse_runs_idempotent UNIQUE (
        document_id, content_hash, parser_version, parse_options_hash
    )
);

-- The status API's list query, and the backfill query that finds documents on a stale
-- parser version. Partial, because deleted rows are never listed.
CREATE INDEX IF NOT EXISTS documents_status_idx
    ON documents (status, updated_at DESC)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS parse_runs_document_idx
    ON parse_runs (document_id, started_at DESC);

-- Finds runs whose worker died mid-parse. Partial so it stays small: only running rows
-- can expire, and there are never many of those.
CREATE INDEX IF NOT EXISTS parse_runs_lease_idx
    ON parse_runs (lease_expires_at)
    WHERE status = 'running';

-- Optional in v1. Add it when a consumer actually needs "give me the Security section"
-- resolved server-side rather than by slicing the artifact client-side.
CREATE TABLE IF NOT EXISTS blocks (
    run_id       uuid NOT NULL REFERENCES parse_runs(id) ON DELETE CASCADE,
    block_id     text NOT NULL,
    type         text NOT NULL,
    depth        int  NOT NULL DEFAULT 0,
    page         int,
    char_start   int  NOT NULL,
    char_end     int  NOT NULL,
    heading_path text,
    PRIMARY KEY (run_id, block_id)
);

CREATE INDEX IF NOT EXISTS blocks_heading_idx ON blocks (run_id, type, depth);
