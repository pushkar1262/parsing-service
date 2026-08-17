-- Tenancy and row-level security.
--
-- The upload event carries tenant_id and project_id, and both services connect as the
-- unprivileged eos_app role, so isolation is enforced by the database rather than by every
-- query remembering a WHERE clause. That distinction is the whole point: a missing
-- `AND tenant_id = ...` in one query is a cross-tenant data leak, and it is the kind of
-- omission that passes review because the query looks complete.
--
-- The application sets the tenant per transaction:
--
--     SET LOCAL app.tenant_id = '11111111-...';
--
-- LOCAL, not SESSION: connections are pooled, and a SESSION setting outlives the request
-- that made it — the next request on that connection would inherit the previous tenant.
--
-- Note that policies do NOT apply to the table owner or to superusers. Migrations run as a
-- privileged role (DATABASE_MIGRATE_URL) and the service runs as eos_app (DATABASE_URL);
-- pointing DATABASE_URL at `postgres` silently disables every policy below while every
-- test still passes.

ALTER TABLE documents  ADD COLUMN IF NOT EXISTS tenant_id  uuid;
ALTER TABLE documents  ADD COLUMN IF NOT EXISTS project_id uuid;
ALTER TABLE parse_runs ADD COLUMN IF NOT EXISTS tenant_id  uuid;

-- Backfill has to happen before NOT NULL can be added, and only the deployment knows what
-- the existing rows belong to — so the column stays nullable here and the check below is
-- the guard that matters instead.
CREATE INDEX IF NOT EXISTS documents_tenant_idx
    ON documents (tenant_id, status, updated_at DESC)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS documents_project_idx
    ON documents (tenant_id, project_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS parse_runs_tenant_idx ON parse_runs (tenant_id, document_id);

-- Resolves to NULL rather than raising when unset, so an unscoped maintenance query fails
-- closed (matching nothing) instead of erroring in a way that tempts someone to disable
-- RLS to get their job done.
CREATE OR REPLACE FUNCTION current_tenant_id() RETURNS uuid
    LANGUAGE sql STABLE AS $$
    SELECT NULLIF(current_setting('app.tenant_id', true), '')::uuid
$$;

ALTER TABLE documents  ENABLE ROW LEVEL SECURITY;
ALTER TABLE parse_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE blocks     ENABLE ROW LEVEL SECURITY;

-- FORCE so the policies also apply to the table owner. Without it, anything connecting as
-- the owner bypasses them, and the isolation quietly depends on which role happens to be
-- in the connection string.
ALTER TABLE documents  FORCE ROW LEVEL SECURITY;
ALTER TABLE parse_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE blocks     FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS documents_tenant_isolation ON documents;
CREATE POLICY documents_tenant_isolation ON documents
    USING (tenant_id = current_tenant_id())
    -- WITH CHECK as well as USING: USING governs what can be read and updated, WITH CHECK
    -- governs what a row may be written *as*. Without it, a session scoped to tenant A
    -- could insert a row labelled tenant B — invisible to itself, and visible to B.
    WITH CHECK (tenant_id = current_tenant_id());

DROP POLICY IF EXISTS parse_runs_tenant_isolation ON parse_runs;
CREATE POLICY parse_runs_tenant_isolation ON parse_runs
    USING (tenant_id = current_tenant_id())
    WITH CHECK (tenant_id = current_tenant_id());

DROP POLICY IF EXISTS blocks_tenant_isolation ON blocks;
CREATE POLICY blocks_tenant_isolation ON blocks
    USING (
        run_id IN (SELECT id FROM parse_runs WHERE tenant_id = current_tenant_id())
    )
    WITH CHECK (
        run_id IN (SELECT id FROM parse_runs WHERE tenant_id = current_tenant_id())
    );

-- The role both services connect as. Created by the deployment's init script; the grants
-- belong with the schema, so they live here rather than there.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eos_app') THEN
        GRANT USAGE ON SCHEMA public TO eos_app;
        GRANT SELECT, INSERT, UPDATE, DELETE ON documents, parse_runs, blocks TO eos_app;
        GRANT EXECUTE ON FUNCTION current_tenant_id() TO eos_app;
    ELSE
        RAISE NOTICE
            'role eos_app does not exist; skipping grants. The service will connect as '
            'whatever DATABASE_URL names, and if that is the table owner then row-level '
            'security is not being enforced.';
    END IF;
END
$$;
