"""The Postgres implementation of `DocumentRepository`.

Every method here is a single statement wherever a single statement is possible, because
the multi-statement version of the same logic is where the races live. Three worth
reading:

`claim` is one `UPDATE ... WHERE status IN (...)` returning the row. Read-then-write is
the classic version of this bug: two workers both read `pending`, both write
`processing`, and both proceed to parse the same document.

`start_run` inserts and lets the unique constraint decide, catching `UniqueViolation`.
Checking for an existing run first is the same race one level down, and under a consumer
rebalance it is not hypothetical.

`complete_run` updates the run and flips `documents.current_run_id` **in one
transaction**. Splitting them leaves a window where a document claims to be ready and
points at nothing, which the content API would answer with a 500.

> Verified against the schema in `migrations/001_init.sql`, but **not executed** in the
> environment this was written in — there is no Postgres server here. The behavioural
> contract is pinned by `tests/test_worker.py` against `InMemoryRepository`, and
> `tests/test_postgres.py` runs the same expectations against a live database when
> `PARSING_TEST_DATABASE_URL` is set. Run it before trusting this in production.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from domain.status import (
    DocumentRecord,
    DocumentStatus,
    ParseRun,
    RunStatus,
)
from store.repository import ClaimResult, Clock, DuplicateRun, utc_now

MIGRATIONS = Path(__file__).parent / "migrations"

_DOCUMENT_COLUMNS = """
    id, tenant_id, project_id, status, content_hash, source_uri, source_bucket, source_key,
    source_version_id, media_type, byte_size, current_run_id, metadata,
    failure_class, failure_reason, created_at, updated_at, deleted_at
"""

_RUN_COLUMNS = """
    id, document_id, content_hash, parser_version, parse_options_hash, status,
    artifact_key, attempt, lease_expires_at, trace_id, started_at, finished_at,
    failure_class, failure_reason
"""


def _document(row: Any) -> DocumentRecord | None:
    if row is None:
        return None
    metadata = row[12]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    return DocumentRecord(
        id=row[0],
        tenant_id=str(row[1]) if row[1] else None,
        project_id=str(row[2]) if row[2] else None,
        status=DocumentStatus(row[3]),
        content_hash=row[4],
        source_uri=row[5],
        source_bucket=row[6],
        source_key=row[7],
        source_version_id=row[8],
        media_type=row[9],
        byte_size=row[10],
        current_run_id=str(row[11]) if row[11] else None,
        metadata=metadata or {},
        failure_class=row[13],
        failure_reason=row[14],
        created_at=row[15],
        updated_at=row[16],
        deleted_at=row[17],
    )


def _run(row: Any) -> ParseRun | None:
    if row is None:
        return None
    return ParseRun(
        id=str(row[0]),
        document_id=row[1],
        content_hash=row[2],
        parser_version=row[3],
        parse_options_hash=row[4],
        status=RunStatus(row[5]),
        artifact_key=row[6],
        attempt=row[7],
        lease_expires_at=row[8],
        trace_id=row[9],
        started_at=row[10],
        finished_at=row[11],
        failure_class=row[12],
        failure_reason=row[13],
    )


class PostgresRepository:
    def __init__(self, connection: Any, *, clock: Clock = utc_now) -> None:
        self._connection = connection
        self._clock = clock
        self._tenant: str | None = None

    # ------------------------------------------------------------ RLS context

    def use_tenant(self, tenant_id: str | None) -> None:
        """Set the tenant every subsequent statement is scoped to.

        Called once per job or request. The value reaches Postgres as
        `SET LOCAL app.tenant_id`, which the policies in `002_tenancy.sql` match against.
        """
        self._tenant = tenant_id

    def _scope(self, cursor: Any) -> None:
        """Apply the tenant to this transaction.

        LOCAL rather than SESSION: connections are pooled, and a SESSION setting outlives
        the request that made it — the next request on that connection would silently
        inherit the previous tenant, which is the worst possible version of this bug
        because it only appears under concurrency.

        set_config with a bound parameter rather than string interpolation, because SET
        does not accept placeholders and building the statement by hand is a SQL injection
        in the one place that would defeat the isolation it is enforcing.
        """
        if self._tenant is None:
            return
        cursor.execute("SELECT set_config('app.tenant_id', %s, true)", (self._tenant,))

    # ---------------------------------------------------------------- schema

    def migrate(self) -> None:
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            for path in sorted(MIGRATIONS.glob("*.sql")):
                cursor.execute(path.read_text())
        self._connection.commit()

    # ------------------------------------------------------------- documents

    def register(
        self,
        document_id: str,
        *,
        source_uri: str | None = None,
        media_type: str | None = None,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> DocumentRecord:
        """Insert if absent, and *never* reset an existing row.

        `DO UPDATE` only fills in a source that was previously unknown. A plain
        `DO UPDATE SET status='pending'` would send a month-old ready document back to
        pending every time its job was replayed.
        """
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                f"""
                INSERT INTO documents
                       (id, tenant_id, project_id, status, source_uri, media_type)
                VALUES (%s, %s, %s, 'pending', %s, %s)
                ON CONFLICT (id) DO UPDATE
                   SET source_uri = COALESCE(documents.source_uri, EXCLUDED.source_uri),
                       media_type = COALESCE(documents.media_type, EXCLUDED.media_type),
                       project_id = COALESCE(documents.project_id, EXCLUDED.project_id)
                RETURNING {_DOCUMENT_COLUMNS}
                """,
                (
                    document_id,
                    tenant_id or self._tenant,
                    project_id,
                    source_uri,
                    media_type,
                ),
            )
            record = _document(cursor.fetchone())
        self._connection.commit()
        return record

    def get(
        self, document_id: str, *, tenant_id: str | None = None
    ) -> DocumentRecord | None:
        """RLS already scopes this; the explicit predicate is belt and braces.

        Deliberate redundancy: if DATABASE_URL is ever pointed at the table owner, every
        policy silently stops applying, and this WHERE clause is the only thing still
        preventing a cross-tenant read.
        """
        scope = tenant_id or self._tenant
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                f"""SELECT {_DOCUMENT_COLUMNS} FROM documents
                     WHERE id = %s AND (%s::uuid IS NULL OR tenant_id = %s::uuid)""",
                (document_id, scope, scope),
            )
            return _document(cursor.fetchone())

    def get_many(
        self, document_ids: list[str], *, tenant_id: str | None = None
    ) -> list[DocumentRecord]:
        if not document_ids:
            return []
        scope = tenant_id or self._tenant
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                f"""SELECT {_DOCUMENT_COLUMNS} FROM documents
                     WHERE id = ANY(%s) AND (%s::uuid IS NULL OR tenant_id = %s::uuid)""",
                (list(document_ids), scope, scope),
            )
            return [_document(row) for row in cursor.fetchall()]

    def claim(self, document_id: str, *, lease_seconds: int = 1800) -> ClaimResult:
        """One conditional UPDATE. The row count *is* the answer."""
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                f"""
                UPDATE documents
                   SET status = 'processing',
                       updated_at = now(),
                       failure_class = NULL,
                       failure_reason = NULL
                 WHERE id = %s
                   AND status IN ('pending', 'failed')
                   AND deleted_at IS NULL
                RETURNING {_DOCUMENT_COLUMNS}
                """,
                (document_id,),
            )
            claimed = _document(cursor.fetchone())
        self._connection.commit()

        if claimed is not None:
            return ClaimResult(True, claimed)

        current = self.get(document_id)
        if current is None:
            return ClaimResult(False, None, "unknown document")
        if current.deleted_at is not None:
            return ClaimResult(False, current, "document is deleted")
        return ClaimResult(False, current, f"status is {current.status.value}")

    def mark_deleted(self, document_id: str) -> DocumentRecord | None:
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                f"""
                UPDATE documents
                   SET status = 'deleted', deleted_at = now(), updated_at = now()
                 WHERE id = %s
                RETURNING {_DOCUMENT_COLUMNS}
                """,
                (document_id,),
            )
            record = _document(cursor.fetchone())
        self._connection.commit()
        return record

    # ------------------------------------------------------------------ runs

    def find_run(
        self,
        document_id: str,
        content_hash: str,
        parser_version: str,
        parse_options_hash: str = "",
    ) -> ParseRun | None:
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                f"""
                SELECT {_RUN_COLUMNS} FROM parse_runs
                 WHERE document_id = %s AND content_hash = %s
                   AND parser_version = %s AND parse_options_hash = %s
                """,
                (document_id, content_hash, parser_version, parse_options_hash),
            )
            return _run(cursor.fetchone())

    def start_run(self, run: ParseRun) -> ParseRun:
        """Insert, and let the unique constraint arbitrate.

        The `UniqueViolation` is not an error path — it is how a concurrent delivery is
        told that another worker owns this one.
        """
        run_id = run.id or str(uuid.uuid4())
        expires = self._clock() + timedelta(seconds=1800)
        try:
            with self._connection.cursor() as cursor:
                self._scope(cursor)
                cursor.execute(
                    f"""
                    INSERT INTO parse_runs (
                        id, tenant_id, document_id, content_hash, parser_version,
                        parse_options_hash, status, attempt, lease_expires_at,
                        trace_id, started_at
                    )
                    VALUES (
                        %s,
                        -- Copied from the document rather than taken from the caller, so a
                        -- run can never be labelled with a different tenant than the
                        -- document it belongs to.
                        (SELECT tenant_id FROM documents WHERE id = %s),
                        %s, %s, %s, %s, 'running', %s, %s, %s, now()
                    )
                    RETURNING {_RUN_COLUMNS}
                    """,
                    (
                        run_id,
                        run.document_id,
                        run.document_id,
                        run.content_hash,
                        run.parser_version,
                        run.parse_options_hash,
                        run.attempt,
                        expires,
                        run.trace_id,
                    ),
                )
                stored = _run(cursor.fetchone())
            self._connection.commit()
            return stored
        except Exception as exc:
            self._connection.rollback()
            if _is_unique_violation(exc):
                existing = self.find_run(
                    run.document_id,
                    run.content_hash,
                    run.parser_version,
                    run.parse_options_hash,
                )
                if existing is not None:
                    raise DuplicateRun(existing) from exc
            raise

    def get_run(self, run_id: str) -> ParseRun | None:
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(f"SELECT {_RUN_COLUMNS} FROM parse_runs WHERE id = %s", (run_id,))
            return _run(cursor.fetchone())

    def runs_for(self, document_id: str) -> list[ParseRun]:
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                f"""
                SELECT {_RUN_COLUMNS} FROM parse_runs
                 WHERE document_id = %s ORDER BY started_at ASC NULLS LAST
                """,
                (document_id,),
            )
            return [_run(row) for row in cursor.fetchall()]

    def complete_run(
        self,
        run_id: str,
        *,
        artifact_key: str,
        content_hash: str,
        metadata: dict[str, Any] | None = None,
    ) -> DocumentRecord:
        """Succeed the run and flip the pointer, in one transaction.

        The single commit is the point. Between the two statements a document would claim
        to be ready while pointing at nothing, and the content API answers that with a
        500 rather than a 409 — an error that looks like our bug because it is.
        """
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                """
                UPDATE parse_runs
                   SET status = 'succeeded', finished_at = now(), artifact_key = %s
                 WHERE id = %s
                RETURNING document_id
                """,
                (artifact_key, run_id),
            )
            row = cursor.fetchone()
            if row is None:
                self._connection.rollback()
                raise KeyError(f"no run {run_id}")
            document_id = row[0]

            cursor.execute(
                f"""
                UPDATE documents
                   SET status = 'ready',
                       current_run_id = %s,
                       content_hash = %s,
                       metadata = COALESCE(%s::jsonb, metadata),
                       failure_class = NULL,
                       failure_reason = NULL,
                       updated_at = now()
                 WHERE id = %s
                RETURNING {_DOCUMENT_COLUMNS}
                """,
                (
                    run_id,
                    content_hash,
                    json.dumps(metadata) if metadata is not None else None,
                    document_id,
                ),
            )
            record = _document(cursor.fetchone())
        self._connection.commit()
        return record

    def fail_run(
        self,
        run_id: str,
        *,
        failure_class: str,
        failure_reason: str,
        permanent: bool,
    ) -> DocumentRecord:
        """Fail the run; touch the document only if it has never succeeded.

        `WHERE current_run_id IS NULL` is the whole reprocessing guarantee, expressed as
        a predicate: a document already serving a good artifact is not affected by a
        later run failing.
        """
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                """
                UPDATE parse_runs
                   SET status = 'failed', finished_at = now(),
                       failure_class = %s, failure_reason = %s
                 WHERE id = %s
                RETURNING document_id
                """,
                (failure_class, failure_reason, run_id),
            )
            row = cursor.fetchone()
            if row is None:
                self._connection.rollback()
                raise KeyError(f"no run {run_id}")
            document_id = row[0]

            cursor.execute(
                f"""
                UPDATE documents
                   SET status = %s,
                       failure_class = %s,
                       failure_reason = %s,
                       updated_at = now()
                 WHERE id = %s AND current_run_id IS NULL
                RETURNING {_DOCUMENT_COLUMNS}
                """,
                (
                    DocumentStatus.FAILED.value if permanent else DocumentStatus.PENDING.value,
                    failure_class,
                    failure_reason,
                    document_id,
                ),
            )
            updated = _document(cursor.fetchone())
        self._connection.commit()
        return updated or self.get(document_id)

    def reclaim_expired(self) -> list[str]:
        """Free documents whose worker died, in one statement.

        A CTE rather than select-then-update, so two sweepers running concurrently cannot
        both reclaim the same document and re-publish it twice.
        """
        with self._connection.cursor() as cursor:
            self._scope(cursor)
            cursor.execute(
                """
                WITH expired AS (
                    UPDATE parse_runs
                       SET status = 'failed',
                           finished_at = now(),
                           failure_class = 'lease_expired',
                           failure_reason = 'the worker holding this run stopped reporting'
                     WHERE status = 'running' AND lease_expires_at < now()
                    RETURNING document_id
                )
                UPDATE documents
                   SET status = 'pending', updated_at = now()
                 WHERE id IN (SELECT document_id FROM expired)
                   AND status = 'processing'
                RETURNING id
                """
            )
            reclaimed = [row[0] for row in cursor.fetchall()]
        self._connection.commit()
        return reclaimed


def _is_unique_violation(exc: Exception) -> bool:
    """23505, without importing psycopg at module import time.

    Checked by SQLSTATE rather than by exception class so this module stays importable
    where psycopg is not installed — the same reason every other optional dependency in
    this service is imported lazily.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    return str(sqlstate) == "23505"
