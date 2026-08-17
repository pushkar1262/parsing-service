"""The same repository contract, against a live Postgres.

Skipped unless `PARSING_TEST_DATABASE_URL` is set, because most environments have no
database — including the one this was written in, which is exactly why this file exists
and says so. `InMemoryRepository` pins the *semantics*; only this file can tell you the
SQL is right.

    docker compose up -d postgres
    PARSING_TEST_DATABASE_URL=postgresql://parsing:parsing@localhost:5432/parsing \\
        python -m pytest tests/test_postgres.py -q

The tests deliberately mirror the ones in `test_worker.py`. If the two implementations
ever disagree, the bug is real and it is in the one that is not covered by the other's
tests.
"""

from __future__ import annotations

import os
import uuid

import pytest

DATABASE_URL = os.environ.get("PARSING_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL, reason="set PARSING_TEST_DATABASE_URL to run the Postgres tests"
)

psycopg = pytest.importorskip("psycopg", reason="psycopg is an optional dependency")

from domain.status import DocumentStatus, ParseRun, RunStatus
from store.postgres import PostgresRepository
from store.repository import DuplicateRun


@pytest.fixture
def repository():
    connection = psycopg.connect(DATABASE_URL)
    repo = PostgresRepository(connection)
    repo.migrate()
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE blocks, parse_runs, documents CASCADE")
    connection.commit()
    yield repo
    connection.close()


def _run(document_id: str, **kwargs) -> ParseRun:
    return ParseRun(
        id=str(uuid.uuid4()),
        document_id=document_id,
        content_hash=kwargs.pop("content_hash", "hash-1"),
        parser_version=kwargs.pop("parser_version", "1.0"),
        **kwargs,
    )


def test_register_is_idempotent_and_never_resets_status(repository) -> None:
    """A replayed job must not send a ready document back to pending."""
    repository.register("doc-1", source_uri="s3://b/k")
    repository.claim("doc-1")
    run = repository.start_run(_run("doc-1"))
    repository.complete_run(run.id, artifact_key="k", content_hash="hash-1")

    repository.register("doc-1", source_uri="s3://b/k")
    assert repository.get("doc-1").status is DocumentStatus.READY


def test_claim_is_atomic_and_only_one_caller_wins(repository) -> None:
    repository.register("doc-2")
    assert repository.claim("doc-2").claimed is True
    second = repository.claim("doc-2")
    assert second.claimed is False
    assert "processing" in second.reason


def test_a_ready_document_cannot_be_claimed(repository) -> None:
    repository.register("doc-3")
    repository.claim("doc-3")
    run = repository.start_run(_run("doc-3"))
    repository.complete_run(run.id, artifact_key="k", content_hash="hash-1")
    assert repository.claim("doc-3").claimed is False


def test_a_failed_document_can_be_reclaimed_so_a_dlq_replay_works(repository) -> None:
    repository.register("doc-4")
    repository.claim("doc-4")
    run = repository.start_run(_run("doc-4"))
    repository.fail_run(
        run.id, failure_class="corrupt", failure_reason="truncated", permanent=True
    )
    assert repository.get("doc-4").status is DocumentStatus.FAILED
    assert repository.claim("doc-4").claimed is True


def test_the_unique_constraint_rejects_a_duplicate_run(repository) -> None:
    """The idempotency key, enforced by Postgres rather than by a prior SELECT."""
    repository.register("doc-5")
    repository.claim("doc-5")
    repository.start_run(_run("doc-5"))
    with pytest.raises(DuplicateRun):
        repository.start_run(_run("doc-5"))


def test_a_different_parser_version_is_a_different_run(repository) -> None:
    repository.register("doc-6")
    repository.claim("doc-6")
    repository.start_run(_run("doc-6", parser_version="1.0"))
    repository.start_run(_run("doc-6", parser_version="2.0"))
    assert len(repository.runs_for("doc-6")) == 2


def test_complete_run_flips_the_pointer_atomically(repository) -> None:
    repository.register("doc-7")
    repository.claim("doc-7")
    run = repository.start_run(_run("doc-7"))
    record = repository.complete_run(
        run.id, artifact_key="parsed/x/1.0/document.json", content_hash="hash-1"
    )
    assert record.status is DocumentStatus.READY
    assert record.current_run_id == run.id
    assert repository.get_run(run.id).status is RunStatus.SUCCEEDED


def test_a_failed_reprocess_leaves_a_ready_document_alone(repository) -> None:
    """`WHERE current_run_id IS NULL` — the reprocessing guarantee as a predicate."""
    repository.register("doc-8")
    repository.claim("doc-8")
    first = repository.start_run(_run("doc-8"))
    repository.complete_run(first.id, artifact_key="k", content_hash="hash-1")

    second = repository.start_run(_run("doc-8", content_hash="hash-2"))
    repository.fail_run(
        second.id, failure_class="corrupt", failure_reason="new parser choked",
        permanent=True,
    )

    record = repository.get("doc-8")
    assert record.status is DocumentStatus.READY
    assert record.current_run_id == first.id


def test_expired_leases_are_reclaimed(repository) -> None:
    repository.register("doc-9")
    repository.claim("doc-9")
    run = repository.start_run(_run("doc-9"))
    with repository._connection.cursor() as cursor:
        cursor.execute(
            "UPDATE parse_runs SET lease_expires_at = now() - interval '1 hour' "
            "WHERE id = %s",
            (run.id,),
        )
    repository._connection.commit()

    assert repository.reclaim_expired() == ["doc-9"]
    assert repository.get("doc-9").status is DocumentStatus.PENDING


def test_metadata_round_trips_through_jsonb(repository) -> None:
    repository.register("doc-10")
    repository.claim("doc-10")
    run = repository.start_run(_run("doc-10"))
    record = repository.complete_run(
        run.id,
        artifact_key="k",
        content_hash="hash-1",
        metadata={"format": "pdf", "page_count": 12, "warnings": ["page_needs_ocr"]},
    )
    assert record.metadata["page_count"] == 12
    assert repository.get("doc-10").metadata["warnings"] == ["page_needs_ocr"]


def test_batch_get_returns_only_known_documents(repository) -> None:
    repository.register("doc-11")
    repository.register("doc-12")
    found = repository.get_many(["doc-11", "doc-12", "ghost"])
    assert {r.id for r in found} == {"doc-11", "doc-12"}


def test_a_deleted_document_cannot_be_claimed(repository) -> None:
    repository.register("doc-13")
    repository.mark_deleted("doc-13")
    claim = repository.claim("doc-13")
    assert claim.claimed is False
    assert "deleted" in claim.reason
