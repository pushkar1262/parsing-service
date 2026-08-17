"""Configuration, reference resolution, and not leaking credentials.

The reference-resolution tests matter most: a bare S3 key read as a filesystem path sends
the worker looking on local disk for a document that lives in S3, and the failure it
produces (`not_found`) says nothing about the actual cause.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from config import ConfigError, Settings, _redact_url

ENV = {
    "S3_BUCKET": "acme-uploads",
    "S3_PREFIX": "raw",
    "AWS_REGION": "eu-west-1",
    "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
    "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
}


@pytest.fixture
def settings(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    for key in (
        "S3_ARTIFACT_BUCKET",
        "S3_ARTIFACT_PREFIX",
        "DATABASE_URL",
        "KAFKA_BOOTSTRAP_SERVERS",
        "ALLOW_PRIVATE_NETWORKS",
        "MAX_FETCH_BYTES",
        "LOCAL_ROOTS",
        "OCR_ENABLED",
    ):
        monkeypatch.delenv(key, raising=False)
    return Settings.from_env(load_dotenv_files=False)


# --------------------------------------------------------------------------- #
# reading the environment
# --------------------------------------------------------------------------- #


def test_the_aws_settings_are_read(settings) -> None:
    assert settings.s3_bucket == "acme-uploads"
    assert settings.aws_region == "eu-west-1"
    assert settings.has_static_credentials is True


def test_a_prefix_is_normalised_to_a_trailing_slash(settings) -> None:
    """So joining a key never yields `rawspec.pdf` or `raw//spec.pdf`."""
    assert settings.s3_prefix == "raw/"


def test_prefix_variants_all_normalise_the_same_way(monkeypatch) -> None:
    for raw in ("raw", "/raw", "raw/", "/raw/"):
        monkeypatch.setenv("S3_PREFIX", raw)
        assert Settings.from_env(load_dotenv_files=False).s3_prefix == "raw/"


def test_an_empty_prefix_stays_empty(monkeypatch) -> None:
    monkeypatch.setenv("S3_PREFIX", "")
    assert Settings.from_env(load_dotenv_files=False).s3_prefix == ""


def test_artifacts_default_to_the_same_bucket(settings) -> None:
    assert settings.artifact_bucket == "acme-uploads"


def test_artifacts_can_live_in_their_own_bucket(monkeypatch, settings) -> None:
    """Worth doing: parsed content is cheap to regenerate and can expire aggressively."""
    monkeypatch.setenv("S3_ARTIFACT_BUCKET", "acme-parsed")
    monkeypatch.setenv("S3_ARTIFACT_PREFIX", "/artifacts/")
    updated = Settings.from_env(load_dotenv_files=False)
    assert updated.artifact_bucket == "acme-parsed"
    assert updated.artifact_prefix == "artifacts/"


def test_no_static_credentials_falls_through_to_the_iam_chain(monkeypatch) -> None:
    """Which is the better production setup: a task role rather than a key in a file."""
    monkeypatch.setenv("S3_BUCKET", "b")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    assert Settings.from_env(load_dotenv_files=False).has_static_credentials is False


def test_flags_and_integers_are_parsed(monkeypatch, settings) -> None:
    monkeypatch.setenv("ALLOW_PRIVATE_NETWORKS", "true")
    monkeypatch.setenv("MAX_FETCH_BYTES", "5242880")
    monkeypatch.setenv("OCR_ENABLED", "no")
    updated = Settings.from_env(load_dotenv_files=False)
    assert updated.allow_private_networks is True
    assert updated.max_fetch_bytes == 5242880
    assert updated.ocr_enabled is False


def test_a_malformed_integer_fails_at_startup_not_at_the_first_document(monkeypatch) -> None:
    """A typo should be loud immediately, not an incident on message one."""
    monkeypatch.setenv("MAX_FETCH_BYTES", "100MB")
    with pytest.raises(ConfigError, match="must be an integer"):
        Settings.from_env(load_dotenv_files=False)


def test_missing_required_settings_are_refused(monkeypatch) -> None:
    monkeypatch.delenv("S3_BUCKET", raising=False)
    with pytest.raises(ConfigError, match="S3_BUCKET"):
        Settings.from_env(load_dotenv_files=False).require_s3()


# --------------------------------------------------------------------------- #
# reference resolution — the reason a job can carry a bare key
# --------------------------------------------------------------------------- #


def test_a_bare_key_resolves_to_the_configured_bucket_and_prefix(settings) -> None:
    """`parse_ref` would read this as a local path and look on disk for an S3 object."""
    assert (
        settings.resolve_reference("doc-42/spec.pdf")
        == "s3://acme-uploads/raw/doc-42/spec.pdf"
    )


def test_a_key_that_already_carries_the_prefix_is_not_doubled(settings) -> None:
    assert (
        settings.resolve_reference("raw/doc-42/spec.pdf")
        == "s3://acme-uploads/raw/doc-42/spec.pdf"
    )


def test_a_leading_slash_is_tolerated(settings) -> None:
    assert (
        settings.resolve_reference("/doc-42/spec.pdf")
        == "s3://acme-uploads/raw/doc-42/spec.pdf"
    )


def test_an_explicit_s3_uri_is_left_alone(settings) -> None:
    """The sender was explicit, possibly about a different bucket."""
    assert (
        settings.resolve_reference("s3://other-bucket/k/spec.pdf")
        == "s3://other-bucket/k/spec.pdf"
    )


def test_a_presigned_url_is_left_alone(settings) -> None:
    url = "https://acme-uploads.s3.eu-west-1.amazonaws.com/raw/spec.pdf?X-Amz-Signature=x"
    assert settings.resolve_reference(url) == url


def test_an_empty_reference_is_refused(settings) -> None:
    with pytest.raises(ConfigError, match="empty reference"):
        settings.resolve_reference("   ")


def test_without_a_bucket_a_bare_path_stays_a_path(monkeypatch) -> None:
    """So local development does not silently become `s3:///spec.md`."""
    monkeypatch.delenv("S3_BUCKET", raising=False)
    assert Settings.from_env(load_dotenv_files=False).resolve_reference(
        "/tmp/spec.md"
    ) == "/tmp/spec.md"


# --------------------------------------------------------------------------- #
# not leaking credentials
# --------------------------------------------------------------------------- #


def test_describe_never_contains_the_secret_key(settings) -> None:
    """`describe()` is what startup logs. A secret here is a secret in the aggregator."""
    rendered = repr(settings.describe())
    assert ENV["AWS_SECRET_ACCESS_KEY"] not in rendered
    assert settings.describe()["secret_access_key"] == "***"


def test_the_access_key_is_masked_but_still_identifiable(settings) -> None:
    """Enough to tell two keys apart in a log; not enough to use one."""
    masked = settings.describe()["access_key_id"]
    assert masked == "AKIA…MPLE"
    assert ENV["AWS_ACCESS_KEY_ID"] not in masked


def test_the_repr_of_settings_does_not_carry_the_secret(settings) -> None:
    """`field(repr=False)`, because an exception traceback prints locals."""
    assert ENV["AWS_SECRET_ACCESS_KEY"] not in repr(settings)


def test_a_database_password_is_redacted(settings) -> None:
    """Logging the URL as-is on startup is normal, and puts the password in the logs."""
    assert (
        _redact_url("postgresql://parsing:s3cr3t@db.internal:5432/parsing")
        == "postgresql://***@db.internal:5432/parsing"
    )
    assert _redact_url("postgresql://localhost/parsing") == "postgresql://localhost/parsing"
    assert _redact_url("") is None


def test_describe_reports_which_credential_source_is_in_use(settings) -> None:
    assert settings.describe()["aws_credentials"] == "static"


# --------------------------------------------------------------------------- #
# construction
# --------------------------------------------------------------------------- #


def test_the_fetch_policy_carries_the_configured_limits(monkeypatch, settings) -> None:
    monkeypatch.setenv("MAX_FETCH_BYTES", "1024")
    monkeypatch.setenv("ALLOWED_HOST_SUFFIXES", ".s3.eu-west-1.amazonaws.com, .example.com")
    policy = Settings.from_env(load_dotenv_files=False).fetch_policy()
    assert policy.max_bytes == 1024
    assert policy.net.allowed_host_suffixes == (
        ".s3.eu-west-1.amazonaws.com",
        ".example.com",
    )
    assert policy.net.allow_private_networks is False


def test_local_roots_are_paths(monkeypatch, settings) -> None:
    monkeypatch.setenv("LOCAL_ROOTS", "/tmp/inbox,/var/spool/docs")
    roots = Settings.from_env(load_dotenv_files=False).local_roots
    assert roots == (Path("/tmp/inbox"), Path("/var/spool/docs"))


def test_the_artifact_store_applies_its_prefix(monkeypatch, settings) -> None:
    monkeypatch.setenv("S3_ARTIFACT_PREFIX", "artifacts")

    class Stub:
        def __init__(self):
            self.keys = []

        def put_object(self, **kwargs):
            self.keys.append(kwargs["Key"])

    stub = Stub()
    store = Settings.from_env(load_dotenv_files=False).artifact_store(s3_client=stub)
    store._write("parsed/abc/1.0/document.json", b"{}")
    assert stub.keys == ["artifacts/parsed/abc/1.0/document.json"]


def test_ocr_disabled_yields_the_null_backend(monkeypatch, settings) -> None:
    monkeypatch.setenv("OCR_ENABLED", "false")
    backend = Settings.from_env(load_dotenv_files=False).ocr_backend()
    assert backend.name == "null"
    assert backend.available() is False
