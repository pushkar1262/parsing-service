"""Fetching raw files: reference parsing, size caps, and the address guard.

The address tests are the ones that matter most and they need no network — the
resolver is injectable, so "what if this hostname resolves to the metadata service"
is a unit test rather than a thing discovered in production.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from domain.errors import (
    AccessDenied,
    BlockedAddress,
    ObjectNotFound,
    ObjectTooLarge,
    StorageUnavailable,
)
from store.blobs import FetchPolicy, Storage
from store.net import NetPolicy, validate_url
from store.refs import parse_ref

PRESIGNED = (
    "https://acme-uploads.s3.eu-west-1.amazonaws.com/raw/doc-42/spec.pdf"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef"
)


def _resolver(*addresses: str):
    """A stand-in for DNS, so address policy is testable offline."""
    return lambda host, port: list(addresses)


# --------------------------------------------------------------------------- #
# reference parsing
# --------------------------------------------------------------------------- #


def test_an_s3_uri_yields_bucket_and_key() -> None:
    ref = parse_ref("s3://acme-uploads/raw/doc-42/spec.pdf")
    assert ref.kind == "s3"
    assert ref.bucket == "acme-uploads"
    assert ref.key == "raw/doc-42/spec.pdf"
    assert ref.version_id is None


def test_an_s3_uri_carries_a_version_id() -> None:
    ref = parse_ref("s3://acme-uploads/raw/spec.pdf?versionId=abc123")
    assert ref.version_id == "abc123"


def test_a_percent_encoded_key_is_decoded() -> None:
    """S3 keys legitimately contain spaces, and the URI form escapes them."""
    ref = parse_ref("s3://acme/raw/Q3%20requirements.docx")
    assert ref.key == "raw/Q3 requirements.docx"


def test_a_presigned_url_is_fetched_over_http_but_keeps_its_linkage() -> None:
    """The signature is the authorisation, so the URL is used verbatim.

    Bucket and key are still recovered, because the linkage back to the raw file has
    to outlive the URL that delivered it — a presigned URL expires in minutes, and a
    reprocess may happen months later.
    """
    ref = parse_ref(PRESIGNED)
    assert ref.kind == "http"
    assert ref.bucket == "acme-uploads"
    assert ref.key == "raw/doc-42/spec.pdf"
    assert ref.region == "eu-west-1"
    assert ref.url == PRESIGNED


def test_path_style_s3_urls_are_recognised_too() -> None:
    ref = parse_ref("https://s3.us-east-1.amazonaws.com/acme-uploads/raw/spec.pdf")
    assert ref.bucket == "acme-uploads"
    assert ref.key == "raw/spec.pdf"


def test_a_non_s3_url_simply_has_no_bucket() -> None:
    """Best-effort linkage must never break the fetch."""
    ref = parse_ref("https://files.example.com/d/abc123")
    assert ref.kind == "http"
    assert ref.bucket is None
    assert ref.url == "https://files.example.com/d/abc123"


def test_a_signature_never_appears_in_the_log_description() -> None:
    """A presigned URL is a bearer credential; logging it whole leaks the object."""
    assert "X-Amz-Signature" not in parse_ref(PRESIGNED).describe
    assert parse_ref(PRESIGNED).describe.endswith("/raw/doc-42/spec.pdf")


def test_a_bare_path_is_a_local_reference() -> None:
    assert parse_ref("/tmp/spec.md").kind == "file"
    assert parse_ref("file:///tmp/spec.md").kind == "file"


def test_a_nonsense_scheme_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported reference scheme"):
        parse_ref("gopher://example.com/spec.pdf")


# --------------------------------------------------------------------------- #
# the address guard — the reason this layer is not just urlopen()
# --------------------------------------------------------------------------- #


def test_the_instance_metadata_address_is_blocked() -> None:
    """The attack this guard exists for.

    A URL resolving to 169.254.169.254 would return the worker's own IAM credentials,
    which would then be parsed, stored as document content, and served over the content
    API. Credential exfiltration dressed as a text file.
    """
    policy = NetPolicy(resolver=_resolver("169.254.169.254"))
    with pytest.raises(BlockedAddress, match="metadata"):
        validate_url("https://uploads.example.com/spec.pdf", policy)


def test_loopback_and_private_addresses_are_blocked() -> None:
    for address in ("127.0.0.1", "10.0.0.5", "192.168.1.10", "172.16.4.4", "::1"):
        policy = NetPolicy(resolver=_resolver(address))
        with pytest.raises(BlockedAddress):
            validate_url("https://uploads.example.com/spec.pdf", policy)


def test_a_hostname_is_judged_by_what_it_resolves_to() -> None:
    """A public-looking name proves nothing: an attacker controls their own DNS."""
    policy = NetPolicy(resolver=_resolver("127.0.0.1"))
    with pytest.raises(BlockedAddress, match="loopback"):
        validate_url("https://totally-legit-cdn.example.com/spec.pdf", policy)


def test_every_resolved_address_must_pass_not_just_the_first() -> None:
    """Which address the socket picks is not ours to decide, so all must be safe."""
    policy = NetPolicy(resolver=_resolver("93.184.216.34", "169.254.169.254"))
    with pytest.raises(BlockedAddress):
        validate_url("https://uploads.example.com/spec.pdf", policy)


def test_an_ipv4_mapped_ipv6_address_cannot_smuggle_a_private_target() -> None:
    """`::ffff:169.254.169.254` passes a naive is_private check."""
    policy = NetPolicy(resolver=_resolver("::ffff:169.254.169.254"))
    with pytest.raises(BlockedAddress):
        validate_url("https://uploads.example.com/spec.pdf", policy)


def test_a_public_address_is_permitted() -> None:
    policy = NetPolicy(resolver=_resolver("93.184.216.34"))
    assert validate_url("https://uploads.example.com/spec.pdf", policy) == [
        "93.184.216.34"
    ]


def test_plain_http_is_refused_by_default() -> None:
    policy = NetPolicy(resolver=_resolver("93.184.216.34"))
    with pytest.raises(BlockedAddress, match="scheme"):
        validate_url("http://uploads.example.com/spec.pdf", policy)


def test_a_host_allowlist_excludes_everything_else() -> None:
    policy = NetPolicy(
        allowed_host_suffixes=(".s3.eu-west-1.amazonaws.com",),
        resolver=_resolver("93.184.216.34"),
    )
    validate_url(PRESIGNED, policy)
    with pytest.raises(BlockedAddress, match="allowlist"):
        validate_url("https://files.example.com/spec.pdf", policy)


def test_private_networks_can_be_allowed_explicitly() -> None:
    """A VPC endpoint resolves the bucket to a private address, legitimately.

    The escape hatch exists for exactly that, and it is off unless configured.
    """
    policy = NetPolicy(allow_private_networks=True, resolver=_resolver("10.0.3.7"))
    assert validate_url("https://bucket.vpce.amazonaws.com/spec.pdf", policy)


# --------------------------------------------------------------------------- #
# local reads, jailed
# --------------------------------------------------------------------------- #


def test_a_local_read_returns_bytes_hash_and_linkage(tmp_path: Path) -> None:
    target = tmp_path / "spec.md"
    target.write_text("# Spec\n\nA requirement.\n")

    fetched = Storage(FetchPolicy(local_roots=(tmp_path,))).fetch(str(target))
    assert fetched.data == b"# Spec\n\nA requirement.\n"
    assert len(fetched.content_hash) == 64
    assert fetched.source.byte_size == len(fetched.data)


def test_the_hash_matches_the_parse_stages_content_hash(tmp_path: Path) -> None:
    """Both sides must agree, or the content-addressed artifact key is wrong."""
    from parse.pipeline import content_hash_of

    target = tmp_path / "spec.md"
    target.write_bytes(b"# Spec\n")
    fetched = Storage(FetchPolicy(local_roots=(tmp_path,))).fetch(str(target))
    assert fetched.content_hash == content_hash_of(b"# Spec\n")


def test_local_references_are_refused_unless_a_root_is_configured(tmp_path: Path) -> None:
    """A worker should not read `file:///etc/shadow` just because paths parse."""
    target = tmp_path / "spec.md"
    target.write_text("x")
    with pytest.raises(AccessDenied, match="not enabled"):
        Storage().fetch(str(target))


def test_a_traversal_out_of_the_jail_is_refused(tmp_path: Path) -> None:
    jail = tmp_path / "inbox"
    jail.mkdir()
    secret = tmp_path / "secret.txt"
    secret.write_text("private")

    storage = Storage(FetchPolicy(local_roots=(jail,)))
    with pytest.raises(AccessDenied, match="outside the permitted roots"):
        storage.fetch(str(jail / ".." / "secret.txt"))


def test_a_missing_file_is_a_permanent_failure(tmp_path: Path) -> None:
    with pytest.raises(ObjectNotFound):
        Storage(FetchPolicy(local_roots=(tmp_path,))).fetch(str(tmp_path / "gone.md"))


def test_an_oversized_file_is_rejected_before_being_read(tmp_path: Path) -> None:
    target = tmp_path / "big.txt"
    target.write_bytes(b"x" * 5000)
    storage = Storage(FetchPolicy(max_bytes=1000, local_roots=(tmp_path,)))
    with pytest.raises(ObjectTooLarge):
        storage.fetch(str(target))


def test_an_empty_object_is_not_a_document(tmp_path: Path) -> None:
    target = tmp_path / "empty.txt"
    target.write_bytes(b"")
    with pytest.raises(ObjectNotFound, match="empty"):
        Storage(FetchPolicy(local_roots=(tmp_path,))).fetch(str(target))


# --------------------------------------------------------------------------- #
# S3, against a stub client
# --------------------------------------------------------------------------- #


class _StubS3:
    """Just enough of the boto3 client surface, so tests never touch AWS."""

    def __init__(self, body: bytes, *, content_type=None, version="v1", error=None):
        self.body = body
        self.content_type = content_type
        self.version = version
        self.error = error
        self.calls: list[dict] = []

    def head_object(self, **kwargs):
        self.calls.append({"op": "head", **kwargs})
        if self.error:
            raise self.error
        return {"ContentLength": len(self.body), "ContentType": self.content_type}

    def get_object(self, **kwargs):
        self.calls.append({"op": "get", **kwargs})
        if self.error:
            raise self.error
        return {
            "Body": io.BytesIO(self.body),
            "ContentType": self.content_type,
            "VersionId": self.version,
        }


class _ClientError(Exception):
    def __init__(self, code: str, status: int = 400):
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


def test_an_s3_fetch_returns_content_and_records_the_version() -> None:
    """The response's VersionId pins which bytes were parsed.

    Without it, an overwrite makes the linkage ambiguous and a later reprocess could
    silently parse different content than the original run.
    """
    stub = _StubS3(b"# Spec\n\nRequirement.\n", content_type="text/markdown; charset=utf-8")
    fetched = Storage(s3_client=stub).fetch("s3://acme/raw/spec.md")

    assert fetched.data.startswith(b"# Spec")
    assert fetched.source.bucket == "acme"
    assert fetched.source.key == "raw/spec.md"
    assert fetched.source.version_id == "v1"
    # Charset parameters are stripped so the type compares cleanly.
    assert fetched.declared_media_type == "text/markdown"


def test_a_requested_version_is_passed_through() -> None:
    stub = _StubS3(b"content")
    Storage(s3_client=stub).fetch("s3://acme/raw/spec.md?versionId=xyz")
    assert all(call.get("VersionId") == "xyz" for call in stub.calls)


def test_an_oversized_object_is_refused_from_the_head_request() -> None:
    """Refuse a 3 GB object without transferring it."""
    stub = _StubS3(b"x" * 4096)
    storage = Storage(FetchPolicy(max_bytes=100), s3_client=stub)
    with pytest.raises(ObjectTooLarge, match="declares"):
        storage.fetch("s3://acme/raw/big.bin")
    assert [call["op"] for call in stub.calls] == ["head"]  # never downloaded


def test_a_missing_key_is_permanent() -> None:
    stub = _StubS3(b"", error=_ClientError("NoSuchKey", 404))
    with pytest.raises(ObjectNotFound) as caught:
        Storage(s3_client=stub).fetch("s3://acme/raw/gone.md")
    assert caught.value.transient is False
    assert caught.value.failure_class == "not_found"


def test_access_denied_mentions_an_expired_signature() -> None:
    """The likeliest cause, and the one with a different fix from an IAM problem."""
    stub = _StubS3(b"", error=_ClientError("AccessDenied", 403))
    with pytest.raises(AccessDenied, match="expired"):
        Storage(s3_client=stub).fetch("s3://acme/raw/spec.md")


def test_a_server_error_is_transient_so_it_reaches_the_retry_tiers() -> None:
    stub = _StubS3(b"", error=_ClientError("ServiceUnavailable", 503))
    with pytest.raises(StorageUnavailable) as caught:
        Storage(s3_client=stub).fetch("s3://acme/raw/spec.md")
    assert caught.value.transient is True


def test_throttling_is_transient() -> None:
    stub = _StubS3(b"", error=_ClientError("SlowDown", 503))
    with pytest.raises(StorageUnavailable) as caught:
        Storage(s3_client=stub).fetch("s3://acme/raw/spec.md")
    assert caught.value.transient is True


def test_an_unrecognised_error_is_treated_as_transient() -> None:
    """A wrong retry costs a little money; a wrong DLQ routing loses a document."""
    stub = _StubS3(b"", error=_ClientError("SomethingNobodyDocumented", 400))
    with pytest.raises(StorageUnavailable) as caught:
        Storage(s3_client=stub).fetch("s3://acme/raw/spec.md")
    assert caught.value.transient is True


# --------------------------------------------------------------------------- #
# fetch → parse, the seam the worker will use
# --------------------------------------------------------------------------- #


def test_a_fetched_object_parses_with_its_hash_and_linkage_intact() -> None:
    from parse.pipeline import parse_document

    stub = _StubS3(b"# Spec\n\nAuthenticate users within 300ms.\n")
    fetched = Storage(s3_client=stub).fetch("s3://acme/raw/spec.md")

    doc = parse_document(
        fetched.data,
        document_id="doc-42",
        content_hash=fetched.content_hash,
        media_type=fetched.declared_media_type,
        filename="spec.md",
        source=fetched.source,
    )
    assert doc.content_hash == fetched.content_hash
    assert doc.source is not None
    assert doc.source.key == "raw/spec.md"
    assert doc.source.version_id == "v1"
    assert "Authenticate users within 300ms." in doc.text
