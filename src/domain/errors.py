"""One failure taxonomy, because retry routing depends on it.

Every failure in this service answers one question before any other: *will trying
again help?* A transient failure walks the retry tiers; a permanent one goes straight
to the dead-letter queue with a recorded reason. Retrying an unsupported format four
times is pure cost with a guaranteed outcome, and retrying nothing at all means a
momentary S3 blip loses a document.

So the taxonomy lives here rather than being re-derived at each `except` clause, and
`failure_class` is the string that reaches the document row, the DLQ message, and the
`dlq_total{failure_class}` metric — the same vocabulary end to end.
"""

from __future__ import annotations


class ServiceError(Exception):
    """Base for every failure this service classifies.

    `transient` drives retry routing. `failure_class` is what gets recorded, and it
    is deliberately a plain string rather than an enum: it crosses into JSON logs, a
    database column and a metric label, and every one of those wants the string.
    """

    transient: bool = False
    failure_class: str = "internal"


# --------------------------------------------------------------------------- #
# permanent — retrying cannot change the outcome
# --------------------------------------------------------------------------- #


class PermanentFailure(ServiceError):
    transient = False


class UnsupportedFormat(PermanentFailure):
    """No parser claims this media type.

    Kept distinct from `CorruptDocument` because the operator-facing message differs:
    "we do not read .pages files" is a feature request, "your PDF is truncated" is a
    problem with the upload. Collapsing them wastes somebody's afternoon.
    """

    failure_class = "unsupported_format"


class CorruptDocument(PermanentFailure):
    """The right parser could not open the bytes."""

    failure_class = "corrupt"


class EncryptedDocument(PermanentFailure):
    """Password-protected, and we have no password.

    Its own class because it is the one permanent failure a *user* can act on by
    re-uploading an unlocked copy, so the status message should say exactly that.
    """

    failure_class = "encrypted"


class ObjectTooLarge(PermanentFailure):
    failure_class = "too_large"


class ObjectNotFound(PermanentFailure):
    """The reference points at nothing.

    Permanent on purpose, even though it looks like it could be a race with an
    in-flight upload. If the backend publishes the job after the upload completes,
    a missing object means the reference is wrong or the object was deleted — and
    retrying a wrong reference for thirty minutes helps no one. A genuine race is
    better fixed by the backend's ordering than by retries here.
    """

    failure_class = "not_found"


class AccessDenied(PermanentFailure):
    """We are not allowed to read the object.

    Permanent, and the most common cause is an expired presigned URL rather than a
    misconfigured role — so the recorded reason should say so, because the fix is to
    reprocess with a fresh reference rather than to page somebody about IAM.
    """

    failure_class = "access_denied"


class BlockedAddress(PermanentFailure):
    """The URL resolves somewhere we refuse to fetch from.

    A security control, not an availability problem — see `store.net`. Permanent
    because a blocked address will still be blocked in thirty minutes, and because a
    retry loop against an internal address is exactly what an attacker would want.
    """

    failure_class = "blocked_address"


class MalwareDetected(PermanentFailure):
    failure_class = "malware"


# --------------------------------------------------------------------------- #
# transient — the same input may well succeed later
# --------------------------------------------------------------------------- #


class TransientFailure(ServiceError):
    transient = True


class StorageUnavailable(TransientFailure):
    """Blob storage returned a 5xx, throttled us, or the connection failed."""

    failure_class = "storage_unavailable"


class FetchTimeout(TransientFailure):
    failure_class = "fetch_timeout"


class OcrUnavailable(TransientFailure):
    failure_class = "ocr_unavailable"
