"""Format detection by looking at the bytes.

The extension is a hint and the uploader's declared content type is a claim; both
are attacker-controlled and both are routinely wrong by accident. What the file
*is* has to come from its contents.

No libmagic dependency: the formats this service cares about all have short,
unambiguous signatures, and the OOXML family needs a zip directory listing that
libmagic cannot do anyway — a .docx and a .xlsx are the same magic bytes, and only
the archive's contents tell them apart.

This returns a *candidate*. Confirmation is the parser opening the file
successfully, which is why `intake` treats a detection that parses as valid and a
detection that fails to parse as `corrupt` rather than `unsupported_format`.
"""

from __future__ import annotations

import io
import zipfile

# Signatures checked longest-first, since some are prefixes of others.
_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"%PDF-", "application/pdf"),
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
    (b"BM", "image/bmp"),
    (b"II*\x00", "image/tiff"),
    (b"MM\x00*", "image/tiff"),
    (b"{\\rtf", "application/rtf"),
    # Legacy OLE compound file: .doc, .xls, .ppt all share it.
    (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", "application/x-ole-storage"),
)

_ZIP_MAGIC = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")

# Entry prefixes that identify which OOXML format a zip actually holds.
_OOXML = (
    ("word/", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ("xl/", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ("ppt/", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
)

_EXTENSIONS = {
    "md": "text/markdown",
    "markdown": "text/markdown",
    "txt": "text/plain",
    "text": "text/plain",
    "csv": "text/csv",
    "json": "application/json",
    "html": "text/html",
    "htm": "text/html",
}


def _zip_media_type(data: bytes) -> str:
    """Tell the OOXML formats apart by what is inside the archive.

    Also the cheapest place to notice a zip that is not an Office document at all:
    a plain .zip upload reaches here and leaves as `application/zip`, which no
    parser claims, so it becomes a clean `unsupported_format` rejection instead of
    an obscure failure inside a DOCX parser.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = archive.namelist()
    except (zipfile.BadZipFile, OSError):
        return "application/zip"

    # An OOXML package declares its part in [Content_Types].xml, but the directory
    # prefixes are simpler and just as decisive.
    for prefix, media_type in _OOXML:
        if any(name.startswith(prefix) for name in names):
            return media_type
    if any(name.startswith("mimetype") for name in names):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                declared = archive.read("mimetype").decode("ascii", "ignore").strip()
            if declared:
                return declared  # ODF and friends state their own type
        except (KeyError, zipfile.BadZipFile, OSError):
            pass
    return "application/zip"


def _looks_like_text(data: bytes) -> bool:
    """Decodable as text, and free of the NUL bytes binaries are full of.

    The UTF-16 check has to come first and be decisive. UTF-16-encoded ASCII is
    *half* NUL bytes by construction, so the usual "a NUL means binary" rule
    misclassifies every UTF-16 document — which is exactly what a Windows "Save as
    Unicode text" produces.
    """
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return True

    sample = data[:8192]
    if b"\x00" in sample:
        return False
    try:
        sample.decode("utf-8")
        return True
    except UnicodeDecodeError:
        pass

    # A multi-byte sequence straddling the sample boundary is not evidence of
    # binary, so retry having trimmed the partial character off the end.
    for trim in (1, 2, 3):
        try:
            sample[:-trim].decode("utf-8")
            return True
        except UnicodeDecodeError:
            continue

    # Not UTF-8. Could still be a legacy single-byte encoding, which `normalise.decode`
    # recovers — so fall back to asking whether the bytes look like prose.
    return _mostly_printable(sample)


def _mostly_printable(sample: bytes) -> bool:
    if not sample:
        return True
    printable = sum(1 for byte in sample if byte in (9, 10, 13) or 32 <= byte < 127)
    return printable / len(sample) > 0.85


def detect(data: bytes, *, filename: str | None = None) -> str:
    """The candidate media type for these bytes.

    `filename` is consulted only to separate text subtypes — Markdown and plain text
    are byte-identical, and the distinction changes nothing about how we parse, only
    what we record. It never overrides a magic-byte match.
    """
    for signature, media_type in _MAGIC:
        if data.startswith(signature):
            return media_type

    if data.startswith(_ZIP_MAGIC):
        return _zip_media_type(data)

    if _looks_like_text(data):
        if filename and "." in filename:
            extension = filename.rsplit(".", 1)[-1].lower()
            if extension in _EXTENSIONS:
                return _EXTENSIONS[extension]
        return "text/plain"

    return "application/octet-stream"
