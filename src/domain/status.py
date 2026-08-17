"""The status vocabulary this service publishes.

Once the database moved upstream, the *machine* moved with it: legal transitions, the
`current_run_id` pointer, run history and the rule that a reprocess must never write
`documents.status` are all properties of a store, and this service no longer has one.

What remains is the vocabulary — the two words that appear on an outcome event, kept as an
enum so the strings are written in exactly one place. Upstream owns everything else, and
`documents.parse.completed` / `.failed` are the only things it hears from here.
"""

from __future__ import annotations

from enum import Enum


class DocumentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"
    DELETED = "deleted"
