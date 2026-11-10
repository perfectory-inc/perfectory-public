"""Which documents a run reads: only the hashes this extractor version has not read yet.

Computed each run from the attachment index and the outcome table (the shape of root
ADR-0352), not from a stored queue. A document read once under a version is never read again
under it, whatever its outcome: a failure is retried only by a new version, which is a
declared full reprocess.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class Document:
    sha256: str
    media_format: str
    notice_sequence: int


def plan(
    index_rows: Iterable[dict],
    done_hashes: set[str],
    *,
    limit: int,
) -> list[Document]:
    """Newest notice first; one entry per hash, at its newest notice."""
    newest: dict[str, Document] = {}
    for row in index_rows:
        sha = row["sha256"]
        if sha in done_hashes:
            continue
        doc = Document(sha, str(row["media_format"]).lower(), int(row["notice_sequence"]))
        current = newest.get(sha)
        if current is None or doc.notice_sequence > current.notice_sequence:
            newest[sha] = doc
    ordered = sorted(newest.values(), key=lambda d: (-d.notice_sequence, d.sha256))
    return ordered[:limit]
