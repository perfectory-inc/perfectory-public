"""How far a table's data files are from its cluster key's order (root ADR-0314 §3, ADR-0344 §5).

One measure, read by two: the shape-ledger check fails a table on it (scripts/ops/shape-ledger-check.py)
and lakehouse_maintenance re-sorts a table on it. Two copies of the number would let the check fail a
table that maintenance never repairs. No PySpark here: the check runs on the host without it.
"""
from __future__ import annotations

# A reader's unit of a cluster key: for `pnu` one 시도, the key's first two digits (a by-PNU bake
# shard, root ADR-0164). On 2026-10-28 every one of gold.parcel_panel's 9 files spanned PNUs 11..52,
# so each shard read the whole 4 GB table.
REGION_DIGITS = {"pnu": 2}
# A one-region read may open at most this share of the table's data files. Range-written tables
# measured 2026-10-28 read 0.16-0.33; the three whose files all span the country read 1.00.
REGION_READ_SHARE_LIMIT = 0.5


def region_read_share(bounds: list[tuple[str, str]], digits: int) -> float:
    """The mean share of data files a read of one region opens, by each file's lower and upper key
    bound: a region is the key's first `digits` characters, and a file holds every region between
    its bounds' regions. The regions are those the bounds name."""
    spans = [(low[:digits], high[:digits]) for low, high in bounds]
    regions = sorted({region for span in spans for region in span})
    if len(spans) < 2 or not regions:
        return 0.0
    return sum(sum(low <= region <= high for low, high in spans) for region in regions) / (len(regions) * len(spans))


def read_share_limit(files: int) -> float:
    """The share past which `files` data files are out of order: REGION_READ_SHARE_LIMIT, or what a
    table in perfect key order reaches with that many files, whichever is higher.

    Files in key order meet where one region ends and the next begins, and a region a split falls in
    is read from both files: n ordered files name n + 1 regions, and n - 1 of them open two files, so
    the measure is 2 / (n + 1). 2026-11-13 gold.parcel_real_transaction, range-written and compacted in
    pnu order into 2 files, measured 67% and the shape-ledger check failed it at every run; maintenance
    could not do better. Files that each span the country still measure 100%."""
    return max(REGION_READ_SHARE_LIMIT, 2 / (files + 1)) if files > 1 else 1.0
