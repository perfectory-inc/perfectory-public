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
