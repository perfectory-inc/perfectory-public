#!/usr/bin/env python3
"""The closed items the onbid collector forgot before it kept them (root ADR-0542 §3).

Until ADR-0542 the collector dropped an item from its state once the item list stopped showing it,
so an item that left the list before the 물건상세 backlog reached it was never asked again
(2026-11-11: 2,569 of 28,500 items, most of them from 10-19..10-29, when detail collection had just
begun). From now on the collector keeps such an item itself (`State.closed_owed`); this reads the ones
it already forgot, once: every item Silver has a list row for and no 물건상세 row, through the round
Silver saw it under last, written as the JSON list of item keys the collector's
`FOUNDATION_PLATFORM_DATA_GO_KR_ONBID_CLOSED_OWED_SEED` reads. onbid-collect.sh runs it until one run
has taken its list (a marker under the collector's state), so it is one read, not a step of every run.

    onbid_closed_owed_seed.py OUTPUT
"""

from __future__ import annotations

import csv
import io
import json
import subprocess
import sys
from pathlib import Path

TRINO_CONTAINER = "foundation-platform-trino"
ITEMS = "silver.onbid_real_estate_item"
DETAILS = "silver.onbid_real_estate_item_detail"

SQL = f"""
SELECT i.cltr_mng_no, CAST(max_by(i.pbct_cdtn_no, i.ingested_at_utc) AS varchar)
FROM {ITEMS} i
WHERE NOT EXISTS (SELECT 1 FROM {DETAILS} d WHERE d.cltr_mng_no = i.cltr_mng_no)
GROUP BY i.cltr_mng_no
ORDER BY i.cltr_mng_no
"""


def trino(sql: str) -> list[list[str]]:
    completed = subprocess.run(
        ["docker", "exec", TRINO_CONTAINER, "trino", "--catalog", "r2", "--output-format", "CSV_UNQUOTED",
         "--execute", sql],
        capture_output=True, text=True, timeout=600, check=False,
    )
    if completed.returncode != 0:
        last = completed.stderr.strip().splitlines()[-1:] or ["trino failed"]
        raise RuntimeError(f"the Silver read failed: {last[0]}")
    return [row for row in csv.reader(io.StringIO(completed.stdout)) if row]


def keys(rows: list[list[str]]) -> list[dict[str, object]]:
    """The collector's `ItemKey`s; a row without both parts is not a key."""
    out = []
    for row in rows:
        if len(row) != 2 or not row[0].strip() or not row[1].strip().lstrip("-").isdigit():
            continue
        out.append({"cltr_mng_no": row[0].strip(), "pbct_cdtn_no": int(row[1])})
    return out


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[-1].strip(), file=sys.stderr)
        return 2
    seed = keys(trino(SQL))
    Path(argv[1]).write_text(json.dumps(seed), encoding="utf-8")
    print(f"onbid closed-owed seed: {len(seed)} items without a 물건상세 in Silver")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
