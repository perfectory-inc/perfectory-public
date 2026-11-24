"""What the 실거래 Gold's Spark job still reads of the 국토교통부 실거래가 Silver load (root ADR-0348, ADR-0349).

The load itself left Spark (root ADR-0559): the publisher's `load-rt-molit-real-transaction-silver`
reads the exports and places their lots in DuckDB (`rt_molit_silver_stage`). This file keeps the two
things `real_transaction_silver_to_gold.py` imports until that job is taken over too: the source
contract, and the load's reading of a deal's full address (`dong_index`, which the DuckDB placement
ports as `real_transaction_placement_stage::dong_index`).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "rt-molit-real-transaction.contract.json"


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dong_index(rows: Iterable[tuple[str, str, str]]) -> dict[str, str]:
    """Full 법정동 name → code, from (code, full name, status) rows of one snapshot. A current (존재)
    code wins over an abolished one; a name two codes of the same standing share is left out."""

    by_rank: dict[str, dict[int, set[str]]] = {}
    for code, name, status in rows:
        if not re.fullmatch(r"\d{10}", code or "") or code[5:] == "00000":
            continue
        key = " ".join((name or "").split())
        by_rank.setdefault(key, {}).setdefault(0 if status == "존재" else 1, set()).add(code)
    index = {}
    for key, ranks in by_rank.items():
        codes = ranks[min(ranks)]
        if len(codes) == 1:
            index[key] = next(iter(codes))
    return index
