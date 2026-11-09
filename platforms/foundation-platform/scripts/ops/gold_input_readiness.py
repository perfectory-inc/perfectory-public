#!/usr/bin/env python3
"""Do the Silver tables a lane's Gold is built from exist yet? (root ADR-0471)

    gold_input_readiness.py <gold table>    `ready ...` (exit 0) or `waiting ...` (exit 75)

The Gold planner refuses a table one of whose inputs does not exist (gold_rebuild.plan: a required
input, or an optional one not named under absent_inputs_accepted), but only inside the gold step, under
Spark, after the cut-over claimed the spark pool and waited for what ran. On 2026-11-09 four cut-overs
failed so in under 75 s (parcel-court-auction 11:59Z and 17:30Z, parcel-building-permits 16:58Z with
8 of its Silver tables not loaded, parcel-nearby 17:42Z with 7), each paging, and the parcel-nearby run
of 18:42Z held the claim behind two Spark jobs for more than 18 minutes while the start of the
data.go.kr load that makes its seven tables waited in the queue behind that claim. This asks the
planner's question first, over the catalog's REST API (iceberg_metadata.py), no Spark and no claim,
with the planner's own input lists (gold_rebuild.producer_inputs) and acceptances. An input not loaded
yet is not ready, not broken: the cut-over records `waiting` and ends (by-pnu-lane-cutover.sh, the
`inputs` step).

A producer that imports Spark at module level (the panel producers) names its inputs only under Spark;
for it the answer is `ready`, saying so, and the planner checks them in the gold step as before.
Exits 65 when it cannot tell (no catalog, a table it cannot read): the cut-over then stops. Waiting is
75 (EX_TEMPFAIL), not 1: an uncaught error exits 1 too, and must fail the step, not wait it out.
"""

from __future__ import annotations

import importlib
import sys
import urllib.error
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
for relative in ("infra/lakehouse/spark/jobs", "scripts/ops"):
    sys.path.insert(0, str(PLATFORM / relative))


def required_inputs(gold_table: str) -> tuple[str, ...] | None:
    """The inputs of `gold_table` whose absence the planner refuses: the producer's required inputs and
    its optional ones the contract does not accept absent; None when the producer cannot be imported
    without Spark."""
    import gold_rebuild  # noqa: PLC0415

    entry = gold_rebuild.load_contract()["tables"].get(gold_table)
    if entry is None:
        raise ValueError(f"{gold_table} is not a table of gold-panel-rebuild.contract.json")
    try:
        producer = importlib.import_module(entry["producer"])
    except ModuleNotFoundError as error:
        if error.name != "pyspark" and not (error.name or "").startswith("pyspark."):
            raise
        return None
    required, optional, _ = gold_rebuild.producer_inputs(producer, entry)
    accepted = entry.get("absent_inputs_accepted", {})
    return (*required, *(name for name in optional if name not in accepted))


def verdict(gold_table: str, required: tuple[str, ...] | None, exists) -> tuple[bool, str]:
    """`exists(name)`: True, False, or raises when the catalog cannot tell."""
    if required is None:
        return True, f"ready {gold_table}: its producer names its inputs only under Spark; the gold step's planner checks them"
    missing = [name for name in required if not exists(name)]
    if missing:
        return False, f"waiting {gold_table}: {len(missing)} of the {len(required)} inputs its planner requires not loaded yet: {', '.join(missing)}"
    return True, f"ready {gold_table}: the {len(required)} inputs its planner requires exist"


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 64
    import iceberg_metadata  # noqa: PLC0415

    try:
        required = required_inputs(argv[0])
    except ValueError as error:
        print(error, file=sys.stderr)
        return 64
    if required is None:
        print(verdict(argv[0], None, None)[1])
        return 0
    try:
        catalog = iceberg_metadata.Catalog("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN",
                                           "foundation-gold-input-readiness")

        def exists(name: str) -> bool:
            try:
                catalog.table(name)
            except urllib.error.HTTPError as error:
                if iceberg_metadata.table_absent(error):
                    return False
                raise
            return True

        ready, line = verdict(argv[0], required, exists)
    except Exception as error:  # noqa: BLE001 - any failure to read means "cannot tell"
        print(f"cannot tell: {argv[0]}'s inputs unreadable: {error}", file=sys.stderr)
        return 65
    print(line)
    return 0 if ready else 75


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
