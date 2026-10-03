"""Refuse a Silver load whose ordinary-land PNU NULL share jumps above the table's current one.

The hub building registers carry a register parcel key whose 11th character is the 대지구분
code; `0` is ordinary land, which always has a standard PNU. A NULL `pnu` there means the
derivation lost the parcel, not that the source lacked one.

The 2026-09-27 title snapshot is what this check exists for. A changed 시군구 resolver withheld
the mapping for every merged-시도 code, so all 916,461 ordinary-land rows of 시도 12 lost their PNU.
Every row-level gate passed, because `pnu` is nullable for block parcels, and the load replaced
a table whose ordinary-land NULL share was one in a million with one at 11.6 percent. A share
that rises is the signal no row-level gate can see.

A rise alone is not enough. The table's current snapshot can itself be the bad one: the 09-27
snapshot is still current, so a reload that loses the same 시도 again rises by about zero and
would pass a rise-only check, ratcheting the damage in. The share therefore also has an absolute
ceiling, and a load is refused when either bound is exceeded.

Both bounds are declared in the table contract as one quality gate,
`ordinary_land_pnu_null_share <= <ceiling> and increase <= <rise>` (see `platform_contracts`),
so the numbers live where the table is defined and this module only reads them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lakehouse_ingest import unquoted_table_name
from platform_contracts import OrdinaryLandPnuNullShareBounds

PARCEL_KEY_COLUMN = "register_parcel_key"
PNU_COLUMN = "pnu"
# 1-based position of the 대지구분 code in the hub register parcel key
# (시군구 5 + 법정동 5 + 대지구분 1 + 본번 4 + 부번 4).
DAEJI_KIND_POSITION = 11
ORDINARY_LAND_CODE = "0"


@dataclass(frozen=True)
class OrdinaryLandPnuNulls:
    rows: int
    nulls: int

    @property
    def share(self) -> float:
        return self.nulls / self.rows if self.rows else 0.0


def ordinary_land(F: Any) -> Any:
    return F.substring(F.col(PARCEL_KEY_COLUMN), DAEJI_KIND_POSITION, 1) == ORDINARY_LAND_CODE


def measure(frame: Any, F: Any) -> OrdinaryLandPnuNulls:
    scoped = ordinary_land(F)
    row = frame.agg(
        F.sum(F.when(scoped, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("rows"),
        F.sum(F.when(scoped & F.col(PNU_COLUMN).isNull(), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("nulls"),
    ).first()
    if row is None:
        raise ValueError("ordinary-land PNU NULL aggregation returned no row")
    return OrdinaryLandPnuNulls(rows=int(row["rows"] or 0), nulls=int(row["nulls"] or 0))


def decide(
    candidate: OrdinaryLandPnuNulls,
    baseline: OrdinaryLandPnuNulls | None,
    bounds: OrdinaryLandPnuNullShareBounds,
    table: str,
) -> dict[str, Any]:
    """Returns the named outcome, or raises when the candidate breaks either bound.

    The ceiling is judged on the candidate alone, so it holds even with no baseline. No baseline
    (a new table, or one with no ordinary-land rows) is an outcome of its own, `no_baseline`,
    rather than a silent pass, so a summary reader can tell the two apart.
    """

    outcome: dict[str, Any] = {
        "table": table,
        "ceiling": bounds.ceiling,
        "tolerance": bounds.increase,
        "candidate_rows": candidate.rows,
        "candidate_nulls": candidate.nulls,
        "candidate_share": candidate.share,
    }
    if candidate.share > bounds.ceiling:
        raise ValueError(
            "Refusing the Silver load: the ordinary-land (대지구분 0) PNU NULL share of the "
            f"candidate for {table} is {candidate.share:.6f} ({candidate.nulls}/{candidate.rows}), "
            f"above the contract ceiling {bounds.ceiling}. A derivation lost parcels; find which "
            "시군구 before loading, whatever the table holds now."
        )
    if baseline is None or baseline.rows == 0:
        return {**outcome, "outcome": "no_baseline"}
    outcome.update(
        baseline_rows=baseline.rows,
        baseline_nulls=baseline.nulls,
        baseline_share=baseline.share,
        increase=candidate.share - baseline.share,
    )
    if candidate.share - baseline.share > bounds.increase:
        raise ValueError(
            "Refusing the Silver load: the ordinary-land (대지구분 0) PNU NULL share rises from "
            f"{baseline.share:.6f} ({baseline.nulls}/{baseline.rows}) in {table} to "
            f"{candidate.share:.6f} ({candidate.nulls}/{candidate.rows}), more than the contract "
            f"tolerance {bounds.increase}. A derivation lost parcels; find which 시군구 before loading."
        )
    return {**outcome, "outcome": "within_bounds"}


def table_baseline(spark: Any, table: str, F: Any) -> OrdinaryLandPnuNulls | None:
    """Measures the table as readers see it now (its current snapshot), or None when absent."""

    if not spark.catalog.tableExists(unquoted_table_name(table)):
        return None
    return measure(spark.table(table), F)
