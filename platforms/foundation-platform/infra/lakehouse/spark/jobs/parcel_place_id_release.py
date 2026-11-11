"""The parcel unit's place-id release, region by region (root ADR-0481; ADR-0113 §8, ADR-0288 §5).

`silver.parcel_registry` holds every parcel id's (PNU, period, status) rows, appended. This publishes
what consumers read about them to `gold.place_id_registry`, `gold.place_id_bridge` and
`gold.place_id_changelog` under `unit = parcel`, one region (`scope`) at a time:

- **The region of a parcel id** is the 시도 prefix of the first PNU it was registered with. It never
  changes, so a parcel whose code moved 시도 (29 → 12 on 2026-07-01) stays in the region that issued
  its id, and a region's release reads only its own ids.
- **Registry**: the newest registry row of each id (status, its PNU when current, its redirect).
- **Bridge**: one period per (id, PNU): from the first current row of the pair to the last historic
  one (open while the pair's newest row is current). A PNU whose legal-dong code a recorded pair
  supersedes is also bridged under its current code (valid_from = valid_to = the pair source), so a
  reference made with the new code (`1211010100…`) finds the id the served edition keeps under the
  old one (`4611010100…`) until the registry itself carries the new PNU (root ADR-0288 §3, ADR-0482).
  A new code two ids would take is bridged to neither.
- **Changelog**: what this release changed against the region's published registry.
- **What runs**: only the regions whose inputs moved since their last release (`stale_scopes`):
  never released, registry rows recorded after it, or a 법정동 code change under the region's open
  PNUs since (`views.changed_codes` between the change table as of the release and now, root
  ADR-0329 §3). A region that did not move is not read, and a region that moved is written, so its
  release time moves and it is not read again for the same change. The first release of every
  region is the declared full exception: it has nothing to compare against.

Spark throughout: a region holds up to ~6M ids, too many for the driver.
"""

from __future__ import annotations

from typing import Any, Mapping

UNIT = "parcel"
PAIR_SOURCE = "legal-dong-code-change"  # the admin release's word for a pair-recovered bridge row
# The registry is read as of the served edition by its canonical reader (root ADR-0490 §4a); the
# release job hands these functions its rows.
from parcel_registry_frames import REGISTRY_TABLE  # noqa: E402,F401


def with_scope(rows: Any) -> Any:
    """`rows` of the registry with `scope`: the 시도 prefix of each id's first registered PNU."""

    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    first = Window.partitionBy("parcel_id").orderBy(F.col("valid_from").asc_nulls_last(),
                                                    F.col("recorded_at_utc").asc(), F.col("pnu").asc())
    return rows.withColumn("scope", F.substring(F.first("pnu").over(first.rowsBetween(Window.unboundedPreceding,
                                                                                         Window.unboundedFollowing)), 1, 2))


def registry_of(rows: Any) -> Any:
    """One row per id: `scope, place_id, status, current_code, redirect_to`, from its newest row."""

    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    # A renumbering records the old PNU's historic row and the new PNU's current row at one instant:
    # the current row is the id's state.
    newest = Window.partitionBy("parcel_id").orderBy(F.col("recorded_at_utc").desc(),
                                                     F.when(F.col("status") == "current", 0).otherwise(1),
                                                     F.coalesce("valid_to", "valid_from").desc_nulls_last())
    return (rows.withColumn("_n", F.row_number().over(newest)).where(F.col("_n") == 1)
            .select("scope", F.col("parcel_id").alias("place_id"), "status",
                    F.when(F.col("status") == "current", F.col("pnu")).alias("current_code"),
                    F.when(F.col("status") == "redirected", F.col("redirect_to")).alias("redirect_to")))


def bridge_of(rows: Any, remap: Mapping[str, str]) -> Any:
    """`scope, place_id, code, valid_from, valid_to`: each (id, PNU) period, and each open PNU under
    a superseded code again under its current code, unless a period already holds that code."""

    from pyspark.sql import Window, functions as F  # noqa: PLC0415

    newest = Window.partitionBy("parcel_id", "pnu").orderBy(F.col("recorded_at_utc").desc())
    periods = (rows.withColumn("_newest", F.first("status").over(newest.rowsBetween(Window.unboundedPreceding,
                                                                                     Window.unboundedFollowing)))
               .groupBy("scope", "parcel_id", "pnu")
               .agg(F.min(F.when(F.col("status") == "current", F.col("valid_from"))).alias("_opened"),
                    F.max(F.when(F.col("status") != "current", F.coalesce("valid_to", "valid_from"))).alias("_closed"),
                    F.first("_newest").alias("_newest"))
               .where(F.col("_opened").isNotNull())
               .select("scope", F.col("parcel_id").alias("place_id"), F.col("pnu").alias("code"),
                       F.col("_opened").alias("valid_from"),
                       F.when(F.col("_newest") != "current", F.col("_closed")).alias("valid_to")))
    if not remap:
        return periods
    spark = rows.sparkSession
    pairs = F.broadcast(spark.createDataFrame(sorted(remap.items()), "_old string, _current string"))
    carried = (periods.where(F.col("valid_to").isNull())
               .join(pairs, F.substring("code", 1, 10) == F.col("_old"))
               .select("scope", "place_id", F.concat(F.col("_current"), F.substring("code", 11, 9)).alias("code"),
                       F.lit(PAIR_SOURCE).alias("valid_from"), F.lit(PAIR_SOURCE).alias("valid_to"))
               .join(periods.select("code").distinct(), "code", "left_anti"))
    # A code two ids would take (a merge whose two old dongs both had the lot) bridges neither: a code
    # maps to one place_id at any time (the table's contract), and which one is a steward's decision.
    alone = carried.groupBy("code").agg(F.countDistinct("place_id").alias("_ids")).where(F.col("_ids") == 1)
    return periods.unionByName(carried.join(alone.select("code"), "code", "left_semi"))


def changelog_of(published: Any, registry: Any) -> Any:
    """`place_id, change, from_code, to_code, redirect_to` between the region's published registry and
    this one: added, retired, renumbered (same id, new PNU), redirected."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    old = published.select("place_id", F.col("status").alias("_old_status"), F.col("current_code").alias("_old_code"))
    new = registry.select("place_id", "status", "current_code", "redirect_to")
    both = old.join(new, "place_id", "full_outer")
    was = F.col("_old_status") == "current"
    now = F.col("status") == "current"
    change = (F.when((F.col("status") == "redirected") & (F.coalesce(F.col("_old_status"), F.lit("")) != "redirected"),
                     F.lit("redirected"))
              .when(now & ~F.coalesce(was, F.lit(False)), F.lit("added"))
              .when(was & ~F.coalesce(now, F.lit(False)), F.lit("retired"))
              .when(was & now & (F.col("_old_code") != F.col("current_code")), F.lit("renumbered")))
    return (both.withColumn("change", change).where(F.col("change").isNotNull())
            .select("place_id", "change", F.col("_old_code").alias("from_code"),
                    F.col("current_code").alias("to_code"),
                    F.when(F.col("change") == "redirected", F.col("redirect_to")).alias("redirect_to")))


def code_moved_scopes(registry_rows: Any, prefixes: set[str] | None) -> set[str]:
    """The regions holding an open PNU under a code ``prefixes`` names (`views.changed_codes` between
    the change table at the region's last release and now, root ADR-0329 §3); every region when
    ``prefixes`` is None (that read of the change table expired: the region is read again)."""

    from functools import reduce  # noqa: PLC0415

    from pyspark.sql import functions as F  # noqa: PLC0415

    scopes = registry_rows.select("scope").distinct()
    if prefixes is None:
        return {row["scope"] for row in scopes.collect()}
    if not prefixes:
        return set()
    under = reduce(lambda a, b: a | b, [F.substring("pnu", 1, len(p)) == F.lit(p) for p in sorted(prefixes)])
    return {row["scope"] for row in registry_rows.where((F.col("status") == "current") & under)
            .select("scope").distinct().collect()}


def stale_scopes(registry_rows: Any, published: Any, code_moved: set[str]) -> list[tuple[str, int]]:
    """[(scope, ids)] of the regions whose inputs moved since their last release, smallest first:
    never released, a registry row recorded after it, or a code change under its PNUs since
    (``code_moved``, `code_moved_scopes`).

    ``registry_rows``: the registry with `scope` (`with_scope`) and `visible_at_utc`. ``published``: the parcel unit's
    published registry rows (`scope`, `published_at_utc`)."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    # A row counts from when it reached the served read (`visible_at_utc`): a candidate's step recorded
    # before its promotion moves its regions at the promotion (root ADR-0490 §4a).
    inputs = registry_rows.groupBy("scope").agg(F.countDistinct("parcel_id").alias("ids"),
                                               F.max("visible_at_utc").alias("_recorded"))
    last = published.groupBy("scope").agg(F.max("published_at_utc").alias("_published"))
    moved = (F.col("_published").isNull() | (F.col("_recorded") > F.col("_published"))
             | F.col("scope").isin(sorted(code_moved) or [""]))
    rows = inputs.join(last, "scope", "left").where(moved).select("scope", "ids").collect()
    return sorted(((row["scope"], int(row["ids"])) for row in rows), key=lambda r: (r[1], r[0]))
