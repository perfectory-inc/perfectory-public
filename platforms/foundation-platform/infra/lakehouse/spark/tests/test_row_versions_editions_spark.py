"""`current_rows` reads an `Editions` table's newest edition, and `pinned_edition` one named edition
(root ADR-0286 §3, ADR-0376), on a Spark runtime.

The edition order is each edition's maxima of `newest_first`, never the edition id: on 2026-10-20 the
complex boundary editions were named `vworldkr__sandan_boundary-30137-1` and the floor generations
by a content hash, neither in time order. Skipped where pyspark is missing; the CI spark-runtime lane
runs it.
"""
import copy
import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def contract(newest_first=("loaded_at",), per=(), read="newest", predicate=None):
    columns = ["region", "edition", "loaded_at", "key", "closed"]
    return {
        "table_name": "silver.example_editions",
        "current_row_predicate": predicate,
        "current_row_latest": None,
        "row_versions": {"kind": "editions", "edition_column": "edition", "newest_first": list(newest_first),
                         "per": list(per), "read": read},
        "columns": [{"name": name, "logical_type": "string", "required": False} for name in columns],
    }


EARLY, LATE = datetime(2026, 9, 1), datetime(2026, 10, 1)


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class EditionsReadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[1]").appName("row-versions-editions")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        # SQL VALUES, not createDataFrame: no Python worker, so it runs where pyspark's worker cannot.
        schema = "region string, edition string, loaded_at timestamp, key string, closed string"
        if not rows:
            return self.spark.createDataFrame([], schema)
        literal = lambda v: "NULL" if v is None else f"TIMESTAMP '{v:%Y-%m-%d %H:%M:%S}'" if isinstance(v, datetime) else f"'{v}'"  # noqa: E731
        values = ", ".join("(" + ", ".join(literal(v) for v in row) + ")" for row in rows)
        return self.spark.sql(f"SELECT * FROM VALUES {values} AS t(region, edition, loaded_at, key, closed)")

    @staticmethod
    def keys(frame):
        return sorted(row.key for row in frame.collect())

    def test_the_newest_edition_is_the_latest_by_its_order_not_by_its_id(self):
        from platform_contracts import current_rows

        # "a-…" sorts after "9-…" as text, but was loaded first.
        rows = [("11", "a-30137-1", EARLY, "x", None), ("11", "a-30137-1", EARLY, "y", None),
                ("11", "9-40000-2", LATE, "x", None)]
        self.assertEqual(self.keys(current_rows(self.frame(rows), contract())), ["x"])

    def test_each_group_takes_its_own_newest_edition(self):
        from platform_contracts import current_rows

        rows = [("11", "e1", EARLY, "seoul-old", None), ("11", "e2", LATE, "seoul-new", None),
                ("26", "e3", EARLY, "busan-only", None)]
        self.assertEqual(self.keys(current_rows(self.frame(rows), contract(per=("region",)))),
                         ["busan-only", "seoul-new"])
        # Without `per` the one table is one group: Busan's only edition is older than Seoul's newest.
        self.assertEqual(self.keys(current_rows(self.frame(rows), contract())), ["seoul-new"])

    def test_two_editions_tied_on_the_order_are_refused(self):
        from platform_contracts import current_rows

        rows = [("11", "e1", LATE, "x", None), ("11", "e2", LATE, "y", None)]
        with self.assertRaisesRegex(ValueError, "tied on"):
            current_rows(self.frame(rows), contract()).collect()

    def test_the_edition_is_chosen_before_the_predicate(self):
        from platform_contracts import current_rows

        # Every row of the newest edition is closed: there is no current row, not the older edition's.
        rows = [("11", "e1", EARLY, "x", None), ("11", "e2", LATE, "x", "yes")]
        self.assertEqual(self.keys(current_rows(self.frame(rows), contract(predicate="closed IS NULL"))), [])

    def test_a_pinned_table_reads_the_edition_it_is_given(self):
        from platform_contracts import current_rows, pinned_edition

        rows = [("11", "e1", EARLY, "x", None), ("11", "e1", EARLY, "gone", "yes"), ("11", "e2", LATE, "y", None)]
        pinned = contract(read="pinned", predicate="closed IS NULL")
        self.assertEqual(self.keys(pinned_edition(self.frame(rows), pinned, "e1")), ["x"])
        # With a predicate every edition is current by its own (root ADR-0148); without one, no default.
        self.assertEqual(self.keys(current_rows(self.frame(rows), pinned)), ["x", "y"])
        with self.assertRaisesRegex(ValueError, "use pinned_edition"):
            current_rows(self.frame(rows), contract(read="pinned"))

    def test_only_an_editions_table_has_editions(self):
        from platform_contracts import all_editions, pinned_edition

        replaced = copy.deepcopy(contract())
        replaced["row_versions"] = {"kind": "replaced", "key": ["key"]}
        for read in (lambda: pinned_edition(self.frame([]), replaced, "e1"),
                     lambda: all_editions(self.frame([]), replaced)):
            with self.assertRaisesRegex(ValueError, "does not append editions"):
                read()
        self.assertEqual(all_editions(self.frame([("11", "e1", EARLY, "x", None)]), contract()).count(), 1)


def old_newest(pairs):
    """The private rule each moved reader had: the edition with the latest order value, a tie to the
    greater id (`newest_source_snapshot`, `newest_floor_batch`, `ORDER BY max(...) DESC, id DESC`)."""
    return max(pairs, key=lambda pair: (pair[1], pair[0]))[0] if pairs else None


# Editions named so that id order and load order disagree, loaded at distinct instants (no tie: the
# old rule broke one by id, the contract refuses it, and 2026-11-04 production held none).
EDITIONS = [("vworldkr__sandan_boundary-30137-1", datetime(2026, 8, 21)),
            ("vworldkr__sandan_boundary-29001-9", datetime(2026, 10, 2)),
            ("building-register-floor-selection-v2-" + "b" * 8, datetime(2026, 9, 29)),
            ("building-register-floor-content-v1-" + "a" * 8, datetime(2026, 7, 1))]


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class MovedReadersPickTheOldRuleRowsTest(unittest.TestCase):
    """Root ADR-0286 stage 4: every reader moved onto `current_rows` picks the rows its private rule did."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[1]").appName("row-versions-moved-readers")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def editions(self, columns, rows):
        literal = lambda v: "NULL" if v is None else f"TIMESTAMP '{v:%Y-%m-%d %H:%M:%S}'" if isinstance(v, datetime) else f"'{v}'"  # noqa: E731
        values = ", ".join("(" + ", ".join(literal(v) for v in row) + ")" for row in rows)
        return self.spark.sql(f"SELECT * FROM VALUES {values} AS t({', '.join(columns)})")

    def test_every_subset_of_editions_gives_the_old_rule_s_edition(self):
        from itertools import combinations

        from platform_contracts import current_rows, load_lakehouse_contract

        # (table, the column ordering its editions): the complex boundaries' handoff and served Gold,
        # the administrative boundaries' served Gold and handoff, the building panel's floors.
        for table, order in (("silver.industrial_complex_boundaries", "valid_from_utc"),
                             ("silver.administrative_boundaries", "ingested_at_utc"),
                             ("silver.building_register_floors", "ingested_at_utc")):
            contract = load_lakehouse_contract(table)
            for size in range(1, len(EDITIONS) + 1):
                for held in combinations(EDITIONS, size):
                    with self.subTest(table=table, held=[edition for edition, _ in held]):
                        # Two rows per edition, loaded a minute apart: the edition's maximum orders it.
                        rows = [(edition, at.replace(minute=minute), f"{edition}#{minute}")
                                for edition, at in held for minute in (0, 1)]
                        frame = self.editions(["source_snapshot_id", order, "row_id"], rows)
                        chosen = {row.source_snapshot_id for row in current_rows(frame, contract).collect()}
                        self.assertEqual(chosen, {old_newest([(e, at.replace(minute=1)) for e, at in held])})

    def test_the_complex_boundary_export_reads_the_newest_editions_official_open_rows(self):
        from pyspark.sql import functions as F

        import industrial_complex_boundaries_silver_to_postgis_handoff as handoff

        columns = ["boundary_id", "complex_id", "boundary_kind", "geometry_srid", "geometry_wkb",
                   "area_sqm_calculated", "geometry_checksum_sha256", "source_record_id", "source_snapshot_id",
                   "valid_from_utc", "valid_to_utc"]
        old, new = EDITIONS[0][0], EDITIONS[1][0]
        rows = [("b-old", "c1", handoff.OFFICIAL_BOUNDARY_KIND, "5179", "00", "1", "h", "r", old, EDITIONS[0][1], None),
                ("b-new", "c1", handoff.OFFICIAL_BOUNDARY_KIND, "5179", "00", "1", "h", "r", new, EDITIONS[1][1], None),
                ("b-closed", "c2", handoff.OFFICIAL_BOUNDARY_KIND, "5179", "00", "1", "h", "r", new, EDITIONS[1][1],
                 EDITIONS[1][1]),
                ("b-other", "c3", "derived", "5179", "00", "1", "h", "r", new, EDITIONS[1][1], None)]
        selected = handoff.current_official_boundaries(self.editions(columns, rows), F)
        self.assertEqual([row.boundary_id for row in selected.collect()], ["b-new"])


if __name__ == "__main__":
    unittest.main()
