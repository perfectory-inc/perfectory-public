"""Parcel building energy by-PNU kind (root ADR-0396, ADR-0398).

Synthetic PNUs (prefix 99999). Pins: a parcel's document holds its newest twelve usage months, newest
first, each energy summed over the month's rows with their count; a month without an energy's row
leaves that energy null, not 0; a 사용량 that is not a number is left out and counted; only served
parcels have a document; the written JSON's paths are exactly the registry's (root ADR-0336); a run
recomputes the PNUs of appended rows of either energy and of parcels that entered or left the served
edition, and nothing else.
"""

import json
import os
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import building_energy_silver_to_gold as job  # noqa: E402

DONG = "9999910100"


def pnu(main):
    return f"{DONG}1{main:04d}0000"


FACTORY, OFFICE, UNSERVED = pnu(1), pnu(2), pnu(3)
LANE = "parcel-energy"
GOLD_COLUMNS = ("pnu", "energy_json", "attached_via", "row_digest", "source_snapshot_id", "published_at_utc")


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_both_energies_and_no_lineage(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.ELECTRICITY_SOURCE, job.GAS_SOURCE,
                                         job.CODE_CHANGE_SOURCE})
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, (job.CODE_CHANGE_SOURCE,)))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns), (job.JOB_NAME, job.SECTION, ("energy_json",)))
        self.assertFalse(kind.follows_host_panel)

    def test_the_parcel_universe_is_the_served_edition_and_codes_follow_reorganizations(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)
        # Root ADR-0329 §3: a row under a renumbered code is read under the served code.
        self.assertIn("panel.renumbered(panel.read_source(spark, args, source, at), renumbering)", src)

    def test_the_affected_pnus_come_only_from_the_inputs_that_changed(self):
        calls = []

        class Frame:
            def __init__(self, name):
                self.name = name

            def select(self, *_):
                return self

            def unionByName(self, other):
                return Frame(f"{self.name}+{other.name}")

            def distinct(self):
                return self

        def run(changed, appended):
            calls.clear()
            frame = job.affected_pnus(
                changed, appended,
                appended_rows=lambda source: calls.append(("appended", source)) or Frame(f"a:{job.ENERGY_SOURCES[source]}"),
                changed_rows=lambda source: calls.append(("changed", source)) or Frame(f"c:{job.ENERGY_SOURCES[source]}"),
                parcel_change=lambda: calls.append(("parcels",)) or Frame("parcels"))
            return frame.name

        # A month's release appends only its changed 지번: read those, never diff the table.
        self.assertEqual(run([job.ELECTRICITY_SOURCE], [job.ELECTRICITY_SOURCE]), "a:electricity")
        self.assertEqual(calls, [("appended", job.ELECTRICITY_SOURCE)])
        self.assertEqual(run([job.GAS_SOURCE, job.ELECTRICITY_SOURCE], [job.GAS_SOURCE]), "c:electricity+a:gas")
        self.assertEqual(run([job.PARCEL_SOURCE, job.GAS_SOURCE], [job.GAS_SOURCE]), "a:gas+parcels")
        with self.assertRaises(ValueError):
            run([job.CODE_CHANGE_SOURCE], [])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("building-energy-gold").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self, electricity, gas):
        schema = "pnu string, use_ym string, usage_amount string"
        parcels = self.spark.createDataFrame([(FACTORY,), (OFFICE,)], "pnu string")
        return {job.ELECTRICITY_SOURCE: self.spark.createDataFrame(electricity, schema),
                job.GAS_SOURCE: self.spark.createDataFrame(gas, schema), job.PARCEL_SOURCE: parcels}

    def documents(self, electricity, gas):
        gold = job.gold_rows(self.frames(electricity, gas), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: json.loads(r.energy_json, parse_float=Decimal) for r in gold.collect()}, gold

    def test_months_are_summed_per_energy_newest_first_and_unserved_parcels_left_out(self):
        electricity = [(FACTORY, "209907", "100"), (FACTORY, "209907", "250.5"), (FACTORY, "209908", "90"),
                       (UNSERVED, "209908", "1")]
        gas = [(FACTORY, "209908", "12"), (OFFICE, "209906", "3")]
        documents, gold = self.documents(electricity, gas)
        self.assertEqual(set(documents), {FACTORY, OFFICE})
        factory = documents[FACTORY]
        self.assertEqual([m["use_ym"] for m in factory["months"]], ["209908", "209907"])
        august, july = factory["months"]
        self.assertEqual((august["electricity_usage"], august["electricity_rows"]), (Decimal("90.000"), 1))
        self.assertEqual((august["gas_usage"], august["gas_rows"]), (Decimal("12.000"), 1))
        self.assertEqual((july["electricity_usage"], july["electricity_rows"]), (Decimal("350.500"), 2))
        # No 가스 row in July: null, not 0.
        self.assertEqual((july["gas_usage"], july["gas_rows"]), (None, 0))
        self.assertEqual((factory["latest_use_ym"], factory["electricity_unit"]), ("209908", "kWh"))
        office = documents[OFFICE]
        self.assertEqual([(m["use_ym"], m["electricity_usage"], m["gas_usage"]) for m in office["months"]],
                         [("209906", None, Decimal("3.000"))])
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_only_the_newest_twelve_months_are_kept(self):
        electricity = [(FACTORY, f"2098{m:02d}", "1") for m in range(1, 13)] + [(FACTORY, "209901", "1")]
        documents, _ = self.documents(electricity, [])
        months = [m["use_ym"] for m in documents[FACTORY]["months"]]
        self.assertEqual(len(months), job.MONTH_BOUND)
        self.assertEqual((months[0], months[-1]), ("209901", "209802"))

    def test_a_usage_that_is_not_a_number_is_left_out_and_counted(self):
        documents, _ = self.documents([(FACTORY, "209908", "12"), (FACTORY, "209908", "-")], [])
        month = documents[FACTORY]["months"][0]
        self.assertEqual((month["electricity_usage"], month["electricity_rows"]), (Decimal("12.000"), 2))
        self.assertEqual(documents[FACTORY]["unparsed_usage_rows"], 1)

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        documents, _ = self.documents([(FACTORY, "209908", "1")], [(OFFICE, "209908", "2")])
        written = set()
        for document in documents.values():
            written |= by_pnu_kinds.json_paths(document, "energy")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_energy_by_pnu") if p.startswith("energy.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        rows = [(FACTORY, "209908", "1")]
        first = job.gold_rows(self.frames(rows, []), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(rows, []), "snap-2", "2099-09-01T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
