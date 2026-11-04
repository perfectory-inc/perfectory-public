"""Every Silver lane's first load into an empty table runs here, on real Iceberg (root ADR-0389).

2026-10-30 silver.land_ownership and silver.land_use_zone_area were new tables. Their first load
was the first time their writer met a table with no snapshot: the first batch went straight to main,
the compare refused the release ("main records 4 of the release's 17 objects; a release is one
commit"), and both lanes failed after an hour of CPU (#986). The sibling lanes had never taken that
path because their tables were made by hand. A lane's first load is a path of its own, so it is
exercised for every lane, before the lane's first production run.

The lanes are not listed here. `FirstLoadCaseTableTest` (no Spark) reads them from the two places
that declare a Silver table — the lakehouse contracts and the pipeline graph — and gives each one the
case its writer needs:

| case | lanes | first load must |
|---|---|---|
| staged | every `silver_refresh` lane (`*-source-objects.json`) | stage every batch on the load branch; main holds none of it until one publish |
| object | a job that writes through `append_source_object` | commit each object once, recording it, even with no rows |
| batch | a job that writes through `append_batch_once` on main | commit the batch once; the same batch again writes nothing |
| own | `OWN_WRITERS`: a writer of its own, with the test that loads its empty table | (that test) |
| none | no job writes the table yet | nothing to load |

A table that fits no case fails the case table, so a lane cannot land without its first load here.
Rows are synthetic (PNUs start 99999).
"""
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

AREA = SPARK_DIR.parents[2]
REPOSITORY = AREA.parents[1]
CONTRACTS = SPARK_DIR.parent / "contracts"
# Quoted by name so a change to either declaration selects this file (root ADR-0365 §1).
LAKEHOUSE_CONTRACTS = CONTRACTS / "industrial_complex_lakehouse_contracts.json"
PIPELINE_GRAPH = AREA / "docs" / "catalog" / "pipeline-graph.v1.json"
JOBS = "platforms/foundation-platform/infra/lakehouse/spark/jobs/"

# The call that makes a job's write one of the shared cases; the first that matches wins.
SHARED_WRITERS = (("object", "append_source_object("), ("batch", "append_batch_once("),
                  ("batch", "append_and_read_back("))
# A table whose job writes it its own way: the test that loads its empty table, as module.Class.method.
OWN_WRITERS = {
    "silver.administrative_boundaries": "test_silver_first_load_iceberg.AdministrativeBoundariesFirstLoadTest."
                                        "test_the_first_snapshot_into_an_empty_table_is_one_commit",
    "silver.map_edit_ledger": "test_silver_first_load_iceberg.MapEditLedgerFirstLoadTest."
                              "test_the_first_edits_into_an_empty_ledger_are_appended_once",
    "silver.building_register_apartment_price_versions": "test_vintage_versions_iceberg.VintageVersionsIcebergTest."
                                                         "test_both_tables_migrate_with_parity_and_a_second_run_appends_nothing",
    "silver.unit_official_price_versions": "test_vintage_versions_iceberg.VintageVersionsIcebergTest."
                                           "test_both_tables_migrate_with_parity_and_a_second_run_appends_nothing",
}


def silver_refresh_lanes(contracts_dir=CONTRACTS):
    lanes = {}
    for path in sorted(contracts_dir.glob("*-source-objects.json")):
        block = json.loads(path.read_text(encoding="utf-8")).get("silver_refresh") or {}
        if block.get("table"):
            lanes[block["table"]] = block
    return lanes


def first_load_cases(contracts, graph, refresh_lanes, read_job, own=OWN_WRITERS):
    """{table: case} for every Silver table, or ValueError naming the tables that fit no case."""
    declared = {name for name in contracts if name.startswith("silver.")}
    nodes = {node["table_name"]: node["id"] for node in graph["nodes"] if node["type"] == "silver_table"}
    problems = [f"{name}: a contract and no pipeline-graph node" for name in sorted(declared - set(nodes))]
    problems += [f"{name}: a pipeline-graph node and no contract" for name in sorted(set(nodes) - declared)]
    problems += [f"{name}: OWN_WRITERS names a table no contract declares" for name in sorted(set(own) - declared)]
    cases = {}
    for table in sorted(declared & set(nodes)):
        jobs = sorted({via for edge in graph["edges"] if edge["to"] == nodes[table]
                       for via in edge.get("via", []) if via.startswith(JOBS) and via.endswith(".py")})
        sources = [read_job(job) for job in jobs]
        if table in refresh_lanes:
            cases[table] = "staged"
        elif table in own:
            cases[table] = "own"
        elif shared := next((case for case, call in SHARED_WRITERS if any(call in s for s in sources)), None):
            cases[table] = shared
        elif not any(edge["to"] == nodes[table] and edge.get("via") for edge in graph["edges"]):
            cases[table] = "none"
        else:
            problems.append(f"{table}: written by {jobs or 'a non-Spark writer'} with no first-load case; "
                            "write it through a shared writer or name its first-load test in OWN_WRITERS")
    if problems:
        raise ValueError("Silver lanes without a first-load case (root ADR-0389):\n" + "\n".join(problems))
    return cases


def the_cases():
    return first_load_cases(json.loads(LAKEHOUSE_CONTRACTS.read_text(encoding="utf-8"))["contracts"],
                            json.loads(PIPELINE_GRAPH.read_text(encoding="utf-8")), silver_refresh_lanes(),
                            lambda job: (REPOSITORY / job).read_text(encoding="utf-8"))


class FirstLoadCaseTableTest(unittest.TestCase):
    """No Spark: every Silver table has a first-load case, and each named test exists."""

    def test_every_silver_lane_has_a_first_load_case(self):
        cases = the_cases()
        self.assertGreaterEqual(len(cases), 42)
        self.assertEqual({t for t, c in cases.items() if c == "staged"}, set(silver_refresh_lanes()))
        self.assertIn("silver.land_ownership", {t for t, c in cases.items() if c == "staged"})

    def test_each_own_writer_names_a_runtime_test_that_exists(self):
        import ast

        for table, target in OWN_WRITERS.items():
            with self.subTest(table=table):
                module, cls, method = target.split(".")
                tree = ast.parse((Path(__file__).parent / f"{module}.py").read_text(encoding="utf-8"))
                found = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == cls]
                self.assertTrue(found, target)
                self.assertIn(method, [f.name for f in found[0].body if isinstance(f, ast.FunctionDef)], target)
                self.assertIn("RUN_ICEBERG_TESTS", ast.unparse(found[0]), f"{target} runs on real Iceberg")

    def synthetic(self, via_source, refresh=None, own=None, edges=True):
        contracts = {"silver.synthetic_lane": {}}
        graph = {"nodes": [{"id": "silver-synthetic", "type": "silver_table", "table_name": "silver.synthetic_lane"}],
                 "edges": [{"to": "silver-synthetic", "via": [JOBS + "synthetic.py"]}] if edges else []}
        return first_load_cases(contracts, graph, refresh or {}, lambda _job: via_source, own or {})

    def test_a_lane_whose_writer_fits_no_case_is_refused(self):
        with self.assertRaisesRegex(ValueError, "silver.synthetic_lane: written by"):
            self.synthetic("spark.sql('INSERT INTO silver.synthetic_lane SELECT * FROM staged')")
        self.assertEqual(self.synthetic("x = append_source_object(spark, frame)"), {"silver.synthetic_lane": "object"})
        self.assertEqual(self.synthetic("INSERT INTO", own={"silver.synthetic_lane": "t.C.m"}),
                         {"silver.synthetic_lane": "own"})
        self.assertEqual(self.synthetic("", edges=False), {"silver.synthetic_lane": "none"})

    def test_a_lane_declared_in_one_place_only_is_refused(self):
        with self.assertRaisesRegex(ValueError, "a contract and no pipeline-graph node"):
            first_load_cases({"silver.synthetic_lane": {}}, {"nodes": [], "edges": []}, {}, lambda _job: "")
        with self.assertRaisesRegex(ValueError, "a pipeline-graph node and no contract"):
            first_load_cases({}, {"nodes": [{"id": "s", "type": "silver_table", "table_name": "silver.synthetic_lane"}],
                                  "edges": []}, {}, lambda _job: "")


def value(column, kind, n):
    if column == "pnu":
        return f"99999101001{n:04d}0000"
    if kind == "string":
        return f"{column}-{n}"
    if kind in ("int", "long"):
        return n
    if kind == "double":
        return float(n)
    if kind == "boolean":
        return True
    if kind.startswith("decimal("):
        return Decimal(n)
    if kind == "array<string>":
        return [f"{column}-{n}"]
    if kind == "binary":
        return bytearray(f"{column}-{n}".encode())
    if kind == "date":
        return date(2099, 1, 1)
    if kind == "timestamp":
        return datetime(2099, 1, 1, tzinfo=timezone.utc)
    raise AssertionError(f"no synthetic {kind}")


def session(name):
    from pyspark.sql import SparkSession

    directory = tempfile.TemporaryDirectory()
    spark = (SparkSession.builder.master("local[2]").appName(name)
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.proof.type", "hadoop")
             .config("spark.sql.catalog.proof.warehouse", directory.name)
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.silver")
    return spark, directory


def tables_of(case):
    return sorted(table for table, found in the_cases().items() if found == case)


def target(table):
    name = table.split(".", 1)[1]
    return f"`proof`.`silver`.`{name}`", f"proof.silver.{name}"


def frame(spark, contract, obj, numbers):
    """Rows of one source object: its load identity in the contract's column, every column filled."""
    from platform_contracts import column_names, create_table_columns_sql

    load, latest = contract["load"], contract.get("current_row_latest") or {}
    kinds = {c["name"]: c["logical_type"] for c in contract["columns"]}
    rows = []
    for n in numbers:
        row = []
        for column in column_names(contract):
            if column == load.get("column"):
                prefix, separator = load.get("object_prefix") or "", load.get("object_suffix_separator")
                row.append(f"{prefix}{obj}{separator}{n}" if separator else f"{prefix}{obj}")
            elif column == latest.get("removal_column"):
                row.append(None)
            else:
                row.append(value(column, kinds[column], n))
        rows.append(tuple(row))
    return spark.createDataFrame(rows, " ".join(create_table_columns_sql(contract, indent=0).split()))


def main_snapshots(spark, table):
    import lakehouse_ingest as ingest

    return ingest.ref_lineage(spark, table, ingest.MAIN)


def stage_with_the_lane_writer(spark, table, contract_name, contract, batch, index, branch):
    """The lane's own write (`write_silver_iceberg`, the table created by it), as silver-refresh runs it."""
    from pyspark.sql import functions as F

    import silver_scalar_handoff_to_lakehouse as job

    lane = silver_refresh_lanes()[contract_name]
    name = contract_name.split(".", 1)[1]
    args = SimpleNamespace(contract=contract_name, iceberg_catalog_name="proof", iceberg_namespace="silver",
                           iceberg_table=name, iceberg_write_mode="append", derivation=None, iceberg_branch=branch)
    # The first batch writes as the lane declares, the rest append (land.rs batch_write_mode).
    mode = lane["spark"]["iceberg_write_mode"] if index == 0 else "append"
    return job.write_silver_iceberg(spark, batch, args, contract, F, iceberg_write_mode=mode)


def assert_first_release_is_staged(test, spark, contract_name, write=stage_with_the_lane_writer):
    """Every batch of the first release stages; main shows none of it, then all of it in one publish."""
    import lakehouse_ingest as ingest
    from platform_contracts import load_lakehouse_contract

    table, unquoted = target(contract_name)
    contract = load_lakehouse_contract(contract_name)
    branch = "load_first"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    objects = {"bronze/first-a.zip": [1, 2], "bronze/first-b.zip": [3]}
    recorded = set()
    for index, (obj, numbers) in enumerate(objects.items()):
        outcome = write(spark, table, contract_name, contract, frame(spark, contract, obj, numbers), index, branch)
        test.assertTrue(outcome["appended"], obj)
        test.assertEqual(outcome["ref"], branch, f"{obj} of a new table's first release stages on its branch")
        recorded |= set(outcome["record_ids"])
    test.assertEqual(spark.table(table).count(), 0, "main shows none of a staged release")
    test.assertEqual(set(ingest.read_ingested_objects(spark, table, unquoted)), set(), "main records none of it")
    base = main_snapshots(spark, table)
    test.assertLessEqual(len(base), 1, "main holds at most the empty commit the branch grew from")
    spark.sql(f"CALL proof.system.fast_forward('silver.{contract_name.split('.', 1)[1]}', 'main', '{branch}')")
    test.assertEqual(set(ingest.read_ingested_objects(spark, table, unquoted)), recorded)
    test.assertEqual(spark.table(table).count(), sum(len(n) for n in objects.values()))
    test.assertEqual(main_snapshots(spark, table), ingest.ref_lineage(spark, table, branch))


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class StagedFirstLoadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.directory = session("silver-first-load-staged")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_every_silver_refresh_lane_stages_its_first_release(self):
        lanes = tables_of("staged")
        self.assertIn("silver.land_ownership", lanes)
        for table in lanes:
            with self.subTest(table=table):
                assert_first_release_is_staged(self, self.spark, table)

    def test_a_writer_that_puts_a_new_tables_first_batch_on_main_fails(self):
        """The writer before #986: no main snapshot to branch from, so batch 0 went to main."""

        def straight_to_main(spark, table, contract_name, contract, batch, index, branch):
            import lakehouse_ingest as ingest
            from platform_contracts import column_names, ensure_contract_table

            ensure_contract_table(spark, table, contract)
            return ingest.append_batch_once(spark, batch, column_names(contract), table, contract_name,
                                            branch=branch if index else None)

        with self.assertRaises(AssertionError):
            assert_first_release_is_staged(self, self.spark, "silver.land_ownership", write=straight_to_main)


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class ObjectFirstLoadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.directory = session("silver-first-load-object")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_every_object_lane_commits_each_first_object_once(self):
        import lakehouse_ingest as ingest
        from platform_contracts import column_names, ensure_contract_table, load_lakehouse_contract

        lanes = tables_of("object")
        self.assertIn("silver.onbid_real_estate_item", lanes)
        for contract_name in lanes:
            with self.subTest(table=contract_name):
                table, unquoted = target(contract_name)
                contract = load_lakehouse_contract(contract_name)
                self.spark.sql(f"DROP TABLE IF EXISTS {table}")
                ensure_contract_table(self.spark, table, contract)
                columns = column_names(contract)
                with_rows = frame(self.spark, contract, "bronze/first-a.json.gz", [1, 2])
                self.assertTrue(ingest.append_source_object(self.spark, with_rows, columns, table, "bronze/first-a.json.gz", contract_table=contract_name))
                # An object with nothing new is still recorded, so it is not fetched again.
                self.assertTrue(ingest.append_source_object(self.spark, with_rows.limit(0), columns, table, "bronze/first-b.json.gz", contract_table=contract_name))
                self.assertEqual(set(ingest.read_ingested_objects(self.spark, table, unquoted)),
                                 {"bronze/first-a.json.gz", "bronze/first-b.json.gz"})
                self.assertEqual(len(main_snapshots(self.spark, table)), 2, "one commit per object")
                self.assertFalse(ingest.append_source_object(self.spark, with_rows, columns, table, "bronze/first-a.json.gz", contract_table=contract_name))
                self.assertEqual(self.spark.table(table).count(), 2)


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class BatchFirstLoadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.directory = session("silver-first-load-batch")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_every_batch_lane_commits_its_first_batch_once(self):
        import lakehouse_ingest as ingest
        from platform_contracts import column_names, ensure_contract_table, load_lakehouse_contract

        lanes = tables_of("batch")
        self.assertIn("silver.parcel_boundaries", lanes)
        for contract_name in lanes:
            with self.subTest(table=contract_name):
                table, unquoted = target(contract_name)
                contract = load_lakehouse_contract(contract_name)
                self.spark.sql(f"DROP TABLE IF EXISTS {table}")
                ensure_contract_table(self.spark, table, contract)
                columns = column_names(contract)
                batch = frame(self.spark, contract, "bronze/first-a.zip", [1, 2])
                first = ingest.append_batch_once(self.spark, batch, columns, table, contract_name)
                self.assertTrue(first["appended"])
                self.assertEqual(first["ref"], ingest.MAIN)
                self.assertEqual(len(main_snapshots(self.spark, table)), 1, "the first batch is one commit")
                self.assertTrue(set(first["record_ids"]) <= set(ingest.read_ingested_objects(self.spark, table, unquoted)))
                self.assertFalse(ingest.append_batch_once(self.spark, batch, columns, table, contract_name)["appended"])
                self.assertEqual(self.spark.table(table).count(), 2)


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class MapEditLedgerFirstLoadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.directory = session("silver-first-load-map-edit-ledger")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_the_first_edits_into_an_empty_ledger_are_appended_once(self):
        import served_gold_common as common

        table = common.ensure_table(self.spark, "`proof`.`silver`.`map_edit_ledger`", "proof", "silver",
                                    common.LEDGER_CONTRACT)
        edits = [{"unit": "complex", "change_seq": seq, "feature_id": f"99999-{seq}", "op": "delete",
                  "geometry_geojson": None, "geometry_wkb_hex": None, "geometry_srid": 4326,
                  "geometry_checksum_sha256": None, "properties_json": "{}", "editor": "synthetic",
                  "edited_at": "2099-01-01T00:00:00Z"} for seq in (1, 2)]
        now = datetime(2099, 1, 2, tzinfo=timezone.utc)
        appended, ledger = common.append_to_ledger(self.spark, table, "complex", edits, "batch-1", now)
        self.assertEqual((appended, len(ledger)), (2, 2))
        self.assertEqual(common.append_to_ledger(self.spark, table, "complex", edits, "batch-2", now)[0], 0)
        self.assertEqual(len(main_snapshots(self.spark, table)), 1, "the first edits are one commit")


class _KeptSession:
    """The test's session, handed to a job that stops its own when it is done."""

    def __init__(self, spark):
        self._spark = spark

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def stop(self):
        pass


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class AdministrativeBoundariesFirstLoadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.directory = session("silver-first-load-administrative-boundaries")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_the_first_snapshot_into_an_empty_table_is_one_commit(self):
        import administrative_boundaries_handoff_to_silver as job

        square = [[[127.1231, 36.1231], [127.1234, 36.1231], [127.1234, 36.1234], [127.1231, 36.1234], [127.1231, 36.1231]]]
        geojson = {"type": "FeatureCollection", "features": [
            {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": square},
             "properties": {"EMD_CD": "99999101", "EMD_NM": "합성동", "SIGUNGU_CD": "99999", "SIGUNGU_NM": "합성구"}}]}
        builder = mock.MagicMock()
        for chained in ("appName", "config", "master"):
            getattr(builder, chained).return_value = builder
        builder.getOrCreate.return_value = _KeptSession(self.spark)
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(job, "load_pyspark", return_value=SimpleNamespace(builder=builder)), \
                mock.patch.object(job, "apply_catalog_settings", side_effect=lambda b, _catalog: b), \
                mock.patch.object(job, "assert_catalog_env"):
            source = Path(tmp) / "merged.geojson"
            source.write_text(json.dumps(geojson), encoding="utf-8")
            self.assertEqual(job.main(["--input", str(source), "--source-snapshot-id", "vworldkr__boundary_emd-99999",
                                       "--source-record-id", "bronze/first.zip", "--iceberg-catalog-name", "proof",
                                       "--allow-non-smoke-write"]), 0)
        table = "`proof`.`silver`.`administrative_boundaries`"
        self.assertEqual(self.spark.table(table).count(), 1)
        self.assertEqual(len(main_snapshots(self.spark, table)), 1, "the first snapshot is one commit")


if __name__ == "__main__":
    unittest.main()
