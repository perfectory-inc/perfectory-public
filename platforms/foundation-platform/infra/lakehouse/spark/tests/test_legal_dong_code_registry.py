"""Pure-python fixtures for the legal-dong registry kernels and the seed crosswalk."""

import contextlib
import io
import json
import sys
import unittest
from pathlib import Path

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

from legal_dong_code_registry import (CONTRACT, CROSSWALK_CONTRACT, crosswalk_rows_from_seed,
                                      parse_args, parse_registry_row, resolve_sigungu)
from platform_contracts import (column_names, create_table_columns_sql, load_lakehouse_contract,
                                load_unit, partition_clause_sql)

SEED_PATH = SPARK_DIR.parent / "contracts" / "sigungu-canonical-crosswalk.seed.json"


def seed():
    return json.loads(SEED_PATH.read_text(encoding="utf-8"))


class ParseRegistryRowTest(unittest.TestCase):
    def test_current_and_abolished_rows(self):
        cur = parse_registry_row(
            ["2911010100", "29", "110", "101", "00", "광주광역시 동구 계림동", "2911000000", "19880423", ""])
        self.assertEqual(cur["region_cd"], "2911010100")
        self.assertIs(cur["is_current"], True)
        self.assertEqual(cur["abolished_date"], "")
        old = parse_registry_row(
            ["1211010100", "12", "110", "101", "00", "전라남도 광주시 계림동", "1211000000", "19800101", "19861101"])
        self.assertIs(old["is_current"], False)
        self.assertEqual(old["abolished_date"], "19861101")

    def test_malformed_rows_are_refused_not_repaired(self):
        good = ["2911010100", "29", "110", "101", "00", "광주광역시 동구 계림동", "2911000000", "19880423", ""]
        for index, bad in ((0, "29110"), (0, "29110101AB"), (7, ""), (7, "1988-04-23"), (8, "someday")):
            with self.subTest(index=index, bad=bad):
                fields = list(good)
                fields[index] = bad
                with self.assertRaises(ValueError):
                    parse_registry_row(fields)
        with self.assertRaisesRegex(ValueError, "fields"):
            parse_registry_row(good[:4])


class ResolveSigunguTest(unittest.TestCase):
    def setUp(self):
        self.crosswalk = crosswalk_rows_from_seed(seed())

    def test_merged_codes_resolve_through_the_seed(self):
        # 12240 (현행 광주 서구) ↔ 29140 (지적이 아직 싣는 코드), and the
        # non-arithmetic 광양 pair 12190 ↔ 46230.
        west = resolve_sigungu("12240", "20260801", self.crosswalk)
        self.assertEqual(west.canonical, "29140")
        self.assertIs(west.unresolved, False)
        gwangyang = resolve_sigungu("12190", "20260801", self.crosswalk)
        self.assertEqual(gwangyang.canonical, "46230")
        self.assertIs(gwangyang.unresolved, False)

    def test_unknown_code_is_flagged_not_fabricated(self):
        result = resolve_sigungu("77999", "20260801", self.crosswalk)
        self.assertIsNone(result.canonical)
        self.assertIs(result.unresolved, True)

    def test_a_link_is_closed_before_it_opens(self):
        # The seed opens the 광주+전남 links at the merger date; the code did not
        # name anything the month before, so resolving it then must not invent a place.
        before = resolve_sigungu("12240", "20260601", self.crosswalk)
        self.assertIsNone(before.canonical)
        self.assertIs(before.unresolved, True)

    def test_every_seed_pair_resolves_to_a_distinct_canonical(self):
        entries = seed()["sigungu"]
        canonicals = set()
        for entry in entries:
            result = resolve_sigungu(entry["current_code"], "20260801", self.crosswalk)
            self.assertEqual(result.canonical, entry["superseded_code"])
            canonicals.add(result.canonical)
        self.assertEqual(len(canonicals), len(entries))

    def test_conflicting_live_rows_are_refused_not_picked_from(self):
        conflicted = self.crosswalk + [
            {"source_code": "12240", "canonical_code": "46999", "valid_from": "20260701",
             "valid_to": "", "provenance": "test:conflict"}]
        with self.assertRaisesRegex(ValueError, "multiple canonical"):
            resolve_sigungu("12240", "20260801", conflicted)

    def test_as_of_must_be_a_date(self):
        with self.assertRaisesRegex(ValueError, "as_of"):
            resolve_sigungu("12240", "August 2026", self.crosswalk)


class ContractArtifactTest(unittest.TestCase):
    """The two registry tables must stay readable by the same loader every job uses."""

    def test_legal_dong_code_contract_round_trips_through_the_loader(self):
        contract = load_lakehouse_contract(CONTRACT)
        names = column_names(contract)
        self.assertEqual(names[0], "region_cd")
        self.assertIn("is_current", names)
        self.assertIn("abolished_date", names)
        self.assertIn("is_current BOOLEAN", create_table_columns_sql(contract))
        self.assertEqual(partition_clause_sql(contract), "PARTITIONED BY (truncate(region_cd, 2))")
        self.assertIn("append_only", contract["quality_gates"])
        self.assertIn("region_cd_not_null", contract["quality_gates"])
        self.assertEqual(load_unit(CONTRACT)["column"], "source_record_id")

    def test_crosswalk_contract_matches_the_rows_the_resolver_reads(self):
        contract = load_lakehouse_contract(CROSSWALK_CONTRACT)
        self.assertEqual(
            column_names(contract),
            ("source_code", "canonical_code", "valid_from", "valid_to", "provenance"))
        rows = crosswalk_rows_from_seed(seed())
        for row in rows:
            self.assertEqual(tuple(row), column_names(contract))
        self.assertIn("append_only", contract["quality_gates"])
        self.assertTrue(load_unit(CROSSWALK_CONTRACT)["unit"])


class ParseArgsTest(unittest.TestCase):
    def test_submission_requires_input_and_a_clean_snapshot_id(self):
        args = parse_args(["--input", "rows.tsv", "--source-snapshot-id", "20260912"])
        self.assertEqual(args.source_snapshot_id, "20260912")
        self.assertEqual(args.iceberg_catalog_name, "r2")
        for argv in ([],
                     ["--input", "rows.tsv"],
                     ["--input", "rows.tsv", "--source-snapshot-id", "a,b"],
                     ["--input", "rows.tsv", "--source-snapshot-id", " "],
                     ["--input", "rows.tsv", "--source-snapshot-id", "x", "--iceberg-catalog-name", "bad-name"]):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(argv)


if __name__ == "__main__":
    unittest.main()
