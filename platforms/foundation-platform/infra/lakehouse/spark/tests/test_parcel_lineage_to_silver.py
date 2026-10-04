import io
import json
import sys
import unittest
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_lineage as pl  # noqa: E402
from legal_dong_code_snapshot_to_reference import read_code_file, snapshot_rows  # noqa: E402
from parcel_lineage_to_silver import derive, parse_args  # noqa: E402

# Synthetic codes in the reserved 99999 range. Old dongs 99999-1xx were renumbered to 99999-2xx on 2099-07-01; in the same
# change one dong's land was partly transferred into another dong and re-lotted.
OLD_A, OLD_B, NEW_A, NEW_B = "9999910100", "9999910200", "9999920100", "9999920200"
# The code change table's rows for that change: the lineage reads its 동 pairs, it does not pair codes.
CHANGES = [
    {"kind": "pair", "old_code": OLD_A, "new_code": NEW_A, "level": "eupmyeondong", "effective_date": "20990701",
     "source": "derived:code-go-kr:date+name:20990630"},
    {"kind": "pair", "old_code": OLD_B, "new_code": NEW_B, "level": "eupmyeondong", "effective_date": "20990701",
     "source": "derived:parcel-jibun:june->september"},
]


def pnu(dong, main, sub=0):
    return f"{dong}1{main:04d}{sub:04d}"


def event(p, reason, when, code="10", area="", category=""):
    return pl.MovementEvent(p, code, reason, when, "", category, area)


class DeriveTest(unittest.TestCase):
    def run_derive(self, changes=CHANGES, also=frozenset()):
        before = {pnu(OLD_A, n) for n in range(1, 6)} | {pnu(OLD_B, 1), pnu(OLD_B, 150), pnu(OLD_B, 151)} | also
        after = {pnu(NEW_A, n) for n in (1, 2, 3, 5, 6)} | {pnu(NEW_B, 1), pnu(NEW_B, 803), pnu(NEW_B, 804)} | also
        window = {
            event(pnu(NEW_A, 4), "5번과 합병되어 말소", "2099-08-01"),
            event(pnu(NEW_A, 6), "3번에서 분할", "2099-08-02"),
            event(pnu(NEW_B, 803), "행정관할구역변경", "2099-07-01", code="52", area="142", category="17"),
            event(pnu(NEW_B, 804), "행정관할구역변경", "2099-07-01", code="52", area="61", category="08"),
        }
        history_before = [
            event(pnu(OLD_B, 150), "지목변경", "2090-01-01", area="142.0", category="17"),
            event(pnu(OLD_B, 151), "지목변경", "2090-01-01", area="61.0", category="08"),
        ]
        return derive(
            before, after, changes, window, "2099-06-01", "2099-10-01",
            lambda pool: [e for e in history_before if e.pnu in pool],
            {"k1": pnu(OLD_B, 150)}, {"k1": pnu(NEW_B, 803)},
            {pnu(OLD_B, 151): {"owner_kind": "01", "co_owner_count": "0"}},
            {pnu(NEW_B, 804): {"owner_kind": "01", "co_owner_count": "0"}},
        )

    def test_every_change_is_linked_with_the_right_grade_and_the_counts_balance(self):
        links, summary = self.run_derive()
        pairs = {(link.predecessor_pnu, link.successor_pnu): (link.relation, link.grade) for link in links}
        self.assertEqual(pairs[(pnu(OLD_A, 1), pnu(NEW_A, 1))], ("code_change", "code_derived"))
        self.assertEqual(pairs[(pnu(OLD_A, 4), pnu(NEW_A, 5))], ("merge", "official"))
        self.assertEqual(pairs[(pnu(OLD_A, 3), pnu(NEW_A, 6))], ("split", "official"))
        self.assertEqual(pairs[(pnu(OLD_B, 150), pnu(NEW_B, 803))], ("jurisdiction_transfer", "code_derived"))
        self.assertEqual(pairs[(pnu(OLD_B, 151), pnu(NEW_B, 804))], ("jurisdiction_transfer", "evidence_strong"))
        rec = summary["reconciliation"]
        self.assertTrue(rec["balanced"])
        self.assertEqual((rec["vanished"], rec["appeared"]), (3, 3))
        self.assertEqual((rec["vanished_explained"], rec["appeared_explained"]), (3, 3))
        self.assertEqual(summary["identity_conflict_count"], 0)
        self.assertEqual(summary["transferred_in"], 2)

    def test_the_dong_pairs_come_from_the_code_change_table_only(self):
        links, summary = self.run_derive()
        self.assertEqual(summary["dongs"]["how"], {CHANGES[0]["source"]: 1, CHANGES[1]["source"]: 1})

    def test_a_vanished_dong_the_table_does_not_pair_is_refused(self):
        # The same parcels with an empty change table, or one missing a pair: the lineage does not pair
        # codes on its own (root ADR-0145 §2), and writing on would record no code_change link for
        # the dongs that vanished, silently.
        for changes, missing in (([], [OLD_A, OLD_B]), (CHANGES[:1], [OLD_B])):
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, rf"2099-06-01.*{missing}"):
                self.run_derive(changes=changes)

    def test_a_pair_outside_the_window_is_not_applied(self):
        # 99999-103 still holds its lots at the later snapshot: it was renumbered after --to-date, and
        # its pair is recorded already. Applied anyway, its lots would be sent to a dong that holds
        # none of them yet.
        later = {"kind": "pair", "old_code": "9999910300", "new_code": "9999920300", "level": "eupmyeondong",
                 "effective_date": "20991002", "source": "derived:code-go-kr:date+name:20991001"}
        links, summary = self.run_derive(changes=[*CHANGES, later], also=frozenset({pnu("9999910300", 1)}))
        self.assertEqual(summary["dongs"]["paired"], 2)
        self.assertFalse([link for link in links if "9999910300" in (link.predecessor_pnu or "")])
        self.assertTrue(summary["reconciliation"]["balanced"])
        on_the_last_day = {**later, "effective_date": "20991001"}
        _, summary = self.run_derive(changes=[*CHANGES, on_the_last_day], also=frozenset({pnu("9999910300", 1)}))
        self.assertEqual(summary["dongs"]["paired"], 3, "the last day of the window counts")

    def test_the_real_tables_need_the_explicit_flag(self):
        args = parse_args([
            "--from-snapshot-id", "a", "--to-snapshot-id", "b", "--from-date", "2099-06-01", "--to-date", "2099-10-01",
            "--from-sido", "99", "--to-sido", "99",
        ])
        from parcel_lineage_to_silver import validate_args

        with self.assertRaisesRegex(ValueError, "allow-non-smoke-write"):
            validate_args(args)


class DerivationIdentityTest(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.pins = Path(self.directory.name) / "pins.json"
        self.pins.write_text(json.dumps({
            role: {"table": table, "snapshot_id": str(index)}
            for index, (role, table) in enumerate([
                ("boundaries_from", "silver.parcel_boundaries"),
                ("boundaries_to", "staging.parcel_boundaries"),
                ("code_changes", "reference.legal_dong_code_change"),
                ("history", "silver.land_transfer_history"),
            ], 1)
        }), encoding="utf-8")

    def args(self):
        return parse_args([
            "--from-snapshot-id", "a", "--to-snapshot-id", "b",
            "--from-date", "2099-06-01", "--to-date", "2099-10-01",
            "--from-sido", "99", "--to-sido", "99",
            "--iceberg-table", "lineage_smoke", "--source-snapshots-path", str(self.pins),
        ])

    def identity(self, args):
        from parcel_lineage_to_silver import derivation_run_id, input_provenance, validate_args

        with mock.patch("parcel_lineage_to_silver.assert_catalog_env"):
            inputs = validate_args(args)
        return derivation_run_id(input_provenance(args, inputs))

    def test_different_history_window_cannot_reuse_the_recorded_append(self):
        args = self.args()
        original = self.identity(args)
        args.from_date = "2099-05-01"
        self.assertNotEqual(original, self.identity(args))

    def test_changed_ownership_evidence_cannot_reuse_the_recorded_append(self):
        args = self.args()
        with TemporaryDirectory() as directory:
            evidence = Path(directory) / "old.jsonl"
            args.ownership_old_jsonl = str(evidence)
            row = {"pnu": pnu(OLD_A, 1), "owner_kind": "01", "co_owner_count": "0"}
            evidence.write_text(json.dumps(row) + "\n", encoding="utf-8")
            original = self.identity(args)
            row["owner_kind"] = "02"
            evidence.write_text(json.dumps(row) + "\n", encoding="utf-8")
            self.assertNotEqual(original, self.identity(args))


class CodeSnapshotLoaderTest(unittest.TestCase):
    def test_the_zipped_cp949_file_becomes_one_row_per_code(self):
        text = "법정동코드\t법정동명\t폐지여부\n9999920100\t합성시 가구 갑동\t존재\n9999910100\t합성도 가구 갑동\t폐지\n"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("codes.txt", text.encode("cp949"))
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "codes.zip"
            path.write_bytes(buffer.getvalue())
            now = datetime(2099, 1, 1, tzinfo=timezone.utc)
            rows = snapshot_rows(read_code_file(path), date(2099, 9, 1), "bronze/synthetic/codes.zip", now)
        self.assertEqual([(r["region_cd"], r["status"]) for r in rows], [("9999910100", pl.ABOLISHED), ("9999920100", pl.EXISTS)])
        self.assertEqual({r["snapshot_date"] for r in rows}, {date(2099, 9, 1)})


if __name__ == "__main__":
    unittest.main()
