import io
import sys
import unittest
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_lineage as pl  # noqa: E402
from legal_dong_code_snapshot_to_reference import read_code_file, snapshot_rows  # noqa: E402
from parcel_lineage_to_silver import derive, parse_args  # noqa: E402

# Synthetic codes (public repository). Old sido 99 was renumbered to 98 on 2099-07-01; in the same
# change one dong's land was partly transferred into another dong and re-lotted.
CODES = pl.parse_code_list(
    "\n".join(
        [
            "법정동코드\t법정동명\t폐지여부",
            "9911010100\t합성도 가구 갑동\t폐지",
            "9911010200\t합성도 가구 을동\t폐지",
            "9812010100\t합성시 가구 갑동\t존재",
            "9812010200\t합성시 나구 을동\t존재",
        ]
    )
)
OLD_A, OLD_B, NEW_A, NEW_B = "9911010100", "9911010200", "9812010100", "9812010200"


def pnu(dong, main, sub=0):
    return f"{dong}1{main:04d}{sub:04d}"


def event(p, reason, when, code="10", area="", category=""):
    return pl.MovementEvent(p, code, reason, when, "", category, area)


class DeriveTest(unittest.TestCase):
    def run_derive(self):
        before = {pnu(OLD_A, n) for n in range(1, 6)} | {pnu(OLD_B, 1), pnu(OLD_B, 150), pnu(OLD_B, 151)}
        after = {pnu(NEW_A, n) for n in (1, 2, 3, 5, 6)} | {pnu(NEW_B, 1), pnu(NEW_B, 803), pnu(NEW_B, 804)}
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
            before, after, CODES, window, "2099-06-01", "2099-10-01",
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

    def test_the_real_tables_need_the_explicit_flag(self):
        args = parse_args([
            "--from-snapshot-id", "a", "--to-snapshot-id", "b", "--from-date", "2099-06-01", "--to-date", "2099-10-01",
            "--from-sido", "99", "--to-sido", "98",
        ])
        from parcel_lineage_to_silver import validate_args

        with self.assertRaisesRegex(ValueError, "allow-non-smoke-write"):
            validate_args(args)


class CodeSnapshotLoaderTest(unittest.TestCase):
    def test_the_zipped_cp949_file_becomes_one_row_per_code(self):
        text = "법정동코드\t법정동명\t폐지여부\n9812010100\t합성시 가구 갑동\t존재\n9911010100\t합성도 가구 갑동\t폐지\n"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("codes.txt", text.encode("cp949"))
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "codes.zip"
            path.write_bytes(buffer.getvalue())
            now = datetime(2099, 1, 1, tzinfo=timezone.utc)
            rows = snapshot_rows(read_code_file(path), date(2099, 9, 1), "bronze/synthetic/codes.zip", now)
        self.assertEqual([(r["region_cd"], r["status"]) for r in rows], [("9812010100", pl.EXISTS), ("9911010100", pl.ABOLISHED)])
        self.assertEqual({r["snapshot_date"] for r in rows}, {date(2099, 9, 1)})


if __name__ == "__main__":
    unittest.main()
