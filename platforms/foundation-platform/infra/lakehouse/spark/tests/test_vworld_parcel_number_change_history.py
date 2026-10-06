"""VWorld 필지고유번호변동연혁 (MK/30527): the parser, the handoff and the links the pairing reads
(root ADR-0144 §4, ADR-0145), on synthetic data.

Codes use the synthetic 시도 99 and lot numbers are made up. Every gate is proven by planting what it
must refuse: a moved header, a short row, a non-digit PNU part, a file of dateless rows, a shrunk
file, a file that did not land in Bronze, a handoff file whose bytes changed.
"""

from __future__ import annotations

import hashlib
import io
import json
import sys
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

import vworld_parcel_number_change_history as job  # noqa: E402

CONTRACT = job.load_source_contract()
HEADER = "|".join(CONTRACT["header"])
NOW = datetime(2099, 9, 16, tzinfo=timezone.utc)
MEMBER = "ABPD_UNQ_NO_CHG_HIST_99_209909.txt"


def line(old, new, reason="52", ymd="20990701", source="99999"):
    """One data row: `old`/`new` are 19-digit PNUs, split into the provider's five fields each."""

    def parts(pnu):
        return [pnu[:5], pnu[5:10], pnu[10], pnu[11:15], pnu[15:]]

    return "|".join([*parts(old), *parts(new), reason, ymd, source])


def dong(code):
    return f"{code}000000000"


def parcel(code, main, sub=0, ledger=1):
    return f"{code}{ledger}{main:04d}{sub:04d}"


def text(*rows, header=HEADER):
    return "\n".join([header, *rows]) + "\n"


def zipped(body, member=MEMBER):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(member, body.encode("utf-8"))
    return buffer.getvalue()


SAMPLE = text(
    line(dong("9999910100"), dong("9999810100"), reason="50", ymd="20990701"),
    line(parcel("9999910200", 3), parcel("9999910300", 993, 172), ymd="20990101"),
    line(parcel("9999010200", 1), parcel("9999110200", 1), reason="51", ymd="19890501"),
    *[line(parcel("9999910400", n), parcel("9999910500", n)) for n in range(1, 1001)],
    line(parcel("9999910400", 2000), parcel("9999910500", 2000), reason="81", ymd="X9A00000"),
)


class ParserTest(unittest.TestCase):
    def test_rows_become_pnu_pairs_with_their_date_and_reason(self):
        rows = job.parse_rows(SAMPLE, CONTRACT)
        self.assertEqual(len(rows), 1004)
        first = rows[0]
        self.assertEqual((first["old_pnu"], first["new_pnu"], first["is_dong_level"], first["reason_code"]),
                         (dong("9999910100"), dong("9999810100"), True, "50"))
        self.assertEqual(first["changed_on"], date(2099, 7, 1))
        self.assertEqual(rows[1]["new_pnu"], parcel("9999910300", 993, 172))
        self.assertFalse(rows[1]["is_dong_level"])
        self.assertEqual((rows[0]["source_line_number"], rows[1]["source_line_number"]), (2, 3))

    def test_a_row_the_file_repeats_is_kept_once_with_its_count_and_a_chain_keeps_every_step(self):
        a, b, c = parcel("9999910600", 1), parcel("9999910700", 1), parcel("9999910800", 1)
        body = text(line(a, b, ymd="20990101"), line(a, b, ymd="20990101"), line(a, b, ymd="20990101"),
                    line(b, c, ymd="20990701"))
        rows = job.parse_rows(body, CONTRACT)
        self.assertEqual([(r["old_pnu"], r["new_pnu"], r["duplicate_count"], r["source_line_number"]) for r in rows],
                         [(a, b, 3, 2), (b, c, 1, 5)])
        # The chain A -> B -> C on two dates is two links, each in its own window.
        self.assertEqual(job.official_links(rows, date(2099, 1, 1)), [(a, b), (b, c)])
        self.assertEqual(job.official_links(rows, date(2099, 6, 1)), [(b, c)])

    def test_a_date_after_the_collection_is_quarantined(self):
        body = text(line(dong("9999910100"), dong("9999810100"), ymd="20990916"),
                    line(dong("9999910200"), dong("9999810200"), ymd="20990917"))
        rows = job.parse_rows(body, CONTRACT, date(2099, 9, 15))
        self.assertEqual([r["quarantine_reason"] for r in rows], [None, "implausible_land_mov_ymd"])
        self.assertEqual(job.official_links(rows, date(2099, 1, 1)), [(dong("9999910100"), dong("9999810100"))])

    def test_a_row_without_a_date_is_kept_and_quarantined(self):
        last = job.parse_rows(SAMPLE, CONTRACT)[-1]
        self.assertIsNone(last["changed_on"])
        self.assertEqual((last["changed_on_raw"], last["quarantine_reason"]), ("X9A00000", "unparseable_land_mov_ymd"))
        job.check_rows(job.parse_rows(SAMPLE, CONTRACT), MEMBER, CONTRACT)  # one in 1,004 is within the share

    def test_a_file_of_dateless_rows_is_refused(self):
        allowed = CONTRACT["quarantine"]["max_rows_any_file"]
        small = text(line(parcel("9999910400", 1), parcel("9999910500", 1), ymd=""),
                     *[line(parcel("9999910400", n), parcel("9999910500", n)) for n in range(2, 40)])
        job.check_rows(job.parse_rows(small, CONTRACT), MEMBER, CONTRACT)  # one bad row in a small file passes
        planted = text(*[line(parcel("9999910400", n), parcel("9999910500", n), ymd="") for n in range(1, allowed + 2)])
        with self.assertRaisesRegex(job.SourceFormatError, "quarantined"):
            job.check_rows(job.parse_rows(planted, CONTRACT), MEMBER, CONTRACT)

    def test_a_row_with_a_malformed_pnu_is_kept_and_quarantined(self):
        # Measured: one real row with 3-digit 본번·부번. It is kept, quarantined, and never evidence.
        rows = job.parse_rows(text(line(dong("9999910100"), dong("9999810100")),
                                   "99999|10100|1|999|999|99998|10100|1|0999|0999|52|20990701|99999"), CONTRACT)
        self.assertEqual([r["quarantine_reason"] for r in rows], [None, "malformed_pnu"])
        self.assertFalse(rows[1]["is_dong_level"])
        self.assertEqual(job.official_links(rows, date(2099, 1, 1)), [(dong("9999910100"), dong("9999810100"))])

    def test_a_moved_header_short_row_or_letters_in_every_pnu_are_refused_whole(self):
        moved = HEADER.replace("OLD_BOBN|OLD_BUBN", "OLD_BUBN|OLD_BOBN")
        with self.assertRaisesRegex(job.SourceFormatError, "header"):
            job.parse_rows(text(line(dong("9999910100"), dong("9999810100")), header=moved), CONTRACT)
        with self.assertRaisesRegex(job.SourceFormatError, "fields"):
            job.parse_rows(text(line(dong("9999910100"), dong("9999810100")) + "|extra"), CONTRACT)
        letters = text(*[line(f"99999A{n:04d}000000000", dong("9999810100")) for n in range(1, 10)])
        with self.assertRaisesRegex(job.SourceFormatError, "quarantined"):
            job.check_rows(job.parse_rows(letters, CONTRACT), MEMBER, CONTRACT)

    def test_the_zip_must_hold_one_named_utf8_member(self):
        self.assertEqual(job.read_member(zipped(SAMPLE), CONTRACT)[0], MEMBER)
        with self.assertRaisesRegex(job.SourceFormatError, "member"):
            job.read_member(zipped(SAMPLE, member="other.txt"), CONTRACT)
        with self.assertRaisesRegex(job.SourceFormatError, "not a zip"):
            job.read_member(b"<html>login</html>", CONTRACT)

    def test_a_shrunk_file_is_refused(self):
        job.check_shrink(100, 100, MEMBER, CONTRACT)
        job.check_shrink(100, None, MEMBER, CONTRACT)
        with self.assertRaisesRegex(job.SourceFormatError, "shrunk"):
            job.check_shrink(90, 100, MEMBER, CONTRACT)

    def test_the_pairing_reads_dated_unquarantined_links_in_its_window_once(self):
        rows = job.parse_rows(SAMPLE, CONTRACT)
        links = job.official_links([*rows, *rows], date(2099, 1, 1))  # an old-name file repeats the rows
        self.assertIn((dong("9999910100"), dong("9999810100")), links)
        self.assertNotIn((parcel("9999010200", 1), parcel("9999110200", 1)), links, "1989 is outside the window")
        self.assertNotIn((parcel("9999910400", 2000), parcel("9999910500", 2000)), links, "a quarantined row is no evidence")
        self.assertEqual(len(links), len(set(links)))


# The provider's download id in the public repository's synthetic range (scripts/guard/public-repository-safety.sh).
DOWNLOAD_ID = "20991231DS99991"
BRONZE = CONTRACT["bronze_source"]


def key(file_no):
    return f"{DOWNLOAD_ID}-{file_no}"


def inventory(*files):
    return {"jobs": [{"endpoint_slug": CONTRACT["endpoint_slug"], "files": list(files)}]}


def item(file_no, name, updated_at="2099-09-15", size_kib="3"):
    selector = CONTRACT["provider_dataset_selector"]
    return {"svc_cde": selector["svc_cde"], "ds_id": selector["ds_id"], "download_ds_id": DOWNLOAD_ID,
            "file_no": file_no, "provider_file_name": name, "updated_at": updated_at, "base_ym": "2099-09",
            "size_kib": size_kib}


class HandoffTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.downloads = self.root / "downloads"
        self.downloads.mkdir()
        self.state = self.root / "state"
        self.state.mkdir()

    def evidence(self, *keys, status="succeeded", names=None, plain=False):
        """The ingest's evidence: each file under the content key of the bytes in `downloads` (root
        ADR-0152), or under its plain provider-file key with `plain`."""
        names = names or {}

        def object_key(k):
            extension = names.get(k, "ABPD_UNQ_NO_CHG_HIST_합성.zip").rsplit(".", 1)[1]
            if plain:
                return f"{BRONZE}/{k}.{extension}"
            path = self.downloads / f"{k}.zip"
            checksum = hashlib.sha256(path.read_bytes()).hexdigest() if extension == "zip" and path.exists() else "0" * 64
            return f"{BRONZE}/{k}--sha256-{checksum}.{extension}"

        return {"files": [{"download_ds_id": k.split("-")[0], "file_no": k.split("-")[1], "status": status,
                           "provider_file_name": names.get(k, "ABPD_UNQ_NO_CHG_HIST_합성.zip"),
                           "object_key": object_key(k)} for k in keys]}

    def test_every_file_is_collected_again_only_when_its_date_or_size_moved(self):
        inv = inventory(item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip"), item("1", "ABPD_UNQ_NO_CHG_HIST.xlsx"),
                        item("7", "ABPD_UNQ_NO_CHG_HIST_합성둘.zip", "2099-06-14"))
        first = job.changed_files(inv, {}, CONTRACT)
        self.assertEqual([f["file_no"] for f in first], ["1", "5", "7"], "the table definition is collected too")
        accepted = {"files": {key("1"): {"updated_at": "2099-09-15", "size_kib": "3"},
                              key("5"): {"updated_at": "2099-09-15", "size_kib": "3"},
                              key("7"): {"updated_at": "2099-06-14", "size_kib": "3"}}}
        self.assertEqual(job.changed_files(inv, accepted, CONTRACT), [])
        inv["jobs"][0]["files"][2]["updated_at"] = "2099-10-01"  # the provider replaced file 7 in place
        self.assertEqual([f["file_no"] for f in job.changed_files(inv, accepted, CONTRACT)], ["7"])
        inv["jobs"][0]["files"][2]["updated_at"] = "2099-06-14"
        inv["jobs"][0]["files"][0]["size_kib"] = "4"  # same date, other bytes
        self.assertEqual([f["file_no"] for f in job.changed_files(inv, accepted, CONTRACT)], ["5"])
        self.assertEqual([f["file_no"] for f in job.select_inventory(inv, first)["jobs"][0]["files"]], ["5", "1", "7"])

    def test_a_listed_file_the_contract_does_not_name_is_refused(self):
        with self.assertRaisesRegex(job.SourceFormatError, "neither"):
            job.changed_files(inventory(item("9", "SOMETHING_ELSE.csv")), {}, CONTRACT)

    def test_the_table_definition_lands_in_bronze_and_is_not_loaded(self):
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        files = [item("1", "ABPD_UNQ_NO_CHG_HIST.xlsx"), item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")]
        evidence = self.evidence(key("1"), key("5"), names={key("1"): "ABPD_UNQ_NO_CHG_HIST.xlsx"})
        self.assertEqual([k for k, _ in job.landed_objects(evidence, CONTRACT)], [key("5")], "only 시도 files are read back")
        result = job.stage_handoff(files, evidence, self.downloads, self.state, CONTRACT, NOW)
        self.assertEqual((result["files"], result["reference_files"]), (1, 1))
        accepted = json.loads((self.state / "accepted.json").read_text(encoding="utf-8"))
        self.assertTrue(accepted["files"][key("1")]["reference"])
        only_reference = job.stage_handoff([item("1", "ABPD_UNQ_NO_CHG_HIST.xlsx", "2099-10-01")],
                                           self.evidence(key("1"), names={key("1"): "ABPD_UNQ_NO_CHG_HIST.xlsx"}),
                                           self.downloads, self.state, CONTRACT, NOW)
        self.assertEqual(only_reference["status"], "reference_only")
        self.assertEqual(len(list((self.state / "pending").iterdir())), 1, "a new table definition hands off nothing")

    def test_a_checked_file_is_handed_off_and_loaded_with_its_lineage(self):
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        files = [item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")]
        result = job.stage_handoff(files, self.evidence(key("5")), self.downloads, self.state, CONTRACT, NOW)
        self.assertEqual((result["status"], result["files"], result["rows"]), ("staged", 1, 1004))
        accepted = json.loads((self.state / "accepted.json").read_text(encoding="utf-8"))
        self.assertEqual(accepted["files"][key("5")]["rows"], 1004)
        [(obj, rows)] = job.handoff_rows(self.state / "pending" / result["handoff"], CONTRACT, NOW)
        checksum = hashlib.sha256(zipped(SAMPLE)).hexdigest()
        self.assertEqual(obj["object_key"], f"{BRONZE}/{key('5')}--sha256-{checksum}.zip")
        self.assertEqual({row["source_snapshot_id"] for row in rows}, {f"{key('5')}@2099-09-15"})
        self.assertEqual({row["source_record_id"] for row in rows}, {obj["object_key"]})
        self.assertEqual(rows[0]["provider_updated_on"], date(2099, 9, 15))

    def test_a_file_that_did_not_land_in_bronze_refuses_the_handoff_and_keeps_the_state(self):
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        files = [item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")]
        with self.assertRaisesRegex(job.SourceFormatError, "did not land"):
            job.stage_handoff(files, self.evidence(key("5"), status="failed"), self.downloads, self.state, CONTRACT, NOW)
        self.assertFalse((self.state / "accepted.json").exists())
        self.assertFalse((self.state / "pending").exists())

    def test_a_shrunk_replacement_refuses_the_handoff(self):
        (self.state / "accepted.json").write_text(json.dumps({"files": {key("5"): {"updated_at": "2099-06-01", "rows": 5000}}}), encoding="utf-8")
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        with self.assertRaisesRegex(job.SourceFormatError, "shrunk"):
            job.stage_handoff([item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")], self.evidence(key("5")), self.downloads,
                              self.state, CONTRACT, NOW)

    def test_a_handoff_file_whose_bytes_changed_is_not_loaded(self):
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        result = job.stage_handoff([item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")], self.evidence(key("5")),
                                   self.downloads, self.state, CONTRACT, NOW)
        handoff = self.state / "pending" / result["handoff"]
        other = zipped(text(line(dong("9999910100"), dong("9999810100"))))
        (handoff / "objects" / f"{key('5')}.zip").write_bytes(other)
        with self.assertRaisesRegex(job.SourceFormatError, "not the file"):
            job.handoff_rows(handoff, CONTRACT, NOW)

    def test_a_plain_provider_file_key_is_never_handed_off(self):
        # 2026-10-05: the first production run wrote every file under its plain key, then stopped.
        # Those keys do not name their bytes; nothing downstream may read them (root ADR-0152).
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        files = [item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")]
        with self.assertRaisesRegex(job.SourceFormatError, "not a content-addressed key"):
            job.landed_objects(self.evidence(key("5"), plain=True), CONTRACT)
        with self.assertRaisesRegex(job.SourceFormatError, "not a content-addressed key"):
            job.stage_handoff(files, self.evidence(key("5"), plain=True), self.downloads, self.state, CONTRACT, NOW)
        self.assertFalse((self.state / "accepted.json").exists())
        self.assertFalse((self.state / "pending").exists())

    def test_bytes_that_are_not_the_ones_the_key_names_are_not_handed_off(self):
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        evidence = self.evidence(key("5"))
        other = zipped(text(line(dong("9999910100"), dong("9999810100"))))
        (self.downloads / f"{key('5')}.zip").write_bytes(other)
        with self.assertRaisesRegex(job.SourceFormatError, "not the ones its key names"):
            job.stage_handoff([item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")], evidence, self.downloads, self.state,
                              CONTRACT, NOW)
        self.assertFalse((self.state / "pending").exists())

    def test_a_key_of_another_file_is_not_handed_off(self):
        (self.downloads / f"{key('5')}.zip").write_bytes(zipped(SAMPLE))
        evidence = self.evidence(key("5"))
        evidence["files"][0]["object_key"] = evidence["files"][0]["object_key"].replace(f"{key('5')}--", f"{key('7')}--")
        with self.assertRaisesRegex(job.SourceFormatError, "not a content-addressed key"):
            job.stage_handoff([item("5", "ABPD_UNQ_NO_CHG_HIST_합성.zip")], evidence, self.downloads, self.state,
                              CONTRACT, NOW)

    def test_nothing_changed_stages_nothing(self):
        self.assertEqual(job.stage_handoff([], {"files": []}, self.downloads, self.state, CONTRACT, NOW)["status"], "unchanged")


class TableContractTest(unittest.TestCase):
    def test_the_loader_fills_every_column_of_the_table_contract(self):
        from platform_contracts import column_names, load_lakehouse_contract  # noqa: PLC0415

        rows = job.table_rows(job.parse_rows(SAMPLE, CONTRACT), member=MEMBER, source_record_id="k",
                              source_snapshot_id="s", provider_updated_on=date(2099, 9, 15), now=NOW)
        self.assertEqual(set(rows[0]), set(column_names(load_lakehouse_contract(job.TABLE_CONTRACT))))


if __name__ == "__main__":
    unittest.main()
