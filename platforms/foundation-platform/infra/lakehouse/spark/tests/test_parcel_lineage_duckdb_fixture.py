"""The lineage kernels and the publisher's DuckDB derivation are held to one fixture (root ADR-0559 §3).

The scheduled derivation left Spark: its rules run as SQL inside the publisher
(`foundation-publisher-silver/src/parcel_lineage_stage/derive.sql`). The kernels of
`parcel_lineage.py` stay, as the rules' reference and for the readers that import them, so the same
rules are written twice. Both are held to the same synthetic group: this test derives the fixture's
inputs with the kernels and compares with `spark_rows.txt` and `summary.json`; the Rust test
`the_staged_rows_are_the_rows_the_spark_kernels_derived` derives the same inputs with the SQL and
compares with the same files. A rule changed on one side alone fails one of the two.

To change a rule: change the kernel and `derive.sql`, then run this file with `--write` to rewrite
the two expected files, and review the difference.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

JOBS = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS))

import parcel_lineage as pl  # noqa: E402

FIXTURES = (Path(__file__).resolve().parents[4] / "crates" / "publisher" / "foundation-publisher-silver" / "src"
            / "parcel_lineage_stage" / "fixtures")


def event(row: dict) -> pl.MovementEvent:
    return pl.MovementEvent(row["pnu"], row["reason_code"] or "", row["reason"] or "", row["moved_at"] or "",
                            row["erased_at"] or "", row["land_category_code"] or "",
                            "" if row["area_m2"] is None else str(row["area_m2"]))


def after(text: str, first: str, then: str) -> bool:
    """SQL's `LIKE '%first%then%'`."""
    return first in text and then in text.split(first, 1)[1]


def derived() -> tuple[list[str], dict]:
    inputs = json.loads((FIXTURES / "inputs.json").read_text(encoding="utf-8"))
    history, since, until = inputs["history"], inputs["from_date"], inputs["to_date"]
    # The window's events the rules read, as `derive.sql` selects them.
    window = {event(row) for row in history if since <= row["moved_at"] < until and (
        row["reason_code"] == pl.JURISDICTION_TRANSFER_CODE or after(row["reason"], "합병되어", "말소")
        or after(row["reason"], "번에서", "분할") or after(row["reason"], "번에서", "등록전환"))}

    def facts_before(pool: set[str]) -> list[pl.MovementEvent]:
        # In the history's own sequence: of several events of one day the rule keeps the last it is handed.
        return [event(row) for row in sorted(history, key=lambda row: row["transfer_history_seq"])
                if row["pnu"] in pool and row["moved_at"] < since]

    links, summary = pl.derive(set(inputs["before"]), set(inputs["after"]), inputs["code_changes"], window, since, until,
                               facts_before, {}, {}, {}, {})
    cards = pl.cardinality(links)
    lines = sorted("|".join([
        link.predecessor_pnu or "NULL", link.successor_pnu, link.relation,
        cards.get((link.predecessor_pnu, link.successor_pnu)) or "NULL", link.effective_date or "NULL", link.grade,
        link.evidence_kind, link.evidence_ref or "NULL"]) for link in links)
    return lines, {key: summary[key] for key in ("reconciliation", "links_by_relation", "links_by_grade")}


class TheKernelsDeriveTheFixtureTheSqlIsHeldTo(unittest.TestCase):
    def test_the_rows_and_the_summary_are_the_expected_files(self):
        lines, summary = derived()
        self.assertEqual(lines, (FIXTURES / "spark_rows.txt").read_text(encoding="utf-8").splitlines())
        self.assertEqual(summary, json.loads((FIXTURES / "summary.json").read_text(encoding="utf-8")))

    def test_the_fixture_reaches_every_rule_the_schedule_applies(self):
        lines, summary = derived()
        self.assertEqual(set(summary["links_by_relation"]),
                         {"code_change", "merge", "split", "registration_conversion", "jurisdiction_transfer"})
        self.assertEqual(set(summary["links_by_grade"]), {"code_derived", "official", "evidence_weak", "pending"})
        self.assertTrue(any("|N:1|" in line for line in lines), "a successor two links reach")
        self.assertTrue(all(line.split("|")[1].startswith("99") for line in lines), "synthetic parcels only")

    def test_a_vanished_dong_code_the_table_does_not_pair_is_refused_with_its_codes(self):
        inputs = json.loads((FIXTURES / "inputs.json").read_text(encoding="utf-8"))
        kept = set(inputs["after"]) - {"9999950100100010000"}
        with self.assertRaises(pl.UnrecordedDongCodes) as refused:
            pl.derive(set(inputs["before"]), kept, inputs["code_changes"], set(), inputs["from_date"], inputs["to_date"],
                      lambda pool: [], {}, {}, {}, {})
        self.assertEqual(refused.exception.codes, ["9999950100"])


if __name__ == "__main__":
    if sys.argv[1:] == ["--write"]:
        rows, counts = derived()
        (FIXTURES / "spark_rows.txt").write_text("\n".join(rows) + "\n", encoding="utf-8", newline="\n")
        (FIXTURES / "summary.json").write_text(json.dumps(counts, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
                                               encoding="utf-8", newline="\n")
        print(f"wrote {len(rows)} rows")
    else:
        unittest.main()
