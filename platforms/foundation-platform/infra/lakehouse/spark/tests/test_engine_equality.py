"""The engine-equality verdict, run on an engine that is neither side of a migration (root ADR-0557 §3).

SQLite runs the same comparison text DuckDB, Spark SQL and Trino run: the verdict must not depend on
either engine being compared. Rows are synthetic (PNU prefix 99999).
"""
import hashlib
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import engine_equality  # noqa: E402
import gold_fingerprint  # noqa: E402


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def pnu(n: int) -> str:
    return f"99999{n:014d}"


def database(reference: list[tuple], candidate: list[tuple]) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    for name, rows in (("reference", reference), ("candidate", candidate)):
        connection.execute(f"CREATE TABLE {name} (pnu TEXT, document TEXT, row_digest TEXT)")
        connection.executemany(f"INSERT INTO {name} VALUES (?, ?, ?)", rows)
    return connection


def runner(connection: sqlite3.Connection):
    def run(sql: str) -> list[dict]:
        if "digest_sum" in sql and "COUNT(*) AS rows" in sql:
            # The fingerprint's hex-to-number step is each engine's own words (DIGEST_NUMBER);
            # SQLite has none, so the test computes it with the module's engine-free function.
            table = sql.rsplit(" ", 1)[-1]
            digests = [row[0] for row in connection.execute(f"SELECT row_digest FROM {table}")]
            rows, total = engine_equality.fingerprint_of(digests).replace("rows:", "").split(";digest_sum:")
            return [{"rows": int(rows), "digest_sum": int(total)}]
        return [dict(row) for row in connection.execute(sql)]
    return run


def rows_of(documents: dict[int, str]) -> list[tuple]:
    return [(pnu(n), text, digest(pnu(n) + text)) for n, text in documents.items()]


class Comparison(unittest.TestCase):
    def compare(self, reference: dict[int, str], candidate: dict[int, str], content=("document",)):
        connection = database(rows_of(reference), rows_of(candidate))
        return engine_equality.compare(runner(connection), "reference", "candidate", ("pnu",), "duckdb", content)

    def test_the_same_rows_in_another_order_are_equal(self):
        documents = {n: f'{{"n":{n}}}' for n in range(1, 50)}
        found = self.compare(documents, dict(reversed(list(documents.items()))))
        self.assertTrue(found.equal)
        self.assertEqual((found.same, found.different, found.only_reference, found.only_candidate), (49, 0, 0, 0))
        self.assertEqual(found.reference_fingerprint, found.candidate_fingerprint)

    def test_a_row_only_one_engine_wrote_is_not_equal_and_is_named(self):
        found = self.compare({1: "a", 2: "b", 3: "c"}, {1: "a", 2: "b", 4: "d"})
        self.assertFalse(found.equal)
        self.assertEqual((found.same, found.only_reference, found.only_candidate), (2, 1, 1))
        self.assertEqual(found.samples["only_reference"], [{"pnu": pnu(3)}])
        self.assertEqual(found.samples["only_candidate"], [{"pnu": pnu(4)}])

    def test_a_different_document_under_the_same_key_is_not_equal(self):
        found = self.compare({1: '{"distance_m":120}'}, {1: '{"distance_m":121}'})
        self.assertFalse(found.equal)
        self.assertEqual(found.different, 1)
        self.assertEqual(found.written_differently, 0)
        self.assertEqual(found.samples["different"][0]["candidate_document"], '{"distance_m":121}')

    def test_a_document_written_differently_is_still_not_equal_but_is_told_apart(self):
        found = self.compare({1: '{"area":10.0,"zone":null}', 2: '{"a":1,"b":2}'}, {1: '{"area":10}', 2: '{"b":2,"a":1}'})
        self.assertFalse(found.equal)
        self.assertEqual((found.different, found.written_differently), (2, 2))

    def test_a_key_written_twice_is_refused_even_when_the_join_counts_agree(self):
        reference = rows_of({1: "a"})
        connection = database(reference, reference + reference)
        found = engine_equality.compare(runner(connection), "reference", "candidate", ("pnu",), "duckdb")
        self.assertEqual(found.duplicate_candidate_keys, 1)
        self.assertFalse(found.equal)

    def test_two_empty_outputs_are_equal(self):
        self.assertTrue(self.compare({}, {}).equal)

    def test_a_comparison_without_a_key_is_refused(self):
        with self.assertRaises(ValueError):
            engine_equality.keyed_diff_sql("reference", "candidate", ())


class Fingerprint(unittest.TestCase):
    def test_the_engine_free_fingerprint_is_the_producers(self):
        digests = [digest(str(n)) for n in range(1000)]
        total = sum(int(d[:gold_fingerprint.HEX_DIGITS], 16) for d in digests)
        self.assertEqual(engine_equality.fingerprint_of(digests), gold_fingerprint.value(1000, total))
        self.assertEqual(engine_equality.fingerprint_of(reversed(digests)), engine_equality.fingerprint_of(digests))

    def test_every_engine_is_asked_for_the_same_digits(self):
        for engine in engine_equality.DIGEST_NUMBER:
            sql = engine_equality.fingerprint_sql("gold", engine)
            self.assertIn(f", {gold_fingerprint.HEX_DIGITS})", sql)
            self.assertIn(gold_fingerprint.DIGEST, sql)

    def test_the_report_carries_the_verdict_and_who_was_compared(self):
        found = engine_equality.verdict_of(
            {"reference_rows": 2, "candidate_rows": 2, "same": 2, "different": 0, "only_reference": 0, "only_candidate": 0},
            reference_fingerprint="rows:2;digest_sum:5", candidate_fingerprint="rows:2;digest_sum:5")
        report = found.report(job="parcel_nearby_silver_to_gold", reference_engine="spark", candidate_engine="duckdb")
        self.assertEqual((report["equal"], report["job"], report["schema_version"]),
                         (True, "parcel_nearby_silver_to_gold", engine_equality.REPORT_SCHEMA_VERSION))


class Command(unittest.TestCase):
    def test_the_command_compares_two_parquet_outputs(self):
        try:
            import duckdb
        except ImportError:
            self.skipTest("the duckdb Python module is not installed on this runner")
        import contextlib
        import io
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as work:
            for name, rows in (("reference", rows_of({1: "a", 2: "b"})), ("candidate", rows_of({1: "a", 2: "x"}))):
                connection = duckdb.connect()
                connection.execute("CREATE TABLE t (pnu VARCHAR, document VARCHAR, row_digest VARCHAR)")
                connection.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
                connection.execute(f"COPY t TO '{Path(work, name).as_posix()}.parquet' (FORMAT parquet)")
            arguments = ["--key", "pnu", "--content", "document", "--job", "j", "--reference-engine", "spark",
                         "--candidate-engine", "duckdb", "--report", str(Path(work, "report.json"))]
            with contextlib.redirect_stdout(io.StringIO()):
                same = engine_equality.main(["--reference", f"{work}/reference.parquet", "--candidate", f"{work}/reference.parquet", *arguments])
                differs = engine_equality.main(["--reference", f"{work}/reference.parquet", "--candidate", f"{work}/candidate.parquet", *arguments])
            report = json.loads(Path(work, "report.json").read_text(encoding="utf-8"))
        self.assertEqual((same, differs), (0, 1))
        self.assertEqual((report["different"], report["same"], report["equal"]), (1, 1, False))


if __name__ == "__main__":
    unittest.main()
