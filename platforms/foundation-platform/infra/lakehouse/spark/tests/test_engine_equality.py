"""The engine-equality verdict, run on an engine that is neither side of a migration (root ADR-0559 §3).

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
        self.assertEqual(found.sampled_written_differently, 0)
        self.assertEqual(found.samples["different"][0]["candidate_document"], '{"distance_m":121}')

    def test_a_document_written_differently_is_still_not_equal_but_is_told_apart(self):
        found = self.compare({1: '{"area":10.0,"zone":null}', 2: '{"a":1,"b":2}'}, {1: '{"area":10}', 2: '{"b":2,"a":1}'})
        self.assertFalse(found.equal)
        self.assertEqual((found.different, found.sampled_written_differently), (2, 2))

    def test_a_key_written_twice_is_refused_even_when_the_join_counts_agree(self):
        reference = rows_of({1: "a"})
        connection = database(reference, reference + reference)
        found = engine_equality.compare(runner(connection), "reference", "candidate", ("pnu",), "duckdb")
        self.assertEqual(found.duplicate_candidate_keys, 1)
        self.assertFalse(found.equal)

    def test_the_same_digest_over_different_content_is_not_equal(self):
        # A port that hashes fewer columns than it writes: the digests agree and the documents do not.
        reference = [(pnu(1), '{"distance_m":120}', digest("same"))]
        candidate = [(pnu(1), '{"distance_m":999}', digest("same"))]
        connection = database(reference, candidate)
        found = engine_equality.compare(runner(connection), "reference", "candidate", ("pnu",), "duckdb", ("document",))
        self.assertEqual((found.same, found.different), (0, 1))
        self.assertFalse(found.equal)
        self.assertEqual(found.reference_fingerprint, found.candidate_fingerprint)

    def test_a_table_without_a_digest_is_compared_on_its_content(self):
        connection = database([(pnu(1), "a", None), (pnu(2), None, None)], [(pnu(1), "a", None), (pnu(2), "b", None)])
        found = engine_equality.compare(runner(connection), "reference", "candidate", ("pnu",), "duckdb", ("document",), None)
        self.assertEqual((found.same, found.different, found.reference_fingerprint), (1, 1, None))
        self.assertFalse(found.equal)
        with self.assertRaises(ValueError):
            engine_equality.keyed_diff_sql("reference", "candidate", ("pnu",), (), None)

    def test_a_column_null_on_both_sides_is_the_same_value(self):
        rows = [(pnu(1), None, digest("a")), (pnu(2), "b", digest("b"))]
        connection = database(rows, list(reversed(rows)))
        found = engine_equality.compare(runner(connection), "reference", "candidate", ("pnu",), "duckdb", ("document",))
        self.assertEqual((found.same, found.different), (2, 0))
        self.assertTrue(found.equal)

    def test_every_row_on_both_sides_is_either_same_or_different(self):
        reference = [(pnu(1), None, digest("a")), (pnu(2), None, digest("b")), (pnu(3), "c", None), (pnu(4), "d", digest("d"))]
        candidate = [(pnu(1), None, digest("a")), (pnu(2), "x", digest("b")), (pnu(3), "c", None), (pnu(4), "d", None)]
        connection = database(reference, candidate)
        found = engine_equality.compare(runner(connection), "reference", "candidate", ("pnu",), "duckdb", ("document",))
        self.assertEqual((found.same, found.different), (2, 2))
        self.assertEqual(found.same + found.different, found.reference_rows)

    def test_a_number_json_cannot_hold_does_not_stop_the_sample(self):
        self.assertFalse(engine_equality.written_alike('{"a":NaN}', '{"a":1}'))
        self.assertTrue(engine_equality.written_alike('{"a":Infinity}', '{"a":Infinity}'))

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


# A runner process as the publisher's `query-takeover-sides` is one: a statement on stdin, log lines
# and then the rows as one JSON array on stdout. SQLite answers it over the database file argv[1]
# names; the fingerprint's hex-to-number words are DuckDB's, so it is computed as `runner` does.
FAKE_RUNNER = """
import json, sqlite3, sys
sys.path.insert(0, sys.argv[2])
import engine_equality
connection = sqlite3.connect(sys.argv[1])
connection.row_factory = sqlite3.Row
sql = sys.stdin.read()
if "digest_sum" in sql and "COUNT(*) AS rows" in sql:
    table = sql.rsplit(" ", 1)[-1]
    rows, total = engine_equality.fingerprint_of(
        [row[0] for row in connection.execute(f"SELECT row_digest FROM {table}")]).replace("rows:", "").split(";digest_sum:")
    answer = [{"rows": int(rows), "digest_sum": int(total)}]
else:
    answer = [dict(row) for row in connection.execute(sql)]
print('{"level":"INFO","message":"a log line before the rows"}')
print(json.dumps(answer))
"""


class RunnerCommand(unittest.TestCase):
    def test_the_bridge_reaches_the_in_process_verdict(self):
        import contextlib
        import io
        import json
        import shlex
        import tempfile

        jobs = Path(engine_equality.__file__).resolve().parent
        reference, candidate = rows_of({1: "a", 2: "b", 3: "c"}), rows_of({1: "a", 2: "x", 4: "d"})
        expected = engine_equality.compare(runner(database(reference, candidate)), "reference", "candidate",
                                           ("pnu",), "duckdb", ("document",))
        with tempfile.TemporaryDirectory() as work:
            path = Path(work, "sides.db")
            on_disk = sqlite3.connect(path)
            on_disk.executescript("".join(f"CREATE TABLE {name} (pnu TEXT, document TEXT, row_digest TEXT);"
                                          for name in ("reference", "candidate")))
            for name, rows in (("reference", reference), ("candidate", candidate)):
                on_disk.executemany(f"INSERT INTO {name} VALUES (?, ?, ?)", rows)
            on_disk.commit()
            on_disk.close()
            Path(work, "runner.py").write_text(FAKE_RUNNER, encoding="utf-8")
            Path(work, "sides.json").write_text(json.dumps({
                "table": "gold.example_versions", "replaced": "gold.example", "reference_snapshot": "1",
                "candidate_snapshot": "2", "pins": {"silver.example": 7}, "keys": ["pnu"],
                "content": ["document"], "digest": "row_digest"}), encoding="utf-8")
            command = " ".join(shlex.quote(part) for part in (sys.executable, "-I", str(Path(work, "runner.py")),
                                                               str(path), str(jobs)))
            with contextlib.redirect_stdout(io.StringIO()):
                code = engine_equality.main(["--sides", str(Path(work, "sides.json")), "--runner-command", command,
                                             "--job", "gold.example_versions", "--reference-engine", "spark",
                                             "--candidate-engine", "duckdb", "--report", str(Path(work, "verdict.json"))])
            report = json.loads(Path(work, "verdict.json").read_text(encoding="utf-8"))
        self.assertEqual(code, 1)
        for name, value in asdict_of(expected).items():
            self.assertEqual(report[name], value, name)
        self.assertEqual((report["input_pins"], report["candidate_snapshot"], report["keys"]),
                         ({"silver.example": 7}, "2", ["pnu"]))

    def test_a_failing_runner_is_an_error_not_a_verdict(self):
        run = engine_equality.command_runner(f"{sys.executable} -I -c \"import sys; sys.exit('no such view')\"")
        with self.assertRaisesRegex(RuntimeError, "no such view"):
            run("SELECT 1")

    def test_the_file_runs_as_the_takeover_verdict_runs_it(self):
        # gold-takeover-verdict.sh runs `python3 -I -B engine_equality.py`; -I leaves the file's own
        # directory off sys.path, and the first production verdict (2026-11-22) stopped at the import of
        # gold_fingerprint beside it.
        import subprocess

        found = subprocess.run([sys.executable, "-I", "-B", engine_equality.__file__, "--help"],
                               capture_output=True, text=True, check=False)
        self.assertEqual(found.returncode, 0, found.stderr)
        self.assertIn("--sides", found.stdout)


def asdict_of(verdict) -> dict:
    import dataclasses
    return {**dataclasses.asdict(verdict), "equal": verdict.equal}


if __name__ == "__main__":
    unittest.main()
