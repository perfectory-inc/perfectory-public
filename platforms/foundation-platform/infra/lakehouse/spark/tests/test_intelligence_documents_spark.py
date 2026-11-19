"""What the intelligence document jobs owe, on a Spark runtime (root ADR-0593): a document the image's
memory cap ended is read again until its attempts run out, and facts follow the newest text.

Skipped where pyspark is missing; the CI spark-runtime lane runs it. Hashes are synthetic.
"""
import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import intelligence_documents as d  # noqa: E402

PLATFORM = Path(__file__).resolve().parents[4]
EXTRACTION = d.load_extraction(PLATFORM.parent / "intelligence-platform" / "config" / "document-extraction.contract.json")
TEXT_SCHEMA = ("sha256 string, extractor_version string, media_format string, outcome string, page_count int, "
               "ocr_page_count int, char_count long, error string, elapsed_ms long, extracted_at_utc timestamp, run_id string")
FACT_SCHEMA = ("sha256 string, extractor_version string, text_extractor_version string, fact_counts_json string, "
               "unsolved_pages_json string, dropped_json string, model_calls int, elapsed_ms long, "
               "extracted_at_utc timestamp, run_id string")
AT = datetime(2099, 1, 1)


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def sha(n):
    return f"{n:064x}"


def text(n, run, outcome="text", error=None, version="document-text.v1"):
    return (sha(n), version, "pdf", outcome, 1, 0, 10, error, 5, AT, run)


def fact(n, run):
    return (sha(n), "document-fact.v1", "document-text.v1", "{}", "[]", "{}", 0, 5, AT, run)


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class OwedSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("intelligence-documents")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.settings = EXTRACTION["document_text"]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def done(self, rows):
        return {r["sha256"] for r in d.text_done(self.spark.createDataFrame(rows, TEXT_SCHEMA), self.settings).collect()}

    def test_a_memory_kill_is_owed_again_until_its_attempts_run_out(self):
        attempts = self.settings["memory_kill"]["attempts"]
        kill = "memory cap: ocrmypdf exit -9: "
        rows = [
            text(1, "20990101T000000Z"),
            text(2, "20990101T000000Z", "failed", "soffice exit 1: x"),  # the document's own failure
            text(3, "20990101T000000Z", "failed", "ocrmypdf exit -9: "),  # a kill before the mark
            text(4, "20990101T000000Z", "failed", "worker killed by signal 9 (the image's memory cap)"),
            *[text(5, f"2099010{i}T000000Z", "failed", kill) for i in range(1, attempts + 1)],  # out of attempts
            text(6, "20990101T000000Z", "failed", kill), text(6, "20990102T000000Z"),  # read again: done
        ]
        self.assertEqual(self.done(rows), {sha(1), sha(2), sha(5), sha(6)})
        # The one out of attempts is what plan-text names as memory_kill_exhausted.
        kills = d.memory_kills(self.spark.createDataFrame(rows, TEXT_SCHEMA), self.settings).collect()
        self.assertEqual({(r["sha256"], r["attempts"]) for r in kills}, {(sha(3), 1), (sha(4), 1), (sha(5), attempts)})

    def test_facts_follow_the_newest_text(self):
        texts = self.spark.createDataFrame([
            text(1, "20990101T000000Z"),  # facts already of a later run
            text(2, "20990101T000000Z", "failed", "memory cap: "), text(2, "20990103T000000Z"),  # read again
            text(3, "20990101T000000Z"),  # no facts yet
        ], TEXT_SCHEMA)
        facts = self.spark.createDataFrame([fact(1, "20990102T000000Z"), fact(2, "20990102T000000Z")], FACT_SCHEMA)
        owed = d.facts_owed(texts, facts).collect()
        self.assertEqual(sorted((r["sha256"], r["run_id"]) for r in owed),
                         [(sha(2), "20990103T000000Z"), (sha(3), "20990101T000000Z")])


if __name__ == "__main__":
    unittest.main()
