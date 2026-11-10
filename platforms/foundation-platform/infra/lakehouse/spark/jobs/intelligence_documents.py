"""The lakehouse half of the intelligence document jobs (root ADR-0477): what is owed, and the rows a run wrote.

    intelligence_documents.py plan-text   --extraction <contract> --out <dir>
    intelligence_documents.py append-text --extraction <contract> --run-dir <dir> --run-id <id>
    intelligence_documents.py plan-facts  --extraction <contract> --out <dir>
    intelligence_documents.py append-facts --extraction <contract> --run-dir <dir> --run-id <id>

The extraction itself runs in the intelligence image (platforms/intelligence-platform/jobs/
document_text) on local files and never touches the lakehouse; this job is the only thing that
reads its inputs and writes its tables. `--extraction` is the intelligence contract the image
carries (document-extraction.contract.json), so the versions planned are the image's own.

Owed is computed from the tables, never from a stored queue (root ADR-0352): a hash of the
attachment index with no text outcome of this extractor version, newest notice first, up to the
contract's per-run limit; for facts, a hash with a text outcome of the fact job's text version and
no fact outcome of its own version. Appends go pages before outcomes and facts before fact
outcomes: an outcome is the record that a document is done, so a run stopped between the two
leaves the document owed and it is read again whole.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

ATTACHMENT_TABLE = "silver.industrial_complex_notice_attachment"
TEXT_OUTCOME = "intelligence.document_text_outcome"
TEXT_PAGE = "intelligence.document_text_page"
FACT = "intelligence.document_fact"
FACT_OUTCOME = "intelligence.document_fact_outcome"
TABLES = (TEXT_OUTCOME, TEXT_PAGE, FACT, FACT_OUTCOME)
# The table each output of the intelligence contract is written to, and the file a run writes it as.
OUTPUTS = {
    TEXT_OUTCOME: ("document_text_outcome", "document_text_outcome.jsonl"),
    TEXT_PAGE: ("document_text_page", "document_text_page.jsonl"),
    FACT: ("document_fact", "document_fact.jsonl"),
    FACT_OUTCOME: ("document_fact_outcome", "document_fact_outcome.jsonl"),
}
RUN_COLUMN = "run_id"
INDEX_FILE = "index.jsonl"
MEDIA_FILE = "media.tsv"
SUMMARY_PREFIX = "intelligence-documents-summary-json "


def load_extraction(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def table_columns(extraction: Mapping[str, Any], table: str) -> list[str]:
    """The lakehouse columns of `table`: the intelligence contract's, then the run that appended them."""
    output, _ = OUTPUTS[table]
    declared = extraction["outputs"][output]
    if declared["table"] != table:
        raise ValueError(f"the intelligence contract writes {output} to {declared['table']}, not {table}")
    return [*declared["columns"], RUN_COLUMN]


def readable_formats(extraction: Mapping[str, Any]) -> list[str]:
    """The formats whose bytes the text job reads; the rest are counted unsupported without them."""
    settings = extraction["document_text"]
    return [*settings["pdf_formats"], *settings["converted_formats"]]


def media_key(prefix: str, sha256: str, media_format: str) -> str:
    """Where the shared content-addressed store keeps a file (root ADR-0298)."""
    if len(sha256) != 64 or any(c not in "0123456789abcdef" for c in sha256):
        raise ValueError(f"not a SHA-256 hex digest: {sha256!r}")
    if not media_format.isalnum():
        raise ValueError(f"not a media format: {media_format!r}")
    return f"{prefix.strip('/')}/sha256/{sha256}.{media_format}"


def media_prefix(platform: Path, extraction: Mapping[str, Any]) -> str:
    """The store's prefix, read through the pointer the intelligence contract names into foundation's."""
    pointer = extraction["inputs"]["media_store"]["object_prefix_from"]
    relative, _, fragment = pointer.partition("#")
    relative = relative.removeprefix("platforms/foundation-platform/")
    value: Any = json.loads((platform / relative).read_text(encoding="utf-8"))
    for part in fragment.strip("/").split("/"):
        value = value[part]
    return str(value)


def utc(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def run_rows(path: Path, columns: Sequence[str], run_id: str, timestamps: Iterable[str]) -> list[tuple]:
    """A run file's rows in the table's column order, stamped with the run; refuses a row whose
    columns are not the contract's, so a newer image cannot write what the table does not hold."""
    if not path.exists():
        return []
    stamped = set(timestamps)
    rows = []
    declared = list(columns[:-1])
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        if list(row) != declared:
            raise ValueError(f"{path.name}:{number} has columns {list(row)}, the contract {declared}")
        row[RUN_COLUMN] = run_id
        rows.append(tuple(utc(row[c]) if c in stamped else row[c] for c in columns))
    return rows


def summary_line(summary: Mapping[str, Any]) -> None:
    print(SUMMARY_PREFIX + json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)


# --- Spark ------------------------------------------------------------------------------------


def session(name: str, catalog: str) -> Any:
    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from pyspark.sql import SparkSession  # noqa: PLC0415

    assert_catalog_env()
    builder = SparkSession.builder.appName(f"intelligence-documents-{name}").config("spark.sql.session.timeZone", "UTC")
    return apply_catalog_settings(builder, catalog).getOrCreate()


def qualified(catalog: str, table: str) -> str:
    namespace, name = table.split(".", 1)
    return f"`{catalog}`.`{namespace}`.`{name}`"


def ensure_tables(spark: Any, catalog: str) -> None:
    from platform_contracts import ensure_contract_table, load_lakehouse_contract  # noqa: PLC0415

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`intelligence`")
    for table in TABLES:
        ensure_contract_table(spark, qualified(catalog, table), load_lakehouse_contract(table))


def done_hashes(spark: Any, catalog: str, table: str, version: str) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415

    return spark.table(qualified(catalog, table)).where(F.col("extractor_version") == version).select("sha256").distinct()


def plan_text(spark: Any, args: argparse.Namespace, extraction: Mapping[str, Any]) -> dict[str, Any]:
    from platform_contracts import current_rows, load_lakehouse_contract  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    settings = extraction["document_text"]
    if not spark.catalog.tableExists(f"{args.catalog}.{ATTACHMENT_TABLE}"):
        # Its Silver load creates it with its first object: until then nothing is owed.
        return {"step": "plan-text", "extractor_version": settings["extractor_version"], "owed": 0, "planned": 0,
                "bytes_to_fetch": 0, "input": "absent"}
    attachments = current_rows(spark.table(qualified(args.catalog, ATTACHMENT_TABLE)), load_lakehouse_contract(ATTACHMENT_TABLE))
    # One document per hash, however many notices attach it; the newest notice orders it.
    documents = attachments.groupBy("sha256", "media_format").agg(
        F.max("notice_sequence").alias("notice_sequence"), F.max("size_bytes").alias("size_bytes"))
    owed = documents.join(done_hashes(spark, args.catalog, TEXT_OUTCOME, settings["extractor_version"]), "sha256", "left_anti").cache()
    total = owed.count()
    planned = owed.orderBy(F.col("notice_sequence").desc(), "sha256").limit(settings["per_run_limit"]).collect()
    owed.unpersist()
    prefix = media_prefix(args.platform, extraction)
    readable = set(readable_formats(extraction))
    args.out.mkdir(parents=True, exist_ok=True)
    columns = extraction["inputs"]["attachment_index"]["columns"]
    with (args.out / INDEX_FILE).open("w", encoding="utf-8") as index, (args.out / MEDIA_FILE).open("w", encoding="utf-8") as media:
        for row in planned:
            index.write(json.dumps({c: row[c] for c in columns}, ensure_ascii=False) + "\n")
            if row["media_format"] in readable:
                media.write(f"{media_key(prefix, row['sha256'], row['media_format'])}\t{row['sha256']}.{row['media_format']}\n")
    return {"step": "plan-text", "extractor_version": settings["extractor_version"], "owed": total, "planned": len(planned),
            "bytes_to_fetch": sum(r["size_bytes"] or 0 for r in planned if r["media_format"] in readable)}


def plan_facts(spark: Any, args: argparse.Namespace, extraction: Mapping[str, Any]) -> dict[str, Any]:
    from pyspark.sql import functions as F  # noqa: PLC0415

    settings = extraction["document_fact"]
    text_version = settings["text_extractor_version"]
    outcomes = spark.table(qualified(args.catalog, TEXT_OUTCOME)).where(F.col("extractor_version") == text_version)
    owed = outcomes.join(done_hashes(spark, args.catalog, FACT_OUTCOME, settings["extractor_version"]), "sha256", "left_anti").cache()
    total = owed.count()
    planned = owed.orderBy(F.col("extracted_at_utc").desc(), "sha256").limit(settings["per_run_limit"]).cache()
    hashes = [r["sha256"] for r in planned.select("sha256").collect()]
    args.out.mkdir(parents=True, exist_ok=True)
    text_columns = extraction["outputs"]["document_text_outcome"]["columns"]
    page_columns = extraction["outputs"]["document_text_page"]["columns"]
    pages = (spark.table(qualified(args.catalog, TEXT_PAGE))
             .where((F.col("extractor_version") == text_version) & F.col("sha256").isin(hashes))
             .orderBy("sha256", "page_number"))
    with (args.out / "document_text_outcome.jsonl").open("w", encoding="utf-8") as handle:
        for row in planned.collect():
            values = row.asDict()
            values["extracted_at_utc"] = values["extracted_at_utc"].strftime("%Y-%m-%dT%H:%M:%SZ")
            handle.write(json.dumps({c: values[c] for c in text_columns}, ensure_ascii=False) + "\n")
    page_count = 0
    with (args.out / "document_text_page.jsonl").open("w", encoding="utf-8") as handle:
        for row in pages.toLocalIterator():
            handle.write(json.dumps({c: row[c] for c in page_columns}, ensure_ascii=False) + "\n")
            page_count += 1
    # The vision re-read draws the OCR'd pages of a PDF: only those documents' bytes are fetched.
    ocr = (pages.where(F.col("method") == "ocr").select("sha256").distinct()
           .join(planned.select("sha256", "media_format"), "sha256")
           .where(F.col("media_format").isin(settings["vision_formats"])).collect())
    prefix = media_prefix(args.platform, extraction)
    with (args.out / MEDIA_FILE).open("w", encoding="utf-8") as media:
        for row in ocr:
            media.write(f"{media_key(prefix, row['sha256'], row['media_format'])}\t{row['sha256']}.{row['media_format']}\n")
    planned.unpersist()
    owed.unpersist()
    return {"step": "plan-facts", "extractor_version": settings["extractor_version"], "owed": total,
            "planned": len(hashes), "pages": page_count, "documents_to_fetch": len(ocr)}


def append(spark: Any, args: argparse.Namespace, extraction: Mapping[str, Any], tables: Sequence[str]) -> dict[str, Any]:
    """Each table's rows of the run, in the order given: the outcome table last."""
    from lakehouse_ingest import append_batch_once  # noqa: PLC0415
    from platform_contracts import load_lakehouse_contract, spark_sql_type  # noqa: PLC0415

    counts: dict[str, Any] = {}
    for table in tables:
        contract = load_lakehouse_contract(table)
        columns = table_columns(extraction, table)
        if columns != [c["name"] for c in contract["columns"]]:
            raise ValueError(f"{table}: the image writes {columns}, the lakehouse contract holds "
                             f"{[c['name'] for c in contract['columns']]}")
        stamped = [c["name"] for c in contract["columns"] if c["logical_type"] == "timestamp"]
        rows = run_rows(args.run_dir / OUTPUTS[table][1], columns, args.run_id, stamped)
        counts[table] = {"rows": len(rows), "appended": False}
        if not rows:
            continue
        ddl = ", ".join(f"`{c['name']}` {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
        frame = spark.createDataFrame(rows, ddl)
        result = append_batch_once(spark, frame, columns, qualified(args.catalog, table), table)
        counts[table]["appended"] = result["appended"]
    return {"step": "append", "run_id": args.run_id, "tables": counts}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("plan-text", "append-text", "plan-facts", "append-facts"))
    parser.add_argument("--extraction", type=Path, required=True, help="the intelligence image's document-extraction.contract.json")
    parser.add_argument("--platform", type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    if args.command.startswith("plan") and args.out is None:
        parser.error("plan needs --out")
    if args.command.startswith("append") and (args.run_dir is None or not args.run_id):
        parser.error("append needs --run-dir and --run-id")
    extraction = load_extraction(args.extraction)
    spark = session(args.command, args.catalog)
    try:
        ensure_tables(spark, args.catalog)
        if args.command == "plan-text":
            summary = plan_text(spark, args, extraction)
        elif args.command == "plan-facts":
            summary = plan_facts(spark, args, extraction)
        elif args.command == "append-text":
            summary = append(spark, args, extraction, (TEXT_PAGE, TEXT_OUTCOME))
        else:
            summary = append(spark, args, extraction, (FACT, FACT_OUTCOME))
    finally:
        spark.stop()
    summary_line(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
