"""The lakehouse half of the intelligence document jobs (root ADR-0477): what is owed, and the rows a run wrote.

    intelligence_documents.py plan-text   --extraction <contract> --out <dir>
    intelligence_documents.py append-text --extraction <contract> --run-dir <dir> --run-id <id>
    intelligence_documents.py plan-facts  --extraction <contract> --out <dir>
    intelligence_documents.py append-facts --extraction <contract> --run-dir <dir> --run-id <id>
    intelligence_documents.py fold-complex-facts --extraction <contract> --run-id <id> [--all]

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

`fold-complex-facts` is foundation's own read of the facts (root ADR-0477 §1: foundation decides
what a screen shows): for the complexes whose attachments got facts in the run, the facts a panel
may state, one row per complex appended to gold.complex_document_facts. Only the kinds that passed
the hand-checked gate are shown (§5), each from the newest notice that states it, with the page it
was read on. `--all` refolds every complex: the declared exception for a changed gate or fact
version, and the run's summary says which.
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
DETAIL_TABLE = "silver.industrial_complex_notice_detail"
COMPLEX_FACTS = "gold.complex_document_facts"
COMPLEX_FACTS_SCHEMA = "complex-document-facts.v1"
# The kinds a complex's panel states. A parcel_lot row is one 토지조서 lot: which parcels the
# complex holds is foundation's place_id resolver's to decide (ADR-0477 §2), not a panel line.
PANEL_KINDS = ("land_use_area", "invited_industry", "sale_price")
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


def admitted(extraction: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    """The (fact_kind, method, page_method) a screen may show: checked by hand on at least the gate's
    sample with at least its precision (root ADR-0477 §5). What was never measured is not shown."""
    gate = extraction["outputs"]["document_fact"]["accuracy_gate"]
    return sorted({(m["fact_kind"], m["method"], m["page_method"]) for m in gate["measured"]
                   if m["checked"] >= gate["hand_checked_sample_min"]
                   and m["correct"] >= gate["min_precision"] * m["checked"]})


def newest_first(row: Mapping[str, Any]) -> tuple:
    """Newest notice first (one with no date last), then the attachment the notice lists first."""
    notice_date = row.get("notice_date")
    return (notice_date is None, -(notice_date.toordinal() if notice_date else 0), -row["notice_sequence"],
            row.get("file_sequence") or 0, row["sha256"])


def complex_facts(rows: Iterable[Mapping[str, Any]], gate: Iterable[tuple[str, str, str]]) -> dict[str, dict[str, Any]]:
    """{official_complex_code: document} from fact rows joined to their attachment, notice and page.

    Per kind one source: the newest notice's attachment that states the kind, with all its admitted
    facts in order. An older notice's table is the plan a later one changed; showing both would
    state two areas for one 용지."""
    allowed = set(gate)
    by_kind: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        if row["fact_kind"] in PANEL_KINDS and (row["fact_kind"], row["method"], row["page_method"]) in allowed:
            by_kind.setdefault((row["official_complex_code"], row["fact_kind"]), []).append(row)
    documents: dict[str, dict[str, Any]] = {}
    for (code, kind), found in sorted(by_kind.items()):
        source = min(found, key=newest_first)
        facts = sorted((r for r in found if (r["sha256"], r["notice_sequence"]) == (source["sha256"], source["notice_sequence"])),
                       key=lambda r: r["fact_index"])
        notice_date = source.get("notice_date")
        documents.setdefault(code, {"schema": COMPLEX_FACTS_SCHEMA, "kinds": {}})["kinds"][kind] = {
            "source": {
                "notice_sequence": source["notice_sequence"],
                "notice_number": source.get("notice_number"),
                "notice_title": source.get("notice_title"),
                "notice_date": notice_date.isoformat() if notice_date else None,
                "sha256": source["sha256"],
                "media_format": source["media_format"],
                "file_title": source.get("file_title"),
            },
            "facts": [{"value": json.loads(r["value_json"]), "page_number": r["page_number"], "quote": r["quote"],
                       "method": r["method"], "page_method": r["page_method"], "check": r["check"]} for r in facts],
        }
    return documents


def complex_fact_rows(documents: Mapping[str, Mapping[str, Any]], codes: Iterable[str], extraction: Mapping[str, Any],
                      gate: Sequence[tuple[str, str, str]], run_id: str, now: datetime) -> list[tuple]:
    """gold.complex_document_facts rows for `codes`, in its column order. A complex in scope with no
    admitted fact gets a row with none, so a fact the gate no longer admits leaves its panel."""
    gate_json = json.dumps([list(g) for g in gate], ensure_ascii=False)
    version = extraction["document_fact"]["extractor_version"]
    rows = []
    for code in sorted(set(codes)):
        document = documents.get(code) or {"schema": COMPLEX_FACTS_SCHEMA, "kinds": {}}
        count = sum(len(kind["facts"]) for kind in document["kinds"].values())
        rows.append((code, json.dumps(document, ensure_ascii=False, sort_keys=True), count, version, gate_json, now, run_id))
    return rows


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


def fold_complex_facts(spark: Any, args: argparse.Namespace, extraction: Mapping[str, Any]) -> dict[str, Any]:
    """Append the panel facts of the complexes this run's facts belong to (every complex with `--all`)."""
    from datetime import timezone  # noqa: PLC0415

    from lakehouse_ingest import append_batch_once  # noqa: PLC0415
    from platform_contracts import (  # noqa: PLC0415
        column_names, current_rows, ensure_contract_table, load_lakehouse_contract, spark_sql_type)
    from pyspark.sql import functions as F  # noqa: PLC0415

    settings = extraction["document_fact"]
    gate = admitted(extraction)
    gate_json = json.dumps([list(g) for g in gate], ensure_ascii=False)
    contract = load_lakehouse_contract(COMPLEX_FACTS)
    target = qualified(args.catalog, COMPLEX_FACTS)
    ensure_contract_table(spark, target, contract)
    held = current_rows(spark.table(target), contract)
    # Rows folded under another gate or fact version say what a panel may no longer show (or not yet):
    # the declared full refold (root ADR-0477 §3), named in the summary.
    stale = held.where((F.col("accuracy_gate_json") != gate_json)
                       | (F.col("fact_extractor_version") != settings["extractor_version"])).limit(1).count()
    whole = args.all or stale > 0
    current = lambda table: current_rows(spark.table(qualified(args.catalog, table)), load_lakehouse_contract(table))  # noqa: E731
    attachments = current(ATTACHMENT_TABLE)
    facts = current(FACT).where((F.col("extractor_version") == settings["extractor_version"])
                                & F.col("fact_kind").isin(list(PANEL_KINDS)))
    if whole:
        found = attachments.join(facts.select("sha256").distinct(), "sha256").select("official_complex_code").distinct()
        codes = [r[0] for r in found.union(held.select("official_complex_code")).collect()]
    else:
        ran = spark.table(qualified(args.catalog, FACT)).where(F.col("run_id") == args.run_id).select("sha256").distinct()
        codes = [r[0] for r in attachments.join(ran, "sha256").select("official_complex_code").distinct().collect()]
    codes = sorted({c for c in codes if c})
    summary = {"step": "fold-complex-facts", "run_id": args.run_id, "whole": whole,
               "whole_reason": ("--all" if args.all else "gate or fact version changed") if whole else None,
               "admitted": [list(g) for g in gate]}
    if not codes:
        return {**summary, "complexes": 0, "with_facts": 0, "facts": 0, "appended": False}
    in_scope = attachments.where(F.col("official_complex_code").isin(codes)).select(
        "official_complex_code", "notice_sequence", "sha256", "file_sequence", "file_title", "media_format")
    notices = current(DETAIL_TABLE).select("notice_sequence", "notice_number", "notice_title", "notice_date")
    pages = current(TEXT_PAGE).select("sha256", F.col("extractor_version").alias("text_extractor_version"),
                                      "page_number", F.col("method").alias("page_method"))
    joined = (facts.join(in_scope, "sha256").join(notices, "notice_sequence", "left")
              .join(pages, ["sha256", "text_extractor_version", "page_number"], "left")
              .select("official_complex_code", "notice_sequence", "notice_number", "notice_title", "notice_date",
                      "sha256", "file_sequence", "file_title", "media_format", "fact_kind", "fact_index",
                      "value_json", "page_number", "quote", "method", "page_method", "check"))
    documents = complex_facts((r.asDict() for r in joined.toLocalIterator()), gate)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    rows = complex_fact_rows(documents, codes, extraction, gate, args.run_id, now)
    ddl = ", ".join(f"`{c['name']}` {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
    result = append_batch_once(spark, spark.createDataFrame(rows, ddl), column_names(contract), target, COMPLEX_FACTS)
    return {**summary, "complexes": len(rows), "with_facts": len(documents), "facts": sum(r[2] for r in rows),
            "appended": result["appended"]}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("plan-text", "append-text", "plan-facts", "append-facts", "fold-complex-facts"))
    parser.add_argument("--extraction", type=Path, required=True, help="the intelligence image's document-extraction.contract.json")
    parser.add_argument("--platform", type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--all", action="store_true", help="fold-complex-facts: every complex, not only this run's")
    args = parser.parse_args(argv)
    if args.command.startswith("plan") and args.out is None:
        parser.error("plan needs --out")
    if args.command.startswith("append") and (args.run_dir is None or not args.run_id):
        parser.error("append needs --run-dir and --run-id")
    if args.command == "fold-complex-facts" and not args.run_id:
        parser.error("fold-complex-facts needs --run-id")
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
        elif args.command == "fold-complex-facts":
            summary = fold_complex_facts(spark, args, extraction)
        else:
            summary = append(spark, args, extraction, (FACT, FACT_OUTCOME))
    finally:
        spark.stop()
    summary_line(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
