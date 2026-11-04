"""Load the other public bulk files of the data.go.kr file data into one Silver table each (root ADR-0401 §3–4).

The collector (`collect-data-go-kr-file-data`) lands each new edition in Bronze under its own source.
What a load owes is decided from R2 alone (`plan-silver-load-objects`, one plan per table). For each
planned file, oldest first, this job:

1. reads its data (the CSV, or every zip member the contract's name pattern matches) strictly in the
   contract's encoding, refuses a header that is not the contract's exactly, and stages a UTF-8 copy;
2. builds one row per data line: `row_key` (the provider key's values joined by U+001F, or the
   SHA-256 of the row's cells joined so, ADR-0397) and `provider_fields_json` (every cell under its
   header name); refuses an empty or repeated provider key, an edition under the row floor or shrunk
   past its bound against the table's current rows;
3. links each row by the contract's `place_link`: an address through `address_pnu` (지번, then
   도로명), the provider's 19-digit lot code through the place-identity resolver (never copied),
   도·시군구·읍면동 names to a 법정동 code, or a 행정동 code kept for the linkage table to bridge;
4. appends only the keys that are new or changed and a removal marker per key the edition no longer
   holds (`silver_release_compare.release_changes`), in a commit that records the file. A file whose
   edition is one period (`row_key.period_column`, the monthly 인구·세대수) is compared only within the
   periods it holds, so the months pile up.

Then it follows code changes (ADR-0329 §3, ADR-0401 §4): each table records the snapshot of
`reference.legal_dong_code_change` its links were made against (a commit recording
`follow:reference.legal_dong_code_change@<snapshot>@<UTC instant>`). When the change table has moved since, the
codes `views.changed_codes` names select the current rows under them (and the unresolved rows); only
those are linked again, and only the ones whose link changed are appended.

`parse --file <path> --dataset <slug>` reads one file without Spark. The last stdout line of `load`
is `data-go-kr-file-data-summary-json {json}`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

JOB_NAME = "data_go_kr_file_data"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "data-go-kr-file-data.contract.json"
PLAN_SCHEMA = "foundation-platform.silver_load_objects.v1"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SEPARATOR = "\x1f"
OWN_JOB = "registered_factory"  # its own job and table (data_go_kr_registered_factory.py)
CHANGE_TABLE = "reference.legal_dong_code_change"
FOLLOW_PREFIX = f"follow:{CHANGE_TABLE}@"
LINK_COLUMNS = {
    "parcel": ("pnu", "pnu_basis", "pnu_unresolved_reason", "legal_dong_code"),
    "area": ("legal_dong_code", "administrative_dong_code", "legal_dong_unresolved_reason"),
}
PARCEL_LINKS = {"address_pnu", "place_identity"}
# The links a 법정동 code change can move; the 행정동 ones are bridged at read time by the linkage table.
FOLLOWED_LINKS = {"address_pnu", "place_identity", "legal_dong_by_names"}
LOT_CODE = re.compile(r"^\d{19}$")


def load_source_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def files(contract: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """The files this job loads, by dataset slug, with the slug in each."""

    return {slug: {"dataset_slug": slug, **entry} for slug, entry in contract["files"].items() if slug != OWN_JOB}


def table_of(slug: str) -> str:
    return f"silver.{slug}"


def source_slug(slug: str) -> str:
    """The Bronze source of a file (root ADR-0014 `{provider}__{dataset}`)."""

    return f"datagokr__{slug}"


def link_kind(entry: Mapping[str, Any]) -> str:
    return "parcel" if entry["place_link"]["kind"] in PARCEL_LINKS else "area"


def data_spec(entry: Mapping[str, Any]) -> Mapping[str, Any]:
    """Where the encoding, header and bounds are: the zip's member rule, or the CSV entry itself."""

    return entry["member"] if entry["format"] == "zip" else entry


# --- reading (plain Python, tested without Spark) -------------------------------------------------


def _member_name(info: zipfile.ZipInfo) -> str:
    """A member's name as written: zips from Windows tools store UTF-8 names without the flag."""

    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode("cp437").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return info.filename


def data_streams(path: Path, entry: Mapping[str, Any]) -> Iterator[tuple[str, io.BufferedIOBase]]:
    """(name, bytes) of every data part of one file: the CSV itself, or each matching zip member."""

    if entry["format"] == "csv":
        with path.open("rb") as stream:
            yield path.name, stream
        return
    pattern = re.compile(entry["member"]["name_pattern"])
    with zipfile.ZipFile(path) as archive:
        members = [info for info in archive.infolist() if pattern.match(_member_name(info))]
        if not members or (len(members) != 1 and not entry["member"].get("every_match")):
            raise ValueError(f"expected {'members' if entry['member'].get('every_match') else 'one member'} "
                             f"matching {entry['member']['name_pattern']}, found {len(members)}")
        for info in members:
            with archive.open(info) as stream:
                yield _member_name(info), stream


def stage(path: Path, entry: Mapping[str, Any], out_dir: Path) -> list[Path]:
    """UTF-8 copies of every data part, each refused unless it decodes strictly, its header is the
    contract's exactly and every row has the header's width. A copy has no header; each row starts
    with its line number in its part (the first data line is 2), and blank rows are left out."""

    spec = data_spec(entry)
    header = list(spec["header"])
    if len(set(header)) != len(header):
        raise ValueError(f"{entry['dataset_slug']}: the contract header repeats a name")
    out_dir.mkdir(parents=True, exist_ok=True)
    staged = []
    for number, (name, stream) in enumerate(data_streams(path, entry)):
        text = io.TextIOWrapper(stream, encoding=spec["encoding"], errors="strict", newline="")
        first = text.readline()
        found = [cell.strip().lstrip("\ufeff") for cell in next(csv.reader([first]), [])]
        if found != header:
            raise ValueError(f"{entry['dataset_slug']} {name}: the header is not the contract's")
        target = out_dir / f"{number:03d}.csv"
        with target.open("w", encoding="utf-8", newline="") as out:
            writer = csv.writer(out, lineterminator="\n")
            # A byte the encoding cannot read refuses here.
            for line, row in enumerate(csv.reader(text), start=2):
                if not any(cell.strip() for cell in row):
                    continue
                if len(row) != len(header):
                    raise ValueError(f"{entry['dataset_slug']} {name}: row {line} has {len(row)} cells, not {len(header)}")
                writer.writerow([line, *row])
        staged.append(target)
    return staged


def row_key(cells: Sequence[str], header: Sequence[str], rule: Mapping[str, Any]) -> str:
    """The plain-Python twin of the Spark key (`keyed`): the provider key's values, or the row's SHA-256."""

    if rule["kind"] == "sha256_of_row":
        return hashlib.sha256(SEPARATOR.join(cells).encode("utf-8")).hexdigest()
    return SEPARATOR.join(cells[header.index(column)] for column in rule["columns"])


def check_bounds(rows: int, previous: int | None, entry: Mapping[str, Any]) -> None:
    bounds = data_spec(entry)["bounds"]
    if rows < bounds["min_rows"]:
        raise ValueError(f"{entry['dataset_slug']}: {rows} rows is under the floor of {bounds['min_rows']}")
    if previous and rows < previous * (1 - bounds["max_shrink_share"]):
        raise ValueError(f"{entry['dataset_slug']}: {rows} rows shrank past {bounds['max_shrink_share']:.0%} "
                         f"of the {previous} current rows")


def names_code(names: Sequence[str], prefixes: Mapping[str, str],
               dongs: Mapping[tuple[str, str], str]) -> tuple[str | None, str | None]:
    """(법정동 code, None) for 도·시군구·읍면동 names, or (None, why not)."""

    import address_pnu  # noqa: PLC0415 - not at the top: `tables` runs with python3 -I on the host

    words = [word.strip() for word in names if word and word.strip()]
    if len(words) < 2:
        return None, "no_address"
    sido = address_pnu.sido_code(words[0], prefixes)
    if sido is None:
        return None, "unparsed"
    code = dongs.get((sido, " ".join(words[1:])))
    return (code, None) if code else (None, "no_legal_dong")


def planned_objects(plan_dirs: Sequence[Path], slugs: Mapping[str, str]) -> list[dict[str, Any]]:
    """Every file the plans fetched, in plan order (oldest first), with its dataset and local file."""

    objects = []
    for plan_dir in plan_dirs:
        plan = json.loads((plan_dir / "plan.json").read_text(encoding="utf-8"))
        if plan.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"{plan_dir}: not a {PLAN_SCHEMA} plan")
        for item in plan["objects"]:
            if item["source_slug"] not in slugs:
                raise ValueError(f"{item['object_key']}: {item['source_slug']} is not a source this job loads")
            objects.append({"object_key": item["object_key"], "dataset_slug": slugs[item["source_slug"]],
                            "path": plan_dir / item["local_path"]})
    return objects


def snapshot_id_of(object_key: str) -> str:
    """The edition id: the Bronze leaf without its extension (the dataset and the portal file id)."""

    return object_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]


def follow_record(snapshot: int, at: datetime) -> str:
    """The record of one linking against a change-table snapshot; the instant orders the records
    (snapshot ids are not ordered)."""

    return f"{FOLLOW_PREFIX}{snapshot}@{at:%Y%m%dT%H%M%SZ}"


def last_followed(recorded: Sequence[str]) -> int | None:
    """The change-table snapshot the table's links were last made against, from its commit records."""

    found = []
    for key in recorded:
        snapshot, _, at = key[len(FOLLOW_PREFIX):].partition("@")
        if key.startswith(FOLLOW_PREFIX) and snapshot.isdigit() and re.fullmatch(r"\d{8}T\d{6}Z", at):
            found.append((at, int(snapshot)))
    return max(found)[1] if found else None


# --- Spark ----------------------------------------------------------------------------------------


def staged_rows(spark: Any, paths: Sequence[Path], entry: Mapping[str, Any]) -> Any:
    """(row_key, provider_fields_json, source_line_number) of every data line of the staged copies."""

    from pyspark.sql import functions as F, types as T  # noqa: PLC0415

    header = list(data_spec(entry)["header"])
    names = [f"c{index}" for index in range(len(header))]
    schema = T.StructType([T.StructField("source_line_number", T.IntegerType()),
                           *(T.StructField(name, T.StringType()) for name in names)])
    frame = (spark.read.option("header", False).option("multiLine", True).option("mode", "FAILFAST")
             .option("encoding", "UTF-8").schema(schema).csv([str(p) for p in paths]))
    cells = [F.coalesce(F.col(name), F.lit("")) for name in names]
    fields = F.to_json(F.struct(*(cell.alias(title) for cell, title in zip(cells, header))))
    rule = entry["row_key"]
    if rule["kind"] == "sha256_of_row":
        key = F.sha2(F.concat_ws(SEPARATOR, *cells), 256)
    else:
        key = F.concat_ws(SEPARATOR, *(cells[header.index(column)] for column in rule["columns"]))
    return frame.select(key.alias("row_key"), fields.alias("provider_fields_json"), "source_line_number")


def field(header_name: str) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415

    return F.from_json("provider_fields_json", "map<string,string>")[header_name]


class LinkInputs:
    """What the links read, read once per run and only when a table needs it."""

    def __init__(self, spark: Any, catalog: str, contract: Mapping[str, Any]) -> None:
        self.spark, self.catalog = spark, catalog
        self.prefixes = contract["files"][OWN_JOB]["pnu_resolution"]["sido_prefixes"]
        self._dongs = self._lakehouse = self._pairs = None

    @property
    def dongs(self) -> dict[tuple[str, str], str]:
        import address_pnu  # noqa: PLC0415

        if self._dongs is None:
            self._dongs = address_pnu.legal_dong_index(address_pnu.current_legal_dongs(self.spark, self.catalog))
        return self._dongs

    @property
    def lakehouse(self) -> tuple[Any, Any]:
        import address_pnu  # noqa: PLC0415

        if self._lakehouse is None:
            self._lakehouse = address_pnu.lakehouse_inputs(self.spark, self.catalog)
        return self._lakehouse

    @property
    def pairs(self) -> tuple[dict[str, str], set[str]]:
        if self._pairs is None:
            import place_identity_resolver  # noqa: PLC0415

            self._pairs = place_identity_resolver.pairs_from_lakehouse(self.spark, self.catalog)
        return self._pairs


def link(frame: Any, entry: Mapping[str, Any], inputs: LinkInputs) -> Any:
    """``frame`` (row_key, provider_fields_json, …) with the table's link columns, by `place_link`."""

    import address_pnu  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    spark = frame.sparkSession
    place = entry["place_link"]
    kind = place["kind"]
    none = F.lit(None).cast("string")
    if kind == "place_identity":
        import place_identity_resolver  # noqa: PLC0415

        code = field(place["provider_pnu_column"])
        offered = frame.withColumn("pnu", F.when(code.rlike(LOT_CODE.pattern), code))
        remap, ambiguous = inputs.pairs
        resolved = place_identity_resolver.resolve(offered, remap, ambiguous, inputs.lakehouse[0])
        return (resolved
                .withColumn("pnu_basis", F.when(F.col("pnu").isNotNull(), F.col("place_identity_grade")))
                .withColumn("legal_dong_code", F.substring("pnu", 1, 10))
                .withColumn("legal_dong_code", F.when(F.col("pnu").isNotNull(), F.col("legal_dong_code")))
                .drop("place_identity_grade"))
    if kind == "address_pnu":
        rows = frame.select("row_key", field(place["address_column"]).alias("_address")).collect()
        offered = []
        for number, row in enumerate(rows):
            offer = address_pnu.candidates(row["_address"] or "", inputs.prefixes, inputs.dongs)
            road = offer["road"]
            offered.append((number, row["row_key"], offer["jibun_pnu"], offer["legal_dong_code"], offer["reason"],
                            road.sido_code if road else None, road.sigungu if road else None,
                            road.road if road else None, road.main_no if road else None,
                            road.sub_no if road else None))
        offers = spark.createDataFrame(offered, "row_id int, row_key string, jibun_pnu string, legal_dong_code string, "
                                                "reason string, road_sido string, road_sigungu string, road_name string, "
                                                "road_main_no int, road_sub_no int")
        resolved = address_pnu.resolve(offers, *inputs.lakehouse).select(
            "row_key", "pnu", "pnu_basis", "pnu_unresolved_reason",
            F.coalesce(F.col("legal_dong_code"), F.substring("pnu", 1, 10)).alias("legal_dong_code"))
        return frame.join(resolved, "row_key", "left")
    if kind == "legal_dong_by_names":
        rows = frame.select("row_key", *(field(name).alias(f"_n{i}") for i, name in enumerate(place["names"]))).collect()
        found = [(row["row_key"], *names_code([row[f"_n{i}"] for i in range(len(place["names"]))],
                                               inputs.prefixes, inputs.dongs)) for row in rows]
        codes = spark.createDataFrame(found, "row_key string, legal_dong_code string, legal_dong_unresolved_reason string")
        return frame.join(codes, "row_key", "left").withColumn("administrative_dong_code", none)
    if kind == "adm_dong_via_linkage":
        return (frame.withColumn("legal_dong_code", none)
                .withColumn("administrative_dong_code", field(place["code_column"]))
                .withColumn("legal_dong_unresolved_reason", none))
    if kind == "is_the_bridge":
        return (frame.withColumn("legal_dong_code", field("법정동코드"))
                .withColumn("administrative_dong_code", field("행정동코드"))
                .withColumn("legal_dong_unresolved_reason", none))
    raise ValueError(f"{entry['dataset_slug']}: unknown place_link kind {kind!r}")


def content_columns(entry: Mapping[str, Any]) -> list[str]:
    """What a change is: the raw row and its link (a re-link that moves a PNU is a change)."""

    return ["provider_fields_json", *LINK_COLUMNS[link_kind(entry)]]


def change_snapshot(spark: Any, catalog: str) -> int | None:
    table = f"`{catalog}`.`reference`.`legal_dong_code_change`"
    if not spark.catalog.tableExists(f"{catalog}.{CHANGE_TABLE}"):
        return None
    row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return int(row["snapshot_id"]) if row else None


def changed_prefixes(spark: Any, catalog: str, before: int, after: int) -> set[str] | None:
    """The PNU prefixes under every code that differs between two snapshots of the change table, or
    None when the earlier snapshot has expired (then every row is linked again)."""

    import legal_dong_code_change_views as views  # noqa: PLC0415

    table = f"`{catalog}`.`reference`.`legal_dong_code_change`"
    columns = ("kind", "old_code", "new_code", "level")
    try:
        rows = [[row.asDict() for row in spark.sql(f"SELECT {', '.join(columns)} FROM {table} VERSION AS OF {snapshot}")
                 .collect()] for snapshot in (before, after)]
    except Exception as error:  # noqa: BLE001 - an expired snapshot is a full re-link, never a skipped one
        print(f"{JOB_NAME}: change table snapshot {before} unreadable ({type(error).__name__}); linking every row again",
              file=sys.stderr)
        return None
    return {views.code_prefix(code) for code in views.changed_codes(rows[0], rows[1], columns)}


def follow_rows(current: Any, entry: Mapping[str, Any], prefixes: set[str] | None) -> Any:
    """The current rows a code change can move: under a changed code, or not linked at all."""

    from pyspark.sql import functions as F  # noqa: PLC0415

    unlinked = F.col("legal_dong_code").isNull()
    if prefixes is None:
        return current
    under = F.lit(False)
    for digits in sorted({len(p) for p in prefixes}):
        under = under | F.substring("legal_dong_code", 1, digits).isin(sorted(p for p in prefixes if len(p) == digits))
    if link_kind(entry) == "parcel":
        for digits in sorted({len(p) for p in prefixes}):
            under = under | F.substring("pnu", 1, digits).isin(sorted(p for p in prefixes if len(p) == digits))
    return current.where(under | unlinked)


def _summary_line(summary: Mapping[str, Any]) -> None:
    print("data-go-kr-file-data-summary-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))


def _tables(argv: Sequence[str]) -> int:
    for slug in files(load_source_contract()):
        print(f"{table_of(slug)} {source_slug(slug)}")
    return 0


def _parse(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="parse")
    parser.add_argument("--file", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--work-dir", required=True)
    args = parser.parse_args(argv)
    entry = files(load_source_contract())[args.dataset]
    staged = stage(Path(args.file), entry, Path(args.work_dir))
    rows = 0
    for path in staged:
        with path.open(encoding="utf-8", newline="") as stream:
            rows += sum(1 for _ in csv.reader(stream))
    _summary_line({"job": JOB_NAME, "status": "parsed", "dataset": args.dataset, "parts": len(staged), "rows": rows})
    return 0


def _load(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="load")
    parser.add_argument("--plan-dir", action="append", default=[], required=True,
                        help="A directory `plan-silver-load-objects` wrote (plan.json + objects/); repeatable")
    parser.add_argument("--iceberg-catalog-name", default="lakehouse")
    parser.add_argument("--table-suffix", default="")
    parser.add_argument("--allow-non-smoke-write", action="store_true")
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.iceberg_catalog_name):
        parser.error("--iceberg-catalog-name must be a plain SQL identifier")
    if args.table_suffix not in ("", "_smoke"):
        parser.error("--table-suffix is empty or _smoke")
    if not args.table_suffix and not args.allow_non_smoke_write:
        parser.error("writing the Silver tables needs --allow-non-smoke-write")

    from lakehouse_engine import apply_catalog_settings, assert_catalog_env  # noqa: PLC0415
    from lakehouse_ingest import append_source_object, read_ingested_objects, unquoted_table_name  # noqa: PLC0415
    from platform_contracts import (  # noqa: PLC0415
        column_names, create_table_columns_sql, current_rows, evolve_iceberg_table_to_contract,
        load_lakehouse_contract, partition_clause_sql,
    )
    from pyspark.sql import SparkSession, functions as F  # noqa: PLC0415
    from silver_release_compare import release_changes  # noqa: PLC0415

    assert_catalog_env()
    source = load_source_contract()
    entries = files(source)
    objects = planned_objects([Path(d) for d in args.plan_dir], {source_slug(s): s for s in entries})
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    catalog = args.iceberg_catalog_name
    summary: dict[str, Any] = {"job": JOB_NAME, "status": "loaded", "editions": [], "follows": []}
    builder = SparkSession.builder.appName(f"foundation-platform-{JOB_NAME}").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, catalog).getOrCreate()
    try:
        inputs = LinkInputs(spark, catalog, source)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`silver`")
        tables = {}
        for slug in entries:
            contract = load_lakehouse_contract(table_of(slug))
            qualified = f"`{catalog}`.`silver`.`{slug}{args.table_suffix}`"
            spark.sql(f"CREATE TABLE IF NOT EXISTS {qualified} ({create_table_columns_sql(contract)}) USING iceberg "
                      f"{partition_clause_sql(contract)} TBLPROPERTIES ('format-version' = '2', "
                      "'write.parquet.compression-codec' = 'zstd')")
            evolve_iceberg_table_to_contract(spark, qualified, contract)
            tables[slug] = (qualified, contract, list(column_names(contract)))
        changes_at = change_snapshot(spark, catalog)

        for item in objects:
            slug, entry = item["dataset_slug"], entries[item["dataset_slug"]]
            qualified, contract, columns = tables[slug]
            work = item["path"].parent / f"{item['path'].name}.staged"
            rows = staged_rows(spark, stage(item["path"], entry, work), entry).cache()
            total = rows.count()
            current = current_rows(spark.table(qualified), contract)
            period = entry["row_key"].get("period_column")
            if period:
                # One period per edition: earlier periods are history, not keys this edition dropped.
                periods = [r[0] for r in rows.select(field(period)).distinct().collect()]
                current = current.where(field(period).isin(periods))
            check_bounds(total, current.count() or None, entry)
            if entry["row_key"]["kind"] == "columns":
                if rows.where(F.col("row_key") == "").limit(1).count():
                    raise ValueError(f"{slug}: a row has an empty provider key {entry['row_key']['columns']}")
                if rows.select("row_key").distinct().count() != total:
                    raise ValueError(f"{slug}: the provider key {entry['row_key']['columns']} repeats")
            snapshot = snapshot_id_of(item["object_key"])
            staged = (link(rows, entry, inputs)
                      .withColumn("source_record_id", F.lit(item["object_key"]))
                      .withColumn("source_snapshot_id", F.lit(snapshot))
                      .withColumn("ingested_at_utc", F.lit(now).cast("timestamp"))
                      .withColumn("row_state", F.lit(None).cast("string"))
                      .select(*columns).cache())
            changes = release_changes(staged, current, key=("row_key",), content=content_columns(entry),
                                      removal="row_state", source_snapshot_id=snapshot,
                                      ingested_at=F.lit(now).cast("timestamp"), columns=columns).cache()
            counts = {"rows": total, "appended": changes.count(),
                      "removed": changes.where(F.col("row_state") == "removed").count()}
            linked = "pnu" if link_kind(entry) == "parcel" else "legal_dong_code"
            if entry["place_link"]["kind"] in FOLLOWED_LINKS:
                counts["linked"] = staged.where(F.col(linked).isNotNull()).count()
                counts["link_rate"] = round(counts["linked"] / total, 4) if total else None
            append_source_object(spark, changes, columns, qualified, item["object_key"])
            if changes_at is not None and entry["place_link"]["kind"] in FOLLOWED_LINKS:
                # The links were made against the change table as it is now: the next follow starts here.
                append_source_object(spark, spark.table(qualified).limit(0), columns, qualified,
                                     follow_record(changes_at, now))
            for frame in (rows, staged, changes):
                frame.unpersist()
            shutil.rmtree(work, ignore_errors=True)
            summary["editions"].append({"dataset": slug, "object": item["object_key"], **counts})

        loaded = {item["dataset_slug"] for item in objects}
        for slug, entry in entries.items():
            if slug in loaded or entry["place_link"]["kind"] not in FOLLOWED_LINKS or changes_at is None:
                continue
            qualified, contract, columns = tables[slug]
            before = last_followed(list(read_ingested_objects(spark, qualified, unquoted_table_name(qualified))))
            if before is None or before == changes_at:
                continue  # never loaded, or its links are as of the change table now
            prefixes = changed_prefixes(spark, catalog, before, changes_at)
            current = current_rows(spark.table(qualified), contract)
            affected = follow_rows(current, entry, prefixes).cache()
            relinked = (link(affected.select("row_key", "provider_fields_json", "source_line_number",
                                             "source_record_id", "source_snapshot_id"), entry, inputs)
                        .withColumn("ingested_at_utc", F.lit(now).cast("timestamp"))
                        .withColumn("row_state", F.lit(None).cast("string"))
                        .select(*columns))
            # Same keys on both sides: only the rows whose link moved are appended, and no marker.
            changes = release_changes(relinked, affected, key=("row_key",), content=content_columns(entry),
                                      removal="row_state", source_snapshot_id="", ingested_at=F.lit(now).cast("timestamp"),
                                      columns=columns).where(F.col("row_state").isNull()).cache()
            moved = changes.count()
            append_source_object(spark, changes, columns, qualified, follow_record(changes_at, now))
            summary["follows"].append({"dataset": slug, "from_snapshot": before, "to_snapshot": changes_at,
                                       "prefixes": None if prefixes is None else len(prefixes),
                                       "checked": affected.count(), "appended": moved})
            affected.unpersist()
            changes.unpersist()
    finally:
        spark.stop()
    _summary_line(summary)
    return 0


COMMANDS = {"tables": _tables, "parse": _parse, "load": _load}


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in COMMANDS:
        print(f"usage: {Path(__file__).name} {{{','.join(COMMANDS)}}} ...", file=sys.stderr)
        return 2
    return COMMANDS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main())
