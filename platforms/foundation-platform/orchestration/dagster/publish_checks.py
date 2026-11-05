"""Publish checks: a published dataset is audited before its readers take it (root ADR-0416).

Every check here is declared `blocking: true` (severity ERROR) in its asset's definition
(`asset_checks`, asset_checks.py); this file holds only the kinds. In a Dagster run that materializes the asset,
a failing check stops every asset downstream of it in that run: the bad release is never published
(dagster.io/docs/guides/test/asset-checks, "Blocking downstream materialization"). While Airflow still
runs the job (ADR-0333 stage 0, airlift observe), airlift's sensor starts the checks after it observes
the materialization, so the failure is recorded on the asset and names what failed, and blocks from the
day the lane's job moves to Dagster.

Each check reads only what its rule needs, through Trino, and selects current rows the one way the
contracts define them (infra/lakehouse/contracts, platform_contracts.current_row_latest): the newest row
per key, a removal marker meaning the key has no current row. No check restates a table, key or media
kind: they come from the pipeline graph, the lakehouse contract and the producer's module.

The checks, each one an incident of 2026-11-04:
- `current_key_unique` on silver.land_use_zone_area: one current row per (layer_code, zone_feature_id).
  Duplicated current keys reached the zoning tiles and the parcel zoning kind.
- `rounds_carry_their_media` on gold.parcel_public_auction: every served round whose item has current
  media rows in silver.onbid_real_estate_item_photo carries a non-empty `photos`. The photo Silver had
  rows while the served rounds showed none.
"""

import json
import sys

from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue

import observe_assets
import unit_price_checks
from asset_checks import Kind

JOBS_DIRECTORY = observe_assets.job_specs.PLATFORM_ROOT / "infra" / "lakehouse" / "spark" / "jobs"
if str(JOBS_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(JOBS_DIRECTORY))

import onbid_public_auction_document as onbid_document  # noqa: E402
import platform_contracts  # noqa: E402

# The examples a failing check records, so the failure names what to fix without a second query.
SAMPLE_LIMIT = 5


def _qualified(table):
    namespace, name = table.split(".", 1)
    return f"{unit_price_checks.TRINO_CATALOG}.{namespace}.{name}"


def _sql_name(column):
    if not column.replace("_", "").isalnum():
        raise ValueError(f"not a contract column name: {column!r}")
    return column


def current_rows_sql(table, columns):
    """SELECT of `columns` from `table`'s current rows, as its lakehouse contract defines them.

    The rule platform_contracts.current_rows applies in Spark, in Trino SQL: the newest row per
    `current_row_latest.key` by `newest_first` (nulls last), every tied row kept (a check counts ties;
    it does not refuse them as a reader does), then rows whose removal column says REMOVED dropped.
    A table whose contract selects by a predicate or by edition has no translation here yet: refused,
    never guessed.
    """
    contract = platform_contracts.load_lakehouse_contract(table)
    if platform_contracts.current_row_predicate(contract) is not None:
        raise ValueError(f"{table}: a current-row predicate has no Trino translation yet")
    if platform_contracts.row_versions(contract)["kind"] == "editions":
        raise ValueError(f"{table}: an edition table has no Trino translation yet")
    latest = platform_contracts.current_row_latest(contract)
    selected = ", ".join(_sql_name(column) for column in columns)
    if latest is None:
        return f"SELECT {selected} FROM {_qualified(table)}"
    key = ", ".join(_sql_name(column) for column in latest.key)
    order = ", ".join(f"{_sql_name(column)} DESC NULLS LAST" for column in latest.newest_first)
    removal = latest.removal_column
    inner = list(dict.fromkeys([*columns, *([removal] if removal else [])]))
    sql = (f"SELECT {selected} FROM (SELECT {', '.join(_sql_name(c) for c in inner)}, "
           f"rank() OVER (PARTITION BY {key} ORDER BY {order}) AS current_rank FROM {_qualified(table)}) "
           "WHERE current_rank = 1")
    if removal:
        sql += f" AND ({_sql_name(removal)} IS NULL OR {_sql_name(removal)} <> '{platform_contracts.REMOVED}')"
    return sql


def key_unique_sql(table):
    """(current rows, current keys, up to SAMPLE_LIMIT duplicated keys as JSON) of `table`."""
    latest = platform_contracts.current_row_latest(platform_contracts.load_lakehouse_contract(table))
    if latest is None:
        raise ValueError(f"{table}: its contract declares no current-row key")
    key = [_sql_name(column) for column in latest.key]
    listed = ", ".join(key)
    pairs = ", ".join(f"KEY '{column}' VALUE {column}" for column in key)
    return (
        f"WITH current_rows AS ({current_rows_sql(table, key)}), "
        f"counted AS (SELECT {listed}, count(*) AS copies FROM current_rows GROUP BY {listed}) "
        "SELECT sum(copies), count(*), "
        f"json_format(CAST(slice(array_agg(json_parse(json_object({pairs}))) FILTER (WHERE copies > 1), 1, "
        f"{SAMPLE_LIMIT}) AS JSON)) FROM counted"
    )


def key_unique_result(table, row, severity=AssetCheckSeverity.ERROR):
    rows, keys, sample = row
    rows, keys = int(rows or 0), int(keys or 0)
    return AssetCheckResult(
        passed=rows == keys,
        severity=severity,
        metadata={"table": table, "current_rows": rows, "current_keys": keys,
                  "duplicate_rows": rows - keys, "duplicated_keys_sample": MetadataValue.json(_json(sample))},
    )


def media_sql(gold_table, photo_table=onbid_document.PHOTO_SOURCE):
    """(served rounds, rounds whose item has current media, those carrying none, up to SAMPLE_LIMIT items).

    A round of the served document (`public_auction_json` `rounds[]`) names its item and carries the
    item's media in `photos` (onbid_public_auction_silver_to_gold.document_rows); the media kinds a
    round carries are the producer's (onbid_public_auction_document.ROUND_MEDIA_KINDS).
    """
    item = _sql_name(onbid_document.ITEM)
    kinds = ", ".join(f"'{kind}'" for kind in onbid_document.ROUND_MEDIA_KINDS)
    media = current_rows_sql(photo_table, [item, "media_kind"])
    return (
        f"WITH media AS (SELECT DISTINCT {item} FROM ({media}) WHERE media_kind IN ({kinds})), "
        f"served AS (SELECT json_extract_scalar(served_round, '$.{item}') AS {item}, "
        "coalesce(json_array_length(json_extract(served_round, '$.photos')), 0) AS photos "
        f"FROM {_qualified(gold_table)} CROSS JOIN UNNEST(CAST(json_extract(public_auction_json, '$.rounds') "
        "AS array(json))) AS served_rounds(served_round)) "
        f"SELECT count(*), count(media.{item}), count_if(media.{item} IS NOT NULL AND served.photos = 0), "
        f"json_format(CAST(slice(array_agg(DISTINCT served.{item}) FILTER "
        f"(WHERE media.{item} IS NOT NULL AND served.photos = 0), 1, {SAMPLE_LIMIT}) AS JSON)) "
        f"FROM served LEFT JOIN media ON served.{item} = media.{item}"
    )


def media_result(gold_table, row, severity=AssetCheckSeverity.ERROR):
    rounds, with_media, missing, sample = (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), row[3])
    return AssetCheckResult(
        passed=missing == 0,
        severity=severity,
        metadata={"table": gold_table, "served_rounds": rounds, "rounds_with_media": with_media,
                  "rounds_missing_media": missing, "items_missing_media_sample": MetadataValue.json(_json(sample))},
    )


def _json(text):
    return json.loads(text) if text else []


def current_key_unique(context, check):
    table = check.node["table_name"]
    (row,) = unit_price_checks.trino_rows(key_unique_sql(table))
    return key_unique_result(table, row, check.severity)


def rounds_carry_their_media(context, check):
    (photos,) = check.reads
    if photos["table_name"] != onbid_document.PHOTO_SOURCE:
        raise ValueError(f"{photos['table_name']} is not the photo source the producer reads ({onbid_document.PHOTO_SOURCE})")
    table = check.node["table_name"]
    (row,) = unit_price_checks.trino_rows(media_sql(table, photos["table_name"]))
    return media_result(table, row, check.severity)


KINDS = {
    "current_key_unique": Kind("One current row per current-row key of the lakehouse contract.", current_key_unique),
    "rounds_carry_their_media": Kind("Every served round whose item has current media rows carries its photos.",
                                     rounds_carry_their_media),
}
