//! The 공매 (Onbid public sale) by-PNU kind's Gold in `DuckDB`.
//!
//! The twin of `onbid_public_auction_silver_to_gold.py` (root ADR-0287, ADR-0296, ADR-0340, ADR-0564
//! §6) in the form its contract describes, not Spark's bytes (coordinator, 2026-11-22: the takeover
//! verdict compares the documents as values and the switch re-bakes the lane once).
//!
//! A PNU's document carries the rounds still open as of the build (`bid_ends_at` after the instant
//! the build serves as of, root ADR-0287 §7), soonest-closing first, each with the provider's names
//! of its codes, its media and the path it was placed by; and once per document the blocks of the
//! items and announcement rounds those rounds name (root ADR-0340 §7). Rounds past the document
//! ceiling (256 KiB less an envelope) are left out, latest-closing first; a block counts with the
//! first round that names it. Where a round is served is `auction_placement_stage`'s (the 경매 Gold
//! places the same way). Each row carries its parcel's place id.
//!
//! The document's shape is the schema registry's current revision of the subject
//! (`parcel_public_auction_by_pnu`, written from `onbid_public_auction_document.document_shape`):
//! every field it names is read from the Silver column of that name, a list from the column's
//! `<name>_json` text in the provider's keys, so a document holds exactly the registry's fields.
//! Instants are written `YYYY-MM-DDTHH:MM:SSZ`.
//!
//! The kind is built whole on every run from one read of its inputs (tens of thousands of rounds;
//! root ADR-0326): time closes rounds with no input moving, so finding the change would read every
//! input twice. The publisher appends only the PNUs whose digest changed.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{ensure, Context};
use duckdb::Connection;
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::auction_placement_stage as placement;
use crate::duckdb_parquet_stage::with_contract_columns;
use crate::kind_versions_stage::{
    elapsed, fill, is_plain, KindVersionsStage, ResultCheck, RowSet, CODE_CHANGE_SOURCE,
    PARCEL_SOURCE, PLACE_IDS, SHARED_SQL, SPOOL_SQL,
};
use crate::parcel_nearby_stage::Source;
use crate::place_id_join::BRIDGE_SOURCE;

/// The rounds (the item list).
pub const ITEM_SOURCE: &str = "silver.onbid_real_estate_item";
/// The media index.
pub const PHOTO_SOURCE: &str = "silver.onbid_real_estate_item_photo";
/// An item's detail.
pub const DETAIL_SOURCE: &str = "silver.onbid_real_estate_item_detail";
/// An item's bid information.
pub const BID_INFO_SOURCE: &str = "silver.onbid_item_bid_info";
/// An item's earlier rounds' results.
pub const RESULT_SOURCE: &str = "silver.onbid_item_bid_result";
/// The announcement rounds.
pub const ANNOUNCEMENT_SOURCE: &str = "silver.onbid_announcement";
/// Their opening state.
pub const ANNOUNCEMENT_RESULT_SOURCE: &str = "silver.onbid_announcement_bid_result";
/// Their detail.
pub const ANNOUNCEMENT_DETAIL_SOURCE: &str = "silver.onbid_announcement_detail";
/// The parcel lineage a round on a retired PNU is carried by.
pub const LINEAGE_SOURCE: &str = "silver.parcel_lineage";
/// What the rules are; part of the build's code version.
pub const RULES_VERSION: &str = "parcel-public-auction-duckdb-v1";
/// The registry's current revision of the document (`schema_registry` holds it current).
pub const SCHEMA: &str = include_str!(
    "../../../../contracts/schema-registry/parcel_public_auction_by_pnu/1/4.schema.json"
);
/// The Onbid source contract: which media kinds are a thumbnail's full-size original.
const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/data-go-kr-onbid.contract.json");
/// One document's ceiling (root ADR-0193) and the envelope it keeps for the rest.
pub const DOCUMENT_BYTE_LIMIT: u64 = 256 * 1024;
/// What the document keeps of its ceiling for its envelope.
pub const DOCUMENT_ENVELOPE_BYTES: u64 = 1024;
/// An announcement's text past this many characters is not served (`anncm_al_cont_omitted`).
pub const ANNOUNCEMENT_TEXT_LIMIT: u32 = 20_000;
/// The datasets a served round comes from (data.go.kr ids).
pub const SOURCE_DATASETS: [&str; 7] = [
    "data.go.kr:15157207",
    "data.go.kr:15157254",
    "data.go.kr:15157247",
    "data.go.kr:15157251",
    "data.go.kr:15157216",
    "data.go.kr:15157222",
    "data.go.kr:15157218",
];
/// The media kinds a round's `photos` keep; every other kind is the item's `media`.
pub const ROUND_MEDIA_KINDS: [&str; 2] = ["photo", "appraisal"];
/// A round's key.
const KEY: [&str; 2] = ["cltr_mng_no", "pbct_cdtn_no"];
/// An announcement round's key.
const ANNOUNCEMENT_KEY: [&str; 2] = ["onbid_pbanc_no", "pbct_no"];
/// The stored-file facts a media list's element carries in our keys (root ADR-0298).
const STORED_FILE: [&str; 3] = ["sha256", "media_format", "size_bytes"];

/// The kind's spool: one row per served round and PNU, with its rendered parts.
const ENTRIES: &str = "entries";

/// `built` of the PNUs of `@targets@`, from `entries`: the rounds kept under the ceiling, the
/// blocks the first round naming them carries, and the place id.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH e AS ( \
  SELECT *, \
    row_number() OVER (PARTITION BY pnu, cltr_mng_no ORDER BY ends_us, cltr_mng_no, pbct_cdtn_no) = 1 AS first_item, \
    row_number() OVER (PARTITION BY pnu, onbid_pbanc_no, pbct_no ORDER BY ends_us, cltr_mng_no, pbct_cdtn_no) = 1 \
      AS first_announcement \
  FROM entries WHERE @targets@), \
sized AS ( \
  SELECT *, length(entry_json) + 1 \
    + CASE WHEN first_item AND item_json IS NOT NULL THEN length(item_json) + 1 ELSE 0 END \
    + CASE WHEN first_announcement AND announcement_json IS NOT NULL THEN length(announcement_json) + 1 ELSE 0 END \
    AS bytes FROM e), \
kept AS ( \
  SELECT * FROM ( \
    SELECT *, sum(bytes) OVER (PARTITION BY pnu ORDER BY ends_us, cltr_mng_no, pbct_cdtn_no \
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM sized) \
  WHERE running <= @limit@), \
documents AS ( \
  SELECT pnu, \
    '{\"rounds\":[' || string_agg(entry_json, ',' ORDER BY ends_us, cltr_mng_no, pbct_cdtn_no) \
    || '],\"items\":[' || coalesce(string_agg(item_json, ',' ORDER BY cltr_mng_no) \
         FILTER (WHERE first_item AND item_json IS NOT NULL), '') \
    || '],\"announcements\":[' || coalesce(string_agg(announcement_json, ',' \
         ORDER BY onbid_pbanc_no NULLS FIRST, pbct_no NULLS FIRST) \
         FILTER (WHERE first_announcement AND announcement_json IS NOT NULL), '') \
    || '],\"source_datasets\":@datasets@}' AS public_auction_json, \
    CASE WHEN bool_or(attached_via IS NULL) THEN NULL ELSE min(attached_via) END AS attached_via \
  FROM kept GROUP BY pnu) \
SELECT d.pnu, d.public_auction_json, d.attached_via, p.place_id, \
  sha256('{\"pnu\":' || to_json(d.pnu)::VARCHAR \
    || ',\"public_auction_json\":' || to_json(d.public_auction_json)::VARCHAR \
    || CASE WHEN d.attached_via IS NULL THEN '' ELSE ',\"attached_via\":' || to_json(d.attached_via)::VARCHAR END \
    || CASE WHEN p.place_id IS NULL THEN '' ELSE ',\"place_id\":' || to_json(p.place_id)::VARCHAR END || '}') \
    AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents d LEFT JOIN @place_ids@ p USING (pnu)";

const BUILT_TABLE: &str =
    "CREATE OR REPLACE TABLE built (pnu VARCHAR, public_auction_json VARCHAR, \
    attached_via VARCHAR, place_id VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, \
    published_at_utc VARCHAR);";

/// The 공매 kind.
#[derive(Clone, Copy, Debug)]
pub struct PublicAuctionKind;

/// The one 공매 kind.
pub const PUBLIC_AUCTION: PublicAuctionKind = PublicAuctionKind;

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The logical type of `column` of `table`, if the table has it.
fn column_type(table: &str, column: &str) -> anyhow::Result<Option<&'static str>> {
    Ok(contract(table)?
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .map(|candidate| candidate.logical_type))
}

/// The SQL value of `alias.column` as the document writes it: an instant as `YYYY-MM-DDTHH:MM:SSZ`
/// (whatever the session's time zone), every other type as `DuckDB`'s `to_json` writes it.
fn value(alias: &str, column: &str, logical_type: &str) -> String {
    if logical_type == "timestamp" {
        format!("strftime(make_timestamp(epoch_us({alias}.{column})), '%Y-%m-%dT%H:%M:%SZ')")
    } else {
        format!("{alias}.{column}")
    }
}

/// A provider key of a served key (`onbid_public_auction_document.provider_key`): the stored
/// file's facts are ours, every other key the provider's camel case.
fn provider_key(key: &str) -> String {
    if STORED_FILE.contains(&key) {
        return key.to_owned();
    }
    let mut parts = key.split('_');
    let head = parts.next().unwrap_or_default().to_owned();
    parts.fold(head, |mut text, part| {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            text.extend(first.to_uppercase());
            text.push_str(chars.as_str());
        }
        text
    })
}

/// The element keys of a list field of the registry (`[{..}]` items' properties), in order.
fn element_keys(field: &Value) -> anyhow::Result<Vec<String>> {
    Ok(field["items"]["properties"]
        .as_object()
        .context("a list field names its element's properties")?
        .keys()
        .cloned()
        .collect())
}

/// Whether a registry field is a list.
fn is_list(field: &Value) -> bool {
    match &field["type"] {
        Value::String(kind) => kind == "array",
        Value::Array(kinds) => kinds.iter().any(|kind| kind == "array"),
        _ => false,
    }
}

/// The list a `*_json` text holds (`alias.<name>_json`), in the document's keys: parsed by the
/// provider's keys, each value as text (the provider writes some numbers as text), an object the
/// provider's V2 made an array of 0..1 (공매재산명세) read as that array.
fn parsed_list(alias: &str, name: &str, keys: &[String]) -> String {
    let text = format!("{alias}.{name}_json");
    let wrapped = format!(
        "CASE WHEN starts_with(trim({text}), '{{') THEN '[' || {text} || ']' ELSE {text} END"
    );
    let structure = format!(
        "[{{{}}}]",
        keys.iter()
            .map(|key| format!("\"{}\":\"VARCHAR\"", provider_key(key)))
            .collect::<Vec<_>>()
            .join(",")
    );
    let element = keys
        .iter()
        .map(|key| format!("'{key}': e.\"{}\"", provider_key(key)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("list_transform(from_json({wrapped}, '{structure}'), lambda e: {{{element}}})")
}

/// The struct of a block from `table` (aliased `alias`) by the registry's `properties`: a list from
/// its `<name>_json` column, a scalar from its column of that name, a name it does not hold null.
fn object(table: &str, alias: &str, properties: &Value) -> anyhow::Result<String> {
    let fields = properties
        .as_object()
        .context("a block names its properties")?
        .iter()
        .map(|(name, field)| {
            let expression = if is_list(field) {
                if column_type(table, &format!("{name}_json"))?.is_some() {
                    parsed_list(alias, name, &element_keys(field)?)
                } else {
                    "NULL".to_owned()
                }
            } else {
                column_type(table, name)?
                    .map_or_else(|| "NULL".to_owned(), |kind| value(alias, name, kind))
            };
            Ok(format!("'{name}': {expression}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("{{{}}}", fields.join(", ")))
}

/// The registry's properties at `path` (each step a property, `[]` an array's items).
fn shape(path: &[&str]) -> anyhow::Result<Value> {
    let schema: Value = serde_json::from_str(SCHEMA)?;
    let mut at = &schema;
    for step in path {
        at = if *step == "[]" {
            &at["items"]
        } else {
            &at["properties"][*step]
        };
    }
    ensure!(
        !at.is_null(),
        "the registry's document has no {}",
        path.join(".")
    );
    Ok(at.clone())
}

/// The media kind of each thumbnail list's full-size originals → the kind of the thumbnails they
/// belong to (`onbid_public_auction_document.original_kinds`, root ADR-0424).
fn original_kinds() -> anyhow::Result<Vec<(String, String)>> {
    let contract: Value = serde_json::from_str(SOURCE_CONTRACT)?;
    let fields = &contract["media"]["fields"];
    let mut kinds = Vec::new();
    if let Some(originals) = fields["originals"].as_object() {
        for (field, rule) in originals {
            let original = rule["media_kind"]
                .as_str()
                .context("an original names its kind")?;
            let thumbnail = fields["list"][field]
                .as_str()
                .with_context(|| format!("no thumbnail list {field}"))?;
            kinds.push((original.to_owned(), thumbnail.to_owned()));
        }
    }
    Ok(kinds)
}

fn quoted_list(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("'{value}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The current rows of `table` at `source` into the temporary table `into` (the contract's one
/// rendering, root ADR-0564 §4): every column of its contract.
fn load_current(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    into: &str,
) -> anyhow::Result<u64> {
    let at = Instant::now();
    let contract = contract(table)?;
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{table} keeps no versions per key"))?;
    let read = with_contract_columns(connection, contract, &source.of(table)?)?;
    let columns: Vec<&str> = contract.columns.iter().map(|column| column.name).collect();
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {into} AS {}",
        latest.current_rows_sql(&read, &columns, "TRUE")
    ))?;
    if !latest.keep_ties {
        let key = latest.key.join(", ");
        let (rows, keys): (i64, i64) = connection.query_row(
            &format!("SELECT count(*), count(DISTINCT ({key})) FROM {into}"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            rows == keys,
            "{table} holds {} current rows tied on their version order",
            rows - keys
        );
    }
    Ok(elapsed(at))
}

/// The round's entry as a struct expression over `s` (a servable placed round with `photos`).
fn entry() -> anyhow::Result<String> {
    let rounds = shape(&["public_auction", "rounds", "[]"])?;
    let fields = rounds["properties"]
        .as_object()
        .context("a round names its properties")?
        .keys()
        .map(|name| {
            let expression = match name.as_str() {
                "photos" => "coalesce(ph.photos, [])".to_owned(),
                "attached_via" => "s.attached_via".to_owned(),
                // The provider's name of a usage code, as every list row carries it
                // (`cltr_usg_<level>_ctgr_nm`, root ADR-0340 §8); never a name of ours.
                usage if usage.starts_with("usg_") && usage.ends_with("_ctgr_name") => {
                    let level = usage
                        .trim_start_matches("usg_")
                        .trim_end_matches("_ctgr_name");
                    let inline = format!("cltr_usg_{level}_ctgr_nm");
                    column_type(ITEM_SOURCE, &inline)?
                        .map_or_else(|| "NULL".to_owned(), |_| format!("s.{inline}"))
                }
                column => {
                    let kind = column_type(ITEM_SOURCE, column)?.with_context(|| {
                        format!("the round field {column} is no column of {ITEM_SOURCE}")
                    })?;
                    value("s", column, kind)
                }
            };
            Ok(format!("'{name}': {expression}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("{{{}}}", fields.join(", ")))
}

/// The statements that leave `item_blocks(cltr_mng_no, item_json)` of the items `servable` names.
fn item_blocks() -> anyhow::Result<String> {
    let item = shape(&["public_auction", "items", "[]"])?;
    let detail = object(
        DETAIL_SOURCE,
        "d",
        &item["properties"]["detail"]["properties"],
    )?;
    let bid_info = object(
        BID_INFO_SOURCE,
        "b",
        &item["properties"]["bid_info"]["properties"],
    )?;
    let history_fields = element_keys(&item["properties"]["history"])?
        .iter()
        .map(|name| {
            let kind = column_type(RESULT_SOURCE, name)?.with_context(|| {
                format!("the history field {name} is no column of {RESULT_SOURCE}")
            })?;
            Ok(format!("'{name}': {}", value("r", name, kind)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    let media_fields = element_keys(&item["properties"]["media"])?
        .iter()
        .map(|name| format!("'{name}': m.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let originals: Vec<String> = original_kinds()?
        .into_iter()
        .map(|(original, _)| original)
        .collect();
    let not_item_media: Vec<&str> = ROUND_MEDIA_KINDS
        .iter()
        .copied()
        .chain(originals.iter().map(String::as_str))
        .collect();
    Ok(format!(
            "CREATE OR REPLACE TEMP TABLE item_history AS \
               SELECT r.cltr_mng_no, list({{{history_fields}}} ORDER BY r.opened_at NULLS FIRST, \
                 r.pbct_cdtn_no NULLS FIRST) AS history \
               FROM result_rows r WHERE r.cltr_mng_no IN (SELECT cltr_mng_no FROM servable) GROUP BY 1; \
             CREATE OR REPLACE TEMP TABLE item_media AS \
               SELECT m.cltr_mng_no, list({{{media_fields}}} ORDER BY m.media_kind NULLS FIRST, \
                 m.sequence NULLS FIRST, m.sha256) AS media \
               FROM photo_rows m WHERE m.media_kind NOT IN ({not_item}) \
                 AND m.cltr_mng_no IN (SELECT cltr_mng_no FROM servable) GROUP BY 1; \
             CREATE OR REPLACE TEMP TABLE item_blocks AS \
               SELECT k.cltr_mng_no, to_json({{'cltr_mng_no': k.cltr_mng_no, \
                 'detail': (SELECT {detail} FROM detail_rows d WHERE d.cltr_mng_no = k.cltr_mng_no), \
                 'bid_info': (SELECT {bid_info} FROM bid_info_rows b WHERE b.cltr_mng_no = k.cltr_mng_no), \
                 'history': coalesce(h.history, []), 'media': coalesce(m.media, [])}})::VARCHAR AS item_json \
               FROM (SELECT DISTINCT cltr_mng_no FROM servable) k \
               LEFT JOIN item_history h USING (cltr_mng_no) LEFT JOIN item_media m USING (cltr_mng_no);",
            not_item = quoted_list(&not_item_media),
        ))
}

/// The statements that leave `announcement_blocks(onbid_pbanc_no, pbct_no, announcement_json)`
/// of the announcement rounds `servable` names: the list's value of a field, else its opening
/// state's, else its detail's; the text past its limit left out and said so.
fn announcement_blocks() -> anyhow::Result<String> {
    let announcement = shape(&["public_auction", "announcements", "[]"])?;
    let tables = [
        ("l", ANNOUNCEMENT_SOURCE),
        ("r", ANNOUNCEMENT_RESULT_SOURCE),
        ("d", ANNOUNCEMENT_DETAIL_SOURCE),
    ];
    let fields = announcement["properties"]
            .as_object()
            .context("an announcement names its properties")?
            .iter()
            .map(|(name, field)| {
                let expression = if ANNOUNCEMENT_KEY.contains(&name.as_str()) {
                    format!("k.{name}")
                } else if name == "anncm_al_cont_omitted" {
                    format!("coalesce(length(d.anncm_al_cont) > {ANNOUNCEMENT_TEXT_LIMIT}, false)")
                } else if name == "anncm_al_cont" {
                    format!(
                        "CASE WHEN length(d.anncm_al_cont) > {ANNOUNCEMENT_TEXT_LIMIT} THEN NULL ELSE d.anncm_al_cont END"
                    )
                } else if is_list(field) {
                    parsed_list("d", name, &element_keys(field)?)
                } else {
                    let mut found = tables
                        .iter()
                        .filter_map(|(alias, table)| {
                            column_type(table, name)
                                .transpose()
                                .map(|kind| kind.map(|kind| value(alias, name, kind)))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    match found.len() {
                        0 => "NULL".to_owned(),
                        1 => found.remove(0),
                        _ => format!("coalesce({})", found.join(", ")),
                    }
                };
                Ok(format!("'{name}': {expression}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!(
            "CREATE OR REPLACE TEMP TABLE announcement_blocks AS \
               SELECT k.onbid_pbanc_no, k.pbct_no, to_json({{{fields}}})::VARCHAR AS announcement_json \
               FROM (SELECT DISTINCT onbid_pbanc_no, pbct_no FROM servable \
                     WHERE onbid_pbanc_no IS NOT NULL AND pbct_no IS NOT NULL) k \
               LEFT JOIN announcement_rows l USING (onbid_pbanc_no, pbct_no) \
               LEFT JOIN announcement_result_rows r USING (onbid_pbanc_no, pbct_no) \
               LEFT JOIN announcement_detail_rows d USING (onbid_pbanc_no, pbct_no);",
            fields = fields.join(", ")
        ))
}

/// The statement that leaves `round_photos(cltr_mng_no, photos)`: each item's round media, by
/// kind then the provider's order, each with its full-size original (the latest landed).
fn round_photos() -> anyhow::Result<String> {
    let kinds = original_kinds()?;
    let mapped = kinds
        .iter()
        .map(|(original, thumbnail)| format!("WHEN '{original}' THEN '{thumbnail}'"))
        .collect::<Vec<_>>()
        .join(" ");
    let originals = kinds
        .iter()
        .map(|(original, _)| format!("'{original}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let originals_sql = if kinds.is_empty() {
        "SELECT CAST(NULL AS VARCHAR) AS cltr_mng_no, CAST(NULL AS VARCHAR) AS media_kind, \
             CAST(NULL AS INTEGER) AS sequence, NULL AS original WHERE FALSE"
            .to_owned()
    } else {
        format!(
                "SELECT cltr_mng_no, CASE media_kind {mapped} END AS media_kind, sequence, \
                   arg_max({{'sha256': sha256, 'media_format': media_format, 'size_bytes': size_bytes}}, \
                     ingested_at_utc) AS original \
                 FROM photo_rows WHERE media_kind IN ({originals}) GROUP BY 1, 2, 3"
            )
    };
    Ok(format!(
            "CREATE OR REPLACE TEMP TABLE round_photos AS \
               SELECT p.cltr_mng_no, list({{'sha256': p.sha256, 'media_kind': p.media_kind, \
                 'sequence': p.sequence, 'title': p.title, 'media_format': p.media_format, \
                 'size_bytes': p.size_bytes, 'original': o.original}} \
                 ORDER BY p.media_kind, p.sequence NULLS FIRST, p.sha256) AS photos \
               FROM photo_rows p LEFT JOIN ({originals_sql}) o USING (cltr_mng_no, media_kind, sequence) \
               WHERE p.media_kind IN ({round}) GROUP BY 1;",
            round = quoted_list(&ROUND_MEDIA_KINDS),
        ))
}

/// The served rounds of `built` that carry no photo though their item has current round media
/// in the session's `photo_rows`: each as its item, `select` naming what is selected of them.
fn rounds_without_their_media(select: &str) -> String {
    format!(
        "SELECT {select} \
         FROM (SELECT json_extract_string(round, '$.cltr_mng_no') AS cltr_mng_no, \
                 coalesce(json_array_length(round, '$.photos'), 0) AS photos \
               FROM (SELECT unnest(json_extract(public_auction_json, '$.rounds[*]')) AS round \
                     FROM built)) served \
         JOIN (SELECT DISTINCT cltr_mng_no FROM photo_rows WHERE media_kind IN ({round})) media \
           USING (cltr_mng_no) \
         WHERE served.photos = 0",
        round = quoted_list(&ROUND_MEDIA_KINDS),
    )
}

impl KindVersionsStage for PublicAuctionKind {
    fn section_column(&self) -> &'static str {
        "public_auction_json"
    }

    /// A served round whose item has current round media carries its photos. 2026-11-04: the photo
    /// Silver had rows while the served rounds showed none (the Trino check
    /// `rounds_carry_their_media`, which read the Spark table; this is the producer's own, over the
    /// media it read).
    fn result_checks(&self) -> Vec<ResultCheck> {
        vec![ResultCheck {
            name: "rounds_carry_their_media",
            sql: rounds_without_their_media("count(*)"),
            blocking: true,
            samples_sql: Some(rounds_without_their_media("DISTINCT cltr_mng_no")),
        }]
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![
            PARCEL_SOURCE,
            ITEM_SOURCE,
            CODE_CHANGE_SOURCE,
            DETAIL_SOURCE,
            BID_INFO_SOURCE,
            RESULT_SOURCE,
            ANNOUNCEMENT_SOURCE,
            ANNOUNCEMENT_RESULT_SOURCE,
            ANNOUNCEMENT_DETAIL_SOURCE,
            PHOTO_SOURCE,
            LINEAGE_SOURCE,
            BRIDGE_SOURCE,
        ]
    }

    fn whole_table_inputs(&self) -> Vec<&'static str> {
        self.inputs()
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let rendered = [
            entry(),
            item_blocks(),
            announcement_blocks(),
            round_photos(),
        ]
        .into_iter()
        .map(|part| part.unwrap_or_else(|error| format!("unrenderable: {error}")))
        .collect::<Vec<_>>();
        for text in [RULES_VERSION, DOCUMENT_SQL, SCHEMA]
            .into_iter()
            .chain(rendered.iter().map(String::as_str))
            .chain(placement::SQL)
            .chain(SHARED_SQL)
        {
            hash.update(text.as_bytes());
            hash.update([0]);
        }
        format!("{:x}", hash.finalize())
    }

    fn row_sets(&self) -> Vec<RowSet> {
        Vec::new()
    }

    fn carries_place_id(&self) -> bool {
        true
    }

    fn as_of_dependent(&self) -> bool {
        true
    }

    fn pnu_spools(&self) -> Vec<&'static str> {
        vec![ENTRIES]
    }

    fn spool_more(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        _edition: &str,
        suffix: &str,
        dir_text: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        let mut read_millis = BTreeMap::new();
        for (table, into) in [
            (ITEM_SOURCE, "item_versions_current"),
            (PHOTO_SOURCE, "photo_rows"),
            (DETAIL_SOURCE, "detail_rows"),
            (BID_INFO_SOURCE, "bid_info_rows"),
            (RESULT_SOURCE, "result_rows"),
            (ANNOUNCEMENT_SOURCE, "announcement_rows"),
            (ANNOUNCEMENT_RESULT_SOURCE, "announcement_result_rows"),
            (ANNOUNCEMENT_DETAIL_SOURCE, "announcement_detail_rows"),
        ] {
            read_millis.insert(
                table.to_owned(),
                load_current(connection, source, table, into)?,
            );
        }
        // The rounds open as of the build's instant (a round without a close time is never served).
        connection.execute_batch(
            "CREATE OR REPLACE TEMP TABLE open_rounds AS SELECT * FROM item_versions_current \
             WHERE bid_ends_at IS NOT NULL AND epoch_us(bid_ends_at) > (SELECT instant_us FROM as_of);",
        )?;
        placement::crosswalk(connection)?;
        placement::locate(connection, "open_rounds", "pnu", "ltno_pnu_raw")?;
        let at = Instant::now();
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE lineage AS SELECT predecessor_pnu, successor_pnu, relation, grade, \
               evidence_kind, evidence_ref FROM {lineage}; \
             CREATE OR REPLACE TEMP TABLE carry_sources AS SELECT DISTINCT source_pnu AS pnu FROM located \
               WHERE source_pnu IS NOT NULL AND source_pnu NOT IN ( \
                 SELECT pnu FROM read_parquet('{dir_text}/served{suffix}/*/*.parquet'));",
            lineage = source.of(LINEAGE_SOURCE)?
        ))?;
        read_millis.insert(LINEAGE_SOURCE.to_owned(), elapsed(at));
        crate::lineage_carry_stage::carry_tables(connection)?;
        placement::place(
            connection,
            &format!("read_parquet('{dir_text}/served{suffix}/*/*.parquet')"),
        )?;
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE servable AS {};",
            placement::SERVABLE_SQL
        ))?;
        connection.execute_batch(&round_photos()?)?;
        connection.execute_batch(&item_blocks()?)?;
        connection.execute_batch(&announcement_blocks()?)?;
        let entry = entry()?;
        let keys = KEY
            .iter()
            .chain(ANNOUNCEMENT_KEY.iter())
            .map(|k| format!("s.{k}"))
            .collect::<Vec<_>>()
            .join(", ");
        connection.execute_batch(&fill(
            SPOOL_SQL,
            &[
                (
                    "query",
                    &format!(
                        "SELECT s.pnu, epoch_us(s.bid_ends_at) AS ends_us, {keys}, s.attached_via, \
                           to_json({entry})::VARCHAR AS entry_json, i.item_json, a.announcement_json \
                         FROM servable s LEFT JOIN round_photos ph USING (cltr_mng_no) \
                         LEFT JOIN item_blocks i USING (cltr_mng_no) \
                         LEFT JOIN announcement_blocks a USING (onbid_pbanc_no, pbct_no)"
                    ),
                ),
                ("dir", dir_text),
                ("name", &format!("{ENTRIES}{suffix}")),
            ],
        ))?;
        Ok(read_millis)
    }

    fn load_more(
        &self,
        connection: &Connection,
        read: &dyn Fn(&str) -> Option<String>,
        suffix: &str,
    ) -> anyhow::Result<u64> {
        let name = format!("{ENTRIES}{suffix}");
        let body = read(&name).map_or_else(
            || {
                "SELECT CAST(NULL AS VARCHAR) AS pnu, CAST(NULL AS BIGINT) AS ends_us, \
                 CAST(NULL AS VARCHAR) AS cltr_mng_no, CAST(NULL AS BIGINT) AS pbct_cdtn_no, \
                 CAST(NULL AS BIGINT) AS onbid_pbanc_no, CAST(NULL AS BIGINT) AS pbct_no, \
                 CAST(NULL AS VARCHAR) AS attached_via, CAST(NULL AS VARCHAR) AS entry_json, \
                 CAST(NULL AS VARCHAR) AS item_json, CAST(NULL AS VARCHAR) AS announcement_json WHERE FALSE"
                    .to_owned()
            },
            // The 시도 is the spool's directory, not a column of its files.
            |read| format!("SELECT * FROM {read}"),
        );
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE entries AS {body} WHERE pnu IN (SELECT pnu FROM served{suffix});"
        ))?;
        let rows: i64 =
            connection.query_row("SELECT count(*) FROM entries", [], |row| row.get(0))?;
        Ok(u64::try_from(rows)?)
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(BUILT_TABLE)?;
        Ok(())
    }

    fn build(
        &self,
        connection: &Connection,
        all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            is_plain(source_snapshot_id) && is_plain(published_at_utc),
            "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
        );
        ensure!(
            all,
            "the 공매 kind is built whole on every run (root ADR-0326)"
        );
        let datasets = format!(
            "[{}]",
            SOURCE_DATASETS
                .iter()
                .map(|dataset| format!("\"{dataset}\""))
                .collect::<Vec<_>>()
                .join(",")
        );
        let limit = (DOCUMENT_BYTE_LIMIT - DOCUMENT_ENVELOPE_BYTES).to_string();
        let at = Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("targets", "TRUE"),
                    ("limit", &limit),
                    ("datasets", &datasets),
                    ("place_ids", PLACE_IDS),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the public-auction document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
