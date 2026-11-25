//! The 경매 (court auction) by-PNU kind's Gold in `DuckDB`.
//!
//! The twin of `court_auction_silver_to_gold.py` (root ADR-0280, ADR-0287, ADR-0353, ADR-0540,
//! ADR-0564 §6) in the form its contract describes, not Spark's bytes (coordinator, 2026-11-22: the
//! takeover verdict compares the documents as values and the switch re-bakes the lane once).
//!
//! A PNU's document carries the items in progress as of the build (the site's in-progress flag, a
//! sale date on or after the build's day, not removed; root ADR-0287 §7), soonest sale first, each
//! with its case's photos and its `detail` (the 물건상세's documents and the case's own, root
//! ADR-0540 §4-§5, each code with the site's name beside it); and the parcel's 매각결과 (root
//! ADR-0454), newest sale first, at most [`SALE_RESULT_BOUND`] within [`SALE_RESULT_BYTES`], with
//! their total. The items take the rest of the document ceiling (256 KiB less an envelope). Where a
//! row is served is `auction_placement_stage`'s (the 공매 Gold places the same way); each Gold row
//! carries its parcel's place id.
//!
//! The document's shape is the schema registry's current revision of the subject
//! (`parcel_court_auction_by_pnu`): every field it names is read from the Silver column of that name
//! (or the code name `with_names` gave it), so a document holds exactly the registry's fields.
//!
//! The kind is built whole on every run from one read of its inputs (root ADR-0326): time closes
//! items with no input moving. The publisher appends only the PNUs whose digest changed.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{ensure, Context};
use duckdb::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::auction_placement_stage as placement;
use crate::kind_versions_stage::{
    elapsed, fill, is_plain, KindVersionsStage, RowSet, CODE_CHANGE_SOURCE, PARCEL_SOURCE,
    PLACE_IDS, SHARED_SQL, SPOOL_SQL,
};
use crate::onbid_kind_stage::{column_type, contract, load_current, value};
use crate::parcel_nearby_stage::Source;
use crate::place_id_join::BRIDGE_SOURCE;

/// The items (the 물건 search).
pub const ITEM_SOURCE: &str = "silver.court_auction_item";
/// The 매각결과 search.
pub const SALE_RESULT_SOURCE: &str = "silver.court_auction_sale_result";
/// A case's photos.
pub const PHOTO_SOURCE: &str = "silver.court_auction_item_photo";
/// The site's 공통코드.
pub const CODE_SOURCE: &str = "silver.court_auction_code";
/// The parcel lineage an item on a retired PNU is carried by.
pub const LINEAGE_SOURCE: &str = "silver.parcel_lineage";
/// What the rules are; part of the build's code version.
pub const RULES_VERSION: &str = "parcel-court-auction-duckdb-v1";
/// The registry's current revision of the document.
pub const SCHEMA: &str = include_str!(
    "../../../../contracts/schema-registry/parcel_court_auction_by_pnu/1/11.schema.json"
);
/// The 법원경매정보 source contract: which 공통코드 group names which column.
const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/courtauction-go-kr.contract.json");
/// One document's ceiling (root ADR-0193) and the envelope it keeps for the rest.
pub const DOCUMENT_BYTE_LIMIT: u64 = 256 * 1024;
/// What the document keeps of its ceiling for its envelope.
pub const DOCUMENT_ENVELOPE_BYTES: u64 = 1024;
/// The share of the ceiling the sale results may take; the items in progress get the rest.
pub const SALE_RESULT_BYTES: u64 = 64 * 1024;
/// At most this many sale results per document.
pub const SALE_RESULT_BOUND: u64 = 50;
/// The source a served row comes from.
pub const SOURCE_DATASETS: [&str; 1] = ["courtauction.go.kr"];
/// 공공저작물 출처 표시 (저작권법 제24조의2, root ADR-0281 §6): a consumer of these rows names it.
pub const SOURCE_ATTRIBUTION: &str = "대한민국법원 법원경매정보";
/// The site's in-progress flag of an item served.
const IN_PROGRESS: &str = "Y";
/// The removal marker of the item list (root ADR-0275).
const REMOVED: &str = "removed";

/// The kind's spool: one row per served item or sale result and PNU, with its entry.
const ENTRIES: &str = "entries";

/// Each document table of a round's `detail`, by the registry's name, and the key it is read by:
/// the case's (court office, case) or the property's (and its sequence).
const CASE: &str = "court_office_code, case_no";
const PROPERTY: &str = "court_office_code, case_no, property_seq";

/// A document table the build reads, the temporary table its current rows go into, and whether it
/// appends editions (its newest per group) rather than versions per key.
const DOCUMENTS: [(&str, &str); 18] = [
    ("silver.court_auction_case", "case_rows"),
    ("silver.court_auction_item_spec", "spec_rows"),
    ("silver.court_auction_item_schedule", "schedule_rows"),
    ("silver.court_auction_item_object", "object_rows"),
    ("silver.court_auction_item_annex", "annex_rows"),
    ("silver.court_auction_appraisal_point", "point_rows"),
    ("silver.court_auction_case_related", "related_rows"),
    ("silver.court_auction_case_object", "case_object_rows"),
    ("silver.court_auction_case_party", "party_rows"),
    ("silver.court_auction_survey", "survey_rows"),
    ("silver.court_auction_survey_object", "survey_object_rows"),
    ("silver.court_auction_survey_tenant", "tenant_rows"),
    ("silver.court_auction_appraisal", "appraisal_rows"),
    ("silver.court_auction_appraisal_report", "report_rows"),
    ("silver.court_auction_case_filing", "filing_rows"),
    ("silver.court_auction_case_delivery", "delivery_rows"),
    ("silver.court_auction_sale_notice_item", "notice_rows"),
    ("silver.court_auction_dividend_notice", "dividend_rows"),
];

/// The temporary table of `table`'s current rows.
fn rows_of(table: &str) -> anyhow::Result<&'static str> {
    DOCUMENTS
        .iter()
        .chain(
            [
                (ITEM_SOURCE, "item_rows"),
                (SALE_RESULT_SOURCE, "sale_rows"),
                (PHOTO_SOURCE, "photo_rows"),
                (CODE_SOURCE, "code_rows"),
            ]
            .iter(),
        )
        .find(|(name, _)| *name == table)
        .map(|(_, rows)| *rows)
        .with_context(|| format!("{table} is no input of the 경매 kind"))
}

/// `built` of the PNUs of `@targets@`, from `entries`: the items in progress under their share of
/// the ceiling, the sale results under theirs and their bound, their total, and the place id.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH r AS ( \
  SELECT *, sum(length(entry_json) + 1) OVER (PARTITION BY pnu \
    ORDER BY sale_date, court_office_code, case_no, property_seq, item_seq \
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
  FROM entries WHERE kind = 'round' AND @targets@), \
rk AS ( \
  SELECT pnu, '[' || coalesce(string_agg(entry_json, ',' \
      ORDER BY sale_date, court_office_code, case_no, property_seq, item_seq) \
      FILTER (WHERE running <= @round_limit@), '') || ']' AS rounds_json, \
    bool_or(attached_via IS NULL) AS own, min(attached_via) AS via \
  FROM r GROUP BY pnu), \
s AS ( \
  SELECT *, sum(length(entry_json) + 1) OVER w AS running, row_number() OVER w AS n \
  FROM entries WHERE kind = 'sale' AND @targets@ \
  WINDOW w AS (PARTITION BY pnu \
    ORDER BY sale_date DESC NULLS LAST, court_office_code, case_no, property_seq, item_seq \
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)), \
sk AS ( \
  SELECT pnu, '[' || coalesce(string_agg(entry_json, ',' \
      ORDER BY sale_date DESC NULLS LAST, court_office_code, case_no, property_seq, item_seq) \
      FILTER (WHERE running <= @sale_limit@ AND n <= @sale_bound@), '') || ']' AS sales_json, \
    count(*) AS total, bool_or(attached_via IS NULL) AS own, min(attached_via) AS via \
  FROM s GROUP BY pnu), \
documents AS ( \
  SELECT pnu, \
    '{\"rounds\":' || coalesce(rk.rounds_json, '[]') || ',\"sale_results\":' || coalesce(sk.sales_json, '[]') \
    || ',\"sale_result_total\":' || coalesce(sk.total, 0) || ',\"source_datasets\":@datasets@' \
    || ',\"source_attribution\":@attribution@}' AS court_auction_json, \
    CASE WHEN coalesce(rk.own, false) OR coalesce(sk.own, false) THEN NULL \
         ELSE least(rk.via, sk.via) END AS attached_via \
  FROM rk FULL OUTER JOIN sk USING (pnu)) \
SELECT d.pnu, d.court_auction_json, d.attached_via, p.place_id, \
  sha256('{\"pnu\":' || to_json(d.pnu)::VARCHAR \
    || ',\"court_auction_json\":' || to_json(d.court_auction_json)::VARCHAR \
    || CASE WHEN d.attached_via IS NULL THEN '' ELSE ',\"attached_via\":' || to_json(d.attached_via)::VARCHAR END \
    || CASE WHEN p.place_id IS NULL THEN '' ELSE ',\"place_id\":' || to_json(p.place_id)::VARCHAR END || '}') \
    AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents d LEFT JOIN @place_ids@ p USING (pnu)";

const BUILT_TABLE: &str =
    "CREATE OR REPLACE TABLE built (pnu VARCHAR, court_auction_json VARCHAR, \
    attached_via VARCHAR, place_id VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, \
    published_at_utc VARCHAR);";

/// The 경매 kind.
#[derive(Clone, Copy, Debug)]
pub struct CourtAuctionKind;

/// The one 경매 kind.
pub const COURT_AUCTION: CourtAuctionKind = CourtAuctionKind;

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

/// The property names of a registry object, or of a list's element, in order.
fn names(field: &Value) -> anyhow::Result<Vec<String>> {
    let at = if field["items"].is_object() {
        &field["items"]
    } else {
        field
    };
    Ok(at["properties"]
        .as_object()
        .context("a block names its properties")?
        .keys()
        .cloned()
        .collect())
}

/// Each code column a 공통코드 group names (the source contract's `common_codes.groups[].names`),
/// by table: `(column, system, owner, group)`. Its name is served beside it as `<column less
/// _code>_name` (`court_auction_silver_to_gold.with_names`, root ADR-0540 §5).
fn named_codes() -> anyhow::Result<BTreeMap<String, Vec<NamedCode>>> {
    let contract: Value = serde_json::from_str(SOURCE_CONTRACT)?;
    let mut out: BTreeMap<String, Vec<NamedCode>> = BTreeMap::new();
    for group in contract["common_codes"]["groups"]
        .as_array()
        .context("the source contract lists its code groups")?
    {
        let text = |key: &str| group[key].as_str().map(str::to_owned);
        let system = text("system").context("a code group names its system")?;
        let owner = text("owner").unwrap_or_else(|| system.clone());
        let code_group = text("group").context("a code group names its group")?;
        for named in group["names"].as_array().into_iter().flatten() {
            let named = named.as_str().context("a named column is text")?;
            let (table, column) = named
                .rsplit_once('.')
                .with_context(|| format!("{named} names no table"))?;
            out.entry(table.to_owned()).or_default().push((
                column.to_owned(),
                system.clone(),
                owner.clone(),
                code_group.clone(),
            ));
        }
    }
    Ok(out)
}

/// A named code column and its group: `(column, system, owner, group)`.
type NamedCode = (String, String, String, String);

/// The column a code's name is served in: `result_code` → `result_name`.
fn name_of(column: &str) -> String {
    format!("{}_name", column.strip_suffix("_code").unwrap_or(column))
}

/// Whether `text` goes into the SQL as a quoted value as it is.
fn quotable(text: &str) -> bool {
    !text.contains('\'') && !text.contains('\\')
}

/// The view `<rows>_named`: `table`'s current rows with each named code's name beside it.
fn named_view(table: &str) -> anyhow::Result<String> {
    let rows = rows_of(table)?;
    let codes = named_codes()?;
    let named = codes.get(table).map(Vec::as_slice).unwrap_or_default();
    let mut replaced = Vec::new();
    let mut selected = Vec::new();
    let mut joins = Vec::new();
    for (n, (column, system, owner, group)) in named.iter().enumerate() {
        ensure!(
            is_plain(column) && [system, owner, group].iter().all(|text| quotable(text)),
            "a named code of {table} is not plain"
        );
        let name = name_of(column);
        if column_type(table, &name)?.is_some() {
            replaced.push(name.clone());
        }
        selected.push(format!("n{n}.name AS {name}"));
        joins.push(format!(
            "LEFT JOIN code_rows n{n} ON n{n}.system_code = '{system}' AND n{n}.owner_code = '{owner}' \
             AND n{n}.group_code = '{group}' AND n{n}.code = t.{column}"
        ));
    }
    let star = if replaced.is_empty() {
        "t.*".to_owned()
    } else {
        format!("t.* EXCLUDE ({})", replaced.join(", "))
    };
    let columns = std::iter::once(star)
        .chain(selected)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "CREATE OR REPLACE TEMP VIEW {rows}_named AS SELECT {columns} FROM {rows} t {};",
        joins.join(" ")
    ))
}

/// A struct of `fields` read from `alias`: a column of one of `tables` written as the document
/// writes it ([`value`]), a name in `direct` read as it is.
fn struct_of(
    alias: &str,
    fields: &[String],
    tables: &[&str],
    direct: &[&str],
) -> anyhow::Result<String> {
    let codes = named_codes()?;
    let parts = fields
        .iter()
        .map(|name| {
            let expression = if direct.contains(&name.as_str()) {
                format!("{alias}.{name}")
            } else {
                let mut found = None;
                for table in tables {
                    if let Some(kind) = column_type(table, name)? {
                        found = Some(value(alias, name, kind));
                        break;
                    }
                    // A code's name `with_names` gives the table.
                    if codes.get(*table).is_some_and(|named| {
                        named.iter().any(|(column, ..)| name_of(column) == *name)
                    }) {
                        found = Some(format!("{alias}.{name}"));
                        break;
                    }
                }
                found.with_context(|| {
                    format!("the field {name} is no column of {}", tables.join(" or "))
                })?
            };
            Ok(format!("'{name}': {expression}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("{{{}}}", parts.join(", ")))
}

/// The statements that leave one table `b_<name>(key…, v)` per block of a round's `detail`: an
/// object per key, or a list per key in the site's order (`court_auction_silver_to_gold.detail_parts`).
#[allow(clippy::too_many_lines)]
fn blocks() -> anyhow::Result<String> {
    let detail = shape(&["court_auction", "rounds", "[]", "detail"])?;
    let part = |name: &str| -> anyhow::Result<Vec<String>> { names(&detail["properties"][name]) };
    let object = |name: &str, key: &str, from: &str, table: &str| -> anyhow::Result<String> {
        Ok(format!(
            "CREATE OR REPLACE TEMP TABLE b_{name} AS SELECT {key}, {} AS v FROM {from} t;",
            struct_of("t", &part(name)?, &[table], &[])?
        ))
    };
    let list = |name: &str,
                key: &str,
                order: &str,
                from: &str,
                tables: &[&str],
                direct: &[&str]|
     -> anyhow::Result<String> {
        Ok(format!(
            "CREATE OR REPLACE TEMP TABLE b_{name} AS SELECT {key}, list({} ORDER BY {order}) AS v \
             FROM {from} t GROUP BY {key};",
            struct_of("t", &part(name)?, tables, direct)?
        ))
    };
    let survey = names(&detail["properties"]["survey"])?;
    let survey_parts = &detail["properties"]["survey"]["properties"];
    let appraisal = names(&detail["properties"]["appraisal"])?;
    let report = names(&detail["properties"]["appraisal"]["properties"]["report"])?;
    let notice_has_case =
        column_type("silver.court_auction_sale_notice_item", "case_no")?.is_some();
    Ok([
        object("case", CASE, "case_rows_named", "silver.court_auction_case")?,
        object("spec", PROPERTY, "spec_rows_named", "silver.court_auction_item_spec")?,
        list(
            "schedule",
            PROPERTY,
            "t.schedule_seq NULLS FIRST",
            "schedule_rows_named",
            &["silver.court_auction_item_schedule"],
            &[],
        )?,
        list(
            "objects",
            PROPERTY,
            "t.object_seq NULLS FIRST",
            "(SELECT o.*, x.closure_kind_name FROM object_rows_named o LEFT JOIN case_object_rows_named x \
              USING (court_office_code, case_no, object_seq))",
            &["silver.court_auction_item_object", "silver.court_auction_case_object"],
            &[],
        )?,
        list(
            "annexes",
            PROPERTY,
            "t.object_seq NULLS FIRST, t.annex_seq NULLS FIRST",
            "annex_rows_named",
            &["silver.court_auction_item_annex"],
            &[],
        )?,
        list(
            "appraisal_points",
            CASE,
            "t.point_seq NULLS FIRST",
            "point_rows_named",
            &["silver.court_auction_appraisal_point"],
            &[],
        )?,
        list(
            "related_cases",
            CASE,
            "t.related_seq NULLS FIRST",
            "related_rows_named",
            &["silver.court_auction_case_related"],
            &[],
        )?,
        // One entry per party kind, in the order of its first party, with how many.
        list(
            "parties",
            CASE,
            "t.first_seq NULLS FIRST",
            "(SELECT court_office_code, case_no, party_kind_name AS kind_name, \
               CAST(count(*) AS INTEGER) AS count, min(party_seq) AS first_seq \
             FROM party_rows_named GROUP BY court_office_code, case_no, party_kind_name)",
            &[],
            &["kind_name", "count"],
        )?,
        // The case's newest 현황조사 round, with its objects and its 임대차 lists.
        format!(
            "CREATE OR REPLACE TEMP TABLE b_survey_objects AS SELECT {CASE}, round_seq, \
               list({} ORDER BY t.object_seq NULLS FIRST) AS v \
             FROM survey_object_rows_named t GROUP BY {CASE}, round_seq; \
             CREATE OR REPLACE TEMP TABLE b_survey_tenants AS SELECT {CASE}, round_seq, \
               list({} ORDER BY t.tenant_seq NULLS FIRST) AS v \
             FROM tenant_rows_named t GROUP BY {CASE}, round_seq; \
             CREATE OR REPLACE TEMP TABLE b_survey AS SELECT {CASE}, {} AS v FROM ( \
               SELECT s.*, coalesce(o.v, []) AS objects, coalesce(n.v, []) AS tenants FROM ( \
                 SELECT * FROM survey_rows_named \
                 QUALIFY row_number() OVER (PARTITION BY {CASE} ORDER BY round_seq DESC NULLS LAST) = 1) s \
               LEFT JOIN b_survey_objects o USING ({CASE}, round_seq) \
               LEFT JOIN b_survey_tenants n USING ({CASE}, round_seq)) t;",
            struct_of(
                "t",
                &names(&survey_parts["objects"])?,
                &["silver.court_auction_survey_object"],
                &[]
            )?,
            struct_of(
                "t",
                &names(&survey_parts["tenants"])?,
                &["silver.court_auction_survey_tenant"],
                &[]
            )?,
            struct_of(
                "t",
                &survey,
                &["silver.court_auction_survey"],
                &["objects", "tenants"]
            )?,
        ),
        // The case's newest 감정평가 round, with its stored 감정평가서.
        format!(
            "CREATE OR REPLACE TEMP TABLE b_appraisal AS SELECT {CASE}, {} AS v FROM ( \
               SELECT a.*, CASE WHEN r.sha256 IS NOT NULL THEN {} END AS report FROM ( \
                 SELECT * FROM appraisal_rows_named \
                 QUALIFY row_number() OVER (PARTITION BY {CASE} ORDER BY round_seq DESC NULLS LAST) = 1) a \
               LEFT JOIN report_rows r USING ({CASE}, round_seq, appraisal_no, written_date)) t;",
            struct_of(
                "t",
                &appraisal,
                &["silver.court_auction_appraisal"],
                &["report"]
            )?,
            struct_of(
                "r",
                &report,
                &["silver.court_auction_appraisal_report"],
                &[]
            )?,
        ),
        list(
            "filings",
            CASE,
            "t.filing_seq NULLS FIRST",
            "filing_rows_named",
            &["silver.court_auction_case_filing"],
            &[],
        )?,
        list(
            "deliveries",
            CASE,
            "t.delivery_seq NULLS FIRST",
            "delivery_rows_named",
            &["silver.court_auction_case_delivery"],
            &[],
        )?,
        // A 정정·취소변경 notice names its case by the user number: the case table gives the key.
        list(
            "notice_changes",
            PROPERTY,
            "t.change_date NULLS FIRST, t.list NULLS FIRST, t.list_seq NULLS FIRST",
            &format!(
                "(SELECT n.* {}, k.case_no FROM notice_rows_named n \
                  JOIN (SELECT court_office_code, user_case_no, min(case_no) AS case_no FROM case_rows \
                        WHERE user_case_no IS NOT NULL GROUP BY court_office_code, user_case_no) k \
                  USING (court_office_code, user_case_no) \
                 WHERE n.list IN ('correction', 'cancellation') AND n.user_case_no IS NOT NULL \
                   AND n.property_seq IS NOT NULL)",
                if notice_has_case {
                    "EXCLUDE (case_no)"
                } else {
                    ""
                }
            ),
            &["silver.court_auction_sale_notice_item"],
            &[],
        )?,
        object(
            "dividend_notice",
            CASE,
            "dividend_rows_named",
            "silver.court_auction_dividend_notice",
        )?,
    ]
    .join(" "))
}

/// The key each block of a round's `detail` is joined by.
fn block_key(name: &str) -> &'static str {
    match name {
        "spec" | "schedule" | "objects" | "annexes" | "notice_changes" => PROPERTY,
        _ => CASE,
    }
}

/// The statement that leaves `round_detail(court_office_code, case_no, property_seq, detail)` of the
/// properties `servable` names: null for one whose 물건상세 was never read (no case and no spec).
fn round_detail() -> anyhow::Result<String> {
    let detail = shape(&["court_auction", "rounds", "[]", "detail"])?;
    let parts = names(&detail)?;
    let fields = parts
        .iter()
        .map(|name| {
            let is_list = names(&detail["properties"][name.as_str()]).is_ok()
                && detail["properties"][name.as_str()]["items"].is_object();
            if is_list {
                format!("'{name}': coalesce(b_{name}.v, [])")
            } else {
                format!("'{name}': b_{name}.v")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let joins = parts
        .iter()
        .map(|name| format!("LEFT JOIN b_{name} USING ({})", block_key(name)))
        .collect::<Vec<_>>()
        .join(" ");
    Ok(format!(
        "CREATE OR REPLACE TEMP TABLE round_detail AS SELECT {PROPERTY}, \
           CASE WHEN b_case.v IS NOT NULL OR b_spec.v IS NOT NULL THEN {{{fields}}} END AS detail \
         FROM (SELECT DISTINCT {PROPERTY} FROM servable WHERE _kind = 'round') r {joins};"
    ))
}

/// The statement that leaves `case_photos(court_office_code, case_no, photos)`: each case's
/// photos, by the site's kind then its order.
fn case_photos() -> anyhow::Result<String> {
    let fields = names(&shape(&["court_auction", "rounds", "[]", "photos"])?)?;
    let parts = fields
        .iter()
        .map(|name| {
            let expression = match name.as_str() {
                "media_kind" => "'photo'".to_owned(),
                "kind_code" => "p.photo_kind_code".to_owned(),
                "size_bytes" => "CAST(p.size_bytes AS BIGINT)".to_owned(),
                column => {
                    let kind = column_type(PHOTO_SOURCE, column)?.with_context(|| {
                        format!("the photo field {column} is no column of {PHOTO_SOURCE}")
                    })?;
                    value("p", column, kind)
                }
            };
            Ok(format!("'{name}': {expression}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    Ok(format!(
        "CREATE OR REPLACE TEMP TABLE case_photos AS SELECT {CASE}, list({{{parts}}} \
           ORDER BY p.photo_kind_code NULLS FIRST, p.sequence NULLS FIRST, p.sha256) AS photos \
         FROM photo_rows p GROUP BY {CASE};"
    ))
}

/// An entry of `kind` (`rounds` or `sale_results`) as a struct over `s` (a servable placed row).
fn entry(kind: &str, table: &str) -> anyhow::Result<String> {
    let fields = names(&shape(&["court_auction", kind])?)?;
    let parts = fields
        .iter()
        .map(|name| {
            let expression = match name.as_str() {
                "photos" => "coalesce(ph.photos, [])".to_owned(),
                "detail" => "rd.detail".to_owned(),
                "attached_via" => "s.attached_via".to_owned(),
                column => {
                    let kind = column_type(table, column)?
                        .with_context(|| format!("the field {column} is no column of {table}"))?;
                    value("s", column, kind)
                }
            };
            Ok(format!("'{name}': {expression}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("{{{}}}", parts.join(", ")))
}

/// The current rows of a document table at `source` into `into`: its newest edition per group, or
/// its newest version per key (the contract's one rendering, root ADR-0564 §4).
fn load_document(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    into: &str,
) -> anyhow::Result<u64> {
    let table_contract = contract(table)?;
    if table_contract.current_row_latest().is_some() {
        return load_current(connection, source, table, into);
    }
    let at = Instant::now();
    let read = crate::duckdb_parquet_stage::with_contract_columns(
        connection,
        table_contract,
        &source.of(table)?,
    )?;
    let columns: Vec<&str> = table_contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    let query = table_contract
        .newest_edition_sql(&read, &columns, "TRUE")
        .with_context(|| format!("{table} keeps neither versions per key nor editions"))?;
    connection.execute_batch(&format!("CREATE OR REPLACE TEMP TABLE {into} AS {query}"))?;
    Ok(elapsed(at))
}

impl KindVersionsStage for CourtAuctionKind {
    fn section_column(&self) -> &'static str {
        "court_auction_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        let mut inputs = vec![
            PARCEL_SOURCE,
            ITEM_SOURCE,
            SALE_RESULT_SOURCE,
            CODE_CHANGE_SOURCE,
            PHOTO_SOURCE,
            CODE_SOURCE,
        ];
        inputs.extend(DOCUMENTS.iter().map(|(table, _)| *table));
        inputs.extend([LINEAGE_SOURCE, BRIDGE_SOURCE]);
        inputs
    }

    fn whole_table_inputs(&self) -> Vec<&'static str> {
        self.inputs()
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let rendered = [
            blocks(),
            round_detail(),
            case_photos(),
            entry("rounds", ITEM_SOURCE),
            entry("sale_results", SALE_RESULT_SOURCE),
        ]
        .into_iter()
        .chain(DOCUMENTS.iter().map(|(table, _)| named_view(table)))
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
        for table in [ITEM_SOURCE, SALE_RESULT_SOURCE, PHOTO_SOURCE, CODE_SOURCE]
            .into_iter()
            .chain(DOCUMENTS.iter().map(|(table, _)| *table))
        {
            read_millis.insert(
                table.to_owned(),
                load_document(connection, source, table, rows_of(table)?)?,
            );
        }
        for (table, _) in DOCUMENTS {
            connection.execute_batch(&named_view(table)?)?;
        }
        // The items in progress as of the build's day, and every sale result, placed as one.
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE auction_rows AS \
               SELECT *, 'round' AS _kind FROM item_rows \
               WHERE (row_state IS NULL OR row_state <> '{REMOVED}') AND item_in_progress_yn = '{IN_PROGRESS}' \
                 AND sale_date IS NOT NULL \
                 AND sale_date >= CAST(make_timestamp((SELECT instant_us FROM as_of)) AS DATE) \
             UNION ALL BY NAME \
               SELECT *, 'sale' AS _kind, CAST(NULL AS VARCHAR) AS source_code_pnu_raw FROM sale_rows;"
        ))?;
        placement::crosswalk(connection)?;
        placement::locate(connection, "auction_rows", "pnu", "source_code_pnu_raw")?;
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
        connection.execute_batch(&blocks()?)?;
        connection.execute_batch(&round_detail()?)?;
        connection.execute_batch(&case_photos()?)?;
        let round = entry("rounds", ITEM_SOURCE)?;
        let sale = entry("sale_results", SALE_RESULT_SOURCE)?;
        let columns = "s.pnu, s._kind AS kind, s.sale_date, s.court_office_code, s.case_no, \
                       s.property_seq, s.item_seq, s.attached_via";
        connection.execute_batch(&fill(
            SPOOL_SQL,
            &[
                (
                    "query",
                    &format!(
                        "SELECT {columns}, to_json({round})::VARCHAR AS entry_json FROM servable s \
                           LEFT JOIN case_photos ph USING (court_office_code, case_no) \
                           LEFT JOIN round_detail rd USING (court_office_code, case_no, property_seq) \
                         WHERE s._kind = 'round' \
                         UNION ALL \
                         SELECT {columns}, to_json({sale})::VARCHAR AS entry_json FROM servable s \
                         WHERE s._kind = 'sale'"
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
                "SELECT CAST(NULL AS VARCHAR) AS pnu, CAST(NULL AS VARCHAR) AS kind, \
                 CAST(NULL AS DATE) AS sale_date, CAST(NULL AS VARCHAR) AS court_office_code, \
                 CAST(NULL AS VARCHAR) AS case_no, CAST(NULL AS INTEGER) AS property_seq, \
                 CAST(NULL AS INTEGER) AS item_seq, CAST(NULL AS VARCHAR) AS attached_via, \
                 CAST(NULL AS VARCHAR) AS entry_json WHERE FALSE"
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
            "the 경매 kind is built whole on every run (root ADR-0326)"
        );
        let datasets = format!(
            "[{}]",
            SOURCE_DATASETS
                .iter()
                .map(|dataset| format!("\"{dataset}\""))
                .collect::<Vec<_>>()
                .join(",")
        );
        let round_limit =
            (DOCUMENT_BYTE_LIMIT - DOCUMENT_ENVELOPE_BYTES - SALE_RESULT_BYTES).to_string();
        let at = Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("targets", "TRUE"),
                    ("round_limit", &round_limit),
                    ("sale_limit", &SALE_RESULT_BYTES.to_string()),
                    ("sale_bound", &SALE_RESULT_BOUND.to_string()),
                    ("datasets", &datasets),
                    ("attribution", &format!("\"{SOURCE_ATTRIBUTION}\"")),
                    ("place_ids", PLACE_IDS),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the court-auction document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
