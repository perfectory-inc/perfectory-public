//! The HUB `|` bulk files that are one lane each by a contract entry alone (root ADR-0551).
//!
//! A family file (`infra/lakehouse/contracts/hub-lane-family-*.v1.json`) holds one entry per lane:
//! its ZIP member, its column count, the columns it names (index and the provider's word), how its
//! key is made and its compare bucket width. Everything else a lane is made of is the same for every
//! entry and derived here, as the by-PNU kind tables are from their lane lines (root ADR-0193):
//!
//! - the lane id is the entry's name, the Bronze source `hubgokr__<id with _>`, the Silver table
//!   `silver.<id with _>`, the export `export-<id>-silver-handoff`;
//! - the Silver table contract: `mgmt_key`, the named columns in file order, then the PNU, the row's
//!   raw columns, the lineage every HUB part lane writes and the removal marker (root ADR-0318);
//! - the source contract the shared engine and the Silver refresh read, in the schema of a hand
//!   lane's `hub-*-source-objects.json` (root ADR-0092, ADR-0169).
//!
//! Adding a lane is adding an entry: no code, list or count names it.

use std::{collections::BTreeMap, sync::OnceLock};

use serde::{de::IgnoredAny, Deserialize};
use serde_json::{json, Map, Value};

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

/// The family files, by name. A new lane is an entry in one of them; a new file is a new program.
const FAMILY_FILES: [(&str, &str); 2] = [
    (
        "hub-lane-family-housing-permit.v1.json",
        include_str!(
            "../../../../infra/lakehouse/contracts/hub-lane-family-housing-permit.v1.json"
        ),
    ),
    (
        "hub-lane-family-building-permit.v1.json",
        include_str!(
            "../../../../infra/lakehouse/contracts/hub-lane-family-building-permit.v1.json"
        ),
    ),
];

/// The 지번 parts every family file names: a family lane composes its `pnu` from them.
const PNU_INPUTS: [&str; 5] = ["sigungu_cd", "bjdong_cd", "san_gubun", "bonbeon", "bubeon"];
/// The columns the export writes itself after the named ones, in table order.
const LINEAGE: [(&str, &str, bool); 8] = [
    ("pnu", "string", false),
    ("raw_columns", "array<string>", true),
    ("vintage", "string", true),
    ("source_record_id", "string", true),
    ("source_part_id", "string", true),
    ("source_line_number", "long", true),
    ("source_snapshot_id", "string", true),
    ("ingested_at_utc", "timestamp", true),
];
const REMOVAL: &str = "row_state";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FamilyFile {
    schema_version: u32,
    #[serde(rename = "$comment")]
    _comment: String,
    decision: String,
    field_layout_authority: String,
    measured_at_utc: String,
    defaults: Defaults,
    lanes: BTreeMap<String, Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Defaults {
    rows_per_part: u64,
    max_row_bytes: u64,
    spark: Value,
    /// The Silver refresh job every lane of the file shares; `declared_definitions.py` reads it.
    #[serde(rename = "job")]
    _job: IgnoredAny,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    title: String,
    /// The Silver table's description in the pipeline graph (`declared_definitions.py`).
    description: String,
    /// The first run's computed estimate, for the job (`declared_definitions.py`).
    #[serde(rename = "first_run")]
    _first_run: IgnoredAny,
    inner_file: String,
    column_count: usize,
    key: Key,
    /// A file some of whose values hold a line break: the engine joins a row cut short with the
    /// lines that complete it.
    #[serde(default)]
    broken_lines: Option<BrokenLines>,
    bucket_prefix_length: u32,
    how_measured: String,
    columns: BTreeMap<String, (usize, String)>,
    #[serde(default)]
    spark: Option<Value>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Key {
    /// The named `mgmt_key` column: a provider key that is filled on every row and parts them.
    Column,
    /// The row's own text (root ADR-0397): no column parts the rows.
    Sha256OfRow,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokenLines {
    /// How many rows of the measured release a line break cut in two, each joined back exactly.
    measured_rows: u64,
}

/// One lane of the family, derived from its entry.
#[derive(Debug)]
pub struct HubFamilyLane {
    /// The Silver refresh lane and systemd instance name (`housing-permit-unit-outline`).
    pub id: &'static str,
    /// The family file that holds the entry.
    pub family_file: &'static str,
    /// The provider's name of the file (`주택인허가 호별개요`).
    pub title: &'static str,
    /// The Bronze source slug.
    pub source_slug: &'static str,
    /// The publisher command that exports a release.
    pub export: &'static str,
    /// The environment prefix the export reads its input and output from.
    pub env_prefix: &'static str,
    /// The Silver table.
    pub table: LakehouseTableContract,
    /// The lane's source contract, in the schema of a hand lane's `hub-*-source-objects.json`.
    pub source_document: &'static str,
}

/// Every family lane, in family file then lane name order; empty when a family file cannot be
/// read (`hub_family_lane_error` says why, and the contract tests refuse it).
#[must_use]
pub fn hub_family_lanes() -> &'static [HubFamilyLane] {
    derived().as_ref().map_or(&[], Vec::as_slice)
}

/// Why the family lanes could not be derived, or `None`.
#[must_use]
pub fn hub_family_lane_error() -> Option<&'static str> {
    derived().as_ref().err().map(String::as_str)
}

/// The family lane `export` runs, if any.
#[must_use]
pub fn hub_family_lane_by_export(export: &str) -> Option<&'static HubFamilyLane> {
    hub_family_lanes().iter().find(|lane| lane.export == export)
}

/// The family lane of a Silver refresh lane id, if any.
#[must_use]
pub fn hub_family_lane_by_id(id: &str) -> Option<&'static HubFamilyLane> {
    hub_family_lanes().iter().find(|lane| lane.id == id)
}

/// The Silver tables of the family, for the table contract list.
pub fn hub_family_tables() -> Vec<LakehouseTableContract> {
    hub_family_lanes().iter().map(|lane| lane.table).collect()
}

fn derived() -> &'static Result<Vec<HubFamilyLane>, String> {
    static LANES: OnceLock<Result<Vec<HubFamilyLane>, String>> = OnceLock::new();
    LANES.get_or_init(|| derive(&FAMILY_FILES))
}

fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

/// The lanes of family files given as `(file name, text)`.
///
/// # Errors
/// Names the file and lane of an entry that cannot be a lane: an id that is not lowercase words, a
/// column the export writes itself, two columns at one index or one outside the row, a missing 지번
/// part, a key rule that does not match the named columns, or a lane two files both declare.
pub fn derive(files: &[(&'static str, &'static str)]) -> Result<Vec<HubFamilyLane>, String> {
    let mut lanes: Vec<HubFamilyLane> = Vec::new();
    for &(name, text) in files {
        let file: FamilyFile =
            serde_json::from_str(text).map_err(|error| format!("{name}: {error}"))?;
        if file.schema_version != 1 {
            return Err(format!("{name}: unsupported schema_version"));
        }
        for (id, entry) in &file.lanes {
            if lanes.iter().any(|lane| lane.id == id) {
                return Err(format!("{name}: lane {id} is declared twice"));
            }
            lanes.push(
                lane(name, &file, id, entry).map_err(|error| format!("{name}: {id}: {error}"))?,
            );
        }
    }
    Ok(lanes)
}

fn lane(
    family_file: &'static str,
    file: &FamilyFile,
    id: &str,
    entry: &Entry,
) -> Result<HubFamilyLane, String> {
    let named = checked_columns(id, entry)?;
    let snake = id.replace('-', "_");
    let table = table_contract(&snake, &named);
    let source_slug = format!("hubgokr__{snake}");
    let export = format!("export-{id}-silver-handoff");
    let document = source_document(
        family_file,
        file,
        entry,
        &table,
        &source_slug,
        &export,
        &named,
    );
    Ok(HubFamilyLane {
        id: leak(id.to_owned()),
        family_file,
        title: leak(entry.title.clone()),
        source_slug: leak(source_slug),
        export: leak(export),
        env_prefix: leak(format!(
            "FOUNDATION_PLATFORM_{}",
            snake.to_ascii_uppercase()
        )),
        table,
        source_document: leak(document.to_string()),
    })
}

/// The entry's named columns other than `mgmt_key`, in the order the file holds them, once the
/// entry is one a lane can be made of.
fn checked_columns<'e>(id: &str, entry: &'e Entry) -> Result<Vec<&'e str>, String> {
    let words = id.split('-').collect::<Vec<_>>();
    if words.len() < 2
        || !words
            .iter()
            .all(|word| !word.is_empty() && word.bytes().all(|b| b.is_ascii_lowercase()))
    {
        return Err("a lane id is lowercase words joined by '-'".to_owned());
    }
    let mut by_index = BTreeMap::new();
    for (column, (index, meaning)) in &entry.columns {
        let generated = LINEAGE.iter().any(|(name, _, _)| name == column) || column == REMOVAL;
        if generated
            || column.is_empty()
            || !column
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(format!("{column:?} is not a column a HUB file can name"));
        }
        if *index >= entry.column_count || by_index.insert(*index, column.as_str()).is_some() {
            return Err(format!("{column} sits outside the row or on a named index"));
        }
        if meaning.trim().is_empty() {
            return Err(format!("{column} names no meaning"));
        }
    }
    if let Some(missing) = PNU_INPUTS
        .iter()
        .find(|input| !entry.columns.contains_key(**input))
    {
        return Err(format!("the 지번 part {missing} is not named"));
    }
    if entry.columns.contains_key("mgmt_key") != (entry.key == Key::Column) {
        return Err("key column names a mgmt_key column; key sha256_of_row names none".to_owned());
    }
    if [&entry.how_measured, &entry.title, &entry.description]
        .iter()
        .any(|text| text.trim().is_empty())
    {
        return Err("an entry says what it is and how it was measured".to_owned());
    }
    if entry
        .broken_lines
        .as_ref()
        .is_some_and(|broken| broken.measured_rows == 0)
    {
        return Err("broken_lines states how many measured rows needed it".to_owned());
    }
    Ok(by_index
        .into_values()
        .filter(|column| *column != "mgmt_key")
        .collect())
}

/// The Silver table: `mgmt_key`, the named columns, the lineage and the removal marker.
fn table_contract(snake: &str, named: &[&str]) -> LakehouseTableContract {
    let mut columns = vec![column("mgmt_key", "string", false)];
    columns.extend(
        named
            .iter()
            .map(|name| column(leak((*name).to_owned()), "string", false)),
    );
    columns.extend(
        LINEAGE
            .iter()
            .map(|(name, kind, required)| column(name, kind, *required)),
    );
    columns.push(column(REMOVAL, "string", false));
    LakehouseTableContract {
        table_name: leak(format!("silver.{snake}")),
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        // A monthly release appends only its changed keys and a removal marker per vanished key
        // (root ADR-0318); a key's version is its newest vintage's rows (ADR-0328).
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: &["mgmt_key"],
            newest_first: &["vintage", "ingested_at_utc"],
            removal_column: Some(REMOVAL),
            keep_ties: true,
        }),
        columns: Box::leak(columns.into_boxed_slice()),
        partition_spec: &["vintage"],
        sort_order: &["pnu", "mgmt_key"],
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "raw_columns_not_null",
            "vintage_not_null",
            "source_part_id_not_null",
        ],
        load: LakehouseLoadUnit::Object {
            column: "source_part_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    }
}

/// The lane's source contract in the schema of a hand lane's `hub-*-source-objects.json`.
fn source_document(
    family_file: &str,
    file: &FamilyFile,
    entry: &Entry,
    table: &LakehouseTableContract,
    source_slug: &str,
    export: &str,
    named: &[&str],
) -> Value {
    let mut content = named.iter().map(|name| json!(name)).collect::<Vec<_>>();
    content.extend([json!("pnu"), json!("raw_columns")]);
    let mut layout = Map::new();
    for (name, (index, meaning)) in &entry.columns {
        layout.insert(name.clone(), json!({"index": index, "meaning": meaning}));
    }
    let mut document = json!({
        "schema_version": 1,
        "decision": file.decision,
        "source": format!("bronze/source={source_slug}"),
        "measured_at_utc": file.measured_at_utc,
        "how_measured": entry.how_measured,
        "field_layout_authority": file.field_layout_authority,
        "load_granularity": "national",
        "inner_file": entry.inner_file,
        "csv_delimiter": "|",
        "encoding": "utf-8",
        "has_header": false,
        "column_count": entry.column_count,
        "columns": layout,
        "handoff_prefix": format!("silver-handoff/{source_slug}"),
        "handoff_suffix": ".jsonl.gz",
        "handoff_layout": "manifest_parts",
        "rows_per_part": file.defaults.rows_per_part,
        "max_row_bytes": file.defaults.max_row_bytes,
        "silver_refresh": {
            "$comment": format!("Run by foundation-silver-refresh@hub-lane-family.service, derived from {family_file} (root ADR-0551)."),
            "table": table.table_name,
            "roles": {"source": source_slug},
            "export": export,
            "spark": entry.spark.clone().unwrap_or_else(|| file.defaults.spark.clone()),
            "compare": {
                "content_columns": content,
                "bucket_prefix_length": entry.bucket_prefix_length,
            },
        },
    });
    if entry.key == Key::Sha256OfRow {
        document["row_key"] = json!("sha256_of_row");
    }
    if entry.broken_lines.is_some() {
        document["joins_broken_lines"] = json!(true);
    }
    document
}

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family(lanes: &str) -> &'static str {
        leak(format!(
            r#"{{"schema_version": 1, "$comment": "x", "decision": "d", "field_layout_authority": "a",
                "measured_at_utc": "2026-11-12",
                "defaults": {{"rows_per_part": 1000000, "max_row_bytes": 1048576, "spark": {{}}, "job": {{}}}},
                "lanes": {lanes}}}"#
        ))
    }

    const PNU: &str = r#""sigungu_cd": [2, "시군구코드"], "bjdong_cd": [3, "법정동코드"],
        "san_gubun": [4, "대지구분코드"], "bonbeon": [5, "번"], "bubeon": [6, "지"]"#;

    fn entry(key: &str, columns: &str) -> String {
        format!(
            r#"{{"synthetic-lane": {{"title": "t", "description": "d", "first_run": {{}},
                "inner_file": "x.txt", "column_count": 9,
                "key": "{key}", "bucket_prefix_length": 0, "how_measured": "m",
                "columns": {{{PNU}{columns}}}}}}}"#
        )
    }

    #[test]
    fn the_family_files_derive() {
        assert_eq!(hub_family_lane_error(), None);
        for lane in hub_family_lanes() {
            assert_eq!(
                hub_family_lane_by_export(lane.export).map(|l| l.id),
                Some(lane.id)
            );
            assert_eq!(lane.table.columns[0].name, "mgmt_key");
        }
    }

    #[test]
    fn an_entry_is_a_whole_lane() -> Result<(), String> {
        let text = family(&entry(
            "column",
            r#", "mgmt_key": [0, "PK"], "area_m2": [8, "면적"]"#,
        ));
        let lanes = derive(&[("f.json", text)])?;
        let lane = &lanes[0];
        assert_eq!(lane.table.table_name, "silver.synthetic_lane");
        assert_eq!(lane.source_slug, "hubgokr__synthetic_lane");
        assert_eq!(lane.export, "export-synthetic-lane-silver-handoff");
        assert_eq!(lane.env_prefix, "FOUNDATION_PLATFORM_SYNTHETIC_LANE");
        let names = lane
            .table
            .columns
            .iter()
            .map(|c| c.name)
            .collect::<Vec<_>>();
        assert_eq!(
            &names[..7],
            [
                "mgmt_key",
                "sigungu_cd",
                "bjdong_cd",
                "san_gubun",
                "bonbeon",
                "bubeon",
                "area_m2"
            ]
        );
        assert_eq!(names.last(), Some(&"row_state"));
        let document: Value =
            serde_json::from_str(lane.source_document).map_err(|e| e.to_string())?;
        assert_eq!(document["columns"]["area_m2"]["index"], 8);
        assert_eq!(
            document["silver_refresh"]["roles"]["source"],
            "hubgokr__synthetic_lane"
        );
        assert!(document.get("row_key").is_none());
        Ok(())
    }

    #[test]
    fn an_entry_that_cannot_be_a_lane_is_refused() {
        for (why, lanes) in [
            (
                "sha key with a key column",
                entry("sha256_of_row", r#", "mgmt_key": [0, "PK"]"#),
            ),
            ("column key without one", entry("column", "")),
            (
                "two columns on one index",
                entry("sha256_of_row", r#", "a": [2, "x"]"#),
            ),
            (
                "outside the row",
                entry("sha256_of_row", r#", "a": [9, "x"]"#),
            ),
            (
                "a generated column",
                entry("sha256_of_row", r#", "pnu": [7, "x"]"#),
            ),
            ("no meaning", entry("sha256_of_row", r#", "a": [7, " "]"#)),
            (
                "a 지번 part missing",
                entry("sha256_of_row", "").replace(r#""bubeon": [6, "지"]"#, r#""x": [6, "지"]"#),
            ),
        ] {
            assert!(
                derive(&[("f.json", family(&lanes))]).is_err(),
                "accepted {why}"
            );
        }
    }
}
