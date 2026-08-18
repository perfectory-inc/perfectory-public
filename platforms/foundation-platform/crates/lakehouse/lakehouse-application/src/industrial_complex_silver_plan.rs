//! Silver handoff helpers for Catalog-owned industrial-complex rows.

use std::collections::BTreeMap;

use catalog_domain::IndustrialComplex;
use chrono::{DateTime, Utc};
use lakehouse_domain::{LakehouseTableContract, SILVER_INDUSTRIAL_COMPLEXES};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

const DEFAULT_COMPLEX_STATUS: &str = "unknown";

/// Input required to normalize Catalog industrial-complex aggregates into Silver rows.
pub struct IndustrialComplexSilverRowsInput<'a> {
    /// Catalog aggregates ordered by the caller.
    pub complexes: &'a [IndustrialComplex],
    /// Source-snapshot lineage id for this Catalog-to-lakehouse handoff.
    pub source_snapshot_id: &'a str,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Silver `silver.industrial_complexes` row prepared from one Catalog aggregate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexSilverRow {
    /// Stable foundation-platform complex identifier.
    pub complex_id: String,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable official industrial-complex name.
    pub complex_name: String,
    /// Lowercase whitespace-normalized name for search/sort projections.
    pub complex_name_normalized: String,
    /// Domain wire value for complex kind.
    pub complex_kind: String,
    /// Operational status wire value. `unknown` is used until a source provides status.
    pub status: String,
    /// Two-digit province/city code derived from `primary_bjdong_code`, when one exists.
    pub sido_code: Option<String>,
    /// Five-digit city/county/district code derived from `primary_bjdong_code`, when one exists.
    pub sigungu_code: Option<String>,
    /// Ten-digit legal-dong code derived from `primary_bjdong_code`.
    pub primary_bjdong_code: Option<String>,
    /// Optional official address text.
    pub address_text: Option<String>,
    /// Optional management-agency name.
    pub management_agency_name: Option<String>,
    /// Optional developer name.
    pub developer_name: Option<String>,
    /// Optional designation date.
    pub designated_date: Option<String>,
    /// Optional site-works start date.
    pub construction_start_date: Option<String>,
    /// Optional completion date.
    pub completion_date: Option<String>,
    /// Official complex area in square meters.
    pub official_area_sqm: Option<u64>,
    /// Optional site-formation progress percentage as exact decimal text.
    pub development_progress_percent: Option<String>,
    /// Optional `lot_sales_status` wire value.
    pub lot_sales_status: Option<String>,
    /// Optional business period exactly as the source wrote it.
    pub business_period_raw: Option<String>,
    /// Optional first month of the business period as `yyyy-MM`.
    pub business_period_start_month: Option<String>,
    /// Optional last month of the business period as `yyyy-MM`.
    pub business_period_end_month: Option<String>,
    /// Optional statute the designation was made under, verbatim.
    pub designation_basis_law_raw: Option<String>,
    /// Optional development method, verbatim.
    pub development_method_raw: Option<String>,
    /// Optional development purpose, verbatim.
    pub development_purpose_raw: Option<String>,
    /// Optional invited industry types, verbatim.
    pub invited_industries_raw: Option<String>,
    /// Stable lineage id for the Catalog source row.
    pub source_record_id: String,
    /// Source-snapshot lineage id.
    pub source_snapshot_id: String,
    /// UTC timestamp from which this fact is valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp until which this fact is valid.
    pub valid_to_utc: Option<DateTime<Utc>>,
    /// UTC timestamp when this fact entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
    /// Lowercase SHA-256 checksum of the row payload excluding this checksum field.
    pub row_checksum_sha256: String,
}

/// Writer-neutral JSONL handoff for `silver.industrial_complexes`.
///
/// This is transient transport for writers and tests. The canonical lakehouse table storage remains
/// the `LakehouseTableContract` physical format, currently `Parquet`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexSilverHandoff {
    /// Static lakehouse contract table name.
    pub contract_table_name: &'static str,
    /// Target table columns in static contract order.
    pub table_columns: Vec<String>,
    /// JSONL transport columns in stable writer input order.
    pub transport_columns: Vec<String>,
    /// Newline-delimited JSON records for a downstream Spark/Iceberg writer, not final lakehouse
    /// storage.
    pub jsonl: String,
    /// Quality metrics keyed using the same convention as `SparkRunSummary`.
    pub quality_metrics: BTreeMap<String, u64>,
    /// Number of distinct source snapshots represented by the handoff.
    pub source_snapshot_count: u64,
    /// Distinct source snapshot ids represented by the handoff.
    pub source_snapshot_ids: Vec<String>,
    /// Whether `source_snapshot_ids` was truncated by this builder.
    pub source_snapshot_truncated: bool,
}

/// Error returned while normalizing industrial complexes into Silver rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IndustrialComplexSilverPlanError {
    /// Input data cannot be represented as a Silver industrial-complex row.
    #[error("invalid industrial-complex Silver input: {0}")]
    InvalidInput(String),
}

/// Normalizes Catalog industrial-complex aggregates into Silver rows.
///
/// # Errors
/// Returns `IndustrialComplexSilverPlanError` when lineage is empty, required Catalog identity
/// fields are empty, or a `primary_bjdong_code` that is present is malformed. A complex that
/// carries no legal-dong code is normalized with all three region columns null: the contract stops
/// requiring them (root ADR-0035) and the canonical column stopped requiring one (root ADR-0040).
pub fn normalize_industrial_complex_silver_rows(
    input: &IndustrialComplexSilverRowsInput<'_>,
) -> Result<Vec<IndustrialComplexSilverRow>, IndustrialComplexSilverPlanError> {
    validate_lineage_part("source_snapshot_id", input.source_snapshot_id)?;

    input
        .complexes
        .iter()
        .map(|complex| {
            normalize_complex(complex, input).map_err(|error| {
                IndustrialComplexSilverPlanError::InvalidInput(format!(
                    "complex_id={} official_complex_code={}: {}",
                    complex.id, complex.official_complex_code, error
                ))
            })
        })
        .collect()
}

/// Builds a writer-neutral JSONL handoff from Silver industrial-complex rows.
///
/// # Errors
/// Returns `IndustrialComplexSilverPlanError` when a row has invalid required fields or JSON
/// serialization fails.
pub fn build_industrial_complex_silver_handoff(
    rows: &[IndustrialComplexSilverRow],
) -> Result<IndustrialComplexSilverHandoff, IndustrialComplexSilverPlanError> {
    let mut quality_metrics = required_quality_metrics(&SILVER_INDUSTRIAL_COMPLEXES);
    quality_metrics.insert("row_count".to_owned(), rows.len() as u64);
    quality_metrics.insert("invalid_official_area_count".to_owned(), 0);
    quality_metrics.insert("invalid_checksum_count".to_owned(), 0);

    let mut records = Vec::with_capacity(rows.len());
    let mut source_snapshot_ids = Vec::<String>::new();

    for row in rows {
        validate_handoff_row(row, &mut quality_metrics);
        if !source_snapshot_ids.contains(&row.source_snapshot_id) {
            source_snapshot_ids.push(row.source_snapshot_id.clone());
        }
        records.push(row_to_json_value(row));
    }

    source_snapshot_ids.sort();
    let jsonl = records
        .iter()
        .map(compact_json_line)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    let jsonl = if jsonl.is_empty() {
        String::new()
    } else {
        format!("{jsonl}\n")
    };

    Ok(IndustrialComplexSilverHandoff {
        contract_table_name: SILVER_INDUSTRIAL_COMPLEXES.table_name,
        table_columns: column_names(&SILVER_INDUSTRIAL_COMPLEXES),
        transport_columns: industrial_complex_transport_columns(),
        jsonl,
        quality_metrics,
        source_snapshot_count: source_snapshot_ids.len() as u64,
        source_snapshot_ids,
        source_snapshot_truncated: false,
    })
}

fn normalize_complex(
    complex: &IndustrialComplex,
    input: &IndustrialComplexSilverRowsInput<'_>,
) -> Result<IndustrialComplexSilverRow, IndustrialComplexSilverPlanError> {
    let official_complex_code = require_source_official_complex_code(
        "official_complex_code",
        complex.official_complex_code.as_str(),
    )?;
    let complex_name = require_clean_text("complex_name", complex.name.as_str())?;
    let primary_bjdong_code = complex
        .primary_bjdong_code
        .as_deref()
        .map(validate_primary_bjdong_code)
        .transpose()?;

    let mut row = IndustrialComplexSilverRow {
        complex_id: complex.id.to_string(),
        official_complex_code,
        complex_name_normalized: normalize_name(complex_name.as_str()),
        complex_name,
        complex_kind: complex.kind.wire_name().to_owned(),
        status: DEFAULT_COMPLEX_STATUS.to_owned(),
        // Both are prefixes of the legal-dong code, so both are unknown when it is
        // (root ADR-0034: a code carries its own granularity, and an absent one carries none).
        sido_code: primary_bjdong_code
            .as_ref()
            .map(|code| code[0..2].to_owned()),
        sigungu_code: primary_bjdong_code
            .as_ref()
            .map(|code| code[0..5].to_owned()),
        primary_bjdong_code,
        address_text: None,
        management_agency_name: None,
        developer_name: None,
        designated_date: None,
        // Read off the aggregate rather than nulled, unlike the five above. Those five predate the
        // canonical columns that now hold them and are a separate gap; these ten have a canonical
        // value from the day the column exists, and writing null beside a value the row carries
        // would claim the source stated nothing.
        construction_start_date: complex
            .construction_start_date
            .map(|date| date.format("%Y-%m-%d").to_string()),
        completion_date: None,
        official_area_sqm: (complex.area_m2 > 0).then_some(complex.area_m2),
        development_progress_percent: complex.development_progress_percent.clone(),
        lot_sales_status: complex
            .lot_sales_status
            .map(|status| status.wire_name().to_owned()),
        business_period_raw: complex.business_period_raw.clone(),
        business_period_start_month: complex.business_period_start_month.clone(),
        business_period_end_month: complex.business_period_end_month.clone(),
        designation_basis_law_raw: complex.designation_basis_law_raw.clone(),
        development_method_raw: complex.development_method_raw.clone(),
        development_purpose_raw: complex.development_purpose_raw.clone(),
        invited_industries_raw: complex.invited_industries_raw.clone(),
        source_record_id: format!(
            "foundation-platform:catalog.industrial_complex:{}",
            complex.id
        ),
        source_snapshot_id: input.source_snapshot_id.to_owned(),
        valid_from_utc: complex.updated_at,
        valid_to_utc: None,
        ingested_at_utc: input.ingested_at_utc,
        row_checksum_sha256: String::new(),
    };
    row.row_checksum_sha256 = row_checksum(&row)?;
    Ok(row)
}

fn validate_handoff_row(
    row: &IndustrialComplexSilverRow,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    record_required_string_quality("complex_id", &row.complex_id, quality_metrics);
    record_required_string_quality(
        "official_complex_code",
        &row.official_complex_code,
        quality_metrics,
    );
    record_required_string_quality("complex_name", &row.complex_name, quality_metrics);
    record_required_string_quality(
        "complex_name_normalized",
        &row.complex_name_normalized,
        quality_metrics,
    );
    record_required_string_quality("complex_kind", &row.complex_kind, quality_metrics);
    record_required_string_quality("status", &row.status, quality_metrics);
    record_optional_string_quality("sido_code", row.sido_code.as_deref(), quality_metrics);
    record_optional_string_quality("sigungu_code", row.sigungu_code.as_deref(), quality_metrics);
    record_required_string_quality("source_record_id", &row.source_record_id, quality_metrics);
    record_required_string_quality(
        "source_snapshot_id",
        &row.source_snapshot_id,
        quality_metrics,
    );
    record_required_string_quality(
        "row_checksum_sha256",
        &row.row_checksum_sha256,
        quality_metrics,
    );
    if row.official_area_sqm == Some(0) {
        increment_metric(quality_metrics, "invalid_official_area_count");
    }
    if !is_lowercase_sha256(&row.row_checksum_sha256) {
        increment_metric(quality_metrics, "invalid_checksum_count");
    }
}

fn required_quality_metrics(contract: &LakehouseTableContract) -> BTreeMap<String, u64> {
    let mut metrics = BTreeMap::from([("row_count".to_owned(), 0)]);
    for column in contract.columns.iter().filter(|column| column.required) {
        metrics.insert(format!("{}__null_count", column.name), 0);
        if column.logical_type == "string" {
            metrics.insert(format!("{}__empty_count", column.name), 0);
        }
    }
    metrics
}

fn record_required_string_quality(
    name: &'static str,
    value: &str,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    if value.is_empty() {
        increment_metric(quality_metrics, &format!("{name}__empty_count"));
    }
}

/// Counts an optional string column that is present but blank.
///
/// Absent is legal for these columns and empty is not: every string column of the contract must be
/// non-empty when it carries a value (root ADR-0035 decision 9). Collapsing the two would erase
/// exactly the distinction that decision protects.
fn record_optional_string_quality(
    name: &'static str,
    value: Option<&str>,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    if value.is_some_and(str::is_empty) {
        increment_metric(quality_metrics, &format!("{name}__empty_count"));
    }
}

fn increment_metric(metrics: &mut BTreeMap<String, u64>, name: &str) {
    *metrics.entry(name.to_owned()).or_insert(0) += 1;
}

/// Renders one row as the JSON object a `silver.industrial_complexes` writer consumes.
///
/// Written as a name/value table rather than a run of `insert` calls so the emitted key set is
/// readable next to the contract it has to equal. `tests/industrial_complex_silver_rows.rs` pins
/// that equality: this emitter spells every key by hand, and without the pin a widened contract
/// would leave a handoff a Spark job rejects at run time for a column nothing here could catch.
fn row_to_json_value(row: &IndustrialComplexSilverRow) -> JsonValue {
    let required = |value: &String| JsonValue::String(value.clone());
    let optional = optional_string_json;
    let entries: [(&str, JsonValue); 31] = [
        ("complex_id", required(&row.complex_id)),
        (
            "official_complex_code",
            required(&row.official_complex_code),
        ),
        ("complex_name", required(&row.complex_name)),
        (
            "complex_name_normalized",
            required(&row.complex_name_normalized),
        ),
        ("complex_kind", required(&row.complex_kind)),
        ("status", required(&row.status)),
        ("sido_code", optional(row.sido_code.as_ref())),
        ("sigungu_code", optional(row.sigungu_code.as_ref())),
        (
            "primary_bjdong_code",
            optional(row.primary_bjdong_code.as_ref()),
        ),
        ("address_text", optional(row.address_text.as_ref())),
        (
            "management_agency_name",
            optional(row.management_agency_name.as_ref()),
        ),
        ("developer_name", optional(row.developer_name.as_ref())),
        ("designated_date", optional(row.designated_date.as_ref())),
        (
            "construction_start_date",
            optional(row.construction_start_date.as_ref()),
        ),
        ("completion_date", optional(row.completion_date.as_ref())),
        (
            "official_area_sqm",
            row.official_area_sqm
                .map_or(JsonValue::Null, JsonValue::from),
        ),
        (
            "development_progress_percent",
            optional(row.development_progress_percent.as_ref()),
        ),
        ("lot_sales_status", optional(row.lot_sales_status.as_ref())),
        (
            "business_period_raw",
            optional(row.business_period_raw.as_ref()),
        ),
        (
            "business_period_start_month",
            optional(row.business_period_start_month.as_ref()),
        ),
        (
            "business_period_end_month",
            optional(row.business_period_end_month.as_ref()),
        ),
        (
            "designation_basis_law_raw",
            optional(row.designation_basis_law_raw.as_ref()),
        ),
        (
            "development_method_raw",
            optional(row.development_method_raw.as_ref()),
        ),
        (
            "development_purpose_raw",
            optional(row.development_purpose_raw.as_ref()),
        ),
        (
            "invited_industries_raw",
            optional(row.invited_industries_raw.as_ref()),
        ),
        ("source_record_id", required(&row.source_record_id)),
        ("source_snapshot_id", required(&row.source_snapshot_id)),
        (
            "valid_from_utc",
            JsonValue::String(timestamp_json(row.valid_from_utc)),
        ),
        ("valid_to_utc", JsonValue::Null),
        (
            "ingested_at_utc",
            JsonValue::String(timestamp_json(row.ingested_at_utc)),
        ),
        ("row_checksum_sha256", required(&row.row_checksum_sha256)),
    ];
    JsonValue::Object(
        entries
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect::<JsonMap<_, _>>(),
    )
}

fn row_checksum(
    row: &IndustrialComplexSilverRow,
) -> Result<String, IndustrialComplexSilverPlanError> {
    let mut payload = row_to_json_value(row);
    if let JsonValue::Object(record) = &mut payload {
        record.remove("row_checksum_sha256");
    }
    Ok(sha256_hex(compact_json_line(&payload)?.as_bytes()))
}

fn optional_string_json(value: Option<&String>) -> JsonValue {
    value.map_or(JsonValue::Null, |value| JsonValue::String(value.clone()))
}

fn timestamp_json(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn compact_json_line(value: &JsonValue) -> Result<String, IndustrialComplexSilverPlanError> {
    serde_json::to_string(value)
        .map_err(|error| IndustrialComplexSilverPlanError::InvalidInput(error.to_string()))
}

fn column_names(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect()
}

/// Contract columns this handoff supplies: everything except the two the writer derives.
///
/// Read off the contract rather than listed here. A hand-written copy of a column list beside the
/// contract it copies is the defect, not the drift it later produces: widening the contract left a
/// second list behind, silently, with nothing that could fail. `tests/industrial_complex_silver_\
/// rows.rs` pins both this list and the emitted record's key set to the contract.
const HANDOFF_WRITER_DERIVED_COLUMNS: &[&str] = &["complex_name_normalized", "valid_to_utc"];

fn industrial_complex_transport_columns() -> Vec<String> {
    SILVER_INDUSTRIAL_COMPLEXES
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !HANDOFF_WRITER_DERIVED_COLUMNS.contains(name))
        .map(str::to_owned)
        .collect()
}

fn validate_lineage_part(
    label: &'static str,
    value: &str,
) -> Result<(), IndustrialComplexSilverPlanError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(());
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(format!(
        "{label} must be non-empty text without surrounding whitespace"
    )))
}

fn require_clean_text(
    label: &'static str,
    value: &str,
) -> Result<String, IndustrialComplexSilverPlanError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(value.to_owned());
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(format!(
        "{label} must be non-empty text without surrounding whitespace"
    )))
}

fn require_source_official_complex_code(
    label: &'static str,
    value: &str,
) -> Result<String, IndustrialComplexSilverPlanError> {
    let value = require_clean_text(label, value)?;
    if !value.starts_with("foundation-platform:") {
        return Ok(value);
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(
        "official_complex_code must be source-side, not a foundation-platform migration placeholder"
            .to_owned(),
    ))
}

fn validate_primary_bjdong_code(value: &str) -> Result<String, IndustrialComplexSilverPlanError> {
    if value.len() == 10 && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(value.to_owned());
    }
    Err(IndustrialComplexSilverPlanError::InvalidInput(format!(
        "primary_bjdong_code must be exactly 10 ASCII digits: {value}"
    )))
}

fn normalize_name(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}
