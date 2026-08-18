//! Silver normalization helpers for industrial-complex boundary rows.
//!
//! The shape of this handoff follows `vworld_cadastral_silver_plan`: rows carry standard
//! little-endian WKB, and the JSONL transport carries that WKB as `geometry_wkb_hex` plus
//! `geometry_wkb_encoding=hex` because JSON has no lossless binary scalar.
//!
//! Two contract columns are deliberately left null here and filled by the writer:
//!
//! - `complex_id` — the only definition of an industrial complex's id lives in
//!   `industrial_complex_bronze_to_silver.py`, which derives it while writing
//!   `silver.industrial_complexes`. Recomputing it here would put the same rule in two places, and
//!   a boundary whose id is derived by a second implementation joins to nothing the moment the two
//!   drift. The writer reads it off `silver.industrial_complexes` instead.
//! - `sido_code` — required and a partition key, but the boundary source does not carry an
//!   administrative code at all. It comes from the same join.
//!
//! What travels instead is `official_complex_code`, which is what the boundary source does say and
//! what the join is on.

use std::{collections::BTreeMap, fmt::Write as _};

use chrono::{DateTime, Utc};
use lakehouse_domain::{LakehouseTableContract, SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::polygonal_geometry::{
    geometry_area, geometry_bounding_box, geometry_centroid, geometry_to_wkb, GeoPoint,
    GeometryBoundingBox, ParsedPolygonalGeometry, PolygonalGeometryError,
};

/// Spatial reference id the industrial-complex boundary source publishes.
///
/// Korea 2000 / Central Belt 2010, in metres. It is carried through unchanged: neither this
/// workspace nor the writer image has a projection library, and reprojection belongs to `PostGIS` at
/// the serving edge (root ADR-0042).
pub const INDUSTRIAL_COMPLEX_BOUNDARY_SRID: i32 = 5186;

/// `boundary_kind` for a boundary the authority itself published.
///
/// The allowed values are `official`, `derived`, `corrected`, and `draft`
/// (`docs/catalog/industrial-complex-lakehouse-poc.md` §4.2). This source is the designation
/// boundary as published, so it is `official`; the contract gate "active official boundary is at
/// most one per `complex_id`" is about exactly this value.
pub const BOUNDARY_KIND_OFFICIAL: &str = "official";

/// One industrial-complex boundary as read from the provider source.
#[derive(Clone, Debug, PartialEq)]
pub struct IndustrialComplexBoundarySource {
    /// Source-side official industrial-complex code, the join key to `silver.industrial_complexes`.
    pub official_complex_code: String,
    /// Polygonal geometry in [`INDUSTRIAL_COMPLEX_BOUNDARY_SRID`].
    pub geometry: ParsedPolygonalGeometry,
}

/// Input required to normalize provider boundaries into Silver rows.
pub struct IndustrialComplexBoundarySilverRowsInput<'a> {
    /// Boundary records in source order.
    pub boundaries: &'a [IndustrialComplexBoundarySource],
    /// Bronze object key the boundaries were decoded from.
    pub source_record_id: &'a str,
    /// Source-snapshot lineage id for this normalization batch.
    pub source_snapshot_id: &'a str,
    /// UTC timestamp from which these boundaries are valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Silver `silver.industrial_complex_boundaries` row prepared from one provider boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct IndustrialComplexBoundarySilverRow {
    /// Stable boundary identity.
    pub boundary_id: String,
    /// Source-side official industrial-complex code the writer joins on.
    pub official_complex_code: String,
    /// Which kind of boundary this row states.
    pub boundary_kind: &'static str,
    /// Standard little-endian WKB geometry bytes for `GeoParquet` writers.
    pub geometry_wkb: Vec<u8>,
    /// Spatial reference id for the geometry.
    pub geometry_srid: i32,
    /// Bounding box derived from the full geometry.
    pub bbox: GeometryBoundingBox,
    /// Area-weighted centroid of the full geometry.
    pub centroid: GeoPoint,
    /// Area enclosed by the geometry, in square metres.
    pub area_sqm_calculated: f64,
    /// Lowercase SHA-256 checksum of `geometry_wkb`.
    pub geometry_checksum_sha256: String,
    /// Source-record lineage id.
    pub source_record_id: String,
    /// Source-snapshot lineage id.
    pub source_snapshot_id: String,
    /// UTC timestamp from which the row is valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp until which the row is valid.
    pub valid_to_utc: Option<DateTime<Utc>>,
    /// UTC timestamp when the row entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Writer-neutral JSONL handoff for `silver.industrial_complex_boundaries`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexBoundarySilverHandoff {
    /// Static lakehouse contract table name.
    pub contract_table_name: &'static str,
    /// Target table columns in static contract order.
    pub table_columns: Vec<String>,
    /// JSONL transport columns in stable writer input order.
    pub transport_columns: Vec<String>,
    /// Newline-delimited JSON records for a downstream `GeoParquet` writer, not final lakehouse
    /// storage.
    pub jsonl: String,
    /// Quality metrics keyed using the same convention as `SparkRunSummary`.
    pub quality_metrics: BTreeMap<String, u64>,
    /// Number of distinct source snapshots represented by the handoff.
    pub source_snapshot_count: u64,
    /// Distinct source snapshot ids represented by the handoff.
    pub source_snapshot_ids: Vec<String>,
}

/// Error returned while normalizing provider boundaries into Silver rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IndustrialComplexBoundarySilverPlanError {
    /// Input data cannot be represented as a Silver boundary row.
    #[error("invalid industrial-complex Silver boundary input: {0}")]
    InvalidInput(String),
}

impl From<PolygonalGeometryError> for IndustrialComplexBoundarySilverPlanError {
    fn from(error: PolygonalGeometryError) -> Self {
        Self::InvalidInput(error.to_string())
    }
}

/// Normalizes provider boundaries into Silver boundary rows, in input order.
///
/// # Errors
/// Returns `IndustrialComplexBoundarySilverPlanError` when lineage fields are empty, an official
/// complex code is empty, a geometry encloses no area, or a geometry cannot be encoded as WKB.
pub fn normalize_industrial_complex_boundary_silver_rows(
    input: &IndustrialComplexBoundarySilverRowsInput<'_>,
) -> Result<Vec<IndustrialComplexBoundarySilverRow>, IndustrialComplexBoundarySilverPlanError> {
    validate_lineage_part("source_record_id", input.source_record_id)?;
    validate_lineage_part("source_snapshot_id", input.source_snapshot_id)?;

    input
        .boundaries
        .iter()
        .map(|boundary| normalize_boundary(boundary, input))
        .collect()
}

/// Builds a writer-neutral JSONL handoff from Silver boundary rows.
///
/// `orphan_boundary_count` and `complex_without_boundary_count` are the two facts this stage cannot
/// know — both need the complex list, which the writer holds — so they are recorded as zero here
/// and overwritten by the writer's own run summary. What this stage can say, it says: how many
/// source records it read, how many it could not turn into a row, and why.
///
/// # Errors
/// Returns `IndustrialComplexBoundarySilverPlanError` when a row cannot be serialized.
pub fn build_industrial_complex_boundary_silver_handoff(
    rows: &[IndustrialComplexBoundarySilverRow],
) -> Result<IndustrialComplexBoundarySilverHandoff, IndustrialComplexBoundarySilverPlanError> {
    let mut quality_metrics = required_quality_metrics(&SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES);
    quality_metrics.insert("row_count".to_owned(), rows.len() as u64);
    quality_metrics.insert("invalid_bbox_count".to_owned(), 0);
    quality_metrics.insert("invalid_checksum_count".to_owned(), 0);
    quality_metrics.insert("invalid_geometry_wkb_count".to_owned(), 0);
    quality_metrics.insert("centroid_outside_bbox_count".to_owned(), 0);
    quality_metrics.insert("non_positive_area_count".to_owned(), 0);

    // `complex_id` and `sido_code` are filled by the writer from its join, so counting their nulls
    // here would report a defect that is really this stage's contract with the next one.
    quality_metrics.remove("complex_id__null_count");
    quality_metrics.remove("complex_id__empty_count");
    quality_metrics.remove("sido_code__null_count");
    quality_metrics.remove("sido_code__empty_count");

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

    Ok(IndustrialComplexBoundarySilverHandoff {
        contract_table_name: SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES.table_name,
        table_columns: column_names(&SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES),
        transport_columns: industrial_complex_boundary_transport_columns(),
        jsonl,
        quality_metrics,
        source_snapshot_count: source_snapshot_ids.len() as u64,
        source_snapshot_ids,
    })
}

/// Returns the JSONL transport columns, contract order plus the transport-only fields.
#[must_use]
pub fn industrial_complex_boundary_transport_columns() -> Vec<String> {
    let mut columns = column_names(&SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES);
    columns.push("official_complex_code".to_owned());
    columns.push("geometry_wkb_hex".to_owned());
    columns.push("geometry_wkb_encoding".to_owned());
    columns
}

fn normalize_boundary(
    boundary: &IndustrialComplexBoundarySource,
    input: &IndustrialComplexBoundarySilverRowsInput<'_>,
) -> Result<IndustrialComplexBoundarySilverRow, IndustrialComplexBoundarySilverPlanError> {
    let official_complex_code = boundary.official_complex_code.trim();
    if official_complex_code.is_empty() {
        return Err(IndustrialComplexBoundarySilverPlanError::InvalidInput(
            "official_complex_code must be non-empty".to_owned(),
        ));
    }
    let bbox = geometry_bounding_box(&boundary.geometry)?;
    let geometry_wkb = geometry_to_wkb(&boundary.geometry)?;
    let area_sqm_calculated = geometry_area(&boundary.geometry);
    if !(area_sqm_calculated.is_finite() && area_sqm_calculated > 0.0) {
        return Err(IndustrialComplexBoundarySilverPlanError::InvalidInput(
            format!(
            "official_complex_code {official_complex_code} encloses no area, so it has no boundary \
             to publish"
        ),
        ));
    }
    let centroid = geometry_centroid(&boundary.geometry).ok_or_else(|| {
        IndustrialComplexBoundarySilverPlanError::InvalidInput(format!(
            "official_complex_code {official_complex_code} has no centroid"
        ))
    })?;
    let geometry_checksum_sha256 = sha256_hex(&geometry_wkb);

    Ok(IndustrialComplexBoundarySilverRow {
        boundary_id: boundary_id(official_complex_code),
        official_complex_code: official_complex_code.to_owned(),
        boundary_kind: BOUNDARY_KIND_OFFICIAL,
        geometry_wkb,
        geometry_srid: INDUSTRIAL_COMPLEX_BOUNDARY_SRID,
        bbox,
        centroid,
        area_sqm_calculated,
        geometry_checksum_sha256,
        source_record_id: input.source_record_id.to_owned(),
        source_snapshot_id: input.source_snapshot_id.to_owned(),
        valid_from_utc: input.valid_from_utc,
        valid_to_utc: None,
        ingested_at_utc: input.ingested_at_utc,
    })
}

/// Builds the boundary identity from what this stage actually knows.
///
/// `silver.parcel_boundaries` writes a readable urn of the same shape rather than a UUID. The
/// natural key here is the official complex code and the kind of boundary: the contract already
/// says at most one active `official` boundary exists per complex.
fn boundary_id(official_complex_code: &str) -> String {
    format!(
        "vworldkr-sandan-boundary:complex-boundary:{BOUNDARY_KIND_OFFICIAL}:{official_complex_code}"
    )
}

fn validate_handoff_row(
    row: &IndustrialComplexBoundarySilverRow,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    record_required_string_quality("boundary_id", &row.boundary_id, quality_metrics);
    record_required_string_quality("boundary_kind", row.boundary_kind, quality_metrics);
    record_required_string_quality(
        "geometry_checksum_sha256",
        &row.geometry_checksum_sha256,
        quality_metrics,
    );
    record_required_string_quality("source_record_id", &row.source_record_id, quality_metrics);
    record_required_string_quality(
        "source_snapshot_id",
        &row.source_snapshot_id,
        quality_metrics,
    );
    if row.geometry_wkb.is_empty() {
        increment_metric(quality_metrics, "geometry_wkb__null_count");
        increment_metric(quality_metrics, "invalid_geometry_wkb_count");
    }
    if !valid_bbox(row.bbox) {
        increment_metric(quality_metrics, "invalid_bbox_count");
    }
    if !centroid_inside_bbox(row.centroid, row.bbox) {
        increment_metric(quality_metrics, "centroid_outside_bbox_count");
    }
    if !(row.area_sqm_calculated.is_finite() && row.area_sqm_calculated > 0.0) {
        increment_metric(quality_metrics, "non_positive_area_count");
    }
    if !is_lowercase_sha256(&row.geometry_checksum_sha256) {
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

fn increment_metric(metrics: &mut BTreeMap<String, u64>, name: &str) {
    *metrics.entry(name.to_owned()).or_insert(0) += 1;
}

fn row_to_json_value(row: &IndustrialComplexBoundarySilverRow) -> JsonValue {
    let mut record = JsonMap::new();
    record.insert(
        "boundary_id".to_owned(),
        JsonValue::String(row.boundary_id.clone()),
    );
    // Filled by the writer from its join against `silver.industrial_complexes`.
    record.insert("complex_id".to_owned(), JsonValue::Null);
    record.insert("sido_code".to_owned(), JsonValue::Null);
    record.insert(
        "boundary_kind".to_owned(),
        JsonValue::String(row.boundary_kind.to_owned()),
    );
    record.insert("geometry_wkb".to_owned(), JsonValue::Null);
    record.insert(
        "geometry_srid".to_owned(),
        JsonValue::from(row.geometry_srid),
    );
    record.insert("bbox_min_x".to_owned(), JsonValue::from(row.bbox.min_x));
    record.insert("bbox_min_y".to_owned(), JsonValue::from(row.bbox.min_y));
    record.insert("bbox_max_x".to_owned(), JsonValue::from(row.bbox.max_x));
    record.insert("bbox_max_y".to_owned(), JsonValue::from(row.bbox.max_y));
    record.insert("centroid_x".to_owned(), JsonValue::from(row.centroid.x));
    record.insert("centroid_y".to_owned(), JsonValue::from(row.centroid.y));
    record.insert(
        "area_sqm_calculated".to_owned(),
        JsonValue::from(row.area_sqm_calculated),
    );
    record.insert(
        "geometry_checksum_sha256".to_owned(),
        JsonValue::String(row.geometry_checksum_sha256.clone()),
    );
    record.insert(
        "source_record_id".to_owned(),
        JsonValue::String(row.source_record_id.clone()),
    );
    record.insert(
        "source_snapshot_id".to_owned(),
        JsonValue::String(row.source_snapshot_id.clone()),
    );
    record.insert(
        "valid_from_utc".to_owned(),
        JsonValue::String(timestamp_json(row.valid_from_utc)),
    );
    record.insert("valid_to_utc".to_owned(), JsonValue::Null);
    record.insert(
        "ingested_at_utc".to_owned(),
        JsonValue::String(timestamp_json(row.ingested_at_utc)),
    );
    record.insert(
        "official_complex_code".to_owned(),
        JsonValue::String(row.official_complex_code.clone()),
    );
    record.insert(
        "geometry_wkb_hex".to_owned(),
        JsonValue::String(hex_lower(&row.geometry_wkb)),
    );
    record.insert(
        "geometry_wkb_encoding".to_owned(),
        JsonValue::String("hex".to_owned()),
    );
    JsonValue::Object(record)
}

fn timestamp_json(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn compact_json_line(
    value: &JsonValue,
) -> Result<String, IndustrialComplexBoundarySilverPlanError> {
    serde_json::to_string(value)
        .map_err(|error| IndustrialComplexBoundarySilverPlanError::InvalidInput(error.to_string()))
}

fn column_names(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect()
}

const fn valid_bbox(bbox: GeometryBoundingBox) -> bool {
    bbox.min_x.is_finite()
        && bbox.min_y.is_finite()
        && bbox.max_x.is_finite()
        && bbox.max_y.is_finite()
        && bbox.max_x >= bbox.min_x
        && bbox.max_y >= bbox.min_y
}

const fn centroid_inside_bbox(centroid: GeoPoint, bbox: GeometryBoundingBox) -> bool {
    centroid.x >= bbox.min_x
        && centroid.x <= bbox.max_x
        && centroid.y >= bbox.min_y
        && centroid.y <= bbox.max_y
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_lineage_part(
    label: &'static str,
    value: &str,
) -> Result<(), IndustrialComplexBoundarySilverPlanError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(());
    }
    Err(IndustrialComplexBoundarySilverPlanError::InvalidInput(
        format!("{label} must be non-empty text without surrounding whitespace"),
    ))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}
