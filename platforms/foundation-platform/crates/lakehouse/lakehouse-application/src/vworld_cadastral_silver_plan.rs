//! Silver normalization helpers for `VWorld` cadastral parcel-boundary rows.

use std::{collections::BTreeMap, fmt::Write as _};

use chrono::{DateTime, Utc};
use collection_domain::VWorldCadastralDedupedFeature;
use foundation_shared_kernel::Pnu;
use lakehouse_domain::{LakehouseTableContract, SILVER_PARCEL_BOUNDARIES};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

const GEOMETRY_SRID: i32 = 4326;

/// Bounding box derived from canonical Silver parcel-boundary geometry.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VWorldCadastralBoundingBox {
    /// Minimum X coordinate in the request CRS.
    pub min_x: f64,
    /// Minimum Y coordinate in the request CRS.
    pub min_y: f64,
    /// Maximum X coordinate in the request CRS.
    pub max_x: f64,
    /// Maximum Y coordinate in the request CRS.
    pub max_y: f64,
}

/// Input required to normalize deduplicated `VWorld` cadastral features into Silver rows.
pub struct VWorldCadastralSilverParcelBoundaryRowsInput<'a> {
    /// Deduplicated feature records ordered by PNU.
    pub records: &'a [VWorldCadastralDedupedFeature],
    /// Source-record lineage id for this normalization batch.
    pub source_record_id: &'a str,
    /// Source-snapshot lineage id for this normalization batch.
    pub source_snapshot_id: &'a str,
    /// UTC timestamp from which these parcel boundaries are valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when the rows were ingested into the foundation-platform lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Silver `silver.parcel_boundaries` row prepared from one `VWorld` cadastral feature.
#[derive(Clone, Debug, PartialEq)]
pub struct VWorldCadastralSilverParcelBoundaryRow {
    /// Stable parcel-boundary identity.
    pub boundary_id: String,
    /// Canonical 19-digit parcel number.
    pub pnu: String,
    /// Two-digit province/city code derived from PNU.
    pub sido_code: String,
    /// Five-digit city/county/district code derived from PNU.
    pub sigungu_code: String,
    /// Ten-digit legal-dong code derived from PNU.
    pub bjdong_code: String,
    /// Provider parcel lot label.
    pub jibun: Option<String>,
    /// Provider main lot number.
    pub bonbun: Option<String>,
    /// Provider sub lot number.
    pub bubun: Option<String>,
    /// Standard little-endian WKB geometry bytes for `GeoParquet` writers.
    pub geometry_wkb: Vec<u8>,
    /// Spatial reference id for the geometry.
    pub geometry_srid: i32,
    /// Bounding box derived from the full polygon geometry.
    pub bbox: VWorldCadastralBoundingBox,
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

/// Writer-neutral JSONL handoff for `silver.parcel_boundaries`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldCadastralSilverParcelBoundaryHandoff {
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
    /// Whether `source_snapshot_ids` was truncated by this builder.
    pub source_snapshot_truncated: bool,
}

/// Error returned while normalizing `VWorld` cadastral features into Silver rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum VWorldCadastralSilverPlanError {
    /// Input data cannot be represented as a Silver parcel-boundary row.
    #[error("invalid VWorld cadastral Silver parcel-boundary input: {0}")]
    InvalidInput(String),
}

/// Normalizes deduplicated `VWorld` cadastral features into Silver parcel-boundary rows.
///
/// The returned rows are ordered by the input records. Geometry is converted from `GeoJSON`
/// `Polygon` or `MultiPolygon` into standard little-endian WKB so the downstream `GeoParquet`
/// writer does not need to guess provider-specific geometry semantics.
///
/// # Errors
///
/// Returns `VWorldCadastralSilverPlanError` when lineage fields are empty, PNU validation fails,
/// provider properties have unexpected types, or the geometry is not a valid polygonal `GeoJSON`
/// object.
pub fn normalize_vworld_cadastral_silver_parcel_boundary_rows(
    input: &VWorldCadastralSilverParcelBoundaryRowsInput<'_>,
) -> Result<Vec<VWorldCadastralSilverParcelBoundaryRow>, VWorldCadastralSilverPlanError> {
    validate_lineage_part("source_record_id", input.source_record_id)?;
    validate_lineage_part("source_snapshot_id", input.source_snapshot_id)?;

    input
        .records
        .iter()
        .map(|record| normalize_record(record, input))
        .collect()
}

/// Builds a writer-neutral JSONL handoff from Silver parcel-boundary rows.
///
/// The lakehouse contract remains `geometry_wkb binary`; JSONL cannot carry binary values
/// losslessly as a native JSON scalar, so this handoff carries `geometry_wkb_hex` plus
/// `geometry_wkb_encoding=hex`. The downstream writer must decode that field into the target
/// `geometry_wkb` column before writing `GeoParquet`.
///
/// # Errors
///
/// Returns `VWorldCadastralSilverPlanError` when a row has invalid lineage, an empty WKB payload,
/// an invalid bbox, or an invalid checksum shape.
pub fn build_vworld_cadastral_silver_parcel_boundary_handoff(
    rows: &[VWorldCadastralSilverParcelBoundaryRow],
) -> Result<VWorldCadastralSilverParcelBoundaryHandoff, VWorldCadastralSilverPlanError> {
    let mut quality_metrics = required_quality_metrics(&SILVER_PARCEL_BOUNDARIES);
    quality_metrics.insert("row_count".to_owned(), rows.len() as u64);
    quality_metrics.insert("invalid_bbox_count".to_owned(), 0);
    quality_metrics.insert("invalid_checksum_count".to_owned(), 0);
    quality_metrics.insert("invalid_geometry_wkb_count".to_owned(), 0);
    quality_metrics.insert("invalid_pnu_count".to_owned(), 0);

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

    Ok(VWorldCadastralSilverParcelBoundaryHandoff {
        contract_table_name: SILVER_PARCEL_BOUNDARIES.table_name,
        table_columns: column_names(&SILVER_PARCEL_BOUNDARIES),
        transport_columns: vworld_cadastral_parcel_boundary_transport_columns(),
        jsonl,
        quality_metrics,
        source_snapshot_count: source_snapshot_ids.len() as u64,
        source_snapshot_ids,
        source_snapshot_truncated: false,
    })
}

fn normalize_record(
    record: &VWorldCadastralDedupedFeature,
    input: &VWorldCadastralSilverParcelBoundaryRowsInput<'_>,
) -> Result<VWorldCadastralSilverParcelBoundaryRow, VWorldCadastralSilverPlanError> {
    let pnu = Pnu::parse(record.pnu.clone()).map_err(|error| {
        VWorldCadastralSilverPlanError::InvalidInput(format!("invalid pnu {}: {error}", record.pnu))
    })?;
    let parsed_geometry = parse_geojson_polygonal_geometry(&record.geometry, pnu.as_str())?;
    let bbox = geometry_bbox(&parsed_geometry)?;
    let geometry_wkb = geometry_to_wkb(&parsed_geometry)?;
    let geometry_checksum_sha256 = sha256_hex(&geometry_wkb);

    Ok(VWorldCadastralSilverParcelBoundaryRow {
        boundary_id: format!("vworld-cadastral:parcel-boundary:pnu:{}", pnu.as_str()),
        pnu: pnu.as_str().to_owned(),
        sido_code: pnu.as_str()[0..2].to_owned(),
        sigungu_code: pnu.as_str()[0..5].to_owned(),
        bjdong_code: pnu.as_str()[0..10].to_owned(),
        jibun: optional_property_string(&record.properties, "jibun", pnu.as_str())?,
        bonbun: optional_property_string(&record.properties, "bonbun", pnu.as_str())?,
        bubun: optional_property_string(&record.properties, "bubun", pnu.as_str())?,
        geometry_wkb,
        geometry_srid: GEOMETRY_SRID,
        bbox,
        geometry_checksum_sha256,
        source_record_id: input.source_record_id.to_owned(),
        source_snapshot_id: input.source_snapshot_id.to_owned(),
        valid_from_utc: input.valid_from_utc,
        valid_to_utc: None,
        ingested_at_utc: input.ingested_at_utc,
    })
}

fn validate_handoff_row(
    row: &VWorldCadastralSilverParcelBoundaryRow,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    record_required_string_quality("boundary_id", &row.boundary_id, quality_metrics);
    record_required_string_quality("pnu", &row.pnu, quality_metrics);
    record_required_string_quality("sido_code", &row.sido_code, quality_metrics);
    record_required_string_quality("sigungu_code", &row.sigungu_code, quality_metrics);
    record_required_string_quality("bjdong_code", &row.bjdong_code, quality_metrics);
    record_required_binary_quality("geometry_wkb", &row.geometry_wkb, quality_metrics);
    record_required_f64_quality("bbox_min_x", row.bbox.min_x, quality_metrics);
    record_required_f64_quality("bbox_min_y", row.bbox.min_y, quality_metrics);
    record_required_f64_quality("bbox_max_x", row.bbox.max_x, quality_metrics);
    record_required_f64_quality("bbox_max_y", row.bbox.max_y, quality_metrics);
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
    if Pnu::parse(row.pnu.clone()).is_err() {
        increment_metric(quality_metrics, "invalid_pnu_count");
    }
    if row.geometry_wkb.is_empty() {
        increment_metric(quality_metrics, "invalid_geometry_wkb_count");
    }
    if !valid_bbox(row.bbox) {
        increment_metric(quality_metrics, "invalid_bbox_count");
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

fn record_required_binary_quality(
    name: &'static str,
    value: &[u8],
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    if value.is_empty() {
        increment_metric(quality_metrics, &format!("{name}__null_count"));
    }
}

fn record_required_f64_quality(
    name: &'static str,
    value: f64,
    quality_metrics: &mut BTreeMap<String, u64>,
) {
    if !value.is_finite() {
        increment_metric(quality_metrics, &format!("{name}__null_count"));
    }
}

fn increment_metric(metrics: &mut BTreeMap<String, u64>, name: &str) {
    *metrics.entry(name.to_owned()).or_insert(0) += 1;
}

fn row_to_json_value(row: &VWorldCadastralSilverParcelBoundaryRow) -> JsonValue {
    let mut record = JsonMap::new();
    record.insert(
        "boundary_id".to_owned(),
        JsonValue::String(row.boundary_id.clone()),
    );
    record.insert("pnu".to_owned(), JsonValue::String(row.pnu.clone()));
    record.insert(
        "sido_code".to_owned(),
        JsonValue::String(row.sido_code.clone()),
    );
    record.insert(
        "sigungu_code".to_owned(),
        JsonValue::String(row.sigungu_code.clone()),
    );
    record.insert(
        "bjdong_code".to_owned(),
        JsonValue::String(row.bjdong_code.clone()),
    );
    record.insert("jibun".to_owned(), optional_string_json(row.jibun.as_ref()));
    record.insert(
        "bonbun".to_owned(),
        optional_string_json(row.bonbun.as_ref()),
    );
    record.insert("bubun".to_owned(), optional_string_json(row.bubun.as_ref()));
    record.insert("geometry_wkb".to_owned(), JsonValue::Null);
    record.insert(
        "geometry_wkb_hex".to_owned(),
        JsonValue::String(hex_lower(&row.geometry_wkb)),
    );
    record.insert(
        "geometry_wkb_encoding".to_owned(),
        JsonValue::String("hex".to_owned()),
    );
    record.insert(
        "geometry_srid".to_owned(),
        JsonValue::from(row.geometry_srid),
    );
    record.insert("bbox_min_x".to_owned(), JsonValue::from(row.bbox.min_x));
    record.insert("bbox_min_y".to_owned(), JsonValue::from(row.bbox.min_y));
    record.insert("bbox_max_x".to_owned(), JsonValue::from(row.bbox.max_x));
    record.insert("bbox_max_y".to_owned(), JsonValue::from(row.bbox.max_y));
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
    JsonValue::Object(record)
}

fn optional_string_json(value: Option<&String>) -> JsonValue {
    value.map_or(JsonValue::Null, |value| JsonValue::String(value.clone()))
}

fn timestamp_json(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn compact_json_line(value: &JsonValue) -> Result<String, VWorldCadastralSilverPlanError> {
    serde_json::to_string(value)
        .map_err(|error| VWorldCadastralSilverPlanError::InvalidInput(error.to_string()))
}

fn column_names(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect()
}

fn vworld_cadastral_parcel_boundary_transport_columns() -> Vec<String> {
    [
        "boundary_id",
        "pnu",
        "sido_code",
        "sigungu_code",
        "bjdong_code",
        "jibun",
        "bonbun",
        "bubun",
        "geometry_wkb",
        "geometry_wkb_hex",
        "geometry_wkb_encoding",
        "geometry_srid",
        "bbox_min_x",
        "bbox_min_y",
        "bbox_max_x",
        "bbox_max_y",
        "geometry_checksum_sha256",
        "source_record_id",
        "source_snapshot_id",
        "valid_from_utc",
        "valid_to_utc",
        "ingested_at_utc",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn valid_bbox(bbox: VWorldCadastralBoundingBox) -> bool {
    bbox.min_x.is_finite()
        && bbox.min_y.is_finite()
        && bbox.max_x.is_finite()
        && bbox.max_y.is_finite()
        && bbox.max_x >= bbox.min_x
        && bbox.max_y >= bbox.min_y
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
) -> Result<(), VWorldCadastralSilverPlanError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(());
    }
    Err(VWorldCadastralSilverPlanError::InvalidInput(format!(
        "{label} must be non-empty text without surrounding whitespace"
    )))
}

fn optional_property_string(
    properties: &JsonValue,
    field: &'static str,
    pnu: &str,
) -> Result<Option<String>, VWorldCadastralSilverPlanError> {
    match properties.get(field) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(VWorldCadastralSilverPlanError::InvalidInput(format!(
            "property {field} for pnu {pnu} must be a string when present"
        ))),
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct GeoPoint {
    x: f64,
    y: f64,
}

type LinearRing = Vec<GeoPoint>;
type PolygonRings = Vec<LinearRing>;

#[derive(Clone, Debug, PartialEq)]
enum ParsedPolygonalGeometry {
    Polygon(PolygonRings),
    MultiPolygon(Vec<PolygonRings>),
}

fn parse_geojson_polygonal_geometry(
    geometry: &JsonValue,
    pnu: &str,
) -> Result<ParsedPolygonalGeometry, VWorldCadastralSilverPlanError> {
    let geometry_type = geometry
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| invalid_geometry(pnu, "geometry.type must be a string"))?;
    let coordinates = geometry
        .get("coordinates")
        .ok_or_else(|| invalid_geometry(pnu, "geometry.coordinates is required"))?;

    match geometry_type {
        "Polygon" => Ok(ParsedPolygonalGeometry::Polygon(parse_polygon(
            coordinates,
            pnu,
        )?)),
        "MultiPolygon" => {
            let polygons = coordinates.as_array().ok_or_else(|| {
                invalid_geometry(pnu, "MultiPolygon coordinates must be an array")
            })?;
            if polygons.is_empty() {
                return Err(invalid_geometry(
                    pnu,
                    "MultiPolygon coordinates must contain at least one polygon",
                ));
            }
            polygons
                .iter()
                .map(|polygon| parse_polygon(polygon, pnu))
                .collect::<Result<Vec<_>, _>>()
                .map(ParsedPolygonalGeometry::MultiPolygon)
        }
        unsupported => Err(VWorldCadastralSilverPlanError::InvalidInput(format!(
            "unsupported geometry type {unsupported} for pnu {pnu}"
        ))),
    }
}

fn parse_polygon(
    coordinates: &JsonValue,
    pnu: &str,
) -> Result<PolygonRings, VWorldCadastralSilverPlanError> {
    let rings = coordinates
        .as_array()
        .ok_or_else(|| invalid_geometry(pnu, "Polygon coordinates must be an array of rings"))?;
    if rings.is_empty() {
        return Err(invalid_geometry(
            pnu,
            "Polygon coordinates must contain at least one ring",
        ));
    }
    rings
        .iter()
        .map(|ring| parse_linear_ring(ring, pnu))
        .collect()
}

fn parse_linear_ring(
    coordinates: &JsonValue,
    pnu: &str,
) -> Result<LinearRing, VWorldCadastralSilverPlanError> {
    let positions = coordinates
        .as_array()
        .ok_or_else(|| invalid_geometry(pnu, "linear ring must be an array of positions"))?;
    if positions.len() < 4 {
        return Err(invalid_geometry(
            pnu,
            "linear ring must contain at least four positions",
        ));
    }
    let ring = positions
        .iter()
        .map(|position| parse_position(position, pnu))
        .collect::<Result<Vec<_>, _>>()?;

    if ring.first() != ring.last() {
        return Err(invalid_geometry(pnu, "linear ring must be closed"));
    }
    Ok(ring)
}

fn parse_position(
    position: &JsonValue,
    pnu: &str,
) -> Result<GeoPoint, VWorldCadastralSilverPlanError> {
    let values = position
        .as_array()
        .ok_or_else(|| invalid_geometry(pnu, "position must be an array"))?;
    if values.len() < 2 {
        return Err(invalid_geometry(
            pnu,
            "position must contain at least x and y coordinates",
        ));
    }
    let x = coordinate_number(&values[0], "x", pnu)?;
    let y = coordinate_number(&values[1], "y", pnu)?;
    Ok(GeoPoint { x, y })
}

fn coordinate_number(
    value: &JsonValue,
    label: &'static str,
    pnu: &str,
) -> Result<f64, VWorldCadastralSilverPlanError> {
    let coordinate = value.as_f64().ok_or_else(|| {
        let reason = format!("coordinate {label} must be a number");
        invalid_geometry(pnu, &reason)
    })?;
    if coordinate.is_finite() {
        return Ok(coordinate);
    }
    let reason = format!("coordinate {label} must be finite");
    Err(invalid_geometry(pnu, &reason))
}

fn geometry_bbox(
    geometry: &ParsedPolygonalGeometry,
) -> Result<VWorldCadastralBoundingBox, VWorldCadastralSilverPlanError> {
    let mut accumulator = BBoxAccumulator::default();
    match geometry {
        ParsedPolygonalGeometry::Polygon(polygon) => accumulator.record_polygon(polygon),
        ParsedPolygonalGeometry::MultiPolygon(polygons) => {
            for polygon in polygons {
                accumulator.record_polygon(polygon);
            }
        }
    }
    accumulator.finish()
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct BBoxAccumulator {
    bbox: Option<VWorldCadastralBoundingBox>,
}

impl BBoxAccumulator {
    fn record_polygon(&mut self, polygon: &[LinearRing]) {
        for ring in polygon {
            for point in ring {
                self.record_point(*point);
            }
        }
    }

    const fn record_point(&mut self, point: GeoPoint) {
        self.bbox = Some(match self.bbox {
            Some(bbox) => VWorldCadastralBoundingBox {
                min_x: bbox.min_x.min(point.x),
                min_y: bbox.min_y.min(point.y),
                max_x: bbox.max_x.max(point.x),
                max_y: bbox.max_y.max(point.y),
            },
            None => VWorldCadastralBoundingBox {
                min_x: point.x,
                min_y: point.y,
                max_x: point.x,
                max_y: point.y,
            },
        });
    }

    fn finish(self) -> Result<VWorldCadastralBoundingBox, VWorldCadastralSilverPlanError> {
        self.bbox.ok_or_else(|| {
            VWorldCadastralSilverPlanError::InvalidInput(
                "geometry must contain at least one coordinate".to_owned(),
            )
        })
    }
}

fn geometry_to_wkb(
    geometry: &ParsedPolygonalGeometry,
) -> Result<Vec<u8>, VWorldCadastralSilverPlanError> {
    let mut bytes = Vec::new();
    match geometry {
        ParsedPolygonalGeometry::Polygon(polygon) => write_polygon_wkb(&mut bytes, polygon)?,
        ParsedPolygonalGeometry::MultiPolygon(polygons) => {
            write_u8(&mut bytes, 1);
            write_u32_le(&mut bytes, 6);
            write_len_u32(&mut bytes, polygons.len(), "MultiPolygon polygon count")?;
            for polygon in polygons {
                write_polygon_wkb(&mut bytes, polygon)?;
            }
        }
    }
    Ok(bytes)
}

fn write_polygon_wkb(
    bytes: &mut Vec<u8>,
    polygon: &[LinearRing],
) -> Result<(), VWorldCadastralSilverPlanError> {
    write_u8(bytes, 1);
    write_u32_le(bytes, 3);
    write_len_u32(bytes, polygon.len(), "Polygon ring count")?;
    for ring in polygon {
        write_len_u32(bytes, ring.len(), "linear ring point count")?;
        for point in ring {
            write_f64_le(bytes, point.x);
            write_f64_le(bytes, point.y);
        }
    }
    Ok(())
}

fn write_len_u32(
    bytes: &mut Vec<u8>,
    len: usize,
    label: &'static str,
) -> Result<(), VWorldCadastralSilverPlanError> {
    let value = u32::try_from(len).map_err(|_| {
        VWorldCadastralSilverPlanError::InvalidInput(format!(
            "{label} exceeds WKB u32 length capacity"
        ))
    })?;
    write_u32_le(bytes, value);
    Ok(())
}

fn write_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn write_u32_le(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_f64_le(bytes: &mut Vec<u8>, value: f64) {
    bytes.extend_from_slice(&value.to_le_bytes());
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

fn invalid_geometry(pnu: &str, reason: &str) -> VWorldCadastralSilverPlanError {
    VWorldCadastralSilverPlanError::InvalidInput(format!(
        "invalid GeoJSON geometry for pnu {pnu}: {reason}"
    ))
}
