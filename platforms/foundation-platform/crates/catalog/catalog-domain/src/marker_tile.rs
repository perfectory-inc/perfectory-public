//! PNU-anchor backed marker tile contract owned by Catalog.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::ComplexId;
use foundation_shared_kernel::Pnu;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Launch marker tile contract consumed by map runtimes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarkerTileContract {
    /// Wire response format for map-wide marker surfaces.
    pub response_format: &'static str,
    /// Canonical source for marker positions.
    pub position_source: &'static str,
    /// Public launch marker requests must not be based on arbitrary bbox inputs.
    pub bbox_marker_runtime_forbidden: bool,
    /// Successful marker responses must not silently drop eligible records.
    pub dropped_marker_success_forbidden: bool,
    /// Canonical launch runtime for static marker tiles.
    pub launch_runtime_source: &'static str,
    /// Runtime manifest endpoint consumed before clients materialize tile URLs.
    pub runtime_manifest_endpoint: &'static str,
    /// Database-backed reference endpoint must not be treated as the launch hot path.
    pub db_reference_endpoint_launch_forbidden: bool,
    /// Intended scope for the database-backed marker endpoint.
    pub db_reference_endpoint_scope: &'static str,
    /// Highest zoom level served by aggregate anchor artifacts.
    pub aggregate_anchor_max_zoom: u8,
    /// Lowest zoom level where exact parcel anchors may be requested.
    pub exact_anchor_min_zoom: u8,
    /// Canonical marker tile endpoint shape.
    pub endpoint_template: &'static str,
}

/// Canonical platform marker contract for parcel-attached map features.
pub const PNU_ANCHOR_PBF_MARKER_TILE_CONTRACT: MarkerTileContract = MarkerTileContract {
    response_format: "mvt_pbf",
    position_source: "pnu_anchor",
    bbox_marker_runtime_forbidden: true,
    dropped_marker_success_forbidden: true,
    launch_runtime_source: "r2_cdn_vector_tile_manifest",
    runtime_manifest_endpoint: "/catalog/v1/vector-tiles/manifest",
    db_reference_endpoint_launch_forbidden: true,
    db_reference_endpoint_scope: "diagnostics_bounded_proof_admin",
    aggregate_anchor_max_zoom: 11,
    exact_anchor_min_zoom: 12,
    endpoint_template: "/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf?filter_hash={hash}",
};

/// Canonical PBF marker layer that exposes active parcel anchors without product-owned data.
pub const PARCEL_ANCHOR_MARKER_TILE_LAYER: &str = "parcel_anchor";
/// Highest zoom level where aggregate parcel anchor artifacts must be used.
pub const PARCEL_ANCHOR_AGGREGATE_MAX_ZOOM: u8 = 11;
/// Lowest zoom level where exact parcel anchor points may be requested.
pub const PARCEL_ANCHOR_EXACT_MIN_ZOOM: u8 = 12;
/// Launch-safe filter identity for all active parcel anchors.
pub const ALL_ACTIVE_MARKER_FILTER_HASH: &str = "all-active-v1";

/// Supported marker tile layers owned by foundation-platform Catalog.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkerTileLayer {
    /// Active parcel anchor points. This layer does not pretend to be listing or market data.
    ParcelAnchor,
}

impl MarkerTileLayer {
    /// Stable wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::ParcelAnchor => PARCEL_ANCHOR_MARKER_TILE_LAYER,
        }
    }

    /// Parses a stable marker tile layer value.
    ///
    /// # Errors
    /// Returns `MarkerTileContractError::UnsupportedMarkerTileLayer` for unsupported layers.
    pub fn from_wire(raw: &str) -> Result<Self, MarkerTileContractError> {
        match raw {
            PARCEL_ANCHOR_MARKER_TILE_LAYER => Ok(Self::ParcelAnchor),
            other => Err(MarkerTileContractError::UnsupportedMarkerTileLayer(
                other.to_owned(),
            )),
        }
    }
}

/// Validated marker tile address and filter identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarkerTileRequest {
    /// Marker tile layer.
    pub layer: MarkerTileLayer,
    /// Slippy-map zoom level.
    pub z: u8,
    /// Slippy-map tile x coordinate.
    pub x: u32,
    /// Slippy-map tile y coordinate.
    pub y: u32,
    /// Validated filter identity, never a free-form SQL expression.
    pub filter_hash: String,
}

impl MarkerTileRequest {
    /// Builds a validated marker tile request.
    ///
    /// # Errors
    /// Returns `MarkerTileContractError` for unsupported layers, invalid tile coordinates, or
    /// unsupported filter identities.
    pub fn new(
        layer: &str,
        z: u8,
        x: u32,
        y: u32,
        filter_hash: &str,
    ) -> Result<Self, MarkerTileContractError> {
        let layer = MarkerTileLayer::from_wire(layer)?;
        validate_tile_address(z, x, y)?;
        validate_layer_zoom(layer, z)?;
        validate_filter_hash(filter_hash)?;

        Ok(Self {
            layer,
            z,
            x,
            y,
            filter_hash: filter_hash.to_owned(),
        })
    }
}

/// Algorithm used to derive a parcel marker anchor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkerAnchorAlgorithm {
    /// Source-provided official label point.
    OfficialLabelPoint,
    /// Computed interior label point for polygonal parcel geometry.
    Polylabel,
}

impl MarkerAnchorAlgorithm {
    /// Stable wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::OfficialLabelPoint => "official_label_point",
            Self::Polylabel => "polylabel",
        }
    }

    /// Parses a stable wire value.
    ///
    /// # Errors
    /// Returns `MarkerTileContractError::UnknownAnchorAlgorithm` for unsupported values.
    pub fn from_wire(raw: &str) -> Result<Self, MarkerTileContractError> {
        match raw {
            "official_label_point" => Ok(Self::OfficialLabelPoint),
            "polylabel" => Ok(Self::Polylabel),
            other => Err(MarkerTileContractError::UnknownAnchorAlgorithm(
                other.to_owned(),
            )),
        }
    }
}

/// Canonical marker anchor for one PNU.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParcelMarkerAnchor {
    /// Parcel identity.
    pub pnu: Pnu,
    /// Longitude in EPSG:4326.
    pub anchor_lng: f64,
    /// Latitude in EPSG:4326.
    pub anchor_lat: f64,
    /// Anchor derivation algorithm.
    pub algorithm: MarkerAnchorAlgorithm,
    /// Stable algorithm version.
    pub algorithm_version: String,
    /// Source geometry build/version that produced this anchor.
    pub source_geometry_version: String,
    /// SHA-256 checksum for the source geometry input or build.
    pub source_geometry_checksum_sha256: String,
    /// UTC timestamp when this anchor was computed.
    pub computed_at_utc: DateTime<Utc>,
}

/// Anchor-derived map summary for one industrial complex.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComplexAnchorSummary {
    /// Industrial complex whose active parcel anchors were summarized.
    pub complex_id: ComplexId,
    /// Canonical source for the summary coordinates.
    pub position_source: &'static str,
    /// Average longitude of active parcel anchors in EPSG:4326.
    pub center_lng: f64,
    /// Average latitude of active parcel anchors in EPSG:4326.
    pub center_lat: f64,
    /// Minimum active anchor longitude in EPSG:4326.
    pub min_lng: f64,
    /// Minimum active anchor latitude in EPSG:4326.
    pub min_lat: f64,
    /// Maximum active anchor longitude in EPSG:4326.
    pub max_lng: f64,
    /// Maximum active anchor latitude in EPSG:4326.
    pub max_lat: f64,
    /// Number of active parcel anchors represented by this summary.
    pub anchor_count: u64,
}

impl ComplexAnchorSummary {
    /// Builds a validated complex anchor summary.
    ///
    /// # Errors
    /// Returns `MarkerTileContractError` when coordinates are invalid, the extent is malformed, or
    /// no active anchor is represented.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        complex_id: ComplexId,
        center_lng: f64,
        center_lat: f64,
        min_lng: f64,
        min_lat: f64,
        max_lng: f64,
        max_lat: f64,
        anchor_count: u64,
    ) -> Result<Self, MarkerTileContractError> {
        if anchor_count == 0 {
            return Err(MarkerTileContractError::InvalidRepresentedCount(0));
        }
        validate_longitude(center_lng)?;
        validate_latitude(center_lat)?;
        validate_longitude(min_lng)?;
        validate_latitude(min_lat)?;
        validate_longitude(max_lng)?;
        validate_latitude(max_lat)?;
        if max_lng < min_lng || max_lat < min_lat {
            return Err(MarkerTileContractError::InvalidAnchorExtent);
        }

        Ok(Self {
            complex_id,
            position_source: PNU_ANCHOR_PBF_MARKER_TILE_CONTRACT.position_source,
            center_lng,
            center_lat,
            min_lng,
            min_lat,
            max_lng,
            max_lat,
            anchor_count,
        })
    }
}

impl ParcelMarkerAnchor {
    /// Builds a validated parcel marker anchor.
    ///
    /// # Errors
    /// Returns `MarkerTileContractError` when coordinates or lineage fields are invalid.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pnu: Pnu,
        anchor_lng: f64,
        anchor_lat: f64,
        algorithm: MarkerAnchorAlgorithm,
        algorithm_version: impl Into<String>,
        source_geometry_version: impl Into<String>,
        source_geometry_checksum_sha256: impl Into<String>,
        computed_at_utc: DateTime<Utc>,
    ) -> Result<Self, MarkerTileContractError> {
        validate_longitude(anchor_lng)?;
        validate_latitude(anchor_lat)?;

        let algorithm_version = require_non_blank("algorithm_version", algorithm_version.into())?;
        let source_geometry_version =
            require_non_blank("source_geometry_version", source_geometry_version.into())?;
        let source_geometry_checksum_sha256 = source_geometry_checksum_sha256.into();
        validate_sha256(
            "source_geometry_checksum_sha256",
            &source_geometry_checksum_sha256,
        )?;

        Ok(Self {
            pnu,
            anchor_lng,
            anchor_lat,
            algorithm,
            algorithm_version,
            source_geometry_version,
            source_geometry_checksum_sha256,
            computed_at_utc,
        })
    }
}

/// Minimal marker feature metadata needed by PBF marker tiles.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarkerTileFeature {
    /// Product-owned object id or aggregate id.
    pub id: String,
    /// Parcel identity used to resolve the anchor.
    pub pnu: Pnu,
    /// Stable marker kind for style selection.
    pub kind: String,
    /// Number of records represented by this feature.
    pub count: u64,
    /// Optional deterministic display priority.
    pub rank: Option<u32>,
    /// Opaque lookup reference for details.
    pub detail_ref: String,
}

impl MarkerTileFeature {
    /// Builds validated marker feature metadata.
    ///
    /// # Errors
    /// Returns `MarkerTileContractError` when the feature would hide represented records or lacks
    /// lookup metadata.
    pub fn new(
        id: impl Into<String>,
        pnu: Pnu,
        kind: impl Into<String>,
        count: u64,
        rank: Option<u32>,
        detail_ref: impl Into<String>,
    ) -> Result<Self, MarkerTileContractError> {
        if count == 0 {
            return Err(MarkerTileContractError::InvalidRepresentedCount(count));
        }

        Ok(Self {
            id: require_non_blank("id", id.into())?,
            pnu,
            kind: require_non_blank("kind", kind.into())?,
            count,
            rank,
            detail_ref: require_non_blank("detail_ref", detail_ref.into())?,
        })
    }
}

/// Validation errors for marker tile contract value objects.
#[derive(Debug, Error)]
pub enum MarkerTileContractError {
    /// Longitude was outside EPSG:4326 bounds or not finite.
    #[error("anchor_lng must be finite and between -180 and 180, got {0}")]
    InvalidLongitude(f64),
    /// Latitude was outside EPSG:4326 bounds or not finite.
    #[error("anchor_lat must be finite and between -90 and 90, got {0}")]
    InvalidLatitude(f64),
    /// A required string field was blank.
    #[error("{0} must not be blank")]
    BlankField(&'static str),
    /// SHA-256 checksum did not have the expected hex shape.
    #[error("{field} must be a 64-character SHA-256 hex string")]
    InvalidSha256 {
        /// Field name.
        field: &'static str,
    },
    /// Marker feature represented no records.
    #[error("marker tile feature count must be positive, got {0}")]
    InvalidRepresentedCount(u64),
    /// Anchor summary extent was malformed.
    #[error("anchor summary extent must have max_lng >= min_lng and max_lat >= min_lat")]
    InvalidAnchorExtent,
    /// Unknown anchor algorithm wire value.
    #[error("unknown marker anchor algorithm wire value: {0:?}")]
    UnknownAnchorAlgorithm(String),
    /// Unsupported marker tile layer.
    #[error("unsupported marker tile layer: {0:?}")]
    UnsupportedMarkerTileLayer(String),
    /// Tile coordinate was outside the slippy-map range for the zoom.
    #[error("marker tile coordinate is out of range: z={z}, x={x}, y={y}")]
    InvalidTileCoordinate {
        /// Zoom level.
        z: u8,
        /// Tile x coordinate.
        x: u32,
        /// Tile y coordinate.
        y: u32,
    },
    /// Exact parcel anchors were requested below the allowed zoom range.
    #[error("exact parcel anchor tiles require z >= {min_zoom}, got {z}")]
    ExactParcelAnchorZoomTooLow {
        /// Requested zoom level.
        z: u8,
        /// Minimum exact anchor zoom.
        min_zoom: u8,
    },
    /// Unsupported marker filter identity.
    #[error("unsupported marker tile filter hash: {0:?}")]
    UnsupportedMarkerFilterHash(String),
}

fn validate_longitude(value: f64) -> Result<(), MarkerTileContractError> {
    if value.is_finite() && (-180.0..=180.0).contains(&value) {
        return Ok(());
    }

    Err(MarkerTileContractError::InvalidLongitude(value))
}

fn validate_latitude(value: f64) -> Result<(), MarkerTileContractError> {
    if value.is_finite() && (-90.0..=90.0).contains(&value) {
        return Ok(());
    }

    Err(MarkerTileContractError::InvalidLatitude(value))
}

fn require_non_blank(
    field: &'static str,
    value: String,
) -> Result<String, MarkerTileContractError> {
    if value.trim().is_empty() {
        return Err(MarkerTileContractError::BlankField(field));
    }

    Ok(value)
}

fn validate_sha256(field: &'static str, value: &str) -> Result<(), MarkerTileContractError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(());
    }

    Err(MarkerTileContractError::InvalidSha256 { field })
}

fn validate_tile_address(z: u8, x: u32, y: u32) -> Result<(), MarkerTileContractError> {
    if z <= 24 {
        let exclusive_max = 1_u32 << u32::from(z);
        if x < exclusive_max && y < exclusive_max {
            return Ok(());
        }
    }

    Err(MarkerTileContractError::InvalidTileCoordinate { z, x, y })
}

const fn validate_layer_zoom(layer: MarkerTileLayer, z: u8) -> Result<(), MarkerTileContractError> {
    match layer {
        MarkerTileLayer::ParcelAnchor if z < PARCEL_ANCHOR_EXACT_MIN_ZOOM => {
            Err(MarkerTileContractError::ExactParcelAnchorZoomTooLow {
                z,
                min_zoom: PARCEL_ANCHOR_EXACT_MIN_ZOOM,
            })
        }
        MarkerTileLayer::ParcelAnchor => Ok(()),
    }
}

fn validate_filter_hash(value: &str) -> Result<(), MarkerTileContractError> {
    if value == ALL_ACTIVE_MARKER_FILTER_HASH {
        return Ok(());
    }

    Err(MarkerTileContractError::UnsupportedMarkerFilterHash(
        value.to_owned(),
    ))
}
