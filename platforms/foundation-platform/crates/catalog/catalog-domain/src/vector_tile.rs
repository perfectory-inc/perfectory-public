//! Static vector tile manifest contract owned by Catalog.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{
    FileAssetId, SourceRecordId, VectorTileArtifactId, VectorTileManifestId,
};
use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_WEB_MERCATOR_ZOOM: u8 = 24;

/// Runtime tile URL template for static vector tile consumers.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TilesUrlTemplate(String);

impl TilesUrlTemplate {
    /// Validates a runtime tile URL template.
    ///
    /// # Errors
    /// Returns `TilesUrlTemplateError` when the template is empty or misses one of
    /// `{object_key_prefix}`, `{z}`, `{x}`, or `{y}`.
    pub fn parse(raw: &str) -> Result<Self, TilesUrlTemplateError> {
        if raw.is_empty() {
            return Err(TilesUrlTemplateError::Empty);
        }
        for placeholder in ["{object_key_prefix}", "{z}", "{x}", "{y}"] {
            if !raw.contains(placeholder) {
                return Err(TilesUrlTemplateError::MissingPlaceholder(placeholder));
            }
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the validated template string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validation errors returned while parsing tile URL templates.
#[derive(Debug, Error)]
pub enum TilesUrlTemplateError {
    /// Template was empty.
    #[error("tiles_url_template must not be empty")]
    Empty,
    /// Template missed a required placeholder.
    #[error("tiles_url_template is missing required placeholder {0}")]
    MissingPlaceholder(&'static str),
}

/// Inclusive zoom range for tile availability or rendering.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ZoomRange {
    min: u8,
    max: u8,
}

impl ZoomRange {
    /// Builds a validated inclusive zoom range.
    ///
    /// # Errors
    /// Returns `ZoomRangeError` when min/max are inverted or outside the supported range.
    pub const fn new(min: u8, max: u8) -> Result<Self, ZoomRangeError> {
        if min > max {
            return Err(ZoomRangeError::Inverted { min, max });
        }
        if max > MAX_WEB_MERCATOR_ZOOM {
            return Err(ZoomRangeError::OutOfBounds {
                max,
                supported_max: MAX_WEB_MERCATOR_ZOOM,
            });
        }
        Ok(Self { min, max })
    }

    /// Returns the minimum zoom level.
    #[must_use]
    pub const fn min(self) -> u8 {
        self.min
    }

    /// Returns the maximum zoom level.
    #[must_use]
    pub const fn max(self) -> u8 {
        self.max
    }
}

/// Validation errors returned while building zoom ranges.
#[derive(Debug, Error)]
pub enum ZoomRangeError {
    /// Minimum zoom was greater than maximum zoom.
    #[error("zoom range is inverted: min={min}, max={max}")]
    Inverted {
        /// Provided minimum zoom.
        min: u8,
        /// Provided maximum zoom.
        max: u8,
    },
    /// Maximum zoom exceeded the supported Web Mercator range.
    #[error("zoom max {max} exceeds supported max {supported_max}")]
    OutOfBounds {
        /// Provided maximum zoom.
        max: u8,
        /// Maximum zoom supported by foundation-platform.
        supported_max: u8,
    },
}

/// Lineage links required to audit a vector tile artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorTileLineage {
    /// Source record that describes the tile build input.
    pub source_record_id: SourceRecordId,
    /// File asset row for the manifest JSON file.
    pub manifest_file_asset_id: FileAssetId,
    /// File asset row for the layer `TileJSON` file.
    pub tilejson_file_asset_id: FileAssetId,
    /// File asset rows for source files used to build this layer.
    pub source_file_asset_ids: Vec<FileAssetId>,
}

/// Per-layer vector tile artifact metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorTileArtifact {
    /// Stable foundation-platform vector tile artifact identifier.
    pub id: VectorTileArtifactId,
    /// Manifest that owns this artifact.
    pub manifest_id: VectorTileManifestId,
    /// Logical layer name used by consumers.
    pub layer: String,
    /// Source layer name embedded in the tile payload.
    pub source_layer: String,
    /// Zoom range available in stored tiles.
    pub tile_zoom: ZoomRange,
    /// Zoom range clients should render.
    pub render_zoom: ZoomRange,
    /// Provider-neutral object key for this layer's `TileJSON` document.
    pub tilejson_object_key: ObjectKey,
    /// Provider-neutral prefix that contains this layer's tile objects.
    pub object_key_prefix: ObjectKeyPrefix,
    /// Number of flat tile objects generated for this layer.
    pub flat_tile_count: u64,
    /// Total bytes across flat tile objects for this layer.
    pub flat_tile_total_bytes: u64,
    /// Source and file lineage that produced this artifact.
    pub lineage: VectorTileLineage,
    /// UTC timestamp when the artifact was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the artifact was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

impl VectorTileArtifact {
    /// Returns consumer-safe feature property names that may be used for client-side filtering.
    #[must_use]
    pub fn feature_filter_properties(&self) -> BTreeMap<String, String> {
        vector_tile_feature_filter_properties(self.layer.as_str())
    }
}

/// Returns the public feature property contract for foundation-platform-owned reference layers.
///
/// Product-owned layers such as Gongzzang `listing` intentionally return no properties here.
#[must_use]
pub fn vector_tile_feature_filter_properties(layer: &str) -> BTreeMap<String, String> {
    match layer {
        "complex" => BTreeMap::from([(
            "official_complex_code".to_owned(),
            "official_complex_code".to_owned(),
        )]),
        "parcels" | "parcel_anchor" => BTreeMap::from([("pnu".to_owned(), "pnu".to_owned())]),
        _ => BTreeMap::new(),
    }
}

/// Runtime vector tile manifest owned by foundation-platform Catalog.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorTileManifest {
    /// Stable foundation-platform vector tile manifest identifier.
    pub id: VectorTileManifestId,
    /// Active tile artifact version.
    pub current_version: String,
    /// Previously active tile artifact version.
    pub previous_version: String,
    /// URL template used by vector tile consumers.
    pub tiles_url_template: TilesUrlTemplate,
    /// UTC timestamp when this manifest became active.
    pub published_at: DateTime<Utc>,
    /// File asset row for the manifest JSON file.
    pub manifest_file_asset_id: FileAssetId,
    /// Source record that describes the tile build input.
    pub source_record_id: SourceRecordId,
    /// Layer artifacts contained in this manifest.
    pub artifacts: Vec<VectorTileArtifact>,
    /// UTC timestamp when the manifest was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the manifest was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}
