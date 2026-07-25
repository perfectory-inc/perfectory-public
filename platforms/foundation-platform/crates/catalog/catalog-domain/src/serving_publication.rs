//! Versioned, single-source spatial serving publication contract.
//!
//! This is deliberately separate from the frozen v1 flat-tile manifest.  A v2 publication
//! selects exactly one complete Martin source for each publication unit: dynamic PostGIS or an
//! immutable PMTiles release.  The validation here is the domain guard used by transport DTOs and
//! publishers; clients do not infer or merge sources.

use std::collections::{BTreeMap, BTreeSet};

use foundation_shared_kernel::ids::{
    FileAssetId, PostgisProjectionRevisionId, SourceRecordId, VectorTileDataRevisionId,
    VectorTileReleaseId, VectorTileRuntimeManifestId,
};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Positive JavaScript-safe global manifest generation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ManifestGeneration(u64);

impl ManifestGeneration {
    /// Creates a valid generation.
    pub fn new(value: u64) -> Result<Self, String> {
        if (1..=MAX_SAFE_INTEGER).contains(&value) {
            Ok(Self(value))
        } else {
            Err("generation must be in 1..=9007199254740991".to_owned())
        }
    }

    /// Returns the wire integer.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for ManifestGeneration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Positive JavaScript-safe per-unit serving generation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ServingGeneration(u64);

impl ServingGeneration {
    /// Creates a valid generation.
    pub fn new(value: u64) -> Result<Self, String> {
        ManifestGeneration::new(value).map(|_| Self(value))
    }

    /// Returns the wire integer.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for ServingGeneration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Positive decimal Iceberg snapshot identifier, kept as a string to avoid JavaScript loss.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CanonicalIcebergSnapshotId(String);

impl CanonicalIcebergSnapshotId {
    /// Creates a positive base-10 identifier.
    pub fn new(value: String) -> Result<Self, String> {
        if !value.is_empty() && value.chars().all(|c| c.is_ascii_digit()) && value != "0" {
            Ok(Self(value))
        } else {
            Err("Iceberg snapshot id must be a positive decimal string".to_owned())
        }
    }

    /// Returns the wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CanonicalIcebergSnapshotId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Complete Martin URL template containing exactly one `{z}`, `{x}`, and `{y}` placeholder.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RuntimeTilesUrlTemplate(String);

impl RuntimeTilesUrlTemplate {
    /// Validates a production URL or the loopback HTTP URL used by the local proof harness.
    pub fn new(value: String) -> Result<Self, String> {
        let scheme_end = value
            .find("://")
            .ok_or_else(|| "tile URL must be absolute".to_owned())?;
        let scheme = &value[..scheme_end];
        if scheme != "https" && scheme != "http" {
            return Err("tile URL scheme must be http or https".to_owned());
        }
        for placeholder in ["{z}", "{x}", "{y}"] {
            if value.matches(placeholder).count() != 1 {
                return Err(format!("tile URL must contain {placeholder} exactly once"));
            }
        }
        let mut remainder = &value[scheme_end + 3..];
        let host_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
        let host = &remainder[..host_end];
        if host.is_empty() || host.contains('{') || host.contains('}') {
            return Err("tile URL must contain a host".to_owned());
        }
        remainder = &remainder[host_end..];
        if value.contains('{') {
            for part in value.split('{').skip(1) {
                if !part.starts_with("z}") && !part.starts_with("x}") && !part.starts_with("y}") {
                    return Err("tile URL contains an unknown placeholder".to_owned());
                }
            }
        }
        if scheme == "http" {
            let host_without_port = host
                .trim_start_matches('[')
                .split(']')
                .next()
                .unwrap_or(host)
                .split(':')
                .next()
                .unwrap_or(host);
            if !matches!(host_without_port, "localhost" | "127.0.0.1" | "::1") {
                return Err("http tile URLs are permitted only for loopback proof hosts".to_owned());
            }
        }
        let _ = remainder;
        Ok(Self(value))
    }

    /// Returns the wire URL template.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RuntimeTilesUrlTemplate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Canonical lower-case feature identity property.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FeatureIdProperty(String);

impl FeatureIdProperty {
    /// Creates a non-empty lower-case feature property name.
    pub fn new(value: String) -> Result<Self, String> {
        if !value.is_empty()
            && value == value.to_ascii_lowercase()
            && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            Ok(Self(value))
        } else {
            Err("feature_id_property must be a non-empty lower-case identifier".to_owned())
        }
    }

    /// Returns the wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for FeatureIdProperty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// MVT layer metadata shared by both complete serving sources.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTileLayer {
    /// Layer name embedded in the MVT payload.
    pub source_layer: String,
    /// Canonical feature identity property.
    pub feature_id_property: FeatureIdProperty,
    /// Stored tile minimum zoom.
    pub tile_min_zoom: u8,
    /// Stored tile maximum zoom.
    pub tile_max_zoom: u8,
    /// Client render minimum zoom.
    pub render_min_zoom: u8,
    /// Client render maximum zoom.
    pub render_max_zoom: u8,
    /// Concrete feature properties used by client filters.
    #[serde(default)]
    pub feature_filter_properties: BTreeMap<String, String>,
}

/// Audit lineage for a complete serving unit.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTileLineage {
    /// Source record describing the build input.
    pub source_record_id: SourceRecordId,
    /// Source file assets used to build this unit.
    pub source_file_asset_ids: Vec<FileAssetId>,
}

/// Dynamic Martin/PostGIS source.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicPostgisSource {
    /// Stable configured Martin source name.
    pub martin_source_id: String,
    /// Stable, query-free tile URL template selected by the Catalog pointer.
    pub tiles_url_template: RuntimeTilesUrlTemplate,
    /// Complete PostGIS projection revision.
    pub postgis_projection_revision: PostgisProjectionRevisionId,
    /// Dynamic sources are never browser-cacheable.
    pub cache_policy: String,
}

/// Immutable PMTiles/Martin source.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StaticPmtilesSource {
    /// Release-addressed Martin source name.
    pub martin_source_id: String,
    /// Release-addressed tile URL template.
    pub tiles_url_template: RuntimeTilesUrlTemplate,
    /// Immutable PMTiles object key.
    pub pmtiles_object_key: String,
    /// File asset row for the PMTiles object.
    pub pmtiles_file_asset_id: FileAssetId,
    /// Lowercase SHA-256 checksum.
    pub pmtiles_sha256: String,
    /// PMTiles object byte size.
    pub pmtiles_bytes: u64,
}

/// Closed union selecting exactly one complete Martin source.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveTileSource {
    /// Complete dynamic PostGIS source.
    DynamicPostgis(DynamicPostgisSource),
    /// Immutable static PMTiles source.
    StaticPmtiles(StaticPmtilesSource),
}

/// One atomically switched publication unit.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationUnit {
    /// UUID of the logical feature set.
    pub data_revision: VectorTileDataRevisionId,
    /// JavaScript-safe source selection generation.
    pub serving_generation: ServingGeneration,
    /// Immutable release descriptor UUID.
    pub active_release_id: VectorTileReleaseId,
    /// Canonical Iceberg snapshot represented by this unit.
    pub canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId,
    /// Exactly one complete serving source.
    pub source: ActiveTileSource,
    /// Non-empty layers served by this complete source.
    pub layers: BTreeMap<String, RuntimeTileLayer>,
    /// Source lineage.
    pub lineage: RuntimeTileLineage,
}

/// The source kind selected by one immutable publication release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingSourceKind {
    /// Complete PostGIS projection served dynamically by Martin.
    DynamicPostgis,
    /// Immutable PMTiles release served through Martin.
    StaticPmtiles,
}

/// The minimum state needed to validate a source switch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServingSelection {
    /// The complete source selected by the release.
    pub source_kind: ServingSourceKind,
    /// Logical content revision represented by the release.
    pub data_revision: VectorTileDataRevisionId,
    /// Monotonic source selection generation for this publication unit.
    pub serving_generation: ServingGeneration,
}

/// Validates the only supported publication state transitions.
///
/// A first publication is always a complete dynamic source. A static release is allowed only
/// when it was built from the currently selected data revision; this prevents an old PMTiles
/// file from being promoted after the canonical source changed. Returning to dynamic is allowed
/// for either the same data revision (a safe fallback) or a newer revision. Every switch advances
/// the per-unit generation exactly once.
pub fn validate_serving_transition(
    previous: Option<ServingSelection>,
    candidate: ServingSelection,
) -> Result<(), String> {
    match previous {
        None => {
            if candidate.source_kind != ServingSourceKind::DynamicPostgis {
                return Err("the first publication must be dynamic_postgis".to_owned());
            }
            if candidate.serving_generation.value() != 1 {
                return Err("the first serving_generation must be 1".to_owned());
            }
        }
        Some(previous) => {
            let expected_generation = previous
                .serving_generation
                .value()
                .checked_add(1)
                .ok_or_else(|| "serving_generation overflow".to_owned())?;
            if candidate.serving_generation.value() != expected_generation {
                return Err("serving_generation must advance exactly once".to_owned());
            }
            if candidate.source_kind == ServingSourceKind::StaticPmtiles
                && candidate.data_revision != previous.data_revision
            {
                return Err(
                    "static_pmtiles must be built from the currently selected data_revision"
                        .to_owned(),
                );
            }
        }
    }
    Ok(())
}

/// Foundation-owned v2 runtime manifest.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VectorTileRuntimeManifest {
    /// Exact schema version.
    pub schema_version: u32,
    /// Immutable manifest UUID and ETag identity.
    pub current_version: VectorTileRuntimeManifestId,
    /// Global JavaScript-safe polling generation.
    pub manifest_generation: ManifestGeneration,
    /// Required bounded polling interval.
    pub refresh_after_seconds: u16,
    /// Publication timestamp.
    pub published_at: chrono::DateTime<chrono::Utc>,
    /// Non-empty publication units keyed by stable unit name.
    pub publication_units: BTreeMap<String, PublicationUnit>,
}

#[derive(Deserialize)]
struct RawVectorTileRuntimeManifest {
    schema_version: u32,
    current_version: VectorTileRuntimeManifestId,
    manifest_generation: ManifestGeneration,
    refresh_after_seconds: u16,
    published_at: chrono::DateTime<chrono::Utc>,
    publication_units: BTreeMap<String, PublicationUnit>,
}

impl<'de> Deserialize<'de> for VectorTileRuntimeManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawVectorTileRuntimeManifest::deserialize(deserializer)?;
        let value = Self {
            schema_version: raw.schema_version,
            current_version: raw.current_version,
            manifest_generation: raw.manifest_generation,
            refresh_after_seconds: raw.refresh_after_seconds,
            published_at: raw.published_at,
            publication_units: raw.publication_units,
        };
        value.validate().map_err(D::Error::custom)?;
        Ok(value)
    }
}

impl VectorTileRuntimeManifest {
    /// Validates all cross-field publication invariants.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 2 {
            return Err("schema_version must be exactly 2".to_owned());
        }
        if self.refresh_after_seconds != 4 {
            return Err("refresh_after_seconds must be exactly 4".to_owned());
        }
        if self.publication_units.is_empty() {
            return Err("publication_units must not be empty".to_owned());
        }
        let mut martin_source_ids = BTreeSet::new();
        for (unit_name, unit) in &self.publication_units {
            if !is_martin_identifier(unit_name) {
                return Err(format!(
                    "publication unit {unit_name:?} must be a safe Martin identifier"
                ));
            }
            if unit.layers.is_empty() {
                return Err(format!("{unit_name}: layers must not be empty"));
            }
            if unit.lineage.source_file_asset_ids.is_empty() {
                return Err(format!(
                    "{unit_name}: source_file_asset_ids must not be empty"
                ));
            }
            let mut source_layers = BTreeSet::new();
            for (layer_name, layer) in &unit.layers {
                if layer_name.trim().is_empty() || layer.source_layer.trim().is_empty() {
                    return Err(format!("{unit_name}: layer names must not be empty"));
                }
                if !is_martin_identifier(layer_name)
                    || !is_martin_identifier(&layer.source_layer)
                    || !source_layers.insert(layer.source_layer.trim().to_owned())
                {
                    return Err(format!(
                        "{unit_name}/{layer_name}: Martin layer ids must be unique safe identifiers"
                    ));
                }
                if layer.tile_min_zoom > layer.tile_max_zoom
                    || layer.render_min_zoom > layer.render_max_zoom
                {
                    return Err(format!("{unit_name}/{layer_name}: zoom range is inverted"));
                }
                if layer
                    .feature_filter_properties
                    .values()
                    .any(|property| property.is_empty())
                {
                    return Err(format!(
                        "{unit_name}/{layer_name}: filter property is empty"
                    ));
                }
            }
            match &unit.source {
                ActiveTileSource::DynamicPostgis(source) => {
                    if !is_martin_identifier(&source.martin_source_id)
                        || !martin_source_ids.insert(source.martin_source_id.trim().to_owned())
                        || source.cache_policy != "no_store"
                        || source.tiles_url_template.as_str().contains('?')
                        || source.tiles_url_template.as_str().contains('#')
                        || !martin_route_matches_source_id(
                            &source.tiles_url_template,
                            &source.martin_source_id,
                        )
                    {
                        return Err(format!(
                            "{unit_name}: dynamic source must use a stable Martin id, no_store, and a query-free URL"
                        ));
                    }
                }
                ActiveTileSource::StaticPmtiles(source) => {
                    if !is_martin_identifier(&source.martin_source_id)
                        || !martin_source_ids.insert(source.martin_source_id.trim().to_owned())
                        || source.pmtiles_object_key.trim().is_empty()
                        || source.pmtiles_bytes == 0
                        || !is_sha256(&source.pmtiles_sha256)
                        || !martin_route_matches_source_id(
                            &source.tiles_url_template,
                            &source.martin_source_id,
                        )
                        || !static_pmtiles_identity_matches(
                            unit_name,
                            unit.active_release_id,
                            &source.martin_source_id,
                            &source.pmtiles_object_key,
                        )
                    {
                        return Err(format!(
                            "{unit_name}: static PMTiles metadata or release-addressed Martin identity is invalid"
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn static_pmtiles_identity_matches(
    unit_name: &str,
    release_id: VectorTileReleaseId,
    martin_source_id: &str,
    object_key: &str,
) -> bool {
    let filename = format!("{unit_name}-{release_id}.pmtiles");
    object_key.rsplit('/').next() == Some(filename.as_str())
        && martin_source_id.trim() == filename.trim_end_matches(".pmtiles")
}

fn martin_route_matches_source_id(template: &RuntimeTilesUrlTemplate, source_id: &str) -> bool {
    template
        .as_str()
        .ends_with(&format!("/{source_id}/{{z}}/{{x}}/{{y}}"))
}

fn is_martin_identifier(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value.len() <= 128
        && value
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_alphabetic())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_uppercase()
                || byte.is_ascii_digit()
                || byte == b'_'
                || byte == b'-'
        })
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value.chars().all(|c| c.is_ascii_hexdigit())
        && value == value.to_ascii_lowercase()
}
