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
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or exceeds JavaScript's safe integer range.
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
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or exceeds JavaScript's safe integer range.
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
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is empty, zero, or contains a non-decimal character.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the URL is not absolute, contains invalid placeholders, or uses
    /// HTTP for a non-loopback host.
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
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is empty, not lower-case, or contains an invalid character.
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
    /// Complete `PostGIS` projection revision.
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
    /// Immutable `PMTiles` object key.
    pub pmtiles_object_key: String,
    /// File asset row for the `PMTiles` object.
    pub pmtiles_file_asset_id: FileAssetId,
    /// Lowercase SHA-256 checksum.
    pub pmtiles_sha256: String,
    /// `PMTiles` object byte size.
    pub pmtiles_bytes: u64,
}

/// Closed union selecting exactly one complete Martin source.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveTileSource {
    /// Complete dynamic `PostGIS` source.
    DynamicPostgis(DynamicPostgisSource),
    /// Immutable static `PMTiles` source.
    StaticPmtiles(StaticPmtilesSource),
}

impl ActiveTileSource {
    /// Configured Martin source name, whichever complete source is selected.
    ///
    /// Manifest-wide uniqueness of this name is the one invariant a single unit cannot check, so
    /// the check needs to read the name without re-matching on the variant.
    #[must_use]
    pub fn martin_source_id(&self) -> &str {
        match self {
            Self::DynamicPostgis(source) => &source.martin_source_id,
            Self::StaticPmtiles(source) => &source.martin_source_id,
        }
    }
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
    /// Complete `PostGIS` projection served dynamically by Martin.
    DynamicPostgis,
    /// Immutable `PMTiles` release served through Martin.
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
/// when it was built from the currently selected data revision; this prevents an old `PMTiles`
/// file from being promoted after the canonical source changed. Returning to dynamic is allowed
/// for either the same data revision (a safe fallback) or a newer revision. Every switch advances
/// the per-unit generation exactly once.
///
/// # Errors
///
/// Returns an error when the first publication is not dynamic generation 1, when a later
/// generation does not advance exactly once, or when a static release uses a different data
/// revision from the currently selected one.
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

/// Lifecycle of one static build attempt.
///
/// Mirrors `catalog.vector_tile_build_job.status` exactly. The database constraint and this enum
/// are two statements of one fact, so [`VectorTileBuildStatus::as_str`] is the only place the
/// spelling is written and `TryFrom<&str>` is the only place it is read back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorTileBuildStatus {
    /// Plan recorded, no work started.
    Planned,
    /// Build running against the frozen snapshot.
    Running,
    /// Artifacts produced and validated; eligible for promotion.
    Validated,
    /// Promoted to the active serving source.
    Promoted,
    /// The input release stopped being active before promotion. Terminal.
    Superseded,
    /// The build could not produce validated artifacts. Terminal.
    Failed,
}

impl VectorTileBuildStatus {
    /// Database spelling of this status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Validated => "validated",
            Self::Promoted => "promoted",
            Self::Superseded => "superseded",
            Self::Failed => "failed",
        }
    }

    /// Whether no further transition is possible.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Promoted | Self::Superseded | Self::Failed)
    }
}

impl TryFrom<&str> for VectorTileBuildStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "planned" => Ok(Self::Planned),
            "running" => Ok(Self::Running),
            "validated" => Ok(Self::Validated),
            "promoted" => Ok(Self::Promoted),
            "superseded" => Ok(Self::Superseded),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown vector tile build status: {other}")),
        }
    }
}

/// Lowercase hex SHA-256 of the evidence produced by a validated build.
///
/// The database enforces `^[0-9a-f]{64}$`; constructing this type is where a caller finds out,
/// rather than at the insert.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildEvidenceDigest(String);

impl BuildEvidenceDigest {
    /// Validates a lowercase hex SHA-256 digest.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not exactly 64 lowercase hex characters.
    pub fn new(value: String) -> Result<Self, String> {
        lowercase_sha256(value, "build evidence digest").map(Self)
    }

    /// Borrowed digest.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Lowercase hex SHA-256 of the `PMTiles` object bytes.
///
/// Separate from [`BuildEvidenceDigest`] despite identical validation: one identifies the evidence a
/// build was validated against, the other identifies the bytes being served. A single type would let
/// the two be passed in each other's place, and the database columns they land in are different.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PmtilesChecksum(String);

impl PmtilesChecksum {
    /// Validates a lowercase hex SHA-256 checksum.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not exactly 64 lowercase hex characters.
    pub fn new(value: String) -> Result<Self, String> {
        lowercase_sha256(value, "PMTiles checksum").map(Self)
    }

    /// Borrowed checksum.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The shape both digest columns enforce: `character(64)` matching `^[0-9a-f]{64}$`.
fn lowercase_sha256(value: String, label: &str) -> Result<String, String> {
    let lowercase_hex = value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
    if value.len() != 64 || !lowercase_hex {
        return Err(format!("{label} must be 64 lowercase hex characters"));
    }
    Ok(value)
}

/// What one build attempt is allowed to report about itself.
///
/// Of the six [`VectorTileBuildStatus`] values only two are outcomes a running build can produce.
/// `planned` and `running` are set when the build starts, and `promoted` and `superseded` are the
/// promotion decision's to make — a build that could write its own `promoted` row would publish
/// itself without the active-release check that [`validate_build_promotion`] exists to perform. So
/// the reporting command carries this type rather than a status, and that bypass is unrepresentable.
///
/// `result_snapshot_id` is deliberately absent. The database requires it to equal the build's
/// frozen snapshot, so a caller-supplied copy could only ever agree or be wrong.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorTileBuildOutcome {
    /// Artifacts were produced and validated. Carries the evidence they were validated against.
    Validated(BuildEvidenceDigest),
    /// The attempt could not produce validated artifacts. Carries the operator-facing reason.
    Failed(String),
}

impl VectorTileBuildOutcome {
    /// The status this outcome records.
    #[must_use]
    pub const fn status(&self) -> VectorTileBuildStatus {
        match self {
            Self::Validated(_) => VectorTileBuildStatus::Validated,
            Self::Failed(_) => VectorTileBuildStatus::Failed,
        }
    }
}

/// Rejects a result reported against a build that has already reached a terminal state.
///
/// A terminal build's row is evidence of a decision that was already acted on: `promoted` moved the
/// pointer, `superseded` recorded that the unit had moved on, `failed` closed the attempt.
/// Overwriting any of them would make the ledger disagree with what was served.
///
/// # Errors
///
/// Returns an error when the observed status is terminal.
pub fn validate_build_result_report(
    observed: VectorTileBuildStatus,
    outcome: &VectorTileBuildOutcome,
) -> Result<(), String> {
    if observed.is_terminal() {
        return Err(format!(
            "build already reached the terminal status {}; it cannot be reported as {}",
            observed.as_str(),
            outcome.status().as_str()
        ));
    }
    Ok(())
}

/// The state a promotion decision is made against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorTileBuildPromotionInput {
    /// Current status of the build.
    pub status: VectorTileBuildStatus,
    /// Release the build was started from.
    pub input_release_id: VectorTileReleaseId,
    /// Release currently selected for the unit.
    pub active_release_id: VectorTileReleaseId,
}

/// Outcome of checking whether a build may promote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorTileBuildPromotionVerdict {
    /// The build may promote.
    Promotable,
    /// The unit moved on while this build ran. The build must be recorded
    /// `superseded`, not retried — its artifacts describe a revision no longer served.
    Superseded,
}

/// Decides whether a validated build may still promote.
///
/// A static release is only meaningful for the revision it was built from. If an edit activated a
/// newer release while the build ran, promoting would publish stale tiles under a current pointer,
/// so the build is superseded rather than merely conflicting — the distinction matters because a
/// conflict invites a retry and a supersession does not.
///
/// # Errors
///
/// Returns an error when the build has not reached `validated`, which means promotion was
/// requested before evidence existed.
pub fn validate_build_promotion(
    input: VectorTileBuildPromotionInput,
) -> Result<VectorTileBuildPromotionVerdict, String> {
    if input.status != VectorTileBuildStatus::Validated {
        return Err(format!(
            "only a validated build may promote; this build is {}",
            input.status.as_str()
        ));
    }
    if input.input_release_id != input.active_release_id {
        return Ok(VectorTileBuildPromotionVerdict::Superseded);
    }
    Ok(VectorTileBuildPromotionVerdict::Promotable)
}

/// Rejects a build whose claimed frozen snapshot is not the one on its input release.
///
/// The frozen snapshot is what makes a build reproducible. A build that claims a different
/// snapshot from the release it names is either pointed at the wrong release or reporting the
/// wrong snapshot, and neither can be told apart afterwards — so it is refused before any
/// artifact is trusted.
///
/// # Errors
///
/// Returns an error when the two snapshot identifiers differ.
pub fn validate_build_snapshot_binding(
    claimed_frozen_snapshot: &CanonicalIcebergSnapshotId,
    input_release_snapshot: &CanonicalIcebergSnapshotId,
) -> Result<(), String> {
    if claimed_frozen_snapshot != input_release_snapshot {
        return Err(format!(
            "build claims frozen snapshot {} but its input release is pinned to {}",
            claimed_frozen_snapshot.as_str(),
            input_release_snapshot.as_str()
        ));
    }
    Ok(())
}

/// Foundation-owned v2 runtime manifest.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VectorTileRuntimeManifest {
    /// Exact schema version.
    pub schema_version: u32,
    /// Immutable manifest UUID and `ETag` identity.
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
    ///
    /// # Errors
    ///
    /// Returns an error when any schema, source, layer, lineage, identity, or zoom invariant is
    /// violated.
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
            unit.validate(unit_name)?;
            let martin_source_id = unit.source.martin_source_id().trim();
            if !martin_source_ids.insert(martin_source_id.to_owned()) {
                return Err(format!(
                    "{unit_name}: Martin source id {martin_source_id} is already used by another publication unit"
                ));
            }
        }
        Ok(())
    }
}

impl PublicationUnit {
    /// Validates every invariant one unit can decide without seeing the rest of the manifest.
    ///
    /// Split out of [`VectorTileRuntimeManifest::validate`] so a publisher can reject the single
    /// unit it is changing, and name it in the error, instead of only learning that the assembled
    /// global manifest is invalid. Both callers read this one definition, so the per-unit check and
    /// the published manifest cannot disagree about what a valid unit is.
    ///
    /// The unit name is a parameter because it is the map key in the manifest, and several
    /// invariants — the Martin identifier rule and the release-addressed static filename — relate
    /// the name to the unit's own contents.
    ///
    /// # Errors
    ///
    /// Returns an error when the unit name, layer set, lineage, zoom ranges, or source metadata
    /// violate a publication invariant.
    pub fn validate(&self, unit_name: &str) -> Result<(), String> {
        if !is_martin_identifier(unit_name) {
            return Err(format!(
                "publication unit {unit_name:?} must be a safe Martin identifier"
            ));
        }
        if self.layers.is_empty() {
            return Err(format!("{unit_name}: layers must not be empty"));
        }
        if self.lineage.source_file_asset_ids.is_empty() {
            return Err(format!(
                "{unit_name}: source_file_asset_ids must not be empty"
            ));
        }
        let mut source_layers = BTreeSet::new();
        for (layer_name, layer) in &self.layers {
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
                .any(String::is_empty)
            {
                return Err(format!(
                    "{unit_name}/{layer_name}: filter property is empty"
                ));
            }
        }
        match &self.source {
            ActiveTileSource::DynamicPostgis(source) => {
                if !is_martin_identifier(&source.martin_source_id)
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
                    || source.pmtiles_object_key.trim().is_empty()
                    || source.pmtiles_bytes == 0
                    || !is_sha256(&source.pmtiles_sha256)
                    || !martin_route_matches_source_id(
                        &source.tiles_url_template,
                        &source.martin_source_id,
                    )
                    || !static_pmtiles_identity_matches(
                        unit_name,
                        self.active_release_id,
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
        Ok(())
    }
}

/// Immutable object-storage root for `PMTiles` release derivatives.
///
/// Matches `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX` (ADR-0002).
pub const STATIC_RELEASE_OBJECT_ROOT: &str = "gold/vector-tiles/releases";

/// Derives the release-addressed Martin source name for a static release.
///
/// The name encodes the release, which is what makes a static source immutable: a new release is a
/// new Martin source rather than new bytes behind the same one.
#[must_use]
pub fn static_release_martin_source_id(unit_key: &str, release_id: VectorTileReleaseId) -> String {
    format!("{}-{release_id}", unit_key.trim())
}

/// Derives the write-once `PMTiles` object key for a static release.
///
/// This is the one definition of the layout. It used to be stated in three places that did not
/// agree: this crate's validator checked only the filename, ADR-0004 documented a nested
/// `releases/{release_id}/…` form, and the publisher wrote the flat form. The lenient check is why
/// the disagreement survived — a consumer building a URL from the documented layout would have
/// received a 404 from the layout that actually shipped.
#[must_use]
pub fn static_release_pmtiles_object_key(
    unit_key: &str,
    release_id: VectorTileReleaseId,
) -> String {
    format!(
        "{STATIC_RELEASE_OBJECT_ROOT}/{}.pmtiles",
        static_release_martin_source_id(unit_key, release_id)
    )
}

fn static_pmtiles_identity_matches(
    unit_name: &str,
    release_id: VectorTileReleaseId,
    martin_source_id: &str,
    object_key: &str,
) -> bool {
    // The whole key, not just its filename. Comparing filenames accepted any prefix, which is
    // exactly where the documented and implemented layouts differed.
    martin_source_id.trim() == static_release_martin_source_id(unit_name, release_id)
        && object_key.trim() == static_release_pmtiles_object_key(unit_name, release_id)
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
            .is_some_and(u8::is_ascii_alphabetic)
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
