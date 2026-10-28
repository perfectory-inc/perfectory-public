//! Bakes a polygon unit's static PMTiles release from a served Gold snapshot and folds the admin
//! edits it contains (root ADR-0112 §7·§9), at any size up to a national parcel layer (root
//! ADR-0133 §3).
//!
//! The input is what a unit's served-Gold job wrote (`served_gold_common.py`): the served rows —
//! Silver with every ledgered edit applied, as feature id + tile properties + WKB in the unit's
//! Silver CRS — and a summary naming the Gold snapshot, the CRS and the last edit it includes. A
//! `v1` summary comes with one JSONL file; a `v2` summary names the parts a Spark executor wrote,
//! each with its row count and SHA-256, and the handoff path is then the parts directory. The bake
//! is unit-agnostic: it carries exactly the properties the rows carry. In order:
//!
//! 1. refuse to start when the work disk has less free space than the contract asks for the
//!    handoff's size;
//! 2. stream the handoff once — validating every row, checking each part's hash and count, writing
//!    the GDAL input, and keeping one 16-byte hash per feature id;
//! 3. start a `lakehouse_bake` build against the active validated static release;
//! 4. reproject and repair with GDAL (`-makevalid`), tile with tippecanoe — both pinned containers
//!    with the memory caps of `config/tile-bake-containers.contract.json`, tippecanoe's temporary
//!    files under the work directory. A unit the contract partitions (root ADR-0316: parcels by
//!    시도) is split by feature-id prefix in step 2, only the partitions whose rows or bake inputs
//!    changed are baked, each into a create-only archive kept under the work root, and every
//!    partition's archive is joined into the unit's with tile-join;
//! 5. gate: the archive is PMTiles v3 MVT over the layer's zoom range, and its maxzoom tiles carry
//!    exactly the served feature ids, read by streaming the archive without decoding geometry;
//! 6. upload create-only and rehash, record the result, promote (the unit is left with no fallback);
//! 7. record the fold in the edit store, which retires the folded edits from the customer overlay.
//!
//! No PostGIS is read or written. A failure before the promotion records the build as failed and
//! changes nothing that is served.

use std::ffi::OsString;
use std::fs::File;
use std::io::{BufRead as _, BufReader, BufWriter, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context as _};
use catalog_application::ports::{
    PromoteTileLayerStaticCommand, RecordVectorTileBuildResultCommand,
    RuntimeManifestPublicationCapability, StartLakehouseBakeCommand,
};
use catalog_application::{PromoteTileLayerStatic, VectorTileBuildLifecycle};
use catalog_domain::{
    static_file_asset_id_for_build, static_release_id_for_build, static_release_martin_source_id,
    static_release_pmtiles_object_key, BuildEvidenceDigest, CanonicalIcebergSnapshotId,
    PmtilesChecksum, RuntimeTilesUrlTemplate, ServingGeneration, ValidatedPmtilesArtifact,
    VectorTileBuildOutcome,
};
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_outbox::object_storage::R2ObjectStorage;
use foundation_shared_kernel::ids::{StaffId, VectorTileBuildJobId, VectorTileReleaseId};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, Row as _};
use tokio::process::Command;
use uuid::Uuid;

use crate::boundary_static_release_publish::{
    bounded_failure_reason, create_only_upload_and_rehash, ensure_tool_success,
};
use crate::public_data_control_support::{optional_env_value, required_env_value};
use crate::static_release_url::public_tiles_base_url;
use crate::tile_derivative_object_storage::TileDerivativeR2Config;

#[path = "pmtiles_feature_ids.rs"]
pub mod pmtiles_feature_ids;
#[path = "subpixel.rs"]
pub mod subpixel;
#[path = "tile_equivalence.rs"]
pub mod tile_equivalence;

#[path = "lakehouse_tile_bake_gate.rs"]
mod gate;
#[path = "lakehouse_tile_bake_partitions.rs"]
mod partitions;

use gate::classify_missing;
pub use gate::{check_pmtiles_header, gate_archive, IdGate, Missing, ServedLayer};
pub use partitions::{
    bake_partitions, partition_archive, tile_join_args, CsvOut, CsvPart, PartitionedCsv,
};
use tile_equivalence::{ActiveOrigin, SampleCollector, SampleContract};

const PREFIX: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE";
const CONTRACT_JSON: &str = include_str!("../../../../config/tile-bake-containers.contract.json");
const SERVED_SUMMARY_V1: &str = "foundation-platform.polygon_served_gold.v1";
const SERVED_SUMMARY_V2: &str = "foundation-platform.polygon_served_gold.v2";
/// The Silver CRSs a served snapshot may arrive in; GDAL reprojects either to EPSG:4326.
const SERVED_SRIDS: [i32; 2] = [4326, 5186];
const MAP_EDIT_GATEWAY_BASE_URL_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL";
const MAP_EDIT_WRITE_TOKEN_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN";
/// How many ids a gate failure names; the counts are always complete.
const NAMED_IDS: usize = 5;
/// The most missing ids the gate reads geometry for (ADR-0262); 202609 at detail 12 missed 542.
const MAX_CLASSIFIED_MISSING: usize = 100_000;
/// Range size for reading the active archive: one directory or a few tiles per request.
const EQUIVALENCE_READ_CHUNK_BYTES: usize = 256 * 1024;

/// `config/tile-bake-containers.contract.json`: the pinned images, their memory caps, and how much
/// free work disk a bake needs.
#[derive(Debug, Deserialize)]
pub struct BakeContract {
    images: ImageSet,
    pub work_disk: WorkDisk,
    /// Root ADR-0262: tippecanoe's `--full-detail` (maxzoom tile extent `2^detail`) for a unit
    /// whose features its default 12 drops; a unit not listed keeps the default.
    #[serde(default)]
    pub maxzoom_detail: std::collections::BTreeMap<String, u8>,
    /// Root ADR-0316: a unit listed here is baked one archive per feature-id prefix (a 시도 for
    /// parcels) and the archives are joined; a prefix whose rows did not change keeps its archive.
    #[serde(default)]
    pub partition_by_feature_id_prefix: std::collections::BTreeMap<String, usize>,
}

#[derive(Debug, Deserialize)]
struct ImageSet {
    gdal: Image,
    tippecanoe: Image,
}

#[derive(Debug, Deserialize)]
pub struct Image {
    image: String,
    #[serde(default)]
    version_banner: Option<String>,
    /// Passed as both `--memory` and `--memory-swap`, so the container cannot swap past it either.
    pub memory_limit: String,
}

/// Free space the work root must have before a bake starts: the larger of a floor and a multiple
/// of the handoff's bytes (the GDAL CSV, the GeoJSON sequence, tippecanoe's temporary files and the
/// archive all live there at once).
#[derive(Debug, Deserialize)]
pub struct WorkDisk {
    pub min_free_bytes: u64,
    pub min_free_bytes_per_handoff_byte: u64,
}

impl BakeContract {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let contract: Self = serde_json::from_str(text).context("tile-bake container contract")?;
        for (name, image) in [
            ("gdal", &contract.images.gdal),
            ("tippecanoe", &contract.images.tippecanoe),
        ] {
            ensure!(
                memory_limit_is_a_size(&image.memory_limit),
                "the {name} container's memory_limit {:?} is not a size like 512m or 8g",
                image.memory_limit
            );
        }
        ensure!(
            contract.work_disk.min_free_bytes > 0
                && contract.work_disk.min_free_bytes_per_handoff_byte > 0,
            "the contract's work_disk minimums must be positive"
        );
        ensure!(
            contract
                .maxzoom_detail
                .values()
                .all(|detail| (12..=16).contains(detail)),
            "a maxzoom_detail is between tippecanoe's default 12 and 16"
        );
        ensure!(
            contract
                .partition_by_feature_id_prefix
                .values()
                .all(|chars| (1..=10).contains(chars)),
            "a partition_by_feature_id_prefix is between 1 and 10 characters"
        );
        Ok(contract)
    }

    /// Runs the tippecanoe image the admitted release built (root ADR-0134 §3): the release
    /// build records its image ID in `build.json` and the job passes it here, in place of the
    /// contract's hand-built tag. The version banner check still runs against it.
    pub fn with_release_tippecanoe(mut self, image: Option<String>) -> anyhow::Result<Self> {
        if let Some(image) = image {
            ensure!(
                image.strip_prefix("sha256:").is_some_and(|digest| digest.len() == 64
                    && digest.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))),
                "the release tippecanoe image must be an image ID like sha256:<64 hex>, got {image:?}"
            );
            self.images.tippecanoe.image = image;
        }
        Ok(self)
    }
}

fn memory_limit_is_a_size(limit: &str) -> bool {
    let digits = limit.trim_end_matches(['m', 'g']);
    limit.len() == digits.len() + 1
        && !digits.is_empty()
        && !digits.starts_with('0')
        && digits.bytes().all(|b| b.is_ascii_digit())
}

/// One part of a `v2` handoff, as the served-Gold job recorded it.
#[derive(Debug, Clone, Deserialize)]
pub struct HandoffPart {
    pub path: String,
    pub rows: usize,
    pub sha256: String,
}

/// What the served-Gold job recorded about the state it wrote.
#[derive(Debug, Clone, Deserialize)]
pub struct ServedSummary {
    schema_version: String,
    pub unit: String,
    pub feature_id_property: String,
    pub geometry_srid: i32,
    pub canonical_iceberg_snapshot_id: String,
    pub edits_through_change_seq: u64,
    pub served_row_count: usize,
    status: String,
    #[serde(default)]
    pub source_snapshot_id: Option<String>,
    #[serde(default)]
    pub handoff_parts: Option<Vec<HandoffPart>>,
}

impl ServedSummary {
    pub fn validate(&self, unit: &str) -> anyhow::Result<()> {
        ensure!(self.status == "ready", "served summary is not ready");
        ensure!(
            self.unit == unit,
            "served summary is for unit {}, not {unit}",
            self.unit
        );
        ensure!(
            self.served_row_count > 0,
            "the served snapshot is empty; baking it would erase the layer"
        );
        ensure!(
            SERVED_SRIDS.contains(&self.geometry_srid),
            "served snapshot CRS EPSG:{} is not one the bake reprojects",
            self.geometry_srid
        );
        match self.schema_version.as_str() {
            SERVED_SUMMARY_V1 => ensure!(
                self.handoff_parts.is_none(),
                "a v1 served summary names no parts; its handoff is one file"
            ),
            SERVED_SUMMARY_V2 => self.validate_parts()?,
            other => bail!("served summary schema is {other}"),
        }
        Ok(())
    }

    /// The Silver snapshot this bake binds (ADR-0133 §5): only a v2 summary binds one. A v1 summary
    /// may still carry `source_snapshot_id` as an extra field (the admin served-Gold job writes it),
    /// and that must never turn a v1 bake into a Silver-bound one.
    pub fn bound_silver_snapshot(&self) -> Option<&str> {
        if self.schema_version == SERVED_SUMMARY_V2 {
            self.source_snapshot_id.as_deref()
        } else {
            None
        }
    }

    fn validate_parts(&self) -> anyhow::Result<()> {
        ensure!(
            self.source_snapshot_id
                .as_deref()
                .is_some_and(|id| !id.is_empty()),
            "a v2 served summary names the Silver source snapshot it read"
        );
        let parts = self
            .handoff_parts
            .as_deref()
            .filter(|parts| !parts.is_empty())
            .context("a v2 served summary names its handoff parts")?;
        let mut paths = std::collections::BTreeSet::new();
        for part in parts {
            let relative = Path::new(&part.path);
            ensure!(
                !part.path.is_empty()
                    && relative
                        .components()
                        .all(|component| matches!(component, Component::Normal(_))),
                "handoff part {:?} is not a path inside the parts directory",
                part.path
            );
            ensure!(
                part.sha256.len() == 64
                    && part
                        .sha256
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "handoff part {} has no lowercase hex SHA-256",
                part.path
            );
            ensure!(
                paths.insert(part.path.as_str()),
                "handoff part {} is named twice",
                part.path
            );
        }
        let rows: usize = parts.iter().map(|part| part.rows).sum();
        ensure!(
            rows == self.served_row_count,
            "the handoff parts hold {rows} rows, the summary promises {}",
            self.served_row_count
        );
        Ok(())
    }
}

/// One served row, as the tile bake reads it: the feature id, the tile properties, the geometry.
#[derive(Debug, Deserialize)]
pub struct ServedRow {
    pub feature_id: String,
    pub properties: std::collections::BTreeMap<String, String>,
    pub geometry_wkb_hex: String,
    pub geometry_srid: i32,
}

/// The fixed-size stand-in for a feature id: the first 16 bytes of its SHA-256.
pub fn id_hash(id: &str) -> u128 {
    let digest = Sha256::digest(id.as_bytes());
    let mut first = [0_u8; 16];
    first.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(first)
}

/// Every served feature id, as sorted distinct hashes: 16 bytes per feature.
#[derive(Debug)]
pub struct ServedIds {
    sorted: Vec<u128>,
}

impl ServedIds {
    /// Sorts the hashes; returns the first repeated one if a feature id was served twice.
    pub fn from_hashes(mut hashes: Vec<u128>) -> Result<Self, u128> {
        hashes.sort_unstable();
        if let Some(pair) = hashes.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(pair[0]);
        }
        Ok(Self { sorted: hashes })
    }

    pub(crate) fn len(&self) -> usize {
        self.sorted.len()
    }

    fn position(&self, hash: u128) -> Option<usize> {
        self.sorted.binary_search(&hash).ok()
    }

    /// Whether `id` is served (ADR-0251: whether the new edition holds it anywhere).
    pub(crate) fn contains(&self, id: &str) -> bool {
        self.position(id_hash(id)).is_some()
    }

    /// Evidence: the SHA-256 of the sorted id hashes.
    pub fn digest(&self) -> String {
        let mut hasher = Sha256::new();
        for hash in &self.sorted {
            hasher.update(hash.to_be_bytes());
        }
        format!("{:x}", hasher.finalize())
    }
}

/// A reader that hashes every byte it hands on, so a part is checked in the same pass that reads it.
struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        Ok(read)
    }
}

/// Calls `on_line` with every non-empty handoff line and where it came from. For a `v2` handoff
/// it checks each part's row count and SHA-256 after reading it to its end.
fn for_each_handoff_line(
    summary: &ServedSummary,
    handoff: &Path,
    mut on_line: impl FnMut(&str, &dyn Fn() -> String) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut read_file = |path: &Path, part: Option<&HandoffPart>| -> anyhow::Result<()> {
        let file =
            File::open(path).with_context(|| format!("served handoff {}", path.display()))?;
        let mut reader = BufReader::with_capacity(
            1 << 20,
            HashingReader {
                inner: file,
                hasher: Sha256::new(),
            },
        );
        let mut line = String::new();
        let (mut number, mut rows) = (0_usize, 0_usize);
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            number += 1;
            if line.trim().is_empty() {
                continue;
            }
            rows += 1;
            on_line(line.trim_end_matches(['\n', '\r']), &|| {
                format!("served handoff {} line {number}", path.display())
            })?;
        }
        if let Some(part) = part {
            ensure!(
                rows == part.rows,
                "handoff part {} holds {rows} rows, the summary promises {}",
                part.path,
                part.rows
            );
            let sha256 = format!("{:x}", reader.into_inner().hasher.finalize());
            ensure!(
                sha256 == part.sha256,
                "handoff part {} hashes to {sha256}, the summary promises {}",
                part.path,
                part.sha256
            );
        }
        Ok(())
    };
    match &summary.handoff_parts {
        None => read_file(handoff, None),
        Some(parts) => parts
            .iter()
            .try_for_each(|part| read_file(&handoff.join(&part.path), Some(part))),
    }
}

/// The bytes the handoff occupies, which sizes the work disk it needs.
pub fn handoff_bytes(summary: &ServedSummary, handoff: &Path) -> anyhow::Result<u64> {
    let size = |path: &Path| {
        std::fs::metadata(path)
            .map(|meta| meta.len())
            .with_context(|| format!("served handoff {}", path.display()))
    };
    match &summary.handoff_parts {
        None => size(handoff),
        Some(parts) => parts
            .iter()
            .map(|part| size(&handoff.join(&part.path)))
            .sum(),
    }
}

/// The served handoff, validated and turned into the GDAL input.
#[derive(Debug)]
pub struct PreparedHandoff<W> {
    pub csv: W,
    /// The tile properties: the feature id first, then every row property.
    pub columns: Vec<String>,
    pub ids: ServedIds,
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Streams the handoff once into `csv` — one CSV row per feature: id, properties, hex WKB —
/// refusing repeats, a CRS or a property set that differs between rows, a part whose bytes or
/// count differ from the summary, and a total the summary did not promise.
pub fn prepare_handoff<W: CsvOut>(
    summary: &ServedSummary,
    handoff: &Path,
    mut csv: W,
) -> anyhow::Result<PreparedHandoff<W>> {
    let mut columns: Option<Vec<String>> = None;
    let mut hashes = Vec::with_capacity(summary.served_row_count);
    for_each_handoff_line(summary, handoff, |line, location| {
        let row: ServedRow = serde_json::from_str(line)
            .with_context(|| format!("{} is not a served row", location()))?;
        ensure!(
            row.geometry_srid == summary.geometry_srid,
            "served row {} is EPSG:{}, the snapshot EPSG:{}",
            row.feature_id,
            row.geometry_srid,
            summary.geometry_srid
        );
        ensure!(
            !row.geometry_wkb_hex.is_empty()
                && row.geometry_wkb_hex.bytes().all(|b| b.is_ascii_hexdigit()),
            "served row {} has no WKB",
            row.feature_id
        );
        ensure!(
            !row.feature_id.is_empty()
                && !row.properties.contains_key(&summary.feature_id_property),
            "served row {} is missing its id or repeats it as a property",
            row.feature_id
        );
        if let Some(first) = &columns {
            ensure!(
                first[1..].iter().eq(row.properties.keys()),
                "served row {} carries other properties than the first row",
                row.feature_id
            );
        } else {
            let names: Vec<String> = std::iter::once(summary.feature_id_property.clone())
                .chain(row.properties.keys().cloned())
                .collect();
            let header: Vec<String> = names.iter().map(|name| csv_field(name)).collect();
            csv.header(&format!("{},geometry", header.join(",")))?;
            columns = Some(names);
        }
        let columns = columns.as_deref().unwrap_or_default();
        let mut fields = vec![csv_field(&row.feature_id)];
        fields.extend(
            columns[1..]
                .iter()
                .map(|name| csv_field(row.properties.get(name).map_or("", String::as_str))),
        );
        fields.push(row.geometry_wkb_hex);
        csv.row(&row.feature_id, &fields.join(","))?;
        hashes.push(id_hash(&row.feature_id));
        Ok(())
    })?;
    ensure!(
        hashes.len() == summary.served_row_count,
        "served handoff holds {} rows, the summary promised {}",
        hashes.len(),
        summary.served_row_count
    );
    let ids = match ServedIds::from_hashes(hashes) {
        Ok(ids) => ids,
        Err(repeated) => {
            let named = ids_with_hashes(summary, handoff, &[repeated])?;
            bail!("feature {named:?} is served twice");
        }
    };
    Ok(PreparedHandoff {
        csv,
        columns: columns.unwrap_or_default(),
        ids,
    })
}

/// Names the served ids whose hashes are given, by reading the handoff again. Used only to word a
/// refusal, so the gate itself never holds the id strings.
fn ids_with_hashes(
    summary: &ServedSummary,
    handoff: &Path,
    hashes: &[u128],
) -> anyhow::Result<Vec<String>> {
    #[derive(Deserialize)]
    struct IdOnly {
        feature_id: String,
    }
    let mut named = Vec::new();
    for_each_handoff_line(summary, handoff, |line, _| {
        if named.len() < NAMED_IDS {
            let row: IdOnly = serde_json::from_str(line)?;
            if hashes.contains(&id_hash(&row.feature_id)) && !named.contains(&row.feature_id) {
                named.push(row.feature_id);
            }
        }
        Ok(())
    })?;
    Ok(named)
}

/// Refuses a work disk with less free space than the contract asks for this handoff.
pub fn ensure_work_disk(free: u64, handoff_bytes: u64, disk: &WorkDisk) -> anyhow::Result<()> {
    let needed = handoff_bytes
        .saturating_mul(disk.min_free_bytes_per_handoff_byte)
        .max(disk.min_free_bytes);
    ensure!(
        free >= needed,
        "the work disk has {free} bytes free; a {handoff_bytes}-byte handoff needs {needed} (config/tile-bake-containers.contract.json work_disk)"
    );
    Ok(())
}

pub use foundation_publisher_core::free_space::free_bytes;

struct Config {
    unit_key: String,
    served_handoff: PathBuf,
    served_summary: PathBuf,
    work_root: PathBuf,
    public_tiles_base_url: String,
    operator_staff_id: StaffId,
    build_idempotency_key: String,
    promote_idempotency_key: String,
    tool_timeout: Duration,
    edit_store: Option<(String, String)>,
    /// The Compose project of the job that runs the bake: GDAL and tippecanoe carry its labels so
    /// the unit's ExecStopPost removes them after a timeout, with the job's Spark container.
    container_project: Option<String>,
    /// ADR-0253: bake and gate only; nothing is recorded, uploaded or promoted.
    rehearsal: bool,
    /// ADR-0262: the contract's maxzoom detail for this unit, if it sets one.
    maxzoom_detail: Option<u8>,
    /// ADR-0316: the contract's partition prefix length for this unit, if it sets one.
    partition_prefix: Option<usize>,
}

/// `{PREFIX}_REHEARSAL`: unset or `0` is a bake, `1` a rehearsal; anything else is refused, so a
/// misspelt rehearsal never promotes.
fn rehearsal_flag(value: Option<String>) -> anyhow::Result<bool> {
    match value.as_deref() {
        None | Some("0") => Ok(false),
        Some("1") => Ok(true),
        Some(other) => bail!("{PREFIX}_REHEARSAL is {other:?}; it is 0 or 1"),
    }
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = format!("{PREFIX}_CONFIRM");
        ensure!(
            required_env_value(&confirm)? == "1",
            "{confirm}=1 is required; a lakehouse bake moves the serving pointer"
        );
        let var = |suffix: &str| required_env_value(&format!("{PREFIX}_{suffix}"));
        let timeout: u64 = var("TOOL_TIMEOUT_SECONDS")?
            .parse()
            .context("TOOL_TIMEOUT_SECONDS")?;
        ensure!(
            (1..=43_200).contains(&timeout),
            "TOOL_TIMEOUT_SECONDS must be in 1..=43200"
        );
        let edit_store = match (
            optional_env_value(MAP_EDIT_GATEWAY_BASE_URL_ENV)?,
            optional_env_value(MAP_EDIT_WRITE_TOKEN_ENV)?,
        ) {
            (Some(url), Some(token)) => Some((url, token)),
            (None, None) => None,
            _ => {
                bail!("{MAP_EDIT_GATEWAY_BASE_URL_ENV} and {MAP_EDIT_WRITE_TOKEN_ENV} go together")
            }
        };
        let unit_key = var("UNIT")?;
        ensure!(
            unit_key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "UNIT {unit_key:?} names the work directory and may hold only letters, digits, _ and -"
        );
        Ok(Self {
            unit_key,
            served_handoff: PathBuf::from(var("SERVED_HANDOFF")?),
            served_summary: PathBuf::from(var("SERVED_SUMMARY")?),
            work_root: PathBuf::from(var("WORK_ROOT")?),
            public_tiles_base_url: public_tiles_base_url(&var("PUBLIC_TILES_BASE_URL")?)
                .context("PUBLIC_TILES_BASE_URL is not a public tile address")?,
            operator_staff_id: StaffId::new(
                Uuid::parse_str(&var("OPERATOR_STAFF_ID")?).context("OPERATOR_STAFF_ID")?,
            ),
            build_idempotency_key: var("BUILD_IDEMPOTENCY_KEY")?,
            promote_idempotency_key: var("PROMOTE_IDEMPOTENCY_KEY")?,
            tool_timeout: Duration::from_secs(timeout),
            edit_store,
            container_project: container_project(optional_env_value(&format!(
                "{PREFIX}_CONTAINER_PROJECT"
            ))?)?,
            rehearsal: rehearsal_flag(optional_env_value(&format!("{PREFIX}_REHEARSAL"))?)?,
            maxzoom_detail: None,
            partition_prefix: None,
        })
    }
}

struct ActiveStatic {
    release_id: VectorTileReleaseId,
    origin: ActiveOrigin,
    pmtiles_object_key: String,
    serving_generation: ServingGeneration,
    snapshot: String,
    layer: ServedLayer,
}

async fn read_active_static(pool: &PgPool, unit_key: &str) -> anyhow::Result<ActiveStatic> {
    let row = sqlx::query(
        "SELECT unit.active_release_id, unit.serving_generation, release.source_kind,
                release.canonical_iceberg_snapshot_id, release.pmtiles_object_key,
                EXISTS (SELECT 1 FROM catalog.vector_tile_build_job AS build
                        WHERE build.result_release_id = release.id
                          AND build.kind = 'lakehouse_bake') AS produced_by_lakehouse_bake,
                layer.source_layer, layer.feature_id_property,
                layer.tile_min_zoom, layer.tile_max_zoom, layer.render_max_zoom,
                layer.feature_filter_properties
         FROM catalog.vector_tile_publication_unit AS unit
         JOIN catalog.vector_tile_release AS release ON release.id = unit.active_release_id
         JOIN catalog.vector_tile_release_layer AS layer ON layer.release_id = release.id
         WHERE unit.unit_key = $1",
    )
    .bind(unit_key)
    .fetch_all(pool)
    .await?;
    ensure!(
        row.len() == 1,
        "the active {unit_key} release must have exactly one layer, has {}",
        row.len()
    );
    let row = &row[0];
    let kind: String = row.try_get("source_kind")?;
    ensure!(
        kind == "static_pmtiles",
        "the active {unit_key} release is {kind}; a lakehouse bake replaces a static one"
    );
    let filters: Value = row.try_get("feature_filter_properties")?;
    let feature_id_property: String = row.try_get("feature_id_property")?;
    let mut properties = vec![feature_id_property.clone()];
    properties.extend(
        filters
            .as_object()
            .map(|map| map.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
    );
    Ok(ActiveStatic {
        release_id: VectorTileReleaseId::new(row.try_get("active_release_id")?),
        origin: if row.try_get("produced_by_lakehouse_bake")? {
            ActiveOrigin::LakehouseBake
        } else {
            ActiveOrigin::OtherOven
        },
        pmtiles_object_key: row
            .try_get::<Option<String>, _>("pmtiles_object_key")?
            .context("the active static release names no PMTiles object")?,
        serving_generation: ServingGeneration::new(u64::try_from(
            row.try_get::<i64, _>("serving_generation")?,
        )?)
        .map_err(anyhow::Error::msg)?,
        snapshot: row.try_get("canonical_iceberg_snapshot_id")?,
        layer: ServedLayer {
            source_layer: row.try_get("source_layer")?,
            feature_id_property,
            tile_min_zoom: u8::try_from(row.try_get::<i16, _>("tile_min_zoom")?)?,
            tile_max_zoom: u8::try_from(row.try_get::<i16, _>("tile_max_zoom")?)?,
            render_max_zoom: u8::try_from(row.try_get::<i16, _>("render_max_zoom")?)?,
            properties,
        },
    })
}

async fn docker(
    args: Vec<OsString>,
    timeout: Duration,
    label: &str,
) -> anyhow::Result<std::process::Output> {
    let output = tokio::time::timeout(
        timeout,
        Command::new("docker")
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .with_context(|| format!("{label} exceeded its time bound"))?
    .with_context(|| format!("failed to start {label}"))?;
    ensure_tool_success(label, &output)?;
    Ok(output)
}

/// A Compose project name: lowercase letters, digits, `_` and `-`, starting with a letter or digit.
pub(crate) fn container_project(value: Option<String>) -> anyhow::Result<Option<String>> {
    if let Some(project) = &value {
        ensure!(
            project
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                && project.bytes().all(|b| b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || b == b'_'
                    || b == b'-'),
            "CONTAINER_PROJECT {project:?} is not a Compose project name"
        );
    }
    Ok(value)
}

/// `docker run` for one pinned tool: no network, the contract's memory cap with no swap beyond
/// it, the work directory at `/w`. With a job's Compose project, the container carries the labels
/// the job's cleanup removes by (`invocation_cleanup`): killing the bake leaves the tool running,
/// and the timeout must not leave it holding its memory.
pub fn run_args(work: &Path, image: &Image, project: Option<&str>) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "run".into(),
        "--rm".into(),
        "--network".into(),
        "none".into(),
        "--memory".into(),
        image.memory_limit.clone().into(),
        "--memory-swap".into(),
        image.memory_limit.clone().into(),
    ];
    if let Some(project) = project {
        use foundation_publisher_core::compose_labels::{ONEOFF_LABEL, PROJECT_LABEL};
        args.extend([
            "--label".into(),
            format!("{PROJECT_LABEL}={project}").into(),
            "--label".into(),
            ONEOFF_LABEL.into(),
        ]);
    }
    // Run as the owner of the work directory, so the tools' outputs stay removable afterwards.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if let Ok(meta) = std::fs::metadata(work) {
            args.push("-u".into());
            args.push(format!("{}:{}", meta.uid(), meta.gid()).into());
        }
    }
    args.push("-v".into());
    args.push(format!("{}:/w", work.display()).into());
    args.push(image.image.clone().into());
    args
}

/// tippecanoe's arguments after the image: the flags measured on the Seoul pilot (root ADR-0133),
/// plus `-t` so its temporary files stay on the work disk and `-P` because the GeoJSON sequence
/// has one feature per line.
pub fn tippecanoe_args(
    layer: &ServedLayer,
    columns: &[String],
    maxzoom_detail: Option<u8>,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = [
        "-o".to_owned(),
        "/w/unit.pmtiles".to_owned(),
        "-t".to_owned(),
        "/w/tmp".to_owned(),
        "-P".to_owned(),
        "-l".to_owned(),
        layer.source_layer.clone(),
        "-Z".to_owned(),
        layer.tile_min_zoom.to_string(),
        "-z".to_owned(),
        layer.tile_max_zoom.to_string(),
        "--no-feature-limit".to_owned(),
        "--no-tile-size-limit".to_owned(),
        "--no-tiny-polygon-reduction".to_owned(),
        "--detect-shared-borders".to_owned(),
        "--force".to_owned(),
        "--quiet".to_owned(),
    ]
    .map(OsString::from)
    .into();
    if let Some(detail) = maxzoom_detail {
        args.push(format!("--full-detail={detail}").into());
    }
    for property in columns {
        args.push("-y".into());
        args.push(property.into());
    }
    args.push("/w/served.geojsons".into());
    args
}

async fn build_archive(
    config: &Config,
    images: &ImageSet,
    work: &Path,
    columns: &[String],
    srid: i32,
    layer: &ServedLayer,
    partitions: Option<Vec<CsvPart>>,
) -> anyhow::Result<PathBuf> {
    let missing: Vec<&String> = layer
        .properties
        .iter()
        .filter(|name| !columns.contains(name))
        .collect();
    ensure!(
        missing.is_empty(),
        "the served snapshot lacks properties the served layer promises: {missing:?}"
    );
    let Some(partitions) = partitions else {
        bake_unit(config, images, work, columns, srid, layer).await?;
        return Ok(work.join("unit.pmtiles"));
    };
    // ADR-0316: the tippecanoe version, not the release's image ID, which every release build
    // changes while the pinned source commit (and so the bytes it bakes) stays the same.
    let bake_inputs = format!(
        "{:?}",
        (
            &images.gdal.image,
            &images.tippecanoe.version_banner,
            srid,
            tippecanoe_args(layer, columns, config.maxzoom_detail),
        )
    );
    let root = config
        .work_root
        .join(format!("{}-partitions", config.unit_key));
    let archives = bake_partitions(work, &root, partitions, &bake_inputs, |dir| async move {
        bake_unit(config, images, &dir, columns, srid, layer).await
    })
    .await?;
    std::fs::remove_dir_all(work_partition_csvs(&work.join("served.csv")))?;
    let join = work.join("join");
    std::fs::create_dir_all(&join)?;
    let mut prefixes = Vec::with_capacity(archives.len());
    for (prefix, archive, _) in &archives {
        std::fs::hard_link(archive, join.join(format!("{prefix}.pmtiles")))?;
        prefixes.push(prefix.clone());
    }
    let baked: Vec<&str> = archives
        .iter()
        .filter(|(_, _, baked)| *baked)
        .map(|(prefix, _, _)| prefix.as_str())
        .collect();
    println!(
        "lakehouse-tile-bake-partitions unit={} partitions={} baked={} reused={}",
        config.unit_key,
        archives.len(),
        baked.join(","),
        archives.len() - baked.len()
    );
    let mut tile_join = run_args(
        work,
        &images.tippecanoe,
        config.container_project.as_deref(),
    );
    // The image's entrypoint is tippecanoe; tile-join is installed beside it from the same commit.
    let image = tile_join
        .pop()
        .context("docker run arguments end with the image")?;
    tile_join.extend(["--entrypoint".into(), "tile-join".into(), image]);
    tile_join.extend(tile_join_args(&prefixes));
    docker(tile_join, config.tool_timeout, "tile-join").await?;
    std::fs::remove_dir_all(&join)?;
    Ok(work.join("unit.pmtiles"))
}

/// GDAL then tippecanoe in `work`: `served.csv` in, `unit.pmtiles` out.
async fn bake_unit(
    config: &Config,
    images: &ImageSet,
    work: &Path,
    columns: &[String],
    srid: i32,
    layer: &ServedLayer,
) -> anyhow::Result<()> {
    let source_srs = format!("EPSG:{srid}");
    let project = config.container_project.as_deref();
    let mut gdal = run_args(work, &images.gdal, project);
    gdal.extend(
        [
            "ogr2ogr",
            "-f",
            "GeoJSONSeq",
            "/w/served.geojsons",
            "/w/served.csv",
            "-oo",
            "GEOM_POSSIBLE_NAMES=geometry",
            "-oo",
            "KEEP_GEOM_COLUMNS=NO",
            "-s_srs",
            source_srs.as_str(),
            "-t_srs",
            "EPSG:4326",
            "-makevalid",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-lco",
            "RS=NO",
            "-lco",
            "COORDINATE_PRECISION=7",
        ]
        .map(OsString::from),
    );
    docker(gdal, config.tool_timeout, "ogr2ogr").await?;
    // The GDAL input is no longer needed and is as large as the handoff.
    std::fs::remove_file(work.join("served.csv"))?;

    let banner = docker(
        run_args(work, &images.tippecanoe, project)
            .into_iter()
            .chain(["--version".into()])
            .collect(),
        config.tool_timeout,
        "tippecanoe --version",
    )
    .await?;
    let reported = String::from_utf8_lossy(&banner.stderr).trim().to_owned()
        + String::from_utf8_lossy(&banner.stdout).trim();
    let expected = images
        .tippecanoe
        .version_banner
        .as_deref()
        .context("tippecanoe version_banner")?;
    ensure!(
        reported.contains(expected),
        "tippecanoe reports {reported:?}, the contract pins {expected:?}"
    );

    let mut tippecanoe = run_args(work, &images.tippecanoe, project);
    tippecanoe.extend(tippecanoe_args(layer, columns, config.maxzoom_detail));
    docker(tippecanoe, config.tool_timeout, "tippecanoe").await?;
    // The GeoJSON sequence is the largest file in the work directory; the gate reads the archive.
    std::fs::remove_file(work.join("served.geojsons"))?;
    Ok(())
}

async fn record_fold(
    config: &Config,
    through: u64,
    release_id: VectorTileReleaseId,
) -> anyhow::Result<()> {
    let Some((base_url, token)) = &config.edit_store else {
        ensure!(
            through == 0,
            "the snapshot folds edits through {through} but no edit store is configured"
        );
        return Ok(());
    };
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .post(format!("{}/folds/{}", base_url.trim_end_matches('/'), config.unit_key))
        .bearer_auth(token)
        .json(&serde_json::json!({"folded_through_change_seq": through, "release_id": release_id.to_string()}))
        .send()
        .await
        .context("edit store unreachable while recording the fold")?
        .error_for_status()
        .context("edit store refused the fold")?;
    Ok(())
}

/// Runs one lakehouse bake end to end.
///
/// # Errors
/// Refuses bad configuration, inputs or too little work disk; records the build as failed for any
/// failure after it started and before promotion; returns an error if the fold cannot be recorded
/// after promotion (the tiles are already correct, and re-running the fold call is safe).
pub async fn run() -> anyhow::Result<()> {
    let mut config = Config::from_env()?;
    let contract = BakeContract::parse(CONTRACT_JSON)?
        .with_release_tippecanoe(optional_env_value(&format!("{PREFIX}_TIPPECANOE_IMAGE"))?)?;
    config.maxzoom_detail = contract.maxzoom_detail.get(&config.unit_key).copied();
    config.partition_prefix = contract
        .partition_by_feature_id_prefix
        .get(&config.unit_key)
        .copied();
    let samples = SampleContract::parse(tile_equivalence::SAMPLE_CONTRACT_JSON)?;
    let summary: ServedSummary =
        serde_json::from_str(&std::fs::read_to_string(&config.served_summary)?)
            .context("served summary")?;
    summary.validate(&config.unit_key)?;
    let input_bytes = handoff_bytes(&summary, &config.served_handoff)?;
    std::fs::create_dir_all(&config.work_root)
        .with_context(|| format!("work root {}", config.work_root.display()))?;
    ensure_work_disk(
        free_bytes(&config.work_root)?,
        input_bytes,
        &contract.work_disk,
    )?;
    let work = config
        .work_root
        .join(format!("{}-{}", config.unit_key, Uuid::now_v7()));
    std::fs::create_dir_all(work.join("tmp"))
        .with_context(|| format!("work directory {}", work.display()))?;
    let result = bake_in(&config, &contract, &samples, summary, &work).await;
    // The archive is in R2 (or the attempt failed); nothing in the work directory is used again.
    if let Err(error) = std::fs::remove_dir_all(&work) {
        tracing::warn!(path = %work.display(), error = %error, "failed to remove the bake work directory");
    }
    result
}

async fn bake_in(
    config: &Config,
    contract: &BakeContract,
    samples: &SampleContract,
    summary: ServedSummary,
    work: &Path,
) -> anyhow::Result<()> {
    let summary = Arc::new(summary);
    let prepared = {
        let (summary, handoff, csv, partition_prefix) = (
            summary.clone(),
            config.served_handoff.clone(),
            work.join("served.csv"),
            config.partition_prefix,
        );
        tokio::task::spawn_blocking(move || -> anyhow::Result<Prepared> {
            if let Some(chars) = partition_prefix {
                let dir = work_partition_csvs(&csv);
                std::fs::create_dir_all(&dir)?;
                let prepared =
                    prepare_handoff(&summary, &handoff, PartitionedCsv::new(dir, chars))?;
                return Ok((prepared.columns, prepared.ids, Some(prepared.csv.finish()?)));
            }
            let out = BufWriter::with_capacity(1 << 20, File::create(&csv)?);
            let prepared = prepare_handoff(&summary, &handoff, out)?;
            prepared
                .csv
                .into_inner()
                .map_err(|error| error.into_error())?
                .sync_all()?;
            Ok((prepared.columns, prepared.ids, None))
        })
        .await
        .context("the handoff reader stopped")??
    };

    let pool = PgPool::connect(&required_env_value("DATABASE_URL")?).await?;
    let active = read_active_static(&pool, &config.unit_key).await?;
    ensure!(
        active.snapshot != summary.canonical_iceberg_snapshot_id,
        "the active release already serves Gold snapshot {}",
        active.snapshot
    );
    if config.rehearsal {
        return rehearse(
            config,
            &contract.images,
            work,
            prepared,
            &active,
            samples,
            summary,
        )
        .await;
    }
    let uow = Arc::new(
        PgCatalogUnitOfWork::new(pool.clone())
            .with_runtime_manifest_publication(RuntimeManifestPublicationCapability::enabled()),
    );
    let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
    let build_job_id = lifecycle
        .start_lakehouse_bake(StartLakehouseBakeCommand {
            unit_key: config.unit_key.clone(),
            input_release_id: active.release_id,
            canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(
                summary.canonical_iceberg_snapshot_id.clone(),
            )
            .map_err(anyhow::Error::msg)?,
            idempotency_key: config.build_idempotency_key.clone(),
            operator_staff_id: config.operator_staff_id,
            silver_source: crate::lakehouse_bake_verdict::silver_source(
                summary.bound_silver_snapshot(),
            )?,
        })
        .await?;

    let built = bake_and_upload(
        config,
        &contract.images,
        build_job_id,
        work,
        prepared,
        &active,
        samples,
        summary.clone(),
    )
    .await;
    let artifact = match built {
        Ok(artifact) => artifact,
        Err(error) => {
            lifecycle
                .record_result(RecordVectorTileBuildResultCommand {
                    build_job_id,
                    outcome: VectorTileBuildOutcome::Failed(bounded_failure_reason(&error)),
                    operator_staff_id: config.operator_staff_id,
                })
                .await
                .context("the lakehouse bake failed and its failure could not be recorded")?;
            return Err(error);
        }
    };
    let release_id = artifact.1.release_id;
    lifecycle
        .record_result(RecordVectorTileBuildResultCommand {
            build_job_id,
            outcome: VectorTileBuildOutcome::Validated {
                evidence: artifact.0,
                artifact: artifact.1,
            },
            operator_staff_id: config.operator_staff_id,
        })
        .await?;
    let manifest = PromoteTileLayerStatic::new(uow)
        .execute(PromoteTileLayerStaticCommand {
            unit_key: config.unit_key.clone(),
            build_job_id,
            expected_active_release_id: active.release_id,
            expected_serving_generation: active.serving_generation,
            idempotency_key: config.promote_idempotency_key.clone(),
            operator_staff_id: config.operator_staff_id,
        })
        .await?;
    // Only after the tiles that contain the edits are served may the overlay forget them.
    record_fold(config, summary.edits_through_change_seq, release_id).await?;
    println!(
        "lakehouse-tile-bake-ok unit={} build_job_id={build_job_id} release_id={release_id} manifest_generation={} gold_snapshot={} folded_through_change_seq={}",
        config.unit_key,
        manifest.manifest_generation.value(),
        summary.canonical_iceberg_snapshot_id,
        summary.edits_through_change_seq
    );
    Ok(())
}

/// The tile properties, the served ids and, for a partitioned unit (ADR-0316), each partition's
/// GDAL input in place of the one `served.csv`.
type Prepared = (Vec<String>, ServedIds, Option<Vec<CsvPart>>);

/// The directory a partitioned unit's per-partition CSVs are written to, beside `served.csv`.
fn work_partition_csvs(served_csv: &Path) -> PathBuf {
    served_csv.with_file_name("partition-csv")
}

/// What the bake produced before anything is written: the archive, the maxzoom id gate (the SHA-256
/// of its id hashes and the sub-pixel ids it allowed to be missing, ADR-0262), and — when the active release came from another oven — the first-release equivalence
/// outcome with the file its whole evidence was written to.
struct Gated {
    archive: PathBuf,
    ids: IdGate,
    equivalence: Option<(tile_equivalence::Equivalence, PathBuf)>,
}

/// Bakes and runs every gate, writing nothing but the work directory and the equivalence evidence
/// (named after `label`) beside it.
#[allow(clippy::too_many_arguments)]
async fn bake_and_gate(
    config: &Config,
    images: &ImageSet,
    label: &str,
    work: &Path,
    (columns, ids, partitions): Prepared,
    active: &ActiveStatic,
    samples: &SampleContract,
    summary: Arc<ServedSummary>,
) -> anyhow::Result<Gated> {
    let layer = &active.layer;
    let archive = build_archive(
        config,
        images,
        work,
        &columns,
        summary.geometry_srid,
        layer,
        partitions,
    )
    .await?;
    let (id_gate, collector, ids) = {
        let (archive, layer, summary, handoff) = (
            archive.clone(),
            layer.clone(),
            summary.clone(),
            config.served_handoff.clone(),
        );
        let mut collector = SampleCollector::new(samples, &config.unit_key);
        tokio::task::spawn_blocking(move || {
            let render_max_zoom = layer.render_max_zoom;
            let gate = gate_archive(
                &archive,
                &layer,
                &columns,
                &ids,
                |missing| classify_missing(&summary, &handoff, missing, render_max_zoom),
                &mut |tile, id| collector.observe(tile, id),
            )?;
            anyhow::Ok((gate, collector, ids))
        })
        .await
        .context("the archive gate stopped")??
    };
    if tile_equivalence::skipped(active.origin).is_some() {
        return Ok(Gated {
            archive,
            ids: id_gate,
            equivalence: None,
        });
    }
    let storage = TileDerivativeR2Config::from_env()?;
    // The active archive is read by range: its directory and the sampled tiles, never whole.
    let active_reader = R2ObjectStorage::from_config(storage.reader_config())
        .open_seekable_object(&active.pmtiles_object_key, EQUIVALENCE_READ_CHUNK_BYTES)
        .await
        .with_context(|| format!("the active archive {}", active.pmtiles_object_key))?;
    let (new_archive, layer, samples) = (archive.clone(), layer.clone(), samples.clone());
    let outcome = tokio::task::spawn_blocking(move || {
        tile_equivalence::first_release_gate(
            active_reader,
            File::open(&new_archive)?,
            &samples,
            (&layer.source_layer, &layer.feature_id_property),
            (layer.tile_min_zoom, layer.tile_max_zoom),
            &collector,
            &ids,
        )
    })
    .await
    .context("the equivalence gate stopped")??;
    // Beside the work directory, which is removed: the whole evidence outlives the bake.
    let evidence_file = config.work_root.join(format!(
        "{}-first-release-equivalence-{label}.json",
        config.unit_key
    ));
    std::fs::write(&evidence_file, serde_json::to_vec_pretty(&outcome)?)
        .with_context(|| format!("equivalence evidence {}", evidence_file.display()))?;
    Ok(Gated {
        archive,
        ids: id_gate,
        equivalence: Some((outcome, evidence_file)),
    })
}

/// ADR-0253: bakes and gates the snapshot like a bake, then stops — no build row, no upload, no
/// promotion, no fold — and prints what a person needs before the contract serves the edition.
async fn rehearse(
    config: &Config,
    images: &ImageSet,
    work: &Path,
    prepared: Prepared,
    active: &ActiveStatic,
    samples: &SampleContract,
    summary: Arc<ServedSummary>,
) -> anyhow::Result<()> {
    let label = format!("rehearsal-{}", Uuid::now_v7());
    let gated = bake_and_gate(
        config,
        images,
        &label,
        work,
        prepared,
        active,
        samples,
        summary.clone(),
    )
    .await?;
    let pmtiles_bytes = std::fs::metadata(&gated.archive)?.len();
    let equivalence = gated.equivalence.as_ref().map(|(outcome, file)| {
        serde_json::json!({
            "passed": outcome.passed(),
            "evidence": file.display().to_string(),
            "counts": outcome.refusal(),
        })
    });
    println!(
        "lakehouse-tile-bake-rehearsal unit={} gold_snapshot={} served_row_count={} pmtiles_bytes={pmtiles_bytes} maxzoom_feature_id_hashes_sha256={} maxzoom_subpixel_ids={} first_release_equivalence={}",
        config.unit_key,
        summary.canonical_iceberg_snapshot_id,
        summary.served_row_count,
        gated.ids.digest,
        serde_json::to_string(&gated.ids.subpixel)?,
        serde_json::to_string(&equivalence)?
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn bake_and_upload(
    config: &Config,
    images: &ImageSet,
    build_job_id: VectorTileBuildJobId,
    work: &Path,
    prepared: Prepared,
    active: &ActiveStatic,
    samples: &SampleContract,
    summary: Arc<ServedSummary>,
) -> anyhow::Result<(BuildEvidenceDigest, ValidatedPmtilesArtifact)> {
    let Gated {
        archive,
        ids: id_gate,
        equivalence,
    } = bake_and_gate(
        config,
        images,
        &build_job_id.to_string(),
        work,
        prepared,
        active,
        samples,
        summary.clone(),
    )
    .await?;
    let equivalence = match &equivalence {
        Some((outcome, file)) => tile_equivalence::verdict(outcome, &file.display().to_string())?,
        None => tile_equivalence::skipped(active.origin).unwrap_or_default(),
    };
    let storage = TileDerivativeR2Config::from_env()?;
    let release_id = static_release_id_for_build(build_job_id);
    let object_key = storage.release_key(&config.unit_key, &release_id.to_string())?;
    ensure!(
        object_key == static_release_pmtiles_object_key(&config.unit_key, release_id),
        "the storage prefix does not produce the release-addressed key"
    );
    let reader = R2ObjectStorage::from_config(storage.reader_config());
    let writer = R2ObjectStorage::from_config(storage.writer);
    let verified = create_only_upload_and_rehash(&writer, &reader, &archive, &object_key).await?;
    let source_id = static_release_martin_source_id(&config.unit_key, release_id);
    let evidence = serde_json::to_vec(&serde_json::json!({
        "schema_version": 2,
        "kind": "lakehouse_bake",
        "build_job_id": build_job_id.to_string(),
        "release_id": release_id.to_string(),
        "gold_snapshot": summary.canonical_iceberg_snapshot_id,
        "source_snapshot_id": summary.source_snapshot_id,
        "served_summary_schema": summary.schema_version,
        "edits_through_change_seq": summary.edits_through_change_seq,
        "served_row_count": summary.served_row_count,
        "maxzoom_feature_id_hashes_sha256": id_gate.digest,
        "maxzoom_subpixel_ids": id_gate.subpixel,
        "first_release_equivalence": equivalence,
        "pmtiles_sha256": verified.checksum_sha256,
        "pmtiles_bytes": verified.size_bytes,
    }))?;
    Ok((
        BuildEvidenceDigest::new(format!("{:x}", Sha256::digest(evidence)))
            .map_err(anyhow::Error::msg)?,
        ValidatedPmtilesArtifact {
            release_id,
            file_asset_id: static_file_asset_id_for_build(build_job_id),
            object_key,
            tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
                "{}/{source_id}/{{z}}/{{x}}/{{y}}",
                config.public_tiles_base_url
            ))
            .map_err(anyhow::Error::msg)?,
            checksum: PmtilesChecksum::new(verified.checksum_sha256).map_err(anyhow::Error::msg)?,
            size_bytes: verified.size_bytes,
        },
    ))
}

#[cfg(test)]
#[path = "lakehouse_tile_bake_tests.rs"]
mod tests;
