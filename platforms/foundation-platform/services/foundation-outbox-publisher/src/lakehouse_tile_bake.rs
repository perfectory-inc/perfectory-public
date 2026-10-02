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
//!    files under the work directory;
//! 5. gate: the archive is PMTiles v3 MVT over the layer's zoom range, and its maxzoom tiles carry
//!    exactly the served feature ids, read by streaming the archive without decoding geometry;
//! 6. upload create-only and rehash, record the result, promote (the unit is left with no fallback);
//! 7. record the fold in the edit store, which retires the folded edits from the customer overlay.
//!
//! No PostGIS is read or written. A failure before the promotion records the build as failed and
//! changes nothing that is served.

use std::ffi::OsString;
use std::fs::File;
use std::io::{BufRead as _, BufReader, BufWriter, Read, Write};
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
pub(crate) mod pmtiles_feature_ids;

const PREFIX: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE";
const CONTRACT_JSON: &str = include_str!("../../../config/tile-bake-containers.contract.json");
const SERVED_SUMMARY_V1: &str = "foundation-platform.polygon_served_gold.v1";
const SERVED_SUMMARY_V2: &str = "foundation-platform.polygon_served_gold.v2";
/// The Silver CRSs a served snapshot may arrive in; GDAL reprojects either to EPSG:4326.
const SERVED_SRIDS: [i32; 2] = [4326, 5186];
const MAP_EDIT_GATEWAY_BASE_URL_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL";
const MAP_EDIT_WRITE_TOKEN_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN";
/// How many ids a gate failure names; the counts are always complete.
const NAMED_IDS: usize = 5;

/// `config/tile-bake-containers.contract.json`: the pinned images, their memory caps, and how much
/// free work disk a bake needs.
#[derive(Debug, Deserialize)]
pub(crate) struct BakeContract {
    images: ImageSet,
    pub(crate) work_disk: WorkDisk,
}

#[derive(Debug, Deserialize)]
struct ImageSet {
    gdal: Image,
    tippecanoe: Image,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Image {
    image: String,
    #[serde(default)]
    version_banner: Option<String>,
    /// Passed as both `--memory` and `--memory-swap`, so the container cannot swap past it either.
    pub(crate) memory_limit: String,
}

/// Free space the work root must have before a bake starts: the larger of a floor and a multiple
/// of the handoff's bytes (the GDAL CSV, the GeoJSON sequence, tippecanoe's temporary files and the
/// archive all live there at once).
#[derive(Debug, Deserialize)]
pub(crate) struct WorkDisk {
    pub(crate) min_free_bytes: u64,
    pub(crate) min_free_bytes_per_handoff_byte: u64,
}

impl BakeContract {
    pub(crate) fn parse(text: &str) -> anyhow::Result<Self> {
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
        Ok(contract)
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
pub(crate) struct HandoffPart {
    pub(crate) path: String,
    pub(crate) rows: usize,
    pub(crate) sha256: String,
}

/// What the served-Gold job recorded about the state it wrote.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ServedSummary {
    schema_version: String,
    pub(crate) unit: String,
    pub(crate) feature_id_property: String,
    pub(crate) geometry_srid: i32,
    pub(crate) canonical_iceberg_snapshot_id: String,
    pub(crate) edits_through_change_seq: u64,
    pub(crate) served_row_count: usize,
    status: String,
    #[serde(default)]
    pub(crate) source_snapshot_id: Option<String>,
    #[serde(default)]
    pub(crate) handoff_parts: Option<Vec<HandoffPart>>,
}

impl ServedSummary {
    pub(crate) fn validate(&self, unit: &str) -> anyhow::Result<()> {
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
pub(crate) struct ServedRow {
    pub(crate) feature_id: String,
    pub(crate) properties: std::collections::BTreeMap<String, String>,
    pub(crate) geometry_wkb_hex: String,
    pub(crate) geometry_srid: i32,
}

/// The fixed-size stand-in for a feature id: the first 16 bytes of its SHA-256.
pub(crate) fn id_hash(id: &str) -> u128 {
    let digest = Sha256::digest(id.as_bytes());
    let mut first = [0_u8; 16];
    first.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(first)
}

/// Every served feature id, as sorted distinct hashes: 16 bytes per feature.
#[derive(Debug)]
pub(crate) struct ServedIds {
    sorted: Vec<u128>,
}

impl ServedIds {
    /// Sorts the hashes; returns the first repeated one if a feature id was served twice.
    pub(crate) fn from_hashes(mut hashes: Vec<u128>) -> Result<Self, u128> {
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

    /// Evidence: the SHA-256 of the sorted id hashes.
    pub(crate) fn digest(&self) -> String {
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
pub(crate) fn handoff_bytes(summary: &ServedSummary, handoff: &Path) -> anyhow::Result<u64> {
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
pub(crate) struct PreparedHandoff<W> {
    pub(crate) csv: W,
    /// The tile properties: the feature id first, then every row property.
    pub(crate) columns: Vec<String>,
    pub(crate) ids: ServedIds,
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Streams the handoff once into `csv` — one CSV row per feature: id, properties, hex WKB —
/// refusing repeats, a CRS or a property set that differs between rows, a part whose bytes or
/// count differ from the summary, and a total the summary did not promise.
pub(crate) fn prepare_handoff<W: Write>(
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
            writeln!(csv, "{},geometry", header.join(","))?;
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
        writeln!(csv, "{}", fields.join(","))?;
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

/// The layer the active release serves, which the new archive must reproduce.
#[derive(Debug, Clone)]
pub(crate) struct ServedLayer {
    pub(crate) source_layer: String,
    pub(crate) feature_id_property: String,
    pub(crate) tile_min_zoom: u8,
    pub(crate) tile_max_zoom: u8,
    pub(crate) properties: Vec<String>,
}

/// Checks a PMTiles v3 header: MVT tiles over exactly the layer's zoom range, in compressions the
/// gate can read.
pub(crate) fn check_pmtiles_header(header: &[u8], layer: &ServedLayer) -> anyhow::Result<()> {
    use pmtiles_feature_ids::{Header, COMPRESSION_GZIP, COMPRESSION_NONE, TILE_TYPE_MVT};
    let header = Header::parse(header)?;
    ensure!(
        header.tile_type == TILE_TYPE_MVT,
        "archive tiles are not MVT"
    );
    ensure!(
        (header.min_zoom, header.max_zoom) == (layer.tile_min_zoom, layer.tile_max_zoom),
        "archive zooms {}..{} differ from the layer's {}..{}",
        header.min_zoom,
        header.max_zoom,
        layer.tile_min_zoom,
        layer.tile_max_zoom
    );
    for (what, code) in [
        ("directory", header.internal_compression),
        ("tile", header.tile_compression),
    ] {
        ensure!(
            [COMPRESSION_NONE, COMPRESSION_GZIP].contains(&code),
            "archive {what} compression {code} is not one the gate reads"
        );
    }
    Ok(())
}

/// The gate: the archive's header matches the layer, and its maxzoom tiles carry exactly the
/// served ids — none missing, none extra. A feature appears in every maxzoom tile it touches, so
/// presence is a bit per served id. Returns the evidence digest of the id set.
pub(crate) fn gate_archive(
    archive: &Path,
    layer: &ServedLayer,
    columns: &[String],
    ids: &ServedIds,
    name_missing: impl FnOnce(&[u128]) -> anyhow::Result<Vec<String>>,
) -> anyhow::Result<String> {
    let mut header = vec![0_u8; pmtiles_feature_ids::HEADER_BYTES];
    File::open(archive)
        .and_then(|mut file| file.read_exact(&mut header))
        .context("archive is shorter than a PMTiles header")?;
    check_pmtiles_header(&header, layer)?;
    let mut found = vec![0_u64; ids.len().div_ceil(64)];
    let (mut extra, mut extra_named) = (0_u64, Vec::new());
    pmtiles_feature_ids::for_each_feature_id(
        archive,
        &pmtiles_feature_ids::Expect {
            zoom: layer.tile_max_zoom,
            layer: &layer.source_layer,
            id_property: &layer.feature_id_property,
            properties: columns,
        },
        |id| {
            if let Some(index) = ids.position(id_hash(id)) {
                found[index / 64] |= 1_u64 << (index % 64);
            } else {
                extra += 1;
                if extra_named.len() < NAMED_IDS && !extra_named.iter().any(|named| named == id) {
                    extra_named.push(id.to_owned());
                }
            }
            Ok(())
        },
    )?;
    let present = |index: usize| found[index / 64] & (1_u64 << (index % 64)) != 0;
    let missing = (0..ids.len()).filter(|index| !present(*index)).count();
    if missing > 0 || extra > 0 {
        let first_missing: Vec<u128> = ids
            .sorted
            .iter()
            .enumerate()
            .filter(|(index, _)| !present(*index))
            .take(NAMED_IDS)
            .map(|(_, hash)| *hash)
            .collect();
        let named = if first_missing.is_empty() {
            Vec::new()
        } else {
            name_missing(&first_missing)?
        };
        bail!(
            "maxzoom tiles miss {missing} of the {} served ids and carry {extra} feature(s) the snapshot does not serve; missing {named:?}, extra {extra_named:?}",
            ids.len()
        );
    }
    Ok(ids.digest())
}

/// Refuses a work disk with less free space than the contract asks for this handoff.
pub(crate) fn ensure_work_disk(
    free: u64,
    handoff_bytes: u64,
    disk: &WorkDisk,
) -> anyhow::Result<()> {
    let needed = handoff_bytes
        .saturating_mul(disk.min_free_bytes_per_handoff_byte)
        .max(disk.min_free_bytes);
    ensure!(
        free >= needed,
        "the work disk has {free} bytes free; a {handoff_bytes}-byte handoff needs {needed} (config/tile-bake-containers.contract.json work_disk)"
    );
    Ok(())
}

/// The bytes an unprivileged writer may still use on the filesystem holding `path`.
#[cfg(unix)]
pub(crate) fn free_bytes(path: &Path) -> anyhow::Result<u64> {
    let stat =
        rustix::fs::statvfs(path).with_context(|| format!("free space of {}", path.display()))?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(not(unix))]
pub(crate) fn free_bytes(path: &Path) -> anyhow::Result<u64> {
    bail!(
        "free space of {} can only be measured on the Linux bake host",
        path.display()
    )
}

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
        })
    }
}

struct ActiveStatic {
    release_id: VectorTileReleaseId,
    serving_generation: ServingGeneration,
    snapshot: String,
    layer: ServedLayer,
}

async fn read_active_static(pool: &PgPool, unit_key: &str) -> anyhow::Result<ActiveStatic> {
    let row = sqlx::query(
        "SELECT unit.active_release_id, unit.serving_generation, release.source_kind,
                release.canonical_iceberg_snapshot_id, layer.source_layer, layer.feature_id_property,
                layer.tile_min_zoom, layer.tile_max_zoom, layer.feature_filter_properties
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

/// `docker run` for one pinned tool: no network, the contract's memory cap with no swap beyond
/// it, the work directory at `/w`.
pub(crate) fn run_args(work: &Path, image: &Image) -> Vec<OsString> {
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
pub(crate) fn tippecanoe_args(layer: &ServedLayer, columns: &[String]) -> Vec<OsString> {
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
    let source_srs = format!("EPSG:{srid}");
    let mut gdal = run_args(work, &images.gdal);
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
        run_args(work, &images.tippecanoe)
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

    let mut tippecanoe = run_args(work, &images.tippecanoe);
    tippecanoe.extend(tippecanoe_args(layer, columns));
    docker(tippecanoe, config.tool_timeout, "tippecanoe").await?;
    // The GeoJSON sequence is the largest file in the work directory; the gate reads the archive.
    std::fs::remove_file(work.join("served.geojsons"))?;
    Ok(work.join("unit.pmtiles"))
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
    let config = Config::from_env()?;
    let contract = BakeContract::parse(CONTRACT_JSON)?;
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
    let result = bake_in(&config, &contract, summary, &work).await;
    // The archive is in R2 (or the attempt failed); nothing in the work directory is used again.
    if let Err(error) = std::fs::remove_dir_all(&work) {
        tracing::warn!(path = %work.display(), error = %error, "failed to remove the bake work directory");
    }
    result
}

async fn bake_in(
    config: &Config,
    contract: &BakeContract,
    summary: ServedSummary,
    work: &Path,
) -> anyhow::Result<()> {
    let summary = Arc::new(summary);
    let prepared = {
        let (summary, handoff, csv) = (
            summary.clone(),
            config.served_handoff.clone(),
            work.join("served.csv"),
        );
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let out = BufWriter::with_capacity(1 << 20, File::create(&csv)?);
            let prepared = prepare_handoff(&summary, &handoff, out)?;
            prepared
                .csv
                .into_inner()
                .map_err(|error| error.into_error())?
                .sync_all()?;
            Ok((prepared.columns, prepared.ids))
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
        })
        .await?;

    let built = bake_and_upload(
        config,
        &contract.images,
        build_job_id,
        work,
        prepared,
        &active.layer,
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

async fn bake_and_upload(
    config: &Config,
    images: &ImageSet,
    build_job_id: VectorTileBuildJobId,
    work: &Path,
    (columns, ids): (Vec<String>, ServedIds),
    layer: &ServedLayer,
    summary: Arc<ServedSummary>,
) -> anyhow::Result<(BuildEvidenceDigest, ValidatedPmtilesArtifact)> {
    let archive =
        build_archive(config, images, work, &columns, summary.geometry_srid, layer).await?;
    let ids_digest = {
        let (archive, layer, summary, handoff) = (
            archive.clone(),
            layer.clone(),
            summary.clone(),
            config.served_handoff.clone(),
        );
        tokio::task::spawn_blocking(move || {
            gate_archive(&archive, &layer, &columns, &ids, |missing| {
                ids_with_hashes(&summary, &handoff, missing)
            })
        })
        .await
        .context("the archive gate stopped")??
    };
    let release_id = static_release_id_for_build(build_job_id);
    let storage = TileDerivativeR2Config::from_env()?;
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
        "maxzoom_feature_id_hashes_sha256": ids_digest,
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
