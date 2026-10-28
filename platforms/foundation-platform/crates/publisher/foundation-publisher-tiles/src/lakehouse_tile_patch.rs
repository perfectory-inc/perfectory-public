//! Root ADR-0323 (ADR-0111 base + patch): a bake of a unit the contract patches publishes only the
//! tiles its changed features touch, as one more patch generation over the active base archive.
//!
//! 1. The handoff is streamed once into a **feature index**: per feature its id hash, a hash of its
//!    tile content (id, properties, WKB) and its longitude/latitude box. The index of the snapshot
//!    the tiles serve was kept by the bake that served it (ADR-0141 §3 fingerprint, here per feature).
//! 2. The two indexes give the changed features (added, changed, removed) with the old and the new
//!    box of each. The **dirty tiles** are every tile of the layer's zooms either box touches,
//!    tippecanoe's buffer included: a tile an old shape touched must lose it.
//! 3. The **neighbourhood** — every served feature touching a dirty tile's minzoom ancestor — is
//!    baked with the same GDAL and tippecanoe runs as a full bake. Each dirty tile is taken from
//!    it whole: it is rendered from scratch from every feature that can appear in it (OSM Tilekiln,
//!    Mapbox MTS incremental), never merged feature by feature. A dirty tile it lacks is written
//!    empty, the tile tombstone (ADR-0111 §3).
//! 4. The patch is **cumulative**: the previous generation's tiles that are not dirty again are
//!    copied over byte for byte, so a request reads at most the patch and the base.
//! 5. Gate, create-only upload, then the manifest — the one mutable object — moves by
//!    compare-and-swap (ADR-0141 §4, Revision 3). The Worker serves patch, tombstone or base.
//!
//! A unit falls back to a full bake (a new base, ADR-0111 §9) when the served snapshot has no
//! index under the same bake inputs, or the patch would hold more than the contract's share of the
//! base's tiles.

use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::pmtiles_feature_ids::{
    self, tile_id, tile_zxy, Archive, COMPRESSION_GZIP, HEADER_BYTES,
};
use super::{
    for_each_handoff_line, gate::Missing, id_hash, subpixel, CsvOut, ServedIds, ServedLayer,
    ServedRow, ServedSummary,
};

const CONTRACT_JSON: &str = include_str!("../../../../config/r2-connections.contract.json");
const INDEX_MAGIC: &[u8; 8] = b"FPTIDX01";
const RECORD_BYTES: usize = 40;
/// tippecanoe's default buffer is 5 of a tile's 256 pixels (the bake does not set `-b`); one more
/// pixel covers the rounding between a box and the rendered geometry.
pub const BUFFER_PIXELS: f64 = 6.0;
/// The deepest Web Mercator latitude.
const MAX_LATITUDE: f64 = 85.051_128_779_806_59;

/// `config/r2-connections.contract.json` `vector_tile_gateway.patches`.
#[derive(Debug, Clone, Deserialize)]
pub struct PatchPolicy {
    pub units: Vec<String>,
    pub manifest_suffix: String,
    pub manifest_schema_version: String,
    pub patch_infix: String,
    pub max_patched_tile_ratio: f64,
}

impl PatchPolicy {
    pub fn load() -> anyhow::Result<Self> {
        Self::parse(CONTRACT_JSON)
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let contract: serde_json::Value = serde_json::from_str(text)?;
        let policy: Self = serde_json::from_value(
            contract
                .pointer("/vector_tile_gateway/patches")
                .cloned()
                .context("the R2 contract names no vector_tile_gateway.patches")?,
        )?;
        ensure!(
            policy.max_patched_tile_ratio > 0.0 && policy.max_patched_tile_ratio < 0.5,
            "max_patched_tile_ratio is a share of the base's tiles below one half"
        );
        ensure!(
            policy.manifest_suffix.starts_with('.') && policy.patch_infix.starts_with('.'),
            "the patch key parts start with a dot, which no source id holds"
        );
        Ok(policy)
    }

    pub fn covers(&self, unit: &str) -> bool {
        self.units.iter().any(|patched| patched == unit)
    }

    /// The manifest beside a base archive: `<root>/<unit>-<release>.patches.json`.
    pub fn manifest_key(&self, base_key: &str) -> anyhow::Result<String> {
        Ok(format!("{}{}", base_stem(base_key)?, self.manifest_suffix))
    }

    /// One patch generation's archive: `<root>/<unit>-<release>.patch-<n>.pmtiles`.
    pub fn patch_key(&self, base_key: &str, generation: u64) -> anyhow::Result<String> {
        ensure!(generation > 0, "patch generations start at 1");
        Ok(format!(
            "{}{}{generation}.pmtiles",
            base_stem(base_key)?,
            self.patch_infix
        ))
    }
}

fn base_stem(base_key: &str) -> anyhow::Result<&str> {
    base_key
        .strip_suffix(".pmtiles")
        .filter(|stem| !stem.contains('.'))
        .with_context(|| format!("{base_key} is not a base release archive key"))
}

/// The serving state of one base archive, the only object the patch path rewrites (ADR-0141 §4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema_version: String,
    pub base_source_id: String,
    /// Moves on every publish, a patch or a snapshot with no tile change.
    pub generation: u64,
    /// The Gold snapshot the base plus this patch serves.
    pub gold_snapshot: String,
    pub folded_through_change_seq: u64,
    pub patch: Option<PatchObject>,
}

/// The cumulative patch archive the manifest serves.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchObject {
    pub generation: u64,
    pub sha256: String,
    pub bytes: u64,
    pub tiles: u64,
    pub empty_tiles: u64,
}

impl Manifest {
    pub fn parse(bytes: &[u8], policy: &PatchPolicy, base_source_id: &str) -> anyhow::Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes).context("tile patch manifest")?;
        ensure!(
            manifest.schema_version == policy.manifest_schema_version,
            "tile patch manifest schema {}",
            manifest.schema_version
        );
        ensure!(
            manifest.base_source_id == base_source_id,
            "the tile patch manifest is for {}, the active base is {base_source_id}",
            manifest.base_source_id
        );
        ensure!(
            manifest
                .patch
                .as_ref()
                .is_none_or(|patch| patch.generation > 0 && patch.generation <= manifest.generation),
            "the manifest's patch generation is not one it published"
        );
        Ok(manifest)
    }
}

/// One feature of a served snapshot, as the change detection needs it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Record {
    pub id: u128,
    /// The first 8 bytes of SHA-256 over the feature's id, properties and WKB.
    pub content: u64,
    /// `[min_lon, min_lat, max_lon, max_lat]`, rounded outward; empty (min > max) for no geometry.
    pub bbox: [f32; 4],
}

pub const EMPTY_BBOX: [f32; 4] = [
    f32::INFINITY,
    f32::INFINITY,
    f32::NEG_INFINITY,
    f32::NEG_INFINITY,
];

fn content_hash(row: &ServedRow) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(row.feature_id.as_bytes());
    for (name, value) in &row.properties {
        hasher.update([0]);
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
    }
    hasher.update([1]);
    hasher.update(row.geometry_wkb_hex.to_ascii_lowercase().as_bytes());
    let digest = hasher.finalize();
    let mut first = [0_u8; 8];
    first.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(first)
}

/// The box of a Polygon or MultiPolygon, widened to the next `f32` outward so it always holds the
/// shape.
pub fn wkb_bbox(wkb: &[u8]) -> anyhow::Result<[f32; 4]> {
    let mut bbox = [
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    ];
    for polygon in subpixel::polygons(wkb)? {
        for (lon, lat) in polygon.into_iter().flatten() {
            ensure!(
                lon.is_finite() && lat.is_finite(),
                "WKB holds a non-finite coordinate"
            );
            bbox = [
                bbox[0].min(lon),
                bbox[1].min(lat),
                bbox[2].max(lon),
                bbox[3].max(lat),
            ];
        }
    }
    if bbox[0] > bbox[2] {
        return Ok(EMPTY_BBOX);
    }
    let down = |value: f64| {
        let narrow = value as f32;
        if f64::from(narrow) > value {
            narrow.next_down()
        } else {
            narrow
        }
    };
    let up = |value: f64| {
        let narrow = value as f32;
        if f64::from(narrow) < value {
            narrow.next_up()
        } else {
            narrow
        }
    };
    Ok([down(bbox[0]), down(bbox[1]), up(bbox[2]), up(bbox[3])])
}

fn hex(text: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(text.len().is_multiple_of(2), "odd-length WKB hex");
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).context("WKB hex"))
        .collect()
}

/// Streams the handoff into the feature index of its snapshot, sorted by id hash, and names the tile
/// properties (the id first, then the first row's properties, as `prepare_handoff` does). A
/// repeated id is refused here without naming it; `prepare_handoff` names it on a full bake.
pub fn build_index(
    summary: &ServedSummary,
    handoff: &Path,
) -> anyhow::Result<(Vec<Record>, Vec<String>)> {
    let mut records = Vec::with_capacity(summary.served_row_count);
    let mut columns = Vec::new();
    for_each_handoff_line(summary, handoff, |line, location| {
        let row: ServedRow = serde_json::from_str(line)
            .with_context(|| format!("{} is not a served row", location()))?;
        if columns.is_empty() {
            columns.push(summary.feature_id_property.clone());
            columns.extend(row.properties.keys().cloned());
        }
        let bbox = wkb_bbox(&hex(&row.geometry_wkb_hex)?)
            .with_context(|| format!("served row {} has no readable polygon", row.feature_id))?;
        records.push(Record {
            id: id_hash(&row.feature_id),
            content: content_hash(&row),
            bbox,
        });
        Ok(())
    })?;
    records.sort_unstable_by_key(|record| record.id);
    ensure!(
        records.windows(2).all(|pair| pair[0].id != pair[1].id),
        "the served snapshot holds a feature id twice"
    );
    Ok((records, columns))
}

/// Where a served snapshot's index lives: named by the snapshot and the bake inputs, so an index
/// is used only under the GDAL, tippecanoe and arguments that baked the tiles it describes.
pub fn index_path(
    work_root: &Path,
    unit: &str,
    snapshot: &str,
    bake_inputs: &str,
) -> anyhow::Result<PathBuf> {
    ensure!(
        !snapshot.is_empty()
            && snapshot
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b':')),
        "snapshot {snapshot:?} cannot name an index file"
    );
    let inputs = format!("{:x}", Sha256::digest(bake_inputs.as_bytes()));
    Ok(work_root
        .join(format!("{unit}-feature-index"))
        .join(format!("{snapshot}.{}.idx", &inputs[..16])))
}

pub fn write_index(path: &Path, records: &[Record]) -> anyhow::Result<()> {
    let mut out = BufWriter::with_capacity(1 << 20, File::create(path)?);
    out.write_all(INDEX_MAGIC)?;
    out.write_all(&(records.len() as u64).to_le_bytes())?;
    for record in records {
        out.write_all(&record.id.to_be_bytes())?;
        out.write_all(&record.content.to_be_bytes())?;
        for value in record.bbox {
            out.write_all(&value.to_le_bytes())?;
        }
    }
    out.into_inner()
        .map_err(|error| error.into_error())?
        .sync_all()?;
    Ok(())
}

pub fn read_index(path: &Path) -> anyhow::Result<Vec<Record>> {
    let mut input = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut head = [0_u8; 16];
    input.read_exact(&mut head)?;
    ensure!(
        &head[..8] == INDEX_MAGIC,
        "{} is not a feature index",
        path.display()
    );
    let count = usize::try_from(u64::from_le_bytes(head[8..].try_into()?))?;
    let expected = 16
        + count
            .checked_mul(RECORD_BYTES)
            .context("index size overflows")?;
    ensure!(
        usize::try_from(std::fs::metadata(path)?.len())? == expected,
        "{} is not {count} records long",
        path.display()
    );
    let mut records = Vec::with_capacity(count);
    let mut raw = [0_u8; RECORD_BYTES];
    for _ in 0..count {
        input.read_exact(&mut raw)?;
        let float =
            |at: usize| f32::from_le_bytes([raw[at], raw[at + 1], raw[at + 2], raw[at + 3]]);
        records.push(Record {
            id: u128::from_be_bytes(raw[..16].try_into()?),
            content: u64::from_be_bytes(raw[16..24].try_into()?),
            bbox: [float(24), float(28), float(32), float(36)],
        });
    }
    ensure!(
        records.windows(2).all(|pair| pair[0].id < pair[1].id),
        "{} is not sorted by id",
        path.display()
    );
    Ok(records)
}

/// Keeps a served snapshot's index: create-only, so an index once used is never rewritten.
pub fn keep_index(pending: &Path, path: &Path) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    match std::fs::hard_link(pending, path) {
        Ok(()) => Ok(()),
        // The same snapshot under the same inputs gives the same records.
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error).with_context(|| format!("feature index {}", path.display())),
    }
}

/// What changed between two snapshots' indexes.
#[derive(Debug, Default, PartialEq)]
pub struct Changes {
    /// Added or changed features: each must be in the patch unless sub-pixel.
    pub upserted: Vec<u128>,
    /// Removed features: none may be in the patch.
    pub deleted: Vec<u128>,
    /// The old and the new box of every changed feature.
    pub boxes: Vec<[f32; 4]>,
}

impl Changes {
    pub fn is_empty(&self) -> bool {
        self.upserted.is_empty() && self.deleted.is_empty()
    }
}

/// Merge-joins two sorted indexes.
pub fn diff(old: &[Record], new: &[Record]) -> Changes {
    let mut changes = Changes::default();
    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < new.len() {
        match (old.get(i), new.get(j)) {
            (Some(before), Some(after)) if before.id == after.id => {
                if before.content != after.content || before.bbox != after.bbox {
                    changes.upserted.push(after.id);
                    changes.boxes.extend([before.bbox, after.bbox]);
                }
                i += 1;
                j += 1;
            }
            (Some(before), after) if after.is_none_or(|after| before.id < after.id) => {
                changes.deleted.push(before.id);
                changes.boxes.push(before.bbox);
                i += 1;
            }
            (_, Some(after)) => {
                changes.upserted.push(after.id);
                changes.boxes.push(after.bbox);
                j += 1;
            }
            (_, None) => break,
        }
    }
    changes
}

/// Web Mercator pixel `(x, y)` of a longitude/latitude at `zoom`, 256 pixels per tile.
fn pixel(lon: f64, lat: f64, zoom: u8) -> (f64, f64) {
    let scale = 256.0 * f64::from(1_u32 << zoom);
    let phi = lat.clamp(-MAX_LATITUDE, MAX_LATITUDE).to_radians();
    (
        (lon + 180.0) / 360.0 * scale,
        (1.0 - (phi.tan() + 1.0 / phi.cos()).ln() / std::f64::consts::PI) / 2.0 * scale,
    )
}

/// The inclusive tile range `(x0, y0, x1, y1)` at `zoom` that a box touches with the buffer, or
/// `None` for an empty box.
pub fn tile_range(bbox: [f32; 4], zoom: u8) -> Option<(u32, u32, u32, u32)> {
    if bbox[0] > bbox[2] || bbox[1] > bbox[3] {
        return None;
    }
    let (left, top) = pixel(f64::from(bbox[0]), f64::from(bbox[3]), zoom);
    let (right, bottom) = pixel(f64::from(bbox[2]), f64::from(bbox[1]), zoom);
    let last = f64::from((1_u32 << zoom) - 1);
    let tile = |pixel: f64| (pixel / 256.0).floor().clamp(0.0, last) as u32;
    Some((
        tile(left - BUFFER_PIXELS),
        tile(top - BUFFER_PIXELS),
        tile(right + BUFFER_PIXELS),
        tile(bottom + BUFFER_PIXELS),
    ))
}

/// Every tile id of `zooms` the boxes touch, or `None` once there are more than `limit`.
pub fn dirty_tiles(boxes: &[[f32; 4]], zooms: (u8, u8), limit: usize) -> Option<BTreeSet<u64>> {
    let mut dirty = BTreeSet::new();
    for bbox in boxes {
        for zoom in zooms.0..=zooms.1 {
            let Some((x0, y0, x1, y1)) = tile_range(*bbox, zoom) else {
                continue;
            };
            let span = (u64::from(x1 - x0) + 1) * (u64::from(y1 - y0) + 1);
            if span > limit as u64 {
                return None;
            }
            for x in x0..=x1 {
                for y in y0..=y1 {
                    dirty.insert(tile_id(zoom, x, y));
                }
            }
            if dirty.len() > limit {
                return None;
            }
        }
    }
    Some(dirty)
}

/// The minzoom ancestors of the dirty tiles: a feature that can appear in a dirty tile touches one.
pub fn neighbourhood(dirty: &BTreeSet<u64>, min_zoom: u8) -> anyhow::Result<HashSet<(u32, u32)>> {
    dirty
        .iter()
        .map(|id| {
            let (zoom, x, y) = tile_zxy(*id)?;
            ensure!(
                zoom >= min_zoom,
                "dirty tile {id} is above the layer's minzoom"
            );
            let shift = zoom - min_zoom;
            Ok((x >> shift, y >> shift))
        })
        .collect()
}

/// Whether a box touches a tile of the neighbourhood at minzoom, with the buffer.
pub fn in_neighbourhood(bbox: [f32; 4], min_zoom: u8, tiles: &HashSet<(u32, u32)>) -> bool {
    tile_range(bbox, min_zoom).is_some_and(|(x0, y0, x1, y1)| {
        (x0..=x1).any(|x| (y0..=y1).any(|y| tiles.contains(&(x, y))))
    })
}

/// The GDAL input of the neighbourhood: the rows whose box touches it, looked up in the new index.
pub struct NeighbourhoodCsv<W> {
    pub inner: W,
    pub index: std::sync::Arc<Vec<Record>>,
    pub min_zoom: u8,
    pub tiles: HashSet<(u32, u32)>,
    pub kept: usize,
}

impl<W: CsvOut> CsvOut for NeighbourhoodCsv<W> {
    fn header(&mut self, line: &str) -> std::io::Result<()> {
        self.inner.header(line)
    }

    fn row(&mut self, feature_id: &str, line: &str) -> std::io::Result<()> {
        let id = id_hash(feature_id);
        let record = self
            .index
            .binary_search_by_key(&id, |record| record.id)
            .map(|at| self.index[at])
            .map_err(|_| {
                std::io::Error::other(format!("feature {feature_id} is not in the index"))
            })?;
        if in_neighbourhood(record.bbox, self.min_zoom, &self.tiles) {
            self.kept += 1;
            self.inner.row(feature_id, line)?;
        }
        Ok(())
    }
}

/// The empty MVT tile, gzip: the tile tombstone.
pub fn empty_tile() -> anyhow::Result<Vec<u8>> {
    Ok(flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default()).finish()?)
}

/// `{}`, gzip: the metadata of a patch with no baked neighbourhood (every dirty tile a tombstone).
pub fn empty_metadata() -> anyhow::Result<Vec<u8>> {
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(b"{}")?;
    Ok(gzip.finish()?)
}

/// The cumulative patch's tiles: every dirty tile from the neighbourhood (or the tombstone; with no
/// neighbourhood every dirty tile is one), then every tile of the previous generation that is not
/// dirty again. Returns the tiles in id order and how many are tombstones.
pub fn assemble<N: Read + Seek, P: Read + Seek>(
    mut neighbourhood: Option<&mut Archive<N>>,
    previous: Option<&mut Archive<P>>,
    dirty: &BTreeSet<u64>,
) -> anyhow::Result<(Vec<(u64, Vec<u8>)>, u64)> {
    if let Some(archive) = &neighbourhood {
        ensure!(
            archive.header().tile_compression == COMPRESSION_GZIP,
            "a patch stores gzip tiles; the neighbourhood bake wrote compression {}",
            archive.header().tile_compression
        );
    }
    let tombstone = empty_tile()?;
    let (mut tiles, mut empty) = (Vec::with_capacity(dirty.len()), 0_u64);
    for id in dirty {
        let entry = match neighbourhood.as_deref_mut() {
            Some(archive) => archive
                .find_entry(*id)?
                .map(|entry| archive.entry_raw(&entry))
                .transpose()?,
            None => None,
        };
        match entry {
            Some(bytes) => tiles.push((*id, bytes)),
            None => {
                tiles.push((*id, tombstone.clone()));
                empty += 1;
            }
        }
    }
    if let Some(previous) = previous {
        ensure!(
            previous.header().tile_compression == COMPRESSION_GZIP,
            "the previous patch is not gzip"
        );
        let mut kept = Vec::new();
        previous.for_each_entry((0, u64::MAX), &mut |archive, entry| {
            let bytes = archive.entry_raw(&entry)?;
            for id in entry.tile_id..entry.tile_id.saturating_add(entry.run_length) {
                if !dirty.contains(&id) {
                    if bytes == tombstone {
                        empty += 1;
                    }
                    kept.push((id, bytes.clone()));
                }
            }
            Ok(())
        })?;
        tiles.extend(kept);
    }
    tiles.sort_unstable_by_key(|(id, _)| *id);
    Ok((tiles, empty))
}

/// The header template and the stored metadata of a local archive.
pub fn template_and_metadata(path: &Path) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let bytes = std::fs::read(path)?;
    ensure!(
        bytes.len() >= HEADER_BYTES,
        "archive is shorter than a PMTiles header"
    );
    let field = |at: usize| -> anyhow::Result<usize> {
        Ok(usize::try_from(u64::from_le_bytes(
            bytes[at..at + 8].try_into()?,
        ))?)
    };
    let (offset, length) = (field(24)?, field(32)?);
    let metadata = bytes
        .get(offset..offset.checked_add(length).context("metadata overflows")?)
        .context("metadata runs past the archive")?
        .to_vec();
    Ok((bytes[..HEADER_BYTES].to_vec(), metadata))
}

/// What the patch gate proves.
#[derive(Debug, PartialEq)]
pub struct PatchGate {
    pub maxzoom_tiles: u64,
    pub subpixel: Vec<String>,
}

/// ADR-0111 §8, for a patch: the header matches the layer; every feature in its maxzoom tiles is
/// served; no removed feature is in it; every added or changed feature is, unless sub-pixel at
/// `render_max_zoom` (ADR-0262).
pub fn gate_patch(
    archive: &Path,
    layer: &ServedLayer,
    columns: &[String],
    served: &ServedIds,
    changes: &Changes,
    classify_missing: impl FnOnce(&[u128]) -> anyhow::Result<Missing>,
) -> anyhow::Result<PatchGate> {
    let mut header = vec![0_u8; HEADER_BYTES];
    File::open(archive)?.read_exact(&mut header)?;
    super::check_pmtiles_header(&header, layer)?;
    let mut present = HashSet::new();
    let mut unserved = Vec::new();
    let maxzoom_tiles = pmtiles_feature_ids::for_each_feature_id(
        archive,
        &pmtiles_feature_ids::Expect {
            zoom: layer.tile_max_zoom,
            layer: &layer.source_layer,
            id_property: &layer.feature_id_property,
            properties: columns,
        },
        |_, id| {
            if !served.contains(id) && unserved.len() < 5 {
                unserved.push(id.to_owned());
            }
            present.insert(id_hash(id));
            Ok(())
        },
    )?;
    ensure!(
        unserved.is_empty(),
        "the patch carries features the snapshot does not serve: {unserved:?}"
    );
    let removed = changes
        .deleted
        .iter()
        .filter(|id| present.contains(id))
        .count();
    ensure!(
        removed == 0,
        "the patch still carries {removed} removed feature(s)"
    );
    let missing: Vec<u128> = changes
        .upserted
        .iter()
        .filter(|id| !present.contains(id))
        .copied()
        .collect();
    let classified = if missing.is_empty() {
        Missing::default()
    } else {
        classify_missing(&missing)?
    };
    ensure!(
        classified.subpixel.len() + classified.unexplained.len() == missing.len(),
        "the handoff named {} of the {} changed ids the patch misses",
        classified.subpixel.len() + classified.unexplained.len(),
        missing.len()
    );
    if !classified.unexplained.is_empty() {
        bail!(
            "the patch's maxzoom tiles miss {} changed feature(s) that are not sub-pixel: {:?}",
            classified.unexplained.len(),
            classified.unexplained.iter().take(5).collect::<Vec<_>>()
        );
    }
    Ok(PatchGate {
        maxzoom_tiles,
        subpixel: classified.subpixel,
    })
}

#[cfg(test)]
#[path = "lakehouse_tile_patch_tests.rs"]
mod tests;
