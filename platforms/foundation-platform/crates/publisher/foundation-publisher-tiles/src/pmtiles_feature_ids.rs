//! The bake's PMTiles reads, all through the `pmtiles` crate (root ADR-0613 §4), and the feature
//! ids streamed out of one zoom of an archive of MVT tiles (root ADR-0133 §3).
//!
//! The lakehouse tile bake's maxzoom gate used to run `tippecanoe-decode`, which turns every tile
//! into GeoJSON — geometry included — and the bake then held that whole document in memory. At
//! the 40M parcels of a national bake that is tens of gigabytes. The walk here reads the asked
//! zoom's entries in tile-id order and decodes from each tile only the layer names, the property
//! tables and the features' tags. Geometry is left unread: the MVT `Feature.geometry` field is not
//! declared below, so the decoder skips it.
//!
//! The header, the directories, the tiles and the tile ids are the crate's: [`PmTilesReader`]
//! over a local file or over any seekable bytes ([`Seekable`]: an R2 object read by range), with a
//! directory cache so a tile looked up by id reads its leaf once; [`TileId`] and [`TileCoord`] for
//! the Hilbert numbering. What is here is the glue: the walk of one tile-id range, and the MVT
//! fields the gate reads (`prost`).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Mutex;

use anyhow::{bail, ensure, Context as _};
use bytes::Bytes;
use pmtiles::{
    BackendResponse, DirEntry, HashMapCache, PmTilesReader, PmtError, PmtResult, SyncBackend,
    TileCoord, TileId, TileType, MAX_ZOOM, PYRAMID_SIZE_BY_ZOOM,
};
use prost::Message as _;

/// An archive as the bake reads it: the crate's reader with a directory cache.
pub type Archive<B> = PmTilesReader<B, HashMapCache>;

/// Any seekable bytes as the crate's sync backend: one positioned read at a time.
pub struct Seekable<R>(Mutex<R>);

impl<R: Read + Seek + Send> SyncBackend for Seekable<R> {
    fn read(&self, offset: u64, length: usize) -> PmtResult<BackendResponse> {
        let mut reader = self
            .0
            .lock()
            .map_err(|_| PmtError::Reading(std::io::Error::other("the reader lock is poisoned")))?;
        reader.seek(SeekFrom::Start(offset))?;
        let mut bytes = Vec::with_capacity(length);
        (&mut *reader)
            .take(u64::try_from(length).unwrap_or(u64::MAX))
            .read_to_end(&mut bytes)?;
        Ok(BackendResponse::new(Bytes::from(bytes)))
    }
}

/// Opens a local archive of MVT tiles.
///
/// # Errors
/// The file cannot be opened, its header or root directory is not a PMTiles v3 one, or its tiles
/// are not MVT.
pub fn open(path: &Path) -> anyhow::Result<Archive<File>> {
    let file = File::open(path).with_context(|| format!("archive {}", path.display()))?;
    mvt(PmTilesReader::try_from_source_and_cache(
        file,
        HashMapCache::default(),
    )?)
    .with_context(|| format!("archive {}", path.display()))
}

/// Opens an archive of MVT tiles over any seekable bytes (an R2 object read by range).
///
/// # Errors
/// Its header or root directory cannot be read or is not a PMTiles v3 one, or its tiles are not
/// MVT.
pub fn open_seekable<R: Read + Seek + Send>(reader: R) -> anyhow::Result<Archive<Seekable<R>>> {
    mvt(PmTilesReader::try_from_source_and_cache(
        Seekable(Mutex::new(reader)),
        HashMapCache::default(),
    )?)
}

/// Every archive the bake reads holds MVT tiles.
fn mvt<B: SyncBackend + Send + Sync>(archive: Archive<B>) -> anyhow::Result<Archive<B>> {
    ensure!(
        archive.get_header().tile_type == TileType::Mvt,
        "archive tiles are not MVT"
    );
    Ok(archive)
}

/// The tile id of `z/x/y`.
///
/// # Errors
/// The coordinate is not one of its zoom.
pub fn tile_id(zoom: u8, x: u32, y: u32) -> anyhow::Result<u64> {
    Ok(TileId::from(TileCoord::new(zoom, x, y)?).value())
}

/// The `z/x/y` of a tile id.
///
/// # Errors
/// The id is beyond the deepest zoom.
pub fn tile_zxy(id: u64) -> anyhow::Result<(u8, u32, u32)> {
    let coord = TileCoord::from(TileId::new(id)?);
    Ok((coord.z(), coord.x(), coord.y()))
}

/// The tile-id range `[start, end)` of one zoom: every lower zoom comes first.
pub fn zoom_range(zoom: u8) -> (u64, u64) {
    let start = PYRAMID_SIZE_BY_ZOOM
        .get(usize::from(zoom))
        .copied()
        .unwrap_or(u64::MAX);
    let end = if zoom >= MAX_ZOOM {
        u64::MAX
    } else {
        PYRAMID_SIZE_BY_ZOOM
            .get(usize::from(zoom) + 1)
            .copied()
            .unwrap_or(u64::MAX)
    };
    (start, end)
}

/// Calls `on_entry` with every tile entry overlapping tile ids `[start, end)`, in tile-id order.
/// The crate walks the root and then each leaf in turn; the order is checked, not assumed, so the
/// walk can stop after the range.
///
/// # Errors
/// A directory cannot be read, the entries are out of tile-id order, or `on_entry` fails.
pub fn for_each_entry<B: SyncBackend + Send + Sync>(
    archive: &Archive<B>,
    (start, end): (u64, u64),
    mut on_entry: impl FnMut(DirEntry) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut covered = 0_u64;
    for entry in archive.entries() {
        let entry = entry.context("an archive directory could not be read")?;
        let first = entry.tile_id();
        ensure!(
            first >= covered,
            "archive entries are out of tile-id order at tile {first}"
        );
        if first >= end {
            break;
        }
        covered = first.saturating_add(u64::from(entry.run_length()));
        if covered > start {
            on_entry(entry)?;
        }
    }
    Ok(())
}

/// The stored bytes of the tile with this id, still in the archive's tile compression (root
/// ADR-0323: a patch copies tiles between archives without re-encoding them), or `None`.
///
/// # Errors
/// The id is not a tile id, or the archive cannot be read there.
pub fn stored<B: SyncBackend + Send + Sync>(
    archive: &Archive<B>,
    id: u64,
) -> anyhow::Result<Option<Bytes>> {
    Ok(archive.get_tile(TileId::new(id)?)?)
}

/// The decompressed bytes of the tile with this id, or `None`.
///
/// # Errors
/// The id is not a tile id, or the tile cannot be read or decompressed.
pub fn tile<B: SyncBackend + Send + Sync>(
    archive: &Archive<B>,
    id: u64,
) -> anyhow::Result<Option<Bytes>> {
    Ok(archive.get_tile_decompressed(TileId::new(id)?)?)
}

/// MVT, declaring only what the gate reads. Undeclared fields (geometry, extent) are skipped.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Tile {
    #[prost(message, repeated, tag = "3")]
    pub layers: Vec<Layer>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Layer {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, repeated, tag = "2")]
    pub features: Vec<Feature>,
    #[prost(string, repeated, tag = "3")]
    pub keys: Vec<String>,
    #[prost(message, repeated, tag = "4")]
    pub values: Vec<Value>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Feature {
    #[prost(uint32, repeated, tag = "2")]
    pub tags: Vec<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Value {
    #[prost(string, optional, tag = "1")]
    pub string_value: Option<String>,
}

/// What the walk expects of every tile it reads.
pub struct Expect<'a> {
    pub zoom: u8,
    pub layer: &'a str,
    pub id_property: &'a str,
    /// The property names a feature may carry; anything else is refused.
    pub properties: &'a [String],
}

/// Calls `on_id` with the tile id and the id property of every feature in every tile entry of
/// `expect.zoom`, in tile-id order. A feature appears once per tile it is in, so the same id can
/// come many times. Returns the number of tile entries read.
///
/// # Errors
/// The archive cannot be read, a tile is not MVT, or a tile breaks what `expect` says.
pub fn for_each_feature_id(
    archive: &Path,
    expect: &Expect<'_>,
    mut on_id: impl FnMut(u64, &str) -> anyhow::Result<()>,
) -> anyhow::Result<u64> {
    let archive = open(archive)?;
    let mut tiles = 0_u64;
    for_each_entry(&archive, zoom_range(expect.zoom), |entry| {
        let tile_id = entry.tile_id();
        let bytes = tile(&archive, tile_id)?.with_context(|| format!("tile {tile_id}"))?;
        let decoded = Tile::decode(bytes.as_ref())
            .with_context(|| format!("tile {tile_id} is not an MVT tile"))?;
        tiles += 1;
        for layer in &decoded.layers {
            ensure!(
                layer.name == expect.layer,
                "tile {tile_id} carries layer {:?}, the bake writes only {:?}",
                layer.name,
                expect.layer
            );
            if let Some(key) = layer
                .keys
                .iter()
                .find(|key| !expect.properties.contains(key))
            {
                bail!(
                    "tile {tile_id} carries property {key:?}, which the served snapshot does not"
                );
            }
            let id_key = layer.keys.iter().position(|key| key == expect.id_property);
            for feature in &layer.features {
                let id = feature_id(layer, feature, id_key).with_context(|| {
                    format!(
                        "a feature in tile {tile_id} has no text {}",
                        expect.id_property
                    )
                })?;
                on_id(tile_id, id)?;
            }
        }
        Ok(())
    })?;
    Ok(tiles)
}

fn feature_id<'a>(layer: &'a Layer, feature: &Feature, id_key: Option<usize>) -> Option<&'a str> {
    let id_key = u32::try_from(id_key?).ok()?;
    let value = feature
        .tags
        .as_chunks::<2>()
        .0
        .iter()
        .find(|pair| pair[0] == id_key)?
        .get(1)?;
    layer
        .values
        .get(usize::try_from(*value).ok()?)?
        .string_value
        .as_deref()
}

#[cfg(test)]
#[path = "pmtiles_feature_ids_tests.rs"]
pub mod tests;
