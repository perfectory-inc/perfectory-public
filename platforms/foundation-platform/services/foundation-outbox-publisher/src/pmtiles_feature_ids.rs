//! Streams the feature ids out of one zoom of a PMTiles v3 archive of MVT tiles (root ADR-0133 §3).
//!
//! The lakehouse tile bake's maxzoom gate used to run `tippecanoe-decode`, which turns every tile
//! into GeoJSON — geometry included — and the bake then held that whole document in memory. At
//! the 40M parcels of a national bake that is tens of gigabytes. This reader walks the archive's
//! directories in tile-id order, reads only the tiles of the asked zoom, and decodes from each
//! only the layer names, the property tables and the features' tags. Geometry is left unread: the
//! MVT `Feature.geometry` field is not declared below, so the decoder skips it. Memory is one
//! directory and one tile at a time.
//!
//! Candidates: the `pmtiles` crate reads tiles by coordinate through an async mmap or HTTP
//! backend and does not expose the entry walk this gate needs without pulling those backends in;
//! the directory format is a few varint arrays (spec: `protomaps/PMTiles` `spec/v3/spec.md`), so
//! it is read here. MVT is protobuf, so it is decoded with `prost` rather than by hand.

use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;

use anyhow::{bail, ensure, Context as _};
use flate2::read::GzDecoder;
use prost::Message as _;

pub(crate) const HEADER_BYTES: usize = 127;
/// Leaf directories may point at further leaves; real archives use one level.
const MAX_DIRECTORY_DEPTH: usize = 4;
/// A directory or tile larger than this is not something tippecanoe writes; refuse instead of
/// allocating whatever a corrupt length says.
const MAX_BLOCK_BYTES: u64 = 512 << 20;

/// The PMTiles v3 header fields the gate reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) root_directory: (u64, u64),
    pub(crate) leaf_directories_offset: u64,
    pub(crate) tile_data_offset: u64,
    pub(crate) internal_compression: u8,
    pub(crate) tile_compression: u8,
    pub(crate) tile_type: u8,
    pub(crate) min_zoom: u8,
    pub(crate) max_zoom: u8,
}

/// Compression codes of the PMTiles v3 header.
pub(crate) const COMPRESSION_NONE: u8 = 1;
pub(crate) const COMPRESSION_GZIP: u8 = 2;
/// Tile type code of MVT.
pub(crate) const TILE_TYPE_MVT: u8 = 1;

fn le_u64(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0_u8; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(word)
}

impl Header {
    pub(crate) fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            bytes.len() >= HEADER_BYTES,
            "archive is shorter than a PMTiles header"
        );
        ensure!(
            &bytes[..7] == b"PMTiles" && bytes[7] == 3,
            "archive is not PMTiles v3"
        );
        Ok(Self {
            root_directory: (le_u64(bytes, 8), le_u64(bytes, 16)),
            leaf_directories_offset: le_u64(bytes, 40),
            tile_data_offset: le_u64(bytes, 56),
            internal_compression: bytes[97],
            tile_compression: bytes[98],
            tile_type: bytes[99],
            min_zoom: bytes[100],
            max_zoom: bytes[101],
        })
    }
}

/// One directory entry. `run_length == 0` points at a leaf directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    pub(crate) tile_id: u64,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) run_length: u64,
}

fn read_varint(bytes: &[u8], at: &mut usize) -> anyhow::Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at).context("directory ends inside a varint")?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("directory varint is longer than 64 bits")
}

/// Decodes an uncompressed directory: count, then tile-id deltas, run lengths, lengths, offsets.
pub(crate) fn decode_directory(bytes: &[u8]) -> anyhow::Result<Vec<Entry>> {
    let mut at = 0;
    let count = usize::try_from(read_varint(bytes, &mut at)?)?;
    // Every entry takes at least four bytes, so a count the bytes cannot hold is corruption.
    ensure!(
        count <= bytes.len(),
        "directory claims {count} entries in {} bytes",
        bytes.len()
    );
    let mut entries = vec![
        Entry {
            tile_id: 0,
            offset: 0,
            length: 0,
            run_length: 0,
        };
        count
    ];
    let mut tile_id = 0_u64;
    for entry in &mut entries {
        tile_id = tile_id
            .checked_add(read_varint(bytes, &mut at)?)
            .context("directory tile id overflows")?;
        entry.tile_id = tile_id;
    }
    for entry in &mut entries {
        entry.run_length = read_varint(bytes, &mut at)?;
    }
    for entry in &mut entries {
        entry.length = read_varint(bytes, &mut at)?;
    }
    for index in 0..count {
        let raw = read_varint(bytes, &mut at)?;
        entries[index].offset = if raw == 0 && index > 0 {
            entries[index - 1]
                .offset
                .checked_add(entries[index - 1].length)
                .context("directory offset overflows")?
        } else {
            raw.checked_sub(1).context("directory offset is zero")?
        };
    }
    ensure!(at == bytes.len(), "directory has trailing bytes");
    Ok(entries)
}

/// The first tile id of zoom `z`: the tiles of every lower zoom come before it.
pub(crate) fn first_tile_id(zoom: u8) -> u64 {
    (0..u32::from(zoom)).map(|z| 1_u64 << (2 * z)).sum()
}

fn decompress(bytes: Vec<u8>, compression: u8, what: &str) -> anyhow::Result<Vec<u8>> {
    match compression {
        COMPRESSION_NONE => Ok(bytes),
        COMPRESSION_GZIP => {
            let mut out = Vec::new();
            GzDecoder::new(bytes.as_slice())
                .read_to_end(&mut out)
                .with_context(|| format!("{what} is not valid gzip"))?;
            Ok(out)
        }
        other => bail!("{what} compression {other} is not one this reader decodes"),
    }
}

/// MVT, declaring only what the gate reads. Undeclared fields (geometry, extent) are skipped.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Tile {
    #[prost(message, repeated, tag = "3")]
    pub(crate) layers: Vec<Layer>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Layer {
    #[prost(string, tag = "1")]
    pub(crate) name: String,
    #[prost(message, repeated, tag = "2")]
    pub(crate) features: Vec<Feature>,
    #[prost(string, repeated, tag = "3")]
    pub(crate) keys: Vec<String>,
    #[prost(message, repeated, tag = "4")]
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Feature {
    #[prost(uint32, repeated, tag = "2")]
    pub(crate) tags: Vec<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Value {
    #[prost(string, optional, tag = "1")]
    pub(crate) string_value: Option<String>,
}

/// What the walk expects of every tile it reads.
pub(crate) struct Expect<'a> {
    pub(crate) zoom: u8,
    pub(crate) layer: &'a str,
    pub(crate) id_property: &'a str,
    /// The property names a feature may carry; anything else is refused.
    pub(crate) properties: &'a [String],
}

/// Calls `on_id` with the id property of every feature in every tile of `expect.zoom`, in
/// tile-id order. A feature appears once per tile it is in, so the same id can come many times.
/// Returns the number of tiles read.
pub(crate) fn for_each_feature_id(
    archive: &Path,
    expect: &Expect<'_>,
    mut on_id: impl FnMut(&str) -> anyhow::Result<()>,
) -> anyhow::Result<u64> {
    let mut file = File::open(archive).with_context(|| format!("archive {}", archive.display()))?;
    let mut bytes = vec![0_u8; HEADER_BYTES];
    file.read_exact(&mut bytes)
        .context("archive is shorter than a PMTiles header")?;
    let header = Header::parse(&bytes)?;
    ensure!(
        header.tile_type == TILE_TYPE_MVT,
        "archive tiles are not MVT"
    );
    let mut walk = Walk {
        file,
        header,
        expect,
        range: (first_tile_id(expect.zoom), first_tile_id(expect.zoom + 1)),
        tiles: 0,
    };
    let root = walk.header.root_directory;
    walk.directory(root.0, root.1, 0, u64::MAX, &mut on_id)?;
    Ok(walk.tiles)
}

struct Walk<'a> {
    file: File,
    header: Header,
    expect: &'a Expect<'a>,
    /// Tile ids `[start, end)` of the asked zoom.
    range: (u64, u64),
    tiles: u64,
}

impl Walk<'_> {
    fn read_block(&mut self, offset: u64, length: u64, what: &str) -> anyhow::Result<Vec<u8>> {
        ensure!(
            length <= MAX_BLOCK_BYTES,
            "{what} claims {length} bytes, more than any tile or directory this bake writes"
        );
        self.file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0_u8; usize::try_from(length)?];
        self.file
            .read_exact(&mut bytes)
            .with_context(|| format!("{what} runs past the end of the archive"))?;
        Ok(bytes)
    }

    /// Visits one directory whose entries' tile ids lie below `limit` (the next sibling's start).
    fn directory(
        &mut self,
        offset: u64,
        length: u64,
        depth: usize,
        limit: u64,
        on_id: &mut impl FnMut(&str) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        ensure!(
            depth < MAX_DIRECTORY_DEPTH,
            "archive directories nest deeper than {MAX_DIRECTORY_DEPTH}"
        );
        let raw = self.read_block(offset, length, "a directory")?;
        let entries = decode_directory(&decompress(
            raw,
            self.header.internal_compression,
            "a directory",
        )?)?;
        for (index, entry) in entries.iter().enumerate() {
            let next = entries.get(index + 1).map_or(limit, |next| next.tile_id);
            if entry.run_length == 0 {
                // A leaf holds tile ids [entry.tile_id, next); skip it when that misses the zoom.
                if next <= self.range.0 || entry.tile_id >= self.range.1 {
                    continue;
                }
                let leaf = self
                    .header
                    .leaf_directories_offset
                    .checked_add(entry.offset)
                    .context("leaf directory offset overflows")?;
                self.directory(leaf, entry.length, depth + 1, next, on_id)?;
            } else {
                let end = entry.tile_id.saturating_add(entry.run_length);
                if end <= self.range.0 || entry.tile_id >= self.range.1 {
                    continue;
                }
                let at = self
                    .header
                    .tile_data_offset
                    .checked_add(entry.offset)
                    .context("tile offset overflows")?;
                self.tile(at, entry.length, entry.tile_id, on_id)?;
            }
        }
        Ok(())
    }

    fn tile(
        &mut self,
        offset: u64,
        length: u64,
        tile_id: u64,
        on_id: &mut impl FnMut(&str) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let raw = self.read_block(offset, length, "a tile")?;
        let bytes = decompress(raw, self.header.tile_compression, "a tile")?;
        let tile = Tile::decode(bytes.as_slice())
            .with_context(|| format!("tile {tile_id} is not an MVT tile"))?;
        self.tiles += 1;
        for layer in &tile.layers {
            ensure!(
                layer.name == self.expect.layer,
                "tile {tile_id} carries layer {:?}, the bake writes only {:?}",
                layer.name,
                self.expect.layer
            );
            if let Some(key) = layer
                .keys
                .iter()
                .find(|key| !self.expect.properties.contains(key))
            {
                bail!(
                    "tile {tile_id} carries property {key:?}, which the served snapshot does not"
                );
            }
            let id_key = layer
                .keys
                .iter()
                .position(|key| key == self.expect.id_property);
            for feature in &layer.features {
                on_id(feature_id(layer, feature, id_key).with_context(|| {
                    format!(
                        "a feature in tile {tile_id} has no text {}",
                        self.expect.id_property
                    )
                })?)?;
            }
        }
        Ok(())
    }
}

fn feature_id<'a>(layer: &'a Layer, feature: &Feature, id_key: Option<usize>) -> Option<&'a str> {
    let id_key = u32::try_from(id_key?).ok()?;
    let value = feature
        .tags
        .chunks_exact(2)
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
pub(crate) mod tests;
