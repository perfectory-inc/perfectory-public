//! Merges the partition archives of one unit into the unit's archive (root ADR-0613).
//!
//! A partitioned unit is baked one `PMTiles` archive per feature-id prefix (root ADR-0316) and the
//! archives are made one. `tile-join` did that by decoding every tile of every archive, writing
//! the result to sqlite and converting it (root ADR-0576: 16 parcel archives, 8.7 GB, never joined
//! inside a run on the data disk). This merge walks the archives' directories together in tile-id
//! order and writes the output once, front to back:
//!
//! - a tile only one archive holds is copied as the bytes that archive stores, never decoded;
//! - a tile several archives hold (a partition border, or every tile when partitions overlap) is
//!   decoded as MVT, the features of same-named layers are put in one layer with their keys and
//!   values re-indexed, and it is encoded and compressed once.
//!
//! Reading and writing the archive format is the `pmtiles` crate's (stadiamaps/pmtiles-rs): its
//! reader lists the directory entries and its stream writer builds the directories and the
//! header. MVT is protobuf, decoded with `prost`; a feature's geometry stays the bytes it was.
//! Memory is the directory entries (the archives' and the output's) and one tile of each archive
//! at a time.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, ensure, Context as _};
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use pmtiles::{
    Compression, DirEntry, PmTilesReader, PmTilesWriter, PmtResult, TileCoord, TileId, TileType,
};
use prost::Message as _;
use serde_json::Value;

/// How many tiles are written between two looks at the clock.
const DEADLINE_CHECK_TILES: u64 = 4096;
const READ_BUFFER_BYTES: usize = 1 << 20;
/// A tile larger than this, stored or decompressed, is not something tippecanoe writes; refuse
/// instead of allocating whatever a corrupt archive says.
const MAX_TILE_BYTES: u64 = 512 << 20;
/// The extent a layer has when it names none (`vector_tile.proto`: `[default = 4096]`).
const DEFAULT_EXTENT: u32 = 4096;

/// A whole MVT tile (`vector_tile.proto` 2.1). Every field the spec declares is declared here, so
/// decoding and encoding a tile loses nothing the spec names.
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
    /// Each `Value` message as its encoded bytes: a value is only moved and compared, and its
    /// bytes say exactly which value and type it is.
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub values: Vec<Vec<u8>>,
    #[prost(uint32, optional, tag = "5")]
    pub extent: Option<u32>,
    #[prost(uint32, optional, tag = "15")]
    pub version: Option<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Feature {
    #[prost(uint64, optional, tag = "1")]
    pub id: Option<u64>,
    #[prost(uint32, repeated, tag = "2")]
    pub tags: Vec<u32>,
    /// `GeomType`, as its number.
    #[prost(int32, optional, tag = "3")]
    pub kind: Option<i32>,
    /// The packed command stream, as bytes: the merge never reads a geometry.
    #[prost(bytes = "vec", tag = "4")]
    pub geometry: Vec<u8>,
}

/// One layer of the merged tile and where its keys and values already are.
struct MergedLayer {
    layer: Layer,
    keys: HashMap<String, u32>,
    values: HashMap<Vec<u8>, u32>,
}

impl MergedLayer {
    fn first(layer: Layer) -> anyhow::Result<Self> {
        let mut keys = HashMap::with_capacity(layer.keys.len());
        for (index, key) in layer.keys.iter().enumerate() {
            keys.entry(key.clone()).or_insert(u32::try_from(index)?);
        }
        let mut values = HashMap::with_capacity(layer.values.len());
        for (index, value) in layer.values.iter().enumerate() {
            values.entry(value.clone()).or_insert(u32::try_from(index)?);
        }
        for feature in &layer.features {
            ensure!(
                feature.tags.len().is_multiple_of(2),
                "a feature of layer {} has an odd number of tags",
                layer.name
            );
            for pair in feature.tags.as_chunks::<2>().0 {
                ensure!(
                    usize::try_from(pair[0])? < layer.keys.len()
                        && usize::try_from(pair[1])? < layer.values.len(),
                    "a tag of layer {} names no key or no value",
                    layer.name
                );
            }
        }
        Ok(Self {
            layer,
            keys,
            values,
        })
    }

    /// Appends another archive's layer of the same name: its features as they are, their tags
    /// pointing at this layer's keys and values.
    fn append(&mut self, other: Layer) -> anyhow::Result<()> {
        ensure!(
            other.extent.unwrap_or(DEFAULT_EXTENT) == self.layer.extent.unwrap_or(DEFAULT_EXTENT)
                && other.version == self.layer.version,
            "layer {} has extent {:?} version {:?} in one archive and extent {:?} version {:?} in \
             another: the partitions were not baked alike",
            other.name,
            self.layer.extent,
            self.layer.version,
            other.extent,
            other.version
        );
        let mut key_at = Vec::with_capacity(other.keys.len());
        for key in other.keys {
            let next = u32::try_from(self.layer.keys.len())?;
            let at = match self.keys.get(&key) {
                Some(at) => *at,
                None => {
                    self.keys.insert(key.clone(), next);
                    self.layer.keys.push(key);
                    next
                }
            };
            key_at.push(at);
        }
        let mut value_at = Vec::with_capacity(other.values.len());
        for value in other.values {
            let next = u32::try_from(self.layer.values.len())?;
            let at = match self.values.get(&value) {
                Some(at) => *at,
                None => {
                    self.values.insert(value.clone(), next);
                    self.layer.values.push(value);
                    next
                }
            };
            value_at.push(at);
        }
        for mut feature in other.features {
            ensure!(
                feature.tags.len().is_multiple_of(2),
                "a feature of layer {} has an odd number of tags",
                self.layer.name
            );
            for pair in feature.tags.as_chunks_mut::<2>().0 {
                pair[0] = *key_at
                    .get(usize::try_from(pair[0])?)
                    .with_context(|| format!("a tag of layer {} names no key", self.layer.name))?;
                pair[1] = *value_at.get(usize::try_from(pair[1])?).with_context(|| {
                    format!("a tag of layer {} names no value", self.layer.name)
                })?;
            }
            self.layer.features.push(feature);
        }
        Ok(())
    }
}

/// The tile that holds every feature of `parts` (decompressed MVT tiles of one tile id, in the
/// archives' order): layers in the order they first appear, a layer's features in the archives'
/// order.
///
/// # Errors
/// Returns an error when a part is not MVT, or when two archives disagree on a layer's extent or
/// version.
pub fn merge_tiles(parts: &[Vec<u8>]) -> anyhow::Result<Vec<u8>> {
    let mut layers: Vec<MergedLayer> = Vec::new();
    let mut at: HashMap<String, usize> = HashMap::new();
    for part in parts {
        let tile = Tile::decode(part.as_slice()).context("a tile is not MVT")?;
        for layer in tile.layers {
            match at.get(&layer.name) {
                Some(index) => layers[*index].append(layer)?,
                None => {
                    at.insert(layer.name.clone(), layers.len());
                    layers.push(MergedLayer::first(layer)?);
                }
            }
        }
    }
    Ok(Tile {
        layers: layers.into_iter().map(|merged| merged.layer).collect(),
    }
    .encode_to_vec())
}

/// A stored tile decompressed: every gzip member of it, and no more than [`MAX_TILE_BYTES`].
fn gunzip(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    MultiGzDecoder::new(bytes)
        .take(MAX_TILE_BYTES + 1)
        .read_to_end(&mut out)
        .context("a tile is not gzip")?;
    ensure!(
        u64::try_from(out.len())? <= MAX_TILE_BYTES,
        "a tile decompresses to more than {MAX_TILE_BYTES} bytes"
    );
    Ok(out)
}

fn gzip(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

/// One archive being merged: its entries in tile-id order and its tile bytes.
struct Source {
    path: PathBuf,
    entries: std::vec::IntoIter<DirEntry>,
    file: BufReader<File>,
    data_offset: u64,
    /// Where `file` stands, when known: an archive stores its tiles in tile-id order, so the next
    /// tile is nearly always the next bytes and needs no seek.
    position: Option<u64>,
    /// The entry being read and how far into its run the merge is.
    current: Option<(DirEntry, u32)>,
    /// The first tile id no earlier entry covers; an entry below it is out of order.
    covered: u64,
    /// The offsets several entries of this archive point at: content it stores once.
    shared: HashSet<u64>,
    /// The bytes last read and their offset: a run, or a repeat, reads them once.
    last: Option<(u64, Vec<u8>)>,
}

impl Source {
    fn open(path: &Path) -> anyhow::Result<(Self, pmtiles::PmTilesReader<File>)> {
        let named = || format!("partition archive {}", path.display());
        let reader = PmTilesReader::new_with_path(path).with_context(named)?;
        let header = reader.get_header();
        ensure!(
            header.tile_type == TileType::Mvt && header.tile_compression == Compression::Gzip,
            "{} holds {:?} tiles compressed as {:?}; the merge takes gzip MVT",
            named(),
            header.tile_type,
            header.tile_compression
        );
        // The directory entries, read once: the order the merge walks, and which contents the
        // archive stores once for several tiles, so the output stores them once too and remembers
        // no other content.
        let entries = reader
            .entries()
            .collect::<PmtResult<Vec<DirEntry>>>()
            .with_context(named)?;
        let (mut seen, mut shared) = (HashSet::new(), HashSet::new());
        for entry in &entries {
            if entry.run_length() > 1 || !seen.insert(entry.offset()) {
                shared.insert(entry.offset());
            }
        }
        drop(seen);
        let source = Self {
            path: path.to_path_buf(),
            entries: entries.into_iter(),
            file: BufReader::with_capacity(
                READ_BUFFER_BYTES,
                File::open(path).with_context(named)?,
            ),
            data_offset: header.data_offset(),
            position: None,
            current: None,
            covered: 0,
            shared,
            last: None,
        };
        Ok((source, reader))
    }

    /// Moves to the next tile and returns its id, or `None` at the archive's end.
    fn step(&mut self) -> anyhow::Result<Option<u64>> {
        if let Some((entry, done)) = &mut self.current {
            if *done + 1 < entry.run_length() {
                *done += 1;
                return Ok(Some(entry.tile_id() + u64::from(*done)));
            }
        }
        let Some(entry) = self.entries.next() else {
            self.current = None;
            return Ok(None);
        };
        ensure!(
            entry.run_length() > 0 && entry.tile_id() >= self.covered,
            "partition archive {} lists tile {} out of tile-id order",
            self.path.display(),
            entry.tile_id()
        );
        self.covered = entry.tile_id() + u64::from(entry.run_length());
        self.current = Some((entry, 0));
        Ok(Some(entry.tile_id()))
    }

    /// The stored (compressed) bytes of the current tile, and the key under which the output may
    /// store them once: `Some` when this archive itself stores them once for several tiles.
    fn stored(&mut self, index: usize) -> anyhow::Result<(&[u8], Option<(usize, u64)>)> {
        let (entry, _) = self
            .current
            .context("the merge read an archive past its end")?;
        let offset = entry.offset();
        if self.last.as_ref().is_none_or(|(at, _)| *at != offset) {
            let length = u64::from(entry.length());
            ensure!(
                length <= MAX_TILE_BYTES,
                "a tile of {} claims {length} bytes",
                self.path.display()
            );
            let start = self
                .data_offset
                .checked_add(offset)
                .context("tile offset overflows")?;
            if self.position != Some(start) {
                self.file.seek(SeekFrom::Start(start))?;
            }
            let mut bytes = self.last.take().map_or_else(Vec::new, |(_, bytes)| bytes);
            bytes.resize(usize::try_from(length)?, 0);
            self.file
                .read_exact(&mut bytes)
                .with_context(|| format!("a tile runs past the end of {}", self.path.display()))?;
            self.position = Some(start + length);
            self.last = Some((offset, bytes));
        }
        let key = self.shared.contains(&offset).then_some((index, offset));
        let (_, bytes) = self.last.as_ref().context("a tile was just read")?;
        Ok((bytes.as_slice(), key))
    }
}

/// What a merge did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Merged {
    pub archives: usize,
    /// Tiles the output addresses.
    pub tiles: u64,
    /// Tiles one archive held, copied as stored.
    pub copied: u64,
    /// Tiles several archives held, decoded and merged.
    pub merged: u64,
}

/// The metadata of the merged archive: the first archive's, with `vector_layers` the union of
/// every archive's (a layer's zoom range widened, its fields united) and without what described
/// one partition's bake only (`tilestats`, `strategies`, `generator_options`), which nothing reads
/// and which would be one partition's numbers under the unit's name. tippecanoe's
/// `antimeridian_adjusted_bounds`, where the first archive carries it, becomes `bounds`.
fn merged_metadata(documents: &[String], bounds: (f64, f64, f64, f64)) -> anyhow::Result<String> {
    let mut merged: Option<serde_json::Map<String, Value>> = None;
    let mut layers: Vec<serde_json::Map<String, Value>> = Vec::new();
    for document in documents {
        let Value::Object(mut metadata) =
            serde_json::from_str(document).context("an archive's metadata is not JSON")?
        else {
            bail!("an archive's metadata is not a JSON object");
        };
        if let Some(Value::Array(listed)) = metadata.remove("vector_layers") {
            for layer in listed {
                let Value::Object(layer) = layer else {
                    bail!("a vector_layers member is not an object");
                };
                let id = layer.get("id").cloned();
                match layers
                    .iter_mut()
                    .find(|known| known.get("id") == id.as_ref())
                {
                    None => layers.push(layer),
                    Some(known) => widen_layer(known, &layer),
                }
            }
        }
        if merged.is_none() {
            for partition_only in ["tilestats", "strategies", "generator_options"] {
                metadata.remove(partition_only);
            }
            merged = Some(metadata);
        }
    }
    let mut merged = merged.context("no archive to take metadata from")?;
    if let Some(adjusted) = merged.get_mut("antimeridian_adjusted_bounds") {
        *adjusted = Value::from(format!(
            "{:.6},{:.6},{:.6},{:.6}",
            bounds.0, bounds.1, bounds.2, bounds.3
        ));
    }
    merged.insert(
        "vector_layers".to_owned(),
        Value::Array(layers.into_iter().map(Value::Object).collect()),
    );
    Ok(serde_json::to_string(&Value::Object(merged))?)
}

fn widen_layer(known: &mut serde_json::Map<String, Value>, other: &serde_json::Map<String, Value>) {
    for (name, pick_lower) in [("minzoom", true), ("maxzoom", false)] {
        let (Some(ours), Some(theirs)) = (
            known.get(name).and_then(Value::as_u64),
            other.get(name).and_then(Value::as_u64),
        ) else {
            continue;
        };
        let wider = if pick_lower {
            ours.min(theirs)
        } else {
            ours.max(theirs)
        };
        known.insert(name.to_owned(), Value::from(wider));
    }
    if let Some(Value::Object(fields)) = other.get("fields") {
        let ours = known
            .entry("fields")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let Value::Object(ours) = ours {
            for (field, kind) in fields {
                ours.entry(field.clone()).or_insert_with(|| kind.clone());
            }
        }
    }
}

/// The same merge over every `*.pmtiles` in `directory`, in file-name order, into `out`, which must
/// not exist yet. The publisher's `merge-pmtiles-archives` runs it, so the merge can be measured on
/// given archives outside a bake (root ADR-0613).
///
/// # Errors
/// As [`merge_archives`]; also when `out` exists or `directory` cannot be listed.
pub fn merge_directory(directory: &Path, out: &Path, deadline: Instant) -> anyhow::Result<Merged> {
    ensure!(
        !out.exists(),
        "{} exists; the merge writes a new file",
        out.display()
    );
    let mut archives = std::fs::read_dir(directory)
        .with_context(|| format!("cannot list {}", directory.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("cannot list {}", directory.display()))?;
    archives.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "pmtiles")
    });
    archives.sort();
    merge_archives(&archives, out, deadline)
}

/// Merges `archives` (gzip MVT `PMTiles`, each in tile-id order) into `out`, written whole and
/// synced before this returns. Stops with an error once `deadline` has passed.
///
/// # Errors
/// Returns an error when an archive cannot be read or is not gzip MVT, when two archives disagree
/// on a layer they share, when `out` cannot be written, or past the deadline.
pub fn merge_archives(
    archives: &[PathBuf],
    out: &Path,
    deadline: Instant,
) -> anyhow::Result<Merged> {
    ensure!(!archives.is_empty(), "no partition archive to merge");
    let mut sources = Vec::with_capacity(archives.len());
    let mut documents = Vec::with_capacity(archives.len());
    let (mut min_zoom, mut max_zoom) = (u8::MAX, u8::MIN);
    let mut bounds = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
    let mut center_zoom = None;
    for path in archives {
        let (source, reader) = Source::open(path)?;
        let header = reader.get_header();
        min_zoom = min_zoom.min(header.min_zoom);
        max_zoom = max_zoom.max(header.max_zoom);
        bounds = (
            bounds.0.min(header.min_longitude),
            bounds.1.min(header.min_latitude),
            bounds.2.max(header.max_longitude),
            bounds.3.max(header.max_latitude),
        );
        center_zoom.get_or_insert(header.center_zoom);
        documents.push(
            reader
                .get_metadata()
                .with_context(|| format!("metadata of {}", path.display()))?,
        );
        sources.push(source);
    }

    let file = File::create(out).with_context(|| format!("cannot create {}", out.display()))?;
    let mut target = BufWriter::with_capacity(READ_BUFFER_BYTES, file);
    let mut writer = PmTilesWriter::new(TileType::Mvt)
        .precompressed(Compression::Gzip)
        .dedup_by_key::<(usize, u64)>()
        .min_zoom(min_zoom)
        .max_zoom(max_zoom)
        .bounds(bounds.0, bounds.1, bounds.2, bounds.3)
        .center((bounds.0 + bounds.2) / 2.0, (bounds.1 + bounds.3) / 2.0)
        .center_zoom(center_zoom.unwrap_or(min_zoom))
        .metadata(merged_metadata(&documents, bounds)?)
        .create(&mut target)?;

    let mut next = BinaryHeap::new();
    for (index, source) in sources.iter_mut().enumerate() {
        if let Some(id) = source.step()? {
            next.push(Reverse((id, index)));
        }
    }
    let mut found = Merged {
        archives: archives.len(),
        tiles: 0,
        copied: 0,
        merged: 0,
    };
    let mut holders = Vec::with_capacity(sources.len());
    while let Some(Reverse((id, first))) = next.pop() {
        holders.clear();
        holders.push(first);
        while let Some(Reverse((other, index))) = next.peek().copied() {
            if other != id {
                break;
            }
            next.pop();
            holders.push(index);
        }
        let coord = TileCoord::from(TileId::new(id)?);
        if let [only] = holders[..] {
            let (bytes, key) = sources[only].stored(only)?;
            writer.add_tile(coord, bytes, key)?;
            found.copied += 1;
        } else {
            let mut parts = Vec::with_capacity(holders.len());
            for index in &holders {
                parts.push(gunzip(sources[*index].stored(*index)?.0)?);
            }
            let tile = merge_tiles(&parts)
                .with_context(|| format!("tile {}/{}/{}", coord.z(), coord.x(), coord.y()))?;
            writer.add_tile(coord, &gzip(&tile)?, None)?;
            found.merged += 1;
        }
        found.tiles += 1;
        for index in &holders {
            if let Some(id) = sources[*index].step()? {
                next.push(Reverse((id, *index)));
            }
        }
        if found.tiles.is_multiple_of(DEADLINE_CHECK_TILES) {
            ensure!(
                Instant::now() < deadline,
                "the partition merge exceeded its time bound after {} tiles ({} copied, {} merged) \
                 of {} archives",
                found.tiles,
                found.copied,
                found.merged,
                found.archives
            );
        }
    }
    writer.finalize()?;
    target.flush()?;
    target
        .into_inner()
        .map_err(|error| error.into_error())?
        .sync_all()
        .with_context(|| format!("cannot sync {}", out.display()))?;
    Ok(found)
}

#[cfg(test)]
#[path = "pmtiles_merge_tests.rs"]
mod tests;
