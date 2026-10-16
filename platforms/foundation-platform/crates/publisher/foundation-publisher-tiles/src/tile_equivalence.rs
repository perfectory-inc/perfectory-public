//! The decode-equivalence gate a lakehouse bake passes before it replaces a release that was not
//! itself baked from the lakehouse (root ADR-0133 §4 as amended by ADR-0135, ADR-0112 §9).
//!
//! The first lakehouse bake of a unit swaps oven as well as data: the active archive came from
//! PostGIS through `martin-cp`, the new one from GDAL and tippecanoe. The maxzoom id gate proves the
//! new archive carries exactly the served ids, but not that a customer sees the same parcels in the
//! same tiles. So sample tiles are decoded from both archives and compared:
//!
//! - (a) every feature id in the active tile is in the new tile — except, for a unit baked from a
//!   new edition (`edition_change`, ADR-0251), an id the new edition holds nowhere: it left the
//!   edition, counted as `left` or, when its whole dong is gone, `left_renumbered`, each bounded;
//! - (b) an id only the new tile has is classified (ADR-0135 §2), and only these features' geometry
//!   is decoded: `buffer` (outside the tile's own `0..extent` square — a neighbour tippecanoe keeps
//!   in the tile buffer), `tiny` (the area of its part inside the tile is at most
//!   `tiny_max_area_tile_units`: geometry the old oven's `ST_AsMVTGeom` drops), `addition` (the
//!   active archive's maxzoom tile under it lacks the id too: the active release never had the
//!   feature; allowed up to `additions_max_ratio` of the sampled active ids) or `unexplained`, which
//!   must be none;
//! - (c) a shared id carries the same properties, compared as text. A value whose MVT type changed
//!   (an integer that is now a string) is counted as `type_changed`; it is not a failure.
//!
//! Samples, at every contract zoom the layer serves (`config/tile-equivalence.contract.json`):
//! each region's centre tile and four inset corners, the densest maxzoom tiles of the active
//! archive (read from its directory only) and a seeded reservoir of the new archive's maxzoom tiles,
//! each with its ancestors. Regions are feature-id prefixes; their tile extents are recorded while
//! the id gate walks the new archive, so no coordinates live in the repository.
//!
//! (a) and (c) are exact. Every category is counted per zoom and per region, with examples, whether
//! or not the bake may promote (ADR-0135 §3).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::io::{Read, Seek};

use anyhow::{bail, ensure, Context as _};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::pmtiles_feature_ids::{tile_id, tile_zxy, zoom_range, Archive};
use super::ServedIds;

pub const SAMPLE_CONTRACT_JSON: &str =
    include_str!("../../../../config/tile-equivalence.contract.json");

/// `config/tile-equivalence.contract.json`.
#[derive(Debug, Clone, Deserialize)]
pub struct SampleContract {
    pub zooms: Vec<u8>,
    pub densest_tiles: usize,
    pub random_tiles: usize,
    pub random_seed: u64,
    pub corner_inset_percent: u32,
    pub example_ids: usize,
    /// ADR-0135 §2: the largest area, in square tile units, of an inside extra counted as tiny.
    pub tiny_max_area_tile_units: f64,
    /// ADR-0135 §2: the largest share of the sampled active ids that additions may reach.
    pub additions_max_ratio: f64,
    #[serde(default)]
    pub region_id_prefix_chars: BTreeMap<String, usize>,
    /// ADR-0251: the units baked from a cadastral edition, whose departures are classified.
    #[serde(default)]
    pub edition_change: BTreeMap<String, EditionChange>,
}

/// ADR-0251: how a unit baked from a new edition tells departures and renumbered dongs apart.
#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EditionChange {
    /// An id's first characters that name its dong (a parcel's 법정동 code).
    pub dong_prefix_chars: usize,
    /// The largest share of the sampled active ids that may leave a dong the new edition holds.
    pub left_max_ratio: f64,
    /// Replaces the contract's `additions_max_ratio` for the unit.
    pub additions_max_ratio: f64,
    /// The largest share, in each direction, of ids in dongs a reorganization renumbered.
    pub renumbered_max_ratio: f64,
}

impl SampleContract {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let contract: Self = serde_json::from_str(text).context("tile equivalence contract")?;
        ensure!(
            !contract.zooms.is_empty()
                && contract.zooms.windows(2).all(|pair| pair[0] < pair[1])
                && contract.zooms.iter().all(|zoom| *zoom <= 30),
            "the equivalence zooms must be a nonempty increasing list up to 30"
        );
        ensure!(
            contract.corner_inset_percent < 50,
            "corner_inset_percent must be below 50"
        );
        ensure!(
            contract.tiny_max_area_tile_units >= 0.0,
            "tiny_max_area_tile_units cannot be negative"
        );
        ensure!(
            (0.0..=1.0).contains(&contract.additions_max_ratio),
            "additions_max_ratio is a share between 0 and 1"
        );
        ensure!(
            contract.example_ids > 0,
            "example_ids must name at least one example"
        );
        ensure!(
            contract
                .region_id_prefix_chars
                .values()
                .all(|chars| *chars > 0),
            "a region id prefix is at least one character"
        );
        for edition in contract.edition_change.values() {
            ensure!(
                edition.dong_prefix_chars > 0,
                "a dong id prefix is at least one character"
            );
            ensure!(
                [
                    edition.left_max_ratio,
                    edition.additions_max_ratio,
                    edition.renumbered_max_ratio
                ]
                .iter()
                .all(|ratio| (0.0..=1.0).contains(ratio)),
                "an edition_change bound is a share between 0 and 1"
            );
        }
        Ok(contract)
    }
}

/// `SplitMix64`: a fixed, seedable sequence, so the same seed samples the same tiles of the same
/// archive on every run.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// A region's extent in maxzoom tile coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    pub min: (u32, u32),
    pub max: (u32, u32),
}

/// Collects, during the id gate's walk of the new archive's maxzoom tiles, each region's extent and
/// a seeded reservoir of tiles.
pub struct SampleCollector {
    prefix_chars: Option<usize>,
    capacity: usize,
    rng: SplitMix,
    seen: u64,
    pub reservoir: Vec<u64>,
    pub regions: BTreeMap<String, Extent>,
    /// ADR-0251: the dongs the new archive's ids fall in, for a unit baked from an edition.
    pub edition: Option<(EditionChange, BTreeSet<String>)>,
    current: Option<(u64, u32, u32)>,
}

impl SampleCollector {
    pub fn new(contract: &SampleContract, unit: &str) -> Self {
        Self {
            prefix_chars: contract.region_id_prefix_chars.get(unit).copied(),
            capacity: contract.random_tiles,
            rng: SplitMix(contract.random_seed),
            seen: 0,
            reservoir: Vec::new(),
            regions: BTreeMap::new(),
            edition: contract
                .edition_change
                .get(unit)
                .map(|edition| (*edition, BTreeSet::new())),
            current: None,
        }
    }

    pub fn observe(&mut self, tile: u64, id: &str) -> anyhow::Result<()> {
        if let Some((edition, dongs)) = &mut self.edition {
            let dong = prefix(id, edition.dong_prefix_chars);
            if !dongs.contains(dong) {
                dongs.insert(dong.to_owned());
            }
        }
        let (x, y) = match self.current {
            Some((current, x, y)) if current == tile => (x, y),
            _ => {
                let (_, x, y) = tile_zxy(tile)?;
                self.current = Some((tile, x, y));
                self.seen += 1;
                if self.reservoir.len() < self.capacity {
                    self.reservoir.push(tile);
                } else if let Ok(slot) = usize::try_from(self.rng.below(self.seen)) {
                    if let Some(kept) = self.reservoir.get_mut(slot) {
                        *kept = tile;
                    }
                }
                (x, y)
            }
        };
        if let Some(chars) = self.prefix_chars {
            let region = prefix(id, chars);
            if let Some(extent) = self.regions.get_mut(region) {
                extent.min = (extent.min.0.min(x), extent.min.1.min(y));
                extent.max = (extent.max.0.max(x), extent.max.1.max(y));
            } else {
                self.regions.insert(
                    region.to_owned(),
                    Extent {
                        min: (x, y),
                        max: (x, y),
                    },
                );
            }
        }
        Ok(())
    }
}

/// An id's first `chars` characters, or the whole id when it is shorter.
fn prefix(id: &str, chars: usize) -> &str {
    id.char_indices().nth(chars).map_or(id, |(at, _)| &id[..at])
}

/// The `limit` maxzoom tiles with the longest directory entries — the most bytes — read from the
/// directory alone.
pub fn densest_tiles<R: Read + Seek>(
    archive: &mut Archive<R>,
    zoom: u8,
    limit: usize,
) -> anyhow::Result<Vec<u64>> {
    let mut heap: BinaryHeap<Reverse<(u64, u64)>> = BinaryHeap::new();
    archive.for_each_entry(zoom_range(zoom), &mut |_, entry| {
        if limit > 0 {
            heap.push(Reverse((entry.length, entry.tile_id)));
            if heap.len() > limit {
                heap.pop();
            }
        }
        Ok(())
    })?;
    Ok(heap.into_iter().map(|Reverse((_, tile))| tile).collect())
}

/// The tile ids to compare, each with why it was chosen (`region:<key>`, `densest`, `random`).
pub fn sample_tiles(
    contract: &SampleContract,
    zooms: (u8, u8),
    regions: &BTreeMap<String, Extent>,
    densest: &[u64],
    random: &[u64],
) -> anyhow::Result<BTreeMap<u64, BTreeSet<String>>> {
    let (min_zoom, max_zoom) = zooms;
    let zooms: Vec<u8> = contract
        .zooms
        .iter()
        .copied()
        .filter(|zoom| (min_zoom..=max_zoom).contains(zoom))
        .collect();
    let mut samples: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut add = |x: u32, y: u32, why: &str| {
        for zoom in &zooms {
            let shift = max_zoom - zoom;
            samples
                .entry(tile_id(*zoom, x >> shift, y >> shift))
                .or_default()
                .insert(why.to_owned());
        }
    };
    for (key, extent) in regions {
        let why = format!("region:{key}");
        let inset = |low: u32, high: u32| {
            let span = u64::from(high - low) * u64::from(contract.corner_inset_percent) / 100;
            let span = u32::try_from(span).unwrap_or(0);
            (low + span, high - span)
        };
        let (x0, x1) = inset(extent.min.0, extent.max.0);
        let (y0, y1) = inset(extent.min.1, extent.max.1);
        let centre = (
            extent.min.0 + (extent.max.0 - extent.min.0) / 2,
            extent.min.1 + (extent.max.1 - extent.min.1) / 2,
        );
        for (x, y) in [centre, (x0, y0), (x1, y0), (x0, y1), (x1, y1)] {
            add(x, y, &why);
        }
    }
    for (tiles, why) in [(densest, "densest"), (random, "random")] {
        for tile in tiles {
            let (zoom, x, y) = tile_zxy(*tile)?;
            ensure!(
                zoom == max_zoom,
                "sample tile {tile} is not at maxzoom {max_zoom}"
            );
            add(x, y, why);
        }
    }
    Ok(samples)
}

/// MVT with what the comparison reads: geometry stays undecoded bytes until a feature needs it.
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
    pub values: Vec<TileValue>,
    #[prost(uint32, optional, tag = "5")]
    pub extent: Option<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Feature {
    #[prost(uint32, repeated, tag = "2")]
    pub tags: Vec<u32>,
    /// The packed command stream, kept as bytes; decoded only for an extra feature.
    #[prost(bytes = "vec", tag = "4")]
    pub geometry: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TileValue {
    #[prost(string, optional, tag = "1")]
    pub string_value: Option<String>,
    #[prost(float, optional, tag = "2")]
    pub float_value: Option<f32>,
    #[prost(double, optional, tag = "3")]
    pub double_value: Option<f64>,
    #[prost(int64, optional, tag = "4")]
    pub int_value: Option<i64>,
    #[prost(uint64, optional, tag = "5")]
    pub uint_value: Option<u64>,
    #[prost(sint64, optional, tag = "6")]
    pub sint_value: Option<i64>,
    #[prost(bool, optional, tag = "7")]
    pub bool_value: Option<bool>,
}

impl TileValue {
    /// The value's MVT type and its text.
    fn typed_text(&self) -> (&'static str, String) {
        if let Some(text) = &self.string_value {
            ("string", text.clone())
        } else if let Some(value) = self.float_value {
            ("float", value.to_string())
        } else if let Some(value) = self.double_value {
            ("double", value.to_string())
        } else if let Some(value) = self.int_value.or(self.sint_value) {
            ("int", value.to_string())
        } else if let Some(value) = self.uint_value {
            ("uint", value.to_string())
        } else if let Some(value) = self.bool_value {
            ("bool", value.to_string())
        } else {
            ("none", String::new())
        }
    }
}

/// A decoded feature: its typed properties and its undecoded geometry.
struct View {
    properties: BTreeMap<String, (&'static str, String)>,
    geometry: Vec<u8>,
}

/// The features of the layer in a tile, by id; the tile's extent.
fn features_by_id(
    bytes: Option<&[u8]>,
    layer_name: &str,
    id_property: &str,
) -> anyhow::Result<(BTreeMap<String, View>, u32)> {
    let mut features = BTreeMap::new();
    let mut extent = 4096;
    let Some(bytes) = bytes else {
        return Ok((features, extent));
    };
    let tile = Tile::decode(bytes).context("a sampled tile is not an MVT tile")?;
    for layer in tile
        .layers
        .into_iter()
        .filter(|layer| layer.name == layer_name)
    {
        extent = layer.extent.unwrap_or(4096);
        for feature in layer.features {
            let mut properties = BTreeMap::new();
            for pair in feature.tags.chunks_exact(2) {
                let key = usize::try_from(pair[0])
                    .ok()
                    .and_then(|at| layer.keys.get(at))
                    .context("a feature tag names no key")?;
                let value = usize::try_from(pair[1])
                    .ok()
                    .and_then(|at| layer.values.get(at))
                    .context("a feature tag names no value")?;
                properties.insert(key.clone(), value.typed_text());
            }
            let id = properties
                .get(id_property)
                .map(|(_, text)| text.clone())
                .with_context(|| format!("a sampled feature has no {id_property}"))?;
            // A feature cut into parts appears more than once; the first carries its properties.
            features.entry(id).or_insert(View {
                properties,
                geometry: feature.geometry,
            });
        }
    }
    Ok((features, extent))
}

/// The rings of an MVT command stream, in tile units: each `MoveTo` starts one.
pub fn geometry_rings(packed: &[u8]) -> anyhow::Result<Vec<Vec<(i64, i64)>>> {
    let mut words = Vec::new();
    let mut at = 0;
    while at < packed.len() {
        let mut value = 0_u64;
        let mut shift = 0;
        loop {
            let byte = *packed.get(at).context("geometry ends inside a varint")?;
            at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            ensure!(shift < 64, "geometry varint is longer than 64 bits");
        }
        words.push(value);
    }
    let (mut x, mut y) = (0_i64, 0_i64);
    let mut rings: Vec<Vec<(i64, i64)>> = Vec::new();
    let mut index = 0;
    while index < words.len() {
        let command = words[index];
        index += 1;
        let (id, count) = (command & 7, command >> 3);
        match id {
            1 | 2 => {
                for _ in 0..count {
                    let dx = words.get(index).copied().context("geometry lacks a dx")?;
                    let dy = words
                        .get(index + 1)
                        .copied()
                        .context("geometry lacks a dy")?;
                    index += 2;
                    x += zigzag(dx);
                    y += zigzag(dy);
                    if id == 1 || rings.is_empty() {
                        rings.push(Vec::new());
                    }
                    if let Some(ring) = rings.last_mut() {
                        ring.push((x, y));
                    }
                }
            }
            7 => {}
            other => bail!("geometry command {other} is not MoveTo, LineTo or ClosePath"),
        }
    }
    Ok(rings)
}

/// The vertices of an MVT command stream, in tile units.
pub fn geometry_points(packed: &[u8]) -> anyhow::Result<Vec<(i64, i64)>> {
    Ok(geometry_rings(packed)?.into_iter().flatten().collect())
}

/// The area, in square tile units, of the part of a polygon's rings inside the tile's
/// `0..extent` square (ADR-0135 §2: tiny is decided by area, not span — a long thin sliver has a
/// large span and almost no area). Each ring is clipped to the square (Sutherland–Hodgman, exact
/// for a convex clip) and the signed areas are summed, so holes subtract.
pub fn area_inside(rings: &[Vec<(i64, i64)>], extent: i64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let to_float = |(x, y): (i64, i64)| (x as f64, y as f64);
    #[allow(clippy::cast_precision_loss)]
    let side = extent as f64;
    let mut total = 0.0;
    for ring in rings {
        let mut polygon: Vec<(f64, f64)> = ring.iter().copied().map(to_float).collect();
        // Each edge of the square as (inside test, intersection with the boundary line).
        let edges: [(fn(f64, f64, f64) -> bool, usize, bool); 4] = [
            (|value, _, _| value >= 0.0, 0, false),
            (|value, _, side| value <= side, 0, true),
            (|value, _, _| value >= 0.0, 1, false),
            (|value, _, side| value <= side, 1, true),
        ];
        for (inside, axis, far) in edges {
            let bound = if far { side } else { 0.0 };
            let coordinate = |point: (f64, f64)| if axis == 0 { point.0 } else { point.1 };
            let mut clipped = Vec::with_capacity(polygon.len() + 4);
            for index in 0..polygon.len() {
                let current = polygon[index];
                let previous = polygon[(index + polygon.len() - 1) % polygon.len()];
                let (current_in, previous_in) = (
                    inside(coordinate(current), 0.0, side),
                    inside(coordinate(previous), 0.0, side),
                );
                if current_in != previous_in {
                    let t = (bound - coordinate(previous))
                        / (coordinate(current) - coordinate(previous));
                    clipped.push((
                        previous.0 + t * (current.0 - previous.0),
                        previous.1 + t * (current.1 - previous.1),
                    ));
                }
                if current_in {
                    clipped.push(current);
                }
            }
            polygon = clipped;
            if polygon.is_empty() {
                break;
            }
        }
        let count = polygon.len();
        total += (0..count)
            .map(|index| {
                let (x0, y0) = polygon[index];
                let (x1, y1) = polygon[(index + 1) % count];
                x0 * y1 - x1 * y0
            })
            .sum::<f64>()
            / 2.0;
    }
    total.abs()
}

const fn zigzag(value: u64) -> i64 {
    // The cast reinterprets the bits, which is what zigzag decoding is.
    #[allow(clippy::cast_possible_wrap)]
    let half = (value >> 1) as i64;
    half ^ -((value & 1) as i64)
}

/// A count with its first examples.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Tally {
    pub count: u64,
    pub examples: Vec<String>,
}

impl Tally {
    fn add(&mut self, example: String, limit: usize) {
        self.count += 1;
        if self.examples.len() < limit {
            self.examples.push(example);
        }
    }
}

/// A count with its first examples, split by zoom and by region.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Breakdown {
    pub count: u64,
    pub examples: Vec<String>,
    pub by_zoom: BTreeMap<u8, u64>,
    pub by_region: BTreeMap<String, u64>,
}

impl Breakdown {
    fn add(&mut self, example: String, zoom: u8, region: Option<&str>, limit: usize) {
        self.count += 1;
        if self.examples.len() < limit {
            self.examples.push(example);
        }
        *self.by_zoom.entry(zoom).or_default() += 1;
        if let Some(region) = region {
            *self.by_region.entry(region.to_owned()).or_default() += 1;
        }
    }
}

/// The ids only the new tile has that lie inside the tile, by what explains them.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ExtraInside {
    /// Its part inside the tile has an area of at most `tiny_max_area_tile_units`: the old oven's
    /// `ST_AsMVTGeom` drops such geometry. Allowed (ADR-0135 §2).
    pub tiny: Breakdown,
    /// The active archive's maxzoom tile at the feature's position does not have the id either: the
    /// active release does not carry the feature at all. Allowed up to `additions_max_ratio` of the
    /// sampled active ids (ADR-0135 §2).
    pub addition: Breakdown,
    /// Neither. Must be none (ADR-0135 §2).
    pub unexplained: Breakdown,
}

/// ADR-0251: the active ids the new edition no longer holds, and the additions in renumbered dongs.
#[derive(Debug, Default, Clone, Serialize)]
pub struct EditionTally {
    /// Gone from a dong the new edition still holds: split, merged or closed.
    pub left: Breakdown,
    /// Gone with its whole dong: the dong was renumbered by a reorganization.
    pub left_renumbered: Breakdown,
    /// An addition where the active maxzoom tile holds a dong the new edition does not.
    pub joined_renumbered: Breakdown,
    /// The contract bounds this tally was judged by.
    pub bounds: EditionChange,
}

/// Sampled and passed tiles of one region.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct RegionTally {
    pub sampled: u64,
    pub passed: u64,
}

/// The comparison's outcome. Examples are `id@z/x/y`.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Equivalence {
    pub tiles: u64,
    pub tiles_passed: u64,
    /// Features in the sampled active tiles, the base the other counts are a share of.
    pub active_ids: u64,
    pub missing: Tally,
    pub extra_inside: ExtraInside,
    pub changed: Tally,
    pub type_changed: Tally,
    pub extra_in_buffer: u64,
    pub regions: BTreeMap<String, RegionTally>,
    /// ADR-0251, for a unit baked from an edition.
    pub edition: Option<EditionTally>,
    /// The contract bounds this outcome was judged by, kept with it as evidence.
    pub tiny_max_area_tile_units: f64,
    pub additions_max_ratio: f64,
}

impl Equivalence {
    /// The share of the sampled active ids that additions reached.
    pub fn additions_ratio(&self) -> f64 {
        self.share(self.extra_inside.addition.count)
    }

    /// `count` as a share of the sampled active ids; any count against none is unbounded.
    pub fn share(&self, count: u64) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let (counted, active) = (count as f64, self.active_ids as f64);
        if self.active_ids == 0 {
            if count == 0 {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            counted / active
        }
    }

    /// ADR-0251: each edition class within its bound (true for a unit with none).
    fn edition_within_bounds(&self) -> bool {
        self.edition.as_ref().is_none_or(|edition| {
            self.share(edition.left.count) <= edition.bounds.left_max_ratio
                && self.share(edition.left_renumbered.count) <= edition.bounds.renumbered_max_ratio
                && self.share(edition.joined_renumbered.count)
                    <= edition.bounds.renumbered_max_ratio
        })
    }

    /// ADR-0135 §1–2: nothing missing, nothing changed, nothing unexplained, and additions no more
    /// than their contract share. Buffer and tiny extras are allowed and only counted.
    pub fn passed(&self) -> bool {
        self.missing.count == 0
            && self.changed.count == 0
            && self.extra_inside.unexplained.count == 0
            && self.additions_ratio() <= self.additions_max_ratio
            && self.edition_within_bounds()
    }

    /// The refusal, short enough for a build's failure reason: every count, and the ids of what
    /// nothing explains. The whole evidence is written next to the work directory.
    pub fn refusal(&self) -> Value {
        let ids = |examples: &[String]| -> Vec<String> {
            examples
                .iter()
                .map(|example| example.split('@').next().unwrap_or_default().to_owned())
                .collect()
        };
        let inside = &self.extra_inside;
        serde_json::json!({
            "tiles": self.tiles,
            "tiles_passed": self.tiles_passed,
            "active_ids": self.active_ids,
            "missing": {"count": self.missing.count, "ids": ids(&self.missing.examples)},
            "changed": {"count": self.changed.count, "ids": ids(&self.changed.examples)},
            "extra_inside": {
                "tiny": {"count": inside.tiny.count, "by_zoom": inside.tiny.by_zoom},
                "addition": {
                    "count": inside.addition.count,
                    "ratio": self.additions_ratio(),
                    "max_ratio": self.additions_max_ratio,
                    "ids": ids(&inside.addition.examples),
                },
                "unexplained": {
                    "count": inside.unexplained.count,
                    "ids": ids(&inside.unexplained.examples),
                },
            },
            "type_changed": self.type_changed.count,
            "edition": self.edition.as_ref().map(|edition| {
                let class = |tally: &Breakdown, max_ratio: f64| {
                    serde_json::json!({
                        "count": tally.count,
                        "ratio": self.share(tally.count),
                        "max_ratio": max_ratio,
                        "ids": ids(&tally.examples),
                    })
                };
                serde_json::json!({
                    "left": class(&edition.left, edition.bounds.left_max_ratio),
                    "left_renumbered": class(&edition.left_renumbered, edition.bounds.renumbered_max_ratio),
                    "joined_renumbered": class(&edition.joined_renumbered, edition.bounds.renumbered_max_ratio),
                })
            }),
        })
    }
}

/// ADR-0251: what the comparison knows about the new edition.
#[derive(Debug, Clone, Copy)]
pub struct EditionCompare<'a> {
    pub dong_prefix_chars: usize,
    /// The dongs the new archive's ids fall in.
    pub dongs: &'a BTreeSet<String>,
    /// Every served id of the new archive.
    pub served: &'a ServedIds,
}

impl EditionCompare<'_> {
    fn dong_held(&self, id: &str) -> bool {
        self.dongs.contains(prefix(id, self.dong_prefix_chars))
    }
}

/// How the comparison reads tiles.
#[derive(Debug, Clone)]
pub struct Compare<'a> {
    pub layer: &'a str,
    pub id_property: &'a str,
    pub max_zoom: u8,
    pub tiny_max_area_tile_units: f64,
    pub region_chars: Option<usize>,
    pub limit: usize,
    pub edition: Option<EditionCompare<'a>>,
}

impl Compare<'_> {
    fn region<'id>(&self, id: &'id str) -> Option<&'id str> {
        Some(prefix(id, self.region_chars?))
    }
}

/// The ids of a tile's layer, for the addition check's lookup of active maxzoom tiles.
pub fn tile_ids(
    bytes: Option<&[u8]>,
    layer: &str,
    id_property: &str,
) -> anyhow::Result<BTreeSet<String>> {
    Ok(features_by_id(bytes, layer, id_property)?
        .0
        .into_keys()
        .collect())
}

/// The maxzoom tile under a feature: at its first vertex inside this tile, or at the centre of
/// its part inside the tile when no vertex is.
fn maxzoom_tile_under(
    points: &[(i64, i64)],
    clipped: (i64, i64, i64, i64),
    (zoom, x, y): (u8, u32, u32),
    extent: i64,
    max_zoom: u8,
) -> anyhow::Result<u64> {
    let (px, py) = points
        .iter()
        .copied()
        .find(|(px, py)| (0..extent).contains(px) && (0..extent).contains(py))
        .unwrap_or(((clipped.0 + clipped.2) / 2, (clipped.1 + clipped.3) / 2));
    let scale = 1_i64 << (max_zoom - zoom);
    let at = |origin: u32, position: i64| -> anyhow::Result<u32> {
        let within = position.clamp(0, extent - 1) * scale / extent;
        Ok(u32::try_from(i64::from(origin) * scale + within)?)
    };
    Ok(tile_id(max_zoom, at(x, px)?, at(y, py)?))
}

/// Compares one tile of each archive, adding to `outcome`. `active_maxzoom` returns the ids of the
/// active archive's maxzoom tile with the given id. Returns whether the tile passed.
pub fn compare_tile(
    active: Option<&[u8]>,
    new: Option<&[u8]>,
    compare: &Compare<'_>,
    tile: u64,
    active_maxzoom: &mut dyn FnMut(u64) -> anyhow::Result<BTreeSet<String>>,
    outcome: &mut Equivalence,
) -> anyhow::Result<bool> {
    let (zoom, x, y) = tile_zxy(tile)?;
    let at = |id: &str| format!("{id}@{zoom}/{x}/{y}");
    let limit = compare.limit;
    let (active, _) = features_by_id(active, compare.layer, compare.id_property)?;
    let (new, extent) = features_by_id(new, compare.layer, compare.id_property)?;
    let extent = i64::from(extent);
    outcome.active_ids += u64::try_from(active.len())?;
    let mut passed = true;
    for (id, before) in &active {
        let Some(after) = new.get(id) else {
            // ADR-0251: an id the new edition holds nowhere left it; one it still holds is missing.
            match compare
                .edition
                .filter(|edition| !edition.served.contains(id))
            {
                Some(edition) => {
                    let tally = outcome.edition.get_or_insert_with(EditionTally::default);
                    let class = if edition.dong_held(id) {
                        &mut tally.left
                    } else {
                        &mut tally.left_renumbered
                    };
                    class.add(at(id), zoom, compare.region(id), limit);
                }
                None => {
                    outcome.missing.add(at(id), limit);
                    passed = false;
                }
            }
            continue;
        };
        let texts = |view: &View| -> BTreeMap<String, String> {
            view.properties
                .iter()
                .map(|(key, (_, text))| (key.clone(), text.clone()))
                .collect()
        };
        if texts(before) != texts(after) {
            outcome.changed.add(at(id), limit);
            passed = false;
        } else if before
            .properties
            .iter()
            .any(|(key, (kind, _))| after.properties.get(key).map(|(k, _)| k) != Some(kind))
        {
            outcome.type_changed.add(at(id), limit);
        }
    }
    for (id, view) in new.iter().filter(|(id, _)| !active.contains_key(*id)) {
        let points = geometry_points(&view.geometry)?;
        let clipped = points
            .iter()
            .fold(None, |bounds: Option<(i64, i64, i64, i64)>, (px, py)| {
                Some(bounds.map_or((*px, *py, *px, *py), |(a, b, c, d)| {
                    (a.min(*px), b.min(*py), c.max(*px), d.max(*py))
                }))
            })
            .filter(|(min_x, min_y, max_x, max_y)| {
                *max_x > 0 && *max_y > 0 && *min_x < extent && *min_y < extent
            })
            .map(|(min_x, min_y, max_x, max_y)| {
                (
                    min_x.max(0),
                    min_y.max(0),
                    max_x.min(extent),
                    max_y.min(extent),
                )
            });
        let Some(clipped) = clipped else {
            outcome.extra_in_buffer += 1;
            continue;
        };
        let region = compare.region(id);
        // ADR-0135 §2: tiny by area inside the tile, then addition, else unexplained.
        let kind = if area_inside(&geometry_rings(&view.geometry)?, extent)
            <= compare.tiny_max_area_tile_units
        {
            &mut outcome.extra_inside.tiny
        } else {
            let under =
                maxzoom_tile_under(&points, clipped, (zoom, x, y), extent, compare.max_zoom)?;
            let held = active_maxzoom(under)?;
            if held.contains(id) {
                passed = false;
                &mut outcome.extra_inside.unexplained
            } else if compare
                .edition
                .is_some_and(|edition| held.iter().any(|active_id| !edition.dong_held(active_id)))
            {
                // ADR-0251: where the active release held a dong the new edition renumbered.
                &mut outcome
                    .edition
                    .get_or_insert_with(EditionTally::default)
                    .joined_renumbered
            } else {
                &mut outcome.extra_inside.addition
            }
        };
        kind.add(at(id), zoom, region, limit);
    }
    outcome.tiles += 1;
    outcome.tiles_passed += u64::from(passed);
    Ok(passed)
}

/// Decodes every sample tile from both archives and compares them. `regions` are the region keys
/// the collector saw, so a region no sample reached reports zero rather than nothing.
pub fn compare_archives<A: Read + Seek, B: Read + Seek>(
    active: &mut Archive<A>,
    new: &mut Archive<B>,
    samples: &BTreeMap<u64, BTreeSet<String>>,
    compare: &Compare<'_>,
    regions: impl IntoIterator<Item = String>,
) -> anyhow::Result<Equivalence> {
    let mut outcome = Equivalence {
        regions: regions
            .into_iter()
            .map(|key| (key, RegionTally::default()))
            .collect(),
        ..Equivalence::default()
    };
    let mut cache: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for (tile, why) in samples {
        let before = active.tile(*tile)?;
        let after = new.tile(*tile)?;
        let passed = compare_tile(
            before.as_deref(),
            after.as_deref(),
            compare,
            *tile,
            &mut |under| {
                if let Some(ids) = cache.get(&under) {
                    return Ok(ids.clone());
                }
                let ids = tile_ids(
                    active.tile(under)?.as_deref(),
                    compare.layer,
                    compare.id_property,
                )?;
                cache.insert(under, ids.clone());
                Ok(ids)
            },
            &mut outcome,
        )?;
        for region in why.iter().filter_map(|why| why.strip_prefix("region:")) {
            let tally = outcome.regions.entry(region.to_owned()).or_default();
            tally.sampled += 1;
            tally.passed += u64::from(passed);
        }
    }
    Ok(outcome)
}

/// Where the active release came from, which decides whether the gate runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveOrigin {
    /// Baked by a lakehouse bake: the maxzoom id gate already compares like with like.
    LakehouseBake,
    /// Built any other way (PostGIS through `martin-cp` today): the first lakehouse bake is compared.
    OtherOven,
}

/// The evidence for an active release the gate does not compare.
pub fn skipped(origin: ActiveOrigin) -> Option<Value> {
    (origin == ActiveOrigin::LakehouseBake).then(|| {
        serde_json::json!({
            "skipped": "the active release is a lakehouse_bake; the maxzoom id gate compares it"
        })
    })
}

/// Runs the whole gate over an active and a new archive: densest tiles from the active directory,
/// the samples, the comparison. Returns the outcome; the caller refuses when it did not pass.
pub fn first_release_gate<A: Read + Seek, B: Read + Seek>(
    active: A,
    new: B,
    contract: &SampleContract,
    (layer, id_property): (&str, &str),
    (min_zoom, max_zoom): (u8, u8),
    collector: &SampleCollector,
    served: &ServedIds,
) -> anyhow::Result<Equivalence> {
    let mut active = Archive::open(active).context("the active archive")?;
    let mut new = Archive::open(new).context("the new archive")?;
    let densest = densest_tiles(&mut active, max_zoom, contract.densest_tiles)?;
    let samples = sample_tiles(
        contract,
        (min_zoom, max_zoom),
        &collector.regions,
        &densest,
        &collector.reservoir,
    )?;
    let mut outcome = compare_archives(
        &mut active,
        &mut new,
        &samples,
        &Compare {
            layer,
            id_property,
            max_zoom,
            tiny_max_area_tile_units: contract.tiny_max_area_tile_units,
            region_chars: collector.prefix_chars,
            limit: contract.example_ids,
            edition: collector
                .edition
                .as_ref()
                .map(|(edition, dongs)| EditionCompare {
                    dong_prefix_chars: edition.dong_prefix_chars,
                    dongs,
                    served,
                }),
        },
        collector.regions.keys().cloned(),
    )?;
    outcome.tiny_max_area_tile_units = contract.tiny_max_area_tile_units;
    outcome.additions_max_ratio = contract.additions_max_ratio;
    if let Some((edition, _)) = &collector.edition {
        outcome.additions_max_ratio = edition.additions_max_ratio;
        outcome
            .edition
            .get_or_insert_with(EditionTally::default)
            .bounds = *edition;
    }
    Ok(outcome)
}

/// The gate's verdict: the evidence when it passed, a refusal naming the counts when it did not.
/// `evidence_file` names where the caller kept the whole evidence.
pub fn verdict(outcome: &Equivalence, evidence_file: &str) -> anyhow::Result<Value> {
    let evidence = serde_json::to_value(outcome)?;
    if !outcome.passed() {
        tracing::error!(evidence = %evidence, "first-release equivalence refused the bake");
        bail!(
            "first-release equivalence refused the bake (whole evidence {evidence_file}): {}",
            outcome.refusal()
        );
    }
    Ok(evidence)
}

#[cfg(test)]
#[path = "tile_equivalence_tests.rs"]
mod tests;
