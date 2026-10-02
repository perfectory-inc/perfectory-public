//! The decode-equivalence gate a lakehouse bake passes before it replaces a release that was not
//! itself baked from the lakehouse (root ADR-0133 §4 as amended by ADR-0135, ADR-0112 §9).
//!
//! The first lakehouse bake of a unit swaps oven as well as data: the active archive came from
//! PostGIS through `martin-cp`, the new one from GDAL and tippecanoe. The maxzoom id gate proves the
//! new archive carries exactly the served ids, but not that a customer sees the same parcels in the
//! same tiles. So sample tiles are decoded from both archives and compared:
//!
//! - (a) every feature id in the active tile is in the new tile;
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

pub(crate) const SAMPLE_CONTRACT_JSON: &str =
    include_str!("../../../config/tile-equivalence.contract.json");

/// `config/tile-equivalence.contract.json`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SampleContract {
    pub(crate) zooms: Vec<u8>,
    pub(crate) densest_tiles: usize,
    pub(crate) random_tiles: usize,
    pub(crate) random_seed: u64,
    pub(crate) corner_inset_percent: u32,
    pub(crate) example_ids: usize,
    /// ADR-0135 §2: the largest area, in square tile units, of an inside extra counted as tiny.
    pub(crate) tiny_max_area_tile_units: f64,
    /// ADR-0135 §2: the largest share of the sampled active ids that additions may reach.
    pub(crate) additions_max_ratio: f64,
    #[serde(default)]
    pub(crate) region_id_prefix_chars: BTreeMap<String, usize>,
}

impl SampleContract {
    pub(crate) fn parse(text: &str) -> anyhow::Result<Self> {
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
pub(crate) struct Extent {
    pub(crate) min: (u32, u32),
    pub(crate) max: (u32, u32),
}

/// Collects, during the id gate's walk of the new archive's maxzoom tiles, each region's extent and
/// a seeded reservoir of tiles.
pub(crate) struct SampleCollector {
    prefix_chars: Option<usize>,
    capacity: usize,
    rng: SplitMix,
    seen: u64,
    pub(crate) reservoir: Vec<u64>,
    pub(crate) regions: BTreeMap<String, Extent>,
    current: Option<(u64, u32, u32)>,
}

impl SampleCollector {
    pub(crate) fn new(contract: &SampleContract, unit: &str) -> Self {
        Self {
            prefix_chars: contract.region_id_prefix_chars.get(unit).copied(),
            capacity: contract.random_tiles,
            rng: SplitMix(contract.random_seed),
            seen: 0,
            reservoir: Vec::new(),
            regions: BTreeMap::new(),
            current: None,
        }
    }

    pub(crate) fn observe(&mut self, tile: u64, id: &str) -> anyhow::Result<()> {
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
            let region = id.char_indices().nth(chars).map_or(id, |(at, _)| &id[..at]);
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

/// The `limit` maxzoom tiles with the longest directory entries — the most bytes — read from the
/// directory alone.
pub(crate) fn densest_tiles<R: Read + Seek>(
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
pub(crate) fn sample_tiles(
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
    pub(crate) values: Vec<TileValue>,
    #[prost(uint32, optional, tag = "5")]
    pub(crate) extent: Option<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Feature {
    #[prost(uint32, repeated, tag = "2")]
    pub(crate) tags: Vec<u32>,
    /// The packed command stream, kept as bytes; decoded only for an extra feature.
    #[prost(bytes = "vec", tag = "4")]
    pub(crate) geometry: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TileValue {
    #[prost(string, optional, tag = "1")]
    pub(crate) string_value: Option<String>,
    #[prost(float, optional, tag = "2")]
    pub(crate) float_value: Option<f32>,
    #[prost(double, optional, tag = "3")]
    pub(crate) double_value: Option<f64>,
    #[prost(int64, optional, tag = "4")]
    pub(crate) int_value: Option<i64>,
    #[prost(uint64, optional, tag = "5")]
    pub(crate) uint_value: Option<u64>,
    #[prost(sint64, optional, tag = "6")]
    pub(crate) sint_value: Option<i64>,
    #[prost(bool, optional, tag = "7")]
    pub(crate) bool_value: Option<bool>,
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
pub(crate) fn geometry_rings(packed: &[u8]) -> anyhow::Result<Vec<Vec<(i64, i64)>>> {
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
pub(crate) fn geometry_points(packed: &[u8]) -> anyhow::Result<Vec<(i64, i64)>> {
    Ok(geometry_rings(packed)?.into_iter().flatten().collect())
}

/// The area, in square tile units, of the part of a polygon's rings inside the tile's
/// `0..extent` square (ADR-0135 §2: tiny is decided by area, not span — a long thin sliver has a
/// large span and almost no area). Each ring is clipped to the square (Sutherland–Hodgman, exact
/// for a convex clip) and the signed areas are summed, so holes subtract.
pub(crate) fn area_inside(rings: &[Vec<(i64, i64)>], extent: i64) -> f64 {
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
pub(crate) struct Tally {
    pub(crate) count: u64,
    pub(crate) examples: Vec<String>,
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
pub(crate) struct Breakdown {
    pub(crate) count: u64,
    pub(crate) examples: Vec<String>,
    pub(crate) by_zoom: BTreeMap<u8, u64>,
    pub(crate) by_region: BTreeMap<String, u64>,
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
pub(crate) struct ExtraInside {
    /// Its part inside the tile has an area of at most `tiny_max_area_tile_units`: the old oven's
    /// `ST_AsMVTGeom` drops such geometry. Allowed (ADR-0135 §2).
    pub(crate) tiny: Breakdown,
    /// The active archive's maxzoom tile at the feature's position does not have the id either: the
    /// active release does not carry the feature at all. Allowed up to `additions_max_ratio` of the
    /// sampled active ids (ADR-0135 §2).
    pub(crate) addition: Breakdown,
    /// Neither. Must be none (ADR-0135 §2).
    pub(crate) unexplained: Breakdown,
}

/// Sampled and passed tiles of one region.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct RegionTally {
    pub(crate) sampled: u64,
    pub(crate) passed: u64,
}

/// The comparison's outcome. Examples are `id@z/x/y`.
#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct Equivalence {
    pub(crate) tiles: u64,
    pub(crate) tiles_passed: u64,
    /// Features in the sampled active tiles, the base the other counts are a share of.
    pub(crate) active_ids: u64,
    pub(crate) missing: Tally,
    pub(crate) extra_inside: ExtraInside,
    pub(crate) changed: Tally,
    pub(crate) type_changed: Tally,
    pub(crate) extra_in_buffer: u64,
    pub(crate) regions: BTreeMap<String, RegionTally>,
    /// The contract bounds this outcome was judged by, kept with it as evidence.
    pub(crate) tiny_max_area_tile_units: f64,
    pub(crate) additions_max_ratio: f64,
}

impl Equivalence {
    /// The share of the sampled active ids that additions reached.
    pub(crate) fn additions_ratio(&self) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let (additions, active) = (
            self.extra_inside.addition.count as f64,
            self.active_ids as f64,
        );
        if self.active_ids == 0 {
            if self.extra_inside.addition.count == 0 {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            additions / active
        }
    }

    /// ADR-0135 §1–2: nothing missing, nothing changed, nothing unexplained, and additions no more
    /// than their contract share. Buffer and tiny extras are allowed and only counted.
    pub(crate) fn passed(&self) -> bool {
        self.missing.count == 0
            && self.changed.count == 0
            && self.extra_inside.unexplained.count == 0
            && self.additions_ratio() <= self.additions_max_ratio
    }

    /// The refusal, short enough for a build's failure reason: every count, and the ids of what
    /// nothing explains. The whole evidence is written next to the work directory.
    pub(crate) fn refusal(&self) -> Value {
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
        })
    }
}

/// How the comparison reads tiles.
#[derive(Debug, Clone)]
pub(crate) struct Compare<'a> {
    pub(crate) layer: &'a str,
    pub(crate) id_property: &'a str,
    pub(crate) max_zoom: u8,
    pub(crate) tiny_max_area_tile_units: f64,
    pub(crate) region_chars: Option<usize>,
    pub(crate) limit: usize,
}

impl Compare<'_> {
    fn region<'id>(&self, id: &'id str) -> Option<&'id str> {
        let chars = self.region_chars?;
        Some(id.char_indices().nth(chars).map_or(id, |(at, _)| &id[..at]))
    }
}

/// The ids of a tile's layer, for the addition check's lookup of active maxzoom tiles.
pub(crate) fn tile_ids(
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
pub(crate) fn compare_tile(
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
            outcome.missing.add(at(id), limit);
            passed = false;
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
            if active_maxzoom(under)?.contains(id) {
                passed = false;
                &mut outcome.extra_inside.unexplained
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
pub(crate) fn compare_archives<A: Read + Seek, B: Read + Seek>(
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
pub(crate) enum ActiveOrigin {
    /// Baked by a lakehouse bake: the maxzoom id gate already compares like with like.
    LakehouseBake,
    /// Built any other way (PostGIS through `martin-cp` today): the first lakehouse bake is compared.
    OtherOven,
}

/// The evidence for an active release the gate does not compare.
pub(crate) fn skipped(origin: ActiveOrigin) -> Option<Value> {
    (origin == ActiveOrigin::LakehouseBake).then(|| {
        serde_json::json!({
            "skipped": "the active release is a lakehouse_bake; the maxzoom id gate compares it"
        })
    })
}

/// Runs the whole gate over an active and a new archive: densest tiles from the active directory,
/// the samples, the comparison. Returns the outcome; the caller refuses when it did not pass.
pub(crate) fn first_release_gate<A: Read + Seek, B: Read + Seek>(
    active: A,
    new: B,
    contract: &SampleContract,
    (layer, id_property): (&str, &str),
    (min_zoom, max_zoom): (u8, u8),
    collector: &SampleCollector,
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
        },
        collector.regions.keys().cloned(),
    )?;
    outcome.tiny_max_area_tile_units = contract.tiny_max_area_tile_units;
    outcome.additions_max_ratio = contract.additions_max_ratio;
    Ok(outcome)
}

/// The gate's verdict: the evidence when it passed, a refusal naming the counts when it did not.
/// `evidence_file` names where the caller kept the whole evidence.
pub(crate) fn verdict(outcome: &Equivalence, evidence_file: &str) -> anyhow::Result<Value> {
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
