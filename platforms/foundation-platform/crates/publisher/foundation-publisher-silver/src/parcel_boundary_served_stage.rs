//! The served parcel boundaries (root ADR-0133 §2) in `DuckDB` inside the publisher (root ADR-0559).
//!
//! One cadastral edition of `silver.parcel_boundaries` with every ledgered `parcels` edit applied, and
//! the fold of the edits after the served build (root ADR-0375 §2).
//!
//! It was the Spark jobs `parcel_boundary_served_gold.py` and `parcel_boundary_fold.py`. This module
//! holds what does not touch the catalog: the ledger's checks, each edited PNU's final state, the
//! fold's neighbourhood (the same tiles the Python computed, in `f64`), the bake handoff's line, and
//! the SQL that builds one 시도 of the served rows into `built`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use anyhow::{bail, ensure, Context};
use duckdb::Connection;
use sha2::{Digest, Sha256};

/// The map edit ledger's unit of the parcels.
pub const UNIT: &str = "parcels";
/// The parcels' one CRS (the Silver and served contracts declare it).
pub const GEOMETRY_SRID: i32 = 4326;
/// The parcel layer's minzoom (`r2-connections.contract.json` vector tile layer `parcels`).
pub const NEIGHBOURHOOD_ZOOM: u8 = 14;
/// Past this many z14 tiles (about 2.4 km each) a fold hands every row.
pub const MAX_FOLD_TILES: usize = 512;
/// One pixel more than the bake's patch buffer, so float boxes cover the bake's f32 ones
/// (`land_use_zone_area_by_tile.MEMBERSHIP_MARGIN_PIXELS`).
const MEMBERSHIP_MARGIN_PIXELS: f64 = 1.0;
/// Web Mercator's latitude bound.
const MAX_LATITUDE: f64 = 85.051_128_779_806_59;
const TILE_CONTRACT: &str = include_str!("../../../../config/tile-bake-containers.contract.json");
const WKB_POLYGON: u32 = 3;
const WKB_MULTIPOLYGON: u32 = 6;
const EWKB_SRID_FLAG: u32 = 0x2000_0000;

/// The pixels (of a tile's 256) a box is widened by: the bake's patch buffer plus one.
///
/// # Errors
/// The tile contract holds no positive `patch_buffer_pixels`.
pub fn membership_buffer_pixels() -> anyhow::Result<f64> {
    let contract: serde_json::Value = serde_json::from_str(TILE_CONTRACT)?;
    let value = contract["patch_buffer_pixels"]
        .as_f64()
        .filter(|value| *value > 0.0)
        .context("patch_buffer_pixels is a positive number of pixels")?;
    Ok(value + MEMBERSHIP_MARGIN_PIXELS)
}

/// What an edit does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Op {
    /// The parcel is served with the edit's geometry.
    Upsert,
    /// The parcel is not served.
    Delete,
}

/// One ledgered `parcels` edit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerEdit {
    /// Its place in the ledger's order.
    pub change_seq: i64,
    /// The PNU.
    pub feature_id: String,
    /// Upsert or delete.
    pub op: Op,
    /// Its CRS.
    pub geometry_srid: i32,
    /// The geometry an upsert puts.
    pub geometry_wkb: Option<Vec<u8>>,
    /// Its digest.
    pub geometry_checksum_sha256: Option<String>,
}

impl LedgerEdit {
    /// The served `source_snapshot_id` of a parcel this edit puts.
    #[must_use]
    pub fn source_snapshot_id(&self) -> String {
        format!("map-edit-{}", self.change_seq)
    }
}

fn is_pnu(value: &str) -> bool {
    value.len() == 19 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The unit's ledgered edits in change order, refusing any the served table could not hold
/// (`parcel_boundary_served_gold.check_ledger`).
///
/// # Errors
/// A repeated or non-positive `change_seq`, another CRS, a feature id that is not a PNU, or an upsert
/// without its geometry or checksum.
pub fn check_ledger(mut ledger: Vec<LedgerEdit>) -> anyhow::Result<Vec<LedgerEdit>> {
    let mut seen = BTreeSet::new();
    for edit in &ledger {
        let seq = edit.change_seq;
        ensure!(
            seq > 0 && seen.insert(seq),
            "the parcels ledger repeats or lacks change_seq {seq}"
        );
        ensure!(
            edit.geometry_srid == GEOMETRY_SRID,
            "edit {seq} is EPSG:{}, the parcels are EPSG:{GEOMETRY_SRID}",
            edit.geometry_srid
        );
        ensure!(
            is_pnu(&edit.feature_id),
            "edit {seq} names {:?}, which is not a PNU",
            edit.feature_id
        );
        if edit.op == Op::Upsert {
            ensure!(
                edit.geometry_wkb
                    .as_ref()
                    .is_some_and(|wkb| !wkb.is_empty()),
                "edit {seq} upserts a parcel without its geometry"
            );
            ensure!(
                edit.geometry_checksum_sha256
                    .as_deref()
                    .is_some_and(is_sha256_hex),
                "edit {seq} upserts a parcel without its geometry checksum"
            );
        }
    }
    ledger.sort_by_key(|edit| edit.change_seq);
    Ok(ledger)
}

/// What the edits did, as the summary counts them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct EditCounts {
    /// Upserts.
    pub upserts: u64,
    /// Deletes.
    pub deletes: u64,
    /// Deletes of a parcel not there at that point in the order: counted, never passed over.
    pub deletes_of_absent_features: u64,
}

/// Each edited PNU's final state (its last upsert, or `None` when deleted) and what the edits did
/// (`parcel_boundary_served_gold.fold_edits`). `present` is which edited PNUs the edition holds.
#[must_use]
pub fn fold_edits<'a>(
    ledger: &'a [LedgerEdit],
    present: &BTreeSet<String>,
) -> (BTreeMap<String, Option<&'a LedgerEdit>>, EditCounts) {
    let mut last: BTreeMap<String, Option<&LedgerEdit>> = BTreeMap::new();
    let mut counts = EditCounts::default();
    for edit in ledger {
        let exists = last
            .get(&edit.feature_id)
            .map_or_else(|| present.contains(&edit.feature_id), Option::is_some);
        match edit.op {
            Op::Delete => {
                counts.deletes += 1;
                if !exists {
                    counts.deletes_of_absent_features += 1;
                }
                last.insert(edit.feature_id.clone(), None);
            }
            Op::Upsert => {
                counts.upserts += 1;
                last.insert(edit.feature_id.clone(), Some(edit));
            }
        }
    }
    (last, counts)
}

/// A longitude/latitude box: (min x, min y, max x, max y).
pub type BoundingBox = (f64, f64, f64, f64);

/// Reads a polygon's points out of its WKB.
struct Reader<'b> {
    bytes: &'b [u8],
    at: usize,
    points: Vec<(f64, f64)>,
}
impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> anyhow::Result<[u8; N]> {
        let end = self.at + N;
        ensure!(end <= self.bytes.len(), "the WKB ends inside a value");
        let mut out = [0; N];
        out.copy_from_slice(&self.bytes[self.at..end]);
        self.at = end;
        Ok(out)
    }
    fn u32(&mut self, little: bool) -> anyhow::Result<u32> {
        let raw = self.take::<4>()?;
        Ok(if little {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        })
    }
    fn f64(&mut self, little: bool) -> anyhow::Result<f64> {
        let raw = self.take::<8>()?;
        Ok(if little {
            f64::from_le_bytes(raw)
        } else {
            f64::from_be_bytes(raw)
        })
    }
    fn geometry(&mut self, multi_allowed: bool) -> anyhow::Result<()> {
        let little = match self.take::<1>()?[0] {
            1 => true,
            0 => false,
            _ => bail!("the WKB has no byte order"),
        };
        let mut kind = self.u32(little)?;
        if kind & EWKB_SRID_FLAG != 0 {
            self.u32(little)?;
            kind &= !EWKB_SRID_FLAG;
        }
        ensure!(
            kind == WKB_POLYGON || (multi_allowed && kind == WKB_MULTIPOLYGON),
            "the WKB is geometry type {kind}, not a 2D polygon"
        );
        if kind == WKB_MULTIPOLYGON {
            for _ in 0..self.u32(little)? {
                self.geometry(false)?;
            }
            return Ok(());
        }
        for _ in 0..self.u32(little)? {
            for _ in 0..self.u32(little)? {
                let x = self.f64(little)?;
                let y = self.f64(little)?;
                self.points.push((x, y));
            }
        }
        Ok(())
    }
}

/// The box of a 2D Polygon or `MultiPolygon` WKB, ISO or EWKB with an SRID
/// (`parcel_boundary_fold.wkb_bbox`).
///
/// # Errors
/// Bytes that are not one such polygon, no point, or a coordinate that is not finite.
pub fn wkb_bbox(wkb: &[u8]) -> anyhow::Result<BoundingBox> {
    let mut reader = Reader {
        bytes: wkb,
        at: 0,
        points: Vec::new(),
    };
    reader.geometry(true)?;
    ensure!(
        reader.at == wkb.len(),
        "the WKB has bytes after its geometry"
    );
    ensure!(!reader.points.is_empty(), "the WKB polygon has no point");
    let fold = |pick: fn(&(f64, f64)) -> f64, min: bool| {
        reader.points.iter().map(pick).fold(
            if min {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            },
            |a, b| {
                if min {
                    a.min(b)
                } else {
                    a.max(b)
                }
            },
        )
    };
    let bbox = (
        fold(|p| p.0, true),
        fold(|p| p.1, true),
        fold(|p| p.0, false),
        fold(|p| p.1, false),
    );
    ensure!(
        <[f64; 4]>::from(bbox).iter().all(|v| v.is_finite()),
        "the WKB has a coordinate that is not a finite number"
    );
    Ok(bbox)
}

// The Python's rounding: a fused multiply-add would move a tile edge by an ulp.
#[allow(clippy::suboptimal_flops)]
fn pixel(lon: f64, lat: f64, zoom: u8) -> (f64, f64) {
    let scale = 256.0 * f64::from(1_u32 << zoom);
    let phi = lat.clamp(-MAX_LATITUDE, MAX_LATITUDE).to_radians();
    (
        (lon + 180.0) / 360.0 * scale,
        (1.0 - (phi.tan() + 1.0 / phi.cos()).ln() / std::f64::consts::PI) / 2.0 * scale,
    )
}

/// The inclusive tile range a box touches with `buffer_pixels`; `None` for an empty box
/// (`land_use_zone_area_by_tile.tile_range`).
#[must_use]
pub fn tile_range(bbox: BoundingBox, zoom: u8, buffer_pixels: f64) -> Option<(u32, u32, u32, u32)> {
    if bbox.0 > bbox.2 || bbox.1 > bbox.3 {
        return None;
    }
    let (left, top) = pixel(bbox.0, bbox.3, zoom);
    let (right, bottom) = pixel(bbox.2, bbox.1, zoom);
    let last = f64::from((1_u32 << zoom) - 1);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let tile = |value: f64| (value / 256.0).floor().clamp(0.0, last) as u32;
    Some((
        tile(left - buffer_pixels),
        tile(top - buffer_pixels),
        tile(right + buffer_pixels),
        tile(bottom + buffer_pixels),
    ))
}

/// Every z14 tile a box touches, widened by the bake's buffer plus one pixel, in order.
#[must_use]
pub fn neighbourhood_tiles(boxes: &[BoundingBox], buffer_pixels: f64) -> Vec<(u32, u32)> {
    let mut tiles = BTreeSet::new();
    for bbox in boxes {
        if let Some((x0, y0, x1, y1)) = tile_range(*bbox, NEIGHBOURHOOD_ZOOM, buffer_pixels) {
            for x in x0..=x1 {
                for y in y0..=y1 {
                    tiles.insert((x, y));
                }
            }
        }
    }
    tiles.into_iter().collect()
}

/// The longitude/latitude rectangle of tile `(x, y)` widened by `pixels` on every side
/// (`parcel_boundary_fold.tile_envelope`).
#[must_use]
// The Python's rounding: a fused multiply-add would move a tile edge by an ulp.
#[allow(clippy::suboptimal_flops)]
pub fn tile_envelope(x: u32, y: u32, zoom: u8, pixels: f64) -> BoundingBox {
    let scale = f64::from(1_u32 << zoom);
    let margin = pixels / 256.0;
    let lon = |tile_x: f64| tile_x / scale * 360.0 - 180.0;
    let lat = |tile_y: f64| {
        (std::f64::consts::PI * (1.0 - 2.0 * tile_y / scale))
            .sinh()
            .atan()
            .to_degrees()
    };
    let (x, y) = (f64::from(x), f64::from(y));
    (
        lon(x - margin),
        lat(y + 1.0 + margin),
        lon(x + 1.0 + margin),
        lat(y - margin),
    )
}

/// Whether `bbox` meets one of the tiles' widened rectangles.
#[must_use]
pub fn touches_any(bbox: BoundingBox, tiles: &[(u32, u32)], buffer_pixels: f64) -> bool {
    tiles.iter().any(|&(x, y)| {
        let (left, bottom, right, top) = tile_envelope(x, y, NEIGHBOURHOOD_ZOOM, buffer_pixels);
        bbox.2 >= left && bbox.0 <= right && bbox.3 >= bottom && bbox.1 <= top
    })
}

/// The `DuckDB` filter of [`touches_any`] over Silver's `bbox_*` columns.
#[must_use]
pub fn silver_predicate(tiles: &[(u32, u32)], buffer_pixels: f64) -> String {
    if tiles.is_empty() {
        return "false".to_owned();
    }
    let mut out = String::from("(");
    for (index, &(x, y)) in tiles.iter().enumerate() {
        let (left, bottom, right, top) = tile_envelope(x, y, NEIGHBOURHOOD_ZOOM, buffer_pixels);
        if index > 0 {
            out.push_str(" OR ");
        }
        let _ = write!(
            out,
            "(bbox_max_x >= {left:?} AND bbox_min_x <= {right:?} AND bbox_max_y >= {bottom:?} AND bbox_min_y <= {top:?})"
        );
    }
    out.push(')');
    out
}

/// A fold of the edits after the served build, or why it is not partial.
#[derive(Debug)]
pub enum FoldPlan<'a> {
    /// The changed PNUs and the neighbourhood handed.
    Partial {
        /// Each changed PNU's state after the fold (`None`: not served).
        after: BTreeMap<String, Option<&'a LedgerEdit>>,
        /// The neighbourhood's z14 tiles.
        tiles: Vec<(u32, u32)>,
        /// Every PNU the ledger ever edited: served from its edit, never its Silver row.
        ledger_pnus: Vec<String>,
        /// The edited PNUs whose served box meets the neighbourhood.
        edited_in_neighbourhood: Vec<String>,
    },
    /// The caller builds every row instead (root ADR-0314).
    Full(String),
}

/// The fold of the edits after `through` (`parcel_boundary_fold.plan`). `silver_boxes` is the
/// edition's box of every changed PNU it holds.
///
/// # Errors
/// An edit's geometry that is not a polygon.
pub fn plan<'a>(
    ledger: &'a [LedgerEdit],
    through: i64,
    silver_boxes: &BTreeMap<String, BoundingBox>,
    buffer_pixels: f64,
) -> anyhow::Result<FoldPlan<'a>> {
    let changed: BTreeSet<&str> = ledger
        .iter()
        .filter(|edit| edit.change_seq > through)
        .map(|edit| edit.feature_id.as_str())
        .collect();
    let mut before: BTreeMap<String, Option<&LedgerEdit>> = BTreeMap::new();
    let mut after: BTreeMap<String, Option<&LedgerEdit>> = BTreeMap::new();
    for edit in ledger {
        if !changed.contains(edit.feature_id.as_str()) {
            continue;
        }
        let state = (edit.op == Op::Upsert).then_some(edit);
        if edit.change_seq <= through {
            before.insert(edit.feature_id.clone(), state);
        }
        after.insert(edit.feature_id.clone(), state);
    }
    let edit_box = |edit: Option<&LedgerEdit>| -> anyhow::Result<Option<BoundingBox>> {
        edit.map(|edit| wkb_bbox(edit.geometry_wkb.as_deref().unwrap_or_default()))
            .transpose()
    };
    let mut boxes = Vec::new();
    for (pnu, state) in &after {
        let old = match before.get(pnu) {
            Some(earlier) => edit_box(*earlier)?,
            None => silver_boxes.get(pnu).copied(),
        };
        boxes.extend(old);
        boxes.extend(edit_box(*state)?);
    }
    let tiles = neighbourhood_tiles(&boxes, buffer_pixels);
    if tiles.len() > MAX_FOLD_TILES {
        return Ok(FoldPlan::Full(format!(
            "the fold touches {} z{NEIGHBOURHOOD_ZOOM} tiles, past {MAX_FOLD_TILES}",
            tiles.len()
        )));
    }
    let mut served_edits: BTreeMap<&str, Option<&LedgerEdit>> = BTreeMap::new();
    for edit in ledger {
        served_edits.insert(
            edit.feature_id.as_str(),
            (edit.op == Op::Upsert).then_some(edit),
        );
    }
    let mut edited_in_neighbourhood = Vec::new();
    for (pnu, state) in &served_edits {
        if let Some(bbox) = edit_box(*state)? {
            if touches_any(bbox, &tiles, buffer_pixels) {
                edited_in_neighbourhood.push((*pnu).to_owned());
            }
        }
    }
    Ok(FoldPlan::Partial {
        after,
        tiles,
        ledger_pnus: served_edits.keys().map(|pnu| (*pnu).to_owned()).collect(),
        edited_in_neighbourhood,
    })
}

/// One bake handoff line without its newline, byte for byte as `served_gold_common.bake_handoff_line`
/// writes it for parcels: sorted keys, no spaces, the WKB as lowercase hex, no properties.
#[must_use]
pub fn handoff_line(feature_id: &str, geometry_wkb: &[u8], origin: &str) -> String {
    let mut hex = String::with_capacity(geometry_wkb.len() * 2);
    for byte in geometry_wkb {
        let _ = write!(hex, "{byte:02x}");
    }
    format!(
        "{{\"feature_id\":{},\"geometry_srid\":{GEOMETRY_SRID},\"geometry_wkb_hex\":\"{hex}\",\"origin\":{},\"properties\":{{}}}}",
        serde_json::Value::from(feature_id),
        serde_json::Value::from(origin),
    )
}

/// A served row's content digest: the PNU, the geometry's checksum, the CRS and the origin, not the
/// edition or the stamps, so a new edition appends only the parcels whose geometry moved.
#[must_use]
pub fn row_digest(pnu: &str, geometry_checksum_sha256: &str, origin: &str) -> String {
    let mut digest = Sha256::new();
    for part in [
        pnu,
        geometry_checksum_sha256,
        &GEOMETRY_SRID.to_string(),
        origin,
    ] {
        digest.update(part.as_bytes());
        digest.update([0x1f]);
    }
    format!("{:x}", digest.finalize())
}

/// The `DuckDB` expression of [`row_digest`] over a row's columns.
pub const ROW_DIGEST_SQL: &str = "sha256(pnu || chr(31) || geometry_checksum_sha256 || chr(31) || \
     CAST(geometry_srid AS VARCHAR) || chr(31) || origin || chr(31))";

/// Loads each edited PNU's final upsert into `served_edits`, and the edited PNUs into
/// `edited_pnus`, for [`build_sido_sql`].
///
/// # Errors
/// `DuckDB` refuses a row.
pub fn load_edits(
    connection: &Connection,
    final_states: &BTreeMap<String, Option<&LedgerEdit>>,
) -> anyhow::Result<()> {
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE edited_pnus (pnu VARCHAR NOT NULL);
         CREATE OR REPLACE TEMP TABLE served_edits (pnu VARCHAR NOT NULL, geometry_wkb BLOB NOT NULL,
           geometry_checksum_sha256 VARCHAR NOT NULL, source_snapshot_id VARCHAR NOT NULL);",
    )?;
    {
        let mut edited = connection.appender("edited_pnus")?;
        for pnu in final_states.keys() {
            edited.append_row([pnu.as_str()])?;
        }
        edited.flush()?;
    }
    let mut served = connection.appender("served_edits")?;
    for (pnu, state) in final_states {
        if let Some(edit) = state {
            served.append_row(duckdb::params![
                pnu,
                edit.geometry_wkb.as_deref().unwrap_or_default(),
                edit.geometry_checksum_sha256.as_deref().unwrap_or_default(),
                edit.source_snapshot_id(),
            ])?;
        }
    }
    served.flush()?;
    Ok(())
}

/// The served rows of one 시도 into `built` (created by the first).
///
/// The edition's rows of the 시도 but the edited PNUs, and each edited PNU's final upsert. `edition` is a `SELECT` of the edition's
/// rows (`pnu`, `geometry_wkb`, `geometry_srid`, `geometry_checksum_sha256`, `source_snapshot_id`);
/// `sido_range` is the 시도's key range as a predicate on `pnu` (`(pnu >= '41' AND pnu < '42')`, the
/// publisher's `sido_range`), so the read of a table ordered by PNU skips the files outside it.
///
/// # Errors
/// A stamp that holds a quote.
pub fn build_sido_sql(
    edition: &str,
    sido_range: &str,
    create: bool,
    silver_snapshot: &str,
    through: i64,
    published_at_utc: &str,
) -> anyhow::Result<String> {
    ensure!(
        !silver_snapshot.contains('\'') && !published_at_utc.contains('\''),
        "a stamp holds a quote"
    );
    let into = if create {
        "CREATE OR REPLACE TABLE built AS"
    } else {
        "INSERT INTO built"
    };
    Ok(format!(
        "{into} SELECT pnu, geometry_wkb, geometry_srid, geometry_checksum_sha256, origin, source_snapshot_id, \
                '{silver_snapshot}' AS silver_iceberg_snapshot_id, CAST({through} AS BIGINT) AS edits_through_change_seq, \
                CAST('{published_at_utc}' AS TIMESTAMP) AS published_at_utc, {ROW_DIGEST_SQL} AS row_digest \
         FROM (SELECT pnu, geometry_wkb, CAST({GEOMETRY_SRID} AS INTEGER) AS geometry_srid, geometry_checksum_sha256, \
                      'source' AS origin, CAST(source_snapshot_id AS VARCHAR) AS source_snapshot_id \
               FROM ({edition}) e WHERE {sido_range} AND pnu NOT IN (SELECT pnu FROM edited_pnus) \
               UNION ALL \
               SELECT pnu, geometry_wkb, CAST({GEOMETRY_SRID} AS INTEGER), geometry_checksum_sha256, 'edit', source_snapshot_id \
               FROM served_edits WHERE {sido_range})"
    ))
}

/// The served rows of a fold into `built`: each changed PNU's final upsert (a deleted one has no
/// row, so the diff over the changed PNUs marks it removed). `changed` is a SQL list of quoted PNUs.
///
/// # Errors
/// A stamp that holds a quote.
pub fn build_fold_sql(
    changed: &str,
    silver_snapshot: &str,
    through: i64,
    published_at_utc: &str,
) -> anyhow::Result<String> {
    ensure!(
        !silver_snapshot.contains('\'') && !published_at_utc.contains('\''),
        "a stamp holds a quote"
    );
    Ok(format!(
        "CREATE OR REPLACE TABLE built AS SELECT pnu, geometry_wkb, geometry_srid, geometry_checksum_sha256, origin, \
                source_snapshot_id, '{silver_snapshot}' AS silver_iceberg_snapshot_id, \
                CAST({through} AS BIGINT) AS edits_through_change_seq, \
                CAST('{published_at_utc}' AS TIMESTAMP) AS published_at_utc, {ROW_DIGEST_SQL} AS row_digest \
         FROM (SELECT pnu, geometry_wkb, CAST({GEOMETRY_SRID} AS INTEGER) AS geometry_srid, geometry_checksum_sha256, \
                      'edit' AS origin, source_snapshot_id FROM served_edits WHERE pnu IN ({changed}))"
    ))
}

/// One line of a fold's change list, as `json.dumps(line, sort_keys=True)` writes it.
#[must_use]
pub fn change_list_line(pnu: &str, upsert: bool) -> String {
    format!(
        "{{\"feature_id\": {}, \"op\": \"{}\"}}",
        serde_json::Value::from(pnu),
        if upsert { "upsert" } else { "delete" }
    )
}

/// The rules this module builds by; a commit records it, so a change of them rebuilds every 시도.
const RULES_VERSION: &str =
    "parcel-boundary-served v1: edition minus edited PNUs plus final upserts";

/// The build's code version (`foundation.build-source-sha256`).
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    for text in [RULES_VERSION, ROW_DIGEST_SQL] {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// One line of the edit store's handoff for a unit (`export-map-edit-handoff`), as the ledger keeps it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct HandoffEdit {
    /// The unit.
    pub unit: String,
    /// Its place in the order.
    pub change_seq: i64,
    /// The PNU.
    pub feature_id: String,
    /// `upsert` or `delete`.
    pub op: String,
    /// The geometry as `GeoJSON`, on an upsert.
    pub geometry_geojson: Option<String>,
    /// The geometry as hex WKB, on an upsert.
    pub geometry_wkb_hex: Option<String>,
    /// Its CRS.
    pub geometry_srid: i32,
    /// Its digest, on an upsert.
    pub geometry_checksum_sha256: Option<String>,
    /// The edit's properties, a JSON object.
    pub properties_json: String,
    /// Who edited.
    pub editor: String,
    /// When.
    pub edited_at: String,
}

fn present(value: Option<&String>) -> bool {
    value.is_some_and(|value| !value.is_empty())
}

/// Parses the handoff for the parcels unit, refusing what the ledger contract would refuse
/// (`served_gold_common.read_edit_handoff`), in change order.
///
/// # Errors
/// A line that is not an edit of the parcels unit in EPSG:4326, a repeated or non-positive
/// `change_seq`, an upsert without its geometry or a delete with one, properties that are not an
/// object.
pub fn read_edit_handoff(text: &str) -> anyhow::Result<Vec<HandoffEdit>> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for (number, line) in text.split('\n').enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffEdit = serde_json::from_str(line)
            .with_context(|| format!("edit handoff line {} is not an edit", number + 1))?;
        ensure!(
            row.unit == UNIT,
            "edit handoff line {} is for unit {}, not {UNIT}",
            number + 1,
            row.unit
        );
        ensure!(
            row.geometry_srid == GEOMETRY_SRID,
            "edit handoff line {} is not in EPSG:{GEOMETRY_SRID}",
            number + 1
        );
        let seq = row.change_seq;
        ensure!(
            seq > 0 && seen.insert(seq),
            "edit handoff line {} repeats or lacks a change_seq",
            number + 1
        );
        ensure!(
            row.op == "upsert" || row.op == "delete",
            "edit {seq} has op {}",
            row.op
        );
        let carries = [
            present(row.geometry_wkb_hex.as_ref()),
            present(row.geometry_geojson.as_ref()),
            present(row.geometry_checksum_sha256.as_ref()),
        ];
        let upsert = row.op == "upsert";
        ensure!(
            if upsert {
                carries.iter().all(|carried| *carried)
            } else {
                !carries.iter().any(|carried| *carried)
            },
            "edit {seq}: an upsert carries its geometry, a delete carries none"
        );
        let properties: serde_json::Value = serde_json::from_str(&row.properties_json)
            .with_context(|| format!("edit {seq} properties are not JSON"))?;
        ensure!(
            properties.is_object(),
            "edit {seq} properties are not an object"
        );
        rows.push(row);
    }
    rows.sort_by_key(|row| row.change_seq);
    Ok(rows)
}

/// What makes two ledger rows the same edit, whichever export carried them
/// (`served_gold_common.edit_fingerprint`).
pub type EditFingerprint = (i64, String, String, Option<String>, String, String, String);

impl HandoffEdit {
    /// Its fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> EditFingerprint {
        (
            self.change_seq,
            self.feature_id.clone(),
            self.op.clone(),
            self.geometry_checksum_sha256
                .clone()
                .filter(|sum| !sum.is_empty()),
            self.properties_json.clone(),
            self.editor.clone(),
            self.edited_at.clone(),
        )
    }

    /// As the ledger's check reads it.
    ///
    /// # Errors
    /// A WKB that is not hex.
    pub fn ledger_edit(&self) -> anyhow::Result<LedgerEdit> {
        let geometry_wkb = match self
            .geometry_wkb_hex
            .as_deref()
            .filter(|hex| !hex.is_empty())
        {
            Some(hex) => Some(
                hex_bytes(hex)
                    .with_context(|| format!("edit {} geometry is not hex WKB", self.change_seq))?,
            ),
            None => None,
        };
        Ok(LedgerEdit {
            change_seq: self.change_seq,
            feature_id: self.feature_id.clone(),
            op: if self.op == "upsert" {
                Op::Upsert
            } else {
                Op::Delete
            },
            geometry_srid: self.geometry_srid,
            geometry_wkb,
            geometry_checksum_sha256: self.geometry_checksum_sha256.clone(),
        })
    }
}

fn hex_bytes(hex: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(hex.len().is_multiple_of(2), "odd length");
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).context("not hex"))
        .collect()
}

/// The handoff's edits the ledger does not hold yet; the same `change_seq` with other content is
/// refused, the ledger being append-only (`served_gold_common.new_ledger_rows`).
///
/// # Errors
/// An edit already ledgered with different content.
pub fn fresh_edits<'a>(
    ledgered: &BTreeMap<i64, EditFingerprint>,
    handoff: &'a [HandoffEdit],
) -> anyhow::Result<Vec<&'a HandoffEdit>> {
    let mut fresh = Vec::new();
    for row in handoff {
        match ledgered.get(&row.change_seq) {
            None => fresh.push(row),
            Some(known) => ensure!(
                *known == row.fingerprint(),
                "edit {} is already ledgered with different content; the ledger is append-only",
                row.change_seq
            ),
        }
    }
    Ok(fresh)
}

#[cfg(test)]
mod tests;
