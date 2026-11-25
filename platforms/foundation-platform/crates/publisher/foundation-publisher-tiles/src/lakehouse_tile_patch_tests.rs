use std::collections::BTreeSet;
use std::io::Cursor;

use serde_json::json;

use super::super::pmtiles_feature_ids::tests::{archive, tile, written};
use super::super::pmtiles_feature_ids::{
    for_each_entry, open, open_seekable, stored, tile_id, zoom_range,
};
use super::super::prepare_handoff;
use super::*;

// Synthetic only: reserved-band PNUs (99999…) and coordinates near 10°E 10°N, no real place.
const A: &str = "9999900000000000001";
const B: &str = "9999900000000000002";
const C: &str = "9999900000000000003";
const D: &str = "9999900000000000004";

pub(crate) fn square(lon: f64, lat: f64, side: f64) -> String {
    let mut wkb = vec![1_u8];
    wkb.extend(3_u32.to_le_bytes());
    wkb.extend(1_u32.to_le_bytes());
    wkb.extend(5_u32.to_le_bytes());
    for (x, y) in [
        (0.0, 0.0),
        (side, 0.0),
        (side, side),
        (0.0, side),
        (0.0, 0.0),
    ] {
        wkb.extend((lon + x).to_le_bytes());
        wkb.extend((lat + y).to_le_bytes());
    }
    wkb.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn row(id: &str, wkb: &str) -> String {
    json!({"feature_id": id, "properties": {}, "geometry_wkb_hex": wkb, "geometry_srid": 4326,
        "origin": "source"})
    .to_string()
}

fn summary(rows: usize) -> ServedSummary {
    serde_json::from_value(json!({
        "schema_version": "foundation-platform.polygon_served_gold.v1",
        "unit": "parcels",
        "feature_id_property": "pnu",
        "geometry_srid": 4326,
        "canonical_iceberg_snapshot_id": "2",
        "edits_through_change_seq": 0,
        "served_row_count": rows,
        "status": "ready",
    }))
    .unwrap_or_else(|error| panic!("summary fixture: {error}"))
}

fn indexed(rows: &[String]) -> anyhow::Result<Vec<Record>> {
    let text: String = rows.iter().map(|line| format!("{line}\n")).collect();
    let file = written(text.as_bytes())?;
    Ok(build_index(&summary(rows.len()), file.path())?.0)
}

fn layer() -> ServedLayer {
    ServedLayer {
        source_layer: "parcels".to_owned(),
        feature_id_property: "pnu".to_owned(),
        tile_min_zoom: 14,
        tile_max_zoom: 16,
        render_max_zoom: 22,
        properties: vec!["pnu".to_owned()],
    }
}

/// The z16 tile holding (10°E, 10°N) and a lon/lat inside it, well away from its edges.
fn z16_tile() -> (u32, u32) {
    tile_range([10.0, 10.0, 10.0, 10.0], 16)
        .map(|(x, y, _, _)| (x, y))
        .unwrap_or_default()
}

#[test]
fn the_contract_patches_parcels_and_derives_keys_beside_the_base() -> anyhow::Result<()> {
    let policy = PatchPolicy::load()?;
    assert!(policy.covers("parcels"));
    // Root ADR-0373: regulation zones patch the tiles a changed zone touches.
    assert!(policy.covers("zoning"));
    assert!(!policy.covers("complex"));
    let base = "gold/vector-tiles/releases/parcels-00000000-0000-7000-8000-000000000001.pmtiles";
    assert_eq!(
        policy.manifest_key(base)?,
        "gold/vector-tiles/releases/parcels-00000000-0000-7000-8000-000000000001.patches.json"
    );
    assert_eq!(
        policy.patch_key(base, 3)?,
        "gold/vector-tiles/releases/parcels-00000000-0000-7000-8000-000000000001.patch-3.pmtiles"
    );
    assert!(policy.patch_key(base, 0).is_err());
    assert!(policy.patch_key("gold/x.patch-1.pmtiles", 2).is_err());
    let mut contract: serde_json::Value = serde_json::from_str(CONTRACT_JSON)?;
    contract["vector_tile_gateway"]["patches"]["max_patched_tile_ratio"] = json!(0.9);
    assert!(PatchPolicy::parse(&contract.to_string()).is_err());
    Ok(())
}

#[test]
fn a_written_patch_reads_back_its_tiles_its_base_header_and_its_metadata_with_leaf_directories(
) -> anyhow::Result<()> {
    // Enough tiles that the root no longer fits the first 16 KiB, so leaves are written and walked.
    let (first, _) = zoom_range(14);
    // Pseudo-random gaps and sizes (an LCG), so the directory cannot compress into the root.
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        1 + (state >> 33) % bound
    };
    let mut id = first;
    let tiles: Vec<(u64, Vec<u8>)> = (0..40_000)
        .map(|n: u32| {
            id += next(5_000);
            // Distinct bytes, so no two tiles share their content.
            let mut bytes = n.to_le_bytes().to_vec();
            bytes.resize(4 + next(400) as usize, 7);
            (id, bytes)
        })
        .collect();
    let file = tempfile::NamedTempFile::new()?;
    let base = base(14, 16);
    let metadata = r#"{"vector_layers":[{"id":"parcels"}]}"#;
    write_patch(file.path(), &base, Some(metadata), &tiles)?;
    let reader = open(file.path())?;
    let header = reader.get_header();
    assert!(header.leaf_length() > 0, "leaf directories were written");
    assert_eq!(
        BaseHeader {
            addressed_tiles: 1_000,
            ..BaseHeader::of(header)
        },
        base,
        "the base's zooms, bounds and centre"
    );
    assert_eq!(BaseHeader::of(header).addressed_tiles, 40_000);
    assert_eq!(reader.get_metadata()?, metadata);
    for (id, bytes) in [&tiles[0], &tiles[17_777], &tiles[39_999]] {
        assert_eq!(
            stored(&reader, *id)?.as_deref(),
            Some(bytes.as_slice()),
            "tile {id}"
        );
    }
    assert!(stored(&reader, first)?.is_none());
    let mut walked = Vec::new();
    for_each_entry(&reader, (0, u64::MAX), |entry| {
        walked.push(entry.tile_id());
        Ok(())
    })?;
    assert_eq!(
        walked,
        tiles.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        "every tile, in tile-id order"
    );
    Ok(())
}

#[test]
fn the_patch_writer_refuses_unordered_or_empty_tiles_and_writes_tombstones_as_tiles(
) -> anyhow::Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    let base = base(14, 16);
    assert!(write_patch(file.path(), &base, None, &[(2, vec![1]), (1, vec![1])]).is_err());
    assert!(write_patch(file.path(), &base, None, &[(1, Vec::new())]).is_err());
    // Repeated tombstones are stored once and read back at every id.
    let (first, _) = zoom_range(14);
    let tombstone = empty_tile()?;
    let tiles = [
        (first, tombstone.clone()),
        (first + 1, tombstone.clone()),
        (first + 7, tombstone.clone()),
    ];
    write_patch(file.path(), &base, None, &tiles)?;
    let reader = open(file.path())?;
    assert_eq!(reader.get_metadata()?, "{}");
    for (id, bytes) in &tiles {
        assert_eq!(stored(&reader, *id)?.as_deref(), Some(bytes.as_slice()));
    }
    Ok(())
}

#[test]
fn a_box_is_widened_outward_and_an_unreadable_polygon_is_refused() -> anyhow::Result<()> {
    let bbox = wkb_bbox(&hex(&square(10.000_000_123, 10.000_000_456, 0.001))?)?;
    assert!(f64::from(bbox[0]) <= 10.000_000_123 && f64::from(bbox[1]) <= 10.000_000_456);
    assert!(f64::from(bbox[2]) >= 10.001_000_123 && f64::from(bbox[3]) >= 10.001_000_456);
    assert!(wkb_bbox(&hex("0106000000")?).is_err());
    Ok(())
}

#[test]
fn the_index_round_trips_sorted_and_is_kept_create_only() -> anyhow::Result<()> {
    let records = indexed(&[
        row(B, &square(10.0, 10.0, 0.001)),
        row(A, &square(10.0, 10.0, 0.001)),
    ])?;
    assert!(records[0].id < records[1].id);
    assert_ne!(
        records[0].content, records[1].content,
        "the id is part of the content"
    );
    let dir = tempfile::tempdir()?;
    let pending = dir.path().join("pending.idx");
    write_index(&pending, &records)?;
    assert_eq!(read_index(&pending)?, records);
    let kept = index_path(dir.path(), "parcels", "123", "inputs")?;
    keep_index(&pending, &kept)?;
    keep_index(&pending, &kept)?;
    assert_eq!(read_index(&kept)?, records);
    assert_ne!(
        kept,
        index_path(dir.path(), "parcels", "123", "other inputs")?
    );
    assert!(index_path(dir.path(), "parcels", "../x", "inputs").is_err());
    assert!(indexed(&[
        row(A, &square(10.0, 10.0, 0.001)),
        row(A, &square(10.0, 10.0, 0.001))
    ])
    .is_err());
    Ok(())
}

#[test]
fn the_diff_names_added_changed_and_removed_features_with_both_boxes() -> anyhow::Result<()> {
    let old = indexed(&[
        row(A, &square(10.0, 10.0, 0.001)),
        row(B, &square(10.0, 10.0, 0.001)),
        row(C, &square(10.0, 10.0, 0.001)),
    ])?;
    let new = indexed(&[
        row(A, &square(10.0, 10.0, 0.001)),
        row(B, &square(10.01, 10.0, 0.001)),
        row(D, &square(10.0, 10.0, 0.001)),
    ])?;
    let changes = diff(&old, &new);
    let mut upserted = changes.upserted.clone();
    upserted.sort_unstable();
    let mut expected = vec![id_hash(B), id_hash(D)];
    expected.sort_unstable();
    assert_eq!(upserted, expected);
    assert_eq!(changes.deleted, vec![id_hash(C)]);
    // B's old and new box, C's old box, D's new box.
    assert_eq!(changes.boxes.len(), 4);
    assert!(diff(&new, &new).is_empty());
    Ok(())
}

#[test]
fn a_box_inside_one_tile_dirties_one_tile_per_zoom_and_the_buffer_reaches_a_neighbour() {
    let (x, y) = z16_tile();
    let middle = tile_range([10.0, 10.0, 10.0, 10.0], 16);
    assert_eq!(middle, Some((x, y, x, y)));
    // The z16 tile's west edge: a box a hair inside it touches the western neighbour's buffer.
    let west = (f64::from(x) / f64::from(1_u32 << 16)).mul_add(360.0, -180.0);
    let edge = [west as f32 + 1e-6, 10.0, west as f32 + 2e-6, 10.0];
    assert_eq!(tile_range(edge, 16).map(|range| range.0), Some(x - 1));
    let dirty = dirty_tiles(&[[10.0, 10.0, 10.0, 10.0]], (14, 16), 100)
        .unwrap()
        .unwrap_or_else(|| panic!("within the limit"));
    assert_eq!(dirty.len(), 3);
    assert!(dirty.contains(&tile_id(16, x, y).unwrap()));
    assert!(dirty_tiles(&[[10.0, 10.0, 10.0, 10.0]], (14, 16), 2)
        .unwrap()
        .is_none());
    assert!(dirty_tiles(&[EMPTY_BBOX], (14, 16), 0)
        .unwrap()
        .is_some_and(|tiles| tiles.is_empty()));
}

#[test]
fn the_neighbourhood_is_every_row_touching_a_dirty_tiles_minzoom_ancestor() -> anyhow::Result<()> {
    let (x, y) = z16_tile();
    let dirty = BTreeSet::from([tile_id(16, x, y)?]);
    let tiles = neighbourhood(&dirty, 14)?;
    assert_eq!(tiles, HashSet::from([(x >> 2, y >> 2)]));
    let near = [
        row(A, &square(10.0, 10.0, 0.000_1)),
        row(B, &square(10.002, 10.002, 0.000_1)),
    ];
    let far = row(C, &square(11.0, 11.0, 0.000_1));
    let lines = [near[0].clone(), far.clone(), near[1].clone()];
    let dir = tempfile::tempdir()?;
    let index = dir.path().join("new.idx");
    write_index(&index, &indexed(&lines)?)?;
    let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
    let file = written(text.as_bytes())?;
    let csv = NeighbourhoodCsv {
        inner: Vec::new(),
        ids: neighbourhood_ids(&index, 14, &tiles)?,
        kept: 0,
    };
    let prepared = prepare_handoff(&summary(3), file.path(), csv)?;
    assert_eq!(prepared.csv.kept, 2);
    let written_csv = String::from_utf8(prepared.csv.inner)?;
    assert!(written_csv.contains(A) && written_csv.contains(B) && !written_csv.contains(C));
    assert_eq!(
        prepared.ids.len(),
        3,
        "the gate still knows every served id"
    );
    Ok(())
}

fn local(
    bytes: Vec<u8>,
) -> anyhow::Result<Archive<super::super::pmtiles_feature_ids::Seekable<Cursor<Vec<u8>>>>> {
    open_seekable(Cursor::new(bytes))
}

#[test]
fn the_patch_takes_dirty_tiles_whole_tombstones_the_rest_and_keeps_the_previous_generation(
) -> anyhow::Result<()> {
    let (first, _) = zoom_range(14);
    let baked = local(archive(
        14,
        16,
        &[
            (first, tile("parcels", &[A])),
            (first + 9, tile("parcels", &[C])),
        ],
    ))?;
    let previous = local(archive(
        14,
        16,
        &[
            (first, tile("parcels", &[B])),
            (first + 5, tile("parcels", &[D])),
        ],
    ))?;
    // first: rebaked; first + 1: dirty but gone, so a tombstone; first + 5: kept from the
    // previous generation; first + 9: baked but not dirty, so not taken.
    let dirty = BTreeSet::from([first, first + 1]);
    let (tiles, empty) = assemble(Some(&baked), Some(&previous), &dirty)?;
    let ids: Vec<u64> = tiles.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![first, first + 1, first + 5]);
    assert_eq!(tiles[0].1, tile("parcels", &[A]));
    assert_eq!(tiles[1].1, empty_tile()?);
    assert_eq!(tiles[2].1, tile("parcels", &[D]));
    assert_eq!(empty, 1);
    let (only_tombstones, empty) = assemble::<std::fs::File, std::fs::File>(None, None, &dirty)?;
    assert_eq!((only_tombstones.len(), empty), (2, 2));
    Ok(())
}

fn patch_file(tiles: &[(u64, Vec<u8>)]) -> anyhow::Result<tempfile::NamedTempFile> {
    let file = tempfile::NamedTempFile::new()?;
    write_patch(file.path(), &base(14, 16), None, tiles)?;
    Ok(file)
}

/// A base over zooms `min..=max` with bounds and a centre, as a patch repeats them.
fn base(min_zoom: u8, max_zoom: u8) -> BaseHeader {
    BaseHeader {
        min_zoom,
        max_zoom,
        bounds: [124.5, 33.0, 131.0, 38.75],
        center: (127.75, 35.875, min_zoom),
        addressed_tiles: 1_000,
        gzip_tiles: true,
    }
}

fn served(ids: &[&str]) -> ServedIds {
    ServedIds::from_hashes(ids.iter().map(|id| id_hash(id)).collect())
        .unwrap_or_else(|_| panic!("distinct ids"))
}

#[test]
fn the_patch_gate_refuses_a_removed_feature_an_unserved_one_and_an_unexplained_miss(
) -> anyhow::Result<()> {
    let (first, _) = zoom_range(16);
    let columns = vec!["pnu".to_owned(), "kind".to_owned()];
    let changes = Changes {
        upserted: vec![id_hash(A)],
        deleted: vec![id_hash(C)],
        boxes: Vec::new(),
    };
    let none = |_: &[u128]| anyhow::Ok(Missing::default());
    let good = patch_file(&[
        (first, tile("parcels", &[A, B])),
        (first + 1, empty_tile()?),
    ])?;
    let gate = gate_patch(
        good.path(),
        &layer(),
        &columns,
        &served(&[A, B]),
        &changes,
        none,
    )?;
    assert_eq!(gate.maxzoom_tiles, 2);

    let removed = patch_file(&[(first, tile("parcels", &[A, C]))])?;
    assert!(gate_patch(
        removed.path(),
        &layer(),
        &columns,
        &served(&[A, C]),
        &changes,
        none
    )
    .is_err());
    assert!(gate_patch(
        removed.path(),
        &layer(),
        &columns,
        &served(&[A]),
        &changes,
        none
    )
    .is_err());

    let lost = patch_file(&[(first, tile("parcels", &[B]))])?;
    let unexplained = |missing: &[u128]| {
        anyhow::Ok(Missing {
            subpixel: Vec::new(),
            unexplained: vec![format!("{}", missing.len())],
        })
    };
    assert!(gate_patch(
        lost.path(),
        &layer(),
        &columns,
        &served(&[A, B]),
        &changes,
        unexplained
    )
    .is_err());
    let subpixel = |_: &[u128]| {
        anyhow::Ok(Missing {
            subpixel: vec![A.to_owned()],
            unexplained: Vec::new(),
        })
    };
    let allowed = gate_patch(
        lost.path(),
        &layer(),
        &columns,
        &served(&[A, B]),
        &changes,
        subpixel,
    )?;
    assert_eq!(allowed.subpixel, vec![A.to_owned()]);
    Ok(())
}

#[test]
fn a_manifest_for_another_base_or_with_unknown_fields_is_refused() -> anyhow::Result<()> {
    let policy = PatchPolicy::load()?;
    let manifest = Manifest {
        schema_version: policy.manifest_schema_version.clone(),
        base_source_id: "parcels-00000000-0000-7000-8000-000000000001".to_owned(),
        generation: 2,
        gold_snapshot: "7".to_owned(),
        folded_through_change_seq: 0,
        patch: Some(PatchObject {
            generation: 2,
            sha256: "0".repeat(64),
            bytes: 9,
            tiles: 3,
            empty_tiles: 1,
        }),
    };
    let bytes = serde_json::to_vec(&manifest)?;
    assert_eq!(
        Manifest::parse(&bytes, &policy, &manifest.base_source_id)?,
        manifest
    );
    assert!(Manifest::parse(
        &bytes,
        &policy,
        "parcels-00000000-0000-7000-8000-000000000002"
    )
    .is_err());
    let mut extra: serde_json::Value = serde_json::from_slice(&bytes)?;
    extra["unexpected"] = json!(1);
    assert!(Manifest::parse(
        &serde_json::to_vec(&extra)?,
        &policy,
        &manifest.base_source_id
    )
    .is_err());
    let mut ahead = manifest.clone();
    ahead.generation = 1;
    assert!(Manifest::parse(&serde_json::to_vec(&ahead)?, &policy, &ahead.base_source_id).is_err());
    Ok(())
}

/// One synthetic regulation zone (root ADR-0373): reserved layer codes and area numbers only.
pub(crate) fn zone(id: &str, name: &str, wkb: &str) -> String {
    json!({"feature_id": id, "geometry_wkb_hex": wkb, "geometry_srid": 4326, "origin": "source",
        "properties": {"layer_code": &id[..4], "layer_name": "SYN/합성", "zone_code": "SYN999",
            "zone_name": name}})
    .to_string()
}

pub(crate) fn zoning_index(rows: &[String]) -> anyhow::Result<Vec<Record>> {
    let text: String = rows
        .iter()
        .map(|line| {
            format!(
                "{line}
"
            )
        })
        .collect();
    let file = written(text.as_bytes())?;
    let summary: ServedSummary = serde_json::from_value(json!({
        "schema_version": "foundation-platform.polygon_served_gold.v1",
        "unit": "zoning",
        "feature_id_property": "zone_id",
        "geometry_srid": 4326,
        "canonical_iceberg_snapshot_id": "2",
        "edits_through_change_seq": 0,
        "served_row_count": rows.len(),
        "status": "ready",
    }))?;
    Ok(build_index(&summary, file.path())?.0)
}

/// Every tile of `zooms` a box touches, as the dirty set names them.
fn touched(bbox: [f32; 4], zooms: (u8, u8)) -> BTreeSet<u64> {
    dirty_tiles(&[bbox], zooms, usize::MAX)
        .unwrap()
        .unwrap_or_default()
}

#[test]
fn a_changed_zone_re_tiles_only_the_tiles_it_touches() -> anyhow::Result<()> {
    let contract = super::super::BakeContract::parse(super::super::CONTRACT_JSON)?;
    let layer = contract.units_born_from_a_lakehouse_bake["zoning"].served_layer();
    let zooms = (layer.tile_min_zoom, layer.tile_max_zoom);
    // Three zones a degree apart, so no two share a tile at any served zoom (a z10 tile is 0.35°).
    let (a, b, c) = ("D999:SYN-0001", "D998:SYN-0001", "D999:SYN-0002");
    let month_1 = [
        zone(a, "합성 A", &square(10.0, 10.0, 0.01)),
        zone(b, "합성 B", &square(11.0, 10.0, 0.01)),
        zone(c, "합성 C", &square(12.0, 10.0, 0.01)),
    ];
    // Next month: A's boundary moves, C is renamed, B is untouched.
    let month_2 = [
        zone(a, "합성 A", &square(10.002, 10.0, 0.01)),
        zone(b, "합성 B", &square(11.0, 10.0, 0.01)),
        zone(c, "합성 C 변경", &square(12.0, 10.0, 0.01)),
    ];
    let (old, new) = (zoning_index(&month_1)?, zoning_index(&month_2)?);
    let changes = diff(&old, &new);
    let mut upserted = changes.upserted.clone();
    upserted.sort_unstable();
    let mut expected = vec![id_hash(a), id_hash(c)];
    expected.sort_unstable();
    assert_eq!(upserted, expected, "a renamed zone is a changed zone too");
    assert!(changes.deleted.is_empty());

    let dirty = dirty_tiles(&changes.boxes, zooms, usize::MAX)?.unwrap_or_default();
    let box_of = |records: &[Record], id: &str| {
        records
            .iter()
            .find(|record| record.id == id_hash(id))
            .map(|record| record.bbox)
            .unwrap_or(EMPTY_BBOX)
    };
    let mut own = touched(box_of(&old, a), zooms);
    own.extend(touched(box_of(&new, a), zooms));
    own.extend(touched(box_of(&new, c), zooms));
    assert_eq!(
        dirty, own,
        "exactly the tiles the changed zones touch, old box and new"
    );
    let untouched = touched(box_of(&new, b), zooms);
    assert!(!untouched.is_empty());
    assert!(
        dirty.is_disjoint(&untouched),
        "the unchanged zone's tiles are not re-tiled"
    );
    // One small zone touches a handful of tiles per zoom (z10..17 since root ADR-0492), far below
    // the 5% patch bound of a base.
    assert!(dirty.len() <= 120, "{} dirty tiles", dirty.len());
    assert!(diff(&new, &new).is_empty());
    Ok(())
}

/// Root ADR-0375 §2: the Python membership of `gold.land_use_zone_area_by_tile` and this tile math
/// compute the same ranges for the same boxes, with the contract's buffer.
#[test]
fn the_tile_range_matches_the_golden_ranges_python_computes() -> anyhow::Result<()> {
    let contract: serde_json::Value = serde_json::from_str(super::super::CONTRACT_JSON)?;
    assert_eq!(
        contract["patch_buffer_pixels"].as_f64(),
        Some(BUFFER_PIXELS),
        "tile-bake-containers.contract.json patch_buffer_pixels is this buffer"
    );
    let golden: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../infra/lakehouse/spark/tests/fixtures/tile_range_golden.json"),
    )?)?;
    assert_eq!(golden["buffer_pixels"].as_f64(), Some(BUFFER_PIXELS));
    let zoom = u8::try_from(golden["zoom"].as_u64().unwrap_or_default())?;
    let cases = golden["cases"].as_array().map_or(&[][..], Vec::as_slice);
    assert!(cases.len() >= 5, "the golden fixture holds its cases");
    for case in cases {
        let bbox: [f32; 4] = serde_json::from_value(case["bbox"].clone())?;
        let expected: Option<[u32; 4]> = serde_json::from_value(case["tiles"].clone())?;
        assert_eq!(
            tile_range(bbox, zoom).map(|(x0, y0, x1, y1)| [x0, y0, x1, y1]),
            expected,
            "{}",
            case["case"]
        );
    }
    Ok(())
}

/// 2026-11-13..18: the national parcel index (39,876,546 records) held whole in a 1 GiB unit swapped
/// until the 115-minute limit. Built in sorted runs merged from disk, it is the same index, and an id
/// in two runs is refused as one in a single run is.
#[test]
fn an_index_built_in_sorted_runs_is_the_index_built_at_once() -> anyhow::Result<()> {
    let rows: Vec<String> = [D, B, A, C]
        .iter()
        .zip([10.0, 11.0, 12.0, 13.0])
        .map(|(id, lon)| row(id, &square(lon, 10.0, 0.001)))
        .collect();
    let text: String = rows.iter().map(|line| format!("{line}\n")).collect();
    let file = written(text.as_bytes())?;
    let dir = tempfile::tempdir()?;
    let (at_once, in_runs) = (dir.path().join("once.idx"), dir.path().join("runs.idx"));
    let columns = build_index_file(&summary(rows.len()), file.path(), &at_once, 100)?;
    assert_eq!(
        build_index_file(&summary(rows.len()), file.path(), &in_runs, 1)?,
        columns
    );
    assert_eq!(std::fs::read(&in_runs)?, std::fs::read(&at_once)?);
    assert_eq!(read_index(&in_runs)?.len(), 4);
    // The sorted runs are gone; only the index stays.
    assert_eq!(std::fs::read_dir(dir.path())?.count(), 2);

    let twice = format!("{}\n{}\n{}\n", rows[0], rows[1], rows[0]);
    let file = written(twice.as_bytes())?;
    let error = build_index_file(&summary(3), file.path(), &dir.path().join("twice.idx"), 2)
        .err()
        .map(|error| error.to_string());
    assert_eq!(
        error.as_deref(),
        Some("the served snapshot holds a feature id twice")
    );
    Ok(())
}

/// The patch path reads the two indexes it compares as streams (2026-11-13..18: two national parcel
/// indexes, 3.2 GB of records, would not fit the bake's unit): the same changes as the slices give,
/// and a writer out of order or a file shorter than its count is refused.
#[test]
fn the_indexes_are_compared_as_streams_with_the_same_changes() -> anyhow::Result<()> {
    let old = indexed(&[
        row(A, &square(10.0, 10.0, 0.001)),
        row(B, &square(11.0, 10.0, 0.001)),
        row(C, &square(12.0, 10.0, 0.001)),
    ])?;
    let new = indexed(&[
        row(A, &square(10.0, 10.0, 0.001)),
        row(C, &square(12.5, 10.0, 0.001)),
        row(D, &square(13.0, 10.0, 0.001)),
    ])?;
    let dir = tempfile::tempdir()?;
    let (old_path, new_path) = (dir.path().join("old.idx"), dir.path().join("new.idx"));
    write_index(&old_path, &old)?;
    write_index(&new_path, &new)?;
    let streamed = diff_files(&old_path, &new_path)?;
    let sliced = diff(&old, &new);
    assert!(!sliced.is_empty());
    assert_eq!(
        (streamed.upserted, streamed.deleted, streamed.boxes),
        (sliced.upserted, sliced.deleted, sliced.boxes)
    );
    assert_eq!(
        index_ids(&new_path)?,
        new.iter().map(|record| record.id).collect::<Vec<_>>()
    );

    let mut writer = IndexWriter::create(&dir.path().join("unsorted.idx"))?;
    writer.push(&new[1])?;
    assert!(
        writer.push(&new[0]).is_err(),
        "the writer refuses a lower id"
    );

    let short = dir.path().join("short.idx");
    std::fs::write(&short, &std::fs::read(&new_path)?[..16 + 40 * 2])?;
    assert!(
        open_index(&short).is_err(),
        "a file shorter than its count is refused"
    );
    Ok(())
}
