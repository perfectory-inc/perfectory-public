use std::collections::BTreeSet;
use std::io::Cursor;

use serde_json::json;

use super::super::pmtiles_feature_ids::tests::{archive, gzip, tile, written};
use super::super::pmtiles_feature_ids::{tile_id, zoom_range, Entry};
use super::super::pmtiles_write::{directories, write_archive};
use super::super::prepare_handoff;
use super::*;

// Synthetic only: reserved-band PNUs (99999…) and coordinates near 10°E 10°N, no real place.
const A: &str = "9999900000000000001";
const B: &str = "9999900000000000002";
const C: &str = "9999900000000000003";
const D: &str = "9999900000000000004";

fn square(lon: f64, lat: f64, side: f64) -> String {
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
fn the_writer_round_trips_through_the_reader_with_leaf_directories() -> anyhow::Result<()> {
    // Enough tiles that the root no longer fits 16 KiB, so leaves are written and walked.
    let (first, _) = zoom_range(14);
    // Irregular gaps and sizes, so the directory does not compress into the root.
    let tiles: Vec<(u64, Vec<u8>)> = (0..40_000_u64)
        .map(|n| {
            (
                first + n * 10 + (n * 7_919) % 9,
                vec![7; 1 + (n * 31 % 200) as usize],
            )
        })
        .collect();
    let mut template = archive(14, 16, &[]);
    template.truncate(HEADER_BYTES);
    let mut bytes = Vec::new();
    let written_bytes = write_archive(&mut bytes, &template, &gzip(b"{}"), &tiles)?;
    assert_eq!(written_bytes, bytes.len() as u64);
    let leaves = u64::from_le_bytes(bytes[48..56].try_into()?);
    let root = u64::from_le_bytes(bytes[16..24].try_into()?);
    assert!(
        leaves > 0 && root <= 16_384 - HEADER_BYTES as u64,
        "root {root}, leaves {leaves}"
    );
    let mut reader = Archive::open(Cursor::new(bytes))?;
    assert_eq!(reader.header().addressed_tiles, 40_000);
    for (id, stored) in [&tiles[0], &tiles[17_777], &tiles[39_999]] {
        let entry = reader
            .find_entry(*id)?
            .unwrap_or_else(|| panic!("tile {id}"));
        assert_eq!(&reader.entry_raw(&entry)?, stored);
    }
    assert!(reader.find_entry(first + 1)?.is_none());
    let mut seen = 0;
    reader.for_each_entry((0, u64::MAX), &mut |_, _| {
        seen += 1;
        Ok(())
    })?;
    assert_eq!(seen, 40_000);
    Ok(())
}

#[test]
fn the_writer_refuses_unordered_or_empty_tiles_and_small_directories_stay_in_the_root(
) -> anyhow::Result<()> {
    let mut template = archive(14, 16, &[]);
    template.truncate(HEADER_BYTES);
    let mut out = Vec::new();
    assert!(write_archive(&mut out, &template, &[], &[(2, vec![1]), (1, vec![1])]).is_err());
    assert!(write_archive(&mut out, &template, &[], &[(1, Vec::new())]).is_err());
    let entries = [Entry {
        tile_id: 5,
        offset: 0,
        length: 3,
        run_length: 1,
    }];
    assert!(directories(&entries)?.1.is_empty());
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
        .unwrap_or_else(|| panic!("within the limit"));
    assert_eq!(dirty.len(), 3);
    assert!(dirty.contains(&tile_id(16, x, y)));
    assert!(dirty_tiles(&[[10.0, 10.0, 10.0, 10.0]], (14, 16), 2).is_none());
    assert!(dirty_tiles(&[EMPTY_BBOX], (14, 16), 0).is_some_and(|tiles| tiles.is_empty()));
}

#[test]
fn the_neighbourhood_is_every_row_touching_a_dirty_tiles_minzoom_ancestor() -> anyhow::Result<()> {
    let (x, y) = z16_tile();
    let dirty = BTreeSet::from([tile_id(16, x, y)]);
    let tiles = neighbourhood(&dirty, 14)?;
    assert_eq!(tiles, HashSet::from([(x >> 2, y >> 2)]));
    let near = [
        row(A, &square(10.0, 10.0, 0.000_1)),
        row(B, &square(10.002, 10.002, 0.000_1)),
    ];
    let far = row(C, &square(11.0, 11.0, 0.000_1));
    let lines = [near[0].clone(), far.clone(), near[1].clone()];
    let records = std::sync::Arc::new(indexed(&lines)?);
    let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
    let file = written(text.as_bytes())?;
    let csv = NeighbourhoodCsv {
        inner: Vec::new(),
        index: records,
        min_zoom: 14,
        tiles,
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

fn local(bytes: Vec<u8>) -> anyhow::Result<Archive<Cursor<Vec<u8>>>> {
    Archive::open(Cursor::new(bytes))
}

#[test]
fn the_patch_takes_dirty_tiles_whole_tombstones_the_rest_and_keeps_the_previous_generation(
) -> anyhow::Result<()> {
    let (first, _) = zoom_range(14);
    let mut baked = local(archive(
        14,
        16,
        &[
            (first, tile("parcels", &[A])),
            (first + 9, tile("parcels", &[C])),
        ],
    ))?;
    let mut previous = local(archive(
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
    let (tiles, empty) = assemble(Some(&mut baked), Some(&mut previous), &dirty)?;
    let ids: Vec<u64> = tiles.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![first, first + 1, first + 5]);
    assert_eq!(tiles[0].1, tile("parcels", &[A]));
    assert_eq!(tiles[1].1, empty_tile()?);
    assert_eq!(tiles[2].1, tile("parcels", &[D]));
    assert_eq!(empty, 1);
    let (only_tombstones, empty) =
        assemble::<Cursor<Vec<u8>>, Cursor<Vec<u8>>>(None, None, &dirty)?;
    assert_eq!((only_tombstones.len(), empty), (2, 2));
    Ok(())
}

fn patch_file(tiles: &[(u64, Vec<u8>)]) -> anyhow::Result<tempfile::NamedTempFile> {
    let mut template = archive(14, 16, &[]);
    template.truncate(HEADER_BYTES);
    let mut bytes = Vec::new();
    write_archive(&mut bytes, &template, &gzip(b"{}"), tiles)?;
    written(&bytes)
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
