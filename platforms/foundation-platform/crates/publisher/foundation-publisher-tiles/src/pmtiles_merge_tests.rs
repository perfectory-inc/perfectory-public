//! The partition merge (root ADR-0613): a tile one archive holds is the bytes that archive
//! stores, a tile several hold carries every feature of each, and on archives the pinned
//! tippecanoe baked the result is, feature for feature, what its `tile-join` writes.
//! Feature ids are synthetic (`99999…`), and the fixture's squares lie around (0, 0).

use std::collections::BTreeMap;
use std::time::Duration;

use super::*;

/// An MVT `Value`, to write test tiles and to read a value whatever bytes wrote it.
#[derive(Clone, PartialEq, prost::Message)]
struct TestValue {
    #[prost(string, optional, tag = "1")]
    string_value: Option<String>,
    #[prost(float, optional, tag = "2")]
    float_value: Option<f32>,
    #[prost(double, optional, tag = "3")]
    double_value: Option<f64>,
    #[prost(int64, optional, tag = "4")]
    int_value: Option<i64>,
    #[prost(uint64, optional, tag = "5")]
    uint_value: Option<u64>,
    #[prost(sint64, optional, tag = "6")]
    sint_value: Option<i64>,
    #[prost(bool, optional, tag = "7")]
    bool_value: Option<bool>,
}

fn text(value: &str) -> Vec<u8> {
    TestValue {
        string_value: Some(value.to_owned()),
        ..TestValue::default()
    }
    .encode_to_vec()
}

/// One layer whose features each carry `properties` in the layer's own key and value order.
fn layer(name: &str, extent: u32, features: &[(u64, &[(&str, &str)])]) -> Layer {
    let mut layer = Layer {
        name: name.to_owned(),
        extent: Some(extent),
        version: Some(2),
        ..Layer::default()
    };
    for (id, properties) in features {
        let mut tags = Vec::new();
        for (key, value) in *properties {
            let key_at = layer
                .keys
                .iter()
                .position(|known| known == key)
                .unwrap_or_else(|| {
                    layer.keys.push((*key).to_owned());
                    layer.keys.len() - 1
                });
            let value = text(value);
            let value_at = layer
                .values
                .iter()
                .position(|known| *known == value)
                .unwrap_or_else(|| {
                    layer.values.push(value);
                    layer.values.len() - 1
                });
            tags.extend([key_at as u32, value_at as u32]);
        }
        layer.features.push(Feature {
            id: Some(*id),
            tags,
            kind: Some(3),
            // Not a real command stream: the merge moves a geometry and never reads it.
            geometry: vec![9, (*id % 251) as u8, 7, 15],
        });
    }
    layer
}

fn mvt(layers: Vec<Layer>) -> Vec<u8> {
    Tile { layers }.encode_to_vec()
}

fn id(z: u8, x: u32, y: u32) -> u64 {
    TileId::from(TileCoord::new(z, x, y).unwrap()).value()
}

/// Writes an archive of `tiles` (tile id, uncompressed MVT) as the crate's writer does:
/// gzip tiles, identical consecutive tiles as one run, identical tiles stored once.
fn write_archive(path: &Path, tiles: &[(u64, Vec<u8>)], metadata: &str, zooms: (u8, u8)) {
    let mut sorted = tiles.to_vec();
    sorted.sort_by_key(|(id, _)| *id);
    let mut writer = PmTilesWriter::new(TileType::Mvt)
        .metadata(metadata)
        .min_zoom(zooms.0)
        .max_zoom(zooms.1)
        .create(File::create(path).unwrap())
        .unwrap();
    for (id, tile) in sorted {
        writer
            .add_tile(TileCoord::from(TileId::new(id).unwrap()), &tile)
            .unwrap();
    }
    writer.finalize().unwrap();
}

/// Every tile of an archive as it is stored (compressed), by tile id.
fn stored_tiles(path: &Path) -> BTreeMap<u64, Vec<u8>> {
    let reader = PmTilesReader::new_with_path(path).unwrap();
    let mut tiles = BTreeMap::new();
    for entry in reader.entries() {
        let entry = entry.unwrap();
        for step in 0..u64::from(entry.run_length()) {
            let id = entry.tile_id() + step;
            let tile = reader.get_tile(TileId::new(id).unwrap()).unwrap().unwrap();
            assert!(tiles.insert(id, tile.to_vec()).is_none(), "tile {id} twice");
        }
    }
    tiles
}

/// What identifies a feature whatever tables its layer keeps: its layer (with extent and
/// version), id, type, geometry bytes, and properties as key and typed value, in key order.
type Described = (
    String,
    Option<u32>,
    Option<u32>,
    Option<u64>,
    Option<i32>,
    Vec<u8>,
    Vec<(String, String)>,
);

/// The features of a stored tile, each with how many times the tile holds it.
fn features(stored: &[u8]) -> BTreeMap<Described, usize> {
    let tile = Tile::decode(gunzip(stored).unwrap().as_slice()).unwrap();
    let mut found = BTreeMap::new();
    for layer in tile.layers {
        for feature in &layer.features {
            let mut properties: Vec<(String, String)> = feature
                .tags
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| {
                    let value = TestValue::decode(layer.values[pair[1] as usize].as_slice());
                    (
                        layer.keys[pair[0] as usize].clone(),
                        format!("{:?}", value.unwrap()),
                    )
                })
                .collect();
            properties.sort();
            *found
                .entry((
                    layer.name.clone(),
                    layer.extent,
                    layer.version,
                    feature.id,
                    feature.kind,
                    feature.geometry.clone(),
                    properties,
                ))
                .or_insert(0) += 1;
        }
    }
    found
}

fn later() -> Instant {
    Instant::now() + Duration::from_secs(600)
}

const METADATA: &str = r#"{"name":"t","vector_layers":[{"id":"parcels","minzoom":5,"maxzoom":5,"fields":{"pnu":"String"}}]}"#;

#[test]
fn a_tile_one_archive_holds_is_the_bytes_that_archive_stores() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let (a, b, out) = (
        work.path().join("a.pmtiles"),
        work.path().join("b.pmtiles"),
        work.path().join("out.pmtiles"),
    );
    let tile = |n: u64| {
        mvt(vec![layer(
            "parcels",
            4096,
            &[(n, &[("pnu", "9999900000000000001")])],
        )])
    };
    write_archive(
        &a,
        &[(id(5, 0, 0), tile(1)), (id(5, 1, 0), tile(2))],
        METADATA,
        (5, 5),
    );
    write_archive(&b, &[(id(5, 2, 0), tile(3))], METADATA, (5, 5));
    let found = merge_archives(&[a.clone(), b.clone()], &out, later())?;
    assert_eq!(
        found,
        Merged {
            archives: 2,
            tiles: 3,
            copied: 3,
            merged: 0
        }
    );
    let mut expected = stored_tiles(&a);
    expected.extend(stored_tiles(&b));
    assert_eq!(stored_tiles(&out), expected);
    Ok(())
}

#[test]
fn a_tile_two_archives_hold_carries_every_feature_of_both() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let (a, b, out) = (
        work.path().join("a.pmtiles"),
        work.path().join("b.pmtiles"),
        work.path().join("out.pmtiles"),
    );
    let shared = id(5, 3, 3);
    // The two archives keep their keys and values in different orders, share some and not others.
    let from_a = mvt(vec![layer(
        "parcels",
        4096,
        &[
            (1, &[("pnu", "9999900000000000001"), ("jimok", "dae")]),
            (2, &[("pnu", "9999900000000000002"), ("jimok", "jeon")]),
        ],
    )]);
    let from_b = mvt(vec![
        layer(
            "parcels",
            4096,
            &[(
                3,
                &[
                    ("jimok", "dae"),
                    ("note", "n1"),
                    ("pnu", "9999900000000000003"),
                ],
            )],
        ),
        layer("roads", 4096, &[(9, &[("name", "r")])]),
    ]);
    write_archive(&a, &[(shared, from_a)], METADATA, (5, 5));
    write_archive(&b, &[(shared, from_b)], METADATA, (5, 5));
    let found = merge_archives(&[a.clone(), b.clone()], &out, later())?;
    assert_eq!((found.tiles, found.copied, found.merged), (1, 0, 1));
    let mut expected = features(&stored_tiles(&a)[&shared]);
    expected.extend(features(&stored_tiles(&b)[&shared]));
    assert_eq!(expected.len(), 4);
    let merged = stored_tiles(&out);
    assert_eq!(features(&merged[&shared]), expected);
    // One layer a name, the first archive's first.
    let tile = Tile::decode(gunzip(&merged[&shared])?.as_slice())?;
    let names: Vec<&str> = tile
        .layers
        .iter()
        .map(|layer| layer.name.as_str())
        .collect();
    assert_eq!(names, ["parcels", "roads"]);
    Ok(())
}

#[test]
fn content_an_archive_stores_once_is_stored_once() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let (a, b, out) = (
        work.path().join("a.pmtiles"),
        work.path().join("b.pmtiles"),
        work.path().join("out.pmtiles"),
    );
    let same = mvt(vec![layer(
        "parcels",
        4096,
        &[(1, &[("pnu", "9999900000000000001")])],
    )]);
    let other = mvt(vec![layer(
        "parcels",
        4096,
        &[(2, &[("pnu", "9999900000000000002")])],
    )]);
    let first = id(6, 0, 0);
    // A run of three, one tile of another content, then the first content again.
    write_archive(
        &a,
        &[
            (first, same.clone()),
            (first + 1, same.clone()),
            (first + 2, same.clone()),
            (first + 3, other.clone()),
            (first + 9, same),
        ],
        METADATA,
        (6, 6),
    );
    write_archive(&b, &[(first + 20, other)], METADATA, (6, 6));
    let found = merge_archives(&[a.clone(), b.clone()], &out, later())?;
    assert_eq!((found.tiles, found.copied, found.merged), (6, 6, 0));
    let mut expected = stored_tiles(&a);
    expected.extend(stored_tiles(&b));
    assert_eq!(stored_tiles(&out), expected);
    let reader = PmTilesReader::new_with_path(&out)?;
    let header = reader.get_header();
    assert_eq!(header.n_addressed_tiles().map(u64::from), Some(6));
    // `same` once, `other` once for each archive that holds it: nothing an archive stored once
    // is stored twice, and the merge hashes no tile to find more.
    assert_eq!(header.n_tile_contents().map(u64::from), Some(3));
    Ok(())
}

/// Three archives hold one tile (a corner where three partitions meet), and a tile in the middle
/// of one archive's run is held by another: the run's other tiles are copied, that one is merged.
#[test]
fn three_holders_and_a_run_another_archive_cuts_into() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let paths: Vec<PathBuf> = ["a", "b", "c"]
        .iter()
        .map(|name| work.path().join(format!("{name}.pmtiles")))
        .collect();
    let out = work.path().join("out.pmtiles");
    let tile = |n: u64| {
        let pnu = format!("99999{n:014}");
        mvt(vec![layer(
            "parcels",
            4096,
            &[(n, &[("pnu", pnu.as_str())])],
        )])
    };
    let corner = id(6, 5, 5);
    let run = id(6, 0, 0);
    write_archive(
        &paths[0],
        &[
            (run, tile(1)),
            (run + 1, tile(1)),
            (run + 2, tile(1)),
            (corner, tile(2)),
        ],
        METADATA,
        (6, 6),
    );
    write_archive(
        &paths[1],
        &[(run + 1, tile(3)), (corner, tile(4))],
        METADATA,
        (6, 6),
    );
    write_archive(&paths[2], &[(corner, tile(5))], METADATA, (6, 6));
    let found = merge_archives(&paths, &out, later())?;
    assert_eq!((found.tiles, found.copied, found.merged), (4, 2, 2));
    let stored: Vec<BTreeMap<u64, Vec<u8>>> = paths.iter().map(|path| stored_tiles(path)).collect();
    let merged = stored_tiles(&out);
    assert_eq!(merged[&run], stored[0][&run]);
    assert_eq!(merged[&(run + 2)], stored[0][&(run + 2)]);
    for (tile_id, holders) in [(run + 1, &stored[..2]), (corner, &stored[..])] {
        let mut expected = BTreeMap::new();
        for holder in holders {
            expected.extend(features(&holder[&tile_id]));
        }
        assert_eq!(expected.len(), holders.len());
        assert_eq!(features(&merged[&tile_id]), expected, "tile {tile_id}");
    }
    Ok(())
}

#[test]
fn archives_that_disagree_on_a_layer_are_refused() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let (a, b) = (work.path().join("a.pmtiles"), work.path().join("b.pmtiles"));
    let shared = id(5, 3, 3);
    let tile = |extent: u32| {
        mvt(vec![layer(
            "parcels",
            extent,
            &[(1, &[("pnu", "9999900000000000001")])],
        )])
    };
    write_archive(&a, &[(shared, tile(4096))], METADATA, (5, 5));
    write_archive(&b, &[(shared, tile(8192))], METADATA, (5, 5));
    let refused = merge_archives(&[a, b], &work.path().join("out.pmtiles"), later())
        .expect_err("two extents of one layer");
    assert!(
        format!("{refused:#}").contains("not baked alike"),
        "{refused:#}"
    );
    assert!(merge_archives(&[], &work.path().join("none.pmtiles"), later()).is_err());
    Ok(())
}

#[test]
fn the_merged_archive_describes_every_partition() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let (a, b, out) = (
        work.path().join("a.pmtiles"),
        work.path().join("b.pmtiles"),
        work.path().join("out.pmtiles"),
    );
    let tile = mvt(vec![layer(
        "parcels",
        4096,
        &[(1, &[("pnu", "9999900000000000001")])],
    )]);
    write_archive(
        &a,
        &[(id(6, 1, 1), tile.clone())],
        r#"{"name":"a","tilestats":{"layerCount":1},"generator_options":"x","vector_layers":[
            {"id":"parcels","minzoom":6,"maxzoom":6,"fields":{"pnu":"String"}}]}"#,
        (6, 6),
    );
    write_archive(
        &b,
        &[(id(8, 1, 1), tile)],
        r#"{"name":"b","vector_layers":[
            {"id":"parcels","minzoom":7,"maxzoom":8,"fields":{"note":"String"}},
            {"id":"roads","minzoom":8,"maxzoom":8,"fields":{}}]}"#,
        (7, 8),
    );
    merge_archives(&[a, b], &out, later())?;
    let reader = PmTilesReader::new_with_path(&out)?;
    let header = reader.get_header();
    assert_eq!((header.min_zoom, header.max_zoom), (6, 8));
    assert_eq!(header.tile_compression, Compression::Gzip);
    let metadata: Value = serde_json::from_str(&reader.get_metadata()?)?;
    assert_eq!(metadata["name"], "a");
    assert!(metadata.get("tilestats").is_none() && metadata.get("generator_options").is_none());
    assert_eq!(
        metadata["vector_layers"],
        serde_json::json!([
            {"id": "parcels", "minzoom": 6, "maxzoom": 8, "fields": {"pnu": "String", "note": "String"}},
            {"id": "roads", "minzoom": 8, "maxzoom": 8, "fields": {}}
        ])
    );
    Ok(())
}

#[test]
fn a_merge_past_its_deadline_stops() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let a = work.path().join("a.pmtiles");
    let first = id(7, 0, 0);
    let tiles: Vec<(u64, Vec<u8>)> = (0..=DEADLINE_CHECK_TILES)
        .map(|n| {
            let pnu = format!("99999{n:014}");
            (
                first + n,
                mvt(vec![layer(
                    "parcels",
                    4096,
                    &[(n, &[("pnu", pnu.as_str())])],
                )]),
            )
        })
        .collect();
    write_archive(&a, &tiles, METADATA, (7, 7));
    let stopped = merge_archives(&[a], &work.path().join("out.pmtiles"), Instant::now())
        .expect_err("the deadline has passed");
    assert!(format!("{stopped:#}").contains("time bound"), "{stopped:#}");
    Ok(())
}

/// `tests/fixtures/tile-join`: two partitions the pinned tippecanoe (2.79.0) baked from synthetic
/// squares with the bake's flags, and what its `tile-join` made of them (`PROVENANCE.txt` there). The
/// merge of the two partitions holds the same tiles, each with the same features, and leaves the
/// tiles one partition holds as that partition stores them.
#[test]
fn the_merge_of_two_baked_partitions_is_what_tile_join_writes() -> anyhow::Result<()> {
    // Read when the test runs, not when it is compiled: cargo and nextest both set the variable.
    let fixtures =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("tests/fixtures/tile-join");
    let (a, b) = (fixtures.join("a.pmtiles"), fixtures.join("b.pmtiles"));
    let work = tempfile::tempdir()?;
    let out = work.path().join("out.pmtiles");
    let found = merge_archives(&[a.clone(), b.clone()], &out, later())?;
    assert_eq!(
        found,
        Merged {
            archives: 2,
            tiles: 21,
            copied: 13,
            merged: 8
        }
    );
    let (from_a, from_b) = (stored_tiles(&a), stored_tiles(&b));
    let joined = stored_tiles(&fixtures.join("joined.pmtiles"));
    let merged = stored_tiles(&out);
    assert_eq!(
        merged.keys().collect::<Vec<_>>(),
        joined.keys().collect::<Vec<_>>(),
        "the same tiles"
    );
    for (id, tile) in &merged {
        assert_eq!(features(tile), features(&joined[id]), "tile {id}");
        match (from_a.get(id), from_b.get(id)) {
            (Some(only), None) | (None, Some(only)) => {
                assert_eq!(tile, only, "tile {id}");
                // tile-join itself changed nothing in a tile one partition holds.
                assert_eq!(gunzip(&joined[id])?, gunzip(only)?, "tile {id}");
            }
            (Some(_), Some(_)) => {}
            (None, None) => panic!("tile {id} is in neither partition"),
        }
    }
    // The bake's own reader (the id gate, the byte budget, the patch path) reads what the crate's
    // writer wrote: the same tiles, in tile-id order, the same bytes once decompressed.
    let mut archive = super::super::pmtiles_feature_ids::Archive::open(File::open(&out)?)?;
    let mut walked = Vec::new();
    archive.for_each_entry((0, u64::MAX), &mut |archive, entry| {
        for step in 0..entry.run_length {
            walked.push((entry.tile_id + step, archive.entry_tile(&entry)?));
        }
        Ok(())
    })?;
    assert_eq!(walked.len(), merged.len());
    for (id, tile) in &walked {
        assert_eq!(*tile, gunzip(&merged[id])?, "tile {id}");
    }
    let (reader, reference) = (
        PmTilesReader::new_with_path(&out)?,
        PmTilesReader::new_with_path(fixtures.join("joined.pmtiles"))?,
    );
    let (header, expected) = (reader.get_header(), reference.get_header());
    assert_eq!(
        (header.min_zoom, header.max_zoom),
        (expected.min_zoom, expected.max_zoom)
    );
    let layers = |reader: &PmTilesReader<File>| -> anyhow::Result<Value> {
        let metadata: Value = serde_json::from_str(&reader.get_metadata()?)?;
        Ok(metadata["vector_layers"].clone())
    };
    assert_eq!(layers(&reader)?, layers(&reference)?);
    Ok(())
}
