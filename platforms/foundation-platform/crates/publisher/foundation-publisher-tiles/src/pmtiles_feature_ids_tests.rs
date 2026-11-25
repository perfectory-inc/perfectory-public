use std::io::{Cursor, Write as _};

use flate2::write::GzEncoder;
use flate2::Compression as Level;
use pmtiles::{Compression, PmTilesWriter, TileType};

use super::*;

pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Level::fast());
    encoder.write_all(bytes).unwrap_or_default();
    encoder.finish().unwrap_or_default()
}

/// A synthetic MVT tile: one layer, each feature tagged with an id and one more property.
pub fn tile(layer: &str, ids: &[&str]) -> Vec<u8> {
    let mut keys = vec!["pnu".to_owned(), "kind".to_owned()];
    let mut values = vec![Value {
        string_value: Some("synthetic".to_owned()),
    }];
    let mut features = Vec::new();
    for id in ids {
        values.push(Value {
            string_value: Some((*id).to_owned()),
        });
        let index = u32::try_from(values.len() - 1).unwrap_or(0);
        features.push(Feature {
            tags: vec![0, index, 1, 0],
        });
    }
    if layer == "foreign-key" {
        keys.push("unexpected".to_owned());
    }
    gzip(
        &Tile {
            layers: vec![Layer {
                name: if layer == "foreign-key" {
                    "parcel".to_owned()
                } else {
                    layer.to_owned()
                },
                features,
                keys,
                values,
            }],
        }
        .encode_to_vec(),
    )
}

/// An archive of `tiles` (tile id, stored gzip bytes, in tile-id order) over zooms
/// `min_zoom..=max_zoom`, as the crate's writer writes it.
pub fn archive(min_zoom: u8, max_zoom: u8, tiles: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let mut out = Cursor::new(Vec::new());
    let mut writer = PmTilesWriter::new(TileType::Mvt)
        .precompressed(Compression::Gzip)
        .min_zoom(min_zoom)
        .max_zoom(max_zoom)
        .create(&mut out)
        .unwrap();
    for (id, bytes) in tiles {
        writer
            .add_tile(TileCoord::from(TileId::new(*id).unwrap()), bytes)
            .unwrap();
    }
    writer.finalize().unwrap();
    out.into_inner()
}

fn properties() -> Vec<String> {
    vec!["pnu".to_owned(), "kind".to_owned()]
}

fn ids_at(path: &Path, zoom: u8, layer: &str) -> anyhow::Result<(Vec<String>, u64)> {
    let mut ids = Vec::new();
    let names = properties();
    let tiles = for_each_feature_id(
        path,
        &Expect {
            zoom,
            layer,
            id_property: "pnu",
            properties: &names,
        },
        |_, id| {
            ids.push(id.to_owned());
            Ok(())
        },
    )?;
    Ok((ids, tiles))
}

pub fn written(bytes: &[u8]) -> anyhow::Result<tempfile::NamedTempFile> {
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(file)
}

#[test]
fn tile_ids_follow_the_spec_hilbert_order_and_invert() -> anyhow::Result<()> {
    // The spec's zoom-1 order: (0,0), (0,1), (1,1), (1,0).
    assert_eq!(tile_id(0, 0, 0)?, 0);
    assert_eq!(
        [
            tile_id(1, 0, 0)?,
            tile_id(1, 0, 1)?,
            tile_id(1, 1, 1)?,
            tile_id(1, 1, 0)?
        ],
        [1, 2, 3, 4]
    );
    assert_eq!(tile_id(2, 0, 0)?, 5);
    for (zoom, x, y) in [
        (3, 5, 2),
        (14, 9_001, 4_002),
        (16, 65_535, 0),
        (16, 1, 65_535),
    ] {
        assert_eq!(tile_zxy(tile_id(zoom, x, y)?)?, (zoom, x, y));
    }
    assert!(tile_id(1, 2, 0).is_err(), "x past the zoom");
    Ok(())
}

#[test]
fn a_zoom_range_starts_after_every_lower_zoom() {
    assert_eq!(zoom_range(0), (0, 1));
    assert_eq!(zoom_range(1), (1, 5));
    assert_eq!(zoom_range(2), (5, 21));
    assert_eq!(zoom_range(3).0, 21);
}

#[test]
fn a_tile_is_found_by_id_or_reported_absent_from_a_file_and_from_seekable_bytes(
) -> anyhow::Result<()> {
    let bytes = archive(
        1,
        2,
        &[
            (1, tile("parcel", &["99999-low"])),
            (5, tile("parcel", &["99999-a"])),
            (6, tile("parcel", &["99999-b"])),
            (20, tile("parcel", &["99999-c"])),
        ],
    );
    let file = written(&bytes)?;
    let local = open(file.path())?;
    let remote = open_seekable(Cursor::new(bytes))?;
    for found in [super::tile(&local, 6)?, super::tile(&remote, 6)?] {
        let found = found.context("tile 6")?;
        assert_eq!(Tile::decode(found.as_ref())?.layers[0].features.len(), 1);
    }
    assert_eq!(
        stored(&local, 6)?.map(|bytes| bytes.to_vec()),
        Some(tile("parcel", &["99999-b"])),
        "stored bytes are the tile as written"
    );
    for id in [7, 0, 21] {
        assert!(
            super::tile(&local, id)?.is_none(),
            "no entry covers tile {id}"
        );
        assert!(
            super::tile(&remote, id)?.is_none(),
            "no entry covers tile {id}"
        );
    }
    Ok(())
}

#[test]
fn only_the_asked_zoom_is_read() -> anyhow::Result<()> {
    // z1 tiles are ids 1..5, z2 tiles 5..21. The z1 tile must not be read when z2 is asked.
    let file = written(&archive(
        1,
        2,
        &[
            (1, tile("parcel", &["99999-low"])),
            (5, tile("parcel", &["99999-a", "99999-b"])),
            (6, tile("parcel", &["99999-b"])),
            (20, tile("parcel", &["99999-c"])),
        ],
    ))?;
    let (ids, tiles) = ids_at(file.path(), 2, "parcel")?;
    assert_eq!(tiles, 3);
    assert_eq!(ids, ["99999-a", "99999-b", "99999-b", "99999-c"]);
    let (low, tiles) = ids_at(file.path(), 1, "parcel")?;
    assert_eq!((low, tiles), (vec!["99999-low".to_owned()], 1));
    Ok(())
}

#[test]
fn a_walk_of_a_range_meets_every_entry_that_overlaps_it_in_order() -> anyhow::Result<()> {
    let same = tile("parcel", &["99999-a"]);
    // Ids 5..=7 hold the same bytes: the writer keeps them as one run.
    let file = written(&archive(
        1,
        2,
        &[
            (1, tile("parcel", &["99999-low"])),
            (5, same.clone()),
            (6, same.clone()),
            (7, same),
            (20, tile("parcel", &["99999-c"])),
        ],
    ))?;
    let archive = open(file.path())?;
    let mut met = Vec::new();
    for_each_entry(&archive, (6, 21), |entry| {
        met.push((entry.tile_id(), entry.run_length()));
        Ok(())
    })?;
    assert_eq!(met, [(5, 3), (20, 1)]);
    Ok(())
}

#[test]
fn a_foreign_layer_a_foreign_property_or_a_feature_without_its_id_is_refused() -> anyhow::Result<()>
{
    let other_layer = written(&archive(1, 1, &[(1, tile("buildings", &["99999-a"]))]))?;
    assert!(ids_at(other_layer.path(), 1, "parcel").is_err());
    let other_key = written(&archive(1, 1, &[(1, tile("foreign-key", &["99999-a"]))]))?;
    assert!(ids_at(other_key.path(), 1, "parcel").is_err());
    let anonymous = gzip(
        &Tile {
            layers: vec![Layer {
                name: "parcel".to_owned(),
                features: vec![Feature { tags: vec![1, 0] }],
                keys: properties(),
                values: vec![Value {
                    string_value: Some("synthetic".to_owned()),
                }],
            }],
        }
        .encode_to_vec(),
    );
    let anonymous = written(&archive(1, 1, &[(1, anonymous)]))?;
    assert!(ids_at(anonymous.path(), 1, "parcel").is_err());
    Ok(())
}

#[test]
fn a_tile_that_is_not_gzip_or_runs_past_the_file_is_refused() -> anyhow::Result<()> {
    let mut bytes = archive(1, 1, &[(1, b"not gzip".to_vec())]);
    let file = written(&bytes)?;
    assert!(ids_at(file.path(), 1, "parcel").is_err());
    bytes.truncate(bytes.len() - 2);
    let cut = written(&bytes)?;
    assert!(ids_at(cut.path(), 1, "parcel").is_err());
    Ok(())
}
