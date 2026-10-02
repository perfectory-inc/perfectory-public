use std::io::Write as _;

use flate2::write::GzEncoder;
use flate2::Compression;

use super::*;

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap_or(0);
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(bytes).unwrap_or_default();
    encoder.finish().unwrap_or_default()
}

fn directory(entries: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    varint(&mut out, entries.len() as u64);
    let mut last = 0;
    for entry in entries {
        varint(&mut out, entry.tile_id - last);
        last = entry.tile_id;
    }
    for entry in entries {
        varint(&mut out, entry.run_length);
    }
    for entry in entries {
        varint(&mut out, entry.length);
    }
    for entry in entries {
        varint(&mut out, entry.offset + 1);
    }
    out
}

/// A synthetic MVT tile: one layer, each feature tagged with an id and one more property.
pub(crate) fn tile(layer: &str, ids: &[&str]) -> Vec<u8> {
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

/// One tile per (tile id, ids); the root points at one leaf per pair of tiles so the walk descends.
pub(crate) fn archive(min_zoom: u8, max_zoom: u8, tiles: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let mut data = Vec::new();
    let mut entries = Vec::new();
    for (tile_id, bytes) in tiles {
        entries.push(Entry {
            tile_id: *tile_id,
            offset: data.len() as u64,
            length: bytes.len() as u64,
            run_length: 1,
        });
        data.extend_from_slice(bytes);
    }
    let mut leaves = Vec::new();
    let mut root = Vec::new();
    for chunk in entries.chunks(2) {
        let leaf = gzip(&directory(chunk));
        root.push(Entry {
            tile_id: chunk[0].tile_id,
            offset: leaves.len() as u64,
            length: leaf.len() as u64,
            run_length: 0,
        });
        leaves.extend_from_slice(&leaf);
    }
    let root = gzip(&directory(&root));
    let root_offset = HEADER_BYTES as u64;
    let leaves_offset = root_offset + root.len() as u64;
    let data_offset = leaves_offset + leaves.len() as u64;
    let mut header = vec![0_u8; HEADER_BYTES];
    header[..7].copy_from_slice(b"PMTiles");
    header[7] = 3;
    header[8..16].copy_from_slice(&root_offset.to_le_bytes());
    header[16..24].copy_from_slice(&(root.len() as u64).to_le_bytes());
    header[40..48].copy_from_slice(&leaves_offset.to_le_bytes());
    header[48..56].copy_from_slice(&(leaves.len() as u64).to_le_bytes());
    header[56..64].copy_from_slice(&data_offset.to_le_bytes());
    header[64..72].copy_from_slice(&(data.len() as u64).to_le_bytes());
    header[97] = COMPRESSION_GZIP;
    header[98] = COMPRESSION_GZIP;
    header[99] = TILE_TYPE_MVT;
    header[100] = min_zoom;
    header[101] = max_zoom;
    [header, root, leaves, data].concat()
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

pub(crate) fn written(bytes: &[u8]) -> anyhow::Result<tempfile::NamedTempFile> {
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(file)
}

#[test]
fn tile_ids_follow_the_spec_hilbert_order_and_invert() -> anyhow::Result<()> {
    // The spec's zoom-1 order: (0,0), (0,1), (1,1), (1,0).
    assert_eq!(tile_id(0, 0, 0), 0);
    assert_eq!(
        [
            tile_id(1, 0, 0),
            tile_id(1, 0, 1),
            tile_id(1, 1, 1),
            tile_id(1, 1, 0)
        ],
        [1, 2, 3, 4]
    );
    assert_eq!(tile_id(2, 0, 0), 5);
    for (zoom, x, y) in [
        (3, 5, 2),
        (14, 9_001, 4_002),
        (16, 65_535, 0),
        (16, 1, 65_535),
    ] {
        assert_eq!(tile_zxy(tile_id(zoom, x, y))?, (zoom, x, y));
    }
    Ok(())
}

#[test]
fn a_tile_is_found_by_id_through_its_leaf_or_reported_absent() -> anyhow::Result<()> {
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
    let mut archive = Archive::open(std::io::Cursor::new(bytes))?;
    let found = archive.tile(6)?.context("tile 6")?;
    assert_eq!(Tile::decode(found.as_slice())?.layers[0].features.len(), 1);
    assert!(archive.tile(7)?.is_none(), "no entry covers tile 7");
    assert!(
        archive.tile(0)?.is_none(),
        "nothing precedes the first entry"
    );
    assert!(archive.tile(21)?.is_none());
    Ok(())
}

#[test]
fn tile_ids_number_every_lower_zoom_first() {
    assert_eq!(first_tile_id(0), 0);
    assert_eq!(first_tile_id(1), 1);
    assert_eq!(first_tile_id(2), 5);
    assert_eq!(first_tile_id(3), 21);
}

#[test]
fn a_directory_round_trips_including_contiguous_offsets() -> anyhow::Result<()> {
    let entries = [
        Entry {
            tile_id: 5,
            offset: 0,
            length: 10,
            run_length: 1,
        },
        Entry {
            tile_id: 7,
            offset: 10,
            length: 4,
            run_length: 3,
        },
    ];
    let mut bytes = directory(&entries);
    assert_eq!(decode_directory(&bytes)?, entries);
    bytes.push(0);
    assert!(
        decode_directory(&bytes).is_err(),
        "trailing bytes are refused"
    );
    assert!(
        decode_directory(&[0x80]).is_err(),
        "a cut varint is refused"
    );
    Ok(())
}

#[test]
fn only_the_asked_zoom_is_read_across_leaf_directories() -> anyhow::Result<()> {
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
fn the_header_is_read_as_the_spec_lays_it_out() -> anyhow::Result<()> {
    let bytes = archive(6, 16, &[]);
    let header = Header::parse(&bytes)?;
    assert_eq!((header.min_zoom, header.max_zoom), (6, 16));
    assert_eq!(header.tile_type, TILE_TYPE_MVT);
    assert_eq!(header.root_directory.0, HEADER_BYTES as u64);
    let mut v2 = bytes.clone();
    v2[7] = 2;
    assert!(Header::parse(&v2).is_err());
    assert!(Header::parse(&bytes[..100]).is_err());
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
