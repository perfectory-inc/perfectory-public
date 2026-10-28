//! Writes a PMTiles v3 archive from tiles that are already encoded and compressed (root ADR-0323):
//! a layer's patch generation, a few percent of its base archive's tiles.
//!
//! The layout is the spec's (`protomaps/PMTiles` `spec/v3/spec.md`): header, root directory,
//! metadata, leaf directories, tile data in tile-id order. The root and leaf split follows the
//! reference writers (`pmtiles` Python `optimize_directories`, go-pmtiles): one root while it fits
//! the first 16 KiB with the header, otherwise leaves of 4096 entries, doubled until the root fits.
//! Every directory is gzip, so the template's internal compression must be gzip.

use std::io::Write;

use anyhow::{ensure, Context as _};
use flate2::write::GzEncoder;
use flate2::Compression;

use super::pmtiles_feature_ids::{Entry, COMPRESSION_GZIP, HEADER_BYTES};

/// The root directory and the header share the first 16 KiB, so one range read gets both.
const ROOT_BUDGET: usize = 16_384 - HEADER_BYTES;
const FIRST_LEAF_SIZE: usize = 4096;

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// A gzip directory: count, tile-id deltas, run lengths, lengths, then offsets (0 when an entry
/// starts where the previous one ended, else offset + 1).
pub fn directory(entries: &[Entry]) -> anyhow::Result<Vec<u8>> {
    let mut raw = Vec::new();
    varint(&mut raw, entries.len() as u64);
    let mut last = 0;
    for entry in entries {
        varint(&mut raw, entry.tile_id - last);
        last = entry.tile_id;
    }
    for entry in entries {
        varint(&mut raw, entry.run_length);
    }
    for entry in entries {
        varint(&mut raw, entry.length);
    }
    for (index, entry) in entries.iter().enumerate() {
        let contiguous =
            index > 0 && entries[index - 1].offset + entries[index - 1].length == entry.offset;
        varint(&mut raw, if contiguous { 0 } else { entry.offset + 1 });
    }
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(&raw)?;
    Ok(gzip.finish()?)
}

/// The root directory and the leaf directories the root points at (empty when the root holds every
/// entry).
pub fn directories(entries: &[Entry]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let root = directory(entries)?;
    if root.len() <= ROOT_BUDGET {
        return Ok((root, Vec::new()));
    }
    let mut leaf_size = FIRST_LEAF_SIZE;
    loop {
        let (mut roots, mut leaves) = (Vec::new(), Vec::new());
        for chunk in entries.chunks(leaf_size) {
            let leaf = directory(chunk)?;
            roots.push(Entry {
                tile_id: chunk[0].tile_id,
                offset: leaves.len() as u64,
                length: leaf.len() as u64,
                run_length: 0,
            });
            leaves.extend_from_slice(&leaf);
        }
        let root = directory(&roots)?;
        if root.len() <= ROOT_BUDGET {
            return Ok((root, leaves));
        }
        leaf_size = leaf_size.checked_mul(2).context("leaf size overflows")?;
    }
}

/// Writes `tiles` — `(tile id, stored bytes)` in strictly ascending tile-id order — as one archive.
/// `template` supplies every header field the writer does not own (tile type and compression, zoom
/// range, bounds, center); `metadata` is stored as given, in the template's internal compression.
/// Returns the bytes written.
pub fn write_archive(
    out: &mut impl Write,
    template: &[u8],
    metadata: &[u8],
    tiles: &[(u64, Vec<u8>)],
) -> anyhow::Result<u64> {
    ensure!(
        template.len() >= HEADER_BYTES && &template[..7] == b"PMTiles" && template[7] == 3,
        "the header template is not PMTiles v3"
    );
    ensure!(
        template[97] == COMPRESSION_GZIP,
        "the writer stores gzip directories; the template's internal compression is {}",
        template[97]
    );
    ensure!(
        tiles.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "tiles must come in strictly ascending tile-id order"
    );
    ensure!(
        tiles.iter().all(|(_, bytes)| !bytes.is_empty()),
        "a stored tile has at least one byte"
    );
    let mut entries = Vec::with_capacity(tiles.len());
    let mut data_bytes = 0_u64;
    for (tile_id, bytes) in tiles {
        entries.push(Entry {
            tile_id: *tile_id,
            offset: data_bytes,
            length: bytes.len() as u64,
            run_length: 1,
        });
        data_bytes += bytes.len() as u64;
    }
    let (root, leaves) = directories(&entries)?;
    let root_offset = HEADER_BYTES as u64;
    let metadata_offset = root_offset + root.len() as u64;
    let leaves_offset = metadata_offset + metadata.len() as u64;
    let data_offset = leaves_offset + leaves.len() as u64;
    let mut header = template[..HEADER_BYTES].to_vec();
    let count = tiles.len() as u64;
    for (at, value) in [
        (8, root_offset),
        (16, root.len() as u64),
        (24, metadata_offset),
        (32, metadata.len() as u64),
        (40, leaves_offset),
        (48, leaves.len() as u64),
        (56, data_offset),
        (64, data_bytes),
        (72, count),
        (80, count),
        (88, count),
    ] {
        header[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    // Clustered: the tile data is in tile-id order.
    header[96] = 1;
    for block in [&header, &root, &metadata.to_vec(), &leaves] {
        out.write_all(block)?;
    }
    for (_, bytes) in tiles {
        out.write_all(bytes)?;
    }
    Ok(data_offset + data_bytes)
}
