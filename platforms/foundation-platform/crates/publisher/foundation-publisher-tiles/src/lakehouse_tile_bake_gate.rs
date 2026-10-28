//! The archive gate: a PMTiles header that matches the layer, and maxzoom tiles that carry exactly
//! the served feature ids but the sub-pixel ones (ADR-0262).

use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{bail, ensure, Context as _};

use super::{
    for_each_handoff_line, id_hash, pmtiles_feature_ids, subpixel, ServedIds, ServedRow,
    ServedSummary, MAX_CLASSIFIED_MISSING, NAMED_IDS,
};

/// The layer the active release serves, which the new archive must reproduce.
#[derive(Debug, Clone)]
pub struct ServedLayer {
    pub source_layer: String,
    pub feature_id_property: String,
    pub tile_min_zoom: u8,
    pub tile_max_zoom: u8,
    /// The deepest zoom the map draws this layer at, overzooming the maxzoom tiles (ADR-0262).
    pub render_max_zoom: u8,
    pub properties: Vec<String>,
}

/// Checks a PMTiles v3 header: MVT tiles over exactly the layer's zoom range, in compressions the
/// gate can read.
pub fn check_pmtiles_header(header: &[u8], layer: &ServedLayer) -> anyhow::Result<()> {
    use pmtiles_feature_ids::{Header, COMPRESSION_GZIP, COMPRESSION_NONE, TILE_TYPE_MVT};
    let header = Header::parse(header)?;
    ensure!(
        header.tile_type == TILE_TYPE_MVT,
        "archive tiles are not MVT"
    );
    ensure!(
        (header.min_zoom, header.max_zoom) == (layer.tile_min_zoom, layer.tile_max_zoom),
        "archive zooms {}..{} differ from the layer's {}..{}",
        header.min_zoom,
        header.max_zoom,
        layer.tile_min_zoom,
        layer.tile_max_zoom
    );
    for (what, code) in [
        ("directory", header.internal_compression),
        ("tile", header.tile_compression),
    ] {
        ensure!(
            [COMPRESSION_NONE, COMPRESSION_GZIP].contains(&code),
            "archive {what} compression {code} is not one the gate reads"
        );
    }
    Ok(())
}

/// The gate: the archive's header matches the layer, and its maxzoom tiles carry exactly the
/// served ids — none missing, none extra. A feature appears in every maxzoom tile it touches, so
/// presence is a bit per served id. Returns the evidence digest of the id set.
pub fn gate_archive(
    archive: &Path,
    layer: &ServedLayer,
    columns: &[String],
    ids: &ServedIds,
    classify_missing: impl FnOnce(&[u128]) -> anyhow::Result<Missing>,
    observe: &mut dyn FnMut(u64, &str) -> anyhow::Result<()>,
) -> anyhow::Result<IdGate> {
    let mut header = vec![0_u8; pmtiles_feature_ids::HEADER_BYTES];
    File::open(archive)
        .and_then(|mut file| file.read_exact(&mut header))
        .context("archive is shorter than a PMTiles header")?;
    check_pmtiles_header(&header, layer)?;
    let mut found = vec![0_u64; ids.len().div_ceil(64)];
    let (mut extra, mut extra_named) = (0_u64, Vec::new());
    pmtiles_feature_ids::for_each_feature_id(
        archive,
        &pmtiles_feature_ids::Expect {
            zoom: layer.tile_max_zoom,
            layer: &layer.source_layer,
            id_property: &layer.feature_id_property,
            properties: columns,
        },
        |tile, id| {
            observe(tile, id)?;
            if let Some(index) = ids.position(id_hash(id)) {
                found[index / 64] |= 1_u64 << (index % 64);
            } else {
                extra += 1;
                if extra_named.len() < NAMED_IDS && !extra_named.iter().any(|named| named == id) {
                    extra_named.push(id.to_owned());
                }
            }
            Ok(())
        },
    )?;
    let present = |index: usize| found[index / 64] & (1_u64 << (index % 64)) != 0;
    let missing: Vec<u128> = ids
        .sorted
        .iter()
        .enumerate()
        .filter(|(index, _)| !present(*index))
        .map(|(_, hash)| *hash)
        .collect();
    // A bake that lost this many is broken, not tiny; it is refused without reading the handoff.
    ensure!(
        missing.len() <= MAX_CLASSIFIED_MISSING && extra == 0,
        "maxzoom tiles miss {} of the {} served ids and carry {extra} feature(s) the snapshot does not serve; extra {extra_named:?}",
        missing.len(),
        ids.len()
    );
    let classified = if missing.is_empty() {
        Missing::default()
    } else {
        classify_missing(&missing)?
    };
    ensure!(
        classified.subpixel.len() + classified.unexplained.len() == missing.len(),
        "the handoff named {} of the {} ids the maxzoom tiles miss",
        classified.subpixel.len() + classified.unexplained.len(),
        missing.len()
    );
    if !classified.unexplained.is_empty() {
        let named: Vec<&String> = classified.unexplained.iter().take(NAMED_IDS).collect();
        bail!(
            "maxzoom tiles miss {} of the {} served ids: {} sub-pixel at render_max_zoom {} (allowed, ADR-0262) and {} that are not; not sub-pixel {named:?}",
            missing.len(),
            ids.len(),
            classified.subpixel.len(),
            layer.render_max_zoom,
            classified.unexplained.len()
        );
    }
    Ok(IdGate {
        digest: ids.digest(),
        subpixel: classified.subpixel,
    })
}

/// The served ids the maxzoom tiles miss, by whether their own geometry explains it (ADR-0262).
#[derive(Debug, Default)]
pub struct Missing {
    /// Sub-pixel at the layer's render_max_zoom: nothing the map could draw. Allowed, listed.
    pub subpixel: Vec<String>,
    /// Anything else, an unreadable geometry included. Refused.
    pub unexplained: Vec<String>,
}

/// What the maxzoom id gate proves: the archive carries every served id but the listed sub-pixel ones.
#[derive(Debug)]
pub struct IdGate {
    pub digest: String,
    pub subpixel: Vec<String>,
}

/// Reads the handoff again for the rows whose id hash is in `missing` and classifies each by its
/// geometry (ADR-0262).
pub(super) fn classify_missing(
    summary: &ServedSummary,
    handoff: &Path,
    missing: &[u128],
    render_max_zoom: u8,
) -> anyhow::Result<Missing> {
    let wanted: std::collections::HashSet<u128> = missing.iter().copied().collect();
    let mut out = Missing::default();
    for_each_handoff_line(summary, handoff, |line, location| {
        let row: ServedRow = serde_json::from_str(line)
            .with_context(|| format!("{} is not a served row", location()))?;
        if wanted.contains(&id_hash(&row.feature_id)) {
            let subpixel = hex_bytes(&row.geometry_wkb_hex)
                .and_then(|wkb| subpixel::is_subpixel(&wkb, render_max_zoom));
            match subpixel {
                Ok(true) => out.subpixel.push(row.feature_id),
                Ok(false) | Err(_) => out.unexplained.push(row.feature_id),
            }
        }
        Ok(())
    })?;
    out.subpixel.sort_unstable();
    out.unexplained.sort_unstable();
    Ok(out)
}

fn hex_bytes(hex: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(hex.len().is_multiple_of(2), "odd-length WKB hex");
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).context("WKB hex"))
        .collect()
}
