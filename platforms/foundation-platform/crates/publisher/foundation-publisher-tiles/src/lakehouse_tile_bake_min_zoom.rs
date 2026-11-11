//! Root ADR-0375 §3: a unit's features may start at a deeper zoom than the layer's minzoom, by one
//! tile property (zoning: the 도시계획시설 layers from z11, measured from the densest z10 tile). The
//! contract's map becomes tippecanoe's `-j` feature filter on `$zoom`, so it is part of the bake
//! inputs: an index or a partition archive made under other inputs is not reused.

use std::collections::BTreeMap;

use anyhow::ensure;
use serde::Deserialize;

use super::ServedLayer;

/// `config/tile-bake-containers.contract.json` `feature_min_zoom.<unit>`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureMinZoom {
    /// The tile property the zoom is chosen by (`layer_code`).
    pub property: String,
    /// The first zoom of the features whose property has this value.
    pub min_zoom: BTreeMap<String, u8>,
    /// The first zoom of every other feature.
    pub default: u8,
}

impl FeatureMinZoom {
    pub(super) fn validate(&self, unit: &str) -> anyhow::Result<()> {
        ensure!(
            !self.property.is_empty() && !self.min_zoom.is_empty(),
            "the {unit} feature_min_zoom names a property and at least one value"
        );
        ensure!(
            self.min_zoom
                .values()
                .chain([&self.default])
                .all(|zoom| *zoom <= 24),
            "the {unit} feature_min_zoom zooms are at most 24"
        );
        Ok(())
    }

    /// The layer's zooms must hold every first zoom, so each feature still reaches the maxzoom
    /// tiles the id gate reads.
    pub(super) fn validate_for(&self, unit: &str, layer: &ServedLayer) -> anyhow::Result<()> {
        ensure!(
            self.min_zoom
                .values()
                .chain([&self.default])
                .all(|zoom| (layer.tile_min_zoom..=layer.tile_max_zoom).contains(zoom)),
            "the {unit} feature_min_zoom zooms lie outside the layer's {}..{}",
            layer.tile_min_zoom,
            layer.tile_max_zoom
        );
        ensure!(
            layer.properties.contains(&self.property),
            "the {unit} feature_min_zoom property {} is not a tile property",
            self.property
        );
        Ok(())
    }

    /// The `-j` clauses for the layer: a feature whose first zoom is past the layer's minzoom
    /// passes only from that zoom on. Empty when every feature starts at the minzoom.
    fn clauses(&self, layer: &ServedLayer) -> Vec<serde_json::Value> {
        let mut by_zoom: BTreeMap<u8, Vec<&str>> = BTreeMap::new();
        for (value, zoom) in &self.min_zoom {
            by_zoom.entry(*zoom).or_default().push(value);
        }
        let mut clauses: Vec<serde_json::Value> = by_zoom
            .iter()
            .filter(|(zoom, _)| **zoom > layer.tile_min_zoom)
            .map(|(zoom, values)| {
                let mut not_in = vec![serde_json::json!("!in"), serde_json::json!(self.property)];
                not_in.extend(values.iter().map(|value| serde_json::json!(value)));
                serde_json::json!(["any", not_in, [">=", "$zoom", zoom]])
            })
            .collect();
        if self.default > layer.tile_min_zoom {
            let mut listed = vec![serde_json::json!("in"), serde_json::json!(self.property)];
            listed.extend(self.min_zoom.keys().map(|value| serde_json::json!(value)));
            clauses.push(serde_json::json!([
                "any",
                listed,
                [">=", "$zoom", self.default]
            ]));
        }
        clauses
    }
}

/// `config/tile-bake-containers.contract.json` `low_zoom_generalization.<unit>` (W42): below the
/// layer's maxzoom a tile carries only `properties`, features left with equal properties are merged
/// into one (tippecanoe's polygon cleaning unions them), and lines and polygons are simplified at
/// `simplification` tile units. The maxzoom tiles, which the id gate reads, keep every property and
/// tippecanoe's standard tolerance.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LowZoomGeneralization {
    pub properties: Vec<String>,
    pub simplification: u8,
}

impl LowZoomGeneralization {
    pub(super) fn validate(&self, unit: &str) -> anyhow::Result<()> {
        ensure!(
            !self.properties.is_empty(),
            "the {unit} low_zoom_generalization keeps at least one property"
        );
        // tippecanoe's README: "You can probably go up to about 10 without too much visible difference".
        ensure!(
            (1..=10).contains(&self.simplification),
            "the {unit} low_zoom_generalization simplification is between 1 and 10"
        );
        Ok(())
    }

    pub(super) fn validate_for(&self, unit: &str, layer: &ServedLayer) -> anyhow::Result<()> {
        ensure!(
            self.properties
                .iter()
                .all(|property| layer.properties.contains(property)),
            "the {unit} low_zoom_generalization keeps a property that is not a tile property"
        );
        ensure!(
            !self.properties.contains(&layer.feature_id_property),
            "the {unit} low_zoom_generalization keeps the feature id, so no two features could merge"
        );
        Ok(())
    }

    /// tippecanoe's flags besides the `-j` clauses.
    pub fn args(&self) -> [String; 4] {
        [
            format!("--simplification={}", self.simplification),
            "--simplification-at-maximum-zoom=1".to_owned(),
            "--coalesce".to_owned(),
            "--reorder".to_owned(),
        ]
    }

    /// An `attribute-filter` clause per tile column not kept: present only from the maxzoom on.
    fn clauses(&self, layer: &ServedLayer, columns: &[String]) -> Vec<serde_json::Value> {
        columns
            .iter()
            .filter(|column| !self.properties.contains(column))
            .map(|column| {
                serde_json::json!([
                    "attribute-filter",
                    column,
                    [">=", "$zoom", layer.tile_max_zoom]
                ])
            })
            .collect()
    }
}

/// tippecanoe's `-j` filter for the layer from both zoom rules; `None` when neither adds a clause.
pub fn feature_filter(
    layer: &ServedLayer,
    columns: &[String],
    min_zoom: Option<&FeatureMinZoom>,
    generalization: Option<&LowZoomGeneralization>,
) -> Option<String> {
    let mut all = vec![serde_json::json!("all")];
    all.extend(
        min_zoom
            .map(|min_zoom| min_zoom.clauses(layer))
            .unwrap_or_default(),
    );
    all.extend(
        generalization
            .map(|generalization| generalization.clauses(layer, columns))
            .unwrap_or_default(),
    );
    (all.len() > 1).then(|| serde_json::json!({ layer.source_layer.as_str(): all }).to_string())
}

/// The deepest zoom a unit may be cut to: the render zooms Catalog accepts end at 22.
const MAX_TILE_ZOOM: u8 = 22;

/// Root ADR-0492: a contract maxzoom is a zoom Catalog serves, and a born unit's agrees with its
/// birth layer.
pub(super) fn validate_tile_max_zoom(contract: &super::BakeContract) -> anyhow::Result<()> {
    for (unit, zoom) in &contract.tile_max_zoom {
        anyhow::ensure!(
            (1..=MAX_TILE_ZOOM).contains(zoom),
            "{unit}'s tile_max_zoom {zoom} is not between 1 and {MAX_TILE_ZOOM}"
        );
        if let Some(newborn) = contract.units_born_from_a_lakehouse_bake.get(unit) {
            let born = newborn.served_layer().tile_max_zoom;
            anyhow::ensure!(
                born == *zoom,
                "{unit} is born at z{born} but its tile_max_zoom is {zoom}"
            );
        }
    }
    Ok(())
}
