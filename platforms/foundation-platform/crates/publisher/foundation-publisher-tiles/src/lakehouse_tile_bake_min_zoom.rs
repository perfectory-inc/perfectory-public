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

    /// tippecanoe's `-j` filter for the layer: a feature whose first zoom is past the layer's
    /// minzoom passes only from that zoom on. `None` when every feature starts at the minzoom.
    pub fn filter(&self, layer: &ServedLayer) -> Option<String> {
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
        if clauses.is_empty() {
            return None;
        }
        let mut all = vec![serde_json::json!("all")];
        all.extend(clauses);
        Some(serde_json::json!({ layer.source_layer.as_str(): all }).to_string())
    }
}
