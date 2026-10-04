//! The building document split into sections, and joined back (root ADR-0147 §1, §3).
//!
//! A section is the part of the document that changes for one reason, so it can be re-baked on
//! its own:
//!
//! | section       | holds                                                               |
//! |---------------|---------------------------------------------------------------------|
//! | `buildings`   | the document's identity and `source`, every building's own fields   |
//! | `floors`      | each building's floor rows, by building id                          |
//! | `units`       | each building's units and the unlinked units, without their prices  |
//! | `unit_prices` | each unit's reference-date official price history, by unit id       |
//!
//! The `buildings` fragment keeps the document's keys in place with empty values (`floors: []`,
//! `units: []`, `unlinked_units: []`, `official_price_history: []`); joining fills them, so the
//! joined document has the served key order. Every other fragment names the ids it belongs to in
//! the anchor's order, and joining refuses a fragment whose ids are not exactly that sequence:
//! sections baked from different Gold content must surface as an error, never as a building with
//! another building's floors.
//!
//! The contract lists the sections (`building_by_pnu_gateway.section_packs.sections`); this file
//! knows how to cut each one, and [`check_contract_sections`] refuses a contract naming a section
//! it cannot cut. The Worker's join (`foundation-building-gateway/src/packs.ts`) is held to this one
//! by the golden fixtures both read.

use anyhow::{bail, ensure, Context};
use foundation_contracts::building_panel::{BuildingPanelBuilding, BuildingPanelFloor};
use foundation_contracts::catalog::{UnitOfficialPriceResponse, UnitResponse};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::building_document::{BuildingByPnuDocument, ServingSource};
use crate::by_pnu_gateway_contract::ByPnuLane;

pub(crate) const BUILDINGS: &str = "buildings";
pub(crate) const FLOORS: &str = "floors";
pub(crate) const UNITS: &str = "units";
pub(crate) const UNIT_PRICES: &str = "unit_prices";
/// Every section this file can cut, in the order of the table above.
pub(crate) const KNOWN_SECTIONS: [&str; 4] = [BUILDINGS, FLOORS, UNITS, UNIT_PRICES];

/// The anchor fragment: the document with every nested part emptied in place.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BuildingsFragment {
    schema_version: String,
    pnu: String,
    source: ServingSource,
    buildings: Vec<BuildingPanelBuilding>,
    unlinked_units: Vec<UnitResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BuildingFloors {
    building_id: Uuid,
    floors: Vec<BuildingPanelFloor>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct UnitsFragment {
    buildings: Vec<BuildingUnits>,
    unlinked_units: Vec<UnitResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BuildingUnits {
    building_id: Uuid,
    units: Vec<UnitResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct UnitPrices {
    unit_id: Uuid,
    official_price_history: Vec<UnitOfficialPriceResponse>,
}

/// Refuses a contract whose section list is not exactly what this file cuts, in its order, with
/// `buildings` as the anchor: a section nobody bakes would 503 every request, and a section the
/// contract forgot would vanish from every document.
///
/// # Errors
/// Returns the mismatch.
pub(crate) fn check_contract_sections() -> anyhow::Result<()> {
    let packs = ByPnuLane::Building.section_packs()?;
    ensure!(
        packs.sections.iter().map(String::as_str).eq(KNOWN_SECTIONS),
        "the contract's building sections {:?} are not the sections the bake cuts {KNOWN_SECTIONS:?}",
        packs.sections
    );
    ensure!(
        packs.anchor_section == BUILDINGS,
        "the building anchor section must be {BUILDINGS}, the contract says {}",
        packs.anchor_section
    );
    Ok(())
}

/// The compact JSON fragment of one section of `document`.
///
/// # Errors
/// Refuses a section this file does not cut.
pub(crate) fn fragment(document: &BuildingByPnuDocument, section: &str) -> anyhow::Result<Vec<u8>> {
    let bytes = match section {
        BUILDINGS => serde_json::to_vec(&BuildingsFragment {
            schema_version: document.schema_version.clone(),
            pnu: document.pnu.clone(),
            source: document.source.clone(),
            buildings: document
                .buildings
                .iter()
                .map(|building| BuildingPanelBuilding {
                    floors: Vec::new(),
                    units: Vec::new(),
                    ..building.clone()
                })
                .collect(),
            unlinked_units: Vec::new(),
        }),
        FLOORS => serde_json::to_vec(
            &document
                .buildings
                .iter()
                .map(|building| BuildingFloors {
                    building_id: building.id,
                    floors: building.floors.clone(),
                })
                .collect::<Vec<_>>(),
        ),
        UNITS => serde_json::to_vec(&UnitsFragment {
            buildings: document
                .buildings
                .iter()
                .map(|building| BuildingUnits {
                    building_id: building.id,
                    units: building.units.iter().map(without_prices).collect(),
                })
                .collect(),
            unlinked_units: document.unlinked_units.iter().map(without_prices).collect(),
        }),
        UNIT_PRICES => serde_json::to_vec(
            &all_units(document)
                .map(|unit| UnitPrices {
                    unit_id: unit.id,
                    official_price_history: unit.official_price_history.clone(),
                })
                .collect::<Vec<_>>(),
        ),
        other => bail!("the building document has no section {other:?}"),
    };
    bytes.with_context(|| format!("failed to serialize the {section} fragment"))
}

fn without_prices(unit: &UnitResponse) -> UnitResponse {
    UnitResponse {
        official_price_history: Vec::new(),
        ..unit.clone()
    }
}

fn all_units(document: &BuildingByPnuDocument) -> impl Iterator<Item = &UnitResponse> {
    document
        .buildings
        .iter()
        .flat_map(|building| building.units.iter())
        .chain(document.unlinked_units.iter())
}

/// Joins the fragments of one PNU, given in [`KNOWN_SECTIONS`] order, into its document.
///
/// # Errors
/// Refuses a missing or extra fragment, a fragment that does not parse, and ids that are not
/// exactly the anchor's sequence.
pub(crate) fn join(fragments: &[(&str, &[u8])]) -> anyhow::Result<BuildingByPnuDocument> {
    let names = fragments.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    ensure!(
        names == KNOWN_SECTIONS,
        "a building document joins exactly {KNOWN_SECTIONS:?}, got {names:?}"
    );
    let parse = |index: usize| -> &[u8] { fragments[index].1 };
    let anchor: BuildingsFragment =
        serde_json::from_slice(parse(0)).context("the buildings fragment does not parse")?;
    ensure!(
        anchor
            .buildings
            .iter()
            .all(|building| building.floors.is_empty() && building.units.is_empty())
            && anchor.unlinked_units.is_empty(),
        "the buildings fragment of {} carries nested rows; they belong to other sections",
        anchor.pnu
    );
    let floors: Vec<BuildingFloors> =
        serde_json::from_slice(parse(1)).context("the floors fragment does not parse")?;
    let units: UnitsFragment =
        serde_json::from_slice(parse(2)).context("the units fragment does not parse")?;
    let prices: Vec<UnitPrices> =
        serde_json::from_slice(parse(3)).context("the unit_prices fragment does not parse")?;

    let building_ids = anchor.buildings.iter().map(|b| b.id).collect::<Vec<_>>();
    ensure!(
        floors
            .iter()
            .map(|f| f.building_id)
            .eq(building_ids.iter().copied()),
        "the floors fragment of {} names other buildings than its buildings fragment",
        anchor.pnu
    );
    ensure!(
        units
            .buildings
            .iter()
            .map(|u| u.building_id)
            .eq(building_ids.iter().copied()),
        "the units fragment of {} names other buildings than its buildings fragment",
        anchor.pnu
    );
    let mut document = BuildingByPnuDocument {
        schema_version: anchor.schema_version,
        pnu: anchor.pnu,
        source: anchor.source,
        buildings: anchor.buildings,
        unlinked_units: units.unlinked_units,
    };
    for ((building, floors), units) in document
        .buildings
        .iter_mut()
        .zip(floors)
        .zip(units.buildings)
    {
        building.floors = floors.floors;
        building.units = units.units;
    }
    let unit_count = all_units(&document).count();
    ensure!(
        prices.len() == unit_count,
        "the unit_prices fragment of {} holds {} units, its units fragment {unit_count}",
        document.pnu,
        prices.len()
    );
    let units = document
        .buildings
        .iter_mut()
        .flat_map(|building| building.units.iter_mut())
        .chain(document.unlinked_units.iter_mut());
    for (unit, price) in units.zip(prices) {
        ensure!(
            unit.id == price.unit_id && unit.official_price_history.is_empty(),
            "the unit_prices fragment names unit {} where the units fragment has {}",
            price.unit_id,
            unit.id
        );
        unit.official_price_history = price.official_price_history;
    }
    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn fixture_document() -> anyhow::Result<BuildingByPnuDocument> {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        let path = std::path::Path::new(&manifest_dir)
            .join("../../infra/lakehouse/spark/tests/fixtures/building_panel_gold_row.json");
        let row = serde_json::from_slice(&std::fs::read(path)?)?;
        let provenance = super::super::super::building_document::GoldSnapshotProvenance {
            table: "gold.building_panel".to_owned(),
            iceberg_snapshot_id: "999990000000000001".to_owned(),
            metadata_location: "s3://fixture/metadata.json".to_owned(),
            manifest_list_location: "s3://fixture/manifest.avro".to_owned(),
        };
        super::super::super::building_document::document_with_approvals(
            &provenance,
            &row,
            &crate::building_link_evidence::ApprovedBuildingLinks::default(),
        )
    }

    fn fragments(document: &BuildingByPnuDocument) -> anyhow::Result<Vec<(&'static str, Vec<u8>)>> {
        KNOWN_SECTIONS
            .iter()
            .map(|section| Ok((*section, fragment(document, section)?)))
            .collect()
    }

    fn joined(parts: &[(&'static str, Vec<u8>)]) -> anyhow::Result<BuildingByPnuDocument> {
        join(
            &parts
                .iter()
                .map(|(name, bytes)| (*name, bytes.as_slice()))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn the_contract_names_exactly_the_sections_the_bake_cuts() -> anyhow::Result<()> {
        check_contract_sections()
    }

    /// Split then join gives back the served bytes exactly: the pack lane serves what the object
    /// lane served.
    #[test]
    fn split_then_join_is_the_served_document_byte_for_byte() -> anyhow::Result<()> {
        let document = fixture_document()?;
        assert!(!document.buildings[0].floors.is_empty());
        assert!(!document.buildings[0].units[0]
            .official_price_history
            .is_empty());
        let parts = fragments(&document)?;
        assert_eq!(joined(&parts)?.to_bytes()?, document.to_bytes()?);
        Ok(())
    }

    /// Each fragment carries only its own part: the anchor holds no floors, units or prices, and
    /// the units fragment holds no prices.
    #[test]
    fn each_fragment_carries_only_its_part() -> anyhow::Result<()> {
        let document = fixture_document()?;
        let anchor: serde_json::Value = serde_json::from_slice(&fragment(&document, BUILDINGS)?)?;
        assert_eq!(anchor["buildings"][0]["floors"], serde_json::json!([]));
        assert_eq!(anchor["buildings"][0]["units"], serde_json::json!([]));
        assert_eq!(anchor["unlinked_units"], serde_json::json!([]));
        let units: serde_json::Value = serde_json::from_slice(&fragment(&document, UNITS)?)?;
        assert_eq!(
            units["buildings"][0]["units"][0]["official_price_history"],
            serde_json::json!([])
        );
        assert!(fragment(&document, "zonings").is_err());
        Ok(())
    }

    /// Fragments of different content must not join silently: another building's floors, a
    /// missing unit price, a reordered section list are all refused.
    #[test]
    fn fragments_that_disagree_are_refused() -> anyhow::Result<()> {
        let document = fixture_document()?;
        let mut other = document.clone();
        other.buildings[0].id = Uuid::nil();
        let mismatched = [
            (BUILDINGS, fragment(&document, BUILDINGS)?),
            (FLOORS, fragment(&other, FLOORS)?),
            (UNITS, fragment(&document, UNITS)?),
            (UNIT_PRICES, fragment(&document, UNIT_PRICES)?),
        ];
        assert!(
            joined(&mismatched).is_err(),
            "another building's floors were joined"
        );

        let mut fewer = document.clone();
        fewer.unlinked_units.clear();
        let short_prices = [
            (BUILDINGS, fragment(&document, BUILDINGS)?),
            (FLOORS, fragment(&document, FLOORS)?),
            (UNITS, fragment(&document, UNITS)?),
            (UNIT_PRICES, fragment(&fewer, UNIT_PRICES)?),
        ];
        assert!(
            joined(&short_prices).is_err(),
            "a unit without its price row was joined"
        );

        let mut parts = fragments(&document)?;
        parts.swap(1, 2);
        assert!(
            joined(&parts).is_err(),
            "a reordered section list was joined"
        );
        let mut nested = fragments(&document)?;
        nested[0].1 = fragment(&document, BUILDINGS)?;
        let mut anchor: serde_json::Value = serde_json::from_slice(&nested[0].1)?;
        anchor["buildings"][0]["floors"] = serde_json::to_value(&document.buildings[0].floors)?;
        nested[0].1 = serde_json::to_vec(&anchor)?;
        assert!(
            joined(&nested).is_err(),
            "an anchor carrying floors was joined"
        );
        Ok(())
    }
}
