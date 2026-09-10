//! Pure, typed assembly of the Gold building row; all identities are verified at the boundary.

use std::collections::BTreeSet;

use anyhow::{ensure, Context};
use catalog_domain::{
    building_id_for_register_pk, building_unit_id_for_register_pk, parcel_id_for_pnu,
};
use foundation_contracts::building_panel::BuildingPanelBuilding;
use foundation_contracts::catalog::UnitResponse;
use foundation_shared_kernel::pnu::Pnu;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

pub(crate) use foundation_contracts::building_panel::BUILDING_DOCUMENT_SCHEMA_VERSION;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BuildingServingArtifact {
    pub(super) pnu: String,
    pub(super) body: Vec<u8>,
    pub(super) checksum_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct GoldSnapshotProvenance {
    pub(super) table: String,
    pub(super) iceberg_snapshot_id: String,
    pub(super) metadata_location: String,
    pub(super) manifest_list_location: String,
}

#[derive(Serialize)]
struct ServingSource<'a> {
    table: &'a str,
    iceberg_snapshot_id: &'a str,
}

#[derive(Serialize)]
struct BuildingByPnuDocument<'a> {
    schema_version: &'static str,
    pnu: &'a str,
    source: ServingSource<'a>,
    buildings: Vec<BuildingPanelBuilding>,
    unlinked_units: Vec<UnitResponse>,
}

pub(super) fn build(
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
) -> anyhow::Result<BuildingServingArtifact> {
    let pnu = row
        .get("pnu")
        .and_then(JsonValue::as_str)
        .context("gold.building_panel row is missing pnu")?;
    let parsed_pnu = Pnu::parse(pnu.to_owned()).context("invalid Gold building PNU")?;
    let parcel_id = parcel_id_for_pnu(&parsed_pnu).as_uuid();
    let raw_buildings = section(row, "buildings_json")?;
    let raw_unlinked = section(row, "unlinked_units_json")?;
    let mut buildings_seen = BTreeSet::new();
    let mut units_seen = BTreeSet::new();
    let mut buildings = Vec::new();
    for raw in raw_buildings {
        let building: BuildingPanelBuilding = serde_json::from_value(raw.clone())
            .context("buildings_json does not parse as the public building contract")?;
        ensure!(
            !building.register_pk.trim().is_empty(),
            "building register_pk is absent"
        );
        ensure!(
            building.id == building_id_for_register_pk(&building.register_pk),
            "building id disagrees with catalog-domain"
        );
        ensure!(
            building.parcel_id == parcel_id,
            "building belongs to a different parcel"
        );
        ensure!(
            buildings_seen.insert(building.id),
            "duplicate building in Gold row"
        );
        ensure!(
            building.stories.is_none_or(|n| n >= 0) && building.below_ground_floors >= 0,
            "negative building floor count"
        );
        ensure!(
            building
                .floor_area_m2
                .is_none_or(|n| n.is_finite() && n > 0.0),
            "building floor area must be positive or absent"
        );
        ensure!(
            building
                .rooftop_area_m2
                .is_none_or(|n| n.is_finite() && n >= 0.0),
            "rooftop area must be non-negative or absent"
        );
        let mut floors = BTreeSet::new();
        for floor in &building.floors {
            ensure!(
                !floor.floor_row_id.is_empty() && floors.insert(&floor.floor_row_id),
                "duplicate or absent floor identity"
            );
        }
        let raw_units = raw
            .get("units")
            .and_then(JsonValue::as_array)
            .context("building units must be an array")?;
        for (unit, raw_unit) in building.units.iter().zip(raw_units) {
            validate_unit(
                unit,
                raw_unit,
                parcel_id,
                Some(building.id),
                &mut units_seen,
            )?;
        }
        buildings.push(building);
    }
    let mut unlinked_units = Vec::new();
    for raw in raw_unlinked {
        let unit: UnitResponse = serde_json::from_value(raw.clone())
            .context("unlinked_units_json does not parse as UnitResponse")?;
        validate_unit(&unit, &raw, parcel_id, None, &mut units_seen)?;
        unlinked_units.push(unit);
    }
    let document = BuildingByPnuDocument {
        schema_version: BUILDING_DOCUMENT_SCHEMA_VERSION,
        pnu,
        source: ServingSource {
            table: &provenance.table,
            iceberg_snapshot_id: &provenance.iceberg_snapshot_id,
        },
        buildings,
        unlinked_units,
    };
    let mut body = serde_json::to_vec_pretty(&document)
        .context("failed to serialize building serving document")?;
    body.push(b'\n');
    Ok(BuildingServingArtifact {
        pnu: pnu.to_owned(),
        checksum_sha256: format!("{:x}", Sha256::digest(&body)),
        body,
    })
}

fn section(row: &JsonMap<String, JsonValue>, key: &str) -> anyhow::Result<Vec<JsonValue>> {
    let raw = row
        .get(key)
        .and_then(JsonValue::as_str)
        .with_context(|| format!("gold.building_panel is missing {key}"))?;
    serde_json::from_str(raw)
        .with_context(|| format!("gold.building_panel {key} must be a JSON array"))
}

fn validate_unit(
    unit: &UnitResponse,
    raw: &JsonValue,
    parcel_id: uuid::Uuid,
    building_id: Option<uuid::Uuid>,
    seen: &mut BTreeSet<uuid::Uuid>,
) -> anyhow::Result<()> {
    let pk = raw
        .get("register_pk")
        .and_then(JsonValue::as_str)
        .filter(|pk| !pk.trim().is_empty())
        .context("Gold unit has no register_pk")?;
    ensure!(
        unit.id == building_unit_id_for_register_pk(pk),
        "unit id disagrees with catalog-domain"
    );
    ensure!(
        unit.parcel_id == parcel_id && unit.building_id == building_id,
        "unit attachment disagrees with its parent"
    );
    ensure!(
        seen.insert(unit.id),
        "unit appears more than once in Gold row"
    );
    ensure!(
        unit.exclusive_area_m2
            .is_none_or(|a| a.is_finite() && a >= 0.0),
        "unit area is invalid"
    );
    let mut years = BTreeSet::new();
    for price in &unit.official_price_history {
        ensure!(
            price.base_year > 0 && price.price_won >= 0 && years.insert(price.base_year),
            "unit annual price is invalid or duplicated"
        );
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use serde_json::json;

    const PNU: &str = "9999900000100000000";

    pub(crate) fn unit(building_id: Option<uuid::Uuid>) -> anyhow::Result<JsonValue> {
        // The reserved fixture PNU is parsed fallibly in tests that use this helper.
        Ok(
            json!({"register_pk": "UNIT-1", "id": building_unit_id_for_register_pk("UNIT-1"),
            "parcel_id": parcel_id_for_pnu(&Pnu::parse(PNU.to_owned())?).as_uuid(), "building_id": building_id,
            "building_name": "", "dong_name": "", "ho_name": "101", "floor_label": "1",
            "exclusive_area_m2": null, "usage_name": "", "structure_name": "", "official_price_history": []}),
        )
    }

    fn provenance() -> GoldSnapshotProvenance {
        GoldSnapshotProvenance {
            table: "gold.building_panel".to_owned(),
            iceberg_snapshot_id: "999990000000000001".to_owned(),
            metadata_location: "s3://fixture/metadata.json".to_owned(),
            manifest_list_location: "s3://fixture/manifest.avro".to_owned(),
        }
    }

    fn row() -> JsonMap<String, JsonValue> {
        JsonMap::from_iter([
            ("pnu".to_owned(), json!(PNU)),
            ("buildings_json".to_owned(), json!("[]")),
            ("unlinked_units_json".to_owned(), json!("[]")),
        ])
    }

    #[test]
    fn spark_fixture_matches_catalog_identity_and_public_dtos() -> anyhow::Result<()> {
        let manifest_dir =
            std::env::var("CARGO_MANIFEST_DIR").context("Cargo test manifest directory")?;
        let path = std::path::Path::new(&manifest_dir)
            .join("../../infra/lakehouse/spark/tests/fixtures/building_panel_gold_row.json");
        let row: JsonMap<String, JsonValue> = serde_json::from_slice(&std::fs::read(path)?)?;
        let artifact = build(&provenance(), &row)?;
        let document: JsonValue = serde_json::from_slice(&artifact.body)?;
        let mut building = document["buildings"][0].clone();
        let object = building
            .as_object_mut()
            .context("fixture building is not an object")?;
        object.remove("floors");
        object.remove("units");
        object.insert("updated_at".to_owned(), json!("2026-01-01T00:00:00Z"));
        let canonical: foundation_contracts::catalog::BuildingResponse =
            serde_json::from_value(building.clone())?;
        assert_eq!(
            serde_json::to_value(canonical)?,
            building,
            "building panel fields drifted from the Catalog DTO"
        );
        assert_eq!(
            document["buildings"][0]["units"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(document["unlinked_units"].as_array().map(Vec::len), Some(1));
        assert!(document["buildings"][0]["units"][0]
            .get("register_pk")
            .is_none());
        Ok(())
    }

    #[test]
    fn same_snapshot_and_content_have_identical_bytes_without_runtime_fields() -> anyhow::Result<()>
    {
        let first = build(&provenance(), &row())?;
        let mut other = row();
        other.insert("published_at_utc".to_owned(), json!("2099-01-01T00:00:00Z"));
        let mut source = provenance();
        source.metadata_location = "s3://fixture/moved-metadata.json".to_owned();
        assert_eq!(first, build(&source, &other)?);
        let body: JsonValue = serde_json::from_slice(&first.body)?;
        assert_eq!(body["buildings"], json!([]));
        assert!(body["source"].get("metadata_location").is_none());
        Ok(())
    }

    #[test]
    fn refuses_missing_sections_bad_pnu_and_foreign_identity() -> anyhow::Result<()> {
        let mut missing = row();
        missing.remove("buildings_json");
        assert!(build(&provenance(), &missing).is_err());
        let mut bad = row();
        bad.insert("pnu".to_owned(), json!("../outside"));
        assert!(build(&provenance(), &bad).is_err());
        let mut foreign = unit(None)?;
        foreign["id"] = json!(uuid::Uuid::nil());
        let mut bad = row();
        bad.insert(
            "unlinked_units_json".to_owned(),
            json!(serde_json::to_string(&vec![foreign])?),
        );
        assert!(build(&provenance(), &bad).is_err());
        Ok(())
    }
}
