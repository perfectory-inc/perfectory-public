//! Pure, typed assembly of the Gold building row; all identities are verified at the boundary.

use std::collections::BTreeSet;

use crate::building_link_evidence::{ApprovedBuildingLinks, BuildingLinkEvidence};
use anyhow::{ensure, Context};
use catalog_domain::{
    building_id_for_register_pk, building_unit_id_for_register_pk, parcel_id_for_pnu,
};
use foundation_contracts::building_panel::BuildingPanelBuilding;
use foundation_contracts::catalog::UnitResponse;
use foundation_shared_kernel::pnu::Pnu;
use serde::{Deserialize, Serialize};
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

/// The `source` block: the Gold snapshot a document was baked from.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServingSource {
    pub(crate) table: String,
    pub(crate) iceberg_snapshot_id: String,
}

/// One PNU's served building document, typed. Its field order is the served JSON's key order.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuildingByPnuDocument {
    pub(crate) schema_version: String,
    pub(crate) pnu: String,
    pub(crate) source: ServingSource,
    pub(crate) buildings: Vec<BuildingPanelBuilding>,
    pub(crate) unlinked_units: Vec<UnitResponse>,
}

impl BuildingByPnuDocument {
    /// The served bytes: pretty JSON, newline-terminated, exactly what the object lane wrote.
    pub(crate) fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut body = serde_json::to_vec_pretty(self)
            .context("failed to serialize building serving document")?;
        body.push(b'\n');
        Ok(body)
    }
}

#[cfg(test)]
pub(super) fn build(
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
) -> anyhow::Result<BuildingServingArtifact> {
    build_with_approvals(provenance, row, &ApprovedBuildingLinks::default())
}

pub(super) fn build_with_approvals(
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
    approvals: &ApprovedBuildingLinks,
) -> anyhow::Result<BuildingServingArtifact> {
    let document = document_with_approvals(provenance, row, approvals)?;
    let body = document.to_bytes()?;
    Ok(BuildingServingArtifact {
        pnu: document.pnu,
        checksum_sha256: format!("{:x}", Sha256::digest(&body)),
        body,
    })
}

/// The typed document of one Gold row, after every identity check the served bytes rely on.
pub(super) fn document_with_approvals(
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
    approvals: &ApprovedBuildingLinks,
) -> anyhow::Result<BuildingByPnuDocument> {
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
                Some(building.register_pk.as_str()),
                approvals,
                &mut units_seen,
            )?;
        }
        buildings.push(building);
    }
    let mut unlinked_units = Vec::new();
    for raw in raw_unlinked {
        let unit: UnitResponse = serde_json::from_value(raw.clone())
            .context("unlinked_units_json does not parse as UnitResponse")?;
        validate_unit(
            &unit,
            &raw,
            parcel_id,
            None,
            None,
            approvals,
            &mut units_seen,
        )?;
        unlinked_units.push(unit);
    }
    Ok(BuildingByPnuDocument {
        schema_version: BUILDING_DOCUMENT_SCHEMA_VERSION.to_owned(),
        pnu: pnu.to_owned(),
        source: ServingSource {
            table: provenance.table.clone(),
            iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
        },
        buildings,
        unlinked_units,
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
    building_register_pk: Option<&str>,
    approvals: &ApprovedBuildingLinks,
    seen: &mut BTreeSet<uuid::Uuid>,
) -> anyhow::Result<()> {
    let pk = raw
        .get("register_pk")
        .and_then(JsonValue::as_str)
        .filter(|pk| !pk.is_empty() && pk.trim() == *pk)
        .context("Gold unit has no register_pk")?;
    ensure!(
        unit.id == building_unit_id_for_register_pk(pk),
        "unit id disagrees with catalog-domain"
    );
    let source_pnu = raw
        .get("unit_pnu")
        .and_then(JsonValue::as_str)
        .context("Gold unit is missing its source unit_pnu")?;
    let source_pnu = Pnu::parse(source_pnu.to_owned()).context("invalid source unit PNU")?;
    let source_parcel_id = parcel_id_for_pnu(&source_pnu).as_uuid();
    ensure!(
        unit.parcel_id == source_parcel_id && unit.building_id == building_id,
        "unit identity disagrees with its source parcel or parent"
    );
    ensure!(
        building_id.is_some() || source_parcel_id == parcel_id,
        "unlinked unit belongs to a different parcel"
    );
    let raw_parent = raw
        .get("building_register_pk")
        .context("Gold unit must carry an explicit nullable building_register_pk")?;
    ensure!(
        raw_parent.is_null() || raw_parent.is_string(),
        "invalid unit parent key type"
    );
    ensure!(
        building_register_pk.is_none() || raw_parent.as_str() == building_register_pk,
        "unit source parent differs from its Gold nesting"
    );
    let evidence: BuildingLinkEvidence = serde_json::from_value(
        raw.get("building_link_evidence")
            .context("Gold unit lacks building_link_evidence")?
            .clone(),
    )?;
    // A verified source parent can be absent from the visible title projection.
    // Keep that claim for evidence validation while the public unit remains unlinked.
    approvals.validate(&evidence, pk, raw_parent.as_str())?;
    ensure!(
        seen.insert(unit.id),
        "unit appears more than once in Gold row"
    );
    ensure!(
        unit.exclusive_area_m2
            .is_none_or(|a| a.is_finite() && a >= 0.0),
        "unit area is invalid"
    );
    let mut dates = BTreeSet::new();
    for price in &unit.official_price_history {
        ensure!(
            catalog_domain::unit_official_price::valid_base_date(&price.base_date)
                && price.price_won >= 0
                && dates.insert(&price.base_date),
            "unit reference-date price is invalid or duplicated"
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
        Ok(json!({"register_pk": "UNIT-1", "unit_pnu": PNU,
            "building_register_pk": building_id.map(|_| "BLDG-1"),
            "building_link_evidence": crate::building_link_evidence::source_fixture(),
            "id": building_unit_id_for_register_pk("UNIT-1"),
            "parcel_id": parcel_id_for_pnu(&Pnu::parse(PNU.to_owned())?).as_uuid(), "building_id": building_id,
            "building_name": "", "dong_name": "", "ho_name": "101", "floor_label": "1",
            "exclusive_area_m2": null, "usage_name": "", "structure_name": "", "official_price_history": []}))
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

    fn row_with_building(unit: JsonValue) -> anyhow::Result<JsonMap<String, JsonValue>> {
        let mut row = row();
        row.insert(
            "buildings_json".to_owned(),
            json!(serde_json::to_string(&json!([{
                "register_pk": "BLDG-1", "id": building_id_for_register_pk("BLDG-1"),
                "parcel_id": parcel_id_for_pnu(&Pnu::parse(PNU.to_owned())?).as_uuid(),
                "purpose_code": "03000", "structure_code": null, "floor_area_m2": null,
                "stories": null, "below_ground_floors": 0, "has_rooftop": false,
                "rooftop_area_m2": null, "rooftop_usage": "", "built_year": null,
                "floors": [], "units": [unit]
            }]))?),
        );
        Ok(row)
    }

    #[test]
    fn nested_unit_keeps_its_source_parcel_even_when_the_parent_pnu_differs() -> anyhow::Result<()>
    {
        let mut raw = unit(Some(building_id_for_register_pk("BLDG-1")))?;
        let other_pnu = "9999900000200000000";
        let own_parcel = parcel_id_for_pnu(&Pnu::parse(other_pnu.to_owned())?).as_uuid();
        raw["unit_pnu"] = json!(other_pnu);
        raw["parcel_id"] = json!(own_parcel);
        let artifact = build(&provenance(), &row_with_building(raw.clone())?)?;
        let document: JsonValue = serde_json::from_slice(&artifact.body)?;
        assert_eq!(document["pnu"], PNU);
        assert_eq!(
            document["buildings"][0]["units"][0]["parcel_id"],
            json!(own_parcel)
        );
        raw["parcel_id"] = json!(parcel_id_for_pnu(&Pnu::parse(PNU.to_owned())?).as_uuid());
        assert!(build(&provenance(), &row_with_building(raw)?).is_err());
        Ok(())
    }

    #[test]
    fn nested_unit_requires_explicit_parent_and_complete_relationship_evidence(
    ) -> anyhow::Result<()> {
        let source = unit(Some(building_id_for_register_pk("BLDG-1")))?;
        for key in ["unit_pnu", "building_register_pk", "building_link_evidence"] {
            let mut bad = source.clone();
            bad.as_object_mut().expect("unit").remove(key);
            assert!(
                build(&provenance(), &row_with_building(bad)?).is_err(),
                "{key}"
            );
        }
        for parent in [json!(null), json!("BLDG-2"), json!(false)] {
            let mut bad = source.clone();
            bad["building_register_pk"] = parent;
            assert!(build(&provenance(), &row_with_building(bad)?).is_err());
        }
        for (key, value) in [
            ("building_link_method", json!("canonical_dong")),
            ("building_link_input_sha256", json!("invalid")),
            ("building_link_reason", json!("basis_parent_conflict")),
        ] {
            let mut bad = source.clone();
            bad["building_link_evidence"][key] = value;
            assert!(
                build(&provenance(), &row_with_building(bad)?).is_err(),
                "{key}"
            );
        }
        Ok(())
    }

    #[test]
    fn proven_parent_without_a_visible_title_remains_an_unlinked_unit() -> anyhow::Result<()> {
        let mut raw = unit(None)?;
        raw["building_register_pk"] = json!("MISSING-TITLE");
        let mut gold = row();
        gold.insert(
            "unlinked_units_json".to_owned(),
            json!(serde_json::to_string(&vec![raw.clone()])?),
        );
        let artifact = build(&provenance(), &gold)?;
        let document: JsonValue = serde_json::from_slice(&artifact.body)?;
        assert_eq!(
            document["unlinked_units"][0]["building_id"],
            JsonValue::Null
        );
        assert_eq!(document["unlinked_units"][0]["id"], raw["id"]);
        for bad_parent in [json!("UNIT-1"), json!(false)] {
            let mut bad = raw.clone();
            bad["building_register_pk"] = bad_parent;
            gold.insert(
                "unlinked_units_json".to_owned(),
                json!(serde_json::to_string(&vec![bad])?),
            );
            assert!(build(&provenance(), &gold).is_err());
        }
        raw["building_link_evidence"]["building_link_input_sha256"] = json!("invalid");
        gold.insert(
            "unlinked_units_json".to_owned(),
            json!(serde_json::to_string(&vec![raw])?),
        );
        assert!(build(&provenance(), &gold).is_err());
        Ok(())
    }

    #[test]
    fn unlinked_approved_parent_still_requires_the_exact_active_binding() -> anyhow::Result<()> {
        let application = "11111111-1111-4111-8111-111111111111";
        let active = ApprovedBuildingLinks::fixture(
            application,
            "source-row:UNIT-1",
            "UNIT-1",
            Some("MISSING-TITLE"),
        );
        let mut raw = unit(None)?;
        raw["building_register_pk"] = json!("MISSING-TITLE");
        raw["building_link_evidence"] = json!({"unit_row_id":"source-row:UNIT-1",
            "building_link_method":"parent_key", "normalization_application_id":application});
        let mut gold = row();
        gold.insert(
            "unlinked_units_json".to_owned(),
            json!(serde_json::to_string(&vec![raw.clone()])?),
        );
        build_with_approvals(&provenance(), &gold, &active)?;
        assert!(build(&provenance(), &gold).is_err());
        for wrong_parent in [json!("OTHER-TITLE"), JsonValue::Null] {
            raw["building_register_pk"] = wrong_parent;
            gold.insert(
                "unlinked_units_json".to_owned(),
                json!(serde_json::to_string(&vec![raw.clone()])?),
            );
            assert!(build_with_approvals(&provenance(), &gold, &active).is_err());
        }
        Ok(())
    }

    #[test]
    fn serving_requires_the_current_approval_for_the_same_unit_and_parent() -> anyhow::Result<()> {
        let application = "11111111-1111-4111-8111-111111111111";
        let active = ApprovedBuildingLinks::fixture(
            application,
            "source-row:UNIT-1",
            "UNIT-1",
            Some("BLDG-1"),
        );
        let mut raw = unit(Some(building_id_for_register_pk("BLDG-1")))?;
        assert!(
            build_with_approvals(&provenance(), &row_with_building(raw.clone())?, &active).is_err()
        );
        raw["building_link_evidence"] = json!({
            "unit_row_id": "source-row:UNIT-1", "building_link_method": "parent_key",
            "normalization_application_id": application
        });
        let row = row_with_building(raw)?;
        build_with_approvals(&provenance(), &row, &active)?;
        assert!(build(&provenance(), &row).is_err());
        for invalid in [
            ApprovedBuildingLinks::fixture(application, "other-row", "UNIT-1", Some("BLDG-1")),
            ApprovedBuildingLinks::fixture(
                application,
                "source-row:UNIT-1",
                "UNIT-2",
                Some("BLDG-1"),
            ),
            ApprovedBuildingLinks::fixture(application, "source-row:UNIT-1", "UNIT-1", None),
        ] {
            assert!(build_with_approvals(&provenance(), &row, &invalid).is_err());
        }
        Ok(())
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
        assert_eq!(
            document["buildings"][0]["units"][0]["official_price_history"],
            json!([
                {"base_date": "20100601", "price_won": 35_000_000},
                {"base_date": "20100101", "price_won": 36_000_000}
            ])
        );
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

    #[test]
    fn refuses_annual_malformed_negative_and_duplicate_reference_dates() -> anyhow::Result<()> {
        for history in [
            json!([{"base_year": 2010, "price_won": 36_000_000}]),
            json!([{"base_date": "2010-01-01", "price_won": 36_000_000}]),
            json!([{"base_date": "20100101", "price_won": -1}]),
            json!([{"base_date": "20100101", "price_won": 36_000_000},
                   {"base_date": "20100101", "price_won": 35_000_000}]),
        ] {
            let mut priced = unit(None)?;
            priced["official_price_history"] = history;
            let mut bad = row();
            bad.insert(
                "unlinked_units_json".to_owned(),
                json!(serde_json::to_string(&vec![priced])?),
            );
            assert!(build(&provenance(), &bad).is_err());
        }
        Ok(())
    }
}
