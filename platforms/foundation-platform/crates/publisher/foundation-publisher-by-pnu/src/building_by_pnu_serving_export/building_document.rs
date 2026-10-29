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

pub use foundation_contracts::building_panel::{
    BUILDING_DOCUMENT_SCHEMA_EXPANDS, BUILDING_DOCUMENT_SCHEMA_VERSION,
    BUILDING_PAGE_SCHEMA_VERSION,
};

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
#[cfg_attr(test, derive(utoipa::ToSchema))]
pub struct ServingSource {
    pub table: String,
    pub iceberg_snapshot_id: String,
}

/// One PNU's served building document, typed. Its field order is the served JSON's key order.
/// Its fields are recorded per schema version (`contracts/by-pnu-documents`): a field gained, lost
/// or renamed here or in `foundation-contracts` is a new `BUILDING_DOCUMENT_SCHEMA_VERSION`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(utoipa::ToSchema))]
pub struct BuildingByPnuDocument {
    pub schema_version: String,
    pub pnu: String,
    pub source: ServingSource,
    pub buildings: Vec<BuildingPanelBuilding>,
    pub unlinked_units: Vec<UnitResponse>,
}

impl BuildingByPnuDocument {
    /// The served bytes: pretty JSON, newline-terminated, exactly what the object lane wrote.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut body = serde_json::to_vec_pretty(self)
            .context("failed to serialize building serving document")?;
        body.push(b'\n');
        Ok(body)
    }
}

/// Where one page sits among its document's pages (root ADR-0325); page 1 answers the PNU's own
/// path, page `n` the path `<pnu>/<n>`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(utoipa::ToSchema))]
pub struct PageOf {
    pub index: u32,
    pub count: u32,
}

/// One page of a building document too large for one request (root ADR-0325). The pages, in
/// order, hold the document's buildings and unlinked units; a building cut across pages appears on
/// each with its fields, its floors on its first page only, and the next page's units continue
/// its own. A reader joins consecutive buildings of the same id ([`join_pages`]).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(utoipa::ToSchema))]
pub struct BuildingByPnuPage {
    pub schema_version: String,
    pub pnu: String,
    pub source: ServingSource,
    pub page: PageOf,
    pub buildings: Vec<BuildingPanelBuilding>,
    pub unlinked_units: Vec<UnitResponse>,
    /// Why the document is not served (root ADR-0325 §4): it cannot be cut into pages within the
    /// contract. The page then holds nothing else and is the only one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withheld: Option<String>,
}

impl BuildingByPnuPage {
    /// The served bytes, written the way the whole document's are.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut body =
            serde_json::to_vec_pretty(self).context("failed to serialize a building page")?;
        body.push(b'\n');
        Ok(body)
    }
}

/// What one PNU's building document is served as (root ADR-0325).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Served {
    /// The document whole: its member fits what one request may copy.
    Whole(Vec<u8>),
    /// Its pages, in order, each member within the contract's page target.
    Pages(Vec<Vec<u8>>),
    /// One page saying why the document is withheld, and the reason.
    Withheld { page: Vec<u8>, reason: String },
}

/// How `document` is served: whole while its gzip member fits `document_max_bytes`, else cut into
/// pages; a document no cut fits is withheld, so one document never stops its lane (2026-10-28:
/// one PNU's 268,404-byte member refused the whole building bake six times).
///
/// # Errors
/// Returns an error when the document does not serialize or the contract cannot be read.
pub(super) fn served(document: &BuildingByPnuDocument) -> anyhow::Result<Served> {
    let whole = document.to_bytes()?;
    let budget = &crate::by_pnu_gateway_contract::section_pack_policy()?
        .read_path
        .request_cpu_budget;
    let bound = usize::try_from(budget.document_max_bytes)?;
    // Deflate adds at most 5 bytes per 16 KiB block and an 18-byte frame: a document this short
    // fits without compressing it twice.
    if whole.len() + whole.len() / 1024 + 64 <= bound {
        return Ok(Served::Whole(whole));
    }
    let member = crate::by_pnu_pack::member_length(&whole)?;
    if member <= bound {
        return Ok(Served::Whole(whole));
    }
    let target = usize::try_from(budget.pages.page_target_bytes)?;
    let max_pages = usize::try_from(budget.pages.max_pages)?;
    match paginate(document, member, target, max_pages) {
        Ok(pages) => Ok(Served::Pages(pages)),
        Err(error) => {
            let reason = format!(
                "document_too_large: its {member}-byte member does not cut into at most \
                 {max_pages} pages of {target} bytes ({error:#})"
            );
            let page = BuildingByPnuPage {
                schema_version: BUILDING_PAGE_SCHEMA_VERSION.to_owned(),
                pnu: document.pnu.clone(),
                source: document.source.clone(),
                page: PageOf { index: 1, count: 1 },
                buildings: Vec::new(),
                unlinked_units: Vec::new(),
                withheld: Some(reason.clone()),
            };
            Ok(Served::Withheld {
                page: page.to_bytes()?,
                reason,
            })
        }
    }
}

/// One piece of a document a page boundary may fall between: a building with its fields and
/// floors, one of its units, or one unlinked unit.
#[derive(Clone, Copy)]
enum Piece {
    Building(usize),
    Unit(usize, usize),
    Unlinked(usize),
}

/// The fewest pages, from the member's share of `target` up to `max_pages`, whose every member is
/// within `target`; they join back into exactly `document`.
fn paginate(
    document: &BuildingByPnuDocument,
    member: usize,
    target: usize,
    max_pages: usize,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut pieces = Vec::new();
    for (at, building) in document.buildings.iter().enumerate() {
        let mut fields = building.clone();
        fields.units.clear();
        pieces.push((Piece::Building(at), serde_json::to_vec(&fields)?.len()));
        for (unit, value) in building.units.iter().enumerate() {
            pieces.push((Piece::Unit(at, unit), serde_json::to_vec(value)?.len()));
        }
    }
    for (at, value) in document.unlinked_units.iter().enumerate() {
        pieces.push((Piece::Unlinked(at), serde_json::to_vec(value)?.len()));
    }
    // A piece no page can hold ends the search at once: more pages never shrink it. Only a piece
    // longer than the target can compress past it.
    for (piece, size) in &pieces {
        if *size > target {
            let alone = match *piece {
                Piece::Building(at) => {
                    let mut fields = document.buildings[at].clone();
                    fields.units.clear();
                    serde_json::to_vec(&fields)?
                }
                Piece::Unit(at, unit) => serde_json::to_vec(&document.buildings[at].units[unit])?,
                Piece::Unlinked(at) => serde_json::to_vec(&document.unlinked_units[at])?,
            };
            let member = crate::by_pnu_pack::member_length(&alone)?;
            ensure!(
                member <= target,
                "one building or unit alone has a {member}-byte member"
            );
        }
    }
    let whole = document.to_bytes()?;
    let mut count = member.div_ceil(target).max(2);
    let mut largest = member;
    while count <= max_pages {
        let pages = cut(document, &pieces, count);
        let bytes = pages
            .iter()
            .map(BuildingByPnuPage::to_bytes)
            .collect::<anyhow::Result<Vec<_>>>()?;
        largest = bytes
            .iter()
            .map(|page| crate::by_pnu_pack::member_length(page))
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        if largest <= target && bytes.len() >= 2 {
            ensure!(
                join_pages(&pages)?.to_bytes()? == whole,
                "the pages of {} do not join back into its document",
                document.pnu
            );
            return Ok(bytes);
        }
        // The largest page's overshoot says about how many more pages it takes; at least one.
        count = (count + 1).max((count * largest).div_ceil(target));
    }
    anyhow::bail!("the largest page still has a {largest}-byte member")
}

/// `document` cut into at most `count` pages of about equal serialized size, in order; a page
/// left empty is dropped and the rest are numbered as served.
fn cut(
    document: &BuildingByPnuDocument,
    pieces: &[(Piece, usize)],
    count: usize,
) -> Vec<BuildingByPnuPage> {
    let total = pieces.iter().map(|(_, size)| size).sum::<usize>().max(1);
    let mut pages: Vec<(Vec<BuildingPanelBuilding>, Vec<UnitResponse>)> =
        vec![(Vec::new(), Vec::new()); count];
    let mut before = 0;
    for (piece, size) in pieces {
        // Each piece goes to the page its middle falls in, so pages stay in document order.
        let at = ((before + size / 2) * count / total).min(count - 1);
        before += size;
        let (buildings, unlinked) = &mut pages[at];
        match *piece {
            Piece::Building(building) => {
                let mut fields = document.buildings[building].clone();
                fields.units.clear();
                buildings.push(fields);
            }
            Piece::Unit(building, unit) => {
                let source = &document.buildings[building];
                let value = source.units[unit].clone();
                match buildings.last_mut().filter(|last| last.id == source.id) {
                    Some(last) => last.units.push(value),
                    None => {
                        let mut continued = source.clone();
                        continued.floors.clear();
                        continued.units = vec![value];
                        buildings.push(continued);
                    }
                }
            }
            Piece::Unlinked(unit) => unlinked.push(document.unlinked_units[unit].clone()),
        }
    }
    pages.retain(|(buildings, unlinked)| !buildings.is_empty() || !unlinked.is_empty());
    let served = u32::try_from(pages.len()).unwrap_or(u32::MAX);
    pages
        .into_iter()
        .zip(1..)
        .map(|((buildings, unlinked_units), index)| BuildingByPnuPage {
            schema_version: BUILDING_PAGE_SCHEMA_VERSION.to_owned(),
            pnu: document.pnu.clone(),
            source: document.source.clone(),
            page: PageOf {
                index,
                count: served,
            },
            buildings,
            unlinked_units,
            withheld: None,
        })
        .collect()
}

/// The document a set of pages holds, as every reader joins them (root ADR-0325): pages 1 to
/// `count` of one PNU and one Gold snapshot, in order; consecutive buildings of one id are one
/// building, its floors and units continued.
///
/// # Errors
/// Refuses pages out of order, of another PNU, snapshot or count, and a withheld page.
pub fn join_pages(pages: &[BuildingByPnuPage]) -> anyhow::Result<BuildingByPnuDocument> {
    let first = pages.first().context("a document has at least one page")?;
    let count = u32::try_from(pages.len())?;
    let mut buildings: Vec<BuildingPanelBuilding> = Vec::new();
    let mut unlinked_units = Vec::new();
    for (page, index) in pages.iter().zip(1..) {
        ensure!(
            page.schema_version == BUILDING_PAGE_SCHEMA_VERSION
                && page.pnu == first.pnu
                && page.source == first.source
                && page.page == PageOf { index, count }
                && page.withheld.is_none(),
            "page {index} of {} is not page {index} of {count} of the same document",
            first.pnu
        );
        for building in &page.buildings {
            match buildings.last_mut().filter(|last| last.id == building.id) {
                Some(last) => {
                    last.floors.extend(building.floors.iter().cloned());
                    last.units.extend(building.units.iter().cloned());
                }
                None => buildings.push(building.clone()),
            }
        }
        unlinked_units.extend(page.unlinked_units.iter().cloned());
    }
    Ok(BuildingByPnuDocument {
        schema_version: BUILDING_DOCUMENT_SCHEMA_VERSION.to_owned(),
        pnu: first.pnu.clone(),
        source: first.source.clone(),
        buildings,
        unlinked_units,
    })
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

    pub fn unit(building_id: Option<uuid::Uuid>) -> anyhow::Result<JsonValue> {
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
            .join("../../../infra/lakehouse/spark/tests/fixtures/building_panel_gold_row.json");
        let row: JsonMap<String, JsonValue> = serde_json::from_slice(&std::fs::read(path)?)?;
        let artifact = build(&provenance(), &row)?;
        let document: JsonValue = serde_json::from_slice(&artifact.body)?;
        let mut building = document["buildings"][0].clone();
        let object = building
            .as_object_mut()
            .context("fixture building is not an object")?;
        object.remove("floors");
        object.remove("units");
        // The fixture's register states no names (Gold null): the document leaves them out, so the
        // golden packs built from it keep their bytes. Named buildings: the focused test below.
        assert!(object.get("purpose_name").is_none() && object.get("structure_name").is_none());
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
    fn a_building_shows_the_registers_names_beside_its_codes() -> anyhow::Result<()> {
        let unit = unit(Some(building_id_for_register_pk("BLDG-1")))?;
        let plain = row_with_building(unit.clone())?;
        let mut named = plain.clone();
        let mut buildings: JsonValue = serde_json::from_str(
            named
                .get("buildings_json")
                .and_then(JsonValue::as_str)
                .context("buildings_json is a string")?,
        )?;
        buildings[0]["purpose_name"] = json!("synthetic-purpose");
        buildings[0]["structure_code"] = json!("21");
        buildings[0]["structure_name"] = json!("synthetic-structure");
        named.insert(
            "buildings_json".to_owned(),
            json!(serde_json::to_string(&buildings)?),
        );

        let document: JsonValue = serde_json::from_slice(&build(&provenance(), &named)?.body)?;
        let building = &document["buildings"][0];
        assert_eq!(
            (&building["purpose_code"], &building["purpose_name"]),
            (&json!("03000"), &json!("synthetic-purpose"))
        );
        assert_eq!(
            (&building["structure_code"], &building["structure_name"]),
            (&json!("21"), &json!("synthetic-structure"))
        );

        // A Gold row without names (every one baked before them) keeps its document bytes.
        let document: JsonValue = serde_json::from_slice(&build(&provenance(), &plain)?.body)?;
        assert!(document["buildings"][0].get("purpose_name").is_none());
        assert!(document["buildings"][0].get("structure_name").is_none());
        Ok(())
    }

    /// Text gzip cannot shrink much: hex digits of a xorshift stream.
    pub fn noise(seed: usize, length: usize) -> String {
        let mut state = u64::try_from(seed)
            .unwrap_or(0)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            | 1;
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                char::from(b"0123456789abcdef"[usize::try_from(state % 16).unwrap_or(0)])
            })
            .collect()
    }

    /// A synthetic Gold row of a reserved (99999) PNU: `buildings` buildings of `units` units
    /// each, and `unlinked` unlinked units, every unit carrying `noise` bytes of name gzip cannot
    /// shrink.
    pub fn large_row(
        pnu: &str,
        buildings: usize,
        units: usize,
        unlinked: usize,
        noise_bytes: usize,
    ) -> anyhow::Result<JsonMap<String, JsonValue>> {
        let parcel = parcel_id_for_pnu(&Pnu::parse(pnu.to_owned())?).as_uuid();
        let unit_of =
            |pk: String, parent: Option<&str>, seed: usize| -> anyhow::Result<JsonValue> {
                let mut value = unit(parent.map(building_id_for_register_pk))?;
                value["unit_pnu"] = json!(pnu);
                value["parcel_id"] = json!(parcel);
                value["building_register_pk"] = json!(parent);
                value["id"] = json!(building_unit_id_for_register_pk(&pk));
                value["register_pk"] = json!(pk);
                value["building_name"] = json!(noise(seed, noise_bytes));
                Ok(value)
            };
        let mut built = Vec::new();
        for b in 0..buildings {
            let pk = format!("BLDG-{b}");
            let units = (0..units)
                .map(|u| unit_of(format!("UNIT-{b}-{u}"), Some(&pk), b * units + u))
                .collect::<anyhow::Result<Vec<_>>>()?;
            built.push(json!({
                "register_pk": pk, "id": building_id_for_register_pk(&pk), "parcel_id": parcel,
                "purpose_code": "03000", "structure_code": null, "floor_area_m2": null,
                "stories": null, "below_ground_floors": 0, "has_rooftop": false,
                "rooftop_area_m2": null, "rooftop_usage": "", "built_year": null,
                "floors": [{"floor_row_id": format!("FLOOR-{b}"), "floor_kind": "above_ground",
                    "floor_number": 1, "floor_index": 1, "floor_display_ko": "1"}],
                "units": units
            }));
        }
        let loose = (0..unlinked)
            .map(|u| unit_of(format!("LOOSE-{u}"), None, 1_000_000 + u))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut gold = row();
        gold.insert("pnu".to_owned(), json!(pnu));
        gold.insert(
            "buildings_json".to_owned(),
            json!(serde_json::to_string(&built)?),
        );
        gold.insert(
            "unlinked_units_json".to_owned(),
            json!(serde_json::to_string(&loose)?),
        );
        Ok(gold)
    }

    fn pages_of(bytes: &[Vec<u8>]) -> anyhow::Result<Vec<BuildingByPnuPage>> {
        bytes
            .iter()
            .map(|page| Ok(serde_json::from_slice(page)?))
            .collect()
    }

    /// Root ADR-0325: a cut at any page target keeps document order, keeps every page within the
    /// target, and joins back into the same bytes, also across a building cut between pages.
    #[test]
    fn pages_join_back_into_the_document_they_cut() -> anyhow::Result<()> {
        let document = document_with_approvals(
            &provenance(),
            &large_row(PNU, 3, 40, 7, 64)?,
            &ApprovedBuildingLinks::default(),
        )?;
        let whole = document.to_bytes()?;
        let member = crate::by_pnu_pack::member_length(&whole)?;
        for target in [member / 2 + 1, member / 3, member / 7] {
            let bytes = paginate(&document, member, target, 64)?;
            let pages = pages_of(&bytes)?;
            assert!(pages.len() >= 2, "{target}: {} pages", pages.len());
            for (page, served) in pages.iter().zip(&bytes) {
                assert!(crate::by_pnu_pack::member_length(served)? <= target);
                assert_eq!(page.schema_version, BUILDING_PAGE_SCHEMA_VERSION);
                assert_eq!(page.page.count, u32::try_from(pages.len())?);
            }
            assert_eq!(join_pages(&pages)?.to_bytes()?, whole);
            // A building's floors are on its first page only.
            let floors = pages
                .iter()
                .flat_map(|page| &page.buildings)
                .map(|building| building.floors.len())
                .sum::<usize>();
            assert_eq!(floors, 3);
        }
        Ok(())
    }

    /// Root ADR-0325: pages that are not one document's 1..n in order are refused, never joined.
    #[test]
    fn pages_of_another_document_or_order_are_refused() -> anyhow::Result<()> {
        let document = document_with_approvals(
            &provenance(),
            &large_row(PNU, 2, 30, 3, 64)?,
            &ApprovedBuildingLinks::default(),
        )?;
        let member = crate::by_pnu_pack::member_length(&document.to_bytes()?)?;
        let pages = pages_of(&paginate(&document, member, member / 3, 64)?)?;
        let mut reversed = pages.clone();
        reversed.reverse();
        assert!(join_pages(&reversed).is_err());
        assert!(join_pages(&pages[1..]).is_err(), "a missing first page");
        let mut other = pages.clone();
        other[1].source.iceberg_snapshot_id = "999990000000000002".to_owned();
        assert!(join_pages(&other).is_err(), "a page of another snapshot");
        Ok(())
    }

    /// The 2026-10-28 stop (root ADR-0325): a document whose member passes what one request may
    /// copy is served as pages, each within the target; one that fits stays whole, byte for byte;
    /// one with a single unit too large for any page is withheld instead of failing the bake.
    #[test]
    fn a_document_past_one_request_is_paged_and_one_no_cut_fits_is_withheld() -> anyhow::Result<()>
    {
        let budget = &crate::by_pnu_gateway_contract::section_pack_policy()?
            .read_path
            .request_cpu_budget;
        let small = document_with_approvals(
            &provenance(),
            &large_row(PNU, 1, 3, 1, 16)?,
            &ApprovedBuildingLinks::default(),
        )?;
        assert_eq!(served(&small)?, Served::Whole(small.to_bytes()?));

        let large = document_with_approvals(
            &provenance(),
            &large_row(PNU, 4, 150, 10, 1_000)?,
            &ApprovedBuildingLinks::default(),
        )?;
        let member = crate::by_pnu_pack::member_length(&large.to_bytes()?)?;
        assert!(
            u64::try_from(member)? > budget.document_max_bytes,
            "{member}"
        );
        let Served::Pages(bytes) = served(&large)? else {
            anyhow::bail!("a document past one request was not paged");
        };
        for page in &bytes {
            assert!(
                u64::try_from(crate::by_pnu_pack::member_length(page)?)?
                    <= budget.pages.page_target_bytes
            );
        }
        assert_eq!(
            join_pages(&pages_of(&bytes)?)?.to_bytes()?,
            large.to_bytes()?
        );

        let one_huge_unit = document_with_approvals(
            &provenance(),
            &large_row(PNU, 1, 1, 0, 600_000)?,
            &ApprovedBuildingLinks::default(),
        )?;
        let Served::Withheld { page, reason } = served(&one_huge_unit)? else {
            anyhow::bail!("a document no cut fits was not withheld");
        };
        assert!(reason.starts_with("document_too_large"), "{reason}");
        let page: BuildingByPnuPage = serde_json::from_slice(&page)?;
        assert_eq!(page.withheld.as_deref(), Some(reason.as_str()));
        assert!(page.buildings.is_empty() && page.unlinked_units.is_empty());
        assert!(
            join_pages(&[page]).is_err(),
            "a withheld page joins into nothing"
        );
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
