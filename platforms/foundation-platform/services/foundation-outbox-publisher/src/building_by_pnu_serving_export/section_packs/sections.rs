//! What a lane's pack holds per PNU (root ADR-0151, superseding the split of ADR-0147 §1, §3).
//!
//! One section, `documents`: each PNU's served document whole, the exact bytes the object lane
//! served. The Worker answers a PNU with that one gzip member as it is (`Content-Encoding: gzip`),
//! without decompressing or parsing anything: the earlier split of the building document into
//! `buildings`, `floors`, `units` and `unit_prices` made the Worker gunzip, parse, join and
//! re-serialise four fragments per request, which cost more CPU than the Workers plan allows
//! (ADR-0151 Context).
//!
//! A new panel field therefore re-bakes every pack of the lane, not one section's. The machinery
//! stays generic over the contract's section list (generations, patches and `patch_floor` per
//! section), so a layout of several sections remains expressible; this file cuts only
//! `documents`, and [`check_contract_sections`] refuses a contract naming anything else.
//!
//! The documents are rendered by the lane's object export builder ([`Renderer`]), so a pack holds
//! exactly what an object would have held.

use anyhow::{bail, ensure, Context};
use lakehouse_domain::{LakehouseTableContract, GOLD_BUILDING_PANEL};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::super::building_document::{self, BuildingByPnuDocument};
use crate::building_link_evidence::ApprovedBuildingLinks;
use crate::by_pnu_gateway_contract::ByPnuLane;

pub(crate) const DOCUMENTS: &str = "documents";
/// Every section this file can cut.
pub(crate) const KNOWN_SECTIONS: [&str; 1] = [DOCUMENTS];

/// Refuses a contract whose section list for `lane` is not exactly what this file cuts, with
/// `documents` as the anchor: a section nobody bakes would 503 every request, and a section the
/// contract forgot would vanish from every document.
///
/// # Errors
/// Returns the mismatch, or that the lane names no section packs.
pub(crate) fn check_contract_sections(lane: ByPnuLane) -> anyhow::Result<()> {
    let packs = lane.section_packs()?;
    ensure!(
        packs.sections.iter().map(String::as_str).eq(KNOWN_SECTIONS),
        "the contract's {} sections {:?} are not the sections the bake cuts {KNOWN_SECTIONS:?}",
        lane.unit(),
        packs.sections
    );
    ensure!(
        packs.anchor_section == DOCUMENTS,
        "the {} anchor section must be {DOCUMENTS}, the contract says {}",
        lane.unit(),
        packs.anchor_section
    );
    Ok(())
}

/// The lane's Gold table, as the scan reads it.
///
/// # Errors
/// Refuses a lane that bakes no packs.
pub(crate) fn gold_table(lane: ByPnuLane) -> anyhow::Result<&'static LakehouseTableContract> {
    match lane {
        ByPnuLane::Building => Ok(&GOLD_BUILDING_PANEL),
        ByPnuLane::Parcel => bail!("the parcel lane bakes no section packs yet"),
    }
}

/// One PNU's served document: its bytes, exactly as the object lane served them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackDocument {
    pub(crate) pnu: String,
    pub(crate) bytes: Vec<u8>,
}

/// The Gold snapshot a run renders its documents from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackProvenance {
    pub(crate) table: String,
    pub(crate) iceberg_snapshot_id: String,
    pub(crate) metadata_location: String,
    pub(crate) manifest_list_location: String,
}

/// Renders a lane's documents with the lane's object export builder, and whatever that builder
/// reads beside the Gold row.
pub(crate) enum Renderer {
    /// The building document reads the approved building links from the runtime database.
    Building(ApprovedBuildingLinks),
}

impl Renderer {
    /// The renderer for `lane`, with what it reads loaded.
    ///
    /// # Errors
    /// Returns an error when the lane's inputs cannot be read, or the lane bakes no packs.
    pub(crate) async fn load(lane: ByPnuLane) -> anyhow::Result<Self> {
        match lane {
            ByPnuLane::Building => Ok(Self::Building(ApprovedBuildingLinks::load_current().await?)),
            ByPnuLane::Parcel => bail!("the parcel lane bakes no section packs yet"),
        }
    }

    /// One row's served document.
    ///
    /// # Errors
    /// Refuses a row the lane's object export would refuse.
    pub(crate) fn render(
        &self,
        provenance: &PackProvenance,
        row: &JsonMap<String, JsonValue>,
    ) -> anyhow::Result<PackDocument> {
        match self {
            Self::Building(approvals) => {
                let provenance = building_document::GoldSnapshotProvenance {
                    table: provenance.table.clone(),
                    iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
                    metadata_location: provenance.metadata_location.clone(),
                    manifest_list_location: provenance.manifest_list_location.clone(),
                };
                let document =
                    building_document::document_with_approvals(&provenance, row, approvals)?;
                Ok(PackDocument {
                    bytes: document.to_bytes()?,
                    pnu: document.pnu,
                })
            }
        }
    }
}

/// The bytes one section holds for `document`: for `documents`, the served bytes themselves.
///
/// # Errors
/// Refuses a section this file does not cut.
pub(crate) fn fragment(document: &PackDocument, section: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(
        section == DOCUMENTS,
        "a served document has no section {section:?}"
    );
    Ok(document.bytes.clone())
}

/// Checks that the fragments of one PNU, given in [`KNOWN_SECTIONS`] order, hold a document of
/// `lane`.
///
/// # Errors
/// Refuses a missing or extra fragment, and one that is not a document of the lane.
pub(crate) fn join(lane: ByPnuLane, fragments: &[(&str, &[u8])]) -> anyhow::Result<()> {
    let names = fragments.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    ensure!(
        names == KNOWN_SECTIONS,
        "a served document is read from exactly {KNOWN_SECTIONS:?}, got {names:?}"
    );
    let (_, bytes) = fragments[0];
    match lane {
        ByPnuLane::Building => {
            serde_json::from_slice::<BuildingByPnuDocument>(bytes)
                .context("the documents fragment is not a building document")?;
            Ok(())
        }
        ByPnuLane::Parcel => bail!("the parcel lane bakes no section packs yet"),
    }
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

    fn pack_document(document: &BuildingByPnuDocument) -> anyhow::Result<PackDocument> {
        Ok(PackDocument {
            pnu: document.pnu.clone(),
            bytes: document.to_bytes()?,
        })
    }

    #[test]
    fn the_contract_names_exactly_the_sections_the_bake_cuts() -> anyhow::Result<()> {
        check_contract_sections(ByPnuLane::Building)
    }

    /// The pack holds the served bytes exactly, and reading them back gives the same bytes: the
    /// pack lane serves what the object lane served.
    #[test]
    fn the_documents_section_holds_the_served_bytes() -> anyhow::Result<()> {
        let document = fixture_document()?;
        assert!(!document.buildings[0].floors.is_empty());
        assert!(!document.buildings[0].units[0]
            .official_price_history
            .is_empty());
        let bytes = fragment(&pack_document(&document)?, DOCUMENTS)?;
        assert_eq!(bytes, document.to_bytes()?);
        join(ByPnuLane::Building, &[(DOCUMENTS, bytes.as_slice())])?;
        Ok(())
    }

    #[test]
    fn other_sections_and_other_bytes_are_refused() -> anyhow::Result<()> {
        let document = pack_document(&fixture_document()?)?;
        assert!(fragment(&document, "floors").is_err());
        let bytes = fragment(&document, DOCUMENTS)?;
        assert!(join(ByPnuLane::Building, &[("floors", bytes.as_slice())]).is_err());
        assert!(join(ByPnuLane::Building, &[]).is_err());
        assert!(join(ByPnuLane::Building, &[(DOCUMENTS, b"[]".as_slice())]).is_err());
        Ok(())
    }
}
