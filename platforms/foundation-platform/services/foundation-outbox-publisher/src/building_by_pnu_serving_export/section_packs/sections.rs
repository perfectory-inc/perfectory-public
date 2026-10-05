//! What a building pack holds per PNU (root ADR-0151, superseding the split of ADR-0147 §1, §3).
//!
//! One section, `documents`: each PNU's served document whole, the exact bytes the object lane
//! served. The Worker answers a PNU with that one gzip member as it is (`Content-Encoding: gzip`),
//! without decompressing or parsing anything: the earlier split into `buildings`, `floors`, `units`
//! and `unit_prices` made the Worker gunzip, parse, join and re-serialise four fragments per
//! request, which cost more CPU than the Workers plan allows (ADR-0151 Context).
//!
//! A new panel field therefore re-bakes every pack of the lane (about 19,000 for buildings), not
//! one section's. The machinery stays generic over the contract's section list (generations,
//! patches and `patch_floor` per section), so a layout of several sections remains expressible;
//! this file cuts only `documents`, and [`check_contract_sections`] refuses a contract naming
//! anything else.

use anyhow::{ensure, Context};

use super::super::building_document::BuildingByPnuDocument;
use crate::by_pnu_gateway_contract::ByPnuLane;

pub(crate) const DOCUMENTS: &str = "documents";
/// Every section this file can cut.
pub(crate) const KNOWN_SECTIONS: [&str; 1] = [DOCUMENTS];

/// Refuses a contract whose section list is not exactly what this file cuts, with `documents` as
/// the anchor: a section nobody bakes would 503 every request, and a section the contract forgot
/// would vanish from every document.
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
        packs.anchor_section == DOCUMENTS,
        "the building anchor section must be {DOCUMENTS}, the contract says {}",
        packs.anchor_section
    );
    Ok(())
}

/// The bytes one section holds for `document`: for `documents`, the served bytes themselves.
///
/// # Errors
/// Refuses a section this file does not cut.
pub(crate) fn fragment(document: &BuildingByPnuDocument, section: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(
        section == DOCUMENTS,
        "the building document has no section {section:?}"
    );
    document.to_bytes()
}

/// The document the fragments of one PNU hold, given in [`KNOWN_SECTIONS`] order.
///
/// # Errors
/// Refuses a missing or extra fragment, and one that is not a building document.
pub(crate) fn join(fragments: &[(&str, &[u8])]) -> anyhow::Result<BuildingByPnuDocument> {
    let names = fragments.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    ensure!(
        names == KNOWN_SECTIONS,
        "a building document is read from exactly {KNOWN_SECTIONS:?}, got {names:?}"
    );
    let (_, bytes) = fragments[0];
    serde_json::from_slice(bytes).context("the documents fragment is not a building document")
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

    #[test]
    fn the_contract_names_exactly_the_sections_the_bake_cuts() -> anyhow::Result<()> {
        check_contract_sections()
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
        let bytes = fragment(&document, DOCUMENTS)?;
        assert_eq!(bytes, document.to_bytes()?);
        assert_eq!(join(&[(DOCUMENTS, bytes.as_slice())])?.to_bytes()?, bytes);
        Ok(())
    }

    #[test]
    fn other_sections_and_other_bytes_are_refused() -> anyhow::Result<()> {
        let document = fixture_document()?;
        assert!(fragment(&document, "floors").is_err());
        let bytes = fragment(&document, DOCUMENTS)?;
        assert!(join(&[("floors", bytes.as_slice())]).is_err());
        assert!(join(&[]).is_err());
        assert!(join(&[(DOCUMENTS, b"[]".as_slice())]).is_err());
        Ok(())
    }
}
