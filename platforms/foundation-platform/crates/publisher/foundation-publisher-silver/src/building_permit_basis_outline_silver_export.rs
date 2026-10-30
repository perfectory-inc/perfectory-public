//! Headerless HUB 건축인허가 기본개요 through the shared streaming engine (root ADR-0356).

use crate::hub_register_silver_export::{self, Layout};

/// The lane's source contract, embedded once: the export reads its layout and the Silver refresh
/// reads its release rules from these same bytes (root ADR-0169).
pub const CONTRACT_JSON: &str = include_str!(
    "../../../../infra/lakehouse/contracts/hub-building-permit-basis-outline-source-objects.json"
);

pub(crate) fn layout() -> anyhow::Result<Layout> {
    Layout::parse(
        CONTRACT_JSON,
        &lakehouse_domain::SILVER_BUILDING_PERMIT_BASIS_OUTLINE,
    )
}

/// Exports the selected national ZIP as a complete Silver handoff.
///
/// # Errors
/// Fails on invalid configuration, source layout/integrity or publication errors.
pub async fn run() -> anyhow::Result<()> {
    hub_register_silver_export::run("FOUNDATION_PLATFORM_PERMIT_BASIS", layout()?).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_contract_layout_matches_the_silver_table() -> anyhow::Result<()> {
        super::layout().map(drop)
    }
}
