//! Headerless HUB exclusive-unit observations through the shared streaming engine.

use crate::hub_register_silver_export::{self, Layout};

/// The lane's source contract, embedded once: the export reads its layout and the Silver refresh
/// reads its release rules from these same bytes (root ADR-0169).
pub const CONTRACT_JSON: &str = include_str!(
    "../../../infra/lakehouse/contracts/hub-building-register-exclusive-unit-source-objects.json"
);

pub(crate) fn layout() -> anyhow::Result<Layout> {
    Layout::parse(
        CONTRACT_JSON,
        &lakehouse_domain::SILVER_BUILDING_REGISTER_EXCLUSIVE_UNIT,
    )
}

/// Exports the selected measured national ZIP as a complete Silver handoff.
///
/// # Errors
/// Fails on invalid configuration, source layout/integrity or publication errors.
pub async fn run() -> anyhow::Result<()> {
    hub_register_silver_export::run("FOUNDATION_PLATFORM_EXCLUSIVE_UNIT", layout()?).await
}
