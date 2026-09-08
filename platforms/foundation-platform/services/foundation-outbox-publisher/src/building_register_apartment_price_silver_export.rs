//! Headerless HUB apartment-price observations through the shared streaming engine.

use crate::hub_register_silver_export::{self, Layout};

pub(crate) fn layout() -> anyhow::Result<Layout> {
    Layout::parse(
        include_str!("../../../infra/lakehouse/contracts/hub-building-register-apartment-price-source-objects.json"),
        &lakehouse_domain::SILVER_BUILDING_REGISTER_APARTMENT_PRICE,
    )
}

/// Exports the selected measured national ZIP as a complete Silver handoff.
///
/// # Errors
/// Fails on invalid configuration, source layout/integrity or publication errors.
pub async fn run() -> anyhow::Result<()> {
    hub_register_silver_export::run("FOUNDATION_PLATFORM_APARTMENT_PRICE", layout()?).await
}
