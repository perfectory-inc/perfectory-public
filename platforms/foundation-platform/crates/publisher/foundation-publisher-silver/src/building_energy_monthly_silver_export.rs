//! Headerless HUB 월별 건물 전기·가스 through the shared streaming engine (root ADR-0396).

use crate::hub_register_silver_export::{self, Layout};

/// The 전기 lane's source contract, embedded once: the export reads its layout and the Silver
/// refresh reads its release rules from these same bytes (root ADR-0169).
pub const ELECTRICITY_CONTRACT_JSON: &str = include_str!(
    "../../../../infra/lakehouse/contracts/hub-building-energy-monthly-electricity-source-objects.json"
);
/// The 가스 lane's source contract, the same way.
pub const GAS_CONTRACT_JSON: &str = include_str!(
    "../../../../infra/lakehouse/contracts/hub-building-energy-monthly-gas-source-objects.json"
);

pub(crate) fn electricity_layout() -> anyhow::Result<Layout> {
    Layout::parse(
        ELECTRICITY_CONTRACT_JSON,
        &lakehouse_domain::SILVER_BUILDING_ENERGY_MONTHLY_ELECTRICITY,
    )
}

pub(crate) fn gas_layout() -> anyhow::Result<Layout> {
    Layout::parse(
        GAS_CONTRACT_JSON,
        &lakehouse_domain::SILVER_BUILDING_ENERGY_MONTHLY_GAS,
    )
}

/// Exports the selected month's 전기 ZIP as a complete Silver handoff.
///
/// # Errors
/// Fails on invalid configuration, source layout/integrity or publication errors.
pub async fn run_electricity() -> anyhow::Result<()> {
    hub_register_silver_export::run(
        "FOUNDATION_PLATFORM_ENERGY_ELECTRICITY",
        electricity_layout()?,
    )
    .await
}

/// Exports the selected month's 가스 ZIP as a complete Silver handoff.
///
/// # Errors
/// Fails on invalid configuration, source layout/integrity or publication errors.
pub async fn run_gas() -> anyhow::Result<()> {
    hub_register_silver_export::run("FOUNDATION_PLATFORM_ENERGY_GAS", gas_layout()?).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_contract_layouts_match_the_silver_tables() -> anyhow::Result<()> {
        super::electricity_layout()?;
        super::gas_layout().map(drop)
    }
}
