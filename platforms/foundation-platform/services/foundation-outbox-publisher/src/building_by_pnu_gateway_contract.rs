//! Typed access to the building by-PNU gateway section of the R2 connection contract.
//!
//! The contract JSON is the SSOT (root ADR-0100): the export, the manifest publisher, and the
//! `foundation-building-gateway` Worker all read the same block, so the key grammar the uploader
//! writes under and the grammar the Worker resolves are one fact.

use std::sync::OnceLock;

use serde::Deserialize;

const R2_CONNECTION_CONTRACT: &str = include_str!("../../../config/r2-connections.contract.json");

#[derive(Debug, Deserialize)]
struct R2ConnectionContract {
    schema_version: u64,
    building_by_pnu_gateway: BuildingByPnuGatewayPolicy,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BuildingByPnuGatewayPolicy {
    pub(crate) object_key: BuildingByPnuObjectKeyPolicy,
    pub(crate) content_type: String,
    pub(crate) cache_control: String,
    pub(crate) manifest_cache_control: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BuildingByPnuObjectKeyPolicy {
    pub(crate) root: String,
    pub(crate) generation_dir_pattern: String,
    pub(crate) pnu_pattern: String,
    pub(crate) suffix: String,
    pub(crate) manifest_object: String,
}

/// Returns the immutable serving and storage policy shared by the baker and the gateway.
pub(crate) fn building_by_pnu_gateway_policy() -> anyhow::Result<&'static BuildingByPnuGatewayPolicy>
{
    static POLICY: OnceLock<Result<BuildingByPnuGatewayPolicy, String>> = OnceLock::new();
    POLICY
        .get_or_init(|| {
            let contract: R2ConnectionContract = serde_json::from_str(R2_CONNECTION_CONTRACT)
                .map_err(|error| format!("invalid R2 connection contract: {error}"))?;
            if contract.schema_version != 2 {
                return Err(format!(
                    "R2 connection contract schema must be 2, got {}",
                    contract.schema_version
                ));
            }
            Ok(contract.building_by_pnu_gateway)
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}
