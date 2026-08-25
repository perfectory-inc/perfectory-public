//! Typed access to the profile-gateway section of the R2 connection contract.

use std::sync::OnceLock;

use serde::Deserialize;

const R2_CONNECTION_CONTRACT: &str = include_str!("../../../config/r2-connections.contract.json");

#[derive(Debug, Deserialize)]
struct R2ConnectionContract {
    schema_version: u64,
    profile_gateway: ProfileGatewayPolicy,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProfileGatewayPolicy {
    pub(crate) object_key: ProfileObjectKeyPolicy,
    pub(crate) content_type: String,
    pub(crate) cache_control: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProfileObjectKeyPolicy {
    pub(crate) root: String,
    pub(crate) artifact_id_pattern: String,
    pub(crate) suffix: String,
}

/// Returns the immutable serving and storage policy shared by the uploader and gateway.
pub(crate) fn profile_gateway_policy() -> anyhow::Result<&'static ProfileGatewayPolicy> {
    static POLICY: OnceLock<Result<ProfileGatewayPolicy, String>> = OnceLock::new();
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
            Ok(contract.profile_gateway)
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}
