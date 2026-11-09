//! Typed access to the profile-gateway section of the R2 connection contract.

use std::sync::OnceLock;

use serde::Deserialize;

const R2_CONNECTION_CONTRACT: &str =
    include_str!("../../../../config/r2-connections.contract.json");

#[derive(Debug, Deserialize)]
struct R2ConnectionContract {
    schema_version: u64,
    profile_gateway: ProfileGatewayPolicy,
    industrial_complex_documents: ComplexDocumentsPolicy,
}

/// Where the industrial-complex index lives beside the profiles (root ADR-0456).
#[derive(Debug, Deserialize)]
pub struct ComplexDocumentsPolicy {
    /// The one overwritten object: the current index.
    pub index_object: String,
    /// Each index generation, kept create-only before the index is replaced.
    pub index_history_dir: String,
    /// The index's `Cache-Control`; profiles keep the profile gateway's immutable one.
    pub index_cache_control: String,
}

#[derive(Debug, Deserialize)]
pub struct ProfileGatewayPolicy {
    pub object_key: ProfileObjectKeyPolicy,
    pub content_type: String,
    pub cache_control: String,
}

#[derive(Debug, Deserialize)]
pub struct ProfileObjectKeyPolicy {
    pub root: String,
    pub artifact_id_pattern: String,
    pub suffix: String,
}

fn contract() -> anyhow::Result<&'static R2ConnectionContract> {
    static CONTRACT: OnceLock<Result<R2ConnectionContract, String>> = OnceLock::new();
    CONTRACT
        .get_or_init(|| {
            let contract: R2ConnectionContract = serde_json::from_str(R2_CONNECTION_CONTRACT)
                .map_err(|error| format!("invalid R2 connection contract: {error}"))?;
            if contract.schema_version != 2 {
                return Err(format!(
                    "R2 connection contract schema must be 2, got {}",
                    contract.schema_version
                ));
            }
            Ok(contract)
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}

/// Returns the immutable serving and storage policy shared by the uploader and gateway.
pub fn profile_gateway_policy() -> anyhow::Result<&'static ProfileGatewayPolicy> {
    Ok(&contract()?.profile_gateway)
}

/// Returns where the industrial-complex index and its history live (root ADR-0456).
pub fn complex_documents_policy() -> anyhow::Result<&'static ComplexDocumentsPolicy> {
    Ok(&contract()?.industrial_complex_documents)
}
