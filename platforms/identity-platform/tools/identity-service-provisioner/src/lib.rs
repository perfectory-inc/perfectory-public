//! Identity-owned workload policy resolution and transactional provisioning.

mod manifest;
mod provision;

pub use manifest::{
    compiled_policy, parse_bindings, parse_policy, resolve_manifest, ManifestError,
    ServicePrincipalDefinition, ValidatedBindings, ValidatedManifest, ValidatedPolicy,
    WorkloadPrincipalPolicy, BINDINGS_SCHEMA_VERSION, POLICY_SCHEMA_VERSION,
};
pub use provision::{
    provision, provision_with_config, ProvisionConfig, ProvisionError, ProvisionErrorCategory,
    ProvisionReport,
};
