use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::bronze_catalog_recovery_manifest::RecoveryEvidenceArtifact;

#[derive(Debug, Deserialize)]
pub(super) struct EndpointCatalogDocument {
    pub(super) schema_version: String,
    pub(super) status: String,
    pub(super) endpoints: Vec<EndpointCatalogEntry>,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct EndpointCatalogEntry {
    pub(super) endpoint_slug: String,
    pub(super) provider: String,
    #[serde(default)]
    pub(super) display_name_ko: String,
    #[serde(default)]
    pub(super) dataset_slug: String,
    pub(super) operation: String,
    pub(super) source_acquisition_lane: String,
    pub(super) provider_dataset_selector: Option<VWorldDatasetSelector>,
    pub(super) auth_kind: String,
    pub(super) bronze: EndpointBronzeContract,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct VWorldDatasetSelector {
    pub(super) svc_cde: String,
    pub(super) ds_id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct EndpointBronzeContract {
    pub(super) source_slug: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct VWorldInventoryDocument {
    pub(super) schema_version: String,
    pub(super) status: String,
    pub(super) jobs: Vec<VWorldInventoryJob>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct VWorldInventoryJob {
    pub(super) endpoint_slug: String,
    pub(super) source_slug: String,
    pub(super) source_name: String,
    pub(super) dataset_name: String,
    pub(super) base_uri: String,
    pub(super) terms_url: Option<String>,
    pub(super) operation: String,
    pub(super) provider_module: String,
    pub(super) svc_cde: String,
    pub(super) ds_id: String,
    pub(super) files: Vec<collection_infrastructure::VWorldDatasetFileInventoryItem>,
}

pub(super) fn artifact(uri: &str, content: &str) -> RecoveryEvidenceArtifact {
    RecoveryEvidenceArtifact {
        uri: uri.to_owned(),
        sha256: sha256_hex(content.as_bytes()),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            let _ = write!(&mut output, "{byte:02x}");
            output
        })
}
