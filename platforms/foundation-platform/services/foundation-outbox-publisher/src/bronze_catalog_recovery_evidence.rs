use std::{fs, path::Path};

use anyhow::{bail, Context as _, Result};
use foundation_outbox::{
    object_storage::{ObjectWriteMode, PutObjectRequest},
    ObjectStorageService, PublishError,
};
use sha2::{Digest as _, Sha256};

use crate::bronze_catalog_recovery_manifest::{
    BronzeCatalogRecoveryManifest, RecoveryEvidenceArtifact,
};
use crate::r2_layout::bronze_catalog_recovery_evidence_key;

const JSON_CONTENT_TYPE: &str = "application/json";
const EVIDENCE_CACHE_CONTROL: &str = "no-store";

pub(crate) struct SealedRecoveryManifest {
    pub(crate) manifest: BronzeCatalogRecoveryManifest,
    pub(crate) bytes: Vec<u8>,
    pub(crate) uri: String,
    pub(crate) sha256: String,
}

pub(crate) async fn seal_recovery_manifest(
    storage: &dyn ObjectStorageService,
    bucket_name: &str,
    mut manifest: BronzeCatalogRecoveryManifest,
) -> Result<SealedRecoveryManifest> {
    if bucket_name.trim().is_empty() || bucket_name.trim() != bucket_name {
        bail!("recovery evidence bucket name must be non-empty and whitespace-free");
    }

    seal_artifact(
        storage,
        bucket_name,
        "endpoint-catalog",
        &mut manifest.endpoint_catalog,
    )
    .await?;
    seal_artifact(
        storage,
        bucket_name,
        "provider-inventory",
        &mut manifest.provider_inventory,
    )
    .await?;
    seal_artifact(
        storage,
        bucket_name,
        "r2-inventory",
        &mut manifest.r2_inventory,
    )
    .await?;

    let mut bytes = serde_json::to_vec_pretty(&manifest)
        .context("failed to serialize sealed Bronze Catalog recovery manifest")?;
    bytes.push(b'\n');
    let sha256 = sha256_hex(&bytes);
    let uri = put_immutable_evidence(storage, bucket_name, "manifests", &bytes).await?;

    Ok(SealedRecoveryManifest {
        manifest,
        bytes,
        uri,
        sha256,
    })
}

async fn seal_artifact(
    storage: &dyn ObjectStorageService,
    bucket_name: &str,
    kind: &'static str,
    artifact: &mut RecoveryEvidenceArtifact,
) -> Result<()> {
    let expected_key = bronze_catalog_recovery_evidence_key(kind, &artifact.sha256)?;
    let expected_uri = r2_uri(bucket_name, &expected_key);
    if artifact.uri.starts_with("r2://") {
        if artifact.uri != expected_uri {
            bail!(
                "sealed recovery {kind} URI does not match its bucket and checksum: {}",
                artifact.uri
            );
        }
        verify_existing_evidence(storage, &expected_key, &artifact.sha256).await?;
        return Ok(());
    }
    if artifact.uri.contains("://") {
        bail!(
            "unsupported recovery {kind} evidence URI before sealing: {}",
            artifact.uri
        );
    }

    let bytes = fs::read(Path::new(&artifact.uri)).with_context(|| {
        format!(
            "failed to read local recovery {kind} evidence {}",
            artifact.uri
        )
    })?;
    let observed_sha256 = sha256_hex(&bytes);
    if observed_sha256 != artifact.sha256 {
        bail!(
            "local recovery {kind} evidence checksum changed before sealing: {}",
            artifact.uri
        );
    }
    artifact.uri = put_immutable_evidence(storage, bucket_name, kind, &bytes).await?;
    Ok(())
}

async fn put_immutable_evidence(
    storage: &dyn ObjectStorageService,
    bucket_name: &str,
    kind: &'static str,
    bytes: &[u8],
) -> Result<String> {
    let checksum = sha256_hex(bytes);
    let key = bronze_catalog_recovery_evidence_key(kind, &checksum)?;
    let request = PutObjectRequest {
        key: key.clone(),
        body: bytes.to_vec(),
        content_type: JSON_CONTENT_TYPE.to_owned(),
        cache_control: EVIDENCE_CACHE_CONTROL.to_owned(),
        write_mode: ObjectWriteMode::CreateOnly,
        sha256: Some(checksum.clone()),
    };
    match storage.put_object(request).await {
        Ok(()) => {}
        Err(PublishError::ObjectAlreadyExists { key: collided }) if collided == key => {
            verify_existing_evidence(storage, &key, &checksum).await?;
        }
        Err(error) => return Err(error).context("failed to seal recovery evidence"),
    }
    Ok(r2_uri(bucket_name, &key))
}

async fn verify_existing_evidence(
    storage: &dyn ObjectStorageService,
    key: &str,
    expected_sha256: &str,
) -> Result<()> {
    let observed = storage
        .read_object_sha256(key)
        .await
        .context("failed to verify existing recovery evidence")?;
    if observed.as_deref() != Some(expected_sha256) {
        bail!("existing recovery evidence checksum metadata does not match: {key}");
    }
    Ok(())
}

fn r2_uri(bucket_name: &str, key: &str) -> String {
    format!("r2://{bucket_name}/{key}")
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        sync::{Mutex, PoisonError},
    };

    use async_trait::async_trait;
    use foundation_outbox::{
        object_storage::{ObjectWriteMode, PutObjectRequest},
        ObjectStorageService, PublishError,
    };

    use super::*;
    use crate::bronze_catalog_recovery_manifest::{
        BronzeCatalogRecoveryManifest, BronzeCatalogRecoveryManifestStatus,
        RecoveryEvidenceArtifact,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn seals_inputs_and_manifest_under_content_addressed_control_keys() -> TestResult {
        let dir = std::path::PathBuf::from("target/bronze-catalog-recovery-evidence-tests");
        fs::create_dir_all(&dir)?;
        let endpoint = write_fixture(&dir, "endpoint.json", br#"{"endpoint":true}"#)?;
        let provider = write_fixture(&dir, "provider.json", br#"{"provider":true}"#)?;
        let inventory = write_fixture(&dir, "r2.json", br#"{"r2":true}"#)?;
        let storage = RecordingStorage::default();

        let sealed = seal_recovery_manifest(
            &storage,
            "foundation-platform-lakehouse-prod",
            manifest(endpoint, provider, inventory),
        )
        .await?;

        assert!(sealed.uri.starts_with(
            "r2://foundation-platform-lakehouse-prod/control/evidence/bronze-catalog-recovery/manifests/sha256="
        ));
        assert_eq!(sealed.sha256.len(), 64);
        assert_eq!(storage.writes().len(), 4);
        assert!(storage
            .writes()
            .values()
            .all(|request| request.write_mode == ObjectWriteMode::CreateOnly));
        assert!(sealed
            .manifest
            .endpoint_catalog
            .uri
            .contains("/endpoint-catalog/sha256="));
        assert!(sealed
            .manifest
            .provider_inventory
            .uri
            .contains("/provider-inventory/sha256="));
        assert!(sealed
            .manifest
            .r2_inventory
            .uri
            .contains("/r2-inventory/sha256="));
        assert_eq!(sha256_hex(&sealed.bytes), sealed.sha256);

        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn existing_content_addressed_evidence_requires_matching_checksum_metadata() -> TestResult
    {
        let storage = RecordingStorage::default();
        let bytes = br#"{"evidence":true}"#;
        let checksum = sha256_hex(bytes);
        let key = bronze_catalog_recovery_evidence_key("provider-inventory", &checksum)?;
        storage.insert_existing(key, "f".repeat(64));

        let result = put_immutable_evidence(
            &storage,
            "foundation-platform-lakehouse-prod",
            "provider-inventory",
            bytes,
        )
        .await;

        assert!(result.is_err());
        Ok(())
    }

    fn write_fixture(
        dir: &std::path::Path,
        name: &str,
        bytes: &[u8],
    ) -> Result<RecoveryEvidenceArtifact, Box<dyn std::error::Error>> {
        let path = dir.join(name);
        fs::write(&path, bytes)?;
        Ok(RecoveryEvidenceArtifact {
            uri: path.to_string_lossy().into_owned(),
            sha256: sha256_hex(bytes),
        })
    }

    fn manifest(
        endpoint_catalog: RecoveryEvidenceArtifact,
        provider_inventory: RecoveryEvidenceArtifact,
        r2_inventory: RecoveryEvidenceArtifact,
    ) -> BronzeCatalogRecoveryManifest {
        BronzeCatalogRecoveryManifest {
            schema_version: "foundation-platform.bronze_catalog_recovery_manifest.v1".to_owned(),
            generated_at_utc: "2026-07-14T00:00:00Z".to_owned(),
            status: BronzeCatalogRecoveryManifestStatus::Ready,
            endpoint_catalog,
            provider_inventory,
            r2_inventory,
            sources: Vec::new(),
            unresolved: Vec::new(),
        }
    }

    #[derive(Default)]
    struct RecordingStorage {
        writes: Mutex<BTreeMap<String, PutObjectRequest>>,
        existing_checksums: Mutex<BTreeMap<String, String>>,
    }

    impl RecordingStorage {
        fn writes(&self) -> BTreeMap<String, PutObjectRequest> {
            self.writes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        fn insert_existing(&self, key: String, checksum: String) {
            self.existing_checksums
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key, checksum);
        }
    }

    #[async_trait]
    impl ObjectStorageService for RecordingStorage {
        async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
            let mut existing = self
                .existing_checksums
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if existing.contains_key(&request.key) {
                return Err(PublishError::ObjectAlreadyExists { key: request.key });
            }
            existing.insert(
                request.key.clone(),
                request.sha256.clone().unwrap_or_default(),
            );
            self.writes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(request.key.clone(), request);
            Ok(())
        }

        async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, PublishError> {
            Ok(self
                .existing_checksums
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(key)
                .cloned())
        }
    }
}
