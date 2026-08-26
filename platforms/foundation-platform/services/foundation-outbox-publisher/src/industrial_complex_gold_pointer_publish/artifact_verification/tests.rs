//! Tests for the pointer-to-object check.
//!
//! Every case here is a way a pointer can lie about the object it names. They are written as
//! rejections rather than as one happy path, because the value of this module is entirely in what
//! it refuses: a check only observed to pass has not been observed at all.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{ClaimedGoldProfileArtifact, PointerPublication, VerifiedGoldProfileArtifact};
use crate::industrial_complex_gold_profile_store::{ProfileObjectStore, ProfileStoreConfig};

const COMPLEX_ID: &str = "0196e7e0-3c20-7000-8000-100000000002";
const ARTIFACT_ID: &str = "018f0000-0000-7000-8000-000000000001";

fn temporary_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "foundation-platform-gold-pointer-verify-{label}-{}",
        Uuid::now_v7()
    ))
}

fn profile_bytes(artifact_id: &str, complex_id: &str) -> Vec<u8> {
    let mut body = serde_json::to_vec_pretty(&json!({
        "schema_version": "foundation-platform.industrial_complex_gold_profile.v1",
        "artifact_id": artifact_id,
        "complex_id": complex_id,
        "source": {
            "table": "gold.complex_catalog",
            // Repository-reserved synthetic snapshot namespace, not a production value.
            "iceberg_snapshot_id": "999990000000000001",
            "metadata_location": "s3://lakehouse/metadata/00001.metadata.json",
            "manifest_list_location": "s3://lakehouse/metadata/snap-1.avro"
        },
        "attributes": {"complex_id": complex_id, "name": "fixture complex"}
    }))
    .expect("fixture serializes");
    body.push(b'\n');
    body
}

fn object_key(artifact_id: &str) -> String {
    format!("gold/industrial-complex/profiles/{artifact_id}.json")
}

fn checksum(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}

/// Opens a store that already holds the canonical fixture object.
async fn store_with_fixture(label: &str) -> anyhow::Result<(ProfileObjectStore, PathBuf, Vec<u8>)> {
    let root = temporary_root(label);
    let store = ProfileObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let body = profile_bytes(ARTIFACT_ID, COMPLEX_ID);
    store
        .write_create_only(&object_key(ARTIFACT_ID), &body, &checksum(&body))
        .await?;
    Ok((store, root, body))
}

fn claim(body: &[u8]) -> anyhow::Result<ClaimedGoldProfileArtifact> {
    Ok(ClaimedGoldProfileArtifact {
        lakehouse_complex_id: LakehouseComplexId::new(Uuid::parse_str(COMPLEX_ID)?),
        current_version: ARTIFACT_ID.to_owned(),
        object_key: object_key(ARTIFACT_ID),
        checksum_sha256: checksum(body),
        size_bytes: u64::try_from(body.len())?,
    })
}

fn publication() -> anyhow::Result<PointerPublication> {
    Ok(PointerPublication {
        expected_current_version: None,
        profile_url_template: "https://lakehouse.example.com/{object_key}".to_owned(),
        spatial_locator_object_key: None,
        source: "foundation-platform.spark.industrial_complex_gold".to_owned(),
        source_url: None,
        source_external_id: None,
        source_snapshot_id: "vworldkr__sandan_profile-202506".to_owned(),
        iceberg_snapshot_id: "999990000000000001".to_owned(),
        profile_row_count: 1,
        spatial_locator_size_bytes: None,
        published_at: DateTime::parse_from_rfc3339("2026-08-18T00:00:00Z")?.with_timezone(&Utc),
    })
}

#[tokio::test]
async fn a_truthful_claim_verifies_and_carries_the_read_values_into_the_input() -> anyhow::Result<()>
{
    let (store, root, body) = store_with_fixture("ok").await?;
    let catalog_complex_id = ComplexId::new(Uuid::now_v7());

    let verified = VerifiedGoldProfileArtifact::verify(&store, claim(&body)?).await?;
    let input = verified.into_publish_input(catalog_complex_id, publication()?);

    std::fs::remove_dir_all(&root)?;
    assert_eq!(input.profile_checksum_sha256, checksum(&body));
    assert_eq!(input.profile_size_bytes, u64::try_from(body.len())?);
    assert_eq!(input.profile_object_key, object_key(ARTIFACT_ID));
    assert_eq!(input.current_version, ARTIFACT_ID);
    assert_eq!(input.complex_id, catalog_complex_id);
    Ok(())
}

/// The case this module exists for: the pointer's checksum and the stored bytes disagree.
#[tokio::test]
async fn a_checksum_that_does_not_match_the_stored_bytes_is_refused() -> anyhow::Result<()> {
    let (store, root, body) = store_with_fixture("checksum").await?;
    let mut lying = claim(&body)?;
    lying.checksum_sha256 = "b".repeat(64);

    let result = VerifiedGoldProfileArtifact::verify(&store, lying).await;

    std::fs::remove_dir_all(&root)?;
    let Err(error) = result else {
        anyhow::bail!("a pointer whose checksum contradicts the object was accepted");
    };
    assert!(
        error.to_string().contains("hashes to"),
        "unexpected rejection: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_size_that_does_not_match_the_stored_bytes_is_refused() -> anyhow::Result<()> {
    let (store, root, body) = store_with_fixture("size").await?;
    let mut lying = claim(&body)?;
    lying.size_bytes += 1;

    let result = VerifiedGoldProfileArtifact::verify(&store, lying).await;

    std::fs::remove_dir_all(&root)?;
    let Err(error) = result else {
        anyhow::bail!("a pointer whose size contradicts the object was accepted");
    };
    assert!(error.to_string().contains("bytes but the pointer claims"));
    Ok(())
}

/// The state the repository is in before any export has run: pointing at nothing.
#[tokio::test]
async fn a_pointer_at_an_object_that_does_not_exist_is_refused() -> anyhow::Result<()> {
    let root = temporary_root("absent");
    let store = ProfileObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let body = profile_bytes(ARTIFACT_ID, COMPLEX_ID);

    let result = VerifiedGoldProfileArtifact::verify(&store, claim(&body)?).await;

    let Err(error) = result else {
        anyhow::bail!("a pointer at an absent object was accepted");
    };
    assert!(error.to_string().contains("could not be read"));
    Ok(())
}

/// A summary row transcribed against the wrong complex — the accident hand-copying produces.
#[tokio::test]
async fn an_object_describing_another_complex_is_refused() -> anyhow::Result<()> {
    let (store, root, body) = store_with_fixture("wrong-complex").await?;
    let mut lying = claim(&body)?;
    lying.lakehouse_complex_id =
        LakehouseComplexId::new(Uuid::parse_str("0196e7e0-3c20-5000-8000-100000000009")?);

    let result = VerifiedGoldProfileArtifact::verify(&store, lying).await;

    std::fs::remove_dir_all(&root)?;
    let Err(error) = result else {
        anyhow::bail!("a pointer aimed at another complex's profile was accepted");
    };
    assert!(error.to_string().contains("describes complex"));
    Ok(())
}

/// The version published must be the version the stored document says it is.
#[tokio::test]
async fn a_version_the_stored_document_does_not_claim_is_refused() -> anyhow::Result<()> {
    let (store, root, body) = store_with_fixture("wrong-version").await?;
    let mut lying = claim(&body)?;
    lying.current_version = "018f0000-0000-7000-8000-000000000002".to_owned();

    let result = VerifiedGoldProfileArtifact::verify(&store, lying).await;

    std::fs::remove_dir_all(&root)?;
    let Err(error) = result else {
        anyhow::bail!("a pointer publishing a version the object does not claim was accepted");
    };
    assert!(error.to_string().contains("states artifact"));
    Ok(())
}

#[tokio::test]
async fn a_key_outside_the_canonical_profile_layout_is_refused() -> anyhow::Result<()> {
    let (store, root, body) = store_with_fixture("non-canonical").await?;
    let mut lying = claim(&body)?;
    lying.object_key = "gold/industrial-complex/profiles/latest.json".to_owned();

    let result = VerifiedGoldProfileArtifact::verify(&store, lying).await;

    std::fs::remove_dir_all(&root)?;
    let Err(error) = result else {
        anyhow::bail!("a non-canonical profile key was accepted");
    };
    assert!(error.to_string().contains("canonical"));
    Ok(())
}
