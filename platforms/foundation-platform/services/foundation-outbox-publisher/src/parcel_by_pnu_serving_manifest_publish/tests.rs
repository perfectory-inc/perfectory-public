use std::path::PathBuf;

use sha2::{Digest, Sha256};

use super::{
    publish, publish_from_listing, ExportArtifactInput, ExportSummaryInput, ListingExpectation,
    ManifestPublishConfig, ManifestPublishInput, ParcelServingManifest,
};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::parcel_by_pnu_serving_export::parcel_document::PARCEL_DOCUMENT_SCHEMA_VERSION;
use crate::parcel_by_pnu_serving_store::ParcelServingObjectStore;
use crate::r2_layout::{parcel_by_pnu_serving_manifest_key, parcel_by_pnu_serving_object_key};

// PNUs and snapshot ids sit in the repository-reserved synthetic namespaces
// (`scripts/guard/public-fixture-safety.py`).
const PNU_A: &str = "9999900000100000000";
const PNU_B: &str = "9999900000200000000";

fn temporary_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "foundation-platform-parcel-serving-manifest-{label}-{}",
        uuid::Uuid::now_v7()
    ))
}

fn config(root: PathBuf, allow_repoint: bool, first_publication: bool) -> ManifestPublishConfig {
    ManifestPublishConfig {
        output: ProfileStoreConfig::Local { root },
        input: ManifestPublishInput::ExportSummary(PathBuf::from("unused-in-tests.json")),
        allow_repoint,
        first_publication,
    }
}

async fn seed_generation(
    store: &ParcelServingObjectStore,
    generation: u64,
    pnus: &[&str],
) -> anyhow::Result<Vec<ExportArtifactInput>> {
    let mut artifacts = Vec::new();
    for pnu in pnus {
        let body = format!("{{\"pnu\":\"{pnu}\",\"generation\":{generation}}}\n").into_bytes();
        let checksum = format!("{:x}", Sha256::digest(&body));
        let key = parcel_by_pnu_serving_object_key(generation, pnu)?;
        store
            .write_object_create_only(&key, &body, &checksum)
            .await?;
        artifacts.push(ExportArtifactInput {
            pnu: (*pnu).to_owned(),
            object_key: key,
            object_checksum_sha256: checksum,
        });
    }
    Ok(artifacts)
}

fn summary(generation: u64, artifacts: Vec<ExportArtifactInput>) -> ExportSummaryInput {
    ExportSummaryInput {
        schema_version: "foundation-platform.parcel_by_pnu_serving_export_summary.v1".to_owned(),
        gold_table: "gold.parcel_panel".to_owned(),
        gold_iceberg_snapshot_id: "999990000000000001".to_owned(),
        target_generation: generation,
        artifacts,
    }
}

#[tokio::test]
async fn publishes_after_read_back_and_pins_the_generation() -> anyhow::Result<()> {
    let root = temporary_root("happy");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let artifacts = seed_generation(&store, 1, &[PNU_A, PNU_B]).await?;

    let manifest = publish(
        &config(root.clone(), false, true),
        &store,
        &summary(1, artifacts),
    )
    .await?;
    let stored = store
        .read_bytes(parcel_by_pnu_serving_manifest_key()?)
        .await?;
    let parsed: ParcelServingManifest = serde_json::from_slice(&stored)?;

    std::fs::remove_dir_all(&root)?;
    assert_eq!(manifest.current_generation, 1);
    assert_eq!(parsed.current_generation, 1);
    assert_eq!(parsed.unit, "parcel-by-pnu");
    assert_eq!(parsed.object_count, 2);
    Ok(())
}

#[tokio::test]
async fn refuses_to_point_at_objects_that_are_not_there() -> anyhow::Result<()> {
    let root = temporary_root("absent-object");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let mut artifacts = seed_generation(&store, 1, &[PNU_A]).await?;
    artifacts.push(ExportArtifactInput {
        pnu: PNU_B.to_owned(),
        object_key: parcel_by_pnu_serving_object_key(1, PNU_B)?,
        object_checksum_sha256: "a".repeat(64),
    });

    let result = publish(
        &config(root.clone(), false, true),
        &store,
        &summary(1, artifacts),
    )
    .await;

    let manifest_missing = store
        .read_bytes(parcel_by_pnu_serving_manifest_key()?)
        .await
        .is_err();
    std::fs::remove_dir_all(&root)?;
    assert!(result.is_err(), "a missing object was pointed at anyway");
    assert!(
        manifest_missing,
        "a refused publish still wrote the manifest"
    );
    Ok(())
}

#[tokio::test]
async fn refuses_a_generation_that_moves_backwards_or_stands_still_silently() -> anyhow::Result<()>
{
    let root = temporary_root("regression");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let generation_two = seed_generation(&store, 2, &[PNU_A]).await?;
    let generation_one = seed_generation(&store, 1, &[PNU_A]).await?;
    publish(
        &config(root.clone(), false, true),
        &store,
        &summary(2, generation_two.clone()),
    )
    .await?;

    let backwards = publish(
        &config(root.clone(), false, false),
        &store,
        &summary(1, generation_one),
    )
    .await;
    let silent_repoint = publish(
        &config(root.clone(), false, false),
        &store,
        &summary(2, generation_two.clone()),
    )
    .await;
    let stated_repoint = publish(
        &config(root.clone(), true, false),
        &store,
        &summary(2, generation_two),
    )
    .await;

    std::fs::remove_dir_all(&root)?;
    assert!(backwards.is_err(), "the generation moved backwards");
    assert!(silent_repoint.is_err(), "a repoint went through unstated");
    assert!(stated_repoint.is_ok(), "a stated repoint was refused");
    Ok(())
}

#[tokio::test]
async fn first_publication_must_be_stated_and_cannot_shadow_an_existing_manifest(
) -> anyhow::Result<()> {
    let root = temporary_root("first-publication");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let artifacts = seed_generation(&store, 1, &[PNU_A]).await?;

    let unstated = publish(
        &config(root.clone(), false, false),
        &store,
        &summary(1, artifacts.clone()),
    )
    .await;
    publish(
        &config(root.clone(), false, true),
        &store,
        &summary(1, artifacts.clone()),
    )
    .await?;
    let shadowed = publish(
        &config(root.clone(), false, true),
        &store,
        &summary(1, artifacts),
    )
    .await;

    std::fs::remove_dir_all(&root)?;
    assert!(
        unstated.is_err(),
        "an unreadable manifest was treated as a first publication without the operator saying so"
    );
    assert!(
        shadowed.is_err(),
        "first-publication was accepted although a manifest already exists"
    );
    Ok(())
}

#[tokio::test]
async fn refuses_an_empty_bake_and_a_key_from_another_generation() -> anyhow::Result<()> {
    let root = temporary_root("shape");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;

    let empty = publish(
        &config(root.clone(), false, true),
        &store,
        &summary(1, Vec::new()),
    )
    .await;

    let mut mismatched = seed_generation(&store, 1, &[PNU_A]).await?;
    mismatched[0].object_key = parcel_by_pnu_serving_object_key(2, PNU_A)?;
    let cross_generation = publish(
        &config(root.clone(), false, true),
        &store,
        &summary(1, mismatched),
    )
    .await;

    std::fs::remove_dir_all(&root)?;
    assert!(empty.is_err(), "an empty bake was published");
    assert!(
        cross_generation.is_err(),
        "an object key from another generation was accepted"
    );
    Ok(())
}

const SNAPSHOT: &str = "999990000000000001";

async fn seed_documents(
    store: &ParcelServingObjectStore,
    generation: u64,
    snapshot: &str,
    pnus: &[&str],
) -> anyhow::Result<()> {
    for pnu in pnus {
        let body = format!(
            "{{\"schema_version\":\"{PARCEL_DOCUMENT_SCHEMA_VERSION}\",\"pnu\":\"{pnu}\",\
             \"source\":{{\"iceberg_snapshot_id\":\"{snapshot}\"}}}}\n"
        )
        .into_bytes();
        let checksum = format!("{:x}", Sha256::digest(&body));
        let key = parcel_by_pnu_serving_object_key(generation, pnu)?;
        store
            .write_object_create_only(&key, &body, &checksum)
            .await?;
    }
    Ok(())
}

fn expectation(generation: u64, snapshot: &str, count: u64) -> ListingExpectation {
    ListingExpectation {
        target_generation: generation,
        expected_gold_iceberg_snapshot_id: snapshot.to_owned(),
        expected_object_count: count,
    }
}

#[tokio::test]
async fn a_listing_publish_pins_what_the_operator_stated() -> anyhow::Result<()> {
    let root = temporary_root("listing-happy");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    seed_documents(&store, 1, SNAPSHOT, &[PNU_A, PNU_B]).await?;

    let manifest = publish_from_listing(
        &config(root.clone(), false, true),
        &store,
        &expectation(1, SNAPSHOT, 2),
    )
    .await?;
    let stored = store
        .read_bytes(parcel_by_pnu_serving_manifest_key()?)
        .await?;
    let parsed: ParcelServingManifest = serde_json::from_slice(&stored)?;

    std::fs::remove_dir_all(&root)?;
    assert_eq!(manifest.object_count, 2);
    assert_eq!(manifest.gold_iceberg_snapshot_id, SNAPSHOT);
    assert_eq!(parsed.current_generation, 1);
    Ok(())
}

#[tokio::test]
async fn a_listing_that_disagrees_with_the_stated_count_is_refused() -> anyhow::Result<()> {
    let root = temporary_root("listing-count");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    seed_documents(&store, 1, SNAPSHOT, &[PNU_A, PNU_B]).await?;

    let missing_shard = publish_from_listing(
        &config(root.clone(), false, true),
        &store,
        &expectation(1, SNAPSHOT, 3),
    )
    .await;

    std::fs::remove_dir_all(&root)?;
    let error = missing_shard.expect_err("a short listing was published");
    assert!(error.to_string().contains("lists 2"), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_document_from_another_snapshot_is_refused() -> anyhow::Result<()> {
    let root = temporary_root("listing-snapshot");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    seed_documents(&store, 1, "999990000000000002", &[PNU_A]).await?;

    let stale = publish_from_listing(
        &config(root.clone(), false, true),
        &store,
        &expectation(1, SNAPSHOT, 1),
    )
    .await;

    std::fs::remove_dir_all(&root)?;
    let error = stale.expect_err("a document of another gold snapshot was published");
    assert!(
        error.to_string().contains("baked from gold snapshot"),
        "{error}"
    );
    Ok(())
}
