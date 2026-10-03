use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;

use serde_json::{json, Map as JsonMap, Value as JsonValue};

use super::building_document::{self, GoldSnapshotProvenance};
use super::{
    claim_a_fresh_shard, refuse_a_moved_table, select_rows, spread_write_order, write_artifacts,
    write_create_only, ServingExportConfig, LANE,
};
use crate::by_pnu_serving_patch_export::PatchTarget;
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;

// PNUs and snapshot ids sit in the repository-reserved synthetic namespaces
// (`scripts/guard/public-fixture-safety.py`).
const PNU_A: &str = "9999900000100000000";
const PNU_B: &str = "9999900000200000000";

fn temporary_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "foundation-platform-building-serving-export-{label}-{}",
        uuid::Uuid::now_v7()
    ))
}

fn provenance() -> GoldSnapshotProvenance {
    GoldSnapshotProvenance {
        table: "gold.building_panel".to_owned(),
        iceberg_snapshot_id: "999990000000000001".to_owned(),
        metadata_location: "s3://lakehouse/metadata/00001.metadata.json".to_owned(),
        manifest_list_location: "s3://lakehouse/metadata/snap-1.avro".to_owned(),
    }
}

fn row(pnu: &str) -> JsonMap<String, JsonValue> {
    JsonMap::from_iter([
        ("pnu".to_owned(), json!(pnu)),
        ("buildings_json".to_owned(), json!("[]")),
        ("unlinked_units_json".to_owned(), json!("[]")),
        ("source_snapshot_id".to_owned(), json!("999990000000000002")),
    ])
}

fn config(root: PathBuf) -> ServingExportConfig {
    ServingExportConfig {
        output: ProfileStoreConfig::Local { root },
        target_generation: 1,
        expected_row_count: None,
        max_concurrency: 1,
        summary_path: None,
        pnu_allowlist: None,
        patch: None,
        resume_from_listing: true,
        pnu_prefix: None,
        expected_gold_snapshot: None,
        fresh_generation: false,
        fresh_check_marker: None,
    }
}

#[test]
fn a_shard_refuses_when_gold_moved_after_the_bake_began() -> anyhow::Result<()> {
    // Synthetic snapshot ids (`scripts/guard/public-fixture-safety.py`).
    refuse_a_moved_table(None, "gold.panel", "999990000000000002")?;
    refuse_a_moved_table(
        Some("999990000000000002"),
        "gold.panel",
        "999990000000000002",
    )?;
    let moved = refuse_a_moved_table(
        Some("999990000000000001"),
        "gold.panel",
        "999990000000000002",
    );
    assert!(
        moved.is_err_and(|error| error.to_string().contains("moved during the bake")),
        "a shard baked from a newer snapshot than the bake's would mix two snapshots"
    );
    Ok(())
}

#[tokio::test]
async fn a_fresh_generation_holding_objects_is_refused_and_only_an_empty_one_is_marked(
) -> anyhow::Result<()> {
    let root = temporary_root("fresh-claimed");
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    let marker = root.join("shard.fresh-checked");
    let mut fresh = config(root.clone());
    fresh.fresh_generation = true;
    fresh.fresh_check_marker = Some(marker.clone());
    // Empty: a fresh generation starts, and says so before its first write.
    std::fs::create_dir_all(&root)?;
    claim_a_fresh_shard(
        &fresh,
        store.list_existing_generation_keys(1, None).await?.len(),
    )?;
    let marked = marker.exists();
    std::fs::remove_file(&marker)?;

    // An older bake left an object in generation 1 and recorded nothing.
    let artifact = building_document::build(&provenance(), &row(PNU_A))?;
    store
        .write_object_create_only(
            &crate::r2_layout::by_pnu::object_key(LANE, 1, PNU_A)?,
            &artifact.body,
            &artifact.checksum_sha256,
        )
        .await?;
    let listed = store.list_existing_generation_keys(1, None).await?.len();
    let refused = claim_a_fresh_shard(&fresh, listed);
    let marked_on_refusal = marker.exists();

    std::fs::remove_dir_all(&root)?;
    assert!(
        marked,
        "an empty fresh range must be recorded before the first write"
    );
    assert!(
        refused.is_err_and(|error| error.to_string().contains("this run did not start")),
        "a fresh generation must not adopt another bake's objects"
    );
    assert!(
        !marked_on_refusal,
        "a refused range must not be recorded as checked"
    );
    // The run that did start it resumes over its own objects.
    claim_a_fresh_shard(&config(PathBuf::new()), listed)?;
    // A fresh run that cannot record its check is refused rather than left unrecorded.
    let mut unmarked = config(PathBuf::new());
    unmarked.fresh_generation = true;
    assert!(claim_a_fresh_shard(&unmarked, 0).is_err());
    Ok(())
}

#[test]
fn select_rows_refuses_duplicates_and_scopes_to_the_allowlist() -> anyhow::Result<()> {
    let rows = vec![row(PNU_A), row(PNU_B)];

    let all = select_rows(&rows, None, None)?;
    assert_eq!(all.len(), 2);

    let allowlist = BTreeSet::from([PNU_A.to_owned()]);
    let scoped = select_rows(&rows, Some(&allowlist), None)?;
    assert_eq!(scoped.len(), 1);
    assert_eq!(
        scoped[0].get("pnu").and_then(JsonValue::as_str),
        Some(PNU_A)
    );

    let duplicated = vec![row(PNU_A), row(PNU_A)];
    let error = select_rows(&duplicated, None, None)
        .err()
        .ok_or_else(|| anyhow::anyhow!("duplicate PNU must refuse"))?;
    assert!(error.to_string().contains(PNU_A), "{error}");

    let absent = BTreeSet::from(["9999900000800000000".to_owned()]);
    let error = select_rows(&rows, Some(&absent), None)
        .err()
        .ok_or_else(|| anyhow::anyhow!("allowlisting an absent building must refuse"))?;
    assert!(error.to_string().contains("9999900000800000000"), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_re_run_reuses_and_a_changed_document_is_refused_in_place() -> anyhow::Result<()> {
    let root = temporary_root("create-only");
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    let key = crate::r2_layout::by_pnu::object_key(LANE, 1, PNU_A)?;

    let artifact = building_document::build(&provenance(), &row(PNU_A))?;
    let first = write_create_only(&store, &key, &artifact.body, &artifact.checksum_sha256).await?;
    let second = write_create_only(&store, &key, &artifact.body, &artifact.checksum_sha256).await?;

    let mut changed_row = row(PNU_A);
    changed_row.insert(
        "unlinked_units_json".to_owned(),
        json!(serde_json::to_string(&vec![
            building_document::tests::unit(None)?
        ])?),
    );
    let changed = building_document::build(&provenance(), &changed_row)?;
    let refused = write_create_only(&store, &key, &changed.body, &changed.checksum_sha256).await;
    let stored = store.read_bytes(&key).await?;

    std::fs::remove_dir_all(&root)?;
    assert_eq!(first, "created");
    assert_eq!(second, "reused");
    // A changed document goes into a new patch generation (root ADR-0141); nothing overwrites.
    assert!(refused.is_err(), "changed bytes replaced a served object");
    assert_eq!(stored, artifact.body);
    Ok(())
}

#[tokio::test]
async fn a_patch_writes_its_change_set_and_tombstones_into_the_patch_directory(
) -> anyhow::Result<()> {
    let root = temporary_root("patch");
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    let mut patch_config = config(root.clone());
    patch_config.patch = Some(PatchTarget {
        patch: 2,
        deleted: BTreeSet::from([PNU_B.to_owned()]),
    });
    let rows = [row(PNU_A)];
    let selected = rows.iter().collect::<Vec<_>>();
    let entries = write_artifacts(
        &patch_config,
        &store,
        &provenance(),
        &selected,
        &HashSet::new(),
        &crate::building_link_evidence::ApprovedBuildingLinks::default(),
    )
    .await?;
    let listed = crate::by_pnu_serving_patch_export::list_existing(
        &store,
        1,
        patch_config.patch.as_ref(),
        None,
    )
    .await?;
    let base = store.list_existing_generation_keys(1, None).await?;
    std::fs::remove_dir_all(&root)?;
    assert_eq!(
        entries[0].object_key,
        crate::r2_layout::by_pnu::patch_object_key(LANE, 1, 2, PNU_A)?
    );
    assert!(base.is_empty(), "a patch wrote into the base generation");
    assert_eq!(listed.len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_listed_generation_key_is_skipped_and_the_rest_are_written() -> anyhow::Result<()> {
    let root = temporary_root("resume-listing");
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    let artifact = building_document::build(&provenance(), &row(PNU_A))?;
    let key = crate::r2_layout::by_pnu::object_key(LANE, 1, PNU_A)?;
    store
        .write_object_create_only(&key, &artifact.body, &artifact.checksum_sha256)
        .await?;

    let existing = store.list_existing_generation_keys(1, None).await?;
    let other_generation = store.list_existing_generation_keys(2, None).await?;

    let rows = [row(PNU_A), row(PNU_B)];
    let selected = rows.iter().collect::<Vec<_>>();
    let entries = write_artifacts(
        &config(root.clone()),
        &store,
        &provenance(),
        &selected,
        &existing,
        &crate::building_link_evidence::ApprovedBuildingLinks::default(),
    )
    .await?;

    std::fs::remove_dir_all(&root)?;
    assert_eq!(existing, HashSet::from([key]));
    assert!(
        other_generation.is_empty(),
        "another generation must not inherit this one's listing"
    );
    assert_eq!(entries[0].write_outcome, "listed");
    assert_eq!(entries[1].write_outcome, "created");
    Ok(())
}

#[tokio::test]
async fn listed_bytes_cannot_bypass_the_fresh_relationship_check() -> anyhow::Result<()> {
    let root = temporary_root("listed-stale-document");
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    let old = building_document::build(&provenance(), &row(PNU_A))?;
    let key = crate::r2_layout::by_pnu::object_key(LANE, 1, PNU_A)?;
    store
        .write_object_create_only(&key, &old.body, &old.checksum_sha256)
        .await?;
    let mut current = row(PNU_A);
    current.insert(
        "unlinked_units_json".to_owned(),
        json!(serde_json::to_string(&vec![
            building_document::tests::unit(None)?
        ])?),
    );
    let result = write_artifacts(
        &config(root.clone()),
        &store,
        &provenance(),
        &[&current],
        &HashSet::from([key.clone()]),
        &crate::building_link_evidence::ApprovedBuildingLinks::default(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(store.read_bytes(&key).await?, old.body);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn serving_writer_receives_the_current_approval_snapshot() -> anyhow::Result<()> {
    let root = temporary_root("approval-injection");
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    let mut current = row(PNU_A);
    current.insert(
        "unlinked_units_json".to_owned(),
        json!(serde_json::to_string(&vec![
            building_document::tests::unit(None)?
        ])?),
    );
    let active = crate::building_link_evidence::ApprovedBuildingLinks::fixture(
        "11111111-1111-4111-8111-111111111111",
        "source-row:UNIT-1",
        "UNIT-1",
        None,
    );
    let result = write_artifacts(
        &config(root.clone()),
        &store,
        &provenance(),
        &[&current],
        &HashSet::new(),
        &active,
    )
    .await;
    assert!(
        result.is_err(),
        "source-only row must not hide the current approved withdrawal"
    );
    assert!(store
        .read_bytes(&crate::r2_layout::by_pnu::object_key(LANE, 1, PNU_A)?)
        .await
        .is_err());
    if root.exists() {
        std::fs::remove_dir_all(root)?;
    }
    Ok(())
}

#[tokio::test]
async fn a_shard_listing_sees_only_its_own_range() -> anyhow::Result<()> {
    let root = temporary_root("shard-listing");
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    for pnu in [PNU_A, PNU_B] {
        let artifact = building_document::build(&provenance(), &row(pnu))?;
        let key = crate::r2_layout::by_pnu::object_key(LANE, 1, pnu)?;
        store
            .write_object_create_only(&key, &artifact.body, &artifact.checksum_sha256)
            .await?;
    }

    // 예약 픽스처 PNU 는 11번째 자리에서 갈라진다 — 저장소 계층은 프리픽스 길이를 제한하지
    // 않으므로 그 지점까지 잘라 두 샤드를 가른다.
    let shard_a = store
        .list_existing_generation_keys(1, Some("99999000001"))
        .await?;
    let both = store
        .list_existing_generation_keys(1, Some("99999"))
        .await?;

    std::fs::remove_dir_all(&root)?;
    assert_eq!(
        shard_a,
        HashSet::from([crate::r2_layout::by_pnu::object_key(LANE, 1, PNU_A)?]),
        "the shard listing leaked another shard's keys"
    );
    assert_eq!(both.len(), 2);
    Ok(())
}

#[test]
fn neighbouring_pnus_are_pushed_apart_deterministically() {
    let rows = (0..16)
        .map(|n| row(&format!("999990000010000{n:04}")))
        .collect::<Vec<_>>();
    let mut first = rows.iter().collect::<Vec<_>>();
    let mut second = rows.iter().collect::<Vec<_>>();
    spread_write_order(&mut first);
    spread_write_order(&mut second);

    let pnus = |ordered: &[&JsonMap<String, JsonValue>]| {
        ordered
            .iter()
            .filter_map(|row| row.get("pnu").and_then(JsonValue::as_str))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    };
    assert_eq!(pnus(&first), pnus(&second), "the spread order must replay");
    let mut sorted = pnus(&first);
    sorted.sort_unstable();
    assert_ne!(
        pnus(&first),
        sorted,
        "sixteen consecutive PNUs must not stay in key order"
    );
}

#[test]
fn the_object_key_carries_the_target_generation() -> anyhow::Result<()> {
    assert_eq!(
        crate::r2_layout::by_pnu::object_key(LANE, 7, PNU_B)?,
        format!("serving/buildings/by-pnu/v7/{PNU_B}.json")
    );
    Ok(())
}
