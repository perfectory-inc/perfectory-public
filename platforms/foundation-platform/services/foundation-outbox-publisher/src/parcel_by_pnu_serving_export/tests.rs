use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;

use serde_json::{json, Map as JsonMap, Value as JsonValue};

use super::parcel_document::{self, GoldSnapshotProvenance};
use super::{
    select_rows, spread_write_order, write_artifacts, write_with_policy, ServingExportConfig,
};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::parcel_by_pnu_serving_store::ParcelServingObjectStore;
use crate::r2_layout::parcel_by_pnu_serving_object_key;

// PNUs and snapshot ids sit in the repository-reserved synthetic namespaces
// (`scripts/guard/public-fixture-safety.py`).
const PNU_A: &str = "9999900000100000000";
const PNU_B: &str = "9999900000200000000";

fn temporary_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "foundation-platform-parcel-serving-export-{label}-{}",
        uuid::Uuid::now_v7()
    ))
}

fn provenance() -> GoldSnapshotProvenance {
    GoldSnapshotProvenance {
        table: "gold.parcel_panel".to_owned(),
        iceberg_snapshot_id: "999990000000000001".to_owned(),
        metadata_location: "s3://lakehouse/metadata/00001.metadata.json".to_owned(),
        manifest_list_location: "s3://lakehouse/metadata/snap-1.avro".to_owned(),
    }
}

fn row(pnu: &str) -> JsonMap<String, JsonValue> {
    let JsonValue::Object(map) = json!({
        "pnu": pnu,
        "area_m2": 331,
        "zonings_json": "[]",
        "price_json": null,
        "characteristics_json": null,
        "forest_ledger_json": null,
        "transfer_history_json": "[]",
        "land_rights_json": "[]",
        "land_right_total": 0,
        "source_snapshot_id": "999990000000000002"
    }) else {
        unreachable!("fixture literal is an object");
    };
    map
}

fn config(root: PathBuf, allow_overwrite: bool) -> ServingExportConfig {
    ServingExportConfig {
        output: ProfileStoreConfig::Local { root },
        target_generation: 1,
        expected_row_count: None,
        max_concurrency: 1,
        summary_path: None,
        allow_overwrite,
        pnu_allowlist: None,
        resume_from_listing: true,
        pnu_prefix: None,
    }
}

#[test]
fn select_rows_refuses_duplicates_and_scopes_to_the_allowlist() -> anyhow::Result<()> {
    let rows = vec![row(PNU_A), row(PNU_B)];

    let all = select_rows(&rows, None)?;
    assert_eq!(all.len(), 2);

    let allowlist = BTreeSet::from([PNU_A.to_owned()]);
    let scoped = select_rows(&rows, Some(&allowlist))?;
    assert_eq!(scoped.len(), 1);
    assert_eq!(
        scoped[0].get("pnu").and_then(JsonValue::as_str),
        Some(PNU_A)
    );

    let duplicated = vec![row(PNU_A), row(PNU_A)];
    let error = select_rows(&duplicated, None).expect_err("duplicate PNU must refuse");
    assert!(error.to_string().contains(PNU_A), "{error}");

    let absent = BTreeSet::from(["9999900000800000000".to_owned()]);
    let error =
        select_rows(&rows, Some(&absent)).expect_err("allowlisting an absent parcel must refuse");
    assert!(error.to_string().contains("9999900000800000000"), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_re_run_reuses_and_a_change_needs_the_stated_overwrite() -> anyhow::Result<()> {
    let root = temporary_root("write-policy");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let baseline_config = config(root.clone(), false);
    let key = parcel_by_pnu_serving_object_key(1, PNU_A)?;

    let artifact = parcel_document::build(&provenance(), &row(PNU_A))?;
    let first = write_with_policy(&baseline_config, &store, &key, &artifact).await?;
    let second = write_with_policy(&baseline_config, &store, &key, &artifact).await?;

    let mut changed_row = row(PNU_A);
    changed_row.insert("area_m2".to_owned(), json!(999));
    let changed = parcel_document::build(&provenance(), &changed_row)?;
    let refused = write_with_policy(&baseline_config, &store, &key, &changed).await;
    let overwritten =
        write_with_policy(&config(root.clone(), true), &store, &key, &changed).await?;
    let stored = store.read_bytes(&key).await?;

    std::fs::remove_dir_all(&root)?;
    assert_eq!(first, "created");
    assert_eq!(second, "reused");
    assert!(
        refused.is_err(),
        "changed bytes were written without the stated overwrite"
    );
    assert_eq!(overwritten, "overwritten");
    assert_eq!(stored, changed.body, "the delta re-bake did not land");
    Ok(())
}

#[tokio::test]
async fn a_listed_generation_key_is_skipped_and_the_rest_are_written() -> anyhow::Result<()> {
    let root = temporary_root("resume-listing");
    let store = ParcelServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
    let artifact = parcel_document::build(&provenance(), &row(PNU_A))?;
    let key = parcel_by_pnu_serving_object_key(1, PNU_A)?;
    store
        .write_object_create_only(&key, &artifact.body, &artifact.checksum_sha256)
        .await?;

    let existing = store.list_existing_generation_keys(1).await?;
    let other_generation = store.list_existing_generation_keys(2).await?;

    let rows = [row(PNU_A), row(PNU_B)];
    let selected = rows.iter().collect::<Vec<_>>();
    let entries = write_artifacts(
        &config(root.clone(), false),
        &store,
        &provenance(),
        &selected,
        &existing,
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
        parcel_by_pnu_serving_object_key(7, PNU_B)?,
        format!("serving/parcels/by-pnu/v7/{PNU_B}.json")
    );
    Ok(())
}
