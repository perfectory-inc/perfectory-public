//! The verified re-base (root ADR-0146 §1): what it calls equal, and every way it refuses.
//!
//! PNUs and snapshot ids sit in the repository-reserved synthetic namespaces
//! (`scripts/guard/public-fixture-safety.py`).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context as _;
use lakehouse_domain::GOLD_PARCEL_PANEL;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use super::{content_digest, gold_digest, verify, GoldShard, VerifyConfig, JOB_NAME};
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_serving_manifest::{ServedManifest, ServingManifest};
use crate::by_pnu_serving_manifest_publish::{publish, PatchInput, PublishConfig, PublishInput};
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::parcel_by_pnu_serving_export::parcel_document::{
    self, GoldSnapshotProvenance, PARCEL_DOCUMENT_SCHEMA_VERSION,
};
use crate::r2_layout::by_pnu;

const LANE: ByPnuLane = ByPnuLane::Parcel;
/// The snapshot the served base was baked from (the one the manifest reflects).
const SERVED_SNAPSHOT: &str = "999990000000000001";
/// The current Gold snapshot.
const GOLD_SNAPSHOT: &str = "999990000000000002";
/// Gold columns that are lineage, not content (`parcel_panel_silver_to_gold.LINEAGE_COLUMNS`).
const LINEAGE_COLUMNS: [&str; 3] = ["row_digest", "source_snapshot_id", "published_at_utc"];

fn provenance(snapshot: &str) -> GoldSnapshotProvenance {
    GoldSnapshotProvenance {
        table: "gold.parcel_panel".to_owned(),
        iceberg_snapshot_id: snapshot.to_owned(),
        metadata_location: format!("s3://lakehouse/metadata/{snapshot}.metadata.json"),
        manifest_list_location: format!("s3://lakehouse/metadata/snap-{snapshot}.avro"),
    }
}

fn pnu(n: u32) -> String {
    format!("9999900000100{n:06}")
}

fn row(pnu: &str) -> JsonMap<String, JsonValue> {
    let JsonValue::Object(map) = json!({
        "pnu": pnu,
        "kind": null,
        "area_m2": 331,
        "zonings_json": "[{\"zone_code\":\"UQA320\",\"zone_name\":\"synthetic zone\",\"anchor_code\":\"UQA320\",\"inclusion_code\":\"1\"}]",
        "price_json": "{\"price_per_m2\":123000,\"base_year\":2026,\"base_month\":1,\"announced_date\":\"2026-01-01\"}",
        "characteristics_json": null,
        "forest_ledger_json": null,
        "transfer_history_json": "[]",
        "land_rights_json": "[]",
        "land_right_total": 0,
        "attached_via_json": null,
        "row_digest": "a-digest",
        "source_snapshot_id": "999990000000000003",
        "published_at_utc": "2026-01-01T00:00:00Z"
    }) else {
        unreachable!("fixture literal is an object");
    };
    map
}

fn digest_of(snapshot: &str, row: &JsonMap<String, JsonValue>) -> anyhow::Result<[u8; 32]> {
    Ok(gold_digest(&provenance(snapshot), row)?.1)
}

/// A change to every content column a document shows. Each must be visible to `row_digest`'s
/// fingerprint too, so the table is keyed by the Gold contract's columns.
fn content_perturbations() -> BTreeMap<&'static str, JsonValue> {
    BTreeMap::from([
        ("pnu", json!(pnu(999))),
        ("kind", json!("synthetic kind")),
        ("area_m2", json!(332)),
        (
            "zonings_json",
            json!("[{\"zone_code\":\"UQA121\",\"zone_name\":\"synthetic zone\",\"anchor_code\":\"UQA121\",\"inclusion_code\":\"1\"}]"),
        ),
        (
            "price_json",
            json!("{\"price_per_m2\":124000,\"base_year\":2026,\"base_month\":1,\"announced_date\":\"2026-01-01\"}"),
        ),
        (
            "characteristics_json",
            json!("{\"land_category\":\"synthetic\",\"area_m2\":331.0,\"land_use_situation\":null,\"terrain_height\":null,\"terrain_shape\":null,\"road_contact\":null}"),
        ),
        (
            "forest_ledger_json",
            json!("{\"land_category\":\"synthetic\",\"area_m2\":331.0,\"ownership_kind\":null,\"co_owner_count\":1}"),
        ),
        (
            "transfer_history_json",
            json!("[{\"reason\":\"synthetic\",\"reason_code\":\"01\",\"moved_at\":\"2026-01-01\",\"erased_at\":null,\"land_category\":null,\"area_m2\":null,\"history_seq\":1,\"parcel_history_seq\":\"1\",\"closure_seq\":null}]"),
        ),
        (
            "land_rights_json",
            json!("[{\"right_serial_no\":\"1\",\"building_name\":null,\"right_ratio\":\"1/2\",\"closure_kind\":null,\"closure_kind_code\":null}]"),
        ),
        ("land_right_total", json!(1)),
        ("attached_via_json", json!("{\"price\":\"lineage:code_derived\"}")),
    ])
}

#[test]
fn every_content_column_of_the_gold_contract_changes_the_content_digest() -> anyhow::Result<()> {
    let perturbations = content_perturbations();
    let contract_content = GOLD_PARCEL_PANEL
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !LINEAGE_COLUMNS.contains(name))
        .collect::<Vec<_>>();
    for column in &contract_content {
        assert!(
            perturbations.contains_key(column),
            "the Gold contract's content column {column} has no perturbation here: a column \
             row_digest covers must be shown to change the served document"
        );
    }
    let base = row(&pnu(1));
    let before = digest_of(GOLD_SNAPSHOT, &base)?;
    for (column, value) in perturbations {
        let mut changed = base.clone();
        changed.insert(column.to_owned(), value);
        let after = digest_of(GOLD_SNAPSHOT, &changed)
            .with_context(|| format!("the {column} perturbation does not build"))?;
        assert_ne!(
            before, after,
            "a change to {column} is invisible to the re-base"
        );
    }
    Ok(())
}

#[test]
fn lineage_and_the_source_block_never_change_the_content_digest() -> anyhow::Result<()> {
    let base = row(&pnu(1));
    let before = digest_of(GOLD_SNAPSHOT, &base)?;
    assert_eq!(
        before,
        digest_of(SERVED_SNAPSHOT, &base)?,
        "the source block counted"
    );
    for column in LINEAGE_COLUMNS {
        let mut moved = base.clone();
        moved.insert(column.to_owned(), json!("999990000000000009"));
        assert_eq!(
            before,
            digest_of(GOLD_SNAPSHOT, &moved)?,
            "{column} counted"
        );
    }
    // Key order and whitespace of the stored bytes do not count either: a served document is
    // compared by what it says.
    let artifact = parcel_document::build(&provenance(SERVED_SNAPSHOT), &base)?;
    let compact = serde_json::to_vec(&serde_json::from_slice::<JsonValue>(&artifact.body)?)?;
    assert_ne!(compact, artifact.body);
    assert_eq!(content_digest(&compact)?, before);
    Ok(())
}

// ---- The comparison over a local store --------------------------------------------------------

struct Lane {
    root: PathBuf,
    store: ByPnuServingStore,
    work: PathBuf,
}

impl Drop for Lane {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A served base generation 1 of `served` rows baked from the served snapshot, under a v2
/// manifest that reflects that snapshot.
async fn lane(label: &str, served: &[JsonMap<String, JsonValue>]) -> anyhow::Result<Lane> {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-by-pnu-rebase-{label}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root)?;
    let store = ByPnuServingStore::open(LANE, &ProfileStoreConfig::Local { root: root.clone() })?;
    for row in served {
        let artifact = parcel_document::build(&provenance(SERVED_SNAPSHOT), row)?;
        store
            .write_object_create_only(
                &by_pnu::object_key(LANE, 1, &artifact.pnu)?,
                &artifact.body,
                &artifact.checksum_sha256,
            )
            .await?;
    }
    let count = u64::try_from(served.len())?;
    let manifest = ServingManifest {
        schema_version: 2,
        unit: LANE.unit().to_owned(),
        base_generation: 1,
        base_object_count: count,
        document_schema_version: PARCEL_DOCUMENT_SCHEMA_VERSION.to_owned(),
        gold_table: "gold.parcel_panel".to_owned(),
        gold_iceberg_snapshot_id: SERVED_SNAPSHOT.to_owned(),
        reflected_gold_iceberg_snapshot_id: SERVED_SNAPSHOT.to_owned(),
        pnu_prefix_length: crate::by_pnu_gateway_contract::by_pnu_serving_patch_policy()?
            .pnu_prefix_length,
        patches: Vec::new(),
        object_count: count,
        published_at_utc: "2026-01-01T00:00:00Z".to_owned(),
        verified_rebase: None,
        reflected_gold_snapshot_tag: None,
    };
    let body = manifest.to_bytes()?;
    store
        .write_manifest(
            by_pnu::manifest_key(LANE)?,
            &body,
            &format!("{:x}", Sha256::digest(&body)),
            None,
        )
        .await?;
    let work = root.join("work");
    Ok(Lane { root, store, work })
}

impl Lane {
    fn config(&self) -> VerifyConfig {
        VerifyConfig {
            reason: "the reflected snapshot carries no row_digest".to_owned(),
            run_id: "rebase-test-1".to_owned(),
            expected_gold_iceberg_snapshot_id: GOLD_SNAPSHOT.to_owned(),
            work_dir: self.work.clone(),
            sample_prefixes: None,
            max_concurrency: 4,
            chunk_objects: 2,
        }
    }

    async fn served(&self) -> anyhow::Result<ServedManifest> {
        let (bytes, _) = self.store.read_manifest().await?;
        ServedManifest::parse(LANE, &bytes)
    }

    /// Runs the comparison against `gold` rows, reading through `read`.
    async fn verify_with(
        &self,
        config: &VerifyConfig,
        gold: &[JsonMap<String, JsonValue>],
        fail_reads_of: Option<&str>,
        reads: &AtomicUsize,
    ) -> anyhow::Result<super::Outcome> {
        let served = self.served().await?;
        let gold_shard = |prefix: String| {
            let rows = gold.to_vec();
            async move {
                let mut digests = HashMap::new();
                for row in rows.iter().filter(|row| {
                    row["pnu"]
                        .as_str()
                        .is_some_and(|pnu| pnu.starts_with(&prefix))
                }) {
                    let (pnu, digest) = gold_digest(&provenance(GOLD_SNAPSHOT), row)?;
                    digests.insert(pnu, digest);
                }
                Ok(GoldShard {
                    digests,
                    table_rows: u64::try_from(rows.len())?,
                })
            }
        };
        let store = &self.store;
        let read = |key: String| async move {
            reads.fetch_add(1, Ordering::SeqCst);
            if fail_reads_of.is_some_and(|bad| key.ends_with(&format!("/{bad}.json"))) {
                anyhow::bail!("planted: {key} cannot be read");
            }
            store.read_bytes(&key).await
        };
        verify(config, store, &served, gold_shard, read).await
    }

    fn lines(&self, name: &str) -> anyhow::Result<Vec<String>> {
        Ok(std::fs::read_to_string(self.work.join(name))?
            .lines()
            .map(ToOwned::to_owned)
            .collect())
    }

    fn summary(&self) -> anyhow::Result<JsonValue> {
        Ok(serde_json::from_slice(&std::fs::read(
            self.work.join("change-set.json"),
        )?)?)
    }

    fn publish_config(&self, patch: Option<u64>) -> PublishConfig {
        PublishConfig {
            output: ProfileStoreConfig::Local {
                root: self.root.clone(),
            },
            input: PublishInput::Patch(PatchInput {
                base_generation: 1,
                patch,
                expected_gold_iceberg_snapshot_id: GOLD_SNAPSHOT.to_owned(),
                change_set_summary: self.work.join("change-set.json"),
                upserts: self.work.join("upserts.txt"),
                deletes: self.work.join("deletes.txt"),
            }),
            first_publication: false,
        }
    }
}

fn rows(count: u32) -> Vec<JsonMap<String, JsonValue>> {
    (1..=count).map(|n| row(&pnu(n))).collect()
}

/// The gateway is never asked: the lane already serves a v2 manifest.
const NO_GATEWAY: &str = "http://127.0.0.1:9";

#[tokio::test]
async fn an_unchanged_lane_reflects_and_records_the_verification() -> anyhow::Result<()> {
    let served = rows(5);
    let lane = lane("equal", &served).await?;
    let reads = AtomicUsize::new(0);
    let outcome = lane
        .verify_with(&lane.config(), &served, None, &reads)
        .await?;
    assert_eq!(outcome.totals.equal, 5);
    assert_eq!(outcome.totals.served_objects_read, 5);
    assert!(lane.lines("upserts.txt")?.is_empty());
    assert!(lane.lines("deletes.txt")?.is_empty());
    let summary = lane.summary()?;
    assert_eq!(summary["job_name"], JOB_NAME);
    assert_eq!(summary["input"]["baseline_snapshot_id"], SERVED_SNAPSHOT);

    // The empty change set is a reflect: only the reflected snapshot moves, and the manifest
    // records the run that proved it.
    let manifest = publish(&lane.publish_config(None), &lane.store, NO_GATEWAY).await?;
    assert!(manifest.patches.is_empty());
    assert_eq!(manifest.reflected_gold_iceberg_snapshot_id, GOLD_SNAPSHOT);
    let record = manifest
        .verified_rebase
        .context("the reflect did not record the re-base")?;
    assert_eq!(record.run_id, "rebase-test-1");
    assert_eq!(record.served_objects_read, 5);
    assert_eq!(record.equal, 5);
    assert_eq!(record.baseline_gold_iceberg_snapshot_id, SERVED_SNAPSHOT);
    Ok(())
}

#[tokio::test]
async fn a_changed_document_becomes_a_patch_not_a_reflect() -> anyhow::Result<()> {
    // A hundred parcels, so three changes stay inside the contract's patch bounds.
    let served = rows(100);
    let lane = lane("changed", &served).await?;
    let mut gold = served.clone();
    // Planted: one parcel's price moved; one parcel left Gold; one parcel is new.
    gold[1].insert(
        "price_json".to_owned(),
        json!("{\"price_per_m2\":999000,\"base_year\":2026,\"base_month\":1,\"announced_date\":\"2026-01-01\"}"),
    );
    gold.remove(4);
    gold.push(row(&pnu(101)));
    let reads = AtomicUsize::new(0);
    let outcome = lane
        .verify_with(&lane.config(), &gold, None, &reads)
        .await?;
    assert_eq!(
        (
            outcome.totals.equal,
            outcome.totals.changed,
            outcome.totals.only_served,
            outcome.totals.only_gold
        ),
        (98, 1, 1, 1)
    );
    assert_eq!(lane.lines("upserts.txt")?, vec![pnu(2), pnu(101)]);
    assert_eq!(lane.lines("deletes.txt")?, vec![pnu(5)]);
    assert_eq!(lane.summary()?["quality_metrics"]["new_count"], 1);

    // It is not a reflect: a publish without a patch is refused, the manifest stays.
    let error = publish(&lane.publish_config(None), &lane.store, NO_GATEWAY)
        .await
        .err()
        .context("a re-base that found a changed document was published as a reflect")?;
    assert!(
        format!("{error:#}").contains("needs a target patch generation"),
        "{error:#}"
    );
    assert_eq!(
        lane.served().await?.reflected_gold_iceberg_snapshot_id,
        SERVED_SNAPSHOT
    );

    // Baked as a patch, it publishes as one.
    for gold_row in gold
        .iter()
        .filter(|row| row["pnu"] == pnu(2) || row["pnu"] == pnu(101))
    {
        let artifact = parcel_document::build(&provenance(GOLD_SNAPSHOT), gold_row)?;
        lane.store
            .write_object_create_only(
                &by_pnu::patch_object_key(LANE, 1, 1, &artifact.pnu)?,
                &artifact.body,
                &artifact.checksum_sha256,
            )
            .await?;
    }
    let (tombstone, checksum) = crate::by_pnu_serving_manifest::tombstone_body(
        &pnu(5),
        "gold.parcel_panel",
        GOLD_SNAPSHOT,
    )?;
    lane.store
        .write_object_create_only(
            &by_pnu::patch_object_key(LANE, 1, 1, &pnu(5))?,
            &tombstone,
            &checksum,
        )
        .await?;
    let manifest = publish(&lane.publish_config(Some(1)), &lane.store, NO_GATEWAY).await?;
    assert_eq!(manifest.patches.len(), 1);
    assert_eq!(
        (manifest.patches[0].upserted, manifest.patches[0].deleted),
        (2, 1)
    );
    assert_eq!(manifest.object_count, 100);
    assert_eq!(
        manifest.verified_rebase.map(|record| (
            record.changed,
            record.only_served,
            record.only_gold
        )),
        Some((1, 1, 1))
    );

    // And a second re-base over the patched lane finds it equal: patches are read too, and the
    // tombstone answers for the deleted parcel.
    let mut config = lane.config();
    config.work_dir = lane.root.join("work-2");
    config.run_id = "rebase-test-2".to_owned();
    let again = lane.verify_with(&config, &gold, None, &reads).await?;
    assert_eq!(
        (
            again.totals.equal,
            again.totals.changed,
            again.totals.only_served,
            again.totals.only_gold,
            again.totals.tombstones_read
        ),
        (100, 0, 0, 0, 1)
    );
    Ok(())
}

#[tokio::test]
async fn one_unreadable_object_refuses_the_rebase_and_a_rerun_resumes() -> anyhow::Result<()> {
    let served = rows(6);
    let lane = lane("unreadable", &served).await?;
    let reads = AtomicUsize::new(0);
    let error = lane
        .verify_with(&lane.config(), &served, Some(&pnu(5)), &reads)
        .await
        .err()
        .context("a re-base with an unreadable object completed")?;
    assert!(
        format!("{error:#}").contains("could not be read"),
        "{error:#}"
    );
    assert!(
        !lane.work.join("change-set.json").exists(),
        "an incomplete comparison wrote a change set"
    );

    // The rerun reads only the chunk that failed (chunks of two objects: 1-2, 3-4 done; 5-6).
    let rerun = AtomicUsize::new(0);
    let outcome = lane
        .verify_with(&lane.config(), &served, None, &rerun)
        .await?;
    assert_eq!(outcome.totals.equal, 6);
    assert_eq!(
        rerun.load(Ordering::SeqCst),
        2,
        "finished chunks were read again"
    );
    assert!(lane.work.join("change-set.json").exists());
    Ok(())
}

#[tokio::test]
async fn counts_that_do_not_add_up_refuse_the_rebase() -> anyhow::Result<()> {
    let served = rows(4);
    let lane = lane("counts", &served).await?;
    // Planted: an object the manifest does not count (a foreign write in the base).
    let extra = parcel_document::build(&provenance(SERVED_SNAPSHOT), &row(&pnu(9)))?;
    lane.store
        .write_object_create_only(
            &by_pnu::object_key(LANE, 1, &extra.pnu)?,
            &extra.body,
            &extra.checksum_sha256,
        )
        .await?;
    let reads = AtomicUsize::new(0);
    let error = lane
        .verify_with(&lane.config(), &served, None, &reads)
        .await
        .err()
        .context("a re-base whose listing disagrees with the manifest completed")?;
    assert!(format!("{error:#}").contains("incomplete"), "{error:#}");
    assert!(!lane.work.join("change-set.json").exists());
    Ok(())
}

#[tokio::test]
async fn a_work_directory_of_another_run_is_refused() -> anyhow::Result<()> {
    let served = rows(2);
    let lane = lane("other-run", &served).await?;
    let reads = AtomicUsize::new(0);
    lane.verify_with(&lane.config(), &served, None, &reads)
        .await?;
    let mut other = lane.config();
    other.run_id = "rebase-test-other".to_owned();
    let error = lane
        .verify_with(&other, &served, None, &reads)
        .await
        .err()
        .context("a re-base adopted another run's work directory")?;
    assert!(format!("{error:#}").contains("another run"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_sample_measures_and_writes_no_change_set() -> anyhow::Result<()> {
    let served = rows(4);
    let lane = lane("sample", &served).await?;
    let mut config = lane.config();
    config.sample_prefixes = Some(vec![pnu(1)[..18].to_owned()]);
    let reads = AtomicUsize::new(0);
    let outcome = lane.verify_with(&config, &served, None, &reads).await?;
    assert_eq!(outcome.totals.served_objects_read, 4);
    assert!(lane.work.join("sample-summary.json").exists());
    assert!(!lane.work.join("change-set.json").exists());
    Ok(())
}
