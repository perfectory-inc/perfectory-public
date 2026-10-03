//! Planted failures for every publish gate (root ADR-0141 §7). PNUs and snapshot ids sit in the
//! repository-reserved synthetic namespaces (`scripts/guard/public-fixture-safety.py`).

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    document_schema_version, gold_table, publish, ListingExpectation, PatchInput, PublishConfig,
    PublishInput,
};
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_serving_manifest::{tombstone_body, ServedManifest, ServingManifest};
use crate::by_pnu_serving_store::ByPnuServingStore;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::by_pnu;

const PNU_A: &str = "9999900000100000000";
const PNU_B: &str = "9999900000200000000";
const PNU_C: &str = "9999900000100000003";
const PNU_D: &str = "9999900000100000004";
const BASE_SNAPSHOT: &str = "999990000000000001";
const NEXT_SNAPSHOT: &str = "999990000000000002";
const LATER_SNAPSHOT: &str = "999990000000000003";
const LANES: [ByPnuLane; 2] = [ByPnuLane::Parcel, ByPnuLane::Building];
/// Parcels every `base` adds beside the named ones (11th digit 8, so they never collide).
const FILLER: usize = 100;

struct Fixture {
    lane: ByPnuLane,
    root: PathBuf,
    store: ByPnuServingStore,
    gateway: MockServer,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn fixture(lane: ByPnuLane, label: &str) -> anyhow::Result<Fixture> {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-by-pnu-publish-{label}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root)?;
    let store = ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
    let gateway = MockServer::start().await;
    serve_capabilities(&gateway, lane, &[1, 2]).await;
    Ok(Fixture {
        lane,
        root,
        store,
        gateway,
    })
}

async fn serve_capabilities(gateway: &MockServer, lane: ByPnuLane, versions: &[u32]) {
    gateway.reset().await;
    Mock::given(method("GET"))
        .and(path(
            lane.policy()
                .map(|p| p.request_path.capabilities.clone())
                .unwrap_or_default(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "unit": lane.unit(),
            "manifest_schema_versions": versions,
        })))
        .mount(gateway)
        .await;
}

impl Fixture {
    fn config(&self, input: PublishInput, first_publication: bool) -> PublishConfig {
        PublishConfig {
            output: ProfileStoreConfig::Local {
                root: self.root.clone(),
            },
            input,
            first_publication,
        }
    }

    async fn publish(&self, input: PublishInput, first: bool) -> anyhow::Result<ServingManifest> {
        publish(&self.config(input, first), &self.store, &self.gateway.uri()).await
    }

    fn document(&self, pnu: &str, snapshot: &str) -> Vec<u8> {
        format!(
            "{{\"schema_version\":\"{}\",\"pnu\":\"{pnu}\",\"source\":{{\"iceberg_snapshot_id\":\
             \"{snapshot}\"}}}}\n",
            document_schema_version(self.lane)
        )
        .into_bytes()
    }

    async fn put(&self, key: &str, body: &[u8]) -> anyhow::Result<()> {
        let checksum = format!("{:x}", Sha256::digest(body));
        self.store
            .write_object_create_only(key, body, &checksum)
            .await
            .map(|_| ())
    }

    /// Bakes and publishes base generation 3 of `pnus`, plus enough other parcels that a patch of
    /// a few changes stays inside the contract's cumulative ratio.
    async fn base(&self, pnus: &[&str]) -> anyhow::Result<ServingManifest> {
        let filler = (0..FILLER)
            .map(|n| format!("99999{n:05}800000000"))
            .collect::<Vec<_>>();
        let mut all = pnus.to_vec();
        all.extend(filler.iter().map(String::as_str));
        self.base_exact(&all).await
    }

    /// Bakes and publishes base generation 3 of exactly `pnus` from the base snapshot.
    async fn base_exact(&self, pnus: &[&str]) -> anyhow::Result<ServingManifest> {
        for pnu in pnus {
            self.put(
                &by_pnu::object_key(self.lane, 3, pnu)?,
                &self.document(pnu, BASE_SNAPSHOT),
            )
            .await?;
        }
        self.publish(
            PublishInput::Listing(ListingExpectation {
                target_generation: 3,
                expected_gold_iceberg_snapshot_id: BASE_SNAPSHOT.to_owned(),
                expected_object_count: u64::try_from(pnus.len())?,
            }),
            true,
        )
        .await
    }

    /// Writes a patch's objects (documents for upserts, tombstones for deletes).
    async fn bake_patch(
        &self,
        patch: u64,
        snapshot: &str,
        upserts: &[&str],
        deletes: &[&str],
    ) -> anyhow::Result<()> {
        for pnu in upserts {
            self.put(
                &by_pnu::patch_object_key(self.lane, 3, patch, pnu)?,
                &self.document(pnu, snapshot),
            )
            .await?;
        }
        for pnu in deletes {
            let (body, _) = tombstone_body(pnu, gold_table(self.lane), snapshot)?;
            self.put(&by_pnu::patch_object_key(self.lane, 3, patch, pnu)?, &body)
                .await?;
        }
        Ok(())
    }

    /// The change set files the delta job would write.
    fn change_set(
        &self,
        label: &str,
        baseline: &str,
        current: &str,
        upserts: &[&str],
        new: u64,
        deletes: &[&str],
    ) -> anyhow::Result<(PathBuf, PathBuf, PathBuf)> {
        let summary = self.root.join(format!("{label}-change.json"));
        let upsert_path = self.root.join(format!("{label}-upserts.txt"));
        let delete_path = self.root.join(format!("{label}-deletes.txt"));
        write_lines(&upsert_path, upserts)?;
        write_lines(&delete_path, deletes)?;
        std::fs::write(
            &summary,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": "foundation-platform.spark_run_summary.v1",
                "job_name": "by_pnu_panel_delta",
                "contract": gold_table(self.lane),
                "row_count": upserts.len(),
                "quality_metrics": {
                    "changed_count": upserts.len() as u64 - new, "new_count": new,
                    "deleted_count": deletes.len(), "unchanged_count": 10,
                    "upsert_count": upserts.len(), "delete_count": deletes.len(),
                },
                "input": {"baseline_snapshot_id": baseline, "current_snapshot_id": current},
            }))?,
        )?;
        Ok((summary, upsert_path, delete_path))
    }

    fn patch_input(
        &self,
        patch: Option<u64>,
        current: &str,
        files: (PathBuf, PathBuf, PathBuf),
    ) -> PublishInput {
        PublishInput::Patch(PatchInput {
            base_generation: 3,
            patch,
            expected_gold_iceberg_snapshot_id: current.to_owned(),
            change_set_summary: files.0,
            upserts: files.1,
            deletes: files.2,
        })
    }

    async fn live(&self) -> anyhow::Result<ServedManifest> {
        let bytes = self
            .store
            .read_bytes(by_pnu::manifest_key(self.lane)?)
            .await?;
        ServedManifest::parse(self.lane, &bytes)
    }

    fn history(&self) -> anyhow::Result<Vec<String>> {
        let directory = self
            .root
            .join(&self.lane.policy()?.object_key.manifest_history_dir);
        let mut keys = std::fs::read_dir(directory)?
            .map(|entry| {
                entry.map(|entry| {
                    format!(
                        "{}/{}",
                        self.lane
                            .policy()
                            .map(|p| p.object_key.manifest_history_dir.clone())
                            .unwrap_or_default(),
                        entry.file_name().to_string_lossy()
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        keys.sort();
        Ok(keys)
    }
}

fn write_lines(path: &Path, pnus: &[&str]) -> anyhow::Result<()> {
    let body = pnus
        .iter()
        .map(|pnu| format!("{pnu}\n"))
        .collect::<String>();
    std::fs::write(path, body)?;
    Ok(())
}

#[tokio::test]
async fn a_patch_publishes_newest_first_with_its_prefixes_and_counts() -> anyhow::Result<()> {
    for lane in LANES {
        let fx = fixture(lane, "patch").await?;
        let base = fx.base(&[PNU_A, PNU_B, PNU_C]).await?;
        fx.bake_patch(1, NEXT_SNAPSHOT, &[PNU_A, PNU_D], &[PNU_C])
            .await?;
        let files = fx.change_set(
            "p1",
            BASE_SNAPSHOT,
            NEXT_SNAPSHOT,
            &[PNU_A, PNU_D],
            1,
            &[PNU_C],
        )?;
        let manifest = fx
            .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
            .await?;

        assert_eq!(manifest.base_generation, 3);
        assert_eq!(manifest.reflected_gold_iceberg_snapshot_id, NEXT_SNAPSHOT);
        assert_eq!(manifest.gold_iceberg_snapshot_id, BASE_SNAPSHOT);
        // + 1 new - 1 deleted.
        assert_eq!(manifest.object_count, base.object_count);
        assert_eq!(manifest.base_object_count, base.object_count);
        let [patch] = manifest.patches.as_slice() else {
            anyhow::bail!("expected one patch, got {:?}", manifest.patches);
        };
        assert_eq!((patch.generation, patch.upserted, patch.deleted), (1, 2, 1));
        assert_eq!(patch.prefixes, vec!["99999".to_owned()]);
        assert_eq!(fx.live().await?.patches, manifest.patches);

        // The second patch goes in front.
        fx.bake_patch(2, LATER_SNAPSHOT, &[PNU_B], &[]).await?;
        let files = fx.change_set("p2", NEXT_SNAPSHOT, LATER_SNAPSHOT, &[PNU_B], 0, &[])?;
        let manifest = fx
            .publish(fx.patch_input(Some(2), LATER_SNAPSHOT, files), false)
            .await?;
        assert_eq!(
            manifest
                .patches
                .iter()
                .map(|p| p.generation)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        // Every publish stored the manifest it replaced: the base, then patch 1.
        assert_eq!(fx.history()?.len(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn gate_a_a_pnu_of_the_change_set_missing_from_the_patch_is_refused() -> anyhow::Result<()> {
    for lane in LANES {
        let fx = fixture(lane, "gate-a").await?;
        fx.base(&[PNU_A, PNU_B]).await?;
        // The change set names A and B; the export wrote only A.
        fx.bake_patch(1, NEXT_SNAPSHOT, &[PNU_A], &[]).await?;
        let files = fx.change_set("a", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A, PNU_B], 0, &[])?;
        let before = fx.live().await?;
        let error = fx
            .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
            .await
            .expect_err("a patch missing a changed PNU was published");
        assert!(error.to_string().contains("lacks 1 objects"), "{error:#}");
        assert_eq!(
            fx.live().await?,
            before,
            "a refused publish moved the manifest"
        );
    }
    Ok(())
}

#[tokio::test]
async fn gate_b_a_sampled_object_of_another_snapshot_is_refused() -> anyhow::Result<()> {
    for lane in LANES {
        let fx = fixture(lane, "gate-b").await?;
        fx.base(&[PNU_A, PNU_B]).await?;
        // The patch holds a document baked from the base snapshot, not the patch's.
        fx.bake_patch(1, BASE_SNAPSHOT, &[PNU_A], &[]).await?;
        let files = fx.change_set("b", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
        let error = fx
            .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
            .await
            .expect_err("a document of another snapshot was published");
        assert!(
            error.to_string().contains("baked from gold snapshot"),
            "{error:#}"
        );

        // A delete whose object is a document, not a tombstone, is refused too.
        fx.bake_patch(2, NEXT_SNAPSHOT, &[PNU_B], &[]).await?;
        let files = fx.change_set("b2", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[], 0, &[PNU_B])?;
        let error = fx
            .publish(fx.patch_input(Some(2), NEXT_SNAPSHOT, files), false)
            .await
            .expect_err("a document stood in for a tombstone");
        assert!(
            error.to_string().contains("should be a tombstone"),
            "{error:#}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn gate_c_an_object_outside_the_change_set_is_refused() -> anyhow::Result<()> {
    for lane in LANES {
        let fx = fixture(lane, "gate-c").await?;
        fx.base(&[PNU_A, PNU_B]).await?;
        // The patch holds A and B; the change set names only A.
        fx.bake_patch(1, NEXT_SNAPSHOT, &[PNU_A, PNU_B], &[])
            .await?;
        let files = fx.change_set("c", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
        let error = fx
            .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
            .await
            .expect_err("a patch with a foreign object was published");
        assert!(
            error.to_string().contains("outside its change set"),
            "{error:#}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_change_set_against_another_snapshot_than_the_reflected_one_is_refused(
) -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Parcel, "baseline").await?;
    fx.base(&[PNU_A]).await?;
    fx.bake_patch(1, LATER_SNAPSHOT, &[PNU_A], &[]).await?;
    let files = fx.change_set("x", NEXT_SNAPSHOT, LATER_SNAPSHOT, &[PNU_A], 0, &[])?;
    let error = fx
        .publish(fx.patch_input(Some(1), LATER_SNAPSHOT, files), false)
        .await
        .expect_err("a change set of the wrong baseline was published");
    assert!(error.to_string().contains("manifest reflects"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn an_empty_change_set_advances_only_the_reflected_snapshot() -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Building, "empty").await?;
    let base = fx.base(&[PNU_A]).await?;
    let files = fx.change_set("e", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[], 0, &[])?;
    let manifest = fx
        .publish(fx.patch_input(None, NEXT_SNAPSHOT, files), false)
        .await?;
    assert!(manifest.patches.is_empty());
    assert_eq!(manifest.reflected_gold_iceberg_snapshot_id, NEXT_SNAPSHOT);
    assert_eq!(manifest.object_count, base.object_count);
    // And the next change set is computed against it.
    fx.bake_patch(1, LATER_SNAPSHOT, &[PNU_A], &[]).await?;
    let files = fx.change_set("e2", NEXT_SNAPSHOT, LATER_SNAPSHOT, &[PNU_A], 0, &[])?;
    fx.publish(fx.patch_input(Some(1), LATER_SNAPSHOT, files), false)
        .await?;
    Ok(())
}

#[tokio::test]
async fn the_patch_bounds_of_the_contract_are_enforced_at_publish() -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Parcel, "bounds").await?;
    // 20 base objects: one change is 5%, the contract's bound; two are over it.
    let pnus = (0..20)
        .map(|n| format!("99999000{n:02}100000000"))
        .collect::<Vec<_>>();
    let refs = pnus.iter().map(String::as_str).collect::<Vec<_>>();
    fx.base_exact(&refs).await?;
    fx.bake_patch(1, NEXT_SNAPSHOT, &refs[..2], &[]).await?;
    let files = fx.change_set("r", BASE_SNAPSHOT, NEXT_SNAPSHOT, &refs[..2], 0, &[])?;
    let error = fx
        .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
        .await
        .expect_err("a patch over the cumulative ratio was published");
    assert!(
        error.to_string().contains("max_cumulative_change_ratio"),
        "{error:#}"
    );
    Ok(())
}

#[tokio::test]
async fn a_patch_number_must_move_past_the_newest_and_stop_at_max_patches() -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Parcel, "count").await?;
    let pnus = (0..200)
        .map(|n| format!("99999{n:05}100000000"))
        .collect::<Vec<_>>();
    let refs = pnus.iter().map(String::as_str).collect::<Vec<_>>();
    fx.base_exact(&refs).await?;
    let snapshots = (2..=9)
        .map(|n| format!("99999000000000000{n}"))
        .collect::<Vec<_>>();
    let mut reflected = BASE_SNAPSHOT.to_owned();
    for (index, snapshot) in snapshots.iter().enumerate() {
        let patch = u64::try_from(index)? + 1;
        let pnu = [refs[index]];
        fx.bake_patch(patch, snapshot, &pnu, &[]).await?;
        let files = fx.change_set(&format!("k{patch}"), &reflected, snapshot, &pnu, 0, &[])?;
        let result = fx
            .publish(fx.patch_input(Some(patch), snapshot, files), false)
            .await;
        if patch <= 7 {
            result?;
            reflected.clone_from(snapshot);
        } else {
            let error = result.expect_err("an eighth patch was published");
            assert!(error.to_string().contains("max_patches"), "{error:#}");
        }
    }
    // A patch number at or below the newest is refused even inside the bound.
    let fx = fixture(ByPnuLane::Parcel, "renumber").await?;
    fx.base_exact(&refs).await?;
    fx.bake_patch(4, NEXT_SNAPSHOT, &[PNU_A], &[]).await?;
    let files = fx.change_set("n1", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
    fx.publish(fx.patch_input(Some(4), NEXT_SNAPSHOT, files), false)
        .await?;
    let files = fx.change_set("n2", NEXT_SNAPSHOT, LATER_SNAPSHOT, &[PNU_A], 0, &[])?;
    assert!(fx
        .publish(fx.patch_input(Some(3), LATER_SNAPSHOT, files), false)
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn a_schema_change_cannot_be_patched() -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Parcel, "schema").await?;
    // A base published as v1 whose objects carry another document schema.
    fx.put(
        &by_pnu::object_key(fx.lane, 3, PNU_A)?,
        format!(
            "{{\"schema_version\":\"older.v0\",\"pnu\":\"{PNU_A}\",\"source\":{{\"iceberg_snapshot_id\":\"{BASE_SNAPSHOT}\"}}}}\n"
        )
        .as_bytes(),
    )
    .await?;
    write_v1_manifest(&fx, 1)?;
    fx.bake_patch(1, NEXT_SNAPSHOT, &[PNU_A], &[]).await?;
    let files = fx.change_set("s", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
    let error = fx
        .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
        .await
        .expect_err("a schema change went out as a patch");
    assert!(
        error.to_string().contains("schema change needs"),
        "{error:#}"
    );
    Ok(())
}

fn write_v1_manifest(fx: &Fixture, object_count: u64) -> anyhow::Result<()> {
    let key = by_pnu::manifest_key(fx.lane)?;
    let path = fx.root.join(key);
    std::fs::create_dir_all(path.parent().unwrap_or(&fx.root))?;
    std::fs::write(
        path,
        format!(
            "{{\"schema_version\":1,\"unit\":\"{}\",\"current_generation\":3,\"gold_table\":\"{}\",\
             \"gold_iceberg_snapshot_id\":\"{BASE_SNAPSHOT}\",\"object_count\":{object_count},\
             \"published_at_utc\":\"2026-01-01T00:00:00Z\"}}\n",
            fx.lane.unit(),
            gold_table(fx.lane)
        ),
    )?;
    Ok(())
}

#[tokio::test]
async fn a_v1_manifest_is_patched_once_the_gateway_reads_v2() -> anyhow::Result<()> {
    for lane in LANES {
        let fx = fixture(lane, "v1").await?;
        fx.put(
            &by_pnu::object_key(lane, 3, PNU_A)?,
            &fx.document(PNU_A, BASE_SNAPSHOT),
        )
        .await?;
        write_v1_manifest(&fx, 100)?;
        fx.bake_patch(1, NEXT_SNAPSHOT, &[PNU_A], &[]).await?;

        // An old gateway (no capabilities path, or v1 only) keeps the v1 manifest.
        for versions in [None, Some(vec![1])] {
            fx.gateway.reset().await;
            if let Some(versions) = versions {
                serve_capabilities(&fx.gateway, lane, &versions).await;
            }
            let files = fx.change_set("v", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
            let error = fx
                .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
                .await
                .expect_err("a v2 manifest went out to a gateway that cannot read it");
            assert!(
                error.to_string().contains("deploy the gateway first"),
                "{error:#}"
            );
            assert_eq!(fx.live().await?.wire_schema_version, 1);
        }

        serve_capabilities(&fx.gateway, lane, &[1, 2]).await;
        let files = fx.change_set("v", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
        let manifest = fx
            .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
            .await?;
        assert_eq!(manifest.schema_version, 2);
        assert_eq!(
            manifest.document_schema_version,
            document_schema_version(lane)
        );
        assert_eq!(manifest.patches.len(), 1);
        // The v1 manifest is kept in the history, byte for byte.
        let [history] = fx
            .history()?
            .try_into()
            .map_err(|_| anyhow::anyhow!("one history key"))?;
        assert!(String::from_utf8(fx.store.read_bytes(&history).await?)?
            .contains("\"schema_version\":1"));
    }
    Ok(())
}

#[tokio::test]
async fn a_rollback_drops_the_newest_patches_and_nothing_else() -> anyhow::Result<()> {
    for lane in LANES {
        let fx = fixture(lane, "rollback").await?;
        fx.base(&[PNU_A, PNU_B]).await?;
        fx.bake_patch(1, NEXT_SNAPSHOT, &[PNU_A], &[]).await?;
        let files = fx.change_set("r1", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
        let one = fx
            .publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
            .await?;
        fx.bake_patch(2, LATER_SNAPSHOT, &[], &[PNU_B]).await?;
        let files = fx.change_set("r2", NEXT_SNAPSHOT, LATER_SNAPSHOT, &[], 0, &[PNU_B])?;
        fx.publish(fx.patch_input(Some(2), LATER_SNAPSHOT, files), false)
            .await?;

        // history: [base manifest, patch-1 manifest]
        let history = fx.history()?;
        let patch_one = history
            .iter()
            .find(|key| {
                std::fs::read(fx.root.join(key)).is_ok_and(|bytes| {
                    ServedManifest::parse(lane, &bytes).is_ok_and(|m| m.patches.len() == 1)
                })
            })
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("the patch-1 manifest is not in the history"))?;
        let rolled = fx.publish(PublishInput::Rollback(patch_one), false).await?;
        assert_eq!(rolled.patches, one.patches);
        assert_eq!(rolled.reflected_gold_iceberg_snapshot_id, NEXT_SNAPSHOT);
        assert_eq!(rolled.object_count, one.object_count);

        // Rolling "back" to the live list, or to a list that is not its suffix, is refused.
        let live_history = fx.history()?;
        let two_patch = live_history
            .iter()
            .find(|key| {
                std::fs::read(fx.root.join(key)).is_ok_and(|bytes| {
                    ServedManifest::parse(lane, &bytes).is_ok_and(|m| m.patches.len() == 2)
                })
            })
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("the patch-2 manifest is not in the history"))?;
        assert!(fx
            .publish(PublishInput::Rollback(two_patch), false)
            .await
            .is_err());
        assert!(fx
            .publish(
                PublishInput::Rollback(by_pnu::object_key(lane, 3, PNU_A)?),
                false
            )
            .await
            .is_err());
    }
    Ok(())
}

#[tokio::test]
async fn a_full_bake_moves_the_base_forward_and_resets_the_patches() -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Parcel, "full").await?;
    fx.base(&[PNU_A]).await?;
    fx.bake_patch(1, NEXT_SNAPSHOT, &[PNU_A], &[]).await?;
    let files = fx.change_set("f", BASE_SNAPSHOT, NEXT_SNAPSHOT, &[PNU_A], 0, &[])?;
    fx.publish(fx.patch_input(Some(1), NEXT_SNAPSHOT, files), false)
        .await?;

    fx.put(
        &by_pnu::object_key(fx.lane, 2, PNU_A)?,
        &fx.document(PNU_A, BASE_SNAPSHOT),
    )
    .await?;
    for (generation, count) in [(3, 1 + FILLER), (2, 1)] {
        let stand_still = fx
            .publish(
                PublishInput::Listing(ListingExpectation {
                    target_generation: generation,
                    expected_gold_iceberg_snapshot_id: BASE_SNAPSHOT.to_owned(),
                    expected_object_count: u64::try_from(count)?,
                }),
                false,
            )
            .await
            .expect_err("the base generation did not move forward");
        assert!(
            stand_still.to_string().contains("only move forward"),
            "{stand_still:#}"
        );
    }
    fx.put(
        &by_pnu::object_key(fx.lane, 4, PNU_A)?,
        &fx.document(PNU_A, LATER_SNAPSHOT),
    )
    .await?;
    let compacted = fx
        .publish(
            PublishInput::Listing(ListingExpectation {
                target_generation: 4,
                expected_gold_iceberg_snapshot_id: LATER_SNAPSHOT.to_owned(),
                expected_object_count: 1,
            }),
            false,
        )
        .await?;
    assert_eq!(compacted.base_generation, 4);
    assert!(compacted.patches.is_empty());
    assert_eq!(compacted.reflected_gold_iceberg_snapshot_id, LATER_SNAPSHOT);
    Ok(())
}

#[tokio::test]
async fn a_listing_that_disagrees_with_the_stated_count_or_snapshot_is_refused(
) -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Building, "listing").await?;
    for pnu in [PNU_A, PNU_B] {
        fx.put(
            &by_pnu::object_key(fx.lane, 3, pnu)?,
            &fx.document(pnu, BASE_SNAPSHOT),
        )
        .await?;
    }
    let short = fx
        .publish(
            PublishInput::Listing(ListingExpectation {
                target_generation: 3,
                expected_gold_iceberg_snapshot_id: BASE_SNAPSHOT.to_owned(),
                expected_object_count: 3,
            }),
            true,
        )
        .await
        .expect_err("a short listing was published");
    assert!(short.to_string().contains("lists 2"), "{short:#}");
    let stale = fx
        .publish(
            PublishInput::Listing(ListingExpectation {
                target_generation: 3,
                expected_gold_iceberg_snapshot_id: NEXT_SNAPSHOT.to_owned(),
                expected_object_count: 2,
            }),
            true,
        )
        .await
        .expect_err("documents of another snapshot were published");
    assert!(
        stale.to_string().contains("baked from gold snapshot"),
        "{stale:#}"
    );
    Ok(())
}

#[tokio::test]
async fn first_publication_must_be_stated_and_cannot_shadow_an_existing_manifest(
) -> anyhow::Result<()> {
    let fx = fixture(ByPnuLane::Parcel, "first").await?;
    fx.put(
        &by_pnu::object_key(fx.lane, 3, PNU_A)?,
        &fx.document(PNU_A, BASE_SNAPSHOT),
    )
    .await?;
    let listing = || {
        PublishInput::Listing(ListingExpectation {
            target_generation: 3,
            expected_gold_iceberg_snapshot_id: BASE_SNAPSHOT.to_owned(),
            expected_object_count: 1,
        })
    };
    assert!(
        fx.publish(listing(), false).await.is_err(),
        "unstated first publication"
    );
    fx.publish(listing(), true).await?;
    assert!(
        fx.publish(listing(), true).await.is_err(),
        "first publication over a manifest"
    );
    Ok(())
}

#[test]
fn the_removed_overwrite_and_repoint_switches_are_refused_when_set() {
    for lane in LANES {
        for name in ["ALLOW_REPOINT", "ALLOW_OVERWRITE"] {
            let variable = lane.env(name);
            let refused = crate::by_pnu_serving_store::refuse_removed_switches_in(
                lane,
                &["ALLOW_REPOINT", "ALLOW_OVERWRITE"],
                |candidate| candidate == variable,
            );
            assert!(refused.is_err(), "{variable} set was not refused");
        }
        assert!(crate::by_pnu_serving_store::refuse_removed_switches_in(
            lane,
            &["ALLOW_REPOINT", "ALLOW_OVERWRITE"],
            |_| false
        )
        .is_ok());
    }
}
