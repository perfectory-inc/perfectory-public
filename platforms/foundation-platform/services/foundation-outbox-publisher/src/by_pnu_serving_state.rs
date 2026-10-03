//! Reports what a by-PNU serving lane has to do (root ADR-0122 scheduled jobs, ADR-0141).
//!
//! The scheduled bake (`scripts/ops/by-pnu-serving-bake.sh`) asks one question before it scans
//! anything: has the published state already reflected the Gold table's current snapshot? The
//! manifest in the bucket is the record (root ADR-0062), so the answer is read from there and
//! from the Iceberg catalog, never from a file the job keeps beside them.
//!
//! It also reports what the bake needs to choose between a patch and a full bake (root ADR-0141
//! §5, §8): the patches the base carries, the changes they hold, the document schema the base
//! was baked with against the one the export bakes now, and the contract's bounds — the bake
//! reads the bounds from here, so the contract is read in one place. And it names every
//! generation and every patch of the base that holds any object, published or not: new ones are
//! numbered above all of them.
//!
//! Read-only: it writes one small JSON file at `FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH`.
//! A manifest that cannot be read is an error, not "nothing published": the first publication
//! of a lane stays an explicit operator step.

use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{bail, Context};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Serialize;

use crate::by_pnu_gateway_contract::{by_pnu_serving_patch_policy, ByPnuLane};
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_manifest_publish::{document_schema_version, gold_table};
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::public_data_control_support::{optional_env_value, required_env_value};
use crate::r2_layout::by_pnu;

const STATE_SCHEMA_VERSION: &str = "foundation-platform.by_pnu_serving_state.v2";
const STATE_PATH_ENV: &str = "FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH";

/// What the bake needs to decide whether to run, and how.
#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct LaneState {
    schema_version: &'static str,
    unit: &'static str,
    gold_table: &'static str,
    /// `None` when the Gold table has no snapshot yet; the bake then has nothing to do.
    gold_iceberg_snapshot_id: Option<String>,
    /// The document schema the export bakes now.
    document_schema_version: &'static str,
    published: PublishedState,
    /// Every generation with at least one object, ascending; includes the published base.
    generations_with_objects: Vec<u64>,
    /// Every patch of the published base with at least one object, ascending.
    patches_with_objects: Vec<u64>,
    policy: PatchBounds,
}

/// The served state, as the manifest states it.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct PublishedState {
    manifest_schema_version: u32,
    base_generation: u64,
    base_object_count: u64,
    /// The base's document schema; read from its first object when a v1 manifest did not say.
    /// `None` only when the base holds no object at all.
    document_schema_version: Option<String>,
    gold_iceberg_snapshot_id: String,
    reflected_gold_iceberg_snapshot_id: String,
    patch_count: usize,
    newest_patch: u64,
    cumulative_changes: u64,
    object_count: u64,
}

#[derive(Debug, Serialize, PartialEq)]
struct PatchBounds {
    max_patches: usize,
    max_cumulative_change_ratio: f64,
    max_delta_fraction: f64,
}

/// `show-parcel-by-pnu-serving-state`.
///
/// # Errors
/// Refuses when the manifest or the catalog cannot be read, or the state file cannot be written.
pub async fn run_parcel() -> anyhow::Result<()> {
    run(ByPnuLane::Parcel).await
}

/// `show-building-by-pnu-serving-state`.
///
/// # Errors
/// Refuses when the manifest or the catalog cannot be read, or the state file cannot be written.
pub async fn run_building() -> anyhow::Result<()> {
    run(ByPnuLane::Building).await
}

async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    let output = ProfileStoreConfig::parse(
        &optional_env_value(&lane.env("OUTPUT_STORAGE_DRIVER"))?
            .unwrap_or_else(|| "local".to_owned()),
        local_root(optional_env_value(&lane.env("OUTPUT_ROOT"))?),
    )?;
    let store = ByPnuServingStore::open(lane, &output)?;
    let state = read_state(&store, current_snapshot(gold_table(lane)).await?).await?;
    write_state(&state)
}

/// The lane's state from its store and the Gold table's current snapshot.
///
/// # Errors
/// Refuses when the manifest cannot be read or a listing fails.
pub(crate) async fn read_state(
    store: &ByPnuServingStore,
    gold_iceberg_snapshot_id: Option<String>,
) -> anyhow::Result<LaneState> {
    let lane = store.lane();
    let raw = store
        .read_bytes(by_pnu::manifest_key(lane)?)
        .await
        .with_context(|| format!("the {} serving manifest could not be read", lane.unit()))?;
    let manifest = ServedManifest::parse(lane, &raw)
        .with_context(|| format!("the {} serving manifest does not parse", lane.unit()))?;
    let served_schema = match &manifest.document_schema_version {
        Some(schema) => Some(schema.clone()),
        None => match store.first_object_key(manifest.base_generation).await? {
            Some(key) => {
                serde_json::from_slice::<serde_json::Value>(&store.read_bytes(&key).await?)
                    .ok()
                    .and_then(|document| {
                        document
                            .get("schema_version")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                    })
            }
            None => None,
        },
    };
    let policy = by_pnu_serving_patch_policy()?;
    Ok(LaneState {
        schema_version: STATE_SCHEMA_VERSION,
        unit: lane.unit(),
        gold_table: gold_table(lane),
        gold_iceberg_snapshot_id,
        document_schema_version: document_schema_version(lane),
        published: PublishedState {
            manifest_schema_version: manifest.wire_schema_version,
            base_generation: manifest.base_generation,
            base_object_count: manifest.base_object_count,
            document_schema_version: served_schema,
            gold_iceberg_snapshot_id: manifest.gold_iceberg_snapshot_id.clone(),
            reflected_gold_iceberg_snapshot_id: manifest.reflected_gold_iceberg_snapshot_id.clone(),
            patch_count: manifest.patches.len(),
            newest_patch: manifest.newest_patch(),
            cumulative_changes: manifest.cumulative_changes(),
            object_count: manifest.object_count,
        },
        generations_with_objects: ascending(store.list_generations_with_objects().await?),
        patches_with_objects: ascending(
            store
                .list_patches_with_objects(manifest.base_generation)
                .await?,
        ),
        policy: PatchBounds {
            max_patches: policy.max_patches,
            max_cumulative_change_ratio: policy.max_cumulative_change_ratio,
            max_delta_fraction: policy.max_delta_fraction,
        },
    })
}

fn ascending(set: BTreeSet<u64>) -> Vec<u64> {
    set.into_iter().collect()
}

async fn current_snapshot(table: &str) -> anyhow::Result<Option<String>> {
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    Ok(catalog
        .load_current_snapshot_manifest_list(table)
        .await
        .with_context(|| format!("failed to resolve the {table} snapshot"))?
        .map(|snapshot| snapshot.snapshot_id.to_string()))
}

fn write_state(state: &LaneState) -> anyhow::Result<()> {
    let path = PathBuf::from(required_env_value(STATE_PATH_ENV)?);
    if !path.is_absolute() {
        bail!("{STATE_PATH_ENV} must be an absolute path");
    }
    let body = serde_json::to_vec_pretty(state).context("serialize the lane state")?;
    std::fs::write(&path, body).with_context(|| format!("failed to write {}", path.display()))?;
    tracing::info!(
        unit = state.unit,
        gold_iceberg_snapshot_id = state.gold_iceberg_snapshot_id.as_deref().unwrap_or("(none)"),
        base_generation = state.published.base_generation,
        patches = state.published.patch_count,
        reflected_gold_iceberg_snapshot_id = %state.published.reflected_gold_iceberg_snapshot_id,
        highest_generation_with_objects = state.generations_with_objects.last().copied().unwrap_or(0),
        "by-PNU serving lane state"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PNU: &str = "9999900000100000000";

    fn temporary_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "foundation-platform-by-pnu-state-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    #[tokio::test]
    async fn a_v1_manifest_reports_its_base_schema_from_the_first_object() -> anyhow::Result<()> {
        let lane = ByPnuLane::Building;
        let root = temporary_root("v1");
        let store =
            ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
        let manifest = "{\"schema_version\":1,\"unit\":\"building-by-pnu\",\"current_generation\":3,\
             \"gold_table\":\"gold.building_panel\",\"gold_iceberg_snapshot_id\":\"999990000000000001\",\
             \"object_count\":1,\"published_at_utc\":\"2026-01-01T00:00:00Z\"}";
        let checksum = "a".repeat(64);
        store
            .write_manifest(by_pnu::manifest_key(lane)?, manifest.as_bytes(), &checksum)
            .await?;
        store
            .write_object_create_only(
                &by_pnu::object_key(lane, 3, PNU)?,
                b"{\"schema_version\":\"older.v0\"}\n",
                &checksum,
            )
            .await?;
        store
            .write_object_create_only(
                &by_pnu::patch_object_key(lane, 3, 2, PNU)?,
                b"{}\n",
                &checksum,
            )
            .await?;

        let state = read_state(&store, Some("999990000000000002".to_owned())).await?;
        std::fs::remove_dir_all(&root)?;
        let value = serde_json::to_value(&state)?;
        assert_eq!(value["schema_version"], STATE_SCHEMA_VERSION);
        assert_eq!(value["published"]["manifest_schema_version"], 1);
        assert_eq!(value["published"]["base_generation"], 3);
        assert_eq!(value["published"]["document_schema_version"], "older.v0");
        assert_eq!(
            value["published"]["reflected_gold_iceberg_snapshot_id"],
            "999990000000000001"
        );
        assert_eq!(value["published"]["patch_count"], 0);
        assert_eq!(value["generations_with_objects"], serde_json::json!([3]));
        assert_eq!(value["patches_with_objects"], serde_json::json!([2]));
        assert_eq!(
            value["policy"]["max_patches"],
            by_pnu_serving_patch_policy()?.max_patches
        );
        assert_eq!(
            value["document_schema_version"],
            document_schema_version(lane)
        );
        Ok(())
    }
}
