//! Reports what a by-PNU serving lane has to do (root ADR-0122 scheduled jobs, ADR-0096, ADR-0100).
//!
//! The scheduled bake (`scripts/ops/by-pnu-serving-bake.sh`) asks one question before it scans
//! anything: is the Gold table's current snapshot the one the published manifest already serves?
//! The manifest in the bucket is the record (root ADR-0062), so the answer is read from there and
//! from the Iceberg catalog, never from a file the job keeps beside them.
//!
//! Read-only: it writes nothing to the bucket or the catalog. It writes one small JSON file at
//! `FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH`. A manifest that cannot be read is an error,
//! not "nothing published": the very first publication of a lane stays an explicit operator step
//! (`..._FIRST_PUBLICATION=true` on the publish command), never something a schedule infers.

use std::path::PathBuf;

use anyhow::{bail, Context};
use lakehouse_domain::{GOLD_BUILDING_PANEL, GOLD_PARCEL_PANEL};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Serialize;

use crate::building_by_pnu_serving_manifest_publish::BuildingServingManifest;
use crate::building_by_pnu_serving_store::{self, BuildingServingObjectStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::parcel_by_pnu_serving_manifest_publish::ParcelServingManifest;
use crate::parcel_by_pnu_serving_store::{self, ParcelServingObjectStore};
use crate::public_data_control_support::{optional_env_value, required_env_value};
use crate::r2_layout::{building_by_pnu_serving_manifest_key, parcel_by_pnu_serving_manifest_key};

const STATE_SCHEMA_VERSION: &str = "foundation-platform.by_pnu_serving_state.v1";
const STATE_PATH_ENV: &str = "FOUNDATION_PLATFORM_BY_PNU_SERVING_STATE_PATH";

/// What the bake needs to decide whether to run and which generation to write.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct LaneState {
    schema_version: &'static str,
    unit: &'static str,
    gold_table: &'static str,
    /// `None` when the Gold table has no snapshot yet; the bake then has nothing to do.
    gold_iceberg_snapshot_id: Option<String>,
    published: PublishedGeneration,
}

/// The served generation, as the manifest states it.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct PublishedGeneration {
    current_generation: u64,
    gold_iceberg_snapshot_id: String,
    object_count: u64,
}

/// `show-parcel-by-pnu-serving-state`.
///
/// # Errors
/// Refuses when the manifest or the catalog cannot be read, or the state file cannot be written.
pub async fn run_parcel() -> anyhow::Result<()> {
    let output = ProfileStoreConfig::parse(
        &output_driver("FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER")?,
        parcel_by_pnu_serving_store::local_root(optional_env_value(
            "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_ROOT",
        )?),
    )?;
    let store = ParcelServingObjectStore::open(&output)?;
    let raw = store
        .read_bytes(parcel_by_pnu_serving_manifest_key()?)
        .await
        .context("the parcel by-PNU serving manifest could not be read")?;
    let manifest: ParcelServingManifest = serde_json::from_slice(&raw)
        .context("the parcel by-PNU serving manifest does not parse")?;
    let published = PublishedGeneration {
        current_generation: manifest.current_generation,
        gold_iceberg_snapshot_id: manifest.gold_iceberg_snapshot_id,
        object_count: manifest.object_count,
    };
    write_state(&lane_state(
        "parcel-by-pnu",
        GOLD_PARCEL_PANEL.table_name,
        current_snapshot(GOLD_PARCEL_PANEL.table_name).await?,
        published,
    ))
}

/// `show-building-by-pnu-serving-state`.
///
/// # Errors
/// Refuses when the manifest or the catalog cannot be read, or the state file cannot be written.
pub async fn run_building() -> anyhow::Result<()> {
    let output = ProfileStoreConfig::parse(
        &output_driver("FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER")?,
        building_by_pnu_serving_store::local_root(optional_env_value(
            "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_OUTPUT_ROOT",
        )?),
    )?;
    let store = BuildingServingObjectStore::open(&output)?;
    let raw = store
        .read_bytes(building_by_pnu_serving_manifest_key()?)
        .await
        .context("the building by-PNU serving manifest could not be read")?;
    let manifest: BuildingServingManifest = serde_json::from_slice(&raw)
        .context("the building by-PNU serving manifest does not parse")?;
    let published = PublishedGeneration {
        current_generation: manifest.current_generation,
        gold_iceberg_snapshot_id: manifest.gold_iceberg_snapshot_id,
        object_count: manifest.object_count,
    };
    write_state(&lane_state(
        "building-by-pnu",
        GOLD_BUILDING_PANEL.table_name,
        current_snapshot(GOLD_BUILDING_PANEL.table_name).await?,
        published,
    ))
}

fn output_driver(name: &str) -> anyhow::Result<String> {
    Ok(optional_env_value(name)?.unwrap_or_else(|| "local".to_owned()))
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

#[must_use]
pub(crate) fn lane_state(
    unit: &'static str,
    gold_table: &'static str,
    gold_iceberg_snapshot_id: Option<String>,
    published: PublishedGeneration,
) -> LaneState {
    LaneState {
        schema_version: STATE_SCHEMA_VERSION,
        unit,
        gold_table,
        gold_iceberg_snapshot_id,
        published,
    }
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
        published_generation = state.published.current_generation,
        published_gold_iceberg_snapshot_id = %state.published.gold_iceberg_snapshot_id,
        "by-PNU serving lane state"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_state_names_both_sides_of_the_comparison() -> anyhow::Result<()> {
        let state = lane_state(
            "parcel-by-pnu",
            "gold.parcel_panel",
            Some("2".to_owned()),
            PublishedGeneration {
                current_generation: 3,
                gold_iceberg_snapshot_id: "1".to_owned(),
                object_count: 7,
            },
        );
        let value = serde_json::to_value(&state)?;
        assert_eq!(value["schema_version"], STATE_SCHEMA_VERSION);
        assert_eq!(value["gold_iceberg_snapshot_id"], "2");
        assert_eq!(value["published"]["current_generation"], 3);
        assert_eq!(value["published"]["gold_iceberg_snapshot_id"], "1");
        assert_eq!(value["published"]["object_count"], 7);
        Ok(())
    }
}
