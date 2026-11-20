//! `run-parcel-matching-gate`: the parcel matching gate (root ADR-0113 §7) over one cadastral
//! edition before its parcels are baked, in `DuckDB` inside the publisher
//! (`foundation_publisher_silver::parcel_matching_gate_stage`). It was the Spark job
//! `parcel_matching_gate.py`; the verdict file, its last stdout line and the exit codes are that
//! job's, so the parcel tile bake reads it unchanged (`parcel-tile-bake.sh`).
//!
//! Every input is read at its head when the run starts. The edition's registry is read as of the
//! edition: its rows and every earlier edition's (root ADR-0490 §4a). Exit 0: the gate passed; 1: it
//! refused (the verdict says why) or failed (no verdict); 3: `silver.parcel_registry` does not exist
//! yet and the bake waits (root ADR-0265).

use std::path::Path;

use anyhow::{bail, ensure, Context};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG},
    parcel_matching_gate_stage::{
        verdict, verdict_file_bytes, waiting_verdict, GateRun, GateSources, Registry, ADMIN_TABLE,
        CODE_CHANGE_TABLE, CODE_LIST_TABLE, INPUT_ABSENT, PARCEL_TABLE, REGISTRY_TABLE,
    },
    vworld_parcel_source_contract,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::Value;

use crate::public_data_control_support::required_env_value;

const SNAPSHOT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MATCHING_GATE_SNAPSHOT_ID";
const SIDO_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MATCHING_GATE_SIDO";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MATCHING_GATE_SUMMARY";
/// `1`: the snapshot may be an edition the contract does not serve yet (a rehearsal, ADR-0490 §4).
const ALLOW_OTHER_EDITION_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MATCHING_GATE_ALLOW_NON_SERVED_EDITION";
/// `1`: the region has no registry yet; check (b) is skipped and the verdict binds no bake.
const NO_REGISTRY_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MATCHING_GATE_NO_REGISTRY";
const PARCEL_SOURCE_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");
/// The share of the memory left that the buffer manager takes; the rest is `DuckDB`'s own
/// structures, inside the bake unit's cap.
const MEMORY_SHARE: (u64, u64) = (3, 5);

fn flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| value == "1")
}

/// The edition's snapshot id, and the snapshot ids of it and every earlier edition (oldest first),
/// as `vworld_parcel_editions.served_reader_id` and `snapshot_ids_through` give them.
fn edition(given: Option<&str>, allow_other: bool) -> anyhow::Result<(String, Vec<String>)> {
    let contract: Value = serde_json::from_str(PARCEL_SOURCE_CONTRACT)?;
    let prefix = contract["snapshot_id_prefix"]
        .as_str()
        .context("the parcel source contract names no snapshot_id_prefix")?;
    let served = vworld_parcel_source_contract::served_edition_view(PARCEL_SOURCE_CONTRACT)?;
    let served = format!(
        "{prefix}{}",
        served["edition"]
            .as_str()
            .context("the served edition view names no edition")?
    );
    let snapshot = match given {
        Some(value) if !allow_other && value != served => bail!(
            "{SNAPSHOT_ENV} {value:?} is not the served edition's {served:?}; set \
             {ALLOW_OTHER_EDITION_ENV}=1 to check another edition"
        ),
        Some(value) => value.to_owned(),
        None => served,
    };
    let names: Vec<&String> = contract["editions"]
        .as_object()
        .context("the parcel source contract holds no editions")?
        .keys()
        .collect();
    let name = snapshot
        .strip_prefix(prefix)
        .filter(|name| names.iter().any(|n| n.as_str() == *name))
        .with_context(|| format!("{snapshot:?} is not the snapshot id of an edition"))?;
    let mut through: Vec<String> = names
        .into_iter()
        .filter(|n| n.as_str() <= name)
        .map(|n| format!("{prefix}{n}"))
        .collect();
    through.sort();
    Ok((snapshot, through))
}

async fn head_of(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<Option<i64>> {
    let Some(history) = catalog.load_main_snapshot_history(table).await? else {
        return Ok(None);
    };
    Ok(history.first().map(|snapshot| snapshot.snapshot_id))
}

async fn pinned(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<String> {
    let head = head_of(catalog, table)
        .await?
        .with_context(|| format!("{table} does not exist or holds no snapshot"))?;
    Ok(format!(
        "{LAKEHOUSE_CATALOG}.{table} AT (VERSION => {head})"
    ))
}

fn write(path: Option<&str>, verdict: &Value) -> anyhow::Result<()> {
    if let Some(path) = path {
        std::fs::write(path, verdict_file_bytes(verdict)?)
            .with_context(|| format!("cannot write {path}"))?;
    }
    println!(
        "parcel-matching-gate-json {}",
        serde_json::to_string(verdict)?
    );
    Ok(())
}

/// Runs the gate and writes its verdict.
pub(crate) async fn run() -> anyhow::Result<()> {
    let sido = required_env_value(SIDO_ENV)?;
    let summary = std::env::var(SUMMARY_ENV)
        .ok()
        .filter(|path| !path.is_empty());
    let given = std::env::var(SNAPSHOT_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let (snapshot, through) = edition(given.as_deref(), flag(ALLOW_OTHER_EDITION_ENV))?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let registry = if flag(NO_REGISTRY_ENV) {
        Registry::Skipped
    } else if head_of(&catalog, REGISTRY_TABLE).await?.is_some() {
        Registry::Checked
    } else {
        Registry::Wait
    };
    if registry == Registry::Wait {
        write(summary.as_deref(), &waiting_verdict(&snapshot, &sido))?;
        std::process::exit(INPUT_ABSENT);
    }
    let sources = GateSources {
        parcels: pinned(&catalog, PARCEL_TABLE).await?,
        code_list: pinned(&catalog, CODE_LIST_TABLE).await?,
        code_changes: pinned(&catalog, CODE_CHANGE_TABLE).await?,
        admin: pinned(&catalog, ADMIN_TABLE).await?,
        registry: if registry == Registry::Checked {
            Some(pinned(&catalog, REGISTRY_TABLE).await?)
        } else {
            None
        },
    };
    let work = summary
        .as_deref()
        .and_then(|path| Path::new(path).parent())
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
    stage.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    let made = verdict(
        &stage,
        &sources,
        &GateRun {
            snapshot_id: &snapshot,
            sido: &sido,
            registry_through: &through,
            registry,
        },
    )?;
    write(summary.as_deref(), &made)?;
    ensure!(
        made["passed"] == true,
        "the matching gate refused {snapshot} (verdict above)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_edition_is_the_served_one_unless_another_is_allowed_and_reads_the_registry_through_it(
    ) -> anyhow::Result<()> {
        let contract: Value = serde_json::from_str(PARCEL_SOURCE_CONTRACT)?;
        let prefix = contract["snapshot_id_prefix"].as_str().unwrap_or_default();
        let mut names: Vec<&String> = contract["editions"]
            .as_object()
            .map(|editions| editions.keys().collect())
            .unwrap_or_default();
        names.sort();
        let (served, through) = edition(None, false)?;
        assert!(
            through.last() == Some(&served),
            "{through:?} ends at {served}"
        );
        let first = format!("{prefix}{}", names[0]);
        assert!(edition(Some(&first), false).is_err() || first == served);
        let (snapshot, through) = edition(Some(&first), true)?;
        assert_eq!((snapshot, through), (first.clone(), vec![first]));
        assert!(edition(Some(&format!("{prefix}209912")), true).is_err());
        assert!(edition(Some("not-an-edition"), true).is_err());
        Ok(())
    }
}
