//! `append-map-edit-ledger`: the edit store's handed `parcels` edits appended to
//! `silver.map_edit_ledger`, once each (root ADR-0375). The ledger is append-only: an edit it holds
//! is not appended again, and one it holds with other content is refused. The served boundaries'
//! Gold reads the ledger at its pin (`kind_versions_build` `BOUNDARY_SERVED_BUILD`); this command is
//! its upstream step, which a rehearsal never runs (root ADR-0615 §7, ADR-0618 §9). It was the first
//! step of `build-parcel-boundary-served`.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter,
};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    parcel_boundary_served_stage::{
        check_ledger, fresh_edits, read_edit_handoff, read_ledger, LEDGER_SOURCE, UNIT,
    },
    parcel_nearby_stage::{NearbyPins, Source},
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    gold_versions_support::{contract, lakehouse_read},
    public_data_control_support::{optional_env_value, required_env_value},
};

/// The edit store's handoff of the parcels unit (`export-map-edit-handoff`).
const INPUT_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_LEDGER_INPUT";
/// Where the `DuckDB` stage lives (the unit's state directory); the input's directory when unset.
const STAGE_ROOT_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_LEDGER_STAGE_ROOT";

/// Appends the handoff's unledgered edits as one batch.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let input = PathBuf::from(required_env_value(INPUT_ENV)?);
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let head = catalog
        .load_main_snapshot_history(LEDGER_SOURCE)
        .await?
        .with_context(|| format!("{LEDGER_SOURCE} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id);
    let shape = committer.table_shape(LEDGER_SOURCE).await?;
    let already = catalog
        .load_ingested_batch_objects(LEDGER_SOURCE)
        .await?
        .unwrap_or_default();
    let text = std::fs::read_to_string(&input)
        .with_context(|| format!("cannot read the edit handoff {}", input.display()))?;
    let handoff = read_edit_handoff(&text)?;

    let work = optional_env_value(STAGE_ROOT_ENV)?
        .map(PathBuf::from)
        .or_else(|| input.parent().map(std::path::Path::to_path_buf))
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("."));
    let stage = DuckDbParquetStage::open_reading_lakehouse(&work, &lakehouse_read()?)?;
    let connection = stage.connection();
    // The ledger at its head: what it holds, checked as the build checks it.
    let (ledger, fingerprints) = match head {
        Some(head) => {
            let pins = NearbyPins(BTreeMap::from([(LEDGER_SOURCE.to_owned(), head)]));
            let source = Source::Pinned {
                catalog: LAKEHOUSE_CATALOG,
                pins: &pins,
            };
            read_ledger(connection, &source.of(LEDGER_SOURCE)?)?
        }
        None => (Vec::new(), BTreeMap::new()),
    };
    let held = ledger.len();
    let fresh = fresh_edits(&fingerprints, &handoff)?;
    let mut checked = ledger;
    for edit in &fresh {
        checked.push(edit.ledger_edit()?);
    }
    // The ledger with the fresh edits is one the served table can hold.
    check_ledger(checked)?;
    if fresh.is_empty() {
        println!(
            "map-edit-ledger-append {}",
            json!({"unit": UNIT, "ledgered": held, "in_handoff": handoff.len(), "appended": 0})
        );
        return Ok(());
    }
    let batch = format!("map-edit-export-{}", uuid::Uuid::new_v4());
    connection.execute_batch(
        "CREATE OR REPLACE TABLE ledger_append (unit VARCHAR, change_seq BIGINT, feature_id VARCHAR, \
           op VARCHAR, geometry_geojson VARCHAR, geometry_wkb BLOB, geometry_srid INTEGER, \
           geometry_checksum_sha256 VARCHAR, properties_json VARCHAR, editor VARCHAR, edited_at VARCHAR, \
           export_batch_id VARCHAR, ingested_at_utc TIMESTAMP);",
    )?;
    {
        let mut appender = connection.appender("ledger_append")?;
        for edit in &fresh {
            let ledgered = edit.ledger_edit()?;
            appender.append_row(foundation_publisher_silver::duckdb::params![
                UNIT,
                edit.change_seq,
                edit.feature_id,
                edit.op,
                edit.geometry_geojson,
                ledgered.geometry_wkb,
                edit.geometry_srid,
                edit.geometry_checksum_sha256,
                edit.properties_json,
                edit.editor,
                edit.edited_at,
                batch,
                now,
            ])?;
        }
        appender.flush()?;
    }
    let file = stage.write(&shape, "SELECT * FROM ledger_append", &batch)?;
    // Every file is staged, so no borrow of the stage lives across the await.
    let outcome = committer
        .append(DataAppend {
            table_name: LEDGER_SOURCE,
            contract: contract(LEDGER_SOURCE)?,
            files: &[file],
            batch: &[batch.clone()],
            already_ingested: &already,
            summary: BTreeMap::new(),
        })
        .await?;
    ensure!(
        matches!(outcome, AppendOutcome::Appended { .. }),
        "the ledger already records batch {batch}"
    );
    println!(
        "map-edit-ledger-append {}",
        json!({
            "unit": UNIT, "ledgered": held, "in_handoff": handoff.len(), "appended": fresh.len(),
            "batch": batch,
        })
    );
    Ok(())
}
