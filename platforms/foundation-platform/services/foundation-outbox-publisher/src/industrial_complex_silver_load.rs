//! The Silver load of the industrial complexes (root ADR-0033, ADR-0559): it was the Spark job
//! `industrial_complex_bronze_to_silver.py`, started by hand.
//!
//! Two commands, run by `industrial-complex-silver-load.sh` around the two that already build the
//! export:
//!
//! 1. `plan-industrial-complex-silver-inputs` lists the Bronze prefixes of the export's inputs on R2
//!    (the lakehouse reader key) and takes the newest object of each: the profile workbook, the ILIS
//!    complex list and notice list, the ILIS detail of each complex the list lacks (the newest per
//!    complex), and the V-World district boundary whose `.dbf` is the district authority. When the
//!    table's commits record every one of them (`load_ingested_batch_objects`), nothing is owed and
//!    nothing is fetched. Otherwise it fetches them into the run's directory under their own keys,
//!    the layout the two builders read (`bronze/source=<slug>/…`), and writes `plan.json`.
//! 2. `build-industrial-complex-address-resolution` and `export-industrial-complex-bronze-raw-jsonl`
//!    build the export from that directory, unchanged.
//! 3. `load-industrial-complex-silver` stages the export in `DuckDB`
//!    (`foundation_publisher_silver::industrial_complex_silver_stage`) and appends the complexes that
//!    are new or changed, in one commit that records every input object the table did not record
//!    yet, even when no complex changed. An input an earlier load recorded (the profile workbook
//!    stays the same for months) stays recorded there: a batch is appended whole or not at all, and
//!    one that named it again was refused (2026-11-22, the first run of this load). The next plan
//!    reads the record of every commit.
//!
//! The whole input is one workbook of 1,442 complexes and four answers of a few megabytes and one
//! 98 MB boundary archive: one pass, no regional split (the declared exception: kilobytes of rows).

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use foundation_outbox::object_storage::{R2InventoryRequest, R2ObjectStorageConfig};
use foundation_outbox::R2ObjectStorage;
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, TableField, TableShape,
};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    industrial_complex_silver_stage::{stage_export, TABLE},
};
use lakehouse_domain::SILVER_INDUSTRIAL_COMPLEXES;
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::public_data_control_support::{required_env_value, write_json_file};

const PREFIX: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_SILVER";
const PLAN_SCHEMA: &str = "foundation-platform.industrial_complex_silver_inputs.v1";
const LIST_PAGE: i32 = 1000;

/// One input of the export: a Bronze source and how its newest object is chosen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Input {
    pub(crate) source_slug: &'static str,
    /// The object's file extension (`zip`, `json`): the builders look objects up by it.
    pub(crate) extension: &'static str,
    /// Whether the source holds one object per complex (`complex=<code>` in its key), the newest of
    /// each taken, rather than one newest object.
    pub(crate) per_complex: bool,
}

/// What the export is built from (`build-industrial-complex-address-resolution` reads them all).
pub(crate) const INPUTS: [Input; 5] = [
    Input {
        source_slug: "vworldkr__sandan_profile",
        extension: "zip",
        per_complex: false,
    },
    Input {
        source_slug: "industrylandorkr__industrial_complex_list",
        extension: "json",
        per_complex: false,
    },
    Input {
        source_slug: "industrylandorkr__industrial_complex_notice",
        extension: "json",
        per_complex: false,
    },
    Input {
        source_slug: "industrylandorkr__industrial_complex_detail",
        extension: "json",
        per_complex: true,
    },
    Input {
        source_slug: "vworldkr__boundary_sigungu",
        extension: "zip",
        per_complex: false,
    },
];

/// One Bronze object as R2 lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Listed {
    pub(crate) key: String,
    pub(crate) last_modified: DateTime<Utc>,
}

/// The `complex=<code>` segment of a per-complex key.
fn complex_of(key: &str) -> Option<&str> {
    key.split('/')
        .find_map(|segment| segment.strip_prefix("complex="))
        .filter(|code| !code.is_empty())
}

/// The newest object of `input` among `listed` (its own prefix), or of each complex: newest by
/// R2's modification time, then by key.
///
/// # Errors
/// A source with no object of its extension: the export cannot be built without it.
pub(crate) fn newest(input: &Input, listed: &[Listed]) -> anyhow::Result<Vec<String>> {
    let suffix = format!(".{}", input.extension);
    let mut chosen: BTreeMap<&str, &Listed> = BTreeMap::new();
    for object in listed
        .iter()
        .filter(|object| object.key.to_ascii_lowercase().ends_with(&suffix))
    {
        let group = if input.per_complex {
            match complex_of(&object.key) {
                Some(code) => code,
                None => continue,
            }
        } else {
            ""
        };
        let newer = chosen.get(group).is_none_or(|held| {
            (object.last_modified, &object.key) > (held.last_modified, &held.key)
        });
        if newer {
            chosen.insert(group, object);
        }
    }
    // The detail source may be empty: the bulk list carried every complex.
    if chosen.is_empty() && !input.per_complex {
        bail!(
            "Bronze holds no .{} object of {}; the industrial-complex export cannot be built without it",
            input.extension,
            input.source_slug
        );
    }
    Ok(chosen.values().map(|object| object.key.clone()).collect())
}

/// Where a key lands in the run's directory: under its own key, the layout the builders read.
fn mirror_path(root: &Path, key: &str) -> anyhow::Result<PathBuf> {
    ensure!(
        key.starts_with("bronze/source=")
            && key.split('/').all(|segment| !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '='))),
        "{key} is not a Bronze object key this load fetches"
    );
    Ok(root.join(key))
}

fn parse_modified(key: &str, value: Option<&str>) -> anyhow::Result<DateTime<Utc>> {
    let text = value.with_context(|| format!("R2 lists {key} without a modification time"))?;
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .with_context(|| format!("R2 lists {key} with an unreadable modification time {text:?}"))
}

/// `plan-industrial-complex-silver-inputs`: chooses the inputs, asks the table, fetches what is
/// owed and writes `plan.json` in `..._WORK_DIR`. Its last stdout line says `owed` or `unchanged`.
///
/// # Errors
/// R2 or the catalog cannot be read, a source has no object, or a file cannot be written.
pub(crate) async fn plan() -> anyhow::Result<()> {
    let work = PathBuf::from(required_env_value(&format!("{PREFIX}_WORK_DIR"))?);
    let storage = R2ObjectStorage::from_config(
        R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("the industrial-complex Silver plan reads R2 with the lakehouse reader key")?,
    );
    let mut chosen = Vec::new();
    for input in &INPUTS {
        let prefix = format!("bronze/source={}/", input.source_slug);
        let report = storage
            .inventory_audit(R2InventoryRequest::new(Some(&prefix), Some(LIST_PAGE))?)
            .await
            .with_context(|| format!("failed to list {prefix}"))?;
        let listed = report
            .objects()
            .iter()
            .map(|object| {
                Ok(Listed {
                    key: object.key.clone(),
                    last_modified: parse_modified(&object.key, object.last_modified.as_deref())?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        for key in newest(input, &listed)? {
            chosen.push((input.source_slug, key));
        }
    }
    let catalog = IcebergRestCatalog::new(catalog_config()?)?;
    let recorded = catalog
        .load_ingested_batch_objects(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?;
    let unrecorded = chosen
        .iter()
        .filter(|(_, key)| !recorded.contains(key))
        .map(|(_, key)| key.clone())
        .collect::<Vec<_>>();
    let owed = !unrecorded.is_empty();
    let bronze_root = work.join("bronze-root");
    let mut objects = Vec::with_capacity(chosen.len());
    for (slug, key) in &chosen {
        let path = mirror_path(&bronze_root, key)?;
        if owed {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            let bytes = storage
                .get_object_bytes(key)
                .await
                .with_context(|| format!("failed to read {key}"))?;
            std::fs::write(&path, &bytes)
                .with_context(|| format!("failed to write {}", path.display()))?;
        }
        objects.push(json!({"source_slug": slug, "object_key": key}));
    }
    let boundary = chosen
        .iter()
        .find(|(slug, _)| *slug == "vworldkr__boundary_sigungu")
        .map(|(_, key)| key.clone())
        .context("no district boundary was chosen")?;
    let plan = json!({
        "schema_version": PLAN_SCHEMA,
        "table": TABLE,
        "owed": owed,
        "unrecorded": unrecorded,
        "bronze_root": "bronze-root",
        "sigungu_authority": format!("bronze-root/{boundary}"),
        "objects": objects,
    });
    write_json_file(&work.join("plan.json"), &plan)?;
    println!(
        "industrial-complex-silver-plan {}",
        json!({"owed": owed, "inputs": chosen.len(), "unrecorded": plan["unrecorded"]})
    );
    Ok(())
}

/// The input keys a plan chose, each once.
pub(crate) fn planned_keys(plan: &Value) -> anyhow::Result<Vec<String>> {
    ensure!(
        plan["schema_version"] == PLAN_SCHEMA,
        "not a {PLAN_SCHEMA} plan"
    );
    let mut keys = BTreeSet::new();
    for object in plan["objects"]
        .as_array()
        .context("the plan lists no objects")?
    {
        let key = object["object_key"]
            .as_str()
            .context("a planned object has no key")?;
        ensure!(keys.insert(key.to_owned()), "the plan names {key} twice");
    }
    ensure!(!keys.is_empty(), "the plan names no input");
    Ok(keys.into_iter().collect())
}

/// The planned inputs `recorded` does not hold, in plan order: what this load's commit records.
///
/// The export is built from every planned input, but an input an earlier commit recorded is not
/// recorded again. The commit's batch is appended whole or not at all, so naming the profile
/// workbook a second time refused the whole load; and the union of every commit's record, which the
/// next plan reads, names it already.
pub(crate) fn unrecorded_inputs(planned: &[String], recorded: &BTreeSet<String>) -> Vec<String> {
    planned
        .iter()
        .filter(|key| !recorded.contains(*key))
        .cloned()
        .collect()
}

/// The catalog settings a step reads with: the read-only token when the environment holds one (a
/// verdict run with the reader key alone), the unit's own otherwise.
fn catalog_config() -> anyhow::Result<LakehouseCatalogConfig> {
    Ok(
        if std::env::var_os("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN").is_some() {
            LakehouseCatalogConfig::reader_from_env()?
        } else {
            LakehouseCatalogConfig::from_env()?
        },
    )
}

/// The table's shape as its contract numbers it: what a dry run stages with, having no writer to
/// ask the live schema of.
fn contract_shape() -> anyhow::Result<TableShape> {
    Ok(TableShape {
        location: String::new(),
        fields: SILVER_INDUSTRIAL_COMPLEXES
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Ok(TableField {
                    id: i32::try_from(index + 1)?,
                    name: column.name.to_owned(),
                    required: column.required,
                    element_id: None,
                })
            })
            .collect::<anyhow::Result<_>>()?,
    })
}

/// `load-industrial-complex-silver`: stages `..._EXPORT_PATH` and, with `..._LIVE_WRITE=1`, appends
/// what changed, recording every input of `..._WORK_DIR/plan.json`. Without it nothing is committed:
/// every row of the export is written to `verdict-rows.parquet` in the work directory, for the
/// engine verdict to compare with the table (the code that would commit is the code that stages).
/// The last stdout line is `industrial-complex-silver-summary-json {json}`.
///
/// # Errors
/// The plan or the export cannot be read, a gate refuses the export, or the commit fails.
pub(crate) async fn load() -> anyhow::Result<()> {
    let work = PathBuf::from(required_env_value(&format!("{PREFIX}_WORK_DIR"))?);
    let export = PathBuf::from(required_env_value(&format!("{PREFIX}_EXPORT_PATH"))?);
    let live = std::env::var(format!("{PREFIX}_LIVE_WRITE")).is_ok_and(|value| value.trim() == "1");
    let plan: Value = serde_json::from_slice(
        &std::fs::read(work.join("plan.json"))
            .with_context(|| format!("cannot read the plan in {}", work.display()))?,
    )?;
    let batch = planned_keys(&plan)?;
    let committer = if live {
        Some(LakehouseDataCommitter::from_env().await?)
    } else {
        None
    };
    let shape = match &committer {
        Some(committer) => committer.table_shape(TABLE).await?,
        None => contract_shape()?,
    };
    let catalog_settings = catalog_config()?;
    let catalog = IcebergRestCatalog::new(catalog_settings.clone())?;
    let head = catalog
        .load_main_snapshot_history(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id);
    // The data files are read with the reader pair, the catalog with the step's token.
    let storage = R2ObjectStorageConfig::lakehouse_reader_from_env().context(
        "the industrial-complex Silver load reads the table with the lakehouse reader key",
    )?;
    let read = LakehouseRead {
        extensions: foundation_publisher_silver::duckdb_extensions::release_directory()?,
        catalog_uri: catalog_settings.catalog_uri,
        warehouse: catalog_settings.warehouse,
        catalog_token: catalog_settings
            .catalog_token
            .context("the lakehouse catalog token is required")?,
        s3_endpoint: storage.endpoint,
        s3_access_key_id: storage.access_key_id,
        s3_secret_access_key: storage.secret_access_key,
    };
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(&work, &read)?);
    let staged = {
        let stage = stage
            .lock()
            .map_err(|_| anyhow::anyhow!("an earlier step of the load panicked"))?;
        let staged = stage_export(
            &stage,
            &shape,
            &SILVER_INDUSTRIAL_COMPLEXES,
            &export,
            head,
            "industrial-complexes",
        )?;
        if committer.is_none() {
            let target = work.join("verdict-rows.parquet");
            let text = target
                .to_str()
                .context("the work path must be UTF-8")?
                .replace('\\', "/");
            ensure!(
                !text.contains('\''),
                "the work path must not contain a quote"
            );
            stage
                .connection()
                .execute_batch(&format!(
                    "COPY (SELECT * FROM complex_rows) TO '{text}' (FORMAT parquet)"
                ))
                .context("DuckDB could not write the verdict rows")?;
        }
        staged
    };
    let appended = match &committer {
        None => false,
        Some(committer) => {
            let already_ingested = catalog
                .load_ingested_batch_objects(TABLE)
                .await?
                .with_context(|| format!("{TABLE} does not exist"))?;
            let unrecorded = unrecorded_inputs(&batch, &already_ingested);
            if unrecorded.is_empty() {
                false
            } else {
                let outcome = committer
                    .append_at_head(
                        DataAppend {
                            table_name: TABLE,
                            contract: &SILVER_INDUSTRIAL_COMPLEXES,
                            files: staged.file.as_slice(),
                            batch: &unrecorded,
                            already_ingested: &already_ingested,
                            summary: BTreeMap::new(),
                        },
                        head,
                    )
                    .await?;
                matches!(outcome, AppendOutcome::Appended { .. })
            }
        }
    };
    drop(stage);
    let status = match (live, appended) {
        (false, _) => "dry_run",
        (true, true) => "loaded",
        (true, false) => "already_loaded",
    };
    let summary = json!({
        "job": "load-industrial-complex-silver",
        "engine": "duckdb",
        "status": status,
        "inputs": batch,
        "head": head,
        "rows": staged.rows,
        "changed_rows": staged.changed,
        "appended": appended,
    });
    println!("industrial-complex-silver-summary-json {summary}");
    Ok(())
}

#[cfg(test)]
mod tests;
