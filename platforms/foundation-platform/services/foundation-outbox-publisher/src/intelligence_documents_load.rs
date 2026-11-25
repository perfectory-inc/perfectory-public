//! `intelligence-documents`: the lakehouse side of the intelligence document jobs (root ADR-0477).
//!
//! `intelligence-document-text.sh` and `intelligence-document-fact.sh` run one step at a time:
//! `plan-text`, `append-text`, `plan-facts`, `append-facts`, and the fold in two halves around
//! its rule (`fold-export`, then `intelligence_documents.py fold-rows` in plain Python, then
//! `fold-append`). `DuckDB` inside the publisher reads the tables, pinned on the snapshot each
//! `main` is at when the step starts, and every append is one Iceberg commit recorded by its run
//! (root ADR-0559, ADR-0560). They were the Spark sessions of those jobs; the files are the same.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{bail, ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    intelligence_documents_stage::{
        export_fold_inputs, plan_facts, plan_text, relation, stage_run_rows, ATTACHMENT_TABLE,
        COMPLEX_FACTS, DETAIL_TABLE, FACT, FACT_OUTCOME, TEXT_OUTCOME, TEXT_PAGE,
    },
    onbid_silver_stage::write,
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Map, Value};

use crate::{
    onbid_silver_load::{append, contract, head, locked, seconds},
    public_data_control_support::required_env_value,
};

const STEP_ENV: &str = "FOUNDATION_PLATFORM_INTELLIGENCE_DOCUMENTS_STEP";
/// The intelligence contract the image carries (`document-extraction.contract.json`).
const EXTRACTION_ENV: &str = "FOUNDATION_PLATFORM_INTELLIGENCE_DOCUMENTS_EXTRACTION";
/// The run's directory: the stage works under it, and a step reads and writes its files in it.
const DIR_ENV: &str = "FOUNDATION_PLATFORM_INTELLIGENCE_DOCUMENTS_DIR";
const RUN_ID_ENV: &str = "FOUNDATION_PLATFORM_INTELLIGENCE_DOCUMENTS_RUN_ID";
/// `1`: the fold takes every complex, not only this run's.
const ALL_ENV: &str = "FOUNDATION_PLATFORM_INTELLIGENCE_DOCUMENTS_ALL";
/// The foundation platform directory of the release, for the pointer to the media store's prefix.
const PLATFORM_ENV: &str = "FOUNDATION_PLATFORM_INTELLIGENCE_DOCUMENTS_PLATFORM_DIR";

/// The file the fold rule writes: `gold.complex_document_facts` rows, one JSON object per line.
const FOLD_APPEND_FILE: &str = "fold-append.jsonl";
/// The admitted gate as the fold writes it, written by `intelligence_documents.py gate`.
const GATE_FILE: &str = "gate.json";
const TABLES: [&str; 4] = [TEXT_OUTCOME, TEXT_PAGE, FACT, FACT_OUTCOME];

/// The store's prefix, read through the pointer the intelligence contract names into foundation's
/// (`platforms/foundation-platform/<file>#/<path>`).
fn media_prefix(platform: &Path, extraction: &Value) -> anyhow::Result<String> {
    let pointer = extraction["inputs"]["media_store"]["object_prefix_from"]
        .as_str()
        .context("the extraction contract names no inputs.media_store.object_prefix_from")?;
    let (relative, fragment) = pointer.split_once('#').unwrap_or((pointer, ""));
    let relative = relative
        .strip_prefix("platforms/foundation-platform/")
        .unwrap_or(relative);
    let path = platform.join(relative);
    let mut value: &Value = &serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("cannot read {}", path.display()))?,
    )?;
    let document = value.clone();
    value = &document;
    for part in fragment
        .trim_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
    {
        value = &value[part];
    }
    value
        .as_str()
        .map(str::to_owned)
        .with_context(|| format!("{pointer} names no text"))
}

/// The lakehouse columns of `table`: the intelligence contract's, then the run that appended them.
fn table_columns(extraction: &Value, output: &str, table: &str) -> anyhow::Result<Vec<String>> {
    let declared = &extraction["outputs"][output];
    ensure!(
        declared["table"].as_str() == Some(table),
        "the intelligence contract writes {output} to {}, not {table}",
        declared["table"]
    );
    let mut columns: Vec<String> = declared["columns"]
        .as_array()
        .with_context(|| format!("outputs.{output}.columns is not a list"))?
        .iter()
        .filter_map(|column| column.as_str().map(str::to_owned))
        .collect();
    columns.push("run_id".to_owned());
    Ok(columns)
}

struct Lakehouse {
    committer: LakehouseDataCommitter,
    catalog: IcebergRestCatalog,
    /// Locked for each use and never held across an await: a `DuckDB` connection is not `Sync`.
    stage: Mutex<DuckDbParquetStage>,
}

impl Lakehouse {
    /// The tables are born on the first step that runs (root ADR-0564 §1), as the Spark job made them.
    async fn open(directory: &Path, tables: &[&str]) -> anyhow::Result<Self> {
        let committer = LakehouseDataCommitter::from_env().await?;
        for table in tables {
            committer.create_table_if_absent(contract(table)?).await?;
        }
        let stage = DuckDbParquetStage::open_reading_lakehouse(
            &directory.join("stage"),
            &LakehouseRead::from_env()?,
        )?;
        Ok(Self {
            committer,
            catalog: IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?,
            stage: Mutex::new(stage),
        })
    }

    /// `table` at the snapshot its `main` is at now (no row for a table with no snapshot).
    async fn at_head(&self, table: &str) -> anyhow::Result<String> {
        let head = head(&self.catalog, table).await?;
        relation(&*locked(&self.stage)?, contract(table)?, head)
    }

    /// Appends the run's file of each `(table, output, file)` in order; returns the summary's tables.
    async fn append_run(
        &self,
        extraction: Option<&Value>,
        outputs: &[(&str, &str, PathBuf)],
        run_id: &str,
    ) -> anyhow::Result<Map<String, Value>> {
        let mut tables = Map::new();
        for (table, output, path) in outputs {
            let table_contract = contract(table)?;
            let columns = match extraction {
                Some(extraction) => table_columns(extraction, output, table)?,
                None => table_contract
                    .columns
                    .iter()
                    .map(|column| column.name.to_owned())
                    .collect(),
            };
            let rows = stage_run_rows(
                &*locked(&self.stage)?,
                table_contract,
                &columns,
                path,
                run_id,
            )?;
            let mut appended = false;
            if rows > 0 {
                let expected_head = head(&self.catalog, table).await?;
                let shape = self.committer.table_shape(table).await?;
                let staged = write(&*locked(&self.stage)?, &shape, table, "id_run_rows", run_id)?;
                // A run the table already records is reported, not appended again.
                appended = append(
                    &self.committer,
                    &self.catalog,
                    table,
                    staged.as_ref(),
                    run_id,
                    expected_head,
                )
                .await?;
            }
            tables.insert(
                (*table).to_owned(),
                json!({"rows": rows, "appended": appended}),
            );
        }
        Ok(tables)
    }
}

/// Runs the step `FOUNDATION_PLATFORM_INTELLIGENCE_DOCUMENTS_STEP` names and prints its summary.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let step = required_env_value(STEP_ENV)?;
    let directory = PathBuf::from(required_env_value(DIR_ENV)?);
    let extraction_path = required_env_value(EXTRACTION_ENV)?;
    let extraction: Value = serde_json::from_slice(
        &std::fs::read(&extraction_path)
            .with_context(|| format!("cannot read {extraction_path}"))?,
    )
    .with_context(|| format!("{extraction_path} is not JSON"))?;
    let mut summary = match step.as_str() {
        "plan-text" => {
            let lakehouse = Lakehouse::open(&directory, &TABLES).await?;
            let prefix = media_prefix(Path::new(&required_env_value(PLATFORM_ENV)?), &extraction)?;
            // Its Silver load creates the attachment index with its first object: until then
            // nothing is owed.
            if lakehouse
                .catalog
                .load_main_snapshot_history(ATTACHMENT_TABLE)
                .await?
                .is_none()
            {
                json!({
                    "step": "plan-text",
                    "extractor_version": extraction["document_text"]["extractor_version"],
                    "owed": 0, "planned": 0, "memory_kill_exhausted": 0, "bytes_to_fetch": 0,
                    "input": "absent",
                })
            } else {
                let attachments = lakehouse.at_head(ATTACHMENT_TABLE).await?;
                let outcomes = lakehouse.at_head(TEXT_OUTCOME).await?;
                let summary = plan_text(
                    &*locked(&lakehouse.stage)?,
                    contract(ATTACHMENT_TABLE)?,
                    contract(TEXT_OUTCOME)?,
                    &attachments,
                    &outcomes,
                    &extraction,
                    &prefix,
                    &directory.join("plan"),
                )?;
                summary
            }
        }
        "plan-facts" => {
            let lakehouse = Lakehouse::open(&directory, &TABLES).await?;
            let prefix = media_prefix(Path::new(&required_env_value(PLATFORM_ENV)?), &extraction)?;
            let text_outcomes = lakehouse.at_head(TEXT_OUTCOME).await?;
            let fact_outcomes = lakehouse.at_head(FACT_OUTCOME).await?;
            let pages = lakehouse.at_head(TEXT_PAGE).await?;
            let summary = plan_facts(
                &*locked(&lakehouse.stage)?,
                contract(TEXT_OUTCOME)?,
                contract(FACT_OUTCOME)?,
                &text_outcomes,
                &fact_outcomes,
                &pages,
                &extraction,
                &prefix,
                &directory.join("plan"),
            )?;
            summary
        }
        // Pages before outcomes and facts before fact outcomes: an outcome is the record that a
        // document is done, so a run stopped between the two leaves the document owed.
        "append-text" | "append-facts" => {
            let lakehouse = Lakehouse::open(&directory, &TABLES).await?;
            let run_id = required_env_value(RUN_ID_ENV)?;
            let out = directory.join("out");
            let outputs = if step == "append-text" {
                [
                    (TEXT_PAGE, "document_text_page", out.join("document_text_page.jsonl")),
                    (
                        TEXT_OUTCOME,
                        "document_text_outcome",
                        out.join("document_text_outcome.jsonl"),
                    ),
                ]
            } else {
                [
                    (FACT, "document_fact", out.join("document_fact.jsonl")),
                    (
                        FACT_OUTCOME,
                        "document_fact_outcome",
                        out.join("document_fact_outcome.jsonl"),
                    ),
                ]
            };
            let tables = lakehouse
                .append_run(Some(&extraction), &outputs, &run_id)
                .await?;
            json!({"step": "append", "run_id": run_id, "tables": tables})
        }
        "fold-export" => {
            let lakehouse = Lakehouse::open(&directory, &[COMPLEX_FACTS]).await?;
            let run_id = required_env_value(RUN_ID_ENV)?;
            let all = match std::env::var(ALL_ENV).as_deref() {
                Ok("1") => true,
                Ok("0") | Err(_) => false,
                Ok(other) => bail!("{ALL_ENV} is 1 or 0, not {other:?}"),
            };
            let gate_path = directory.join(GATE_FILE);
            let gate_json = std::fs::read_to_string(&gate_path)
                .with_context(|| format!("cannot read {}", gate_path.display()))?;
            let relations = [
                lakehouse.at_head(COMPLEX_FACTS).await?,
                lakehouse.at_head(ATTACHMENT_TABLE).await?,
                lakehouse.at_head(FACT).await?,
                lakehouse.at_head(DETAIL_TABLE).await?,
                lakehouse.at_head(TEXT_PAGE).await?,
            ];
            let inputs = export_fold_inputs(
                &*locked(&lakehouse.stage)?,
                [
                    contract(COMPLEX_FACTS)?,
                    contract(ATTACHMENT_TABLE)?,
                    contract(FACT)?,
                    contract(DETAIL_TABLE)?,
                    contract(TEXT_PAGE)?,
                ],
                [
                    &relations[0],
                    &relations[1],
                    &relations[2],
                    &relations[3],
                    &relations[4],
                ],
                &extraction,
                gate_json.trim(),
                &run_id,
                all,
                &directory,
            )?;
            let reason = if !inputs.whole {
                Value::Null
            } else if all {
                json!("--all")
            } else {
                json!("gate or fact version changed")
            };
            json!({
                "step": "fold-export", "run_id": run_id, "whole": inputs.whole, "whole_reason": reason,
                "complexes": inputs.complexes, "rows": inputs.rows,
            })
        }
        "fold-append" => {
            let lakehouse = Lakehouse::open(&directory, &[COMPLEX_FACTS]).await?;
            let run_id = required_env_value(RUN_ID_ENV)?;
            let outputs = [(COMPLEX_FACTS, "", directory.join(FOLD_APPEND_FILE))];
            let tables = lakehouse.append_run(None, &outputs, &run_id).await?;
            let folded = &tables[COMPLEX_FACTS];
            json!({
                "step": "fold-complex-facts", "run_id": run_id,
                "complexes": folded["rows"], "appended": folded["appended"],
            })
        }
        other => bail!(
            "{STEP_ENV} is plan-text, append-text, plan-facts, append-facts, fold-export or fold-append, not {other:?}"
        ),
    };
    summary["engine"] = json!("duckdb");
    summary["seconds"] = json!(seconds(started));
    summary["memory_peak_bytes"] = json!(process_memory::cgroup_peak_bytes());
    println!("intelligence-documents-summary-json {summary}");
    Ok(())
}
