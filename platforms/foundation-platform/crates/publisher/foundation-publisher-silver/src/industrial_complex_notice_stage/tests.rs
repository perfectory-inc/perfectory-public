//! The staged rows are the rows the Spark job built (root ADR-0559 §3).
//!
//! `fixtures/object.jsonl` is a synthetic object (notice numbers `99…`, complex codes `99…`, 2099
//! dates) with the cases the job's own test held and the values a provider may send: a line with
//! no notice number or no readable time, a failed read, a result or file list of the wrong kind,
//! numbers with commas, blank and boolean fields, times to the nanosecond and with offsets, files
//! outside the store, a hash listed twice. `fixtures/spark_reads.txt` and
//! `fixtures/spark_attachments.txt` are what `industryland_or_kr.rows_of` built from it before the
//! job was removed, one row per line, its columns joined by `|`, sorted. The engine-equality
//! verdict the move was made on ran `rows.sql` and `changed.sql` over every object production
//! holds; this keeps the binary on the job's rows.

use std::io::Write as _;

use anyhow::Context;
use chrono::DateTime;
use foundation_publisher_core::lakehouse_data_commit::{TableField, TableShape};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use serde_json::Value;

use super::{
    notice_source, planned_objects, snapshot_id, stage_object, PlannedObject, ATTACHMENT_TABLE,
    DETAIL_TABLE, PLAN_SCHEMA,
};
use crate::duckdb_parquet_stage::{DuckDbParquetStage, FetchedFile};

const OBJECT: &str = include_str!("fixtures/object.jsonl");
const META: &str = include_str!("fixtures/object.json");
const SPARK_READS: &str = include_str!("fixtures/spark_reads.txt");
const SPARK_ATTACHMENTS: &str = include_str!("fixtures/spark_attachments.txt");

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("the {table} contract"))
}

fn shape(table: &str) -> anyhow::Result<TableShape> {
    Ok(TableShape {
        location: String::new(),
        fields: contract(table)?
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

/// The fixture object as a plan would have fetched it.
fn object(directory: &std::path::Path) -> anyhow::Result<PlannedObject> {
    let meta: Value = serde_json::from_str(META)?;
    let path = directory.join("object.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&path)?,
        flate2::Compression::default(),
    );
    gzip.write_all(OBJECT.as_bytes())?;
    gzip.finish()?;
    Ok(PlannedObject {
        object_key: meta["object_key"]
            .as_str()
            .context("object_key")?
            .to_owned(),
        source_snapshot_id: meta["run"].as_str().context("run")?.to_owned(),
        path: FetchedFile::at(path),
    })
}

/// The rows of a staged file as the fixture writes them: each column as text, `NULL` for none,
/// a time as UTC to the microsecond.
fn lines(
    stage: &DuckDbParquetStage,
    table: &str,
    file: &std::path::Path,
) -> anyhow::Result<Vec<String>> {
    let path = file.to_str().context("UTF-8")?.replace('\\', "/");
    let parts = contract(table)?
        .columns
        .iter()
        .map(|column| {
            if column.logical_type == "timestamp" {
                format!(
                    "coalesce(strftime(make_timestamp(epoch_us({})), '%Y-%m-%d %H:%M:%S.%f'), 'NULL')",
                    column.name
                )
            } else {
                format!("coalesce(CAST({} AS VARCHAR), 'NULL')", column.name)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut found = stage
        .connection()
        .prepare(&format!(
            "SELECT concat_ws('|', {parts}) FROM read_parquet('{path}')"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    found.sort();
    Ok(found)
}

fn ingested_at() -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let meta: Value = serde_json::from_str(META)?;
    DateTime::from_timestamp(
        meta["ingested_at_epoch_seconds"]
            .as_i64()
            .context("ingested_at_epoch_seconds")?,
        0,
    )
    .context("an instant")
}

#[test]
fn the_staged_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    let object = object(root.path())?;
    let staged = stage_object(
        &stage,
        &notice_source()?,
        (&shape(DETAIL_TABLE)?, &shape(ATTACHMENT_TABLE)?),
        contract(ATTACHMENT_TABLE)?,
        &object,
        None,
        ingested_at()?,
    )?;
    let reads = SPARK_READS.lines().collect::<Vec<_>>();
    let attachments = SPARK_ATTACHMENTS.lines().collect::<Vec<_>>();
    assert_eq!(staged.reads, u64::try_from(reads.len())?);
    assert_eq!(staged.attachments, u64::try_from(attachments.len())?);
    // With no snapshot every attachment is new.
    assert_eq!(staged.changed_attachments, staged.attachments);
    let file = staged.reads_file.context("a reads file")?;
    assert_eq!(lines(&stage, DETAIL_TABLE, &file.local_path)?, reads);
    let file = staged.attachments_file.context("an attachments file")?;
    assert_eq!(
        lines(&stage, ATTACHMENT_TABLE, &file.local_path)?,
        attachments
    );
    Ok(())
}

#[test]
fn a_run_id_is_read_from_the_key_and_a_key_without_one_is_refused() -> anyhow::Result<()> {
    assert_eq!(
        snapshot_id(
            "bronze/source=x/operation=y/industrial-complex-notice-detail-20990102t030405z-000.gz"
        )?,
        "20990102T030405Z"
    );
    for refused in [
        "bronze/source=x/no-run-id.gz",
        "bronze/source=x/detail-20990102t030405z.gz",
        "bronze/source=x/detail-20990102T030405Z-000.gz",
        "bronze/source=x/detail-20990102t030405z-000.json",
    ] {
        assert!(snapshot_id(refused).is_err(), "{refused}");
    }
    Ok(())
}

#[test]
fn a_plan_is_read_oldest_run_first_and_refuses_what_is_not_this_loads() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let source = notice_source()?;
    let key = |run: &str, part: &str| {
        format!(
            "bronze/source={}/detail-{run}-{part}.gz",
            source.source_slug
        )
    };
    let write = |objects: Value, schema: &str| -> anyhow::Result<()> {
        std::fs::write(
            root.path().join("plan.json"),
            serde_json::to_vec(&serde_json::json!({"schema_version": schema, "objects": objects}))?,
        )?;
        Ok(())
    };
    let entry = |object_key: String, slug: &str, local: &str| serde_json::json!({"object_key": object_key, "source_slug": slug, "local_path": local});
    write(
        serde_json::json!([
            entry(
                key("20990103t000000z", "000"),
                &source.source_slug,
                "objects/b.gz"
            ),
            entry(
                key("20990102t000000z", "001"),
                &source.source_slug,
                "objects/a1.gz"
            ),
            entry(
                key("20990102t000000z", "000"),
                &source.source_slug,
                "objects/a0.gz"
            ),
        ]),
        PLAN_SCHEMA,
    )?;
    let objects = planned_objects(root.path(), &source)?;
    assert_eq!(
        objects.iter().map(|o| o.path.clone()).collect::<Vec<_>>(),
        ["a0.gz", "a1.gz", "b.gz"]
            .map(|name| FetchedFile::at(root.path().join("objects").join(name)))
    );
    write(
        serde_json::json!([entry(
            key("20990102t000000z", "000"),
            "industrylandorkr__other",
            "objects/a.gz"
        )]),
        PLAN_SCHEMA,
    )?;
    assert!(planned_objects(root.path(), &source).is_err());
    write(
        serde_json::json!([entry(
            key("20990102t000000z", "000"),
            &source.source_slug,
            "../a.gz"
        )]),
        PLAN_SCHEMA,
    )?;
    assert!(planned_objects(root.path(), &source).is_err());
    write(serde_json::json!([]), "another.v1")?;
    assert!(planned_objects(root.path(), &source).is_err());
    Ok(())
}
