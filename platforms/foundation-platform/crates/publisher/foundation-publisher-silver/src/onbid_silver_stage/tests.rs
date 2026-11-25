//! The staged rows are the rows the Spark job built (root ADR-0559 §3).
//!
//! `fixtures/` holds synthetic objects (item numbers `99…`, PNUs under the synthetic 99999 code,
//! 2099 dates) with the cases the job's own tests held and the values a provider may send, and
//! `spark_<role>.txt`: what `data_go_kr_onbid.rows_of` / `photo_rows_of` built from them before the
//! job was removed, one row per line, its columns joined by `|`, sorted, with one correction: a
//! provider time is read as its fixed 12 or 14 digits (`onbid_kst`), not as the job's `strptime`
//! misread a 12-digit minute (09:30 as 09:03:00). The engine-equality verdict
//! the move was made on ran the same SQL over the production objects; this keeps the binary on the
//! job's rows. `fixtures/rows_<role>.sql` is the row SQL the verdict ran: the binary renders the same
//! text.

use std::io::Write as _;

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{TableField, TableShape};
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};

use super::{
    planned_objects, read_object, snapshot_id, write, OnbidSource, PlannedObject, Role,
    PLAN_SCHEMA, ROLES,
};
use crate::duckdb_parquet_stage::{DuckDbParquetStage, FetchedFile};

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(table)
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

fn fixture(name: &str) -> anyhow::Result<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/onbid_silver_stage/fixtures")
        .join(name);
    std::fs::read_to_string(&path).with_context(|| format!("cannot read {}", path.display()))
}

const OBJECT_KEY: &str =
    "bronze/source=datagokr__synthetic/year=2099/month=01/day=02/onbid-20990102t030405z.gz";

fn object(directory: &std::path::Path, role: Role) -> anyhow::Result<PlannedObject> {
    let path = directory.join(format!("{}.gz", role.name()));
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&path)?,
        flate2::Compression::default(),
    );
    gzip.write_all(fixture(&format!("{}.jsonl", role.name()))?.as_bytes())?;
    gzip.finish()?;
    Ok(PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        role,
        source_snapshot_id: "20990102T030405Z".to_owned(),
        path: FetchedFile::at(path),
    })
}

/// The rows of a staged file as the fixture writes them: each column as text, `NULL` for none, a
/// time as UTC to the microsecond.
fn lines(
    stage: &DuckDbParquetStage,
    table: &str,
    file: &std::path::Path,
) -> anyhow::Result<Vec<String>> {
    let path = file.to_str().context("UTF-8")?.replace('\\', "/");
    let parts = contract(table)?
        .columns
        .iter()
        .filter(|column| column.name != "ingested_at_utc")
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

#[test]
fn the_run_id_is_read_from_the_key() -> anyhow::Result<()> {
    assert_eq!(snapshot_id(OBJECT_KEY)?, "20990102T030405Z");
    assert!(snapshot_id("bronze/x/onbid-2099.gz").is_err());
    assert!(snapshot_id("bronze/x/onbid-20990102t030405z.json").is_err());
    Ok(())
}

/// The SQL text with its runs of whitespace as one space: the binary's rendering and the verdict's
/// are the same statements, not the same line breaks.
fn squashed(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn the_rendered_row_sql_is_the_fixture() -> anyhow::Result<()> {
    let source = OnbidSource::compiled()?;
    for role in ROLES {
        assert_eq!(
            squashed(&source.rows_sql(role, contract(role.table())?)?),
            squashed(&fixture(&format!("rows_{}.sql", role.name()))?),
            "{}",
            role.name()
        );
    }
    Ok(())
}

#[test]
fn a_plan_names_its_table_and_each_object_once() -> anyhow::Result<()> {
    let source = OnbidSource::compiled()?;
    let directory = tempfile::tempdir()?;
    let plan = directory.path().join("onbid_item_bid_result");
    std::fs::create_dir_all(&plan)?;
    let slug = source.source_slug(Role::ItemBidResult)?;
    let entry = serde_json::json!({"object_key": OBJECT_KEY, "source_slug": slug, "local_path": "objects/a.gz"});
    std::fs::write(
        plan.join("plan.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema_version": PLAN_SCHEMA, "table": "silver.onbid_item_bid_result", "objects": [entry],
        }))?,
    )?;
    let objects = planned_objects(std::slice::from_ref(&plan), &source)?;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].role, Role::ItemBidResult);
    assert!(planned_objects(&[plan.clone(), plan.clone()], &source).is_err());
    std::fs::write(
        plan.join("plan.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema_version": PLAN_SCHEMA, "table": "silver.onbid_announcement", "objects": [entry],
        }))?,
    )?;
    assert!(
        planned_objects(&[plan], &source).is_err(),
        "a bid result object does not load into the announcements"
    );
    Ok(())
}

/// 2026-11-17: the provider answered an announcement detail request with a 200 whose body was its
/// "503 - Service Unavailable" HTML page, and the object stopped every hourly load. Such a line is
/// a request with no answer: counted, no rows, and the lines beside it are read.
#[test]
fn a_200_whose_body_is_not_json_is_counted_and_read_as_no_answer() -> anyhow::Result<()> {
    let source = OnbidSource::compiled()?;
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(&source.values_sql()?)?;
    let ingested_at = chrono::DateTime::from_timestamp(4_070_000_000, 0).context("an instant")?;
    let role = Role::AnnouncementDetail;
    let answered = fixture("announcement_detail.jsonl")?;
    let expected = read_object(
        &stage,
        &source,
        contract(role.table())?,
        &object(directory.path(), role)?,
        ingested_at,
    )?;
    assert_eq!(expected.unreadable_answers, 0);

    let page = serde_json::json!({
        "request": {"pbancMngNo": "209901-99999-00"},
        "http_status": 200,
        "body": "<HTML><HEAD><TITLE>503 - Service Unavailable</TITLE></HEAD></HTML>",
    });
    let missing =
        serde_json::json!({"request": {"pbancMngNo": "209901-99999-01"}, "http_status": 200});
    let path = directory.path().join("with-a-page.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&path)?,
        flate2::Compression::default(),
    );
    writeln!(gzip, "{page}")?;
    writeln!(gzip, "{}", answered.trim_end())?;
    writeln!(gzip, "{missing}")?;
    gzip.finish()?;
    let read = read_object(
        &stage,
        &source,
        contract(role.table())?,
        &PlannedObject {
            object_key: OBJECT_KEY.to_owned(),
            role,
            source_snapshot_id: "20990102T030405Z".to_owned(),
            path: FetchedFile::at(path),
        },
        ingested_at,
    )?;
    assert_eq!(
        (read.unreadable_answers, read.rows),
        (2, expected.rows),
        "the page and the missing body are counted; the answered lines give their rows"
    );
    Ok(())
}

/// One role's fixture object staged, against the rows the Spark job built from it. A test per role:
/// the eight together ran past the 60 s limit on CI (run 44440260869), where the bundled `DuckDB`
/// is built unoptimized and plans each role's SQL in seconds.
fn the_staged_rows_are_the_rows_the_spark_job_built(role: Role) -> anyhow::Result<()> {
    let source = OnbidSource::compiled()?;
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(&source.values_sql()?)?;
    let ingested_at = chrono::DateTime::from_timestamp(4_070_000_000, 0).context("an instant")?;
    let table = role.table();
    let read = read_object(
        &stage,
        &source,
        contract(table)?,
        &object(directory.path(), role)?,
        ingested_at,
    )?;
    let expected = fixture(&format!("spark_{}.txt", role.name()))?;
    let expected = expected.lines().map(str::to_owned).collect::<Vec<_>>();
    assert_eq!(read.rows, u64::try_from(expected.len())?, "{}", role.name());
    if expected.is_empty() {
        return Ok(());
    }
    let file =
        write(&stage, &shape(table)?, table, "onbid_rows", OBJECT_KEY)?.context("rows to stage")?;
    assert_eq!(
        lines(&stage, table, &file.local_path)?,
        expected,
        "{}",
        role.name()
    );
    Ok(())
}

#[test]
fn the_staged_item_list_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::ItemList)
}

#[test]
fn the_staged_item_bid_result_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::ItemBidResult)
}

#[test]
fn the_staged_item_photo_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::ItemPhoto)
}

#[test]
fn the_staged_item_detail_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::ItemDetail)
}

#[test]
fn the_staged_item_bid_info_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::ItemBidInfo)
}

#[test]
fn the_staged_announcement_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::Announcement)
}

#[test]
fn the_staged_announcement_bid_result_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()>
{
    the_staged_rows_are_the_rows_the_spark_job_built(Role::AnnouncementBidResult)
}

#[test]
fn the_staged_announcement_detail_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::AnnouncementDetail)
}

#[test]
fn the_roles_are_the_tables_the_source_contract_lists() -> anyhow::Result<()> {
    let contract: serde_json::Value = serde_json::from_str(super::SOURCE_CONTRACT)?;
    let listed = contract["silver_load"]["tables"]
        .as_object()
        .context("silver_load.tables")?
        .iter()
        .filter(|(table, _)| !table.starts_with('$'))
        .map(|(table, endpoint)| {
            (
                table.clone(),
                endpoint.as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let roles = ROLES
        .iter()
        .map(|role| (role.table().to_owned(), role.endpoint().to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(listed, roles);
    Ok(())
}

/// The rows `read_object` staged of `role`'s fixture object, each as text, sorted, and how many
/// answers it could not read. With `one_line_a_part`, the contract's memory per byte is so large
/// that every line is a part of its own.
fn staged_rows(role: Role, one_line_a_part: bool) -> anyhow::Result<(Vec<String>, u64)> {
    let mut source = OnbidSource::compiled()?;
    if one_line_a_part {
        source.contract["silver_load"]["memory_per_line_byte"]["bytes"] =
            serde_json::json!(u64::MAX);
    }
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(&source.values_sql()?)?;
    let ingested_at = chrono::DateTime::from_timestamp(4_070_000_000, 0).context("an instant")?;
    let read = read_object(
        &stage,
        &source,
        contract(role.table())?,
        &object(directory.path(), role)?,
        ingested_at,
    )?;
    let mut rows = stage
        .connection()
        .prepare("SELECT CAST(t AS VARCHAR) FROM onbid_rows t")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort();
    Ok((rows, read.unreadable_answers))
}

#[test]
fn every_role_stages_the_same_rows_read_a_line_a_part_as_read_in_one_part() -> anyhow::Result<()> {
    // 2026-11-25 the load moved from its own loop over lines to the shared reader's parts.
    for role in ROLES {
        let whole = staged_rows(role, false)?;
        assert_eq!(staged_rows(role, true)?, whole, "{}", role.name());
    }
    Ok(())
}
