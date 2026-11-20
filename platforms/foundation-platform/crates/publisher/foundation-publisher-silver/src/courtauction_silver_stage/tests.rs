//! The staged rows are the rows the Spark job built (root ADR-0559 §3).
//!
//! `fixtures/` holds synthetic objects (the court `B999999`, 2099 dates) with the cases the job's
//! own tests held and the values a site answer may hold (a failed read, a body that is not JSON or
//! not an object, a later line naming the same key, `0`, `5`, `1.5`, `true`, a nine-digit fraction),
//! and `spark_<table>.txt`: what `courtauction_go_kr`'s `code_rows_of`, `report_rows_of`,
//! `spec_read_rows_of` and `dividend_rows_of` built from them before the job left Spark, one row per
//! line, its columns (but `ingested_at_utc`) joined by `|`, sorted.

use std::io::Write as _;

use anyhow::Context;
use chrono::TimeZone as _;
use foundation_publisher_core::lakehouse_data_commit::{TableField, TableShape};
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseRowVersions,
    LakehouseTableContract,
};

use super::{
    planned_objects, read_object, CourtAuctionSource, Keep, PlannedObject, Role, PLAN_SCHEMA, ROLES,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;
use crate::onbid_silver_stage::write;

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
        .join("src/courtauction_silver_stage/fixtures")
        .join(name);
    std::fs::read_to_string(&path).with_context(|| format!("cannot read {}", path.display()))
}

const OBJECT_KEY: &str =
    "bronze/source=courtauctiongokr__synthetic/year=2099/month=01/day=02/ca-20990102t030405z.gz";

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
        path,
    })
}

/// The rows of a staged file as the fixture writes them: each column as text (a line break as
/// `\n`), `NULL` for none, a time as UTC to the microsecond.
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
                format!(
                    "coalesce(replace(CAST({} AS VARCHAR), chr(10), '\\n'), 'NULL')",
                    column.name
                )
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

fn the_staged_rows_are_the_rows_the_spark_job_built(role: Role) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    let ingested_at = chrono::Utc
        .with_ymd_and_hms(2099, 1, 2, 3, 4, 5)
        .single()
        .context("a time")?;
    let staged = read_object(
        &stage,
        &source,
        &object(directory.path(), role)?,
        ingested_at,
    )?;
    assert_eq!(
        staged.iter().map(|table| table.table).collect::<Vec<_>>(),
        role.tables()
            .iter()
            .map(|(table, _)| *table)
            .collect::<Vec<_>>()
    );
    for table in &staged {
        let short = table.table.split_once('.').context("a schema")?.1;
        // A table more than one role writes has the rows of each role's object apart.
        let expected = fixture(&format!("spark_{}__{short}.txt", role.name()))
            .or_else(|_| fixture(&format!("spark_{short}.txt")))?;
        if table.rows == 0 {
            assert_eq!(expected, "", "{}", table.table);
            continue;
        }
        let file = write(
            &stage,
            &shape(table.table)?,
            table.table,
            &table.staged,
            OBJECT_KEY,
        )?
        .context("rows to stage")?;
        assert_eq!(
            lines(&stage, table.table, &file.local_path)?,
            expected.lines().map(str::to_owned).collect::<Vec<_>>(),
            "{}",
            table.table
        );
    }
    Ok(())
}

#[test]
fn the_staged_common_code_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::CommonCodes)
}

#[test]
fn the_staged_appraisal_report_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::AppraisalReports)
}

#[test]
fn the_staged_sale_specification_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::SaleSpecifications)
}

#[test]
fn the_staged_dividend_notice_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::DividendNotices)
}

#[test]
fn the_staged_sale_notice_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::SaleNotices)
}

#[test]
fn the_staged_case_detail_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::CaseDetail)
}

#[test]
fn the_staged_case_survey_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::CaseSurvey)
}

#[test]
fn the_staged_case_appraisal_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::CaseAppraisal)
}

#[test]
fn the_staged_case_filing_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::CaseFilings)
}

#[test]
fn the_staged_case_survey_report_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::CaseSurveyReport)
}

#[test]
fn the_staged_item_detail_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    the_staged_rows_are_the_rows_the_spark_job_built(Role::ItemDetail)
}

#[test]
fn a_case_document_read_without_its_case_refuses_the_object() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    let path = directory.path().join("keyless.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&path)?,
        flate2::Compression::default(),
    );
    let line = serde_json::json!({
        "operation": "selectAuctnCsSrchRslt",
        "read_outcome": "failed",
        "request": {"dma_srchCsDtlInf": {"cortOfcCd": "B999999"}},
    });
    gzip.write_all(format!("{line}\n").as_bytes())?;
    gzip.finish()?;
    let object = PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        role: Role::CaseDetail,
        source_snapshot_id: "20990102T030405Z".to_owned(),
        path,
    };
    let refused = read_object(&stage, &source, &object, chrono::Utc::now());
    assert!(
        refused.is_err(),
        "a case document read without its case is refused"
    );
    Ok(())
}

#[test]
fn a_notice_detail_without_its_key_refuses_the_object() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    let path = directory.path().join("keyless.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&path)?,
        flate2::Compression::default(),
    );
    let line = serde_json::json!({
        "operation": "selectRletDspslPbancDtl",
        "read_outcome": "read",
        "request": {"dma_srchGnrlPbanc": {"cortOfcCd": "B999999"}},
        "body": serde_json::json!({"data": {"result": {}}}).to_string(),
    });
    gzip.write_all(format!("{line}\n").as_bytes())?;
    gzip.finish()?;
    let object = PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        role: Role::SaleNotices,
        source_snapshot_id: "20990102T030405Z".to_owned(),
        path,
    };
    let refused = read_object(&stage, &source, &object, chrono::Utc::now());
    assert!(
        refused.is_err(),
        "a notice detail without its key is refused"
    );
    Ok(())
}

#[test]
fn a_report_read_without_its_key_refuses_the_object() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    let path = directory.path().join("keyless.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&path)?,
        flate2::Compression::default(),
    );
    gzip.write_all(b"{\"request\": {\"case_no\": \"20990130000001\"}, \"outcome\": \"failed\"}\n")?;
    gzip.finish()?;
    let object = PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        role: Role::AppraisalReports,
        source_snapshot_id: "20990102T030405Z".to_owned(),
        path,
    };
    let refused = read_object(&stage, &source, &object, chrono::Utc::now());
    assert!(refused.is_err(), "a read without its key is refused");
    Ok(())
}

#[test]
fn each_role_writes_its_tables_and_the_contract_names_its_source() -> anyhow::Result<()> {
    let source = CourtAuctionSource::compiled()?;
    for role in ROLES {
        assert!(source.source_slug(role)?.starts_with("courtauctiongokr__"));
        for (table, keep) in role.tables() {
            // A table more than one role writes (the 사건 문서' read record, the case) names one of them.
            assert!(Role::of_table(table)?
                .tables()
                .iter()
                .any(|(name, _)| name == table));
            let contract = contract(table)?;
            // A changed-keys table keeps its newest row per key; an editions table appends whole
            // lists; a read record keeps every row.
            match keep {
                Keep::Changed => assert!(contract.current_row_latest().is_some(), "{table}"),
                Keep::Editions => assert!(
                    matches!(contract.row_versions, LakehouseRowVersions::Editions { .. }),
                    "{table}"
                ),
                Keep::Every => assert!(
                    table.ends_with("_read") || *table == "silver.court_auction_item_detail",
                    "{table}"
                ),
            }
        }
    }
    assert!(Role::of_table("silver.court_auction_item").is_err());
    Ok(())
}

#[test]
fn a_plan_names_its_table_and_an_object_two_plans_name_is_loaded_once() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let slug = source.source_slug(Role::DividendNotices)?;
    for table in [
        "silver.court_auction_dividend_notice",
        "silver.court_auction_dividend_object",
    ] {
        let plan_dir = directory.path().join(table);
        std::fs::create_dir_all(&plan_dir)?;
        std::fs::write(
            plan_dir.join("plan.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema_version": PLAN_SCHEMA, "table": table,
                "objects": [{"object_key": OBJECT_KEY, "source_slug": slug, "local_path": "objects/a.gz"}],
            }))?,
        )?;
    }
    let dirs = vec![
        directory
            .path()
            .join("silver.court_auction_dividend_notice"),
        directory
            .path()
            .join("silver.court_auction_dividend_object"),
    ];
    let objects = planned_objects(&dirs, &source)?;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].role, Role::DividendNotices);
    // An object of another source does not load into the plan's table.
    std::fs::write(
        dirs[0].join("plan.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema_version": PLAN_SCHEMA, "table": "silver.court_auction_dividend_notice",
            "objects": [{"object_key": OBJECT_KEY, "source_slug": "courtauctiongokr__auction_item", "local_path": "a.gz"}],
        }))?,
    )?;
    assert!(planned_objects(&dirs[..1], &source).is_err());
    Ok(())
}

#[test]
fn the_contract_names_the_tables_the_roles_write() -> anyhow::Result<()> {
    let contract: serde_json::Value =
        serde_json::from_str(super::SOURCE_CONTRACT).context("the source contract")?;
    let reports = Role::AppraisalReports.tables();
    assert_eq!(contract["appraisal_reports"]["reports_from"], reports[0].0);
    assert_eq!(contract["appraisal_reports"]["reads_from"], reports[1].0);
    assert_eq!(
        contract["sale_specifications"]["reads_from"],
        Role::SaleSpecifications.tables()[0].0
    );
    assert_eq!(
        contract["dividend_notices"]["objects_from"],
        Role::DividendNotices.tables()[1].0
    );
    // The 감정평가서 index is one the media reconcile reads; the 매각물건명세서 PDFs, which name
    // people, are outside the served media store (root ADR-0575 §3).
    let onbid: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../infra/lakehouse/contracts/data-go-kr-onbid.contract.json"
    ))?;
    let index = onbid["media"]["reconcile"]["index_tables"]
        .as_array()
        .context("the media index tables")?;
    assert!(index.contains(&serde_json::json!(reports[0].0)));
    assert!(!index.contains(&serde_json::json!(Role::SaleSpecifications.tables()[0].0)));
    let served = onbid["media"]["object_prefix"]
        .as_str()
        .context("the media prefix")?;
    assert!(!contract["sale_specifications"]["object_prefix"]
        .as_str()
        .context("the specification prefix")?
        .starts_with(served));
    Ok(())
}
