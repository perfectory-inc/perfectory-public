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
use crate::duckdb_parquet_stage::{sql_path, DuckDbParquetStage};
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
        &object(stage.directory(), role)?,
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
        // The load's result checks hold on the rows each role builds.
        let checks =
            crate::silver_changes::result_checks(&stage, contract(table.table)?, &table.staged)?;
        assert_eq!(
            (checks.rows, checks.duplicate_keys, checks.missing_required),
            (table.rows, 0, 0),
            "{}",
            table.table
        );
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
    let path = stage.directory().join("keyless.gz");
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
    let path = stage.directory().join("keyless.gz");
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
    let path = stage.directory().join("keyless.gz");
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
                Keep::Changed | Keep::Removals => {
                    assert!(contract.current_row_latest().is_some(), "{table}");
                }
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
    assert!(Role::of_table("silver.onbid_real_estate_item").is_err());
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
fn a_plan_of_the_case_read_record_loads_each_object_as_its_sources_document() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let plan_dir = directory.path().join("cases");
    std::fs::create_dir_all(&plan_dir)?;
    let survey = OBJECT_KEY.replace("ca-2099", "survey-2099");
    std::fs::write(
        plan_dir.join("plan.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema_version": PLAN_SCHEMA, "table": "silver.court_auction_case_document_read",
            "objects": [
                {"object_key": OBJECT_KEY, "source_slug": source.source_slug(Role::CaseDetail)?,
                 "local_path": "objects/a.gz"},
                {"object_key": survey, "source_slug": source.source_slug(Role::CaseSurvey)?,
                 "local_path": "objects/b.gz"},
            ],
        }))?,
    )?;
    let objects = planned_objects(&[plan_dir], &source)?;
    assert_eq!(
        objects
            .iter()
            .map(|object| (object.object_key.as_str(), object.role))
            .collect::<Vec<_>>(),
        [
            (OBJECT_KEY, Role::CaseDetail),
            (survey.as_str(), Role::CaseSurvey)
        ]
    );
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

/// The expected rows of a fixture with a `# a|b|…` header: (columns, rows), the rows sorted.
fn expected_with_header(name: &str) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let text = fixture(name)?;
    let mut lines = text.lines();
    let header = lines
        .next()
        .and_then(|line| line.strip_prefix("# "))
        .context("a header line")?;
    let mut rows = lines.map(str::to_owned).collect::<Vec<_>>();
    rows.sort();
    Ok((header.split('|').map(str::to_owned).collect(), rows))
}

/// `query`'s rows as the fixtures write them: each column as text, `NULL` for none, sorted.
fn rows_as_text(
    stage: &DuckDbParquetStage,
    columns: &[String],
    from: &str,
) -> anyhow::Result<Vec<String>> {
    let parts = columns
        .iter()
        .map(|column| format!("coalesce(CAST({column} AS VARCHAR), 'NULL')"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut found = stage
        .connection()
        .prepare(&format!("SELECT concat_ws('|', {parts}) FROM {from}"))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    found.sort();
    Ok(found)
}

/// A stage with the court readers and the synthetic place-identity reference of the place stage's
/// own test (합성도 99, 합성도 가상시 가나동 9999910100).
fn search_stage() -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage, CourtAuctionSource)> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    stage.connection().execute_batch(&format!(
        "CREATE TABLE place_code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
         effective_date VARCHAR, source VARCHAR);
         CREATE TABLE place_dong_names AS SELECT * FROM (VALUES
           ('9900000000', '합성도', '존재'), ('9999910100', '합성도 가상시 가나동', '존재')) t(region_cd, full_name, status);
         {}",
        crate::place_identity_stage::REFERENCE_SQL
    ))?;
    Ok((directory, stage, CourtAuctionSource::compiled()?))
}

#[test]
fn the_search_rows_and_their_offers_are_the_spark_jobs() -> anyhow::Result<()> {
    let (directory, stage, source) = search_stage()?;
    let staged = read_object(
        &stage,
        &source,
        &object(stage.directory(), Role::ItemSearch)?,
        chrono::Utc::now(),
    )?;
    assert_eq!(staged[0].rows, 5);
    let (columns, rows) = expected_with_header("spark_item_search__ca_search_rows.txt")?;
    assert_eq!(rows_as_text(&stage, &columns, "ca_search_rows")?, rows);
    super::offer_search(
        &stage,
        Role::ItemSearch,
        contract("silver.court_auction_item")?,
    )?;
    let (columns, rows) = expected_with_header("spark_item_search__offers.txt")?;
    let renamed = columns
        .iter()
        .map(|column| match column.as_str() {
            "legal_dong_code" => "jibun_code".to_owned(),
            // The offer's road key, not the row's raw road name.
            "road_name_key" => "place_offers.road_name".to_owned(),
            other => other.to_owned(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows_as_text(
            &stage,
            &renamed,
            "place_offers JOIN ca_place_first USING (_row)"
        )?,
        rows
    );
    Ok(())
}

#[test]
fn a_search_row_takes_the_code_first_then_its_lot_then_its_road() -> anyhow::Result<()> {
    let (directory, stage, source) = search_stage()?;
    read_object(
        &stage,
        &source,
        &object(stage.directory(), Role::ItemSearch)?,
        chrono::Utc::now(),
    )?;
    let table = contract("silver.court_auction_item")?;
    super::offer_search(&stage, Role::ItemSearch, table)?;
    stage.connection().execute_batch(
        "CREATE OR REPLACE TABLE place_parcels AS SELECT * FROM (VALUES ('9999910100100070000')) t(pnu);
         CREATE OR REPLACE TABLE place_titles AS SELECT * FROM (VALUES
           ('9999910100100070000', '합성도 가상시 가나대로 3')) t(pnu, road_address_raw);",
    )?;
    super::finish_search(&stage, table)?;
    let found = rows_as_text(
        &stage,
        &[
            "case_no".to_owned(),
            "pnu".to_owned(),
            "pnu_basis".to_owned(),
            "pnu_candidate_count".to_owned(),
            "legal_dong_code".to_owned(),
        ],
        "ca_court_auction_item",
    )?;
    assert_eq!(
        found,
        [
            "20990130000001|9999910100100070000|legal_dong_code|1|9999910100",
            "20990130000002|NULL|NULL|NULL|9999910100",
            "20990130000003|9999910100100070000|jibun_address|1|9999910100",
            "20990130000004|NULL|NULL|NULL|NULL",
            "20990130000007|9999910100100070000|road_address|1|NULL",
        ]
    );
    let reason: String = stage.connection().query_row(
        "SELECT pnu_unresolved_reason FROM ca_court_auction_item WHERE case_no = '20990130000004'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(reason, "no_legal_dong");
    Ok(())
}

#[test]
fn a_search_is_complete_as_the_spark_job_judged_it() -> anyhow::Result<()> {
    // One stage with only the value readers, and each case's lines only (`search_complete` reads
    // their pages): a stage per case with the search rows and the place-identity reference took over
    // 30 s in the dev profile's unoptimized DuckDB and timed out beside other tests (2026-11-23).
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    let cases: serde_json::Value = serde_json::from_str(&fixture("item_search_complete.json")?)?;
    for case in cases.as_array().context("cases")? {
        let path = stage.directory().join("case.jsonl");
        let lines = case["lines"]
            .as_array()
            .context("lines")?
            .iter()
            .map(|line| line.to_string() + "\n")
            .collect::<String>();
        std::fs::write(&path, lines)?;
        stage.connection().execute_batch(&super::search_pages_sql(
            &source,
            Role::ItemSearch,
            &sql_path(&path)?,
        )?)?;
        assert_eq!(
            super::search_complete(&stage, &source, Role::ItemSearch)?,
            case["complete"].as_bool().context("complete")?,
            "{}",
            case["case"]
        );
    }
    Ok(())
}

/// A synthetic object of `role` as large as a night's: `count` lines, each the fixture's line at
/// `template` with every list of its answer `times` as long and its own case number.
fn large_object(
    directory: &std::path::Path,
    role: Role,
    template: usize,
    count: usize,
    times: usize,
) -> anyhow::Result<PlannedObject> {
    let text = fixture(&format!("{}.jsonl", role.name()))?;
    let mut line: serde_json::Value = serde_json::from_str(
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .nth(template)
            .context("the fixture has the template line")?,
    )?;
    let mut body: serde_json::Value =
        serde_json::from_str(line["body"].as_str().context("a body")?)?;
    let answer = if role == Role::ItemDetail {
        body.pointer_mut("/data/dma_result")
    } else {
        body.pointer_mut("/data")
    }
    .and_then(serde_json::Value::as_object_mut)
    .context("an answer")?;
    for value in answer.values_mut() {
        if let Some(list) = value.as_array_mut() {
            let once = list.clone();
            for _ in 1..times {
                list.extend(once.iter().cloned());
            }
        }
    }
    let body = body.to_string();
    let path = directory.join(format!("{}-large.gz", role.name()));
    let mut gzip =
        flate2::write::GzEncoder::new(std::fs::File::create(&path)?, flate2::Compression::fast());
    for n in 0..count {
        let case_no = format!("2099013{n:07}");
        let request = line["request"]
            .as_object_mut()
            .and_then(|request| request.values_mut().next())
            .context("a request")?;
        request["csNo"] = serde_json::Value::String(case_no.clone());
        line["body"] = serde_json::Value::String(body.replace("20990130000001", &case_no));
        gzip.write_all(format!("{line}\n").as_bytes())?;
    }
    gzip.finish()?;
    Ok(PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        role,
        source_snapshot_id: "20990102T030405Z".to_owned(),
        path,
    })
}

#[test]
fn a_case_detail_object_of_wide_answers_is_read_within_its_memory_budget() -> anyhow::Result<()> {
    // 2026-11-22: the first production run stopped on a 1,541-line 사건상세 object above the stage's
    // 2 GiB: the case row's macro read the whole answer in every expression it expands to. A
    // night-sized object takes minutes in the dev profile's unoptimized DuckDB (planning the macro
    // SQL alone is most of a fixture test), so this is 150 lines of answers as wide as that night's,
    // under 256 MiB: the query before #1646 needed more, this one fits 200 MiB (DuckDB 2.0). The
    // 물건상세 shapes were checked offline against #1646 on three production objects (#1649).
    let directory = tempfile::tempdir()?;
    let source = CourtAuctionSource::compiled()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.limit_memory(256)?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    let object = large_object(stage.directory(), Role::CaseDetail, 0, 150, 10)?;
    read_object(&stage, &source, &object, chrono::Utc::now())?;
    let cases: i64 =
        stage
            .connection()
            .query_row("SELECT count(*) FROM ca_court_auction_case", [], |row| {
                row.get(0)
            })?;
    assert_eq!(cases, 150);
    Ok(())
}

#[test]
fn a_follow_places_again_the_rows_a_code_change_can_move_with_its_record() -> anyhow::Result<()> {
    let (directory, stage, source) = search_stage()?;
    read_object(
        &stage,
        &source,
        &object(stage.directory(), Role::ItemSearch)?,
        chrono::Utc::now(),
    )?;
    let table = contract("silver.court_auction_item")?;
    super::offer_search(&stage, Role::ItemSearch, table)?;
    stage.connection().execute_batch(
        "CREATE OR REPLACE TABLE place_parcels AS SELECT * FROM (VALUES ('9999910100100070000')) t(pnu);
         CREATE OR REPLACE TABLE place_titles AS SELECT * FROM (VALUES
           ('9999910100100070000', '합성도 가상시 가나대로 3')) t(pnu, road_address_raw);",
    )?;
    super::finish_search(&stage, table)?;
    // The table's current rows: the night's, one of them since removed.
    stage.connection().execute_batch(
        "CREATE TABLE ca_current AS SELECT * REPLACE (
           CASE WHEN case_no = '20990130000004' THEN 'removed' END AS row_state) FROM ca_court_auction_item;",
    )?;
    let at = chrono::Utc
        .with_ymd_and_hms(2099, 1, 3, 4, 5, 6)
        .single()
        .context("a valid instant")?;
    let record = "follow:reference.legal_dong_code_change@7@20990103T040506Z";
    let followed = |prefix: Option<&str>| -> anyhow::Result<Vec<String>> {
        stage.connection().execute_batch(&format!(
            "CREATE OR REPLACE TABLE file_prefixes AS SELECT * FROM (VALUES ('{}')) t(prefix)",
            prefix.unwrap_or("-")
        ))?;
        super::follow_rows(
            &stage,
            "SELECT * FROM ca_current",
            prefix.is_some(),
            record,
            at,
        )?;
        rows_as_text(&stage, &["case_no".to_owned()], "ca_search_rows")
    };
    // Under no moved code only the row without a PNU; under 99999 every placed row too; the removed
    // row never; every other row when the earlier change snapshot is unknown.
    assert_eq!(followed(Some("98888"))?, ["20990130000002"]);
    let moved = [
        "20990130000001",
        "20990130000002",
        "20990130000003",
        "20990130000007",
    ];
    assert_eq!(followed(Some("99999"))?, moved);
    assert_eq!(followed(None)?, moved);
    // Placed again, a row keeps what it was observed with and carries the follow's record.
    super::offer_search(&stage, Role::ItemSearch, table)?;
    super::finish_search(&stage, table)?;
    let found = rows_as_text(
        &stage,
        &[
            "case_no".to_owned(),
            "pnu".to_owned(),
            "pnu_basis".to_owned(),
            "source_record_id".to_owned(),
            "source_snapshot_id".to_owned(),
        ],
        "ca_court_auction_item WHERE case_no = '20990130000001'",
    )?;
    assert_eq!(
        found,
        [format!(
            "20990130000001|9999910100100070000|legal_dong_code|{record}|20990103T040506Z"
        )]
    );
    Ok(())
}

/// The rows of `table` on `stage`, each as text, sorted.
fn sorted_rows(stage: &DuckDbParquetStage, table: &str) -> anyhow::Result<Vec<String>> {
    let mut found = stage
        .connection()
        .prepare(&format!("SELECT CAST(t AS VARCHAR) FROM {table} t"))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    found.sort();
    Ok(found)
}

/// The search pages as the load built them from `ca_parsed` before the typed read (2026-11-25):
/// the oracle of `a_search_reads_its_file_as_lines_sql_read_it`.
fn pages_before_typed_read(source: &CourtAuctionSource, role: Role) -> anyhow::Result<String> {
    let search = source.section(role);
    let (_, fields) = super::search_conditions(source, role)?;
    let window = search["date_window"].as_object().map(|window| {
        (
            window
                .get("start_field")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            window
                .get("end_field")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        )
    });
    let asked = "coalesce(CASE WHEN py_truthy(json_extract(j, '$.request')) \
                 THEN json_extract(j, '$.request.dma_srchGdsDtlSrchInfo') END, '{}'::JSON)";
    let window_ok = window.map_or_else(
        || "true".to_owned(),
        |(start, end)| {
            format!(
                "py_truthy(json_extract(asked, '$.{start}')) AND py_truthy(json_extract(asked, '$.{end}'))"
            )
        },
    );
    let key = fields
        .iter()
        .map(|field| format!("coalesce(CAST(json_extract(asked, '$.{field}') AS VARCHAR), 'null')"))
        .collect::<Vec<_>>()
        .join(" || '\u{1f}' || ");
    Ok(format!(
        "CREATE OR REPLACE TEMP TABLE ca_search_pages AS \
         SELECT line_number, {key} AS condition, \
                CASE WHEN json_type(json_extract(data, '$.dlt_srchResult')) = 'ARRAY' \
                     THEN json_array_length(json_extract(data, '$.dlt_srchResult')) ELSE 0 END AS rows, \
                ca_whole(json_extract(data, '$.dma_pageInfo.totalCnt')) AS total \
         FROM (SELECT line_number, j, {asked} AS asked, \
                      json_extract(TRY_CAST(json_extract_string(j, '$.body') AS JSON), '$.data') AS data \
               FROM ca_parsed \
               WHERE json_type(json_extract(j, '$.http_status')) IN ('BIGINT', 'UBIGINT', 'DOUBLE') \
                 AND TRY_CAST(json_extract(j, '$.http_status') AS DOUBLE) = 200) \
         WHERE json_type(TRY_CAST(json_extract_string(j, '$.body') AS JSON)) = 'OBJECT' AND ({window_ok})"
    ))
}

#[test]
fn a_search_reads_its_file_as_lines_sql_read_it() -> anyhow::Result<()> {
    // 2026-11-25 a search reads its file itself, typed (root ADR-0616 §3-1); its rows, its pages and
    // its completeness are the ones lines.sql and the search's first statement of that day built.
    // The other roles read their file through lines.sql, and their Spark fixtures are their oracle.
    let role = Role::ItemSearch;
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
    let object = object(stage.directory(), role)?;
    read_object(&stage, &source, &object, ingested_at)?;
    let typed = (
        sorted_rows(&stage, "ca_search_rows")?,
        sorted_rows(&stage, "ca_search_pages")?,
        super::search_complete(&stage, &source, role)?,
    );
    stage.load_lines(&object.path, "ca_lines")?;
    stage
        .connection()
        .execute_batch(&fixture("before_typed_read/lines.sql")?)?;
    stage
        .connection()
        .execute_batch(&fixture("before_typed_read/search_lines.sql")?)?;
    let role_sql = source.role_sql(role)?;
    let rows = &role_sql[role_sql
        .find("CREATE OR REPLACE TEMP TABLE ca_search_rows")
        .context("searches.sql builds its rows")?..];
    stage.connection().execute_batch(
        &rows
            .replace("{object_key}", super::literal(&object.object_key)?)
            .replace("{snapshot_id}", super::literal(&object.source_snapshot_id)?)
            .replace(
                "{ingested_at_epoch_seconds}",
                &ingested_at.timestamp().to_string(),
            ),
    )?;
    stage
        .connection()
        .execute_batch(&pages_before_typed_read(&source, role)?)?;
    let before = (
        sorted_rows(&stage, "ca_search_rows")?,
        sorted_rows(&stage, "ca_search_pages")?,
        super::search_complete(&stage, &source, role)?,
    );
    assert!(!typed.0.is_empty(), "the fixture stages rows");
    assert_eq!(typed, before);
    Ok(())
}

#[test]
fn an_object_larger_than_the_stage_holds_for_its_role_is_refused_with_the_reason(
) -> anyhow::Result<()> {
    // The read refuses before it starts rather than running out of memory part way (root ADR-0616
    // §3-1): here the role's measured figure is so large that any object is above the limit.
    let role = Role::CaseDetail;
    let directory = tempfile::tempdir()?;
    let mut source = CourtAuctionSource::compiled()?;
    source.contract["silver_load"]["memory_per_text_byte"][role.name()] =
        serde_json::json!(u64::MAX / 4);
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())?;
    let Err(refused) = read_object(
        &stage,
        &source,
        &object(stage.directory(), role)?,
        chrono::Utc::now(),
    ) else {
        anyhow::bail!("an object above the stage's limit was read");
    };
    let refused = format!("{refused:#}");
    assert!(
        refused.contains("bytes of memory per byte of text"),
        "{refused}"
    );
    Ok(())
}

#[test]
fn only_a_line_that_is_not_json_is_called_one() -> anyhow::Result<()> {
    // 2026-11-24 21:35Z the load reported running out of memory as "a line that is not JSON".
    let failure = |error: duckdb::Error| {
        format!(
            "{:#}",
            super::read_failure(error, "k", "DuckDB failed to read the lines of")
        )
    };
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let read = |lines: &str| -> anyhow::Result<duckdb::Error> {
        let path = stage.directory().join("lines.jsonl");
        std::fs::write(&path, lines)?;
        stage
            .connection()
            .execute_batch(&format!(
                "CREATE OR REPLACE TABLE r AS SELECT * FROM read_json('{}', \
                 format = 'newline_delimited', columns = {{a: 'JSON'}})",
                sql_path(&path)?
            ))
            .err()
            .context("the read is refused")
    };
    let malformed = failure(read("{\"a\": 1}\nnot json\n")?);
    assert!(
        malformed.starts_with("k holds a line that is not JSON"),
        "{malformed}"
    );
    let not_an_object = failure(read("{\"a\": 1}\n[1, 2]\n")?);
    assert!(
        not_an_object.starts_with("k holds a line that is not a JSON object"),
        "{not_an_object}"
    );
    let other = failure(
        stage
            .connection()
            .execute_batch("SELECT * FROM no_such_table")
            .err()
            .context("an absent table is refused")?,
    );
    assert!(
        other.starts_with("DuckDB failed to read the lines of k"),
        "{other}"
    );
    assert!(!other.contains("not JSON"), "{other}");
    Ok(())
}
