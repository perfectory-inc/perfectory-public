//! `fixtures/object.jsonl` is a synthetic object (notice ids `999…`, a fake host, 2099 dates) with
//! one line of each source and the cases a read may hold: files stored, already stored, withdrawn
//! and failed; a failed read; a time to the nanosecond; a size with a comma; and the same notice
//! read twice in one object with a hash both reads name.

use std::io::Write as _;

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{TableField, TableShape};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use serde_json::Value;

use super::{
    detail_source_slugs, planned_objects, stage_object, PlannedObject, ATTACHMENT_TABLE,
    FETCH_ATTEMPT_TABLE, PLAN_SCHEMA,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

const OBJECT: &str = include_str!("fixtures/object.jsonl");
const KEY: &str = "bronze/source=industrylandorkr__industrial_land_sale_notice_detail/industrial-land-sale-notice-detail-000--sha256-0000000000000000000000000000000000000000000000000000000000000000.gz";

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

fn object(directory: &std::path::Path, lines: &str) -> anyhow::Result<PlannedObject> {
    let path = directory.join("object.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&path)?,
        flate2::Compression::default(),
    );
    gzip.write_all(lines.as_bytes())?;
    gzip.finish()?;
    Ok(PlannedObject {
        object_key: KEY.to_owned(),
        source_snapshot_id: "20990102T030405Z".to_owned(),
        path,
    })
}

fn rows(stage: &DuckDbParquetStage, query: &str) -> anyhow::Result<Vec<String>> {
    Ok(stage
        .connection()
        .prepare(query)?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?)
}

#[test]
fn every_source_s_line_stages_its_read_and_its_stored_files() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    let staged = stage_object(
        &stage,
        (&shape(FETCH_ATTEMPT_TABLE)?, &shape(ATTACHMENT_TABLE)?),
        contract(ATTACHMENT_TABLE)?,
        &object(root.path(), OBJECT)?,
        None,
        chrono::DateTime::from_timestamp(4_070_908_800, 0).context("an instant")?,
    )?;
    assert_eq!(
        (staged.reads, staged.attachments, staged.changed_attachments),
        (4, 3, 3)
    );
    assert!(staged.reads_file.is_some() && staged.attachments_file.is_some());
    assert_eq!(
        rows(
            &stage,
            "SELECT concat_ws('|', notice_source, notice_id, list_fingerprint, \
             strftime(make_timestamp(epoch_us(requested_at_utc)), '%Y-%m-%d %H:%M:%S.%f'), \
             coalesce(CAST(http_status AS VARCHAR), 'NULL'), read_outcome, \
             coalesce(CAST(file_count AS VARCHAR), 'NULL'), coalesce(CAST(files_stored AS VARCHAR), 'NULL'), \
             source_line_number, source_snapshot_id) FROM sale_reads ORDER BY source_line_number"
        )?,
        [
            "ilis|99901|f1|2099-01-02 03:04:05.123456|200|read|3|2|1|20990102T030405Z",
            "kicox|99902|f2|2099-01-02 03:05:00.000000|200|incomplete|2|1|2|20990102T030405Z",
            "lh|9999999999|f3|2099-01-02 03:06:00.000000|NULL|failed|NULL|NULL|3|20990102T030405Z",
            "ilis|99901|f1b|2099-01-02 04:00:00.000000|200|read|1|1|4|20990102T030405Z",
        ]
    );
    let (a, b, c) = ("a".repeat(64), "b".repeat(64), "c".repeat(64));
    assert_eq!(
        rows(
            &stage,
            "SELECT concat_ws('|', notice_source, notice_id, sha256, file_sequence, file_title, \
             provider_file_id, coalesce(CAST(provider_file_size AS VARCHAR), 'NULL'), media_format, size_bytes, \
             strftime(make_timestamp(epoch_us(fetched_at_utc)), '%Y-%m-%d %H:%M:%S'), source_line_number) \
             FROM sale_attachments_changed ORDER BY notice_source, notice_id, file_sequence"
        )?,
        [
            // The second read of the notice names the hash again: its row is the one kept.
            format!("ilis|99901|{a}|1|붙임01. 공고문(정정)|u1|1234|pdf|1234|2099-01-02 04:00:01|4"),
            // No fetch time: the read's.
            format!("ilis|99901|{b}|2|붙임02. 리플렛|u2|20|pdf|20|2099-01-02 03:04:05|1"),
            format!(
                "kicox|99902|{c}|1|01. 분양공고문.hwp [hwp, 1 KB]|99999/uuid-1|NULL|hwp|999|2099-01-02 03:05:01|2"
            ),
        ]
    );
    Ok(())
}

#[test]
fn a_line_the_collector_does_not_write_is_refused() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    for refused in [
        r#"{"notice_source":"ilis","list_fingerprint":"f","requested_at_utc":"2099-01-02T00:00:00Z"}"#,
        r#"{"notice_source":"ilis","notice_id":"1","list_fingerprint":"f","requested_at_utc":"2099-01-02T00:00:00Z","attachments":{"a":1}}"#,
        r#"{"notice_source":"ilis","notice_id":"1","list_fingerprint":"f","requested_at_utc":"2099-01-02T00:00:00Z","attachments":[1]}"#,
    ] {
        let staged = stage_object(
            &stage,
            (&shape(FETCH_ATTEMPT_TABLE)?, &shape(ATTACHMENT_TABLE)?),
            contract(ATTACHMENT_TABLE)?,
            &object(root.path(), refused)?,
            None,
            chrono::Utc::now(),
        );
        assert!(staged.is_err(), "{refused}");
    }
    Ok(())
}

#[test]
fn the_load_reads_one_detail_source_per_source_of_the_lane() -> anyhow::Result<()> {
    assert_eq!(
        detail_source_slugs()?,
        [
            "factoryongokr__industrial_land_sale_notice_detail",
            "industrylandorkr__industrial_land_sale_notice_detail",
            "kicoxorkr__industrial_land_sale_notice_detail",
            "datagokr__korea_land_housing_land_notice_detail",
        ]
    );
    Ok(())
}

#[test]
fn a_plan_is_read_oldest_run_first_and_refuses_what_is_not_this_loads() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let slugs = detail_source_slugs()?;
    // Run-free keys (root ADR-0626 G5): the run is the ledger's collected_at the plan carries.
    let key = |slug: &str, run: &str| {
        format!("bronze/source={slug}/industrial-land-sale-notice-detail-000--sha256-{run:0<64}.gz")
    };
    let collected = |run: &str| format!("{}-{}-{}T00:00:00Z", &run[..4], &run[4..6], &run[6..8]);
    let write = |objects: Value| -> anyhow::Result<()> {
        std::fs::write(
            root.path().join("plan.json"),
            serde_json::to_vec(
                &serde_json::json!({"schema_version": PLAN_SCHEMA, "objects": objects}),
            )?,
        )?;
        Ok(())
    };
    let entry = |slug: &str, run: &str, local: &str| serde_json::json!({"object_key": key(slug, run), "source_slug": slug, "local_path": local, "collected_at": collected(run)});
    write(serde_json::json!([
        entry(&slugs[2], "20990103", "objects/b.gz"),
        entry(&slugs[1], "20990102", "objects/a.gz"),
    ]))?;
    let objects = planned_objects(root.path(), &slugs)?;
    assert_eq!(
        objects
            .iter()
            .map(|o| o.path.file_name())
            .collect::<Vec<_>>(),
        ["a.gz", "b.gz"].map(|name| Some(std::ffi::OsStr::new(name)))
    );
    write(serde_json::json!([entry(
        "industrylandorkr__industrial_complex_notice_detail",
        "20990102",
        "objects/a.gz"
    )]))?;
    assert!(planned_objects(root.path(), &slugs).is_err());
    write(serde_json::json!([entry(&slugs[0], "20990102", "../a.gz")]))?;
    assert!(planned_objects(root.path(), &slugs).is_err());
    let plain = format!(
        "bronze/source={}/industrial-land-sale-notice-detail-000.gz",
        slugs[0]
    );
    write(
        serde_json::json!([{"object_key": plain, "source_slug": slugs[0], "local_path": "objects/a.gz", "collected_at": collected("20990102")}]),
    )?;
    assert!(
        planned_objects(root.path(), &slugs).is_err(),
        "a detail object's key names its bytes (root ADR-0152)"
    );
    Ok(())
}
