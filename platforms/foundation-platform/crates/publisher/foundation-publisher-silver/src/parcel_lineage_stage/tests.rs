//! The derivation's rows are the rows the Spark job's kernels derived (root ADR-0559 §3).
//!
//! `fixtures/inputs.json` is a synthetic group (every PNU under sido `99`): both editions' parcels,
//! code change rows and land movement history that reach each rule the schedule applies — a 동
//! renamed in the window, a pair a supersession withdraws and the official pair that replaces it,
//! a steward's pair, a split 동, a pair effective after the window, merge, split and conversion
//! texts (one with a lot the rule cannot use, one outside the window, one reported twice), and
//! jurisdiction transfers that area and category single out, tie, or cannot place.
//! `fixtures/spark_rows.txt` is what `parcel_lineage_to_silver.derive` and
//! `parcel_lineage.cardinality` returned for it before the job was removed, one line per row,
//! sorted; `fixtures/summary.json` its reconciliation and counts. The engine-equality verdict the
//! move was made on compared `derive.sql` with production's rows of sixteen groups; this keeps
//! the binary on those rules.

use anyhow::Context;
use chrono::{DateTime, Utc};
use duckdb::params;
use foundation_publisher_core::lakehouse_data_commit::{TableField, TableShape};
use lakehouse_domain::industrial_complex_lakehouse_contracts;
use serde_json::Value;

use super::{
    derivation_run_id, in_sido, stage_group, LineageGroup, LineagePins, UnrecordedDongCodes,
    LINEAGE_TABLE,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

const INPUTS: &str = include_str!("fixtures/inputs.json");
const SPARK_ROWS: &str = include_str!("fixtures/spark_rows.txt");
const SUMMARY: &str = include_str!("fixtures/summary.json");
const LINE: &str = "concat_ws('|', coalesce(predecessor_pnu, 'NULL'), successor_pnu, relation, \
     coalesce(cardinality, 'NULL'), coalesce(effective_date, 'NULL'), grade, evidence_kind, \
     coalesce(evidence_ref, 'NULL'))";
const PINS: LineagePins = LineagePins {
    boundaries: 11,
    code_changes: 12,
    history: 13,
};

fn shape() -> anyhow::Result<TableShape> {
    let contract = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == LINEAGE_TABLE)
        .context("the lineage contract")?;
    Ok(TableShape {
        location: String::new(),
        fields: contract
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Ok(TableField {
                    id: i32::try_from(index + 1)?,
                    name: column.name.to_owned(),
                    required: column.required,
                })
            })
            .collect::<anyhow::Result<_>>()?,
    })
}

fn derived_at() -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp(1_794_441_600, 0).context("2026-11-12T00:00:00Z")
}

fn text(row: &Value, name: &str) -> Option<String> {
    row[name].as_str().map(str::to_owned)
}

/// The fixture's group, and its inputs loaded as `inputs.sql` leaves them; `drop_after` leaves one
/// parcel out of the later edition.
fn fixture(stage: &DuckDbParquetStage, drop_after: Option<&str>) -> anyhow::Result<LineageGroup> {
    let inputs: Value = serde_json::from_str(INPUTS)?;
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE TABLE before_raw (pnu VARCHAR); CREATE TABLE after_raw (pnu VARCHAR); \
         CREATE TABLE code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
         effective_date VARCHAR, source VARCHAR); \
         CREATE TABLE history (pnu VARCHAR, reason_code VARCHAR, reason VARCHAR, moved_at VARCHAR, \
         erased_at VARCHAR, land_category_code VARCHAR, area_m2 DOUBLE, transfer_history_seq BIGINT)",
    )?;
    for (table, key) in [("before_raw", "before"), ("after_raw", "after")] {
        let mut insert = connection.prepare(&format!("INSERT INTO {table} VALUES (?)"))?;
        for pnu in inputs[key].as_array().context("a list of parcels")? {
            let pnu = pnu.as_str().context("a parcel")?;
            if table == "after_raw" && drop_after == Some(pnu) {
                continue;
            }
            insert.execute(params![pnu])?;
        }
    }
    let mut insert = connection.prepare("INSERT INTO code_changes VALUES (?, ?, ?, ?, ?, ?)")?;
    for row in inputs["code_changes"].as_array().context("code changes")? {
        insert.execute(params![
            text(row, "kind"),
            text(row, "old_code"),
            text(row, "new_code"),
            text(row, "level"),
            text(row, "effective_date"),
            text(row, "source"),
        ])?;
    }
    let mut insert = connection.prepare("INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)")?;
    for row in inputs["history"].as_array().context("history")? {
        insert.execute(params![
            text(row, "pnu"),
            text(row, "reason_code"),
            text(row, "reason"),
            text(row, "moved_at"),
            text(row, "erased_at"),
            text(row, "land_category_code"),
            row["area_m2"].as_f64(),
            row["transfer_history_seq"].as_i64(),
        ])?;
    }
    Ok(LineageGroup {
        from_snapshot_id: "synthetic__parcel-209906".to_owned(),
        to_snapshot_id: "synthetic__parcel-209909".to_owned(),
        from_date: text(&inputs, "from_date").context("from_date")?,
        to_date: text(&inputs, "to_date").context("to_date")?,
        from_sido: vec!["99".to_owned()],
        to_sido: vec!["99".to_owned()],
    })
}

#[test]
fn the_staged_rows_are_the_rows_the_spark_kernels_derived() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    let group = fixture(&stage, None)?;
    let staged = stage_group(&stage, &shape()?, &group, PINS, derived_at()?)?;

    let expected = SPARK_ROWS.lines().collect::<Vec<_>>();
    assert_eq!(staged.rows, u64::try_from(expected.len())?);
    assert_eq!(staged.derivation_run_id, derivation_run_id(&group, PINS)?);
    let file = staged.file.context("a staged file")?;
    assert_eq!(file.name, staged.derivation_run_id);
    let path = file
        .local_path
        .to_str()
        .context("UTF-8")?
        .replace('\\', "/");
    let lines = stage
        .connection()
        .prepare(&format!(
            "SELECT {LINE} FROM read_parquet('{path}') ORDER BY 1"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(lines, expected);
    // Every row names its editions, rules and run.
    let named: i64 = stage.connection().query_row(
        &format!(
            "SELECT count(*) FROM read_parquet('{path}') WHERE from_snapshot_id = 'synthetic__parcel-209906' \
             AND to_snapshot_id = 'synthetic__parcel-209909' AND rules_version = 'parcel-lineage.v1' \
             AND derivation_run_id = '{}'",
            staged.derivation_run_id
        ),
        [],
        |row| row.get(0),
    )?;
    assert_eq!(u64::try_from(named)?, staged.rows);

    let summary: Value = serde_json::from_str(SUMMARY)?;
    let found = serde_json::to_value(&staged.summary)?;
    for part in ["reconciliation", "links_by_relation", "links_by_grade"] {
        assert_eq!(found[part], summary[part], "{part}");
    }
    // The one parcel whose last day holds two sets of facts: the history's sequence orders them.
    assert_eq!(staged.summary.same_day_facts, 1);
    Ok(())
}

#[test]
fn a_group_whose_code_change_table_is_behind_is_refused_with_its_codes() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    // The 동 the transferred lots left keeps one lot; without it the code held parcels before and
    // none after, and no pair of the window names it.
    let group = fixture(&stage, Some("9999950100100010000"))?;
    let Err(refused) = stage_group(&stage, &shape()?, &group, PINS, derived_at()?) else {
        anyhow::bail!("a vanished 동 code the table does not pair must be refused");
    };
    let codes = refused
        .downcast_ref::<UnrecordedDongCodes>()
        .context("the refusal names its codes")?;
    assert_eq!(codes.codes, ["9999950100"]);
    assert!(
        refused.to_string().contains("Record the change"),
        "{refused}"
    );
    Ok(())
}

#[test]
fn the_run_id_is_the_one_the_spark_job_gave_the_same_inputs() -> anyhow::Result<()> {
    // A group production derived on 2026-10-16 with the Spark job: its editions, dates, sido and the
    // Iceberg snapshots its inputs were pinned at. The rows it appended carry this run id.
    let group = LineageGroup {
        from_snapshot_id: "vworldkr__parcel-202606".to_owned(),
        to_snapshot_id: "vworldkr__parcel-202609".to_owned(),
        from_date: "2026-06-01".to_owned(),
        to_date: "2026-09-01".to_owned(),
        from_sido: vec!["30".to_owned()],
        to_sido: vec!["30".to_owned()],
    };
    let pins = LineagePins {
        boundaries: 6_498_754_906_780_223_795,
        code_changes: 4_923_715_983_584_928_762,
        history: 755_692_870_280_159_514,
    };
    assert_eq!(
        derivation_run_id(&group, pins)?,
        "parcel-lineage-26a0f1dd6bff3742d25d1e90"
    );
    // The prefixes are a set: their order and repeats do not make another run.
    let renumbered = |from: &[&str]| LineageGroup {
        from_sido: from.iter().map(|&prefix| prefix.to_owned()).collect(),
        to_sido: vec!["12".to_owned()],
        ..group.clone()
    };
    assert_eq!(
        derivation_run_id(&renumbered(&["46", "29", "46"]), pins)?,
        derivation_run_id(&renumbered(&["29", "46"]), pins)?
    );
    Ok(())
}

#[test]
fn a_sido_predicate_is_a_range_per_prefix_and_refuses_anything_else() -> anyhow::Result<()> {
    assert_eq!(
        in_sido("pnu", &["46".to_owned(), "29".to_owned()])?,
        "((pnu >= '29' AND pnu < '29:' AND substr(pnu, 1, 2) = '29') OR \
         (pnu >= '46' AND pnu < '46:' AND substr(pnu, 1, 2) = '46'))"
    );
    for refused in [vec![], vec!["1".to_owned()], vec!["1' OR '1".to_owned()]] {
        assert!(in_sido("pnu", &refused).is_err(), "{refused:?}");
    }
    Ok(())
}
