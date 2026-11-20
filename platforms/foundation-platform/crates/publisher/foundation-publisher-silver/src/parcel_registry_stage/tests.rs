//! The registry step gives the answers the Spark job's Python gave (root ADR-0113, ADR-0490 §4a).
//!
//! `fixtures/python_answers.json` is what `parcel_identity` (the id, the fold, advance, upgrade),
//! `parcel_registry_frames.advance_groups` and `lineage_review_queue` (a steward's fingerprint and
//! resolution) made of the cases `test_parcel_registry_frames.py` and `test_lineage_review_queue.py`
//! held, written before the job was removed. Reserved 99999 codes and 2099 dates only.

use std::collections::BTreeSet;

use anyhow::Context as _;
use serde_json::Value;

use super::{
    advance_groups, fingerprint, resolve_stewarded, run_id, run_step, snapshot_ids_through, step,
    steward_resolved, LineageRow, Mode, StepRequest,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

type RegistryRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn answers() -> anyhow::Result<Value> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/parcel_registry_stage/fixtures/python_answers.json");
    Ok(serde_json::from_str(&std::fs::read_to_string(&path)?)?)
}

fn text(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

fn list<'a>(value: &'a Value, what: &str) -> anyhow::Result<&'a Vec<Value>> {
    value
        .as_array()
        .with_context(|| format!("{what} is a list"))
}

fn registry_rows(value: &Value) -> anyhow::Result<BTreeSet<RegistryRow>> {
    list(value, "registry rows")?
        .iter()
        .map(|row| {
            Ok((
                text(&row[0]).context("parcel id")?,
                text(&row[1]).context("pnu")?,
                text(&row[2]).context("status")?,
                text(&row[3]),
                text(&row[4]),
                text(&row[5]),
            ))
        })
        .collect()
}

/// A stage with the registry's readers and the three input tables, empty.
fn stage() -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage)> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(step("macros")?)?;
    stage.connection().execute_batch(
        "CREATE TEMP TABLE registry_region (parcel_id VARCHAR, pnu VARCHAR, status VARCHAR, valid_from VARCHAR, \
         valid_to VARCHAR, redirect_to VARCHAR, to_snapshot_id VARCHAR);
         CREATE TEMP TABLE registry_after (pnu VARCHAR);
         CREATE TEMP TABLE lineage_pair (predecessor_pnu VARCHAR, successor_pnu VARCHAR, relation VARCHAR, grade VARCHAR, \
         evidence_kind VARCHAR, evidence_ref VARCHAR, derived_at_utc TIMESTAMPTZ, from_snapshot_id VARCHAR, \
         to_snapshot_id VARCHAR);",
    )?;
    Ok((directory, stage))
}

fn put_registry(stage: &DuckDbParquetStage, rows: &BTreeSet<RegistryRow>) -> anyhow::Result<()> {
    let mut appender = stage.connection().appender("registry_region")?;
    for (parcel_id, pnu, status, valid_from, valid_to, redirect_to) in rows {
        appender.append_row(duckdb::params![
            parcel_id,
            pnu,
            status,
            valid_from,
            valid_to,
            redirect_to,
            "synthetic__parcel-209906"
        ])?;
    }
    appender.flush()?;
    Ok(())
}

fn put_after(stage: &DuckDbParquetStage, pnus: &Value) -> anyhow::Result<()> {
    let mut appender = stage.connection().appender("registry_after")?;
    for pnu in list(pnus, "the edition's parcels")? {
        appender.append_row(duckdb::params![text(pnu)])?;
    }
    appender.flush()?;
    Ok(())
}

/// Links as (predecessor, successor, relation, grade, evidence kind), of one derivation.
fn put_links(stage: &DuckDbParquetStage, links: &Value) -> anyhow::Result<()> {
    let mut appender = stage.connection().appender("lineage_pair")?;
    for link in list(links, "links")? {
        appender.append_row(duckdb::params![
            text(&link[0]),
            text(&link[1]),
            text(&link[2]),
            text(&link[3]),
            text(&link[4]),
            Option::<String>::None,
            Option::<String>::None,
            "synthetic__parcel-209906",
            "synthetic__parcel-209909"
        ])?;
    }
    appender.flush()?;
    Ok(())
}

fn staged_rows(stage: &DuckDbParquetStage, table: &str) -> anyhow::Result<BTreeSet<RegistryRow>> {
    let rows = stage
        .connection()
        .prepare(&format!(
            "SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to FROM {table}"
        ))?
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(rows)
}

fn pairs(stage: &DuckDbParquetStage, query: &str) -> anyhow::Result<Vec<Vec<String>>> {
    let rows = stage
        .connection()
        .prepare(query)?
        .query_map([], |row| Ok(vec![row.get::<_, String>(0)?, row.get(1)?]))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn expected_pairs(value: &Value) -> anyhow::Result<Vec<Vec<String>>> {
    list(value, "pairs")?
        .iter()
        .map(|pair| {
            Ok(vec![
                text(&pair[0]).context("pnu")?,
                text(&pair[1]).context("id")?,
            ])
        })
        .collect()
}

/// The lineage as the registry reads it, from `lineage_pair`.
fn carry(stage: &DuckDbParquetStage, day: &str) -> anyhow::Result<()> {
    run_step(stage, "lineage", day)?;
    resolve_stewarded(stage)?;
    run_step(stage, "carry", day)
}

#[test]
fn a_parcel_id_is_the_uuid5_python_makes() -> anyhow::Result<()> {
    let (_directory, stage) = stage()?;
    for case in list(&answers()?["ids"], "ids")? {
        let id: String = stage.connection().query_row(
            "SELECT parcel_id_for(?, ?)",
            duckdb::params![text(&case[0]), text(&case[1])],
            |row| row.get(0),
        )?;
        assert_eq!(Some(id), text(&case[2]));
    }
    Ok(())
}

#[test]
fn an_advance_appends_the_rows_parcel_identity_appends() -> anyhow::Result<()> {
    let answers = answers()?;
    let (_directory, stage) = stage()?;
    put_registry(&stage, &registry_rows(&answers["registry"])?)?;
    put_after(&stage, &answers["after"])?;
    put_links(&stage, &answers["links"])?;
    carry(&stage, "2099-09-01")?;
    run_step(&stage, "current", "2099-09-01")?;
    run_step(&stage, "advance", "2099-09-01")?;
    assert_eq!(
        staged_rows(&stage, "registry_rows")?,
        registry_rows(&answers["advance"]["rows"])?
    );
    let counts: Vec<i64> = stage.connection().query_row(
        "SELECT kept, carried, issued, retired, redirected FROM registry_counts",
        [],
        |row| {
            Ok(vec![
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ])
        },
    )?;
    let expected = list(&answers["advance"]["counts"], "counts")?
        .iter()
        .map(|count| count.as_i64().unwrap_or(-1))
        .collect::<Vec<_>>();
    assert_eq!(counts, expected);
    run_step(&stage, "check", "2099-09-01")?;
    let problems: i64 =
        stage
            .connection()
            .query_row("SELECT count(*) FROM registry_problems", [], |row| {
                row.get(0)
            })?;
    assert_eq!(problems, 0);
    Ok(())
}

#[test]
fn the_fold_is_parcel_identitys_and_a_stronger_link_redirects_the_interim_id() -> anyhow::Result<()>
{
    let answers = answers()?;
    let (_directory, stage) = stage()?;
    let mut advanced = registry_rows(&answers["registry"])?;
    advanced.extend(registry_rows(&answers["advance"]["rows"])?);
    put_registry(&stage, &advanced)?;
    put_links(&stage, &answers["stronger_links"])?;
    carry(&stage, "2099-10-01")?;
    run_step(&stage, "current", "2099-10-01")?;
    run_step(&stage, "closed", "2099-10-01")?;
    assert_eq!(
        pairs(
            &stage,
            "SELECT pnu, parcel_id FROM registry_current ORDER BY 1, 2"
        )?,
        expected_pairs(&answers["advanced_fold"]["current"])?
    );
    assert_eq!(
        pairs(
            &stage,
            "SELECT pnu, parcel_id FROM registry_closed ORDER BY 1, 2"
        )?,
        expected_pairs(&answers["advanced_fold"]["closed"])?
    );
    run_step(&stage, "upgrade", "2099-10-01")?;
    assert_eq!(
        staged_rows(&stage, "registry_rows")?,
        registry_rows(&answers["upgrade"]["rows"])?
    );
    let redirected: i64 =
        stage
            .connection()
            .query_row("SELECT redirected FROM registry_counts", [], |row| {
                row.get(0)
            })?;
    assert_eq!(Some(redirected), answers["upgrade"]["redirected"].as_i64());
    Ok(())
}

#[test]
fn two_identity_claims_on_one_successor_are_a_conflict() -> anyhow::Result<()> {
    let (_directory, stage) = stage()?;
    stage.connection().execute_batch(
        "INSERT INTO lineage_pair VALUES
           ('9999910100100010000', '9999920100100010000', 'jurisdiction_transfer', 'official', 't', NULL, NULL, 'a', 'b'),
           ('9999910100100020000', '9999920100100010000', 'jurisdiction_transfer', 'evidence_strong', 't', NULL, NULL, 'a', 'b')",
    )?;
    carry(&stage, "2099-09-01")?;
    let conflicts: Vec<String> = stage
        .connection()
        .prepare("SELECT successor_pnu FROM lineage_conflicts")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(conflicts, ["9999920100100010000"]);
    Ok(())
}

#[test]
fn a_parcel_without_an_id_is_refused() -> anyhow::Result<()> {
    let answers = answers()?;
    let (_directory, stage) = stage()?;
    put_registry(&stage, &registry_rows(&answers["registry"])?)?;
    stage.connection().execute_batch(
        "INSERT INTO registry_after VALUES ('9999920100100090000');
         CREATE TEMP TABLE registry_rows AS SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to \
         FROM registry_region WHERE false;",
    )?;
    run_step(&stage, "check", "2099-09-01")?;
    let problems: Vec<String> = stage
        .connection()
        .prepare("SELECT problem FROM registry_problems")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(problems, ["no current id: 9999920100100090000"]);
    Ok(())
}

/// The table appends every derivation run: a pair derived again after a supersession holds two
/// runs, and only the newest one's rows are read; of one predecessor's code-change links, only
/// those of the best-graded pair source stand.
#[test]
fn only_the_newest_derivation_and_the_best_pair_source_are_read() -> anyhow::Result<()> {
    let (_directory, stage) = stage()?;
    stage.connection().execute_batch(
        "INSERT INTO lineage_pair VALUES
           -- the first run linked 1 -> A; the second run withdrew it and linked 1 -> B
           ('9999910100100010000', '9999920100100010000', 'code_change', 'code_derived', 'same_lot_paired_dong', 'derived:children', to_timestamp(4070000000), 'a', 'b'),
           ('9999910100100090000', '9999920100100010000', 'jurisdiction_transfer', 'pending', 'x', NULL, to_timestamp(4070000900), 'a', 'b'),
           ('9999910100100010000', '9999920100100020000', 'code_change', 'code_derived', 'same_lot_paired_dong', 'derived:children', to_timestamp(4070000900), 'a', 'b'),
           -- two pair sources for one predecessor: the official one stands
           ('9999910100100030000', '9999920100100030000', 'code_change', 'code_derived', 'same_lot_paired_dong', 'derived:parcel-jibun:x', to_timestamp(4070000900), 'a', 'b'),
           ('9999910100100030000', '9999920100100040000', 'code_change', 'code_derived', 'same_lot_paired_dong', 'official:y', to_timestamp(4070000900), 'a', 'b')",
    )?;
    run_step(&stage, "lineage", "2099-09-01")?;
    assert_eq!(
        pairs(
            &stage,
            "SELECT predecessor_pnu, successor_pnu FROM lineage_resolved ORDER BY 1, 2"
        )?,
        [
            ["9999910100100010000", "9999920100100020000"],
            ["9999910100100030000", "9999920100100040000"],
            ["9999910100100090000", "9999920100100010000"],
        ]
    );
    Ok(())
}

#[test]
fn a_link_across_sido_reads_them_as_one_region() -> anyhow::Result<()> {
    let answers = answers()?;
    let groups = &answers["groups"];
    let strings = |value: &Value| -> anyhow::Result<Vec<String>> {
        list(value, "strings")?
            .iter()
            .map(|item| text(item).context("a string"))
            .collect()
    };
    let links = list(&groups["links"], "links")?
        .iter()
        .map(|link| Ok((text(&link[0]).context("a")?, text(&link[1]).context("b")?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let found = advance_groups(&links, &strings(&groups["to"])?, &strings(&groups["from"])?);
    let expected = list(&groups["expected"], "groups")?
        .iter()
        .map(|group| {
            Ok((
                text(&group[0]).context("to")?,
                text(&group[1]).context("from")?,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(found, expected);
    assert_eq!(
        advance_groups(&[], &["98".to_owned()], &["98".to_owned()]),
        [("98".to_owned(), "98".to_owned())]
    );
    Ok(())
}

#[test]
fn a_read_as_of_an_edition_holds_it_and_the_ones_before() -> anyhow::Result<()> {
    let contract = r#"{"snapshot_id_prefix": "synthetic__parcel-",
                       "editions": {"209909": {}, "209906": {}, "209910": {}}}"#;
    assert_eq!(
        snapshot_ids_through(contract, "synthetic__parcel-209909")?,
        ["synthetic__parcel-209906", "synthetic__parcel-209909"]
    );
    assert!(snapshot_ids_through(contract, "synthetic__parcel-209912").is_err());
    assert!(snapshot_ids_through(contract, "209909").is_err());
    Ok(())
}

fn lineage(value: &Value) -> anyhow::Result<Vec<LineageRow>> {
    list(value, "lineage rows")?
        .iter()
        .map(|row| {
            Ok(LineageRow {
                predecessor_pnu: text(&row[0]),
                successor_pnu: text(&row[1]).context("successor")?,
                relation: text(&row[2]).context("relation")?,
                grade: text(&row[3]).context("grade")?,
                evidence_kind: text(&row[4]),
                evidence_ref: text(&row[5]),
                derived_at_micros: None,
                from_snapshot_id: text(&row[6]).context("from")?,
                to_snapshot_id: text(&row[7]).context("to")?,
            })
        })
        .collect()
}

#[test]
fn the_fingerprint_is_the_one_the_queue_showed_a_steward() -> anyhow::Result<()> {
    for case in list(&answers()?["fingerprints"], "fingerprints")? {
        let rows = lineage(&case["rows"])?;
        let derived = rows.iter().collect::<Vec<_>>();
        assert_eq!(
            Some(fingerprint(&derived)?),
            text(&case["etag"]),
            "{}",
            case["candidates"]
        );
    }
    Ok(())
}

#[test]
fn a_standing_decision_replaces_the_derived_rows_and_a_lapsed_one_is_not_read() -> anyhow::Result<()>
{
    for case in list(&answers()?["steward"], "steward cases")? {
        // As the SQL hands the rows over and as Python wrote the answer: in one order.
        let key = |row: &LineageRow| {
            (
                row.predecessor_pnu.clone(),
                row.successor_pnu.clone(),
                row.grade.clone(),
                row.evidence_kind.clone(),
                row.evidence_ref.clone(),
            )
        };
        let mut found = steward_resolved(&lineage(&case["rows"])?)?;
        found.sort_by_key(key);
        let mut expected = lineage(&case["resolved"])?;
        expected.sort_by_key(key);
        assert_eq!(found, expected, "{}", case["what"]);
    }
    Ok(())
}

#[test]
fn the_run_id_is_the_one_python_made_of_the_same_inputs() -> anyhow::Result<()> {
    for case in list(&answers()?["run_ids"], "run ids")? {
        let strings = |value: &Value| -> Vec<String> {
            value
                .as_array()
                .map(|items| items.iter().filter_map(text).collect())
                .unwrap_or_default()
        };
        let mode = match case[3].as_str() {
            Some("bootstrap") => Mode::Bootstrap,
            Some("advance") => Mode::Advance,
            _ => Mode::Upgrade,
        };
        let request = StepRequest {
            to_snapshot: text(&case[0]).context("to")?,
            to_date: "2099-09-01".to_owned(),
            to_sido: strings(&case[2]),
            from_sido: None,
            from_snapshot: text(&case[1]),
            bootstrap: mode == Mode::Bootstrap,
        };
        let found = run_id(
            &request,
            &strings(&case[2]),
            mode,
            &strings(&case[4]),
            case[5].as_u64().context("registry rows")?,
        )?;
        assert_eq!(Some(found), text(&case[6]));
    }
    Ok(())
}
