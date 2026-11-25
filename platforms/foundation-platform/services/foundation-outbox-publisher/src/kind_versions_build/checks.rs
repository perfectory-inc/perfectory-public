//! The checks a run judges: each 시도's counts and result checks, the declared explanation of a
//! commit, and the checks of the whole build held before anything is appended.
//!
//! Every check a run can report is in [`declared`], read from the same places the run reads it, and
//! `describe-checks` answers with it: the code location declares the checks from that answer, never
//! from a list of its own (root ADR-0615 §7 note 2026-11-25). A check is reported as the Pipes
//! `report_asset_check` message of the versions table ([`report`]), on a line that still carries
//! [`CHECK_LINE_PREFIX`]: ai-server's runner fails a run that reports a check no declaration names,
//! so the message goes out bare in the same PR that declares the checks (the lanes' Dagster assets on
//! AWS, ADR-0616 §7 A5).

use foundation_publisher_silver::kind_versions_stage::{national_verdict, ChangeExplanation};

use super::*;

/// The keys a failing check shows.
const CHECK_SAMPLES: usize = 5;

/// The check every 시도 reports with its numbers: the rows the build holds (one per key: the diff
/// refuses a key held twice) and what they add, change and remove of the head's. Never blocking.
pub(crate) const ROWS_AND_KEYS_CHECK: &str = "rows_and_keys";
/// The check every lane's changed documents get: rendered by the lane's renderer and held to its
/// schema-registry revision.
pub(crate) const DOCUMENTS_CHECK: &str = "changed_documents_hold_their_schema";
/// What a check's line starts with until the message goes out bare (the module's note).
pub(crate) const CHECK_LINE_PREFIX: &str = "gold-versions-check ";

/// A check a run of a lane can report: its name and whether a breach refuses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeclaredCheck {
    pub name: &'static str,
    pub blocking: bool,
}

/// Every check a run of `kind` can report: the counts, the documents check where the lane's
/// documents are checked (by the table whose renderer the run loads: the replaced one, else its
/// own), the lane's result checks and its checks of the whole build.
///
/// # Errors
/// Returns an error when the by-PNU lane contract cannot be read.
pub(crate) fn declared(kind: &KindBuild) -> anyhow::Result<Vec<DeclaredCheck>> {
    let mut found = vec![DeclaredCheck {
        name: ROWS_AND_KEYS_CHECK,
        blocking: false,
    }];
    if DocumentRules::checked_lane(kind.replaced)?.is_ok()
        || DocumentRules::checked_lane(kind.table)?.is_ok()
    {
        found.push(DeclaredCheck {
            name: DOCUMENTS_CHECK,
            blocking: true,
        });
    }
    found.extend(kind.result_checks().iter().map(|check| DeclaredCheck {
        name: check.name,
        blocking: check.blocking,
    }));
    found.extend(kind.national_checks().iter().map(|check| DeclaredCheck {
        name: check.name,
        blocking: check.blocking,
    }));
    Ok(found)
}

/// Prints `result`, a check's record as the summary keeps it (`check`, `passed` and what it
/// found), as the Pipes message of that check on `table`: every other field is its metadata.
///
/// # Errors
/// Returns an error when the record names a check `declared` does not hold: the code location would
/// refuse the run that reports it.
pub(crate) fn report(
    table: &str,
    declared: &[DeclaredCheck],
    result: &Value,
) -> anyhow::Result<()> {
    let name = result["check"].as_str().unwrap_or_default();
    ensure!(
        declared.iter().any(|check| check.name == name),
        "{table} reports the check {name:?}, which describe-checks does not declare"
    );
    let metadata = result
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(field, _)| !matches!(field.as_str(), "table" | "check" | "passed"))
        .map(|(field, value)| (field.clone(), value.clone()));
    let message = crate::pipes::asset_check(
        &crate::pipes::asset_key(table),
        name,
        result["passed"].as_bool() == Some(true),
        metadata,
    );
    println!("{CHECK_LINE_PREFIX}{message}");
    Ok(())
}

/// The record of the counts check of one 시도's diff.
fn rows_and_keys(table: &str, sido: &str, diff: &Diff) -> Value {
    json!({
        "table": table, "sido": sido, "check": ROWS_AND_KEYS_CHECK, "blocking": false,
        "passed": true, "rows": diff.built, "added": diff.added, "changed": diff.changed,
        "removed": diff.removed, "current_rows_read": diff.current_in_range,
    })
}

/// Reports the counts of the 시도's diff, runs the kind's result checks on the 시도 the connection
/// holds, reports each ([`report`]; a run a check refuses writes no summary), adds them to
/// `results` and returns the blocking ones that counted a row.
pub(super) fn run_result_checks(
    lane_checks: &[foundation_publisher_silver::kind_versions_stage::ResultCheck],
    connection: &foundation_publisher_silver::duckdb::Connection,
    (table, sido): (&str, &str),
    diff: &Diff,
    declared: &[DeclaredCheck],
    results: &mut Vec<serde_json::Value>,
) -> anyhow::Result<Vec<&'static str>> {
    let counts = rows_and_keys(table, sido, diff);
    report(table, declared, &counts)?;
    results.push(counts);
    let mut refused = Vec::new();
    for check in lane_checks {
        let breaches: i64 = connection
            .query_row(&check.sql, [], |row| row.get(0))
            .with_context(|| format!("the check {} of {table} did not run", check.name))?;
        // The first keys that break it, when the check names them.
        let samples = match &check.samples_sql {
            Some(sql) if breaches != 0 => connection
                .prepare(&format!("SELECT * FROM ({sql}) LIMIT {CHECK_SAMPLES}"))?
                .query_map([], |row| row.get::<_, Option<String>>(0))?
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("the samples of {} of {table} did not run", check.name))?,
            _ => Vec::new(),
        };
        let result = json!({
            "table": table, "sido": sido, "check": check.name, "breaches": breaches,
            "samples": samples, "blocking": check.blocking, "passed": breaches == 0,
        });
        report(table, declared, &result)?;
        results.push(result);
        if check.blocking && breaches != 0 {
            refused.push(check.name);
        }
    }
    Ok(refused)
}

/// The explanation a run gathers and judges: the kind's, on a run it can explain, one with a base
/// in which neither the code nor the served edition moved (`unexplained`) and exactly the inputs it
/// names did.
pub(super) fn explaining(
    explanation: Option<ChangeExplanation>,
    moved: &BTreeSet<String>,
    unexplained: bool,
) -> Option<ChangeExplanation> {
    explanation.filter(|explanation| {
        unexplained
            && *moved
                == explanation
                    .moved
                    .iter()
                    .map(|input| (*input).to_owned())
                    .collect::<BTreeSet<_>>()
    })
}

/// Judges the kind's checks of the whole build over what its 시도 gathered, reports each
/// ([`report`]) and adds it to `results`; a blocking one past its bound refuses the
/// build, before anything is appended. A bound is the replaced table's entry in the rebuild contract.
///
/// # Errors
/// Returns the refusals, or an error when a check does not run or names a bound the contract lacks.
pub(super) fn run_national_checks(
    national: &[foundation_publisher_silver::kind_versions_stage::NationalCheck],
    connection: &foundation_publisher_silver::duckdb::Connection,
    (table, replaced): (&str, &str),
    declared: &[DeclaredCheck],
    results: &mut Vec<Value>,
) -> anyhow::Result<()> {
    if national.is_empty() {
        return Ok(());
    }
    let contract: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    let entry = &contract["tables"][replaced];
    let mut refusals = Vec::new();
    for check in national {
        let (breaches, of): (i64, i64) = connection
            .query_row(&check.count_sql, [], |row| Ok((row.get(0)?, row.get(1)?)))
            .with_context(|| format!("the check {} of {table} did not run", check.name))?;
        let (breaches, of) = (u64::try_from(breaches)?, u64::try_from(of)?);
        let bound = check
            .bound
            .map(|key| {
                entry[key].as_f64().with_context(|| {
                    format!("gold-panel-rebuild.contract.json names no {key} of {replaced}")
                })
            })
            .transpose()?;
        let refused = national_verdict(check, breaches, of, bound);
        let result = json!({
            "table": table, "check": check.name, "breaches": breaches, "of": of, "bound": bound,
            "blocking": check.blocking, "passed": refused.is_none(),
        });
        report(table, declared, &result)?;
        results.push(result);
        if let Some(why) = refused.filter(|_| check.blocking) {
            refusals.push(why);
        }
    }
    ensure!(
        refusals.is_empty(),
        "{}: nothing of {table} was appended",
        refusals.join("; ")
    );
    Ok(())
}
