//! The checks a run judges: each 시도's result checks, the declared explanation of a commit,
//! and the checks of the whole build held before anything is appended.

use foundation_publisher_silver::kind_versions_stage::{national_verdict, ChangeExplanation};

use super::*;

/// The keys a failing check shows.
const CHECK_SAMPLES: usize = 5;

/// Runs the kind's result checks on the 시도 the connection holds, prints each as a
/// `gold-versions-check` line (a run a check refuses writes no summary), adds them to `results` and
/// returns the blocking ones that counted a row.
pub(super) fn run_result_checks(
    stage: &dyn KindVersionsStage,
    connection: &foundation_publisher_silver::duckdb::Connection,
    table: &str,
    sido: &str,
    results: &mut Vec<serde_json::Value>,
) -> anyhow::Result<Vec<&'static str>> {
    let mut refused = Vec::new();
    for check in stage.result_checks() {
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
        println!("gold-versions-check {result}");
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

/// Judges the kind's checks of the whole build over what its 시도 gathered, prints each as a
/// `gold-versions-check` line and adds it to `results`; a blocking one past its bound refuses the
/// build, before anything is appended. A bound is the replaced table's entry in the rebuild contract.
///
/// # Errors
/// Returns the refusals, or an error when a check does not run or names a bound the contract lacks.
pub(super) fn run_national_checks(
    national: &[foundation_publisher_silver::kind_versions_stage::NationalCheck],
    connection: &foundation_publisher_silver::duckdb::Connection,
    table: &str,
    replaced: &str,
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
        println!("gold-versions-check {result}");
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
