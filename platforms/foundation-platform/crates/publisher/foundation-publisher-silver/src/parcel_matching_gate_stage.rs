//! The parcel matching gate (root ADR-0113 §7) in `DuckDB` inside the publisher (root ADR-0559).
//!
//! It was the Spark job `parcel_matching_gate.py`. Before a cadastral edition's parcels are baked the
//! gate checks, from the lakehouse only, that (a) every parcel's legal dong exists in the newest
//! official code list (or was renumbered 1:1 into one, root ADR-0329) and its 읍면동 has a served
//! administrative boundary, and (b) every parcel carries exactly one current parcel id in
//! `silver.parcel_registry`, read as of the edition (root ADR-0490 §4a). The verdict is the one the
//! Spark job wrote, `foundation-platform.parcel_matching_verdict.v1`, key for key: the bake binds its
//! revision to it (`lakehouse_bake_verdict`) and the database stores it.
//!
//! The Spark job's `--attribute` and `--lineage-from-snapshot-id` checks are not here: no caller
//! passed them (the parcel tile bake passes the edition and its 시도), and `attributes` stays `{}`.
//!
//! `gate.sql` holds the joins over one 시도; this module runs it per 시도 checked, adds each run up
//! (`collect.sql`), checks the ids two parcels share over every run, and assembles the verdict.

use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use duckdb::Connection;
use serde_json::{json, Map, Value};

use crate::duckdb_parquet_stage::DuckDbParquetStage;
use crate::legal_dong_code_change_stage::{PAIR_ROWS_SQL, RENUMBERED_INTO_SQL};

const GATE_SQL: &str = include_str!("parcel_matching_gate_stage/gate.sql");
const COLLECT_SQL: &str = include_str!("parcel_matching_gate_stage/collect.sql");

/// The verdict schema the gate writes and the bake reads.
pub const VERDICT_SCHEMA_VERSION: &str = "foundation-platform.parcel_matching_verdict.v1";
/// The job name the verdict carries (unchanged from the Spark job, so stored verdicts compare).
pub const JOB_NAME: &str = "parcel_matching_gate";
/// The parcels of the edition.
pub const PARCEL_TABLE: &str = "silver.parcel_boundaries";
/// The official code list, one snapshot per release.
pub const CODE_LIST_TABLE: &str = "reference.legal_dong_code_snapshot";
/// The legal-dong code change record.
pub const CODE_CHANGE_TABLE: &str = "reference.legal_dong_code_change";
/// The served administrative boundaries.
pub const ADMIN_TABLE: &str = "gold.administrative_boundary_served";
/// The parcel registry (check (b)).
pub const REGISTRY_TABLE: &str = "silver.parcel_registry";
/// The exit code of a gate whose registry does not exist yet (root ADR-0265): the bake waits.
pub const INPUT_ABSENT: i32 = 3;
/// Items sampled per reason (`map_matching_gate.SAMPLE`).
pub const SAMPLE: usize = 20;
/// The national edition's bound on parcels whose legal dong nothing explains.
///
/// Like a dbt test's `error_if`: past it the gate refuses, within it the parcels are sampled.
/// 2026-11-11: 2 of 202609's 39,876,546 (a 리 code no list ever held, an abolished 동 with no pair).
pub const UNEXPLAINED_LEGAL_DONG_BOUND: u64 = 100;

/// The refusals and allowances, worded as `map_matching_gate.py` words them.
pub mod reasons {
    /// A PNU that is not 19 digits.
    pub const MALFORMED_PNU: &str = "malformed PNU";
    /// A legal dong the newest list does not hold (past the bound).
    pub const NO_LEGAL_DONG: &str = "legal dong not existing in the official list";
    /// A placed parcel whose 읍면동 has no served boundary.
    pub const NO_BOUNDARY: &str = "no administrative boundary for the parcel's 읍면동";
    /// A parcel with no current id.
    pub const NO_CURRENT_ID: &str = "no current parcel id";
    /// An id two parcels carry.
    pub const ID_ON_TWO_PARCELS: &str = "parcel id on two parcels";
    /// Allowed: renumbered 1:1 after the edition, checked under the successor.
    pub const RENUMBERED_LEGAL_DONG: &str =
        "legal dong renumbered since the edition, checked under its successor";
    /// Allowed up to the bound: a stray code no official pair explains.
    pub const UNEXPLAINED_LEGAL_DONG: &str =
        "legal dong the list does not hold and no official pair explains, within the bound";
}

/// How check (b) runs (root ADR-0265).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Registry {
    /// The registry exists: read as of the edition.
    Checked,
    /// The region has no registry yet; check (b) is skipped.
    Skipped,
    /// The registry table does not exist yet: the gate does not run.
    Wait,
}

impl Registry {
    /// The verdict's `registry` value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Checked => "checked",
            Self::Skipped => "skipped",
            Self::Wait => "wait",
        }
    }
}

/// The table references the gate reads (a lakehouse table pinned at its head snapshot, or a test's
/// own table).
pub struct GateSources {
    /// `silver.parcel_boundaries`.
    pub parcels: String,
    /// `reference.legal_dong_code_snapshot`.
    pub code_list: String,
    /// `reference.legal_dong_code_change`.
    pub code_changes: String,
    /// `gold.administrative_boundary_served`.
    pub admin: String,
    /// `silver.parcel_registry`, when check (b) runs.
    pub registry: Option<String>,
}

/// One run of the gate.
pub struct GateRun<'a> {
    /// The edition's `source_snapshot_id`.
    pub snapshot_id: &'a str,
    /// The 시도 prefixes checked, as the caller gave them (`11,26,…`).
    pub sido: &'a str,
    /// The registry's `to_snapshot_id`s read: the edition's and every earlier edition's.
    pub registry_through: &'a [String],
    /// How check (b) runs.
    pub registry: Registry,
}

fn quoted(value: &str) -> anyhow::Result<String> {
    ensure!(
        !value.contains('\''),
        "a gate input holds a quote: {value:?}"
    );
    Ok(format!("'{value}'"))
}

/// The verdict of a gate whose registry does not exist yet (root ADR-0265).
#[must_use]
pub fn waiting_verdict(snapshot_id: &str, sido: &str) -> Value {
    json!({
        "schema_version": VERDICT_SCHEMA_VERSION,
        "job": JOB_NAME,
        "snapshot_id": snapshot_id,
        "sido": sido,
        "registry": Registry::Wait.as_str(),
        "passed": false,
        "inputs_absent": [REGISTRY_TABLE],
    })
}

/// Runs the gate over `sources` and returns its verdict.
///
/// # Errors
/// Returns a `DuckDB` failure, a 시도 that is not two digits, or an input that holds a quote.
pub fn verdict(
    stage: &DuckDbParquetStage,
    sources: &GateSources,
    run: &GateRun<'_>,
) -> anyhow::Result<Value> {
    ensure!(
        run.registry != Registry::Wait,
        "a gate that waits for its registry does not run"
    );
    let prefixes: Vec<&str> = run.sido.split(',').collect();
    ensure!(
        prefixes
            .iter()
            .all(|p| p.len() == 2 && p.bytes().all(|b| b.is_ascii_digit())),
        "the 시도 are comma-separated two-digit prefixes, got {:?}",
        run.sido
    );
    let registry = registry_read(sources, run)?;
    let connection = stage.connection();
    load_references(connection, sources)?;
    let edition = format!(
        "SELECT pnu FROM {parcels} WHERE source_snapshot_id = {snapshot} AND valid_to_utc IS NULL",
        parcels = sources.parcels,
        snapshot = quoted(run.snapshot_id)?,
    );
    let registry_checked = sources.registry.is_some() && run.registry == Registry::Checked;
    run_each_sido(connection, &edition, &registry, registry_checked, &prefixes)?;
    let totals = Totals { connection };
    let parcels = parcels_report(&totals, run.registry == Registry::Checked)?;

    let snapshot_parcel_count = totals.total("scanned")?;
    let renumbered = totals.scalar("SELECT count(*) FROM renumbering")?;
    let newest: Option<String> = connection
        .prepare("SELECT snapshot_date FROM gate_official LIMIT 1")?
        .query_map([], |row| row.get::<_, Option<String>>(0))?
        .next()
        .transpose()?
        .flatten();
    let passed = parcels["passed"] == true;
    Ok(json!({
        "schema_version": VERDICT_SCHEMA_VERSION,
        "job": JOB_NAME,
        "snapshot_id": run.snapshot_id,
        "sido": run.sido,
        "registry": run.registry.as_str(),
        "snapshot_parcel_count": snapshot_parcel_count,
        "renumbered_legal_dongs": renumbered,
        "parcels": parcels,
        "official_code_snapshot": newest,
        "attributes": {},
        "passed": passed,
    }))
}

/// The registry rows as of the edition (check (b)), or an empty typed read when check (b) is skipped.
fn registry_read(sources: &GateSources, run: &GateRun<'_>) -> anyhow::Result<String> {
    Ok(match (&sources.registry, run.registry) {
        (Some(table), Registry::Checked) => {
            let through = run
                .registry_through
                .iter()
                .map(|id| quoted(id))
                .collect::<anyhow::Result<Vec<_>>>()?;
            ensure!(!through.is_empty(), "the registry is read as of no edition");
            format!(
                "SELECT pnu, parcel_id, status, valid_from, valid_to FROM {table} \
                 WHERE to_snapshot_id IN ({})",
                through.join(", ")
            )
        }
        (_, Registry::Checked) => anyhow::bail!("check (b) runs with no registry table"),
        _ => "SELECT NULL::VARCHAR AS pnu, NULL::VARCHAR AS parcel_id, NULL::VARCHAR AS status, \
              NULL::VARCHAR AS valid_from, NULL::VARCHAR AS valid_to WHERE false"
            .to_owned(),
    })
}

/// The small inputs every 시도's run shares (the code list, the boundaries, the renumberings) and the
/// tables the runs add up into.
fn load_references(connection: &Connection, sources: &GateSources) -> anyhow::Result<()> {
    connection
        .execute_batch(&format!(
            "SET preserve_insertion_order = false;\n\
             CREATE OR REPLACE TEMP TABLE gate_official AS \
               SELECT region_cd, status, CAST(snapshot_date AS VARCHAR) AS snapshot_date FROM {code_list} \
               WHERE snapshot_date = (SELECT max(snapshot_date) FROM {code_list});\n\
             CREATE OR REPLACE TEMP TABLE gate_admin AS SELECT canonical_code FROM {admin};\n\
             CREATE OR REPLACE TEMP TABLE code_changes AS \
               SELECT kind, old_code, new_code, level, effective_date, source FROM {code_changes};\n\
             {PAIR_ROWS_SQL}\n\
             CREATE OR REPLACE TEMP TABLE served_codes AS \
               SELECT DISTINCT region_cd AS code FROM gate_official WHERE status = '존재';\n\
             {RENUMBERED_INTO_SQL}\n\
             CREATE OR REPLACE TEMP TABLE gate_acc_counts (scanned UBIGINT, well UBIGINT, malformed UBIGINT, \
               strays UBIGINT, moved UBIGINT, no_boundary UBIGINT, no_current UBIGINT);\n\
             CREATE OR REPLACE TEMP TABLE gate_acc_malformed (pnu VARCHAR);\n\
             CREATE OR REPLACE TEMP TABLE gate_acc_stray (n UBIGINT);\n\
             CREATE OR REPLACE TEMP TABLE gate_acc_no_boundary (n UBIGINT);\n\
             CREATE OR REPLACE TEMP TABLE gate_acc_no_current (n UBIGINT);\n\
             CREATE OR REPLACE TEMP TABLE gate_acc_ids (parcel_id VARCHAR);",
            code_list = sources.code_list,
            admin = sources.admin,
            code_changes = sources.code_changes,
        ))
        .context("DuckDB failed to read the parcel matching gate's reference inputs")
}

/// One run of gate.sql per 시도 checked, its prefix written into both reads so the Iceberg scan
/// prunes to its files (2026-11-21, 시도 41 of 202609: 5,211,798 parcels in 97 s, peak 1.4 GiB at 2
/// threads; the whole edition at once spilled past two hours in the unit's 2 GiB). The edition's
/// other prefixes (a 시도 not checked, a malformed PNU's first characters, a NULL PNU) are only
/// counted: `snapshot_parcel_count` is the whole edition's distinct PNUs, as Spark's distinct was.
fn run_each_sido(
    connection: &Connection,
    edition: &str,
    registry: &str,
    registry_checked: bool,
    prefixes: &[&str],
) -> anyhow::Result<()> {
    let held: Vec<Option<String>> = connection
        .prepare(&format!(
            "SELECT DISTINCT substr(pnu, 1, 2) AS part FROM ({edition}) e ORDER BY part NULLS LAST"
        ))?
        .query_map([], |row| row.get::<_, Option<String>>(0))?
        .collect::<Result<_, _>>()?;
    let collect_sql = COLLECT_SQL.replace("{SAMPLE}", &SAMPLE.to_string());
    for part in &held {
        let Some(part) = part.as_deref().filter(|part| prefixes.contains(part)) else {
            connection
                .execute(
                    &format!(
                        "INSERT INTO gate_acc_counts (scanned) \
                         SELECT count(DISTINCT pnu) + (count(*) FILTER (WHERE pnu IS NULL) > 0)::UBIGINT \
                         FROM ({edition}) e WHERE substr(pnu, 1, 2) IS NOT DISTINCT FROM ?"
                    ),
                    [part],
                )
                .context("DuckDB failed to count a prefix the gate does not check")?;
            continue;
        };
        let registry_part = if registry_checked {
            format!(" AND starts_with(pnu, '{part}')")
        } else {
            String::new()
        };
        connection
            .execute_batch(&format!(
                "CREATE OR REPLACE TEMP VIEW gate_edition_source AS {edition} AND starts_with(pnu, '{part}');\n\
                 CREATE OR REPLACE TEMP TABLE gate_sido AS SELECT '{part}' AS prefix;\n\
                 CREATE OR REPLACE TEMP VIEW gate_registry AS {registry}{registry_part};\n\
                 {GATE_SQL}\n{collect_sql}",
            ))
            .with_context(|| format!("DuckDB failed to run the parcel matching gate over 시도 {part}"))?;
    }
    if registry_checked {
        connection
            .execute_batch(
                "CREATE OR REPLACE TEMP TABLE gate_shared_id AS \
                 SELECT parcel_id FROM gate_acc_ids GROUP BY parcel_id HAVING count(*) > 1",
            )
            .context("DuckDB failed to find the ids two parcels share")?;
    }
    Ok(())
}

/// Reads the added-up runs: totals, counts and samples.
struct Totals<'a> {
    connection: &'a Connection,
}

impl Totals<'_> {
    fn scalar(&self, query: &str) -> anyhow::Result<u64> {
        let value: i64 = self.connection.query_row(query, [], |row| row.get(0))?;
        Ok(u64::try_from(value)?)
    }

    fn total(&self, column: &str) -> anyhow::Result<u64> {
        self.scalar(&format!(
            "SELECT CAST(coalesce(sum({column}), 0) AS BIGINT) FROM gate_acc_counts"
        ))
    }

    fn sample(&self, query: &str) -> anyhow::Result<Vec<String>> {
        Ok(self
            .connection
            .prepare(&format!("{query} LIMIT {SAMPLE}"))?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn pnus(&self, table: &str) -> anyhow::Result<Vec<String>> {
        self.sample(&format!(
            "SELECT lpad(CAST(n AS VARCHAR), 19, '0') FROM {table} ORDER BY n"
        ))
    }

    /// `n` and, when there are any, the sample `items` reads.
    fn counted(
        n: u64,
        items: impl FnOnce() -> anyhow::Result<Vec<String>>,
    ) -> anyhow::Result<(u64, Vec<String>)> {
        Ok((n, if n == 0 { Vec::new() } else { items()? }))
    }
}

/// The verdict's `parcels`: the reasons found, the allowances, and the strays within the bound.
fn parcels_report(totals: &Totals<'_>, registry_checked: bool) -> anyhow::Result<Value> {
    let checked = totals.total("well")? + totals.total("malformed")?;
    let strays = totals.total("strays")?;
    let over = strays > UNEXPLAINED_LEGAL_DONG_BOUND;
    let stray_sample = if strays == 0 {
        Vec::new()
    } else {
        totals.pnus("gate_acc_stray")?
    };
    let mut violations = BTreeMap::new();
    violations.insert(
        reasons::MALFORMED_PNU,
        Totals::counted(totals.total("malformed")?, || {
            totals.sample("SELECT pnu FROM gate_acc_malformed ORDER BY pnu")
        })?,
    );
    violations.insert(
        reasons::NO_LEGAL_DONG,
        if over {
            (strays, stray_sample.clone())
        } else {
            (0, Vec::new())
        },
    );
    violations.insert(
        reasons::NO_BOUNDARY,
        Totals::counted(totals.total("no_boundary")?, || {
            totals.pnus("gate_acc_no_boundary")
        })?,
    );
    if registry_checked {
        violations.insert(
            reasons::NO_CURRENT_ID,
            Totals::counted(totals.total("no_current")?, || {
                totals.pnus("gate_acc_no_current")
            })?,
        );
        violations.insert(
            reasons::ID_ON_TWO_PARCELS,
            Totals::counted(
                totals.scalar("SELECT count(*) FROM gate_shared_id")?,
                || totals.sample("SELECT parcel_id FROM gate_shared_id ORDER BY parcel_id"),
            )?,
        );
    }
    let mut allowed = BTreeMap::new();
    allowed.insert(reasons::RENUMBERED_LEGAL_DONG, totals.total("moved")?);
    allowed.insert(
        reasons::UNEXPLAINED_LEGAL_DONG,
        if over { 0 } else { strays },
    );
    Ok(report(
        checked,
        &violations,
        &allowed,
        if over { &[] } else { &stray_sample },
    ))
}

/// `parcel_matching_gate.report`: the reasons found (count and sample), the allowances counted, and
/// the strays sampled within the bound.
fn report(
    checked: u64,
    violations: &BTreeMap<&str, (u64, Vec<String>)>,
    allowed: &BTreeMap<&str, u64>,
    unexplained_sample: &[String],
) -> Value {
    let found: Map<String, Value> = violations
        .iter()
        .filter(|(_, (count, _))| *count > 0)
        .map(|(reason, (count, sample))| {
            (
                (*reason).to_owned(),
                json!({"count": count, "sample": sample}),
            )
        })
        .collect();
    let kept: Map<String, Value> = allowed
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(reason, n)| ((*reason).to_owned(), json!(n)))
        .collect();
    let mut made = json!({
        "passed": found.is_empty(),
        "checked": checked,
        "violations": found,
        "allowed": kept,
    });
    if !unexplained_sample.is_empty() {
        made["unexplained_sample"] = json!(unexplained_sample);
    }
    made
}

/// The verdict as the Spark job wrote it to its summary file: two-space indent, keys sorted, UTF-8
/// as is, one newline (Python's `json.dumps(…, ensure_ascii=False, indent=2, sort_keys=True)`).
///
/// # Errors
/// Returns a serialization failure.
pub fn verdict_file_bytes(verdict: &Value) -> anyhow::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(verdict)?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests;
