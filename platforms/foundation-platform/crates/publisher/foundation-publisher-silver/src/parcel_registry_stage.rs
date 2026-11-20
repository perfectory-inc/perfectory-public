//! The next step of `silver.parcel_registry` on `DuckDB` (root ADR-0113 §2·§3·§7(b), ADR-0490 §4a,
//! ADR-0559).
//!
//! It was the Spark job `parcel_registry_to_silver.py` over `parcel_registry_frames.py`. The
//! registry is append-only and every row names the edition it registered, so it is read **as of
//! an edition**: the rows of that edition and of the ones before it. From what that holds for a
//! region the step is one of three: `bootstrap` (nothing yet: every parcel of the edition gets
//! its id), `advance` (no row registers the edition yet: ids are carried across the strong
//! identity links of `silver.parcel_lineage`, other new parcels get fresh ids, vanished ones
//! close; the parcels both editions hold write nothing) or `upgrade` (the edition is registered:
//! a lineage that got stronger redirects the interim ids). Before anything is appended the result
//! is folded and checked: every parcel of the edition must carry exactly one current id.
//!
//! The joins are `parcel_registry_stage/step.sql`. What is not a set rule is here: the
//! lineage rows of the parcels a steward decided (`steward_resolved`), the run's id, and which
//! regions a run takes (`missing_regions`, `advance_groups`).

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    data_go_kr_file_data_stage::{column_names, count, epoch, literal},
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, LAKEHOUSE_CATALOG},
};

/// The table this step appends to.
pub const TABLE: &str = "silver.parcel_registry";
/// The cadastral parcels, an edition per `source_snapshot_id`.
pub const BOUNDARY_TABLE: &str = "silver.parcel_boundaries";
/// The lineage between two editions.
pub const LINEAGE_TABLE: &str = "silver.parcel_lineage";
/// `parcel_identity.ID_RULES_VERSION`.
pub const ID_RULES_VERSION: &str = "parcel-identity.v1";

const STEP_SQL: &str = include_str!("parcel_registry_stage/step.sql");
const LINEAGE_COLUMNS: &str =
    "predecessor_pnu, successor_pnu, relation, grade, evidence_kind, evidence_ref, \
                               derived_at_utc, from_snapshot_id, to_snapshot_id";
const REGISTRY_ROW: &str = "parcel_id, pnu, status, valid_from, valid_to, redirect_to";
const STEWARD: &str = "steward";
/// `parcel_lineage.GRADES`, strongest first.
const GRADES: [&str; 6] = [
    "official",
    "code_derived",
    "evidence_strong",
    "evidence_weak",
    "needs_review",
    "pending",
];

/// The part of `step.sql` named `name`.
fn step(name: &str) -> anyhow::Result<&'static str> {
    let marker = format!("\n-- step: {name}\n");
    let (_, rest) = STEP_SQL
        .split_once(&marker)
        .with_context(|| format!("step.sql has no {name} step"))?;
    Ok(rest
        .split_once("\n-- step: ")
        .map_or(rest, |(body, _)| body))
}

// `{to_date}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
fn run_step(stage: &DuckDbParquetStage, name: &str, to_date: &str) -> anyhow::Result<()> {
    stage
        .connection()
        .execute_batch(&step(name)?.replace("{to_date}", to_date))
        .with_context(|| format!("DuckDB failed the registry's {name} step"))
}

/// A comma-separated list of two-digit 시도 prefixes, as SQL literals.
fn sido_list(prefixes: &[String]) -> anyhow::Result<String> {
    ensure!(!prefixes.is_empty(), "a region names at least one 시도");
    for prefix in prefixes {
        ensure!(
            prefix.len() == 2 && prefix.bytes().all(|byte| byte.is_ascii_digit()),
            "{prefix:?} is not a two-digit 시도 prefix"
        );
    }
    Ok(prefixes
        .iter()
        .map(|prefix| literal(prefix))
        .collect::<Vec<_>>()
        .join(", "))
}

/// `YYYY-MM-DD`.
fn is_day(text: &str) -> bool {
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok() && text.len() == 10
}

/// A snapshot id as the editions spell it: letters, digits, `_`, `-`, `.`.
fn is_snapshot_id(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

/// The snapshot ids of the edition `snapshot` names and of every edition before it, oldest first
/// (`vworld_parcel_editions.snapshot_ids_through`): editions order as their `YYYYMM` names.
///
/// # Errors
/// The contract is not the parcel source contract, or `snapshot` is not one of its editions.
pub fn snapshot_ids_through(contract_json: &str, snapshot: &str) -> anyhow::Result<Vec<String>> {
    let contract: Value = serde_json::from_str(contract_json)?;
    let prefix = contract["snapshot_id_prefix"]
        .as_str()
        .context("the parcel source contract names no snapshot_id_prefix")?;
    let editions = contract["editions"]
        .as_object()
        .context("the parcel source contract lists no editions")?;
    let name = snapshot
        .strip_prefix(prefix)
        .filter(|name| editions.contains_key(*name))
        .with_context(|| format!("{snapshot:?} is not the snapshot id of an edition"))?;
    let mut names = editions.keys().map(String::as_str).collect::<Vec<_>>();
    names.sort_unstable();
    Ok(names
        .into_iter()
        .filter(|edition| *edition <= name)
        .map(|edition| format!("{prefix}{edition}"))
        .collect())
}

/// The registry at `head`, as a relation of the contract's columns; an empty one when the table
/// has no snapshot yet.
fn registry_source(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
) -> anyhow::Result<String> {
    let Some(head) = head else {
        let typed = contract
            .columns
            .iter()
            .map(|column| {
                let sql_type = if column.logical_type == "timestamp" {
                    "TIMESTAMPTZ"
                } else {
                    "VARCHAR"
                };
                format!("CAST(NULL AS {sql_type}) AS {}", column.name)
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(format!("(SELECT {typed} WHERE false)"));
    };
    let pinned = format!(
        "{LAKEHOUSE_CATALOG}.{} AT (VERSION => {head})",
        contract.table_name
    );
    with_contract_columns(stage.connection(), contract, &pinned)
}

/// The edition's parcels at `pin` (`silver.parcel_boundaries`).
fn boundaries(pin: i64, snapshot: &str) -> anyhow::Result<String> {
    ensure!(is_snapshot_id(snapshot), "{snapshot:?} is no snapshot id");
    Ok(format!(
        "(SELECT pnu FROM {LAKEHOUSE_CATALOG}.{BOUNDARY_TABLE} AT (VERSION => {pin}) WHERE source_snapshot_id = {})",
        literal(snapshot)
    ))
}

fn text_rows(stage: &DuckDbParquetStage, query: &str) -> anyhow::Result<Vec<(String, i64)>> {
    let mut found = stage.connection().prepare(query)?;
    let rows = found
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Where the step's inputs are read: the heads the run pinned.
#[derive(Clone, Copy, Debug)]
pub struct Pins {
    /// `silver.parcel_registry`; none while the table has no snapshot.
    pub registry: Option<i64>,
    /// `silver.parcel_boundaries`.
    pub boundaries: i64,
    /// `silver.parcel_lineage`; none while the table has no snapshot.
    pub lineage: Option<i64>,
}

// --- which regions -------------------------------------------------------------------------------

/// The edition's 시도 the registry as of it holds nothing for, with their parcel counts.
///
/// Smallest first (`--probe-missing`, root ADR-0243): a run that stops at its time limit has
/// registered as many regions as it could, and the next run takes the rest.
///
/// # Errors
/// `DuckDB` fails a read.
pub fn missing_regions(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    pins: Pins,
    snapshot: &str,
    through: &[String],
) -> anyhow::Result<Vec<(String, i64)>> {
    let registry = registry_source(stage, contract, pins.registry)?;
    let through = through
        .iter()
        .map(|id| literal(id))
        .collect::<Vec<_>>()
        .join(", ");
    text_rows(
        stage,
        &format!(
            "SELECT sido, n FROM (SELECT substr(pnu, 1, 2) AS sido, count(DISTINCT pnu) AS n FROM {} GROUP BY 1) \
             WHERE sido NOT IN (SELECT DISTINCT substr(pnu, 1, 2) FROM {registry} WHERE to_snapshot_id IN ({through})) \
             ORDER BY n, sido",
            boundaries(pins.boundaries, snapshot)?
        ),
    )
    .context("DuckDB failed to read the regions the registry does not hold")
}

/// One region an advance reads together: the 시도 of the new edition it registers, the 시도 it
/// reads the registry of, the edition it advances from, and the new edition's parcels there.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceRegion {
    /// 시도 of the edition, comma-joined.
    pub to_sido: String,
    /// 시도 the registry is read for, comma-joined.
    pub from_sido: String,
    /// The edition the region advances from.
    pub from_snapshot: String,
    /// The edition's parcels in `to_sido`.
    pub parcels: i64,
}

fn root(parent: &mut BTreeMap<String, String>, start: &str) -> String {
    let mut at = start.to_owned();
    while parent[&at] != at {
        let up = parent[&parent[&at]].clone();
        parent.insert(at.clone(), up.clone());
        at = up;
    }
    at
}

/// The regions one advance must read together (`parcel_registry_frames.advance_groups`).
///
/// The 시도 a link joins (predecessor 시도 → successor 시도, 29·46 → 12 on 2026-07-01) are one
/// region, every other 시도 its own. Each is (to 시도, from 시도), comma-joined, in name order.
#[must_use]
pub fn advance_groups(
    links: &[(String, String)],
    to_sido: &[String],
    from_sido: &[String],
) -> Vec<(String, String)> {
    let mut parent = to_sido
        .iter()
        .chain(from_sido)
        .chain(links.iter().flat_map(|(a, b)| [a, b]))
        .map(|sido| (sido.clone(), sido.clone()))
        .collect::<BTreeMap<_, _>>();
    for (a, b) in links {
        let (a, b) = (root(&mut parent, a), root(&mut parent, b));
        parent.insert(a, b);
    }
    let mut groups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    // Collected first: finding a root rewrites the map it walks.
    #[allow(clippy::needless_collect)]
    let members = parent.keys().cloned().collect::<Vec<_>>();
    for sido in members {
        groups
            .entry(root(&mut parent, &sido))
            .or_default()
            .insert(sido);
    }
    let to_set = to_sido.iter().collect::<BTreeSet<_>>();
    let read_set = from_sido.iter().chain(to_sido).collect::<BTreeSet<_>>();
    let join = |members: &BTreeSet<String>, keep: &BTreeSet<&String>| {
        members
            .iter()
            .filter(|sido| keep.contains(sido))
            .cloned()
            .collect::<Vec<_>>()
            .join(",")
    };
    let mut out = groups
        .values()
        .filter_map(|members| {
            let to = join(members, &to_set);
            (!to.is_empty()).then(|| (to, join(members, &read_set)))
        })
        .collect::<Vec<_>>();
    out.sort();
    out
}

/// The regions of `snapshot` no registry row registers yet, smallest first (`--probe-advance`,
/// root ADR-0490 §4a).
///
/// A region advances from the newest edition registered in it, over the lineage pair from that
/// edition. A region whose pair has no lineage yet is left out: advancing it would issue every
/// renumbered parcel a fresh id.
///
/// # Errors
/// `DuckDB` fails a read.
pub fn advance_regions(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    pins: Pins,
    snapshot: &str,
    through: &[String],
) -> anyhow::Result<Vec<AdvanceRegion>> {
    let Some(lineage_pin) = pins.lineage else {
        return Ok(Vec::new());
    };
    let registry = registry_source(stage, contract, pins.registry)?;
    let through_list = through
        .iter()
        .map(|id| literal(id))
        .collect::<Vec<_>>()
        .join(", ");
    let to = literal(snapshot);
    let to_counts = text_rows(
        stage,
        &format!(
            "SELECT substr(pnu, 1, 2), count(DISTINCT pnu) FROM {} GROUP BY 1 ORDER BY 1",
            boundaries(pins.boundaries, snapshot)?
        ),
    )?
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    let connection = stage.connection();
    // Per 시도: the newest edition registered there, and whether this edition is registered.
    let mut newest = connection.prepare(&format!(
        "SELECT substr(pnu, 1, 2), max(to_snapshot_id), bool_or(to_snapshot_id = {to}) FROM {registry} \
         WHERE to_snapshot_id IN ({through_list}) GROUP BY 1 ORDER BY 1"
    ))?;
    let registered = newest
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let held = registered
        .iter()
        .filter(|(_, _, holds)| *holds)
        .map(|(sido, _, _)| sido.clone())
        .collect::<BTreeSet<_>>();
    let lineage = format!("{LAKEHOUSE_CATALOG}.{LINEAGE_TABLE} AT (VERSION => {lineage_pin})");
    let mut pairs = connection.prepare(&format!(
        "SELECT DISTINCT from_snapshot_id FROM {lineage} WHERE to_snapshot_id = {to}"
    ))?;
    let pairs = pairs
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut sides = connection.prepare(&format!(
        "SELECT DISTINCT from_snapshot_id, substr(predecessor_pnu, 1, 2), substr(successor_pnu, 1, 2) FROM {lineage} \
         WHERE to_snapshot_id = {to} AND coalesce(predecessor_pnu, '') <> '' \
           AND substr(predecessor_pnu, 1, 2) <> substr(successor_pnu, 1, 2)"
    ))?;
    let sides = sides
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let to_sido = to_counts.keys().cloned().collect::<Vec<_>>();
    let sources = registered
        .iter()
        .map(|(_, source, _)| source.clone())
        .filter(|source| source != snapshot)
        .collect::<BTreeSet<_>>();
    let mut waiting = Vec::new();
    for source in sources {
        let from_sido = registered
            .iter()
            .filter(|(_, newest, _)| *newest == source)
            .map(|(sido, _, _)| sido.clone())
            .collect::<Vec<_>>();
        let links = sides
            .iter()
            .filter(|(from, _, _)| *from == source)
            .map(|(_, a, b)| (a.clone(), b.clone()))
            .collect::<Vec<_>>();
        for (to, read) in advance_groups(&links, &to_sido, &from_sido) {
            if to.split(',').any(|sido| held.contains(sido)) || !pairs.contains(&source) {
                continue;
            }
            let parcels = to.split(',').map(|sido| to_counts[sido]).sum();
            waiting.push(AdvanceRegion {
                to_sido: to,
                from_sido: read,
                from_snapshot: source.clone(),
                parcels,
            });
        }
    }
    waiting.sort_by(|a, b| (a.parcels, &a.to_sido).cmp(&(b.parcels, &b.to_sido)));
    Ok(waiting)
}

// --- a steward's decisions -----------------------------------------------------------------------

/// One lineage row, as the registry reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageRow {
    /// The earlier edition's PNU; none for a row that links nothing.
    pub predecessor_pnu: Option<String>,
    /// The later edition's PNU.
    pub successor_pnu: String,
    /// What happened to the land.
    pub relation: String,
    /// How sure the derivation is.
    pub grade: String,
    /// What the row rests on; `steward` for a person's decision.
    pub evidence_kind: Option<String>,
    /// The evidence; for a steward row, the decision as JSON.
    pub evidence_ref: Option<String>,
    /// The instant its derivation run took, in microseconds; none for a row without one.
    pub derived_at_micros: Option<i64>,
    /// The earlier edition.
    pub from_snapshot_id: String,
    /// The later edition.
    pub to_snapshot_id: String,
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(String::new(), |mut text, byte| {
        // Writing to a String cannot fail.
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn grade_rank(grade: &str) -> anyhow::Result<usize> {
    GRADES
        .iter()
        .position(|known| *known == grade)
        .with_context(|| format!("{grade:?} is no lineage grade"))
}

/// `(status, candidates_json, evidence_etag)` of one parcel's derived rows
/// (`lineage_review_queue.fingerprint`): exactly what the queue showed a steward.
fn fingerprint(derived: &[&LineageRow]) -> anyhow::Result<String> {
    let mut best = 0;
    for (at, row) in derived.iter().enumerate() {
        if grade_rank(&row.grade)? < grade_rank(&derived[best].grade)? {
            best = at;
        }
    }
    let grade = derived[best].grade.as_str();
    let status = if matches!(grade, "needs_review" | "pending") {
        grade
    } else {
        "sample"
    };
    let mut candidates = Vec::new();
    for (at, row) in derived.iter().enumerate() {
        let Some(predecessor) = row.predecessor_pnu.as_deref().filter(|p| !p.is_empty()) else {
            continue;
        };
        candidates.push((
            grade_rank(&row.grade)?,
            predecessor,
            *row,
            status == "sample" && at == best,
        ));
    }
    // A stable sort, as Python's: equal keys stay in the rows' order.
    candidates.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    // `json.dumps(candidates, ensure_ascii=False, sort_keys=True)`: keys in order, ", " and ": ".
    let text = |value: &str| serde_json::to_string(value);
    let mut parts = Vec::with_capacity(candidates.len());
    for (_, predecessor, row, in_effect) in candidates {
        parts.push(format!(
            "{{\"evidence_kind\": {}, \"evidence_ref\": {}, \"grade\": {}, \"in_effect\": {in_effect}, \"predecessor_pnu\": {}, \"relation\": {}}}",
            text(row.evidence_kind.as_deref().unwrap_or(""))?,
            text(row.evidence_ref.as_deref().unwrap_or(""))?,
            text(&row.grade)?,
            text(predecessor)?,
            text(&row.relation)?,
        ));
    }
    let candidates_json = format!("[{}]", parts.join(", "));
    let digest = Sha256::digest(format!("{status}\n{candidates_json}").as_bytes());
    Ok(hex(&digest))
}

/// A field of the decision a steward row records (its `evidence_ref`, written by the fold).
fn decision_field(row: &LineageRow, name: &str) -> String {
    row.evidence_ref
        .as_deref()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .and_then(|decision| {
            decision
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

/// The lineage rows of the parcels a steward decided, as every consumer must read them
/// (`lineage_review_queue.steward_resolved`, root ADR-0115 §3·§8).
///
/// For a parcel with a standing decision, that decision replaces the derived rows. A decision
/// stands only on the evidence it was made on: when the parcel's derived rows no longer give the
/// fingerprint it records, it has lapsed and the derived rows are read. Among standing decisions
/// the latest wins. `rows` are already the newest derivation's, without outranked code changes.
///
/// # Errors
/// A row has a grade the lineage does not use.
pub fn steward_resolved(rows: &[LineageRow]) -> anyhow::Result<Vec<LineageRow>> {
    let mut by_successor: BTreeMap<&str, Vec<&LineageRow>> = BTreeMap::new();
    for row in rows {
        by_successor
            .entry(&row.successor_pnu)
            .or_default()
            .push(row);
    }
    let mut resolved = Vec::new();
    for group in by_successor.values() {
        let is_steward = |row: &&&LineageRow| row.evidence_kind.as_deref() == Some(STEWARD);
        let derived = group
            .iter()
            .filter(|row| !is_steward(row))
            .copied()
            .collect::<Vec<_>>();
        let mut standing: Option<&LineageRow> = None;
        if !derived.is_empty() && group.iter().any(|row| is_steward(&row)) {
            let current = fingerprint(&derived)?;
            for steward in group.iter().filter(is_steward) {
                if decision_field(steward, "evidence_etag") != current {
                    continue;
                }
                let key = |row: &LineageRow| {
                    (
                        decision_field(row, "decided_at"),
                        decision_field(row, "decision_id"),
                    )
                };
                // The first of the latest, as Python's `max` keeps.
                if standing.is_none_or(|held| key(steward) > key(held)) {
                    standing = Some(steward);
                }
            }
        }
        match standing {
            Some(decision) => resolved.push(decision.clone()),
            None => resolved.extend(derived.into_iter().cloned()),
        }
    }
    Ok(resolved)
}

// --- the step ------------------------------------------------------------------------------------

/// Which of the three steps a region takes, from what the registry as of the edition holds for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    /// Nothing yet.
    Bootstrap,
    /// Something, but no row of this edition.
    Advance,
    /// Rows of this edition.
    Upgrade,
}

impl Mode {
    /// The word the summary and the run id carry.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Advance => "advance",
            Self::Upgrade => "upgrade",
        }
    }
}

/// What one region's step is asked.
#[derive(Clone, Debug)]
pub struct StepRequest {
    /// The edition to register.
    pub to_snapshot: String,
    /// The day the edition reflects, `YYYY-MM-DD`.
    pub to_date: String,
    /// 시도 of the edition's parcels.
    pub to_sido: Vec<String>,
    /// 시도 the registry held before, when they differ (29, 46 → 12).
    pub from_sido: Option<Vec<String>>,
    /// The edition the lineage runs from (advance, upgrade).
    pub from_snapshot: Option<String>,
    /// Whether the first registration of the region is allowed.
    pub bootstrap: bool,
}

/// What a step staged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Step {
    /// The step taken.
    pub mode: Mode,
    /// Rows `registry_staged` holds.
    pub rows: u64,
    /// The edition's parcels in the region.
    pub snapshot_parcels: u64,
    /// The changelog counts of the step (`kept`, `carried`, `issued`, `retired`, `redirected`).
    pub counts: BTreeMap<String, u64>,
    /// The run's id: the same inputs give the same id.
    pub derivation_run_id: String,
}

/// `"parcel-registry-"` and the first 24 hex digits of the SHA-256 of what the run read, as
/// Python's `json.dumps(..., sort_keys=True)` wrote it.
fn run_id(
    request: &StepRequest,
    region: &[String],
    mode: Mode,
    lineage_runs: &[String],
    registry_rows: u64,
) -> anyhow::Result<String> {
    let text = |value: &str| serde_json::to_string(value);
    let list = |values: &[String]| -> anyhow::Result<String> {
        Ok(format!(
            "[{}]",
            values
                .iter()
                .map(|value| text(value))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ))
    };
    let from = match &request.from_snapshot {
        Some(snapshot) => text(snapshot)?,
        None => "null".to_owned(),
    };
    let seed = format!(
        "{{\"from\": {from}, \"lineage\": {}, \"mode\": {}, \"registry_rows\": {registry_rows}, \"rules\": {}, \"sido\": {}, \"to\": {}}}",
        list(lineage_runs)?,
        text(mode.name())?,
        text(ID_RULES_VERSION)?,
        list(region)?,
        text(&request.to_snapshot)?,
    );
    let digest = Sha256::digest(seed.as_bytes());
    Ok(format!("parcel-registry-{}", &hex(&digest)[..24]))
}

fn lineage_rows(stage: &DuckDbParquetStage, table: &str) -> anyhow::Result<Vec<LineageRow>> {
    let mut found = stage.connection().prepare(&format!(
        "SELECT predecessor_pnu, successor_pnu, relation, grade, evidence_kind, evidence_ref, epoch_us(derived_at_utc), \
                from_snapshot_id, to_snapshot_id FROM {table} \
         ORDER BY successor_pnu, derived_at_utc, evidence_kind, grade, predecessor_pnu, evidence_ref"
    ))?;
    let rows = found
        .query_map([], |row| {
            Ok(LineageRow {
                predecessor_pnu: row.get(0)?,
                successor_pnu: row.get(1)?,
                relation: row.get(2)?,
                grade: row.get(3)?,
                evidence_kind: row.get(4)?,
                evidence_ref: row.get(5)?,
                derived_at_micros: row.get(6)?,
                from_snapshot_id: row.get(7)?,
                to_snapshot_id: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Resolves the parcels a steward decided and adds their rows to `lineage_resolved`.
fn resolve_stewarded(stage: &DuckDbParquetStage) -> anyhow::Result<()> {
    let decided = lineage_rows(stage, "lineage_stewarded")?;
    if decided.is_empty() {
        return Ok(());
    }
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE lineage_decided (predecessor_pnu VARCHAR, successor_pnu VARCHAR, relation VARCHAR,          grade VARCHAR, evidence_kind VARCHAR, evidence_ref VARCHAR, derived_at_micros BIGINT, from_snapshot_id VARCHAR,          to_snapshot_id VARCHAR)",
    )?;
    {
        let mut appender = connection.appender("lineage_decided")?;
        for row in steward_resolved(&decided)? {
            appender.append_row(duckdb::params![
                row.predecessor_pnu,
                row.successor_pnu,
                row.relation,
                row.grade,
                row.evidence_kind,
                row.evidence_ref,
                row.derived_at_micros,
                row.from_snapshot_id,
                row.to_snapshot_id
            ])?;
        }
        appender.flush()?;
    }
    connection
        .execute_batch(
            "INSERT INTO lineage_resolved SELECT predecessor_pnu, successor_pnu, relation, grade, evidence_kind,              evidence_ref, make_timestamptz(derived_at_micros), from_snapshot_id, to_snapshot_id FROM lineage_decided",
        )
        .context("DuckDB failed to add the stewards' rows")
}

/// Builds one region's step: `registry_staged` holds the rows to append (the table's columns),
/// checked so that every parcel of the edition carries exactly one current id.
///
/// `through` are the edition's snapshot id and the ones before it ([`snapshot_ids_through`]).
///
/// # Errors
/// The request is malformed, the region has no parcels in the edition, the registry holds nothing
/// for the region and a bootstrap was not allowed, an advance has no lineage pair to read, the
/// lineage holds identity conflicts, the result would leave a parcel without one current id, or
/// `DuckDB` fails.
pub fn build_step(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    pins: Pins,
    request: &StepRequest,
    through: &[String],
    recorded_at: DateTime<Utc>,
) -> anyhow::Result<Step> {
    ensure!(
        is_day(&request.to_date),
        "the edition's day must be YYYY-MM-DD"
    );
    ensure!(
        is_snapshot_id(&request.to_snapshot)
            && request.from_snapshot.as_deref().is_none_or(is_snapshot_id),
        "a snapshot id has characters a snapshot id does not use"
    );
    let from_sido = request.from_sido.as_ref().unwrap_or(&request.to_sido);
    let region = request
        .to_sido
        .iter()
        .chain(from_sido)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let read = read_inputs(stage, contract, pins, request, &region, through)?;
    let mode = if read.registry_rows == 0 {
        ensure!(
            request.bootstrap,
            "the registry holds nothing for this region; allow a bootstrap for the first registration"
        );
        Mode::Bootstrap
    } else if read.holds_snapshot {
        Mode::Upgrade
    } else {
        ensure!(
            request.from_snapshot.is_some(),
            "advancing to a new snapshot reads its lineage: name the snapshot it runs from"
        );
        Mode::Advance
    };
    let (lineage_runs, counts) = if mode == Mode::Bootstrap {
        run_step(stage, "bootstrap", &request.to_date)?;
        (Vec::new(), BTreeMap::new())
    } else {
        carried_step(stage, pins, request, mode)?
    };
    run_step(stage, "check", &request.to_date)?;
    let problems = text_rows(
        stage,
        "SELECT problem, kind FROM registry_problems ORDER BY kind, at",
    )?;
    if !problems.is_empty() {
        let first = problems
            .iter()
            .take(10)
            .map(|(problem, _)| problem.as_str())
            .collect::<Vec<_>>();
        bail!("refusing to register: {}", first.join("; "));
    }
    let derivation_run_id = run_id(request, &region, mode, &lineage_runs, read.registry_rows)?;
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE registry_staged AS SELECT {columns} FROM (\
             SELECT {REGISTRY_ROW}, {to} AS to_snapshot_id, {rules} AS id_rules_version, {run} AS derivation_run_id, \
                    {at} AS recorded_at_utc FROM registry_rows)",
            columns = column_names(contract).join(", "),
            to = literal(&request.to_snapshot),
            rules = literal(ID_RULES_VERSION),
            run = literal(&derivation_run_id),
            at = epoch(recorded_at),
        ))
        .context("DuckDB failed to stage the registry rows")?;
    Ok(Step {
        mode,
        rows: count(stage, "registry_staged")?,
        snapshot_parcels: read.snapshot_parcels,
        counts,
        derivation_run_id,
    })
}

/// What the registry as of the edition and the edition hold for a region.
struct RegionRead {
    registry_rows: u64,
    snapshot_parcels: u64,
    holds_snapshot: bool,
}

/// `registry_region` and `registry_after` of the request's region.
fn read_inputs(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    pins: Pins,
    request: &StepRequest,
    region: &[String],
    through: &[String],
) -> anyhow::Result<RegionRead> {
    let connection = stage.connection();
    connection
        .execute_batch(step("macros")?)
        .context("DuckDB refused the registry's readers")?;
    let through_list = through
        .iter()
        .map(|id| literal(id))
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE registry_region AS SELECT {REGISTRY_ROW}, to_snapshot_id FROM {} \
             WHERE to_snapshot_id IN ({through_list}) AND substr(pnu, 1, 2) IN ({});
             CREATE OR REPLACE TEMP TABLE registry_after AS SELECT DISTINCT pnu FROM {} \
             WHERE substr(pnu, 1, 2) IN ({});",
            registry_source(stage, contract, pins.registry)?,
            sido_list(region)?,
            boundaries(pins.boundaries, &request.to_snapshot)?,
            sido_list(&request.to_sido)?,
        ))
        .context("DuckDB failed to read the registry and the edition's parcels")?;
    let snapshot_parcels = count(stage, "registry_after")?;
    ensure!(
        snapshot_parcels > 0,
        "no parcels in {} for {}",
        request.to_snapshot,
        request.to_sido.join(",")
    );
    Ok(RegionRead {
        registry_rows: count(stage, "registry_region")?,
        snapshot_parcels,
        holds_snapshot: count(
            stage,
            &format!(
                "registry_region WHERE to_snapshot_id = {}",
                literal(&request.to_snapshot)
            ),
        )? > 0,
    })
}

/// An advance or an upgrade: `registry_rows` from the lineage pair. Returns the pair's derivation
/// runs and the step's changelog counts.
fn carried_step(
    stage: &DuckDbParquetStage,
    pins: Pins,
    request: &StepRequest,
    mode: Mode,
) -> anyhow::Result<(Vec<String>, BTreeMap<String, u64>)> {
    let connection = stage.connection();
    // An upgrade without a snapshot to run from has no lineage pair to read: nothing grew stronger.
    let from = request.from_snapshot.as_deref().unwrap_or("");
    let Some(pin) = pins.lineage else {
        bail!("{LINEAGE_TABLE} has no snapshot to read the pair from");
    };
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE lineage_read AS SELECT {LINEAGE_COLUMNS}, derivation_run_id \
             FROM {LAKEHOUSE_CATALOG}.{LINEAGE_TABLE} AT (VERSION => {pin}) \
             WHERE from_snapshot_id = {} AND to_snapshot_id = {};
             CREATE OR REPLACE TEMP TABLE lineage_pair AS SELECT {LINEAGE_COLUMNS} FROM lineage_read;",
            literal(from),
            literal(&request.to_snapshot)
        ))
        .context("DuckDB failed to read the lineage pair")?;
    let lineage_runs = text_rows(
        stage,
        "SELECT derivation_run_id, count(*) FROM lineage_read GROUP BY 1 ORDER BY 1",
    )?
    .into_iter()
    .map(|(run, _)| run)
    .collect::<Vec<_>>();
    ensure!(
        mode != Mode::Advance || !lineage_runs.is_empty(),
        "no lineage from {from} to {}: advancing would issue every renumbered parcel a fresh id",
        request.to_snapshot
    );
    run_step(stage, "lineage", &request.to_date)?;
    // A steward's standing decision replaces the derived rows of its parcel (root ADR-0115 §9).
    resolve_stewarded(stage)?;
    run_step(stage, "carry", &request.to_date)?;
    let conflicts = text_rows(
        stage,
        "SELECT successor_pnu, 0 FROM lineage_conflicts ORDER BY 1 LIMIT 20",
    )?;
    if !conflicts.is_empty() {
        let sample = conflicts
            .iter()
            .take(5)
            .map(|(pnu, _)| pnu.as_str())
            .collect::<Vec<_>>();
        bail!(
            "{} identity conflicts in the lineage (e.g. {sample:?}); resolve them before registering ids",
            count(stage, "lineage_conflicts")?
        );
    }
    run_step(stage, "current", &request.to_date)?;
    let names: &[&str] = if mode == Mode::Advance {
        run_step(stage, "advance", &request.to_date)?;
        &["kept", "carried", "issued", "retired", "redirected"]
    } else {
        run_step(stage, "closed", &request.to_date)?;
        run_step(stage, "upgrade", &request.to_date)?;
        &["redirected"]
    };
    let mut counts = BTreeMap::new();
    for name in names {
        let value: i64 =
            connection.query_row(&format!("SELECT {name} FROM registry_counts"), [], |row| {
                row.get(0)
            })?;
        counts.insert((*name).to_owned(), u64::try_from(value)?);
    }
    Ok((lineage_runs, counts))
}

#[cfg(test)]
mod tests;
