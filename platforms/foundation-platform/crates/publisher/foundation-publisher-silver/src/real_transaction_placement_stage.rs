//! Where each 실거래 deal belongs, in `DuckDB` inside the publisher (root ADR-0350 §1, ADR-0351, ADR-0405).
//!
//! The rules of `placed_deals` in `real_transaction_silver_to_gold.py` and the resolver functions it
//! calls (W81c slice 1): an unmasked lot is carried to its current parcel (`deals.sql`, `resolve.sql`);
//! a masked lot (`3**`) is inferred level by level from the evidence the 실거래 contract enables
//! (`inference_sql`, rendered here from `place_inference`); a deal the 본번 group level links is a
//! row on every current parcel of the group; a deal on a retired parcel follows the lineage
//! (`placed.sql`, over the candidates `lineage_carry` walks).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use anyhow::{ensure, Context as _};
use lakehouse_domain::industrial_complex_lakehouse_contracts;
use lakehouse_domain::{
    LakehouseTableContract, CURRENT_ROWS_COLUMNS, CURRENT_ROWS_FILTER, CURRENT_ROWS_SOURCE,
};
use serde_json::Value;

use crate::parcel_nearby_stage::Source;

/// The 실거래 Silver.
pub const RT_SOURCE: &str = "silver.real_transaction";
/// The parcels; the served edition's are the ones a deal is placed on.
pub const PARCEL_SOURCE: &str = "silver.parcel_boundaries";
/// The code change table the crosswalk is read from.
pub const CODE_CHANGE_SOURCE: &str = "reference.legal_dong_code_change";
/// The code snapshots an address is looked up in.
pub const LEGAL_DONG_SOURCE: &str = "reference.legal_dong_code_snapshot";
/// The land register (지목, 면적, 용도지역).
pub const LAND_SOURCE: &str = "silver.land_characteristic";
/// The building register titles.
pub const TITLE_SOURCE: &str = "silver.building_register_titles";
/// The land owners.
pub const OWNERSHIP_SOURCE: &str = "silver.land_ownership";
/// The parcel lineage.
pub const LINEAGE_SOURCE: &str = "silver.parcel_lineage";
/// The PNU-keyed inputs and the tables `place` reads them as.
pub const PNU_INPUTS: [(&str, &str); 4] = [
    (PARCEL_SOURCE, "parcels"),
    (LAND_SOURCE, "land"),
    (TITLE_SOURCE, "titles"),
    (OWNERSHIP_SOURCE, "ownership"),
];
/// The key an address is looked up by in `address_codes`: its runs of whitespace made one space.
/// `deals.sql` and the region spool read it alike.
pub const ADDRESS_KEY_MACRO: &str = "CREATE OR REPLACE MACRO rt_address_key(a) AS \
    trim(regexp_replace(coalesce(a, ''), '\\s+', ' ', 'g'))";

/// `deals.sql`: the deals with their address codes, `row_id` and source PNU.
pub const DEALS_SQL: &str = include_str!("real_transaction_placement_stage/deals.sql");
/// `resolve.sql`: each source PNU carried to the current parcel.
pub const RESOLVE_SQL: &str = include_str!("real_transaction_placement_stage/resolve.sql");
/// `masked.sql`: the masked lots as the inference reads them.
pub const MASKED_SQL: &str = include_str!("real_transaction_placement_stage/masked.sql");

/// The evidence a level may name (`place_identity_resolver.EVIDENCE`).
pub const EVIDENCE: [&str; 8] = [
    "lot_range",
    "zone",
    "jimok",
    "land_area",
    "building_year",
    "floor_area",
    "registers_agree",
    "ownership_change",
];
/// The parcel-row evidence the candidate join itself checks (`PARCEL_EVIDENCE`).
const PARCEL_EVIDENCE: [&str; 3] = ["zone", "jimok", "land_area"];
/// The grades a level may link with (`INFERRED_GRADES`).
pub const INFERRED_GRADES: [&str; 3] = [
    "inferred_ownership_change",
    "inferred_attributes",
    MAIN_LOT_GROUP,
];
/// The 본번 group level: one parcel's evidence, a claim on its group (root ADR-0405).
pub const MAIN_LOT_GROUP: &str = "inferred_same_main_lot_group";
/// The 본번 group level on a dataset measured under the bar, while the owner's switch shows it.
pub const MAIN_LOT_GROUP_BELOW_BAR: &str = "inferred_same_main_lot_group_below_bar";
/// A PNU's 법정동 (10), 산 flag (1) and 본번 (4): the key of its 본번 group.
pub const MAIN_LOT_KEY_LENGTH: usize = 15;
/// 본번 per bin of the candidate join.
pub const LOT_BIN: u32 = 10;

/// One inference level a dataset may link by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    /// The grade a link by this level carries.
    pub grade: String,
    /// The evidence every candidate must fit.
    pub evidence: Vec<String>,
}

/// What `place_inference` gives every level: the area tolerance, the ownership window and causes, and
/// the 용도지역 a deal's zone has been split into since.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceRules {
    /// `area_tolerance_m2`.
    pub tolerance_m2: f64,
    /// `ownership_window_days`: days from the contract to the sale the land file records.
    pub ownership_window_days: (i64, i64),
    /// `ownership_causes`: the change reasons that are a sale.
    pub ownership_causes: Vec<String>,
    /// `zone_subdivisions`: a deal's zone → the zones it was split into.
    pub zone_subdivisions: BTreeMap<String, Vec<String>>,
}

fn number(value: &Value, what: &str) -> anyhow::Result<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| anyhow::anyhow!("place_inference {what} is not a number: {value}"))
}

fn level_of(level: &Value) -> anyhow::Result<Level> {
    let grade = level["grade"].as_str().unwrap_or_default().to_owned();
    let evidence: Vec<String> = level["evidence"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e.as_str().map(str::to_owned))
        .collect();
    let unknown: Vec<&String> = evidence
        .iter()
        .filter(|e| !EVIDENCE.contains(&e.as_str()))
        .collect();
    if !unknown.is_empty() || !INFERRED_GRADES.contains(&grade.as_str()) {
        anyhow::bail!("place_inference level {grade} names {unknown:?} or an unknown grade");
    }
    Ok(Level { grade, evidence })
}

/// The datasets the contract measured a level for (and the below-bar switch), in name order.
#[must_use]
pub fn measured_datasets(inference: &Value) -> Vec<String> {
    let mut found: BTreeSet<String> = BTreeSet::new();
    for level in inference["levels"].as_array().into_iter().flatten() {
        if let Some(measured) = level["measured"].as_object() {
            found.extend(measured.keys().cloned());
        }
    }
    if let Some(measured) = inference["below_bar"]["measured"].as_object() {
        found.extend(measured.keys().cloned());
    }
    found.into_iter().collect()
}

/// The levels a dataset links by, strongest first.
///
/// `enabled_levels` (root ADR-0370, ADR-0483 §4): those
/// whose measured `precision_wilson_low` reaches `precision_bar` on at least `min_measured_rows`, then
/// the 본번 group level as `MAIN_LOT_GROUP_BELOW_BAR` when the owner's switch shows it for a dataset
/// measured under the bar at or above its `floor`.
///
/// # Errors
/// A level naming unknown evidence or grade, a missing number, or a switch naming no group level.
pub fn enabled_levels(inference: &Value, dataset: &str) -> anyhow::Result<Vec<Level>> {
    let bar = number(&inference["precision_bar"], "precision_bar")?;
    let minimum = number(&inference["min_measured_rows"], "min_measured_rows")?;
    let mut levels = Vec::new();
    let every = inference["levels"].as_array().cloned().unwrap_or_default();
    for level in &every {
        let parsed = level_of(level)?;
        let measured = &level["measured"][dataset];
        if measured.is_object()
            && number(&measured["precision_wilson_low"], "precision_wilson_low")? >= bar
            && number(&measured["rows"], "rows")? >= minimum
        {
            levels.push(parsed);
        }
    }
    let below = &inference["below_bar"];
    let measured = &below["measured"][dataset];
    let shown = below["shown"].as_bool().unwrap_or(false)
        && measured.is_object()
        && number(&measured["precision_wilson_low"], "precision_wilson_low")?
            >= number(&below["floor"], "floor")?
        && number(&measured["rows"], "rows")? >= minimum;
    if shown && !levels.iter().any(|level| level.grade == MAIN_LOT_GROUP) {
        let group = every
            .iter()
            .find(|level| level["grade"].as_str() == Some(MAIN_LOT_GROUP))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "place_inference.below_bar shows {MAIN_LOT_GROUP}, which no level names"
                )
            })?;
        levels.push(Level {
            grade: MAIN_LOT_GROUP_BELOW_BAR.to_owned(),
            ..level_of(group)?
        });
    }
    Ok(levels)
}

/// `place_inference`'s values every level reads.
///
/// # Errors
/// A missing or malformed value.
pub fn inference_rules(inference: &Value) -> anyhow::Result<InferenceRules> {
    let window = inference["ownership_window_days"]
        .as_array()
        .filter(|w| w.len() == 2)
        .ok_or_else(|| {
            anyhow::anyhow!("place_inference.ownership_window_days is not [low, high]")
        })?;
    let day = |v: &Value| {
        v.as_i64()
            .ok_or_else(|| anyhow::anyhow!("ownership_window_days holds {v}"))
    };
    let strings = |v: &Value| -> Vec<String> {
        v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s.as_str().map(str::to_owned))
            .collect()
    };
    Ok(InferenceRules {
        tolerance_m2: number(&inference["area_tolerance_m2"], "area_tolerance_m2")?,
        ownership_window_days: (day(&window[0])?, day(&window[1])?),
        ownership_causes: strings(&inference["ownership_causes"]),
        zone_subdivisions: inference["zone_subdivisions"]
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(key, _)| !key.starts_with('$'))
            .map(|(zone, parts)| (zone.clone(), strings(parts)))
            .collect(),
    })
}

fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn sets(levels: &[Level]) -> Vec<Vec<&'static str>> {
    let mut out = Vec::new();
    for level in levels {
        out.push(
            EVIDENCE
                .iter()
                .copied()
                .filter(|e| *e != "registers_agree" && level.evidence.iter().any(|x| x == e))
                .collect(),
        );
        if level.evidence.iter().any(|e| e == "registers_agree") {
            out.push(vec!["zone", "land_area"]);
            out.push(vec!["zone", "building_year", "floor_area"]);
        }
    }
    out
}

/// The SQL of one evidence check over a candidate row (`infer_masked`'s `checks`).
fn check(name: &str, rules: &InferenceRules) -> String {
    let tolerance = rules.tolerance_m2;
    match name {
        "zone" => {
            let split: Vec<String> = rules
                .zone_subdivisions
                .iter()
                .flat_map(|(zone, parts)| parts.iter().map(move |part| quoted(&format!("{zone}|{part}"))))
                .collect();
            let split = if split.is_empty() { "''".to_owned() } else { split.join(", ") };
            format!(
                "(zone IS NULL OR zone_name_1 = zone OR concat_ws('|', zone, zone_name_1) IN ({split}))"
            )
        }
        "jimok" => "(jimok IS NULL OR land_category = jimok)".to_owned(),
        "land_area" => format!("(land_area_m2 IS NOT NULL AND abs(area_m2 - land_area_m2) < {tolerance:?})"),
        "building_year" => {
            "(building_year IS NOT NULL AND list_contains(coalesce(_years, []), building_year))".to_owned()
        }
        "floor_area" => format!(
            "(floor_area_m2 IS NOT NULL AND (abs(_floor_sum - floor_area_m2) < {tolerance:?} \
             OR len(list_filter(coalesce(_floors, []), lambda f: abs(f - floor_area_m2) < {tolerance:?})) > 0))"
        ),
        "ownership_change" => "_owner_ok".to_owned(),
        _ => "true".to_owned(),
    }
}

/// The inference of one dataset's masked deals (`infer_masked`), as one `SELECT`.
///
/// It reads the tables the
/// caller holds: `masked` (masked.sql, this dataset's rows), `land_parcels` (`pnu`, `land_category`,
/// `area_m2`, `zone_name_1`: the land register of the served parcels), `titles` (`pnu`,
/// `approval_year`, `floor_area_m2`: the main buildings), `ownership` (`pnu`, `ownership_changed_on`,
/// `ownership_change_reason`), `crosswalk` and `group_parcels` (`pnu`).
///
/// Returns `row_id`, `pnu`, `pnu_group` (the sorted current parcels of a 본번 group; `pnu` is then
/// NULL), `place_identity_grade`, `pnu_unresolved_reason`, `pnu_candidate_count`.
#[must_use]
pub fn inference_sql(levels: &[Level], rules: &InferenceRules) -> String {
    if levels.is_empty() {
        return "SELECT row_id, NULL::VARCHAR AS pnu, NULL::VARCHAR[] AS pnu_group, \
                'unresolved' AS place_identity_grade, 'jibun_masked' AS pnu_unresolved_reason, \
                NULL::INTEGER AS pnu_candidate_count FROM masked"
            .to_owned();
    }
    let every = sets(levels);
    let joined: Vec<&str> = PARCEL_EVIDENCE
        .iter()
        .copied()
        .filter(|e| every.iter().all(|set| set.contains(e)))
        .collect();
    let asked: Vec<&str> = EVIDENCE
        .iter()
        .copied()
        .filter(|e| every.iter().any(|set| set.contains(e)))
        .collect();
    let mut sql = candidates_sql(&joined, rules);
    evidence_sql(&mut sql, &asked, rules);
    let tried: Vec<String> = levels
        .iter()
        .enumerate()
        .map(|(rank, level)| level_sql(&mut sql, rank, level, rules))
        .collect();
    outcome_sql(&mut sql, levels.len() - 1, &tried);
    sql
}

/// The candidate join: every served parcel of a masked deal's 법정동, 산 flag and 본번 range that fits
/// the parcel-row evidence every level asks for.
fn candidates_sql(joined: &[&str], rules: &InferenceRules) -> String {
    let mut sql = String::new();
    let _ = write!(
        sql,
        "WITH carried AS (\n  SELECT p.pnu, p.land_category, p.area_m2, p.zone_name_1,\n         \
         coalesce(x.current_code, substr(p.pnu, 1, 10)) AS _dong, substr(p.pnu, 11, 1) AS _mountain,\n         \
         CAST(floor(TRY_CAST(substr(p.pnu, 12, 4) AS INTEGER) / {LOT_BIN}) AS INTEGER) AS _bin\n  \
         FROM land_parcels p LEFT JOIN crosswalk x ON x.old_code = substr(p.pnu, 1, 10)\n),\n\
         binned AS (\n  SELECT m.*, unnest(range(CAST(floor(lot_low / {LOT_BIN}) AS INTEGER), \
         CAST(floor(lot_high / {LOT_BIN}) AS INTEGER) + 1)) AS _bin FROM masked m\n),\n\
         joined AS (\n  SELECT d.* EXCLUDE (_bin), c.pnu AS candidate, c.land_category, c.area_m2, c.zone_name_1\n  \
         FROM binned d JOIN carried c\n    ON c._dong = d.legal_dong_code\n   \
         AND c._mountain = CASE WHEN d.lot_mountain THEN '2' ELSE '1' END\n   AND c._bin = d._bin\n   \
         AND TRY_CAST(substr(c.pnu, 12, 4) AS INTEGER) BETWEEN d.lot_low AND d.lot_high\n"
    );
    for name in joined {
        let _ = writeln!(sql, "   AND {}", check(name, rules));
    }
    sql.push_str("),\n");
    sql
}

/// The building and ownership evidence the levels ask for, joined to the candidates.
fn evidence_sql(sql: &mut String, asked: &[&str], rules: &InferenceRules) {
    let from = if asked
        .iter()
        .any(|e| *e == "building_year" || *e == "floor_area")
    {
        sql.push_str(
            "buildings AS (\n  SELECT pnu, list(DISTINCT approval_year) AS _years, list(floor_area_m2) AS _floors,\n         \
             sum(floor_area_m2) AS _floor_sum FROM titles GROUP BY pnu\n),\n\
             with_buildings AS (\n  SELECT j.*, b._years, b._floors, b._floor_sum FROM joined j \
             LEFT JOIN buildings b ON b.pnu = j.candidate\n),\n",
        );
        "with_buildings"
    } else {
        "joined"
    };
    if asked.contains(&"ownership_change") {
        let causes: Vec<String> = rules.ownership_causes.iter().map(|c| quoted(c)).collect();
        let causes = if causes.is_empty() {
            "NULL".to_owned()
        } else {
            causes.join(", ")
        };
        let (low, high) = rules.ownership_window_days;
        let _ = write!(
            *sql,
            "sold AS (\n  SELECT pnu AS candidate, list(DISTINCT CAST(ownership_changed_on AS DATE)) AS _sold_on\n  \
             FROM ownership WHERE ownership_change_reason IN ({causes}) AND ownership_changed_on IS NOT NULL\n  \
             GROUP BY pnu\n),\ncandidates AS (\n  SELECT f.*, coalesce(len(list_filter(s._sold_on, \
             lambda day: date_diff('day', f.deal_date, day) BETWEEN {low} AND {high})) > 0, false) AS _owner_ok\n  \
             FROM {from} f LEFT JOIN sold s ON s.candidate = f.candidate\n),\n"
        );
    } else {
        let _ = writeln!(
            *sql,
            "candidates AS (SELECT *, false AS _owner_ok FROM {from}),"
        );
    }
}

/// One level's outcome per deal: how many candidates fit and, for one, which.
fn level_sql(sql: &mut String, rank: usize, level: &Level, rules: &InferenceRules) -> String {
    let fits: Vec<String> = level
        .evidence
        .iter()
        .filter(|e| e.as_str() != "registers_agree")
        .map(|e| check(e, rules))
        .collect();
    let fits = if fits.is_empty() {
        "true".to_owned()
    } else {
        fits.join(" AND ")
    };
    let grade = quoted(&level.grade);
    if level.evidence.iter().any(|e| e == "registers_agree") {
        let land = format!("{} AND {}", check("zone", rules), check("land_area", rules));
        let build = format!(
            "{} AND {} AND {}",
            check("zone", rules),
            check("building_year", rules),
            check("floor_area", rules)
        );
        let _ = write!(
            *sql,
            "level_{rank} AS (\n  SELECT row_id, {rank} AS _rank, {grade} AS _grade,\n    \
             list_sort(list(DISTINCT candidate) FILTER ({fits})) AS _set,\n    \
             list_sort(list(DISTINCT candidate) FILTER ({land})) AS _land,\n    \
             list_sort(list(DISTINCT candidate) FILTER ({build})) AS _build\n  FROM candidates GROUP BY row_id\n),\n\
             level_{rank}_sized AS (\n  SELECT *, coalesce(len(_land), 0) AS _nl, coalesce(len(_build), 0) AS _nb,\n    \
             coalesce(len(_land), 0) = 1 AND coalesce(len(_build), 0) = 1 AND _land = _build AS _agreed\n  \
             FROM level_{rank} WHERE coalesce(len(_set), 0) > 0\n),\n\
             level_{rank}_out AS (\n  SELECT row_id, _rank, _grade,\n    \
             CASE WHEN _agreed THEN len(_set)\n         \
             ELSE greatest(_nl, _nb, CASE WHEN _nl = 1 AND _nb = 1 THEN 2 ELSE 0 END) END AS _count,\n    \
             CASE WHEN _agreed THEN _set[1] END AS _pnu\n  FROM level_{rank}_sized\n),\n"
        );
    } else {
        let _ = write!(
            *sql,
            "level_{rank}_out AS (\n  SELECT row_id, {rank} AS _rank, {grade} AS _grade, count(DISTINCT candidate) AS _count,\n    \
             min(candidate) AS _pnu\n  FROM candidates WHERE {fits} GROUP BY row_id\n),\n"
        );
    }
    format!("SELECT * FROM level_{rank}_out")
}

/// The first level that linked each deal, its 본번 group, and why an unlinked deal is not.
fn outcome_sql(sql: &mut String, last: usize, tried: &[String]) {
    let groups: Vec<String> = [MAIN_LOT_GROUP, MAIN_LOT_GROUP_BELOW_BAR]
        .iter()
        .map(|g| quoted(g))
        .collect();
    let groups = groups.join(", ");
    let _ = write!(
        *sql,
        "tried AS (\n  {}\n),\n\
         linked AS (\n  SELECT row_id, arg_min(_pnu, _rank) AS _pnu, arg_min(_grade, _rank) AS _grade,\n         \
         arg_min(_count, _rank) AS _count\n  FROM tried WHERE _count = 1 GROUP BY row_id\n),\n\
         last AS (SELECT row_id, _count AS _last FROM tried WHERE _rank = {last}),\n\
         members AS (\n  SELECT substr(pnu, 1, {MAIN_LOT_KEY_LENGTH}) AS _key, list_sort(list(DISTINCT pnu)) AS _members\n  \
         FROM group_parcels GROUP BY 1\n),\n\
         grouped AS (\n  SELECT l.row_id, l._grade, l._count,\n    \
         CASE WHEN l._grade IN ({groups}) THEN list_sort(list_distinct(list_concat(coalesce(m._members, []), [l._pnu]))) END AS _group,\n    \
         CASE WHEN l._grade NOT IN ({groups}) THEN l._pnu END AS _pnu\n  \
         FROM linked l LEFT JOIN members m ON m._key = CASE WHEN l._grade IN ({groups}) \
         THEN substr(l._pnu, 1, {MAIN_LOT_KEY_LENGTH}) END\n)\n\
         SELECT m.row_id, g._pnu AS pnu, g._group AS pnu_group,\n       \
         coalesce(g._grade, 'unresolved') AS place_identity_grade,\n       \
         CASE WHEN g._pnu IS NOT NULL OR g._group IS NOT NULL THEN NULL\n            \
         WHEN coalesce(t._last, 0) > 1 THEN 'inference_ambiguous' ELSE 'inference_no_candidate' END AS pnu_unresolved_reason,\n       \
         CAST(coalesce(g._count, t._last, 0) AS INTEGER) AS pnu_candidate_count\n\
         FROM (SELECT DISTINCT row_id FROM masked) m LEFT JOIN grouped g ON g.row_id = m.row_id \
         LEFT JOIN last t ON t.row_id = m.row_id",
        tried.join("\n  UNION ALL BY NAME\n  ")
    );
}

/// `placed.sql`: every deal with the PNU it is placed on.
pub const PLACED_SQL: &str = include_str!("real_transaction_placement_stage/placed.sql");

/// The placement's rules as text, for the build's code version: a change of any rebuilds every PNU.
/// The lineage walk's are [`crate::lineage_carry_stage::SQL`].
pub const CODE_TEXTS: [&str; 8] = [
    crate::place_identity_stage::REFERENCE_SQL,
    ADDRESS_KEY_MACRO,
    DEALS_SQL,
    RESOLVE_SQL,
    MASKED_SQL,
    PLACED_SQL,
    INFERENCE_SETUP_SQL,
    CARRY_SOURCES_SQL,
];

/// What one placement counted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementCounts {
    /// Deals read.
    pub deals: u64,
    /// Rows `placed` holds (a lineage or group deal is a row per parcel).
    pub placed_rows: u64,
    /// Deals placed on at least one parcel.
    pub placed_deals: u64,
    /// Retired source PNUs the lineage walk started from.
    pub lineage_sources: u64,
    /// Candidates the walk found for them.
    pub lineage_candidates: u64,
}

fn count(connection: &duckdb::Connection, sql: &str) -> anyhow::Result<u64> {
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(u64::try_from(value)?)
}

fn has_table(connection: &duckdb::Connection, name: &str) -> anyhow::Result<bool> {
    Ok(count(
        connection,
        &format!(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = {}",
            quoted(name)
        ),
    )? > 0)
}

/// The code crosswalk as tables (`crosswalk`, `ambiguous`, `served_forms`) from `code_changes` and
/// `code_names`: the one place-identity resolver's (`place_identity_stage::reference_from_loaded`,
/// root ADR-0288 §1), read back for the address codes.
pub(crate) fn crosswalk_tables(
    connection: &duckdb::Connection,
) -> anyhow::Result<(BTreeMap<String, String>, BTreeSet<String>)> {
    crosswalk_tables_from(connection, "code_changes", "code_names")
}

/// [`crosswalk_tables`] from the code change table `changes` and the code snapshot `names`, for a
/// caller whose connection holds another `code_changes` (the shared versions build keeps its own,
/// of the pairs in force only).
pub(crate) fn crosswalk_tables_from(
    connection: &duckdb::Connection,
    changes: &str,
    names: &str,
) -> anyhow::Result<(BTreeMap<String, String>, BTreeSet<String>)> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE place_code_changes AS SELECT * FROM {changes};
         CREATE OR REPLACE TEMP TABLE place_dong_names AS SELECT * FROM {names};"
    ))?;
    crate::place_identity_stage::reference_from_loaded(connection)?;
    connection.execute_batch(
        "CREATE OR REPLACE TABLE crosswalk AS SELECT old_code, current_code FROM place_remap;
         CREATE OR REPLACE TABLE ambiguous AS SELECT code AS amb_code FROM place_ambiguous;
         CREATE OR REPLACE TABLE served_forms AS
           SELECT code10 AS _code10, form10 AS _form10, rank::INTEGER AS _rank FROM place_forms;",
    )?;
    let remap = connection
        .prepare("SELECT old_code, current_code FROM crosswalk")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<BTreeMap<String, String>, _>>()?;
    let ambiguous = connection
        .prepare("SELECT amb_code FROM ambiguous")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<BTreeSet<String>, _>>()?;
    Ok((remap, ambiguous))
}

/// A 법정동's full name with its runs of whitespace made one space, the key an address is looked up by.
fn address_key(name: &str) -> String {
    name.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Full 법정동 name → code, from `(code, full name, status)` rows of one code snapshot.
///
/// `rt_molit_real_transaction.dong_index` (the 실거래 load's own reading of a deal's 시군구 and 법정동
/// names, not `address_pnu`'s): a current (`존재`) code wins over an abolished one, and a name two
/// codes of the same standing share is left out. 시도·시군구 rows (`…00000`) are not 법정동s.
fn dong_index<'a>(
    rows: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
) -> BTreeMap<String, String> {
    let mut by_rank: BTreeMap<String, BTreeMap<u8, BTreeSet<&str>>> = BTreeMap::new();
    for (code, name, status) in rows {
        let ten_digits = code.len() == 10 && code.bytes().all(|b| b.is_ascii_digit());
        if !ten_digits || code.get(5..) == Some("00000") {
            continue;
        }
        let rank = u8::from(status != "존재");
        by_rank
            .entry(address_key(name))
            .or_default()
            .entry(rank)
            .or_default()
            .insert(code);
    }
    by_rank
        .into_iter()
        .filter_map(|(key, ranks)| {
            let codes = ranks.into_values().next()?;
            (codes.len() == 1)
                .then(|| (key, codes.into_iter().next().unwrap_or_default().to_owned()))
        })
        .collect()
}

/// `(address key, its code in the snapshot, its current code or None)` for every named 법정동: the code
/// carried through the pairs in force; a code whose chain splits has no current code.
fn address_codes(
    index: &BTreeMap<String, String>,
    remap: &BTreeMap<String, String>,
    ambiguous: &BTreeSet<String>,
) -> Vec<(String, String, Option<String>)> {
    index
        .iter()
        .map(|(name, code)| {
            let current =
                (!ambiguous.contains(code)).then(|| remap.get(code).unwrap_or(code).clone());
            (name.clone(), code.clone(), current)
        })
        .collect()
}

/// `address_codes` from `code_names` (`region_cd`, `full_name`, `status`: the newest code snapshot).
pub(crate) fn address_code_table(
    connection: &duckdb::Connection,
    remap: &BTreeMap<String, String>,
    ambiguous: &BTreeSet<String>,
) -> anyhow::Result<()> {
    address_code_table_from(connection, "code_names", remap, ambiguous)
}

/// [`address_code_table`] from the code snapshot `code_names` is read as.
pub(crate) fn address_code_table_from(
    connection: &duckdb::Connection,
    code_names: &str,
    remap: &BTreeMap<String, String>,
    ambiguous: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let names: Vec<(String, String, String)> = connection
        .prepare(&format!(
            "SELECT region_cd, coalesce(full_name, ''), coalesce(status, '') FROM {code_names}"
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    let index = dong_index(
        names
            .iter()
            .map(|(c, n, s)| (c.as_str(), n.as_str(), s.as_str())),
    );
    connection.execute_batch(
        "CREATE OR REPLACE TABLE address_codes (_address VARCHAR, source_legal_dong VARCHAR, legal_dong VARCHAR);",
    )?;
    let mut appender = connection.appender("address_codes")?;
    for (name, code, current) in address_codes(&index, remap, ambiguous) {
        appender.append_row(duckdb::params![name, code, current])?;
    }
    Ok(())
}

const CARRY_SOURCES_SQL: &str = "
CREATE OR REPLACE TEMP TABLE carry_sources AS
  SELECT DISTINCT pnu_candidate AS pnu FROM resolved WHERE pnu_candidate IS NOT NULL
  AND pnu_candidate NOT IN (SELECT pnu FROM parcels WHERE pnu IS NOT NULL);
";

const INFERENCE_SETUP_SQL: &str = "
CREATE OR REPLACE TABLE land_parcels AS
  SELECT * FROM land WHERE pnu IN (SELECT pnu FROM parcels WHERE pnu IS NOT NULL);
CREATE OR REPLACE VIEW group_parcels AS SELECT pnu FROM parcels;
CREATE OR REPLACE TABLE inferred (row_id VARCHAR, pnu VARCHAR, pnu_group VARCHAR[], place_identity_grade VARCHAR,
  pnu_unresolved_reason VARCHAR, pnu_candidate_count INTEGER);";

/// Places every deal and leaves `placed`.
///
/// The caller has loaded `deals_raw` (the Silver's current rows, the job's `DEAL_COLUMNS`),
/// `code_changes`, `code_names` (the newest code snapshot), `parcels` (`pnu`), `land` (`pnu`,
/// `land_category`, `area_m2`, `zone_name_1`), `titles` (the main buildings' `pnu`, `approval_year`,
/// `floor_area_m2`), `ownership` (`pnu`, `ownership_changed_on`, `ownership_change_reason`) and, to follow
/// the lineage, `lineage` (`predecessor_pnu`, `successor_pnu`, `relation`, `grade`, `evidence_kind`,
/// `evidence_ref`).
///
/// # Errors
/// A malformed `place_inference`, a lineage grade not known, or a SQL error.
pub fn place(
    connection: &duckdb::Connection,
    inference: &Value,
) -> anyhow::Result<PlacementCounts> {
    place_from(connection, inference, "code_changes", "code_names")
}

/// [`place`] with the code change table and the code snapshot under the names given: the shared
/// versions build keeps a `code_changes` of its own on the connection (the pairs in force only).
///
/// # Errors
/// As [`place`].
pub fn place_from(
    connection: &duckdb::Connection,
    inference: &Value,
    code_changes: &str,
    code_names: &str,
) -> anyhow::Result<PlacementCounts> {
    let (remap, ambiguous) = crosswalk_tables_from(connection, code_changes, code_names)?;
    address_code_table_from(connection, code_names, &remap, &ambiguous)?;
    connection.execute_batch(ADDRESS_KEY_MACRO)?;
    connection.execute_batch(DEALS_SQL)?;
    connection.execute_batch(RESOLVE_SQL)?;
    connection.execute_batch(MASKED_SQL)?;
    connection.execute_batch(INFERENCE_SETUP_SQL)?;
    let rules = inference_rules(inference)?;
    for dataset in measured_datasets(inference) {
        let levels = enabled_levels(inference, &dataset)?;
        if levels.is_empty() {
            continue;
        }
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP VIEW masked AS SELECT * FROM masked_all WHERE dataset_slug = {};
             INSERT INTO inferred SELECT * FROM ({}) WHERE pnu IS NOT NULL OR pnu_group IS NOT NULL;",
            quoted(&dataset),
            inference_sql(&levels, &rules)
        ))?;
    }
    if has_table(connection, "lineage")? {
        connection.execute_batch(CARRY_SOURCES_SQL)?;
    }
    let (lineage_sources, lineage_candidates) =
        crate::lineage_carry_stage::carry_tables(connection)?;
    connection.execute_batch(PLACED_SQL)?;
    Ok(PlacementCounts {
        deals: count(connection, "SELECT count(*) FROM deals")?,
        placed_rows: count(connection, "SELECT count(*) FROM placed")?,
        placed_deals: count(
            connection,
            "SELECT count(DISTINCT row_id) FROM placed WHERE pnu IS NOT NULL",
        )?,
        lineage_sources,
        lineage_candidates,
    })
}

/// The Silver columns a deal is placed and served from (the job's `DEAL_COLUMNS`); never the Silver's
/// own `pnu`, `legal_dong_code` or grade (root ADR-0351 §3).
pub const DEAL_COLUMNS: [&str; 27] = [
    "dataset_slug",
    "deal_ym",
    "row_key",
    "deal_date",
    "deal_amount_10k_krw",
    "deposit_10k_krw",
    "monthly_rent_10k_krw",
    "rent_kind",
    "address_raw",
    "jibun_raw",
    "bonbun",
    "bubun",
    "building_type",
    "building_use",
    "building_name",
    "building_area_m2",
    "building_area_kind",
    "land_area_m2",
    "land_area_kind",
    "floor",
    "build_year",
    "land_use",
    "jimok",
    "share_dealing_type",
    "dealing_gbn",
    "cancelled",
    "cancel_date",
];

fn contract_of(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The rows the newest-per-key window ranks at once: a slice of a table's versions, by a hash of its
/// first key column (a key falls in one slice whole). One window over 82M+ versions of 소유 spilled
/// past 4 GB on 2026-11-19.
const WINDOW_SLICE_ROWS: i64 = 10_000_000;

/// Builds `target` by the current-rows `template` over the local `raw` versions, a slice of
/// [`WINDOW_SLICE_ROWS`] at a time by a hash of `first_key`.
fn windowed_into(
    connection: &duckdb::Connection,
    template: &str,
    projected: &[&str],
    raw: &str,
    target: &str,
    first_key: &str,
) -> anyhow::Result<()> {
    let versions: i64 =
        connection.query_row(&format!("SELECT count(*) FROM {raw}"), [], |row| row.get(0))?;
    let slices = versions / WINDOW_SLICE_ROWS + 1;
    for slice in 0..slices {
        let rows = template
            .replace(CURRENT_ROWS_COLUMNS, &projected.join(", "))
            .replace(CURRENT_ROWS_SOURCE, raw)
            .replace(
                CURRENT_ROWS_FILTER,
                &format!("coalesce(hash({first_key}), 0) % {slices} = {slice}"),
            );
        if slice == 0 {
            connection.execute_batch(&format!("CREATE OR REPLACE TABLE {target} AS {rows}"))?;
        } else {
            connection.execute_batch(&format!("INSERT INTO {target} {rows}"))?;
        }
    }
    Ok(())
}

/// Builds `target` from `table`'s current rows at its pin, projecting `columns`, by the contract's one
/// rendering (root ADR-0564 §4). The pinned table is read once, into a local copy of the columns the
/// rule needs; the rule then runs over it a slice of keys at a time. A table whose rule keeps no ties
/// is refused, on the built table, when two current rows of a key tie.
fn current_rows_into(
    connection: &duckdb::Connection,
    source: &Source<'_>,
    table: &str,
    columns: &[&str],
    target: &str,
) -> anyhow::Result<()> {
    let contract = contract_of(table)?;
    let template = contract
        .current_rows_sql_template()
        .with_context(|| format!("{table} keeps no versions per key"))?;
    let pinned = source.of(table)?;
    // A column the table gained after the pinned snapshot reads as NULL there; a missing required
    // one is refused (the one implementation, `duckdb_parquet_stage::with_contract_columns`).
    let from = crate::duckdb_parquet_stage::with_contract_columns(connection, contract, &pinned)?;
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{table} keeps no versions per key"))?;
    let raw = format!("{target}_versions");
    // The key rides along for the tie check and leaves after it.
    let extra: Vec<&str> = if latest.keep_ties {
        Vec::new()
    } else {
        latest
            .key
            .iter()
            .copied()
            .filter(|column| !columns.contains(column))
            .collect()
    };
    let projected: Vec<&str> = columns
        .iter()
        .copied()
        .chain(extra.iter().copied())
        .collect();
    // One read of the pin: every column the rule or the projection names (all of them when the
    // contract selects by a predicate, whose columns are SQL this reader does not parse).
    let needed = if contract.current_row_predicate.is_some() {
        "*".to_owned()
    } else {
        projected
            .iter()
            .copied()
            .chain(latest.key.iter().copied())
            .chain(latest.newest_first.iter().copied())
            .chain(latest.removal_column)
            .collect::<BTreeSet<&str>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ")
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {raw} AS SELECT {needed} FROM {from}"
    ))?;
    let first_key = latest
        .key
        .first()
        .context("a newest-per-key rule names its key")?;
    windowed_into(connection, &template, &projected, &raw, target, first_key)?;
    connection.execute_batch(&format!("DROP TABLE {raw}"))?;
    if !latest.keep_ties {
        let (rows, keys): (i64, i64) = connection.query_row(
            &format!(
                "SELECT count(*), count(DISTINCT ({})) FROM {target}",
                latest.key.join(", ")
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            rows == keys,
            "{table} ({from}) holds {} current rows tied on their version order",
            rows - keys
        );
    }
    for column in extra {
        connection.execute_batch(&format!("ALTER TABLE {target} DROP COLUMN {column}"))?;
    }
    Ok(())
}

/// Leaves the code tables `place` reads whole, at the pins of `source`: `code_changes` and
/// `code_names` (the newest code snapshot).
///
/// # Errors
/// A pin missing or a SQL error.
pub fn reference_tables(
    connection: &duckdb::Connection,
    source: &Source<'_>,
) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE code_changes AS
           SELECT kind, old_code, new_code, level, effective_date, source FROM {changes};
         CREATE OR REPLACE TABLE code_names AS
           SELECT region_cd, full_name, status FROM {names}
           WHERE snapshot_date = (SELECT max(snapshot_date) FROM {names});",
        changes = source.of(CODE_CHANGE_SOURCE)?,
        names = source.of(LEGAL_DONG_SOURCE)?,
    ))?;
    Ok(())
}

/// Leaves `target`: the 실거래 Silver's current rows at the pin of `source`, the job's [`DEAL_COLUMNS`].
///
/// # Errors
/// A pin missing, a tie the contract forbids, or a SQL error.
pub fn deal_rows_into(
    connection: &duckdb::Connection,
    source: &Source<'_>,
    target: &str,
) -> anyhow::Result<()> {
    current_rows_into(connection, source, RT_SOURCE, &DEAL_COLUMNS, target)
}

/// Leaves `table` (one of [`PNU_INPUTS`]) as `place` reads it, from `input` at the pin of `source`:
/// the served `edition`'s parcels, or a register's current rows.
///
/// # Errors
/// An edition that is not a plain snapshot id, an input that is not one of [`PNU_INPUTS`], a pin
/// missing, or a SQL error.
pub fn pnu_rows_into(
    connection: &duckdb::Connection,
    source: &Source<'_>,
    input: &str,
    table: &str,
    edition: &str,
) -> anyhow::Result<()> {
    ensure!(
        !edition.is_empty()
            && edition
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-')),
        "the edition {edition:?} is not a plain snapshot id"
    );
    ensure!(
        PNU_INPUTS.contains(&(input, table)),
        "{input} as {table} is not an input place reads by PNU"
    );
    match input {
        PARCEL_SOURCE => {
            let predicate = contract_of(PARCEL_SOURCE)?
                .current_row_predicate
                .context("silver.parcel_boundaries names its current rows by a predicate")?;
            connection.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {table} AS
                   SELECT DISTINCT pnu FROM {parcels}
                   WHERE source_snapshot_id = '{edition}' AND ({predicate}) AND pnu IS NOT NULL;",
                parcels = source.of(PARCEL_SOURCE)?,
            ))?;
        }
        LAND_SOURCE => current_rows_into(
            connection,
            source,
            LAND_SOURCE,
            &["pnu", "land_category", "area_m2", "zone_name_1"],
            table,
        )?,
        TITLE_SOURCE => {
            current_rows_into(
                connection,
                source,
                TITLE_SOURCE,
                &[
                    "pnu",
                    "approval_year",
                    "floor_area_m2",
                    "main_or_annex_kind",
                ],
                table,
            )?;
            connection.execute_batch(&format!(
                "DELETE FROM {table} WHERE main_or_annex_kind IS DISTINCT FROM 'main';
                 ALTER TABLE {table} DROP COLUMN main_or_annex_kind;"
            ))?;
        }
        _ => current_rows_into(
            connection,
            source,
            OWNERSHIP_SOURCE,
            &["pnu", "ownership_changed_on", "ownership_change_reason"],
            table,
        )?,
    }
    Ok(())
}

#[cfg(test)]
mod address_tests {
    //! The 실거래 load's address rule (`rt_molit_real_transaction.dong_index`), case for case.
    use super::*;

    fn map(rows: &[(&str, &str)]) -> BTreeMap<String, String> {
        rows.iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect()
    }

    #[test]
    fn a_current_code_wins_and_a_shared_name_is_left_out() {
        let index = dong_index([
            ("9999910100", "Synth  City   Eastgu Adong", "존재"),
            ("9999910200", "Synth City Eastgu Adong", "폐지"),
            ("9999910300", "Synth City Eastgu Bdong", "존재"),
            ("9999910400", "Synth City Eastgu Bdong", "존재"),
            ("9999900000", "Synth City Eastgu", "존재"),
            ("99999103", "Short", "존재"),
        ]);
        assert_eq!(index, map(&[("Synth City Eastgu Adong", "9999910100")]));
    }

    #[test]
    fn an_address_takes_its_current_code_through_the_pairs_in_force() {
        let index = map(&[
            ("A", "9999910100"),
            ("B", "9999920100"),
            ("C", "9999930100"),
        ]);
        let remap = map(&[("9999910100", "9999940100")]);
        let ambiguous = BTreeSet::from(["9999920100".to_owned()]);
        assert_eq!(
            address_codes(&index, &remap, &ambiguous),
            vec![
                (
                    "A".to_owned(),
                    "9999910100".to_owned(),
                    Some("9999940100".to_owned())
                ),
                ("B".to_owned(), "9999920100".to_owned(), None),
                (
                    "C".to_owned(),
                    "9999930100".to_owned(),
                    Some("9999930100".to_owned())
                ),
            ]
        );
    }
}

#[cfg(test)]
pub(crate) mod tests;
