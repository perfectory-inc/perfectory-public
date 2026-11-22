//! The 실거래 kind's Gold rows (root ADR-0350, ADR-0371) in `DuckDB`, one closed region at a time, for
//! `gold.parcel_real_transaction_versions` (root ADR-0564 §6).
//!
//! Every input is read once into a spool split by 시군구 (a PNU's first five digits). The 시군구 are then
//! joined into units no placement crosses: a code pair of the crosswalk, a served form and a lineage
//! row each link the 시군구 of their two codes, so every hop `real_transaction_placement_stage::place`
//! can take stays inside one unit (강원·전북's new codes, 광주·전남's merge, 군위's move to 대구).
//! Each unit is placed and written (`real_transaction_gold_stage::gold_rows`) into `built`.
//!
//! The 실거래 Silver is the one input read whole into the stage: its current rows are chosen per
//! (dataset, 계약 달, row key), which no region bounds, and `history_coverage` is defined over all of it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

use anyhow::{ensure, Context as _};
use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::parcel_nearby_stage::Source;
use crate::place_id_join::{place_ids_sql, BRIDGE_SOURCE};
use crate::real_transaction_gold_stage as gold;
use crate::real_transaction_placement_stage as placement;

/// Every input, in the order the summary names them.
pub const INPUTS: [&str; 9] = [
    placement::RT_SOURCE,
    placement::PARCEL_SOURCE,
    placement::LAND_SOURCE,
    placement::TITLE_SOURCE,
    placement::OWNERSHIP_SOURCE,
    placement::LINEAGE_SOURCE,
    placement::CODE_CHANGE_SOURCE,
    placement::LEGAL_DONG_SOURCE,
    BRIDGE_SOURCE,
];

/// The deals a unit takes before the next one starts (a 시군구 component larger than this is one unit).
/// 경기 whole (4.8M deals) did not fit 12 GB in the 2026-11-21 verdict and 부산 whole (617k) not its
/// documents' 12 GB before they were built per 시군구; the unit's `MemoryMax` is 8G, of which `DuckDB`
/// takes three fifths. A first bound to measure, not a measured one.
pub const UNIT_DEAL_BUDGET: u64 = 700_000;

/// The rules version a change of which, with the SQL texts, rebuilds every PNU.
const RULES_VERSION: &str = "rt-versions-1";

/// The table each unit's rows are appended to: the replaced table's content columns, in its order.
pub const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, real_transaction_json VARCHAR, \
    transaction_total BIGINT, attached_via VARCHAR, place_id VARCHAR, main_lot_group_place_ids VARCHAR, \
    row_digest VARCHAR, source_snapshot_id VARCHAR, published_at_utc VARCHAR);";

/// The build's code version: the placement's and the document's SQL, the spool's, and the rules.
#[must_use]
pub fn code_version(inference: &Value) -> String {
    let mut hash = Sha256::new();
    let document = gold::code_texts();
    let inference = inference.to_string();
    for text in placement::CODE_TEXTS
        .into_iter()
        .chain(document.iter().map(String::as_str))
        .chain([RULES_VERSION, inference.as_str()])
    {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// One unit: the 시군구 it builds and the deals they hold.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Unit {
    /// 시군구 codes (five digits).
    pub nodes: BTreeSet<String>,
    /// The deals whose address names one of them.
    pub deals: u64,
}

/// The root of `node` in the union-find `parent`, every node on the way pointed at it.
fn root(parent: &mut BTreeMap<String, String>, node: &str) -> String {
    let mut at = node.to_owned();
    while let Some(up) = parent.get(&at).filter(|up| **up != at).cloned() {
        at = up;
    }
    let mut walk = node.to_owned();
    while walk != at {
        match parent.insert(walk.clone(), at.clone()) {
            Some(up) => walk = up,
            None => break,
        }
    }
    at
}

/// The units of `deals_by_node` (deals per 시군구) under `edges` (pairs of 시군구 a placement links):
/// each connected component whole, components packed largest first into units of at most `budget`
/// deals (a larger component alone). Deterministic: the same inputs give the same units.
#[must_use]
pub fn units(
    deals_by_node: &BTreeMap<String, u64>,
    edges: &[(String, String)],
    budget: u64,
) -> Vec<Unit> {
    let mut parent: BTreeMap<String, String> = deals_by_node
        .keys()
        .chain(edges.iter().flat_map(|(a, b)| [a, b]))
        .map(|node| (node.clone(), node.clone()))
        .collect();
    for (a, b) in edges {
        let (ra, rb) = (root(&mut parent, a), root(&mut parent, b));
        if ra != rb {
            let (low, high) = if ra < rb { (ra, rb) } else { (rb, ra) };
            parent.insert(high, low);
        }
    }
    let nodes: Vec<String> = parent.keys().cloned().collect();
    let mut components: BTreeMap<String, Unit> = BTreeMap::new();
    for node in nodes {
        let top = root(&mut parent, &node);
        let unit = components.entry(top).or_insert_with(|| Unit {
            nodes: BTreeSet::new(),
            deals: 0,
        });
        unit.deals += deals_by_node.get(&node).copied().unwrap_or(0);
        unit.nodes.insert(node);
    }
    let mut components: Vec<Unit> = components.into_values().collect();
    components.sort_by(|a, b| b.deals.cmp(&a.deals).then_with(|| a.nodes.cmp(&b.nodes)));
    let mut packed: Vec<Unit> = Vec::new();
    for component in components {
        match packed
            .iter_mut()
            .find(|unit| unit.deals + component.deals <= budget)
        {
            Some(unit) => {
                unit.deals += component.deals;
                unit.nodes.extend(component.nodes);
            }
            None => packed.push(component),
        }
    }
    packed
}

/// What one [`spool`] read and left.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Spooled {
    /// Current deals of the whole Silver.
    pub deals: u64,
    /// Of them, deals whose address names no 법정동 of the code snapshot (placed nowhere).
    pub unaddressed: u64,
    /// The served edition's parcels.
    pub served_parcels: u64,
    /// The (dataset, 계약 달 `YYYYMM`) pairs of the whole Silver, for `history_coverage`.
    pub months: Vec<(String, String)>,
    /// The units, in build order.
    pub units: Vec<Unit>,
    /// Milliseconds each input took to read, by table name.
    pub read_millis: BTreeMap<String, u64>,
    /// Per 시군구, a digest of every spooled row in it (rows and the sum of their hashes, per
    /// table): a 시군구 whose digest stayed builds the same documents.
    pub node_digests: BTreeMap<String, String>,
    /// A digest of what every document reads whatever its 시군구: the code tables and the whole
    /// Silver's (dataset, 계약 달) pairs.
    pub global_digest: String,
}

fn sql_dir(dir: &Path) -> anyhow::Result<String> {
    let text = dir
        .to_str()
        .context("the spool directory must be UTF-8")?
        .replace('\\', "/");
    ensure!(
        !text.contains('\''),
        "the spool directory must not contain a quote"
    );
    Ok(text)
}

/// A spooled table's files, their `node` read back as text.
fn spooled(dir: &str, name: &str) -> String {
    format!(
        "read_parquet('{dir}/{name}/*/*.parquet', hive_partitioning = true, hive_types_autocast = false)"
    )
}

fn millis(at: Instant) -> u64 {
    u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `COPY` of `query` to `<dir>/<name>`, one directory per value of its `node` column.
fn copy_by_node(connection: &Connection, query: &str, dir: &str, name: &str) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "COPY ({query}) TO '{dir}/{name}' \
         (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (node), OVERWRITE_OR_IGNORE)"
    ))?;
    Ok(())
}

fn count(connection: &Connection, sql: &str) -> anyhow::Result<u64> {
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(u64::try_from(value)?)
}

/// Leaves the code tables `place` reads whole from a spool [`spool`] wrote, so a run that resumes it
/// reads nothing from the lakehouse again.
///
/// # Errors
/// A spool without them, or a SQL error.
pub fn load_reference(connection: &Connection, dir: &Path) -> anyhow::Result<()> {
    let dir = sql_dir(dir)?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE code_changes AS SELECT * FROM read_parquet('{dir}/code_changes.parquet');
         CREATE OR REPLACE TABLE code_names AS SELECT * FROM read_parquet('{dir}/code_names.parquet');"
    ))?;
    Ok(())
}

/// Reads every input at `source` once, for the served `edition`, into `dir`, and leaves the code
/// tables (`code_changes`, `code_names`, `crosswalk`, `ambiguous`, `served_forms`, `address_codes`).
///
/// # Errors
/// An edition that is not a plain snapshot id or holds no parcel, a pin missing, a tie the contract
/// forbids, or a SQL error.
pub fn spool(
    connection: &Connection,
    source: &Source<'_>,
    edition: &str,
    dir: &Path,
) -> anyhow::Result<Spooled> {
    let dir = sql_dir(dir)?;
    let mut read_millis = BTreeMap::new();
    let at = Instant::now();
    placement::reference_tables(connection, source)?;
    connection.execute_batch(&format!(
        "COPY code_changes TO '{dir}/code_changes.parquet' (FORMAT parquet);
         COPY code_names TO '{dir}/code_names.parquet' (FORMAT parquet);"
    ))?;
    let (remap, ambiguous) = placement::crosswalk_tables(connection)?;
    placement::address_code_table(connection, &remap, &ambiguous)?;
    read_millis.insert(placement::CODE_CHANGE_SOURCE.to_owned(), millis(at));

    // The deals, once, by the 시군구 their address names (`deals.sql` reads the same key).
    let at = Instant::now();
    placement::deal_rows_into(connection, source, "deals_raw")?;
    connection.execute_batch(placement::ADDRESS_KEY_MACRO)?;
    copy_by_node(
        connection,
        "SELECT d.*, substr(c.source_legal_dong, 1, 5) AS node FROM deals_raw d \
         JOIN address_codes c ON c._address = rt_address_key(d.address_raw)",
        &dir,
        "deals",
    )?;
    let deals = count(connection, "SELECT count(*) FROM deals_raw")?;
    let addressed = count(
        connection,
        "SELECT count(*) FROM deals_raw d JOIN address_codes c ON c._address = rt_address_key(d.address_raw)",
    )?;
    let months = connection
        .prepare(
            "SELECT DISTINCT dataset_slug, deal_ym FROM deals_raw \
             WHERE dataset_slug IS NOT NULL AND deal_ym IS NOT NULL ORDER BY 1, 2",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<(String, String)>, _>>()?;
    connection.execute_batch("DROP TABLE deals_raw")?;
    read_millis.insert(placement::RT_SOURCE.to_owned(), millis(at));

    // Each PNU-keyed input, once, by its PNU's 시군구.
    for (input, table) in placement::PNU_INPUTS {
        let at = Instant::now();
        placement::pnu_rows_into(connection, source, input, table, edition)?;
        copy_by_node(
            connection,
            &format!("SELECT *, substr(pnu, 1, 5) AS node FROM {table}"),
            &dir,
            table,
        )?;
        connection.execute_batch(&format!("DROP TABLE {table}"))?;
        read_millis.insert(input.to_owned(), millis(at));
    }
    let served_parcels = count(
        connection,
        &format!("SELECT count(*) FROM read_parquet('{dir}/parcels/*/*.parquet')"),
    )?;
    ensure!(
        served_parcels > 0,
        "edition {edition} holds no served parcel"
    );
    let at = Instant::now();
    copy_by_node(
        connection,
        &format!(
            "SELECT predecessor_pnu, successor_pnu, relation, grade, evidence_kind, evidence_ref, \
               substr(coalesce(predecessor_pnu, successor_pnu), 1, 5) AS node FROM {}",
            source.of(placement::LINEAGE_SOURCE)?
        ),
        &dir,
        "lineage",
    )?;
    read_millis.insert(placement::LINEAGE_SOURCE.to_owned(), millis(at));
    let at = Instant::now();
    copy_by_node(
        connection,
        &format!(
            "SELECT pnu, place_id, substr(pnu, 1, 5) AS node FROM ({})",
            place_ids_sql(source, "TRUE")?
        ),
        &dir,
        "place_ids",
    )?;
    read_millis.insert(BRIDGE_SOURCE.to_owned(), millis(at));

    // The units: 시군구 a code pair, a served form or a lineage row links are one.
    let deals_by_node = connection
        .prepare(&format!(
            "SELECT node, count(*) FROM {} GROUP BY node",
            spooled(&dir, "deals")
        ))?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .map(|row| {
            let (node, n) = row?;
            Ok((node, u64::try_from(n)?))
        })
        .collect::<anyhow::Result<BTreeMap<String, u64>>>()?;
    let edges = connection
        .prepare(&format!(
            "SELECT DISTINCT a, b FROM ( \
               SELECT substr(old_code, 1, 5) AS a, substr(current_code, 1, 5) AS b FROM crosswalk \
               UNION ALL SELECT substr(_code10, 1, 5), substr(_form10, 1, 5) FROM served_forms \
               UNION ALL SELECT substr(predecessor_pnu, 1, 5), substr(successor_pnu, 1, 5) FROM {}) \
             WHERE a IS NOT NULL AND b IS NOT NULL AND a <> b ORDER BY 1, 2",
            spooled(&dir, "lineage")
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<(String, String)>, _>>()?;
    let node_digests = node_digests(connection, &dir)?;
    let codes: String = connection.query_row(
        "SELECT (SELECT count(*) || ':' || coalesce(sum(hash(t)::HUGEINT), 0) FROM code_changes t) || '/' || \
                (SELECT count(*) || ':' || coalesce(sum(hash(t)::HUGEINT), 0) FROM code_names t)",
        [],
        |row| row.get(0),
    )?;
    let mut global = Sha256::new();
    global.update(codes.as_bytes());
    for (dataset, month) in &months {
        global.update(format!("|{dataset}:{month}").as_bytes());
    }
    Ok(Spooled {
        deals,
        unaddressed: deals.saturating_sub(addressed),
        served_parcels,
        months,
        units: units(&deals_by_node, &edges, UNIT_DEAL_BUDGET),
        read_millis,
        node_digests,
        global_digest: format!("{:x}", global.finalize()),
    })
}

/// The tables a spool holds by 시군구.
const SPOOLED_TABLES: [&str; 7] = [
    "deals",
    "parcels",
    "land",
    "titles",
    "ownership",
    "lineage",
    "place_ids",
];

/// Per 시군구 of the spool in `dir`, `<table>=<rows>:<sum of row hashes>` for each table that holds a
/// row of it: the same rows in any order give the same digest, and any row added, removed or changed
/// in a 시군구 changes that 시군구's digest alone.
///
/// # Errors
/// A spool that does not hold every table, or a SQL error.
pub fn node_digests(
    connection: &Connection,
    dir: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let union = SPOOLED_TABLES
        .iter()
        .map(|name| {
            format!(
                "SELECT '{name}' AS name, node, count(*) AS n, sum(hash(t)::HUGEINT) AS s FROM {} t GROUP BY node",
                spooled(dir, name)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let digests = connection
        .prepare(&format!(
            "SELECT node, string_agg(name || '=' || n || ':' || s, ',' ORDER BY name) FROM ({union}) GROUP BY node"
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<BTreeMap<String, String>, _>>()?;
    Ok(digests)
}

/// What every unit of one build shares.
pub struct UnitBuild<'a> {
    /// The 실거래 contract's `place_inference`.
    pub inference: &'a Value,
    /// The build month (its first day).
    pub as_of: NaiveDate,
    /// The whole Silver's coverage (`history_coverage`).
    pub coverage: &'a [gold::Coverage],
    /// The commit's `source_snapshot_id` stamp.
    pub source_snapshot_id: &'a str,
    /// The commit's `published_at_utc` stamp.
    pub published_at_utc: &'a str,
}

/// Places `unit`'s deals and appends its Gold rows to `built` (created by [`BUILT_TABLE`]); returns
/// the rows.
///
/// # Errors
/// A 시군구 that is not five digits, a stamp that is not plain, a placement that leaves the unit (a
/// link the units missed), or a SQL error.
pub fn build_unit(
    connection: &Connection,
    dir: &Path,
    unit: &Unit,
    build: &UnitBuild<'_>,
) -> anyhow::Result<u64> {
    let dir = sql_dir(dir)?;
    let (source_snapshot_id, published_at_utc) = (build.source_snapshot_id, build.published_at_utc);
    for node in &unit.nodes {
        ensure!(
            node.len() == 5 && node.bytes().all(|b| b.is_ascii_digit()),
            "the 시군구 {node:?} is not five digits"
        );
    }
    for stamp in [source_snapshot_id, published_at_utc] {
        ensure!(
            stamp
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_:.TZ".contains(&b)),
            "the stamp {stamp:?} is not plain"
        );
    }
    let nodes = unit
        .nodes
        .iter()
        .map(|node| format!("'{node}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = [
        ("deals_raw", "deals"),
        ("parcels", "parcels"),
        ("land", "land"),
        ("titles", "titles"),
        ("ownership", "ownership"),
        ("lineage", "lineage"),
        ("place_ids", "place_ids"),
    ]
    .iter()
    .map(|(table, name)| {
        format!(
            "CREATE OR REPLACE TABLE {table} AS SELECT * EXCLUDE (node) FROM {} WHERE node IN ({nodes});",
            spooled(&dir, name)
        )
    })
    .collect::<Vec<_>>()
    .join("\n");
    connection.execute_batch(&sql)?;
    placement::place(connection, build.inference)?;
    let outside = count(
        connection,
        &format!(
            "SELECT count(*) FROM placed WHERE pnu IS NOT NULL AND substr(pnu, 1, 5) NOT IN ({nodes})"
        ),
    )?;
    ensure!(
        outside == 0,
        "{outside} deals of a unit were placed outside it: a link the units did not join"
    );
    gold::gold_rows(connection, build.as_of, build.coverage)?;
    connection.execute_batch(&format!(
        "INSERT INTO built SELECT pnu, real_transaction_json, transaction_total, attached_via, place_id, \
           main_lot_group_place_ids, row_digest, '{source_snapshot_id}', '{published_at_utc}' FROM gold_rows;"
    ))?;
    count(connection, "SELECT count(*) FROM gold_rows")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deals(rows: &[(&str, u64)]) -> BTreeMap<String, u64> {
        rows.iter().map(|(n, d)| ((*n).to_owned(), *d)).collect()
    }

    fn edge(a: &str, b: &str) -> (String, String) {
        (a.to_owned(), b.to_owned())
    }

    fn nodes(unit: &Unit) -> Vec<&str> {
        unit.nodes.iter().map(String::as_str).collect()
    }

    /// A spool whose every table holds `rows` of `(시군구, value)`.
    fn spool_of(rows: &[(&str, &str)]) -> anyhow::Result<(tempfile::TempDir, Connection, String)> {
        let dir = tempfile::tempdir()?;
        let connection = Connection::open_in_memory()?;
        let text = sql_dir(dir.path())?;
        let values = rows
            .iter()
            .map(|(node, value)| format!("('{node}', '{value}')"))
            .collect::<Vec<_>>()
            .join(", ");
        for name in SPOOLED_TABLES {
            copy_by_node(
                &connection,
                &format!("SELECT * FROM (VALUES {values}) AS r(node, v)"),
                &text,
                name,
            )?;
        }
        Ok((dir, connection, text))
    }

    #[test]
    fn a_sigungu_digest_moves_with_its_own_rows_and_not_their_order() -> anyhow::Result<()> {
        let (_a, a, a_dir) = spool_of(&[("99901", "x"), ("99901", "y"), ("99902", "z")])?;
        let (_b, b, b_dir) = spool_of(&[("99902", "z"), ("99901", "y"), ("99901", "x")])?;
        let (_c, c, c_dir) = spool_of(&[("99901", "x"), ("99901", "y"), ("99902", "w")])?;
        let first = node_digests(&a, &a_dir)?;
        assert_eq!(
            first,
            node_digests(&b, &b_dir)?,
            "the order of the rows does not count"
        );
        let changed = node_digests(&c, &c_dir)?;
        assert_eq!(
            first.get("99901"),
            changed.get("99901"),
            "an untouched 시군구 keeps its digest"
        );
        assert_ne!(
            first.get("99902"),
            changed.get("99902"),
            "a changed row moves its 시군구's"
        );
        Ok(())
    }

    #[test]
    fn a_link_across_two_sigungu_keeps_them_in_one_unit() {
        // A moved 군 (old code 99901 -> 99951) and a lineage row out of 99902: the three are one unit
        // however small the budget, the unlinked 99903 its own.
        let built = units(
            &deals(&[("99901", 5), ("99951", 5), ("99902", 5), ("99903", 5)]),
            &[edge("99901", "99951"), edge("99951", "99902")],
            1,
        );
        assert_eq!(built.len(), 2);
        assert_eq!(nodes(&built[0]), ["99901", "99902", "99951"]);
        assert_eq!(built[0].deals, 15);
        assert_eq!(nodes(&built[1]), ["99903"]);
    }

    #[test]
    fn unlinked_sigungu_are_packed_up_to_the_budget_largest_first() {
        let built = units(
            &deals(&[("99901", 6), ("99902", 4), ("99903", 3), ("99904", 1)]),
            &[],
            7,
        );
        let sets: Vec<Vec<&str>> = built.iter().map(nodes).collect();
        assert_eq!(sets, [vec!["99901", "99904"], vec!["99902", "99903"]]);
        assert!(built.iter().all(|unit| unit.deals <= 7));
    }

    #[test]
    fn a_sigungu_only_an_edge_names_is_still_built() {
        // A served form whose 시군구 holds no deal: its parcels still get documents.
        let built = units(&deals(&[("99901", 2)]), &[edge("99901", "99909")], 10);
        assert_eq!(built.len(), 1);
        assert_eq!(nodes(&built[0]), ["99901", "99909"]);
    }

    #[test]
    fn the_units_do_not_depend_on_the_order_of_their_inputs() {
        let a = units(
            &deals(&[("99901", 3), ("99902", 3), ("99903", 3)]),
            &[edge("99903", "99901")],
            4,
        );
        let b = units(
            &deals(&[("99903", 3), ("99901", 3), ("99902", 3)]),
            &[edge("99901", "99903")],
            4,
        );
        assert_eq!(a, b);
    }
}
