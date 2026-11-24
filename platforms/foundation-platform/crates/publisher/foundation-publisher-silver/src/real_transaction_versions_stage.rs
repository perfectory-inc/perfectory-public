//! The 실거래 kind's Gold rows (root ADR-0350, ADR-0371) as a stage of the shared versions build
//! (root ADR-0564 §6, ADR-0615 §7), for `gold.parcel_real_transaction_versions`.
//!
//! A deal has no PNU until it is placed, and a placement may leave the deal's own 시군구: a code pair
//! of the crosswalk, an earlier served form of a code and a lineage row each link the 시군구 of
//! their two codes (강원·전북's new codes, 광주·전남's renumbering, 군위's move to 대구). So the
//! stage reads its inputs itself ([`KindVersionsStage::spool_more`]) and gives each row a `part`:
//! the 시군구 that reads it. A row is spooled once under its own 시군구 and once under every other
//! 시군구 of its linked group (the connected component of those links: a lineage chain may cross
//! three), so the build of one part holds every row a placement into it can touch and nothing else.
//! The shared build shows the stage one part at a time; the stage places the part's deals
//! (`real_transaction_placement_stage::place`), writes their documents
//! (`real_transaction_gold_stage::gold_rows`) and keeps those whose PNU lies in the part.
//!
//! Measured 2026-11-24 on production (the lineage and the code change table): 72 시군구 are linked,
//! in 32 groups of 2 to 5, 28 of them across 시도; every other 시군구 reads itself alone.
//!
//! The stage owns no split, loop, budget or resume: on 2026-11-23 and 11-24 the lane's own spool
//! by 시군구 (1,790 files) failed 16 runs of 16, first on the unit's open files and then on a
//! register row without a PNU, whose 시군구 is none.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::{ensure, Context as _};
use chrono::{Datelike as _, NaiveDate};
use duckdb::Connection;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::kind_versions_stage::{
    elapsed, is_plain, KindVersionsStage, RowSet, BUILD_PART, PART_DIGITS,
};
use crate::parcel_nearby_stage::Source;
use crate::place_id_join::BRIDGE_SOURCE;
use crate::real_transaction_gold_stage as gold;
use crate::real_transaction_placement_stage as placement;

/// The rules version a change of which, with the SQL texts, rebuilds every PNU.
const RULES_VERSION: &str = "rt-versions-2";

/// `rt-molit-real-transaction.contract.json`: the placement's inference rules and each dataset's
/// first month.
const RT_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/rt-molit-real-transaction.contract.json");

/// The table a part's rows are appended to: the replaced table's content columns, in its order.
pub const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, real_transaction_json VARCHAR, \
    transaction_total BIGINT, attached_via VARCHAR, place_id VARCHAR, main_lot_group_place_ids VARCHAR, \
    row_digest VARCHAR, source_snapshot_id VARCHAR, published_at_utc VARCHAR);";

/// The spools the stage writes, each row with the part that reads it, and the table
/// `placement::place` reads each as.
const PART_TABLES: [(&str, &str); 6] = [
    ("rt_deals", "deals_raw"),
    ("rt_parcels", "parcels"),
    ("rt_land", "land"),
    ("rt_titles", "titles"),
    ("rt_ownership", "ownership"),
    ("rt_lineage", "lineage"),
];
/// The (dataset, 계약 달) pairs the whole Silver holds: `history_coverage` reads every one.
const MONTHS: &str = "rt_months";
/// The code change table and the newest code snapshot, whole, as the placement reads them. The
/// shared build keeps a `code_changes` of its own (the pairs in force only).
const CODE_CHANGES: &str = "rt_code_changes";
const CODE_NAMES: &str = "rt_code_names";
/// `(node, part)`: the other 시군구 (`part`) that read a row of `node`.
const READERS: &str = "rt_part_readers";

/// The 실거래 kind.
#[derive(Clone, Copy, Debug)]
pub struct RealTransactionKind {
    /// The leading digits of a code or PNU that name its part: a 시군구's five (a test's synthetic
    /// codes share their first five, so it names more).
    part_digits: usize,
}

/// The one 실거래 kind.
pub const REAL_TRANSACTION: RealTransactionKind = RealTransactionKind {
    part_digits: PART_DIGITS,
};

fn rt_contract() -> anyhow::Result<&'static Value> {
    static CONTRACT: OnceLock<Option<Value>> = OnceLock::new();
    CONTRACT
        .get_or_init(|| serde_json::from_str(RT_CONTRACT).ok())
        .as_ref()
        .context("rt-molit-real-transaction.contract.json is not JSON")
}

/// The build month's first day, taken once: every document names it (`as_of_month`, and the
/// windows it opens), so the code version carries it and a new month rebuilds every PNU.
fn build_month() -> NaiveDate {
    static MONTH: OnceLock<NaiveDate> = OnceLock::new();
    *MONTH.get_or_init(|| {
        let today = chrono::Utc::now().date_naive();
        today.with_day(1).unwrap_or(today)
    })
}

/// The rules' version: the placement's and the document's SQL, this stage's and the rules.
fn rules_version(inference: &Value) -> String {
    let mut hash = Sha256::new();
    let document = gold::code_texts();
    let inference = inference.to_string();
    for text in placement::CODE_TEXTS
        .into_iter()
        .chain(crate::lineage_carry_stage::SQL)
        .chain(document.iter().map(String::as_str))
        .chain([
            RULES_VERSION,
            SPOOL_SQL,
            CHANGED_PARTS_SQL,
            inference.as_str(),
        ])
    {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
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

/// `(node, part)` for every two different 시군구 of one linked group under `edges`: `part` reads
/// the rows of `node`. The group is the connected component, so a chain of links is one group.
#[must_use]
pub fn readers(edges: &[(String, String)]) -> BTreeSet<(String, String)> {
    let mut parent: BTreeMap<String, String> = BTreeMap::new();
    for (a, b) in edges {
        let (a, b) = (root(&mut parent, a), root(&mut parent, b));
        if a != b {
            parent.insert(a.clone(), b.clone());
        }
        parent.entry(a).or_insert_with(|| b.clone());
        parent.entry(b.clone()).or_insert(b);
    }
    let nodes: Vec<String> = parent.keys().cloned().collect();
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for node in nodes {
        groups
            .entry(root(&mut parent, &node))
            .or_default()
            .push(node);
    }
    let mut pairs = BTreeSet::new();
    for members in groups.values() {
        for node in members {
            for part in members.iter().filter(|part| *part != node) {
                pairs.insert((node.clone(), part.clone()));
            }
        }
    }
    pairs
}

/// One input with the 시군구 (`node`) each row lies in, written once for every part that reads it.
///
/// `@query@` holds a `node` column; a row without one names no place and is not spooled. The file
/// carries the query's columns less `node`, then `part`; the 시도 is the directory.
const SPOOL_SQL: &str = "CREATE OR REPLACE TABLE @shape@ AS \
       SELECT * EXCLUDE (node), node AS part FROM (@query@) LIMIT 0; \
     COPY (SELECT * EXCLUDE (node), substr(part, 1, 2) AS sido FROM ( \
             SELECT t.*, t.node AS part FROM (@query@) t WHERE t.node IS NOT NULL \
             UNION ALL \
             SELECT t.*, r.part FROM (@query@) t JOIN rt_part_readers r ON r.node = t.node)) \
     TO '@dir@/@name@' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (sido), OVERWRITE_OR_IGNORE);";

/// The parts one spool's two reads differ in: a part's rows as a multiset, by their count and the
/// sum of their hashes (a row added, removed or changed moves its part, and so every part that
/// reads it, since the row is spooled under each).
const CHANGED_PARTS_SQL: &str = "SELECT coalesce(a.part, b.part) AS part FROM \
       (SELECT part, count(*) AS n, sum(hash(t)::HUGEINT) AS s \
        FROM read_parquet('@dir@/@name@/*/*.parquet', hive_partitioning = false) t GROUP BY part) a \
     FULL JOIN \
       (SELECT part, count(*) AS n, sum(hash(t)::HUGEINT) AS s \
        FROM read_parquet('@dir@/@name@_old/*/*.parquet', hive_partitioning = false) t GROUP BY part) b \
     ON a.part = b.part \
     WHERE a.n IS DISTINCT FROM b.n OR a.s IS DISTINCT FROM b.s";

fn fill(template: &str, values: &[(&str, &str)]) -> String {
    crate::kind_versions_stage::fill(template, values)
}

fn count(connection: &Connection, sql: &str) -> anyhow::Result<u64> {
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(u64::try_from(value)?)
}

/// Writes `query`'s rows (with their `node`) as the spool `name<suffix>`; returns the rows without
/// a 시군구, which are not spooled.
fn spool_by_part(
    connection: &Connection,
    query: &str,
    name: &str,
    suffix: &str,
    dir_text: &str,
) -> anyhow::Result<u64> {
    connection
        .execute_batch(&fill(
            SPOOL_SQL,
            &[
                ("query", query),
                ("shape", &format!("rt_shape_{name}")),
                ("dir", dir_text),
                ("name", &format!("{name}{suffix}")),
            ],
        ))
        .with_context(|| format!("spooling {name}{suffix} by the part that reads each row"))?;
    count(
        connection,
        &format!("SELECT count(*) FROM ({query}) WHERE node IS NULL"),
    )
}

/// The deals a read holds whole until they are spooled.
const DEALS_READ: &str = "rt_deals_all";
/// The lineage a read holds whole until it is spooled.
const LINEAGE_READ: &str = "rt_lineage_all";

impl RealTransactionKind {
    /// What the placement derives from the code tables ([`CODE_CHANGES`], [`CODE_NAMES`]): the
    /// crosswalk, the codes that split, the served forms and the address codes.
    fn reference(connection: &Connection) -> anyhow::Result<()> {
        let (remap, ambiguous) =
            placement::crosswalk_tables_from(connection, CODE_CHANGES, CODE_NAMES)?;
        placement::address_code_table_from(connection, CODE_NAMES, &remap, &ambiguous)?;
        connection.execute_batch(placement::ADDRESS_KEY_MACRO)?;
        Ok(())
    }

    /// Leaves the (dataset, 계약 달) pairs [`DEALS_READ`] holds as `rt_months<suffix>`.
    fn months(connection: &Connection, suffix: &str) -> anyhow::Result<()> {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {MONTHS}{suffix} AS \
             SELECT DISTINCT dataset_slug, deal_ym FROM {DEALS_READ} \
             WHERE dataset_slug IS NOT NULL AND deal_ym IS NOT NULL;"
        ))?;
        Ok(())
    }

    /// Leaves [`READERS`]: the parts the crosswalk, the served forms and the lineage
    /// ([`LINEAGE_READ`]) link, each reading the rows of every other part of its group.
    fn links(&self, connection: &Connection) -> anyhow::Result<()> {
        let digits = self.part_digits;
        let edges = connection
            .prepare(&format!(
                "SELECT DISTINCT a, b FROM ( \
                   SELECT substr(old_code, 1, {digits}) AS a, substr(current_code, 1, {digits}) AS b FROM crosswalk \
                   UNION ALL SELECT substr(_code10, 1, {digits}), substr(_form10, 1, {digits}) FROM served_forms \
                   UNION ALL SELECT substr(predecessor_pnu, 1, {digits}), substr(successor_pnu, 1, {digits}) \
                             FROM {LINEAGE_READ}) \
                 WHERE a IS NOT NULL AND b IS NOT NULL AND a <> b ORDER BY 1, 2"
            ))?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<(String, String)>, _>>()?;
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {READERS} (node VARCHAR, part VARCHAR);"
        ))?;
        let mut appender = connection.appender(READERS)?;
        for (node, part) in readers(&edges) {
            appender.append_row(duckdb::params![node, part])?;
        }
        appender.flush()?;
        Ok(())
    }

    /// The lineage rows with the part each lies in: its predecessor's, else its successor's.
    fn lineage_query(&self) -> String {
        format!(
            "SELECT *, substr(coalesce(predecessor_pnu, successor_pnu), 1, {}) AS node FROM {LINEAGE_READ}",
            self.part_digits
        )
    }

    /// The deals with the part each lies in: the one its address names.
    fn deals_query(&self) -> String {
        format!(
            "SELECT d.*, substr(c.source_legal_dong, 1, {}) AS node FROM {DEALS_READ} d \
             LEFT JOIN address_codes c ON c._address = rt_address_key(d.address_raw)",
            self.part_digits
        )
    }

    /// The rows of `relation` (a `pnu` column) with the part each lies in.
    fn pnu_query(&self, relation: &str) -> String {
        format!(
            "SELECT *, substr(pnu, 1, {}) AS node FROM {relation}",
            self.part_digits
        )
    }

    /// The whole Silver's coverage as of the build month (`history_coverage`), from the months the
    /// read left.
    fn coverage(connection: &Connection, as_of: NaiveDate) -> anyhow::Result<Vec<gold::Coverage>> {
        let contract = rt_contract()?;
        let first_months: BTreeMap<String, String> = contract["datasets"]
            .as_object()
            .map(|datasets| {
                datasets
                    .iter()
                    .filter_map(|(slug, dataset)| {
                        Some((slug.clone(), dataset["first_month"].as_str()?.to_owned()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let recent = u32::try_from(
            contract["recent_history_months"]
                .as_u64()
                .context("the 실거래 contract names no recent_history_months")?,
        )?;
        let months = connection
            .prepare(&format!(
                "SELECT dataset_slug, deal_ym FROM {MONTHS} ORDER BY 1, 2"
            ))?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<(String, String)>, _>>()?;
        gold::history_coverage(
            months
                .iter()
                .map(|(dataset, month)| (dataset.as_str(), month.as_str())),
            &first_months,
            as_of,
            recent,
        )
    }
}

impl KindVersionsStage for RealTransactionKind {
    fn section_column(&self) -> &'static str {
        "real_transaction_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![
            placement::RT_SOURCE,
            placement::PARCEL_SOURCE,
            placement::LAND_SOURCE,
            placement::TITLE_SOURCE,
            placement::OWNERSHIP_SOURCE,
            placement::LINEAGE_SOURCE,
            placement::CODE_CHANGE_SOURCE,
            placement::LEGAL_DONG_SOURCE,
            BRIDGE_SOURCE,
        ]
    }

    /// The code change table and the code snapshot: a new pair or name may move any deal.
    fn whole_table_inputs(&self) -> Vec<&'static str> {
        vec![placement::CODE_CHANGE_SOURCE, placement::LEGAL_DONG_SOURCE]
    }

    /// The rules' version and the build month.
    fn code_version(&self) -> String {
        let inference =
            rt_contract().map_or(Value::Null, |contract| contract["place_inference"].clone());
        let month = build_month();
        format!(
            "{}.{:04}-{:02}",
            rules_version(&inference),
            month.year(),
            month.month()
        )
    }

    /// None: no input is keyed by the parcel its document is on before the placement.
    fn row_sets(&self) -> Vec<RowSet> {
        Vec::new()
    }

    fn carries_place_id(&self) -> bool {
        true
    }

    #[allow(clippy::too_many_lines)] // each input, once, in the order the placement reads them
    fn spool_more(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        suffix: &str,
        dir_text: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        let mut read_millis = BTreeMap::new();

        // The code tables, whole (small), and what the placement derives from them.
        let at = Instant::now();
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {CODE_CHANGES} AS \
               SELECT kind, old_code, new_code, level, effective_date, source FROM {changes}; \
             CREATE OR REPLACE TABLE {CODE_NAMES} AS \
               SELECT region_cd, full_name, status FROM {names} \
               WHERE snapshot_date = (SELECT max(snapshot_date) FROM {names});",
            changes = source.of(placement::CODE_CHANGE_SOURCE)?,
            names = source.of(placement::LEGAL_DONG_SOURCE)?,
        ))?;
        Self::reference(connection)?;
        read_millis.insert(placement::LEGAL_DONG_SOURCE.to_owned(), elapsed(at));

        // The deals, once: their months, and each under the 시군구 its address names.
        let at = Instant::now();
        placement::deal_rows_into(connection, source, DEALS_READ)?;
        Self::months(connection, suffix)?;
        read_millis.insert(placement::RT_SOURCE.to_owned(), elapsed(at));

        // The lineage, once: its links, then its rows.
        let at = Instant::now();
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {LINEAGE_READ} AS \
             SELECT predecessor_pnu, successor_pnu, relation, grade, evidence_kind, evidence_ref FROM {};",
            source.of(placement::LINEAGE_SOURCE)?
        ))?;
        self.links(connection)?;
        let mut unkeyed = BTreeMap::new();
        unkeyed.insert(
            "rt_lineage",
            spool_by_part(
                connection,
                &self.lineage_query(),
                "rt_lineage",
                suffix,
                dir_text,
            )?,
        );
        connection.execute_batch(&format!("DROP TABLE {LINEAGE_READ};"))?;
        read_millis.insert(placement::LINEAGE_SOURCE.to_owned(), elapsed(at));

        // A deal whose address names no 법정동 has no 시군구: it is placed nowhere.
        unkeyed.insert(
            "rt_deals",
            spool_by_part(
                connection,
                &self.deals_query(),
                "rt_deals",
                suffix,
                dir_text,
            )?,
        );
        connection.execute_batch(&format!("DROP TABLE {DEALS_READ};"))?;

        // The served parcels the shared build spooled, under the parts that read them.
        spool_by_part(
            connection,
            &self.pnu_query(&format!(
                "read_parquet('{dir_text}/served{suffix}/*/*.parquet', hive_partitioning = false)"
            )),
            "rt_parcels",
            suffix,
            dir_text,
        )?;

        // Each register, once, by its PNU's 시군구.
        for (input, table, name) in [
            (placement::LAND_SOURCE, "land", "rt_land"),
            (placement::TITLE_SOURCE, "titles", "rt_titles"),
            (placement::OWNERSHIP_SOURCE, "ownership", "rt_ownership"),
        ] {
            let at = Instant::now();
            // The edition names the parcels only; a register is read by its own current rows.
            placement::pnu_rows_into(connection, source, input, table, "registers")?;
            unkeyed.insert(
                name,
                spool_by_part(connection, &self.pnu_query(table), name, suffix, dir_text)?,
            );
            connection.execute_batch(&format!("DROP TABLE {table};"))?;
            read_millis.insert(input.to_owned(), elapsed(at));
        }
        let side = if suffix.is_empty() { "new" } else { "base" };
        tracing::info!(side, ?unkeyed, "rows without a 시군구 are not spooled");
        Ok(read_millis)
    }

    fn load_more(
        &self,
        connection: &Connection,
        read: &dyn Fn(&str) -> Option<String>,
        suffix: &str,
    ) -> anyhow::Result<u64> {
        let mut rows = 0;
        for (name, _) in PART_TABLES {
            let spool = format!("{name}{suffix}");
            // A 시도 no row is read in holds the table empty, in its columns.
            let from = read(&spool).unwrap_or_else(|| format!("rt_shape_{name}"));
            connection.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {spool} AS SELECT * FROM {from};"
            ))?;
            rows += count(connection, &format!("SELECT count(*) FROM {spool}"))?;
        }
        Ok(rows)
    }

    fn part_tables(&self) -> Vec<&'static str> {
        PART_TABLES.iter().map(|(name, _)| *name).collect()
    }

    /// The served parcels: a deal may be placed on any of them.
    fn pnu_spools(&self) -> Vec<&'static str> {
        vec!["served"]
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(BUILT_TABLE)?;
        Ok(())
    }

    /// Every served PNU of a part whose rows moved (a deal has no PNU to name), and every served
    /// PNU when the months the Silver holds moved: each document's coverage reads them all.
    fn changed_keys_sql(&self) -> Option<String> {
        let parts = PART_TABLES
            .iter()
            .map(|(name, _)| CHANGED_PARTS_SQL.replace("@name@", name))
            .collect::<Vec<_>>()
            .join(" UNION ");
        let digits = self.part_digits;
        Some(format!(
            "SELECT s.pnu FROM read_parquet('@dir@/served/*/*.parquet', hive_partitioning = false) s \
             WHERE substr(s.pnu, 1, {digits}) IN ({parts}) \
                OR EXISTS (SELECT * FROM {MONTHS} EXCEPT SELECT * FROM {MONTHS}_old) \
                OR EXISTS (SELECT * FROM {MONTHS}_old EXCEPT SELECT * FROM {MONTHS})"
        ))
    }

    fn build(
        &self,
        connection: &Connection,
        all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            is_plain(source_snapshot_id) && is_plain(published_at_utc),
            "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
        );
        // The part's rows as the placement reads them: tables of their own, without `part`.
        let tables = PART_TABLES
            .iter()
            .map(|(name, table)| {
                format!("CREATE OR REPLACE TABLE {table} AS SELECT * EXCLUDE (part) FROM {name};")
            })
            .collect::<Vec<_>>()
            .join("\n");
        connection.execute_batch(&tables)?;
        let inference = &rt_contract()?["place_inference"];
        placement::place_from(connection, inference, CODE_CHANGES, CODE_NAMES)?;
        let as_of = build_month();
        gold::gold_rows(connection, as_of, &Self::coverage(connection, as_of)?)?;
        // Only the part's documents: a deal it was shown may belong to a linked part, which builds it.
        let targets = if all {
            ""
        } else {
            " AND pnu IN (SELECT pnu FROM targets)"
        };
        connection.execute_batch(&format!(
            "INSERT INTO built SELECT pnu, real_transaction_json, transaction_total, attached_via, place_id, \
               main_lot_group_place_ids, row_digest, '{source_snapshot_id}', '{published_at_utc}' FROM gold_rows \
             WHERE substr(pnu, 1, {digits}) = (SELECT part FROM {BUILD_PART}){targets};",
            digits = self.part_digits
        ))?;
        let drops = PART_TABLES
            .iter()
            .map(|(_, table)| format!("DROP TABLE {table};"))
            .collect::<Vec<_>>()
            .join("\n");
        connection.execute_batch(&drops)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
