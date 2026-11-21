// one-off: until the last served Gold has moved to its `_versions` table (no lane serves the table a
// versions table replaces, so no takeover is left to prove): this module and its three commands go.
//! The two sides of a takeover verdict (root ADR-0564 §6, ADR-0559 §5): the served Gold a Spark
//! build replaced rows of, and the `_versions` table `DuckDB` built from the same input pins as its
//! shadow.
//!
//! `export-gold-takeover-sides` writes both as Parquet with the columns they share and a
//! `sides.json` naming them; `query-takeover-sides` runs one SQL statement over the two files and
//! prints its rows as JSON, so `engine_equality.py` reaches its verdict through this `DuckDB` (the
//! release images have no Python `duckdb`).
//!
//! `show-gold-takeover-switch` names that pair for the switch, whose full bake holds every
//! document to the served one (`section_packs::takeover` of the by-PNU publisher).

use std::{io::Read as _, path::Path};

use anyhow::{ensure, Context};
use foundation_publisher_silver::{
    duckdb::{Config, Connection},
    duckdb_parquet_stage::{
        with_contract_columns, DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG,
    },
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseTableContract, GOLD_VERSIONS_TABLE_SUFFIX,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    gold_versions_commit::{shadow_of, Shadow, SOURCE_SNAPSHOTS_PROPERTY},
    public_data_control_support::{optional_env_value, required_env_value},
};

const PREFIX: &str = "FOUNDATION_PLATFORM_GOLD_TAKEOVER_";
/// The columns each build stamps on the rows it writes: the replaced table's per-commit partition
/// and the run's start (`gold_versions_commit::appended_sql` stamps the same two). The two engines
/// commit at different times, so these differ on every row and say nothing of the content.
const PER_COMMIT_STAMPS: [&str; 2] = ["source_snapshot_id", "published_at_utc"];
/// The row digest of content only (root ADR-0099); the verdict compares it apart from the content.
const DIGEST: &str = "row_digest";

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The columns both sides are compared on: the replaced table's that the versions table keeps
/// (every one: it adds `version_seq` and `row_state`, which the replaced table has no counterpart
/// of), less the per-commit stamps.
fn compared_columns(
    replaced: &LakehouseTableContract,
    versions: &LakehouseTableContract,
) -> Vec<&'static str> {
    replaced
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| {
            !PER_COMMIT_STAMPS.contains(name)
                && versions.columns.iter().any(|column| column.name == *name)
        })
        .collect()
}

/// A path in the work directory as `DuckDB` SQL text.
fn sql_path(path: &Path) -> anyhow::Result<String> {
    let text = path
        .to_str()
        .context("the work directory must be UTF-8")?
        .replace('\\', "/");
    ensure!(
        !text.contains('\''),
        "the work directory must not contain a quote"
    );
    Ok(text)
}

/// The commit of `table` whose input pins are the shadow's: the build the verdict compares.
async fn shadow_commit(
    catalog: &IcebergRestCatalog,
    table: &str,
    shadow: &Shadow,
) -> anyhow::Result<i64> {
    let history = catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    history
        .iter()
        .find(|snapshot| snapshot.source_snapshots() == shadow.pins)
        .map(|snapshot| snapshot.snapshot_id)
        .with_context(|| {
            format!(
                "the shadow has not built {}'s build {} inputs yet: no commit of {table} records \
                 those {SOURCE_SNAPSHOTS_PROPERTY}",
                shadow.replaced, shadow.build
            )
        })
}

/// Writes `reference.parquet`, `candidate.parquet` and `sides.json` for the versions table the
/// environment names.
#[allow(clippy::too_many_lines)]
pub(crate) async fn export() -> anyhow::Result<()> {
    let table = required_env_value(&format!("{PREFIX}TABLE"))?;
    let work = required_env_value(&format!("{PREFIX}WORK_DIR"))?;
    let memory_limit = optional_env_value(&format!("{PREFIX}MEMORY_LIMIT"))?;
    let replaced_name = table
        .strip_suffix(GOLD_VERSIONS_TABLE_SUFFIX)
        .with_context(|| format!("{table} is not a versions table (gold.<x>_versions)"))?;
    let (versions, replaced) = (contract(&table)?, contract(replaced_name)?);
    let latest = versions
        .current_row_latest()
        .with_context(|| format!("{table} is no table of appended row versions"))?;

    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    // Asked as if the lane served the replaced table: the verdict is about that table's build.
    let shadow = shadow_of(&catalog, &table, replaced_name)
        .await?
        .with_context(|| format!("{table} is not a shadow of {replaced_name}"))?;
    let candidate = shadow_commit(&catalog, &table, &shadow).await?;

    let columns = compared_columns(replaced, versions);
    ensure!(
        latest.key.iter().all(|key| columns.contains(key)),
        "{replaced_name}'s columns lack its key {:?}",
        latest.key
    );
    let work = Path::new(&work);
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
    let connection = stage.connection();
    if let Some(limit) = &memory_limit {
        connection.execute("SET memory_limit = ?", [limit.as_str()])?;
    }
    let listed = columns.join(", ");
    let reference_source = with_contract_columns(
        connection,
        replaced,
        &format!(
            "{LAKEHOUSE_CATALOG}.{replaced_name} AT (VERSION => {})",
            shadow.build
        ),
    )?;
    let candidate_source = with_contract_columns(
        connection,
        versions,
        &format!("{LAKEHOUSE_CATALOG}.{table} AT (VERSION => {candidate})"),
    )?;
    for (name, query) in [
        (
            "reference",
            format!("SELECT {listed} FROM {reference_source}"),
        ),
        (
            "candidate",
            latest.current_rows_sql(&candidate_source, &columns, "TRUE"),
        ),
    ] {
        let target = sql_path(&work.join(format!("{name}.parquet")))?;
        connection
            .execute_batch(&format!(
                "COPY ({query}) TO '{target}' (FORMAT parquet, COMPRESSION zstd)"
            ))
            .with_context(|| format!("DuckDB failed to write {name}.parquet"))?;
    }
    drop(stage);

    let keys = latest.key;
    let digest = columns.contains(&DIGEST).then_some(DIGEST);
    let content: Vec<&str> = columns
        .iter()
        .copied()
        .filter(|name| !keys.contains(name) && *name != DIGEST)
        .collect();
    let sides = json!({
        "table": table, "replaced": replaced_name,
        "reference_snapshot": shadow.build.to_string(), "candidate_snapshot": candidate.to_string(),
        "pins": shadow.pins, "keys": keys, "content": content, "digest": digest,
    });
    let path = work.join("sides.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&sides)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    println!(
        "gold-takeover-sides {table}: reference {replaced_name}@{} candidate {table}@{candidate} \
         keys={keys:?} content={} digest={}",
        shadow.build,
        content.len(),
        digest.is_some()
    );
    Ok(())
}

/// Prints the build pair a takeover's switch is baked from (root ADR-0564 §6-5) as the last stdout
/// line: the replaced table's build-tag commit (`replaced_build`) and the versions table's shadow
/// commit of the same input pins (`shadow_build`), the pair the verdict compares. The bake script
/// pins the switch's full bake to `shadow_build` and refuses unless the packs reflect
/// `replaced_build`; the bake then refuses any document that differs from the served one.
///
/// # Errors
/// Refuses a table that is no shadow of a replaced one, and a shadow that has not built the
/// replaced build's pins (the replaced table built again after the shadow's last build: stop its
/// job, build the shadow and run the verdict again before the switch).
pub(crate) async fn switch() -> anyhow::Result<()> {
    let table = required_env_value(&format!("{PREFIX}TABLE"))?;
    let replaced_name = table
        .strip_suffix(GOLD_VERSIONS_TABLE_SUFFIX)
        .with_context(|| format!("{table} is not a versions table (gold.<x>_versions)"))?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let shadow = shadow_of(&catalog, &table, replaced_name)
        .await?
        .with_context(|| format!("{table} is not a shadow of {replaced_name}"))?;
    let shadow_build = shadow_commit(&catalog, &table, &shadow).await?;
    println!(
        "{}",
        json!({
            "table": table, "replaced": replaced_name,
            "replaced_build": shadow.build.to_string(), "shadow_build": shadow_build.to_string(),
        })
    );
    Ok(())
}

/// The rows `sql` answers as one JSON array of objects, each as `DuckDB`'s `to_json` writes it:
/// numbers as JSON numbers at full width (a `HUGEINT` sum included, which Python's `json` reads
/// exactly), text as strings, NULL as null.
fn json_array(connection: &Connection, sql: &str) -> anyhow::Result<String> {
    let rows = connection
        .prepare(&format!("SELECT to_json(q)::VARCHAR FROM ({sql}) AS q"))
        .context("DuckDB refused the query")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()
        .context("DuckDB failed to run the query")?;
    Ok(format!("[{}]", rows.join(",")))
}

/// Runs the SQL statement on stdin over the views `reference` and `candidate` of the work
/// directory's two files and prints its rows as the last stdout line.
pub(crate) fn query() -> anyhow::Result<()> {
    let work = required_env_value(&format!("{PREFIX}WORK_DIR"))?;
    let mut sql = String::new();
    std::io::stdin()
        .read_to_string(&mut sql)
        .context("cannot read the SQL statement from stdin")?;
    let sql = sql.trim().trim_end_matches(';');
    ensure!(!sql.is_empty(), "no SQL statement on stdin");
    let connection = Connection::open_in_memory_with_flags(
        Config::default()
            .with("autoload_known_extensions", "false")?
            .with("autoinstall_known_extensions", "false")?,
    )?;
    for (name, value) in [
        (
            "memory_limit",
            optional_env_value(&format!("{PREFIX}MEMORY_LIMIT"))?,
        ),
        (
            "temp_directory",
            optional_env_value(&format!("{PREFIX}TEMP_DIRECTORY"))?,
        ),
    ] {
        if let Some(value) = value {
            connection.execute(&format!("SET {name} = ?"), [value])?;
        }
    }
    for name in ["reference", "candidate"] {
        let path = sql_path(&Path::new(&work).join(format!("{name}.parquet")))?;
        connection.execute_batch(&format!(
            "CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')"
        ))?;
    }
    println!("{}", json_array(&connection, sql)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sides_share_the_replaced_columns_less_the_stamps() -> anyhow::Result<()> {
        let columns = compared_columns(
            contract("gold.parcel_nearby")?,
            contract("gold.parcel_nearby_versions")?,
        );
        assert!(columns.contains(&"pnu") && columns.contains(&DIGEST));
        for absent in [
            "source_snapshot_id",
            "published_at_utc",
            "version_seq",
            "row_state",
        ] {
            assert!(!columns.contains(&absent), "{absent} is compared");
        }
        Ok(())
    }

    #[test]
    fn rows_print_as_json_with_wide_numbers_and_nulls() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        let text = json_array(
            &connection,
            "SELECT 1 AS n, 'a' AS s, NULL AS z, CAST('170141183460469231731687303715884105727' AS HUGEINT) AS h",
        )?;
        let rows: serde_json::Value = serde_json::from_str(&text)?;
        assert_eq!(rows[0]["n"], 1);
        assert_eq!(rows[0]["s"], "a");
        assert!(rows[0]["z"].is_null());
        assert!(text.contains("170141183460469231731687303715884105727"));
        assert_eq!(json_array(&connection, "SELECT 1 AS n WHERE FALSE")?, "[]");
        Ok(())
    }
}
