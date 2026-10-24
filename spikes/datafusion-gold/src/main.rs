//! Spike (root ADR-0192): the building panel Gold build of
//! `platforms/foundation-platform/infra/lakehouse/spark/jobs/building_panel_silver_to_gold.py`
//! (`build_gold_panel_frame` + its checks) in Apache DataFusion, and the same SQL in DuckDB, over the
//! same Parquet inputs, to measure time and memory against the Spark job and prove the output is the
//! same row for row. Not wired into anything. Iceberg reads/writes are out of scope (see the ADR).
//!
//! Usage: foundation-datafusion-gold-spike <input-root> <output-dir> <published-at-utc>
//!        [threads] [memory-limit-gib]
//! SPIKE_ENGINE=duckdb runs DuckDB; SPIKE_NO_PRICES=1 builds the panel without unit prices.

use std::sync::Arc;
use std::time::Instant;

use arrow_json::writer::{make_encoder, Encoder, EncoderFactory, EncoderOptions, NullableEncoder};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{exec_err, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::*;
use sha2::{Digest, Sha256};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Read at run time, not compiled in: the spike adds nothing to the repository's compile-time file reads.
const HANDOFF: &str = "platforms/foundation-platform/infra/lakehouse/contracts/building-unit-handoff.json";
const R2_CONNECTIONS: &str = "platforms/foundation-platform/config/r2-connections.contract.json";
const SOURCES: [(&str, &str); 5] = [
    ("titles", "silver.building_register_titles"),
    ("floors", "silver.building_register_floors"),
    ("units", "silver.building_register_units"),
    ("areas", "silver.building_register_unit_areas"),
    ("prices", "silver.unit_official_price"),
];
/// DuckDB's spellings of the DataFusion functions the build uses. canonical_id hashes bytes, since a
/// VARCHAR cannot carry the NUL separator.
const DUCK_MACROS: &str = "
    CREATE MACRO regexp_like(s, p) AS regexp_matches(s, p);
    CREATE MACRO json_text(x) AS CAST(to_json(x) AS VARCHAR);
    CREATE MACRO uuid8(d) AS substr(d, 1, 8) || '-' || substr(d, 9, 4) || '-8' || substr(d, 14, 3) || '-'
        || substr('89ab', ((strpos('0123456789abcdef', substr(d, 17, 1)) - 1) & 3) + 1, 1) || substr(d, 18, 3)
        || '-' || substr(d, 21, 12);
    CREATE MACRO canonical_id(kind, key) AS CASE WHEN key IS NOT NULL THEN
        uuid8(sha256(encode('perfectory.catalog.' || kind || '.v1') || '\\x00'::BLOB || encode(CAST(key AS VARCHAR)))) END;";

enum Engine {
    DataFusion(SessionContext),
    DuckDb(duckdb::Connection),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        return exec_err!("usage: <input-root> <output-dir> <published-at-utc> [threads] [memory-limit-gib]");
    }
    let (input, output, published) = (&args[1], &args[2], &args[3]);
    let threads: usize = args.get(4).map_or(Ok(8), |v| v.parse()).map_err(ext)?;
    let memory_gib = args.get(5).map(|v| v.parse::<f64>()).transpose().map_err(ext)?;
    let engine = if std::env::var("SPIKE_ENGINE").as_deref() == Ok("duckdb") {
        let conn = duckdb::Connection::open_in_memory().map_err(ext)?;
        let mut settings = format!("SET threads = {threads}; SET temp_directory = '{output}.spill';");
        if let Some(gib) = memory_gib {
            settings += &format!("SET memory_limit = '{}MiB';", (gib * 1024.0) as u64);
        }
        // SPIKE_DUCK_SET: more settings to try, e.g. "SET preserve_insertion_order = false;".
        settings += &std::env::var("SPIKE_DUCK_SET").unwrap_or_default();
        conn.execute_batch(&(settings + DUCK_MACROS)).map_err(ext)?;
        Engine::DuckDb(conn)
    } else {
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(gib) = memory_gib {
            runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new((gib * (1u64 << 30) as f64) as usize)));
        }
        let config = SessionConfig::new().with_target_partitions(threads);
        let ctx = SessionContext::new_with_config_rt(config, runtime.build_arc()?);
        ctx.register_udf(ScalarUDF::from(CanonicalId::new()));
        ctx.register_udf(ScalarUDF::from(ToJson::new()));
        Engine::DataFusion(ctx)
    };

    let started = Instant::now();
    let phase = |name: &str| eprintln!("phase {name} at {:.1}s", started.elapsed().as_secs_f64());
    for (table, contract) in sorted_sources() {
        let path = std::path::Path::new(input).join(contract.trim_start_matches("silver."));
        engine.register_parquet(&format!("raw_{table}"), &path.to_string_lossy()).await?;
    }

    // The Gold's source id: sha256 of Python's json.dumps(snapshots, sort_keys=True).
    let mut snapshot_json = String::from("{");
    for (i, (table, contract)) in sorted_sources().iter().enumerate() {
        let ids = engine.strings(&format!("SELECT DISTINCT source_snapshot_id FROM raw_{table} LIMIT 3")).await?;
        if ids.len() != 1 || ids[0].is_empty() {
            return exec_err!("{contract} must carry exactly one non-empty source_snapshot_id: {ids:?}");
        }
        snapshot_json += &format!("{}\"{contract}\": \"{}\"", if i > 0 { ", " } else { "" }, ids[0]);
    }
    snapshot_json += "}";
    let source_id = hex::encode(Sha256::digest(snapshot_json.as_bytes()));
    phase("snapshots");

    let policy: serde_json::Value = serde_json::from_str(&contract(HANDOFF)?).map_err(ext)?;
    let policy = &policy["relationship_evidence_policy"];
    let r2: serde_json::Value = serde_json::from_str(&contract(R2_CONNECTIONS)?).map_err(ext)?;
    let pnu_pattern = format!("^(?:{})$", r2["building_by_pnu_gateway"]["object_key"]["pnu_pattern"].as_str().unwrap_or_default());
    let mut counters = Vec::new();

    for statement in views(policy, engine.is_duckdb()) {
        engine.run(&statement).await?;
        // SPIKE_PROBE=1: materialise each view as it is defined, to find the step that costs.
        if let (Some(_), Engine::DataFusion(ctx)) = (std::env::var_os("SPIKE_PROBE"), &engine) {
            let name = statement.split_whitespace().nth(2).unwrap_or_default();
            let at = Instant::now();
            let rows: usize = ctx.sql(&format!("SELECT * FROM {name}")).await?.collect().await?.iter().map(|b| b.num_rows()).sum();
            eprintln!("probe {name}: {rows} rows in {:.2}s", at.elapsed().as_secs_f64());
        }
    }
    for table in ["titles", "units"] {
        counters.push((format!("building_register_{table}_null_pnu_count"),
            engine.count(&format!("SELECT count(*) FROM raw_{table} WHERE pnu IS NULL OR length(trim(pnu)) = 0")).await?));
        if engine.count(&format!("SELECT count(*) FROM {table} WHERE NOT regexp_like(pnu, '{pnu_pattern}')")).await? > 0 {
            return exec_err!("{table} has an invalid cadastral PNU");
        }
    }
    for (table, key) in [("titles", "mgm_bldrgst_pk"), ("units", "mgm_bldrgst_pk"), ("raw_floors", "floor_row_id"), ("raw_areas", "area_row_id")] {
        engine.assert_unique(table, &[key], false).await?;
    }
    for (table, column) in [("titles", "floor_area_m2"), ("raw_areas", "area_m2")] {
        if engine.count(&format!("SELECT count(*) FROM {table} WHERE isnan({column}) OR abs({column}) = CAST('inf' AS DOUBLE) OR {column} < 0")).await? > 0 {
            return exec_err!("{column} must be finite and non-negative");
        }
    }
    for column in ["ground_floor_count", "basement_floor_count"] {
        if engine.count(&format!("SELECT count(*) FROM titles WHERE {column} < 0 OR {column} > 32767")).await? > 0 {
            return exec_err!("{column} must fit the canonical floor count");
        }
    }
    if with_prices() {
        engine.assert_unique("raw_prices", &["mgm_bldrgst_pk", "base_date"], true).await?;
        if engine.count("SELECT count(*) FROM raw_prices WHERE NOT regexp_like(base_date, '^[0-9]{8}$') OR price_won < 0 OR base_date IS NULL OR price_won IS NULL").await? > 0 {
            return exec_err!("unit official price has an invalid base_date or negative price");
        }
    }
    counters.push(("unlinked_floor_count".into(), engine.count(
        "SELECT count(*) FROM raw_floors f WHERE NOT EXISTS (SELECT 1 FROM titles t WHERE t.mgm_bldrgst_pk = f.mgm_bldrgst_pk)").await?));
    counters.push(("unlinked_unit_count".into(), engine.count("SELECT count(*) FROM projected_units WHERE building_id IS NULL").await?));
    phase("checks");

    let digest = engine.sha256_hex(&format!("json_text({})", engine.object(&["pnu", "buildings_json", "unlinked_units_json"]
        .map(|c| (c.to_string(), c.to_string())))));
    engine.write_parquet(&format!(
        "SELECT pnu, buildings_json, unlinked_units_json, {digest} AS row_digest,
                '{source_id}' AS source_snapshot_id, '{published}' AS published_at_utc
         FROM gold_content ORDER BY pnu"), output).await?;
    phase("write");

    // The quality gates, on what was written (the Spark job checks the persisted Gold the same way).
    engine.register_parquet("written", output).await?;
    engine.assert_unique("written", &["pnu"], false).await?;
    let rows = engine.count("SELECT count(*) FROM written").await?;
    let bad = engine.count(&format!(
        "SELECT count(*) FROM written WHERE pnu IS NULL OR buildings_json IS NULL OR unlinked_units_json IS NULL
           OR row_digest IS NULL OR source_snapshot_id IS NULL OR published_at_utc IS NULL OR NOT regexp_like(pnu, '{pnu_pattern}')")).await?;
    if bad > 0 {
        return exec_err!("Gold quality gates failed: {bad} rows");
    }
    phase("validate");
    println!("{}", serde_json::json!({"row_count": rows, "source_snapshot_id": source_id,
        "counters": counters.into_iter().map(|(k, v)| (k, serde_json::json!(v))).collect::<serde_json::Map<_, _>>(),
        "seconds": started.elapsed().as_secs_f64()}));
    Ok(())
}

/// A repository file, relative to SPIKE_REPO_ROOT (bench/measure.py sets it; default: the working directory).
fn contract(path: &str) -> Result<String> {
    let root = std::env::var("SPIKE_REPO_ROOT").unwrap_or_else(|_| ".".into());
    std::fs::read_to_string(std::path::Path::new(&root).join(path)).map_err(ext)
}

fn ext(e: impl std::error::Error + Send + Sync + 'static) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

/// SPIKE_NO_PRICES=1: the panel without unit prices (PR #430, root ADR-0190's v3 document).
fn with_prices() -> bool {
    std::env::var_os("SPIKE_NO_PRICES").is_none()
}

fn sorted_sources() -> Vec<(&'static str, &'static str)> {
    let mut sources: Vec<_> = SOURCES.into_iter().filter(|(t, _)| with_prices() || *t != "prices").collect();
    sources.sort_by_key(|(_, contract)| *contract);
    sources
}

impl Engine {
    fn is_duckdb(&self) -> bool {
        matches!(self, Engine::DuckDb(_))
    }

    /// A struct literal: DataFusion's named_struct, DuckDB's struct_pack.
    fn object(&self, fields: &[(String, String)]) -> String {
        object(fields, self.is_duckdb())
    }

    fn sha256_hex(&self, value: &str) -> String {
        if self.is_duckdb() { format!("sha256({value})") } else { format!("encode(sha256({value}), 'hex')") }
    }

    async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        match self {
            Engine::DataFusion(ctx) => ctx.register_parquet(name, path, ParquetReadOptions::default()).await,
            Engine::DuckDb(conn) => conn.execute_batch(&format!(
                "CREATE VIEW {name} AS SELECT * FROM read_parquet('{}/*.parquet')", path.replace('\\', "/"))).map_err(ext),
        }
    }

    async fn run(&self, sql: &str) -> Result<()> {
        match self {
            Engine::DataFusion(ctx) => ctx.sql(sql).await.map(|_| ()),
            Engine::DuckDb(conn) => conn.execute_batch(sql).map_err(ext),
        }
    }

    async fn write_parquet(&self, sql: &str, output: &str) -> Result<()> {
        match self {
            Engine::DataFusion(ctx) => ctx.sql(sql).await?.write_parquet(output, DataFrameWriteOptions::new(), None).await.map(|_| ()),
            Engine::DuckDb(conn) => {
                std::fs::create_dir_all(output).map_err(ext)?;
                conn.execute_batch(&format!("COPY ({sql}) TO '{}/part-0.parquet' (FORMAT parquet, COMPRESSION zstd)",
                    output.replace('\\', "/"))).map_err(ext)
            }
        }
    }

    async fn strings(&self, sql: &str) -> Result<Vec<String>> {
        match self {
            Engine::DataFusion(ctx) => {
                let mut out = Vec::new();
                for batch in ctx.sql(sql).await?.collect().await? {
                    let column = cast(batch.column(0), &DataType::Utf8)?;
                    let column = column.as_string::<i32>();
                    out.extend((0..column.len()).map(|i| column.value(i).to_string()));
                }
                Ok(out)
            }
            Engine::DuckDb(conn) => {
                let mut statement = conn.prepare(sql).map_err(ext)?;
                let rows = statement.query_map([], |row| row.get::<_, String>(0)).map_err(ext)?;
                rows.collect::<std::result::Result<_, _>>().map_err(ext)
            }
        }
    }

    async fn count(&self, sql: &str) -> Result<i64> {
        match self {
            Engine::DataFusion(ctx) => {
                let batches = ctx.sql(sql).await?.collect().await?;
                let column = cast(batches[0].column(0), &DataType::Int64)?;
                Ok(column.as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0))
            }
            Engine::DuckDb(conn) => conn.query_row(sql, [], |row| row.get(0)).map_err(ext),
        }
    }

    async fn assert_unique(&self, table: &str, keys: &[&str], allow_empty: bool) -> Result<()> {
        let list = keys.join(", ");
        if self.count(&format!("SELECT count(*) FROM (SELECT {list} FROM {table} GROUP BY {list} HAVING count(*) > 1)")).await? > 0 {
            return exec_err!("{table} has duplicate identity {keys:?}");
        }
        let absent = keys.iter().map(|k| if allow_empty { format!("{k} IS NULL") } else {
            format!("{k} IS NULL OR trim(CAST({k} AS VARCHAR)) = ''") }).collect::<Vec<_>>().join(" OR ");
        if self.count(&format!("SELECT count(*) FROM {table} WHERE {absent}")).await? > 0 {
            return exec_err!("{table} has an absent identity {keys:?}");
        }
        Ok(())
    }
}

fn object(fields: &[(String, String)], duckdb: bool) -> String {
    if duckdb {
        format!("struct_pack({})", fields.iter().map(|(k, v)| format!("\"{k}\" := {v}")).collect::<Vec<_>>().join(", "))
    } else {
        format!("named_struct({})", fields.iter().map(|(k, v)| format!("'{k}', {v}")).collect::<Vec<_>>().join(", "))
    }
}

/// The build, as views in dependency order: the Spark job's frames, one for one. The SQL is the
/// same for both engines but for the struct literal, `* EXCEPT`, and the ordered-first aggregate.
fn views(policy: &serde_json::Value, duckdb: bool) -> Vec<String> {
    let obj = |fields: &[(&str, String)]| object(&fields.iter().map(|(k, v)| (k.to_string(), v.clone())).collect::<Vec<_>>(), duckdb);
    let same = |names: &[&str]| names.iter().map(|n| (n.to_string(), n.to_string())).collect::<Vec<_>>();
    let (except, first) = if duckdb { ("EXCLUDE", "first") } else { ("EXCEPT", "first_value") };
    let text = |c: &str| format!("CASE WHEN length(trim({c})) > 0 THEN trim({c}) END");
    let source_method = policy["source_method"].as_str().unwrap_or_default();
    let sha = policy["sha256_pattern"].as_str().unwrap_or_default();
    let application = policy["application_id_pattern"].as_str().unwrap_or_default();
    let columns: Vec<&str> = policy["columns"].as_array().into_iter().flatten().filter_map(|c| c.as_str()).collect();
    // verified_building_links: the evidence struct, and the parent key only when the evidence holds.
    let valid = format!(
        "((building_mgm_bldrgst_pk IS NULL OR (length(building_mgm_bldrgst_pk) > 0 AND building_mgm_bldrgst_pk = trim(building_mgm_bldrgst_pk)))
          AND {uid} IS NOT NULL AND {method} IS NOT NULL AND {reason} IS NULL
          AND coalesce(({method} = '{source_method}' AND {source} IS NOT NULL AND regexp_like({digest}, '{sha}') AND {app} IS NULL)
                    OR (regexp_like({app}, '{application}') AND {app} <> '00000000-0000-0000-0000-000000000000'
                        AND {source} IS NULL AND {digest} IS NULL), false))",
        uid = text("unit_row_id"), method = text("building_link_method"), reason = text("building_link_reason"),
        source = text("building_link_source_record_id"), digest = text("building_link_input_sha256"),
        app = text("normalization_application_id"));
    let evidence = object(&columns.iter().map(|c| {
        let value = if *c == "building_link_reason" {
            format!("CASE WHEN building_mgm_bldrgst_pk IS NOT NULL AND NOT _valid THEN coalesce({}, 'unverified_building_link_evidence') ELSE {} END",
                text(c), text(c))
        } else {
            text(c)
        };
        (c.to_string(), value)
    }).collect::<Vec<_>>(), duckdb);
    let floor_label = "CASE WHEN u.floor_number IS NULL THEN '' WHEN u.floor_kind = 'basement' THEN concat('지하 ', CAST(u.floor_number AS VARCHAR), '층')
        WHEN u.floor_kind = 'rooftop' THEN '옥탑' ELSE concat(CAST(u.floor_number AS VARCHAR), '층') END";
    let unit = object(&same(&["dong_name", "ho_name", "id", "parcel_id", "building_id", "register_pk", "building_register_pk",
        "building_name", "floor_label", "exclusive_area_m2", "usage_name", "structure_name", "official_price_history",
        "building_link_evidence", "unit_pnu"].into_iter().filter(|f| with_prices() || *f != "official_price_history")
        .collect::<Vec<_>>()), duckdb);
    let (history, history_join) = if with_prices() {
        (", coalesce(h.official_price_history, []) AS official_price_history".to_string(),
         format!("LEFT JOIN (SELECT mgm_bldrgst_pk, array_sort(array_agg({}), 'DESC') AS official_price_history
            FROM raw_prices GROUP BY mgm_bldrgst_pk) h ON u.mgm_bldrgst_pk = h.mgm_bldrgst_pk", obj(&[
                ("base_date", "base_date".into()), ("price_won", "price_won".into())])))
    } else {
        (String::new(), String::new())
    };
    let floor = obj(&[("floor_row_id", "floor_row_id".into()), ("floor_kind", "floor_kind".into()),
        ("floor_number", "floor_number".into()), ("floor_index", "floor_index".into()), ("floor_display_ko", "floor_display_ko".into())]);
    let building = obj(&[("register_pk", "t.mgm_bldrgst_pk".into()), ("id", "t.id".into()),
        ("parcel_id", "canonical_id('parcel', t.pnu)".into()), ("purpose_code", text("t.purpose_code_raw")),
        ("structure_code", text("t.structure_code_raw")),
        ("floor_area_m2", "CASE WHEN t.floor_area_m2 > 0 THEN t.floor_area_m2 END".into()), ("stories", "t.ground_floor_count".into()),
        ("below_ground_floors", "coalesce(t.basement_floor_count, 0)".into()), ("has_rooftop", "coalesce(f._roof, 0) = 1".into()),
        ("rooftop_area_m2", "r.rooftop_area_m2".into()), ("rooftop_usage", "coalesce(r.rooftop_usage, '')".into()),
        ("built_year", "t.approval_year".into()), ("floors", "coalesce(f.floors, [])".into()), ("units", "coalesce(n.units, [])".into())]);
    let usage = text("a.usage_name_raw");
    let order = "ORDER BY area_m2 DESC NULLS LAST, usage_name_raw ASC NULLS LAST, structure_name_raw ASC NULLS LAST, area_row_id";
    vec![
        "CREATE VIEW titles AS SELECT trim(pnu) AS pnu, mgm_bldrgst_pk, purpose_code_raw, structure_code_raw, floor_area_m2,
            ground_floor_count, basement_floor_count, approval_year, canonical_id('building', mgm_bldrgst_pk) AS id
         FROM raw_titles WHERE NOT (pnu IS NULL OR length(trim(pnu)) = 0)".to_string(),
        format!("CREATE VIEW units AS SELECT * {except} (pnu), trim(pnu) AS pnu FROM raw_units WHERE NOT (pnu IS NULL OR length(trim(pnu)) = 0)"),
        format!("CREATE VIEW verified_units AS SELECT * {except} (building_mgm_bldrgst_pk, _valid),
            {evidence} AS building_link_evidence,
            CASE WHEN _valid THEN building_mgm_bldrgst_pk END AS building_mgm_bldrgst_pk
         FROM (SELECT *, {valid} AS _valid FROM units)"),
        // unit_areas: the window's first row is an ordered first value, which spills where a window cannot.
        format!("CREATE VIEW unit_areas AS SELECT mgm_bldrgst_pk,
            CAST(sum(CAST(area_m2 AS DECIMAL(38, 6))) AS DOUBLE) AS exclusive_area_m2,
            {first}(usage_name_raw {order}) AS usage_name, {first}(structure_name_raw {order}) AS structure_name
         FROM raw_areas WHERE area_kind = 'exclusive' GROUP BY mgm_bldrgst_pk"),
        // array_sort(array_agg(..)), never array_agg(.. ORDER BY ..): DataFusion's ordered form never finished on
        // 5 MB of nested input. Each struct leads with its unique sort key, so sorting the structs is the same order.
        format!("CREATE VIEW projected_units AS SELECT u.pnu, u.pnu AS unit_pnu, l.pnu AS _parent_pnu, u.building_register_pk,
            l.id AS building_id, u.building_link_evidence, u.mgm_bldrgst_pk AS register_pk,
            canonical_id('building_unit', u.mgm_bldrgst_pk) AS id, canonical_id('parcel', u.pnu) AS parcel_id,
            coalesce(u.dong_join_name, '') AS building_name, coalesce(u.dong_name_raw, '') AS dong_name,
            coalesce(u.unit_label_ko, u.unit_name_raw, '') AS ho_name, {floor_label} AS floor_label,
            a.exclusive_area_m2, coalesce(a.usage_name, '') AS usage_name, coalesce(a.structure_name, '') AS structure_name{history}
         FROM (SELECT *, {bpk} AS building_register_pk FROM verified_units) u
         LEFT JOIN titles l ON u.building_register_pk = l.mgm_bldrgst_pk
         LEFT JOIN unit_areas a ON u.mgm_bldrgst_pk = a.mgm_bldrgst_pk
         {history_join}", bpk = text("building_mgm_bldrgst_pk")),
        format!("CREATE VIEW nested_units AS SELECT _parent_pnu AS pnu, building_id, array_sort(array_agg({unit})) AS units
         FROM projected_units WHERE building_id IS NOT NULL GROUP BY _parent_pnu, building_id"),
        format!("CREATE VIEW unlinked AS SELECT pnu, array_sort(array_agg({unit})) AS unlinked_units
         FROM projected_units WHERE building_id IS NULL GROUP BY pnu"),
        format!("CREATE VIEW nested_floors AS SELECT mgm_bldrgst_pk, array_sort(array_agg({floor})) AS floors,
            max(CASE WHEN floor_kind = 'rooftop' THEN 1 ELSE 0 END) AS _roof
         FROM raw_floors GROUP BY mgm_bldrgst_pk"),
        format!("CREATE VIEW roof_areas AS SELECT coalesce(l._title_pk, a.mgm_bldrgst_pk) AS _building_pk,
            CAST(sum(CAST(a.area_m2 AS DECIMAL(38, 6))) AS DOUBLE) AS rooftop_area_m2,
            array_to_string(array_sort(array_agg(DISTINCT {usage}) FILTER (WHERE {usage} IS NOT NULL)), ' · ') AS rooftop_usage
         FROM raw_areas a
         LEFT JOIN (SELECT mgm_bldrgst_pk AS _unit_pk, building_mgm_bldrgst_pk AS _title_pk FROM verified_units) l ON a.mgm_bldrgst_pk = l._unit_pk
         WHERE a.area_kind = 'common' AND a.floor_kind = 'rooftop' GROUP BY 1"),
        format!("CREATE VIEW grouped AS SELECT t.pnu, array_sort(array_agg({building})) AS buildings
         FROM titles t
         LEFT JOIN nested_floors f ON t.mgm_bldrgst_pk = f.mgm_bldrgst_pk
         LEFT JOIN roof_areas r ON t.mgm_bldrgst_pk = r._building_pk
         LEFT JOIN nested_units n ON t.pnu = n.pnu AND t.id = n.building_id
         GROUP BY t.pnu"),
        "CREATE VIEW gold_content AS SELECT p.pnu,
            CASE WHEN g.buildings IS NULL THEN '[]' ELSE json_text(g.buildings) END AS buildings_json,
            CASE WHEN u.unlinked_units IS NULL THEN '[]' ELSE json_text(u.unlinked_units) END AS unlinked_units_json
         FROM (SELECT pnu FROM titles UNION SELECT pnu FROM units) p
         LEFT JOIN grouped g ON p.pnu = g.pnu LEFT JOIN unlinked u ON p.pnu = u.pnu".to_string(),
    ]
}

/// Catalog-domain UUIDv8 projection of `perfectory.catalog.{kind}.v1\0{key}` (the Spark job's canonical_id).
#[derive(Debug, PartialEq, Eq, Hash)]
struct CanonicalId(Signature);

impl CanonicalId {
    fn new() -> Self {
        Self(Signature::any(2, Volatility::Immutable))
    }
}

impl ScalarUDFImpl for CanonicalId {
    fn name(&self) -> &str { "canonical_id" }
    fn signature(&self) -> &Signature { &self.0 }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ColumnarValue::Scalar(kind) = &args.args[0] else { return exec_err!("canonical_id kind must be a literal") };
        let prefix = format!("perfectory.catalog.{}.v1\0", kind.to_string());
        let keys = cast(&args.args[1].to_array(args.number_rows)?, &DataType::Utf8)?;
        let keys = keys.as_string::<i32>();
        let mut out = StringBuilder::with_capacity(keys.len(), keys.len() * 36);
        let mut hasher = Sha256::new();
        for i in 0..keys.len() {
            if keys.is_null(i) {
                out.append_null();
                continue;
            }
            hasher.update(prefix.as_bytes());
            hasher.update(keys.value(i).as_bytes());
            let d = hex::encode(hasher.finalize_reset());
            let variant = (u8::from_str_radix(&d[16..17], 16).unwrap_or(0) & 3) + 8;
            out.append_value(format!("{}-{}-8{}-{:x}{}-{}", &d[0..8], &d[8..12], &d[13..16], variant, &d[17..20], &d[20..32]));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

/// Spark's `to_json(..., ignoreNullFields=false)`: arrow-json with explicit nulls and Java's
/// `Double.toString` for doubles (Jackson writes doubles that way).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToJson(Signature);

impl ToJson {
    fn new() -> Self {
        Self(Signature::any(1, Volatility::Immutable))
    }
}

impl ScalarUDFImpl for ToJson {
    fn name(&self) -> &str { "json_text" }
    fn signature(&self) -> &Signature { &self.0 }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args.args[0].to_array(args.number_rows)?;
        let field: FieldRef = Arc::new(Field::new("v", array.data_type().clone(), true));
        let options = EncoderOptions::default().with_explicit_nulls(true).with_encoder_factory(Arc::new(JavaDoubles));
        let mut encoder = make_encoder(&field, array.as_ref(), &options)?;
        let mut out = StringBuilder::with_capacity(array.len(), 0);
        let mut buffer = Vec::new();
        for i in 0..array.len() {
            if array.is_null(i) {
                out.append_null();
                continue;
            }
            buffer.clear();
            encoder.encode(i, &mut buffer);
            out.append_value(std::str::from_utf8(&buffer).map_err(ext)?);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

#[derive(Debug)]
struct JavaDoubles;

impl EncoderFactory for JavaDoubles {
    fn make_default_encoder<'a>(&self, _: &'a FieldRef, array: &'a dyn Array, _: &'a EncoderOptions)
        -> std::result::Result<Option<NullableEncoder<'a>>, ArrowError> {
        if array.data_type() != &DataType::Float64 {
            return Ok(None);
        }
        let values = array.as_primitive::<Float64Type>();
        Ok(Some(NullableEncoder::new(Box::new(JavaDoubleEncoder(values.values().clone())), values.nulls().cloned())))
    }
}

struct JavaDoubleEncoder(datafusion::arrow::buffer::ScalarBuffer<f64>);

impl Encoder for JavaDoubleEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.extend_from_slice(java_double(self.0[idx]).as_bytes());
    }
}

/// `Double.toString` (JDK 19+, shortest repr): plain in [1e-3, 1e7), else `d.dddE±n`; always a fraction digit.
fn java_double(v: f64) -> String {
    if !v.is_finite() {
        return "null".into(); // shortcut: Spark writes NaN/Infinity as strings; the build refuses them upstream.
    }
    let magnitude = v.abs();
    if magnitude == 0.0 || (1e-3..1e7).contains(&magnitude) {
        let plain = format!("{v}");
        return if plain.contains('.') { plain } else { plain + ".0" };
    }
    let sci = format!("{v:e}");
    let (mantissa, exponent) = sci.split_once('e').unwrap_or((&sci, "0"));
    let mantissa = if mantissa.contains('.') { mantissa.to_string() } else { format!("{mantissa}.0") };
    format!("{mantissa}E{exponent}")
}

#[cfg(test)]
mod tests {
    use super::java_double;

    #[test]
    fn doubles_print_as_java_does() {
        for (value, java) in [(62637.74, "62637.74"), (100.0, "100.0"), (0.0, "0.0"), (1e7, "1.0E7"),
                              (12345678.9, "1.23456789E7"), (0.0001, "1.0E-4"), (0.001, "0.001"), (-2.5, "-2.5")] {
            assert_eq!(java_double(value), java);
        }
    }
}
