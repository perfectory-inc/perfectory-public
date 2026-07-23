use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::public_data_control_support::{repo_relative_path, utc_now, write_json_file};

const EVIDENCE_SCHEMA_VERSION: &str = "foundation-platform.cutover_postgis_mirror_dlq_schema.v1";
const DB_SCHEMA_CONTRACT_PATH: &str = "docs/db/catalog-schema-contract.v1.example.json";
const DB_SCHEMA_CONTRACT_VERSION: &str = "foundation-platform.catalog_schema_contract.v1";
const REBUILD_SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.postgis_parcel_boundary_mirror_rebuild_summary.v1";
const SERVING_POSTGIS_LOADED_ROW_COUNT_POSITIVE_MESSAGE: &str =
    "ServingPostgisLoadedRowCount must be a positive integer";
const LIVE_SCHEMA_JSON_INSIDE_ROOT_MESSAGE: &str = "LiveSchemaJson must be inside Root";
const OUTPUT_PATH_STAY_WITHIN_ROOT_MESSAGE: &str = "OutputPath must stay within Root";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    validate_migration_sql(&config)?;
    if config.output_path.is_file() {
        bail!(
            "postgis mirror dlq cutover evidence already exists: {}",
            config.output_path.display()
        );
    }
    let live_schema_json = config.resolve_live_schema_json()?;
    validate_live_schema_contract(&config.root, &live_schema_json)?;

    let mirror_rebuild_summary_path = config.resolve_mirror_rebuild_summary_path()?;
    let mirror_rebuild_summary = if let Some(path) = &mirror_rebuild_summary_path {
        validate_mirror_rebuild_summary(&config, path)?;
        Some(repo_relative_path(&config.root, path))
    } else {
        None
    };

    let evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        migration: MigrationEvidence {
            id: config.migration_id.clone(),
        },
        serving_postgis: ServingPostgisEvidence {
            srid: config.serving_postgis_srid.clone(),
            rebuild_status: config.serving_postgis_rebuild_status.clone(),
            source_snapshot_id: config.serving_postgis_source_snapshot_id.clone(),
            loaded_row_count: config.serving_postgis_loaded_row_count,
            rebuild_summary_json: mirror_rebuild_summary,
        },
        dlq: DlqEvidence {
            table: config.dlq_table.clone(),
            persistence_status: config.dlq_persistence_status.clone(),
            inspectable_status: config.dlq_inspectable_status.clone(),
        },
        schema_contract: SchemaContractEvidence {
            live_schema_json: repo_relative_path(&config.root, &live_schema_json),
            status: "passed",
        },
    };
    write_json_file(&config.output_path, &evidence)?;
    println!(
        "postgis-mirror-dlq-cutover-evidence-written path={}",
        config.output_path.display()
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    root: PathBuf,
    migration_id: String,
    serving_postgis_srid: String,
    serving_postgis_rebuild_status: String,
    serving_postgis_source_snapshot_id: String,
    serving_postgis_loaded_row_count: u64,
    dlq_table: String,
    dlq_persistence_status: String,
    dlq_inspectable_status: String,
    live_schema_json_raw: Option<String>,
    mirror_rebuild_summary_path_raw: Option<String>,
    output_path: PathBuf,
}

#[derive(Debug, Serialize)]
struct Evidence {
    schema_version: &'static str,
    generated_at_utc: String,
    migration: MigrationEvidence,
    serving_postgis: ServingPostgisEvidence,
    dlq: DlqEvidence,
    schema_contract: SchemaContractEvidence,
}

#[derive(Debug, Serialize)]
struct MigrationEvidence {
    id: String,
}

#[derive(Debug, Serialize)]
struct ServingPostgisEvidence {
    srid: String,
    rebuild_status: String,
    source_snapshot_id: String,
    loaded_row_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    rebuild_summary_json: Option<String>,
}

#[derive(Debug, Serialize)]
struct DlqEvidence {
    table: String,
    persistence_status: String,
    inspectable_status: String,
}

#[derive(Debug, Serialize)]
struct SchemaContractEvidence {
    live_schema_json: String,
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct RebuildSummary {
    schema_version: String,
    validate_only: bool,
    source_snapshot_id: String,
    source_table: String,
    target_srid: String,
    loaded_row_count: u64,
    db_verification: Option<RebuildSummaryDbVerification>,
}

#[derive(Debug, Deserialize)]
struct RebuildSummaryDbVerification {
    mirror_row_count: u64,
    invalid_srid_count: u64,
}

#[derive(Debug, Deserialize)]
struct DbSchemaContract {
    schema_version: String,
    #[serde(default)]
    required_extensions: Vec<String>,
    #[serde(default)]
    required_tables: Vec<RequiredTable>,
}

#[derive(Debug, Deserialize)]
struct RequiredTable {
    name: String,
    #[serde(default)]
    columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LiveSchemaDocument {
    #[serde(default)]
    extensions: Vec<String>,
    #[serde(default)]
    columns: Vec<LiveSchemaColumn>,
}

#[derive(Clone, Debug, Deserialize)]
struct LiveSchemaColumn {
    table_schema: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    column_name: Option<String>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root_raw = optional_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_EVIDENCE_ROOT")?
            .unwrap_or_else(|| ".".to_owned());
        let root = normalize_absolute_path(Path::new(&root_raw))?;
        if !root.is_dir() {
            bail!("Root does not exist: {}", root.display());
        }

        let migration_id = required_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_MIGRATION_ID")?;
        validate_migration_id(&migration_id)?;
        let serving_postgis_srid =
            required_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_SRID")?;
        validate_srid(&serving_postgis_srid)?;
        let serving_postgis_rebuild_status =
            required_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_REBUILD_STATUS")?;
        if serving_postgis_rebuild_status != "passed" {
            bail!("ServingPostgisRebuildStatus must be passed");
        }
        let serving_postgis_source_snapshot_id = required_env(
            "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_SOURCE_SNAPSHOT_ID",
        )?;
        validate_source_snapshot_id(&serving_postgis_source_snapshot_id)?;
        let serving_postgis_loaded_row_count = parse_positive_u64(
            &required_env(
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_LOADED_ROW_COUNT",
            )?,
            SERVING_POSTGIS_LOADED_ROW_COUNT_POSITIVE_MESSAGE,
        )?;
        let dlq_table = required_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_TABLE")?;
        validate_dlq_table(&dlq_table)?;
        let dlq_persistence_status =
            required_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_PERSISTENCE_STATUS")?;
        if dlq_persistence_status != "passed" {
            bail!("DlqPersistenceStatus must be passed");
        }
        let dlq_inspectable_status =
            required_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_INSPECTABLE_STATUS")?;
        if dlq_inspectable_status != "passed" {
            bail!("DlqInspectableStatus must be passed");
        }

        let output_path_raw = optional_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_OUTPUT_PATH")?
            .unwrap_or_else(|| "target/cutover/postgis-mirror-and-dlq-schema.json".to_owned());
        let output_path = resolve_output_path(&root, Path::new(&output_path_raw), "OutputPath")?;
        let live_schema_json_raw =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_LIVE_SCHEMA_JSON")?;
        let mirror_rebuild_summary_path_raw =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_MIRROR_REBUILD_SUMMARY_PATH")?;

        Ok(Self {
            root,
            migration_id,
            serving_postgis_srid,
            serving_postgis_rebuild_status,
            serving_postgis_source_snapshot_id,
            serving_postgis_loaded_row_count,
            dlq_table,
            dlq_persistence_status,
            dlq_inspectable_status,
            live_schema_json_raw,
            mirror_rebuild_summary_path_raw,
            output_path,
        })
    }

    fn resolve_live_schema_json(&self) -> anyhow::Result<PathBuf> {
        let Some(path) = &self.live_schema_json_raw else {
            bail!("LiveSchemaJson is required");
        };
        resolve_input_path(&self.root, Path::new(path), "LiveSchemaJson")
    }

    fn resolve_mirror_rebuild_summary_path(&self) -> anyhow::Result<Option<PathBuf>> {
        self.mirror_rebuild_summary_path_raw
            .as_ref()
            .map(|path| resolve_input_path(&self.root, Path::new(path), "MirrorRebuildSummaryPath"))
            .transpose()
    }
}

fn validate_migration_sql(config: &Config) -> anyhow::Result<()> {
    let migration_path = config
        .root
        .join("migrations")
        .join(format!("{}.sql", config.migration_id));
    if !migration_path.is_file() {
        bail!("MigrationId must reference an existing migrations/<id>.sql file");
    }
    let sql = fs::read_to_string(&migration_path)
        .with_context(|| format!("failed to read {}", migration_path.display()))?;
    let sql = normalize_sql(&remove_sql_comments(&sql));
    if !sql.contains("serving_postgis") {
        bail!("MigrationId must reference serving_postgis schema changes");
    }
    if !contains_identifier(&sql, &config.dlq_table) {
        bail!("MigrationId must reference DlqTable");
    }
    let srid_number = config
        .serving_postgis_srid
        .strip_prefix("EPSG:")
        .unwrap_or(&config.serving_postgis_srid);
    if !sql.contains("srid") || !contains_numeric_token(&sql, srid_number) {
        bail!("MigrationId must reference ServingPostgisSrid");
    }
    if !contains_create_schema(&sql, "serving_postgis") {
        bail!("MigrationId must create serving_postgis schema");
    }
    if !contains_create_table_in_schema(&sql, "serving_postgis") {
        bail!("MigrationId must create serving_postgis mirror table");
    }
    if !contains_create_table(&sql, &config.dlq_table) {
        bail!("MigrationId must create DlqTable");
    }
    if !contains_create_extension(&sql, "postgis") {
        bail!("MigrationId must create PostGIS extension");
    }
    Ok(())
}

fn validate_live_schema_contract(root: &Path, live_schema_json: &Path) -> anyhow::Result<()> {
    validate_live_schema_contract_inner(root, live_schema_json).map_err(|error| {
        anyhow::anyhow!("LiveSchemaJson must satisfy live DB schema contract. Output: {error}")
    })
}

fn validate_live_schema_contract_inner(root: &Path, live_schema_json: &Path) -> anyhow::Result<()> {
    let contract_path = root.join(DB_SCHEMA_CONTRACT_PATH);
    if !contract_path.is_file() {
        bail!("missing DB schema contract fixture: {DB_SCHEMA_CONTRACT_PATH}");
    }
    let contract = read_json::<DbSchemaContract>(&contract_path, "DB schema contract")?;
    if contract.schema_version != DB_SCHEMA_CONTRACT_VERSION {
        bail!("DB schema contract schema_version mismatch");
    }
    if contract.required_tables.is_empty() {
        bail!("DB schema contract required_tables must not be empty");
    }

    let live_schema = read_live_schema(live_schema_json)?;
    let live_extensions = live_schema
        .extensions
        .into_iter()
        .map(|extension| extension.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    for extension in &contract.required_extensions {
        if !is_sql_identifier(extension) {
            bail!("live DB schema contract extension invalid: required_extensions");
        }
        if !live_extensions.contains(&extension.to_ascii_lowercase()) {
            bail!("live DB schema contract extension missing: {extension}");
        }
    }

    let live_tables = live_schema_index(live_schema.columns)?;
    for table in contract.required_tables {
        validate_required_table_name(&table.name)?;
        let required_columns = live_tables
            .get(&table.name.to_ascii_lowercase())
            .with_context(|| format!("live DB schema contract table missing: {}", table.name))?;
        for column in table.columns {
            if !is_sql_identifier(&column) {
                bail!(
                    "live DB schema contract column invalid: {}.{column}",
                    table.name
                );
            }
            if !required_columns.contains(&column.to_ascii_lowercase()) {
                bail!(
                    "live DB schema contract column missing: {}.{column}",
                    table.name
                );
            }
        }
    }
    Ok(())
}

fn validate_mirror_rebuild_summary(config: &Config, path: &Path) -> anyhow::Result<()> {
    let summary = read_json::<RebuildSummary>(path, "MirrorRebuildSummaryPath")?;
    if summary.schema_version != REBUILD_SUMMARY_SCHEMA_VERSION {
        bail!("MirrorRebuildSummaryPath schema_version mismatch");
    }
    if summary.validate_only {
        bail!("MirrorRebuildSummaryPath must reference an executed rebuild summary");
    }
    if summary.source_snapshot_id != config.serving_postgis_source_snapshot_id {
        bail!(
            "MirrorRebuildSummaryPath source_snapshot_id must match ServingPostgisSourceSnapshotId"
        );
    }
    if summary.source_table != "silver.parcel_boundaries" {
        bail!("MirrorRebuildSummaryPath source_table must be silver.parcel_boundaries");
    }
    if summary.target_srid != config.serving_postgis_srid {
        bail!("MirrorRebuildSummaryPath target_srid must match ServingPostgisSrid");
    }
    if summary.loaded_row_count != config.serving_postgis_loaded_row_count {
        bail!("MirrorRebuildSummaryPath loaded_row_count must match ServingPostgisLoadedRowCount");
    }
    if let Some(db_verification) = summary.db_verification {
        if db_verification.invalid_srid_count != 0 {
            bail!("MirrorRebuildSummaryPath db_verification invalid_srid_count must be zero");
        }
        if db_verification.mirror_row_count < config.serving_postgis_loaded_row_count {
            bail!(
                "MirrorRebuildSummaryPath db_verification mirror_row_count must cover ServingPostgisLoadedRowCount"
            );
        }
    }
    Ok(())
}

fn read_live_schema(path: &Path) -> anyhow::Result<LiveSchemaDocument> {
    let value = read_json::<serde_json::Value>(path, "LiveSchemaJson")?;
    if value.get("columns").is_some() {
        return serde_json::from_value(value).context("invalid LiveSchemaJson");
    }
    let columns = serde_json::from_value::<Vec<LiveSchemaColumn>>(value)
        .context("invalid LiveSchemaJson columns")?;
    Ok(LiveSchemaDocument {
        extensions: Vec::new(),
        columns,
    })
}

fn read_json<T>(path: &Path, label: &str) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes =
        fs::read(path).with_context(|| format!("failed to read {label} {}", path.display()))?;
    let bytes = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(&bytes);
    serde_json::from_slice(bytes)
        .with_context(|| format!("failed to parse {label} {}", path.display()))
}

fn live_schema_index(
    rows: Vec<LiveSchemaColumn>,
) -> anyhow::Result<BTreeMap<String, BTreeSet<String>>> {
    let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for row in rows {
        let schema_name = row.table_schema.or(row.schema_name).unwrap_or_default();
        let table_name = row.table_name.unwrap_or_default();
        let column_name = row.column_name.unwrap_or_default();
        if schema_name.trim().is_empty()
            || table_name.trim().is_empty()
            || column_name.trim().is_empty()
        {
            bail!("live DB schema row must include table_schema, table_name, and column_name");
        }
        tables
            .entry(format!(
                "{}.{}",
                schema_name.to_ascii_lowercase(),
                table_name.to_ascii_lowercase()
            ))
            .or_default()
            .insert(column_name.to_ascii_lowercase());
    }
    Ok(tables)
}

fn remove_sql_comments(content: &str) -> String {
    let mut output = String::with_capacity(content.len());
    let mut chars = content.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '/' && chars.peek() == Some(&'*') {
            let _ = chars.next();
            while let Some(block_ch) = chars.next() {
                if block_ch == '*' && chars.peek() == Some(&'/') {
                    let _ = chars.next();
                    break;
                }
            }
            output.push(' ');
        } else {
            output.push(ch);
        }
    }
    output
        .lines()
        .map(|line| line.split("--").next().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n")
}

fn normalize_sql(content: &str) -> String {
    let mut normalized = content
        .replace('"', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    for pattern in [" . ", " .", ". "] {
        normalized = normalized.replace(pattern, ".");
    }
    normalized
}

fn contains_identifier(sql: &str, identifier: &str) -> bool {
    sql.contains(&identifier.to_ascii_lowercase())
}

fn contains_numeric_token(sql: &str, token: &str) -> bool {
    sql.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .any(|part| part == token)
}

fn contains_create_schema(sql: &str, schema: &str) -> bool {
    sql.contains(&format!("create schema {schema}"))
        || sql.contains(&format!("create schema if not exists {schema}"))
}

fn contains_create_table_in_schema(sql: &str, schema: &str) -> bool {
    sql.contains(&format!("create table {schema}."))
        || sql.contains(&format!("create table if not exists {schema}."))
}

fn contains_create_table(sql: &str, table: &str) -> bool {
    sql.contains(&format!("create table {}", table.to_ascii_lowercase()))
        || sql.contains(&format!(
            "create table if not exists {}",
            table.to_ascii_lowercase()
        ))
}

fn contains_create_extension(sql: &str, extension: &str) -> bool {
    sql.contains(&format!("create extension {extension}"))
        || sql.contains(&format!("create extension if not exists {extension}"))
}

fn validate_migration_id(value: &str) -> anyhow::Result<()> {
    let Some((prefix, suffix)) = value.split_once('_') else {
        bail!("MigrationId must match <yyyymmddhhmmss>_<snake_case>");
    };
    if prefix.len() != 14
        || !prefix.bytes().all(|byte| byte.is_ascii_digit())
        || suffix.is_empty()
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        bail!("MigrationId must match <yyyymmddhhmmss>_<snake_case>");
    }
    Ok(())
}

fn validate_srid(value: &str) -> anyhow::Result<()> {
    let Some(srid) = value.strip_prefix("EPSG:") else {
        bail!("ServingPostgisSrid must use EPSG:<srid> format");
    };
    if srid.is_empty() || !srid.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("ServingPostgisSrid must use EPSG:<srid> format");
    }
    Ok(())
}

fn validate_source_snapshot_id(value: &str) -> anyhow::Result<()> {
    if !value.starts_with("iceberg:") || value.len() < "iceberg:abc".len() || value.len() > 136 {
        bail!("ServingPostgisSourceSnapshotId must use iceberg:<snapshot-id> format");
    }
    let suffix = &value["iceberg:".len()..];
    let mut chars = suffix.chars();
    let first = chars
        .next()
        .context("ServingPostgisSourceSnapshotId must use iceberg:<snapshot-id> format")?;
    if !first.is_ascii_alphanumeric()
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | ':' | '-'))
    {
        bail!("ServingPostgisSourceSnapshotId must use iceberg:<snapshot-id> format");
    }
    Ok(())
}

fn validate_dlq_table(value: &str) -> anyhow::Result<()> {
    let Some((schema, table)) = value.split_once('.') else {
        bail!("DlqTable must be a schema-qualified lowercase identifier");
    };
    if !is_sql_identifier(schema) || !is_sql_identifier(table) {
        bail!("DlqTable must be a schema-qualified lowercase identifier");
    }
    Ok(())
}

fn validate_required_table_name(value: &str) -> anyhow::Result<()> {
    let Some((schema, table)) = value.split_once('.') else {
        bail!("required_tables.name");
    };
    if !is_sql_identifier(schema) || !is_sql_identifier(table) {
        bail!("required_tables.name");
    }
    Ok(())
}

fn is_sql_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_lowercase() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn parse_positive_u64(value: &str, message: &str) -> anyhow::Result<u64> {
    let parsed = value.parse::<u64>().with_context(|| message.to_owned())?;
    if parsed == 0 {
        bail!("{message}");
    }
    Ok(parsed)
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.map_or_else(
        || {
            bail!(match name {
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_MIGRATION_ID" => "MigrationId is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_SRID" =>
                    "ServingPostgisSrid is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_REBUILD_STATUS" =>
                    "ServingPostgisRebuildStatus is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_SOURCE_SNAPSHOT_ID" =>
                    "ServingPostgisSourceSnapshotId is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_SERVING_POSTGIS_LOADED_ROW_COUNT" =>
                    "ServingPostgisLoadedRowCount is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_TABLE" => "DlqTable is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_PERSISTENCE_STATUS" =>
                    "DlqPersistenceStatus is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_INSPECTABLE_STATUS" =>
                    "DlqInspectableStatus is required",
                "FOUNDATION_PLATFORM_POSTGIS_MIRROR_DLQ_LIVE_SCHEMA_JSON" =>
                    "LiveSchemaJson is required",
                _ => "required environment variable is missing",
            })
        },
        Ok,
    )
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn resolve_input_path(root: &Path, path: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let resolved = normalize_under_root(root, path);
    if !path_within_root(root, &resolved) {
        if name == "LiveSchemaJson" {
            bail!("{LIVE_SCHEMA_JSON_INSIDE_ROOT_MESSAGE}");
        }
        bail!("{name} must be inside Root");
    }
    if !resolved.is_file() {
        bail!("{name} not found: {}", resolved.display());
    }
    Ok(resolved)
}

fn resolve_output_path(root: &Path, path: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let resolved = normalize_under_root(root, path);
    if !path_within_root(root, &resolved) {
        if name == "OutputPath" {
            bail!("{OUTPUT_PATH_STAY_WITHIN_ROOT_MESSAGE}");
        }
        bail!("{name} must stay within Root");
    }
    Ok(resolved)
}

fn normalize_under_root(root: &Path, path: &Path) -> PathBuf {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    normalize_path(candidate)
}

fn normalize_absolute_path(path: &Path) -> anyhow::Result<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed to read current directory")?
            .join(path)
    };
    Ok(normalize_path(candidate))
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn path_within_root(root: &Path, path: &Path) -> bool {
    let root = comparable_path(root);
    let path = comparable_path(path);
    path == root || path.starts_with(&format!("{root}/"))
}

fn comparable_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .trim_end_matches('/')
        .to_ascii_lowercase()
}
