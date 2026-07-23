use std::{
    env, fs,
    io::{BufRead, BufReader},
    path::{Component, Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

use crate::public_data_control_support::{repo_relative_path, utc_now, write_json_file};

const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.postgis_parcel_boundary_mirror_rebuild_summary.v1";
const SOURCE_TABLE: &str = "silver.parcel_boundaries";
const TARGET_SRID: i32 = 5179;
const SOURCE_SRID: i32 = 4326;
const GEOMETRY_REPAIR_STRATEGY: &str = "postgis-st_makevalid-collectionextract-polygon-v1";
const SUMMARY_OUTPUT_PATH_STAY_WITHIN_ROOT_MESSAGE: &str =
    "SummaryOutputPath must stay within Root";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let rows = read_handoff_rows(&config.handoff_jsonl_path)?;
    if rows.is_empty() {
        bail!("HandoffJsonlPath must contain at least one row");
    }
    if let Some(expected) = config.expected_count {
        if rows.len() as u64 != expected {
            bail!(
                "ExpectedCount does not match handoff row count. expected={} actual={}",
                expected,
                rows.len()
            );
        }
    }

    let source_object_key = config.source_object_key.clone().unwrap_or_else(|| {
        repo_relative_path(&config.root, &config.handoff_jsonl_path).replace('\\', "/")
    });
    validate_source_object_key(&source_object_key)?;

    let rebuild_run_id = Uuid::new_v4();
    let db_verification = if config.validate_only {
        None
    } else {
        let sql = build_rebuild_sql(
            &rows,
            rebuild_run_id,
            &config.source_snapshot_id,
            &source_object_key,
        )?;
        let verification = invoke_rebuild_sql(&config, &sql)?;
        verify_db_result(&verification, rows.len() as u64)?;
        Some(verification)
    };

    let summary = RebuildSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        validate_only: config.validate_only,
        rebuild_run_id: rebuild_run_id.to_string(),
        source_snapshot_id: config.source_snapshot_id.clone(),
        source_table: SOURCE_TABLE,
        source_object_key,
        handoff_jsonl_path: repo_relative_path(&config.root, &config.handoff_jsonl_path),
        target_srid: format!("EPSG:{TARGET_SRID}"),
        geometry_repair_strategy: GEOMETRY_REPAIR_STRATEGY,
        row_count: rows.len() as u64,
        loaded_row_count: if config.validate_only {
            0
        } else {
            rows.len() as u64
        },
        db_verification,
    };
    if let Some(path) = &config.summary_output_path {
        write_json_file(path, &summary)?;
    }

    if config.validate_only {
        println!(
            "postgis-parcel-boundary-mirror-rebuild-plan-ok rows={}",
            rows.len()
        );
    } else {
        println!(
            "postgis-parcel-boundary-mirror-rebuild-ok rows={} rebuild_run_id={}",
            rows.len(),
            summary.rebuild_run_id
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    root: PathBuf,
    handoff_jsonl_path: PathBuf,
    source_snapshot_id: String,
    source_object_key: Option<String>,
    expected_count: Option<u64>,
    database_url: Option<String>,
    psql_path: Option<PathBuf>,
    docker_path: Option<PathBuf>,
    docker_container_name: Option<String>,
    docker_user: String,
    docker_database: String,
    summary_output_path: Option<PathBuf>,
    validate_only: bool,
}

#[derive(Clone, Debug)]
struct HandoffRow {
    boundary_id: String,
    pnu: String,
    geometry_wkb_hex: String,
    geometry_checksum_sha256: String,
    handoff_source_snapshot_id: String,
    handoff_source_record_id: String,
    jibun: JsonValue,
    bonbun: JsonValue,
    bubun: JsonValue,
    bbox_min_x: f64,
    bbox_min_y: f64,
    bbox_max_x: f64,
    bbox_max_y: f64,
    valid_from_utc: JsonValue,
    valid_to_utc: JsonValue,
    ingested_at_utc: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DbVerification {
    mirror_row_count: u64,
    successful_rebuild_loaded_row_count: u64,
    invalid_srid_count: u64,
    invalid_geometry_count: u64,
}

#[derive(Debug, Serialize)]
struct RebuildSummary {
    schema_version: &'static str,
    generated_at_utc: String,
    validate_only: bool,
    rebuild_run_id: String,
    source_snapshot_id: String,
    source_table: &'static str,
    source_object_key: String,
    handoff_jsonl_path: String,
    target_srid: String,
    geometry_repair_strategy: &'static str,
    row_count: u64,
    loaded_row_count: u64,
    db_verification: Option<DbVerification>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root_raw =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_REBUILD_ROOT")?
                .unwrap_or_else(|| ".".to_owned());
        let root = normalize_absolute_path(Path::new(&root_raw))?;
        if !root.is_dir() {
            bail!("Root does not exist: {}", root.display());
        }

        if !bool_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_CONFIRM_REBUILD")? {
            bail!("Refusing PostGIS mirror rebuild without -ConfirmRebuild");
        }

        let source_snapshot_id =
            required_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_SNAPSHOT_ID")?;
        validate_source_snapshot_id(&source_snapshot_id)?;

        let handoff_raw =
            required_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_HANDOFF_JSONL_PATH")?;
        let handoff_jsonl_path =
            resolve_input_path(&root, Path::new(&handoff_raw), "HandoffJsonlPath")?;

        let summary_output_path =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SUMMARY_OUTPUT_PATH")?
                .map(|path| resolve_output_path(&root, Path::new(&path), "SummaryOutputPath"))
                .transpose()?;

        let expected_count =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_EXPECTED_COUNT")?
                .map(|value| parse_positive_u64(&value, "ExpectedCount"))
                .transpose()?;

        Ok(Self {
            root,
            handoff_jsonl_path,
            source_snapshot_id,
            source_object_key: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_OBJECT_KEY",
            )?,
            expected_count,
            database_url: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_DATABASE_URL",
            )?,
            psql_path: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_PSQL_PATH",
            )?
            .map(PathBuf::from),
            docker_path: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_DOCKER_PATH",
            )?
            .map(PathBuf::from),
            docker_container_name: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_DOCKER_CONTAINER_NAME",
            )?,
            docker_user: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_DOCKER_USER",
            )?
            .unwrap_or_else(|| "foundation_platform".to_owned()),
            docker_database: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_DOCKER_DATABASE",
            )?
            .unwrap_or_else(|| "foundation_platform".to_owned()),
            summary_output_path,
            validate_only: bool_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_VALIDATE_ONLY",
            )?,
        })
    }
}

fn read_handoff_rows(path: &Path) -> anyhow::Result<Vec<HandoffRow>> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.with_context(|| {
            format!(
                "failed to read handoff JSONL line {line_number} from {}",
                path.display()
            )
        })?;
        let line = if line_number == 1 {
            line.strip_prefix('\u{feff}').unwrap_or(&line)
        } else {
            line.as_str()
        };
        if line.trim().is_empty() {
            continue;
        }

        let row = serde_json::from_str::<JsonValue>(line)
            .map_err(|_| anyhow::anyhow!("handoff JSONL line {line_number} is not valid JSON"))?;
        rows.push(validate_handoff_row(&row)?);
    }

    Ok(rows)
}

fn validate_handoff_row(row: &JsonValue) -> anyhow::Result<HandoffRow> {
    let pnu = required_text(row, "pnu")?;
    if !is_digits(&pnu, 19) {
        bail!("handoff row pnu must be 19 digits: {pnu}");
    }
    let boundary_id = required_text(row, "boundary_id")?;
    let geometry_wkb_hex = required_text(row, "geometry_wkb_hex")?;
    if !is_lower_hex(&geometry_wkb_hex) || geometry_wkb_hex.len() % 2 != 0 {
        bail!("handoff row geometry_wkb_hex must be lowercase even-length hex");
    }
    let geometry_wkb_encoding = required_text(row, "geometry_wkb_encoding")?;
    if geometry_wkb_encoding != "hex" {
        bail!("handoff row geometry_wkb_encoding must be hex");
    }
    let geometry_srid = required_i32(row, "geometry_srid")?;
    if geometry_srid != SOURCE_SRID {
        bail!("handoff row geometry_srid must be {SOURCE_SRID}");
    }
    let geometry_checksum_sha256 = required_text(row, "geometry_checksum_sha256")?;
    if !is_sha256_hex(&geometry_checksum_sha256) {
        bail!("handoff row geometry_checksum_sha256 must be lowercase sha256");
    }

    Ok(HandoffRow {
        boundary_id,
        pnu,
        geometry_wkb_hex,
        geometry_checksum_sha256,
        handoff_source_snapshot_id: required_text(row, "source_snapshot_id")?,
        handoff_source_record_id: required_text(row, "source_record_id")?,
        jibun: field_or_null(row, "jibun"),
        bonbun: field_or_null(row, "bonbun"),
        bubun: field_or_null(row, "bubun"),
        bbox_min_x: required_f64(row, "bbox_min_x")?,
        bbox_min_y: required_f64(row, "bbox_min_y")?,
        bbox_max_x: required_f64(row, "bbox_max_x")?,
        bbox_max_y: required_f64(row, "bbox_max_y")?,
        valid_from_utc: field_or_null(row, "valid_from_utc"),
        valid_to_utc: field_or_null(row, "valid_to_utc"),
        ingested_at_utc: field_or_null(row, "ingested_at_utc"),
    })
}

fn build_rebuild_sql(
    rows: &[HandoffRow],
    rebuild_run_id: Uuid,
    snapshot_id: &str,
    object_key: &str,
) -> anyhow::Result<String> {
    let quality_report = json!({
        "row_count": rows.len(),
        "source_object": object_key,
        "target_srid": TARGET_SRID,
        "geometry_repair_strategy": GEOMETRY_REPAIR_STRATEGY,
    });
    let pnu_list = rows
        .iter()
        .map(|row| sql_literal(&row.pnu))
        .collect::<Vec<_>>()
        .join(", ");
    let mut statements = Vec::new();
    statements.push("BEGIN;".to_owned());
    statements.push(format!(
        r#"INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run (
    id, source_snapshot_id, source_table, srid, status, loaded_row_count,
    rejected_row_count, quality_report, started_at, created_at, updated_at
) VALUES (
    {}::uuid,
    {},
    '{}',
    {},
    'running',
    0,
    0,
    {},
    now(),
    now(),
    now()
);"#,
        sql_literal(&rebuild_run_id.to_string()),
        sql_literal(snapshot_id),
        SOURCE_TABLE,
        TARGET_SRID,
        jsonb_literal(&quality_report)?
    ));
    statements.push(format!(
        "DELETE FROM serving_postgis.parcel_boundary_mirror WHERE pnu IN ({pnu_list});"
    ));
    for row in rows {
        let properties = mirror_row_properties(row);
        let serving_geometry_sql = serving_geometry_sql(&row.geometry_wkb_hex);
        statements.push(format!(
            r#"INSERT INTO serving_postgis.parcel_boundary_mirror (
    pnu,
    rebuild_run_id,
    source_snapshot_id,
    source_table,
    source_record_id,
    source_file_asset_id,
    source_object_key,
    source_row_id,
    complex_id,
    parcel_id,
    geometry_checksum_sha256,
    properties,
    geom,
    loaded_at,
    updated_at,
    version
) VALUES (
    {},
    {}::uuid,
    {},
    '{}',
    NULL,
    NULL,
    {},
    {},
    NULL,
    NULL,
    {},
    {},
    {},
    now(),
    now(),
    1
);"#,
            sql_literal(&row.pnu),
            sql_literal(&rebuild_run_id.to_string()),
            sql_literal(snapshot_id),
            SOURCE_TABLE,
            sql_literal(object_key),
            sql_literal(&row.boundary_id),
            sql_literal(&row.geometry_checksum_sha256),
            jsonb_literal(&properties)?,
            serving_geometry_sql
        ));
    }
    statements.push(format!(
        r#"UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
SET status = 'succeeded',
    loaded_row_count = {},
    rejected_row_count = 0,
    finished_at = now(),
    updated_at = now()
WHERE id = {}::uuid
  AND source_snapshot_id = {};
COMMIT;
SELECT json_build_object(
    'mirror_row_count', (
        SELECT count(*) FROM serving_postgis.parcel_boundary_mirror
        WHERE source_snapshot_id = {}
    ),
    'successful_rebuild_loaded_row_count', (
        SELECT loaded_row_count FROM serving_postgis.parcel_boundary_mirror_rebuild_run
        WHERE id = {}::uuid
          AND source_snapshot_id = {}
          AND status = 'succeeded'
    ),
    'invalid_srid_count', (
        SELECT count(*) FROM serving_postgis.parcel_boundary_mirror
        -- TARGET_SRID validation for serving geom.
        WHERE source_snapshot_id = {}
          AND ST_SRID(geom) <> {}
    ),
    'invalid_geometry_count', (
        SELECT count(*) FROM serving_postgis.parcel_boundary_mirror
        -- TARGET_SRID geometry validity check for serving geom.
        WHERE source_snapshot_id = {}
          AND NOT ST_IsValid(geom)
    )
)::text;"#,
        rows.len(),
        sql_literal(&rebuild_run_id.to_string()),
        sql_literal(snapshot_id),
        sql_literal(snapshot_id),
        sql_literal(&rebuild_run_id.to_string()),
        sql_literal(snapshot_id),
        sql_literal(snapshot_id),
        TARGET_SRID,
        sql_literal(snapshot_id)
    ));
    Ok(statements.join("\n"))
}

fn mirror_row_properties(row: &HandoffRow) -> JsonValue {
    json!({
        "boundary_id": row.boundary_id,
        "handoff_source_snapshot_id": row.handoff_source_snapshot_id,
        "handoff_source_record_id": row.handoff_source_record_id,
        "jibun": row.jibun,
        "bonbun": row.bonbun,
        "bubun": row.bubun,
        "bbox": {
            "min_x": row.bbox_min_x,
            "min_y": row.bbox_min_y,
            "max_x": row.bbox_max_x,
            "max_y": row.bbox_max_y,
        },
        "valid_from_utc": row.valid_from_utc,
        "valid_to_utc": row.valid_to_utc,
        "ingested_at_utc": row.ingested_at_utc,
    })
}

fn source_geometry_sql(geometry_wkb_hex: &str) -> String {
    format!(
        "ST_SetSRID(ST_GeomFromWKB(decode({}, 'hex')), {})",
        sql_literal(geometry_wkb_hex),
        SOURCE_SRID
    )
}

fn serving_geometry_sql(geometry_wkb_hex: &str) -> String {
    let source_geometry = source_geometry_sql(geometry_wkb_hex);
    format!(
        "ST_Multi(ST_Transform(ST_CollectionExtract(ST_MakeValid({source_geometry}), 3), {TARGET_SRID}))"
    )
}

fn invoke_rebuild_sql(config: &Config, sql: &str) -> anyhow::Result<DbVerification> {
    let raw = if config.docker_container_name.is_some() {
        invoke_docker_psql(config, sql)?
    } else {
        invoke_host_psql(config, sql)?
    };
    parse_db_verification(&raw)
}

fn invoke_host_psql(config: &Config, sql: &str) -> anyhow::Result<String> {
    let database_url = config
        .database_url
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("DatabaseUrl must not be blank")?;
    let psql = resolve_executable(
        config.psql_path.as_deref(),
        "psql",
        "psql is required for PostGIS mirror rebuild",
    )?;
    let sql_path = temp_sql_path("foundation-platform-postgis-mirror-rebuild");
    fs::write(&sql_path, sql).with_context(|| format!("failed to write {}", sql_path.display()))?;
    let output = Command::new(&psql)
        .args([
            "-d",
            database_url,
            "-X",
            "-q",
            "-v",
            "ON_ERROR_STOP=1",
            "-t",
            "-A",
            "-f",
        ])
        .arg(&sql_path)
        .output()
        .with_context(|| format!("failed to run {}", psql.display()));
    let _ = fs::remove_file(&sql_path);
    let output = output?;
    if !output.status.success() {
        bail!(
            "psql PostGIS mirror rebuild failed with exit code {}. Output: {}{}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .trim()
    .to_owned())
}

fn invoke_docker_psql(config: &Config, sql: &str) -> anyhow::Result<String> {
    let container = config
        .docker_container_name
        .as_deref()
        .context("DockerContainerName must use a safe docker name")?;
    if !is_safe_docker_name(container) {
        bail!("DockerContainerName must use a safe docker name");
    }
    if !is_sql_identifier(&config.docker_user) || !is_sql_identifier(&config.docker_database) {
        bail!("DockerUser and DockerDatabase must be SQL identifiers");
    }
    let docker = resolve_executable(
        config.docker_path.as_deref(),
        "docker",
        "docker is required for Docker PostGIS mirror rebuild",
    )?;
    let sql_file_name = format!(
        "foundation-platform-postgis-mirror-rebuild-{}.sql",
        Uuid::new_v4()
    );
    let host_sql_path = env::temp_dir().join(&sql_file_name);
    let container_sql_path = format!("/tmp/{sql_file_name}");
    fs::write(&host_sql_path, sql)
        .with_context(|| format!("failed to write {}", host_sql_path.display()))?;

    let copy_target = format!("{container}:{container_sql_path}");
    let copy_output = Command::new(&docker)
        .arg("cp")
        .arg(&host_sql_path)
        .arg(&copy_target)
        .output()
        .with_context(|| format!("failed to run {}", docker.display()));
    let _ = fs::remove_file(&host_sql_path);
    let copy_output = copy_output?;
    if !copy_output.status.success() {
        bail!(
            "docker cp PostGIS mirror SQL failed with exit code {}. Output: {}{}",
            copy_output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&copy_output.stdout),
            String::from_utf8_lossy(&copy_output.stderr)
        );
    }

    let output = Command::new(&docker)
        .args([
            "exec",
            container,
            "psql",
            "-U",
            &config.docker_user,
            "-d",
            &config.docker_database,
            "-X",
            "-q",
            "-v",
            "ON_ERROR_STOP=1",
            "-t",
            "-A",
            "-f",
            &container_sql_path,
        ])
        .output()
        .with_context(|| format!("failed to run {}", docker.display()))?;
    let _ = Command::new(&docker)
        .args(["exec", container, "rm", "-f", &container_sql_path])
        .output();
    if !output.status.success() {
        bail!(
            "docker psql PostGIS mirror rebuild failed with exit code {}. Output: {}{}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .trim()
    .to_owned())
}

fn parse_db_verification(raw: &str) -> anyhow::Result<DbVerification> {
    if let Ok(value) = serde_json::from_str::<DbVerification>(raw.trim()) {
        return Ok(value);
    }
    let candidate = raw
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| line.starts_with('{') && line.ends_with('}'))
        .context("PostGIS mirror rebuild did not return JSON verification")?;
    serde_json::from_str(candidate).context("failed to parse PostGIS mirror verification JSON")
}

fn verify_db_result(verification: &DbVerification, expected_rows: u64) -> anyhow::Result<()> {
    if verification.mirror_row_count != expected_rows {
        bail!("PostGIS mirror row count verification failed");
    }
    if verification.successful_rebuild_loaded_row_count != expected_rows {
        bail!("PostGIS mirror rebuild row count verification failed");
    }
    if verification.invalid_srid_count != 0 {
        bail!("PostGIS mirror SRID verification failed");
    }
    if verification.invalid_geometry_count != 0 {
        bail!("PostGIS mirror geometry validity verification failed");
    }
    Ok(())
}

fn resolve_executable(
    explicit_path: Option<&Path>,
    command_name: &str,
    _missing_message: &str,
) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit_path {
        if !path.is_file() {
            bail!("{command_name} path does not exist: {}", path.display());
        }
        return normalize_absolute_path(path);
    }
    Ok(PathBuf::from(command_name))
}

fn resolve_input_path(root: &Path, path: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let resolved = normalize_under_root(root, path);
    if !path_within_root(root, &resolved) {
        if name == "SummaryOutputPath" {
            bail!("{SUMMARY_OUTPUT_PATH_STAY_WITHIN_ROOT_MESSAGE}");
        }
        bail!("{name} must stay within Root");
    }
    if !resolved.is_file() {
        bail!("{name} not found: {}", resolved.display());
    }
    Ok(resolved)
}

fn resolve_output_path(root: &Path, path: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let resolved = normalize_under_root(root, path);
    if !path_within_root(root, &resolved) {
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

fn validate_source_snapshot_id(value: &str) -> anyhow::Result<()> {
    if !value.starts_with("iceberg:") || value.len() < "iceberg:abc".len() || value.len() > 136 {
        bail!("SourceSnapshotId must use iceberg:<snapshot-id> format");
    }
    let suffix = &value["iceberg:".len()..];
    let mut chars = suffix.chars();
    let first = chars
        .next()
        .context("SourceSnapshotId must use iceberg:<snapshot-id> format")?;
    if !first.is_ascii_alphanumeric()
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | ':' | '-'))
    {
        bail!("SourceSnapshotId must use iceberg:<snapshot-id> format");
    }
    Ok(())
}

fn validate_source_object_key(value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty()
        || value.starts_with('/')
        || value.contains('\\')
        || value.contains("//")
        || value.starts_with("./")
        || value.starts_with("../")
        || value.contains("/./")
        || value.contains("/../")
        || value.ends_with("/.")
        || value.ends_with("/..")
    {
        bail!("SourceObjectKey must be a non-rooted normalized object key");
    }
    Ok(())
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.map_or_else(|| bail!("{name} is required"), Ok)
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn bool_env(name: &str) -> anyhow::Result<bool> {
    Ok(optional_env(name)?.is_some_and(|value| value.eq_ignore_ascii_case("true")))
}

fn parse_positive_u64(value: &str, label: &str) -> anyhow::Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{label} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{label} must be a positive integer");
    }
    Ok(parsed)
}

fn required_text(row: &JsonValue, field: &str) -> anyhow::Result<String> {
    let value = field_text(row, field);
    if value.trim().is_empty() {
        bail!("handoff row missing required text field: {field}");
    }
    Ok(value)
}

fn required_i32(row: &JsonValue, field: &str) -> anyhow::Result<i32> {
    let value = row.get(field);
    let parsed = match value {
        Some(JsonValue::Number(number)) => {
            number.as_i64().and_then(|value| i32::try_from(value).ok())
        }
        Some(JsonValue::String(text)) => text.parse::<i32>().ok(),
        _ => None,
    };
    parsed.with_context(|| format!("handoff row missing required integer field: {field}"))
}

fn required_f64(row: &JsonValue, field: &str) -> anyhow::Result<f64> {
    let value = row.get(field);
    let parsed = match value {
        Some(JsonValue::Number(number)) => number.as_f64(),
        Some(JsonValue::String(text)) => text.parse::<f64>().ok(),
        _ => None,
    };
    let parsed = parsed
        .with_context(|| format!("handoff row missing required finite numeric field: {field}"))?;
    if !parsed.is_finite() {
        bail!("handoff row missing required finite numeric field: {field}");
    }
    Ok(parsed)
}

fn field_text(row: &JsonValue, field: &str) -> String {
    match row.get(field) {
        Some(JsonValue::String(text)) => text.clone(),
        Some(JsonValue::Number(number)) => number.to_string(),
        Some(JsonValue::Bool(flag)) => flag.to_string(),
        Some(JsonValue::Null) | None => String::new(),
        Some(value) => value.to_string(),
    }
}

fn field_or_null(row: &JsonValue, field: &str) -> JsonValue {
    row.get(field).cloned().unwrap_or(JsonValue::Null)
}

fn is_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_lower_hex(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && is_lower_hex(value)
}

fn is_safe_docker_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn is_sql_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn jsonb_literal(value: &JsonValue) -> anyhow::Result<String> {
    let json = serde_json::to_string(value).context("failed to serialize JSONB value")?;
    let hex = json
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "convert_from(decode({}, 'hex'), 'UTF8')::jsonb",
        sql_literal(&hex)
    ))
}

fn temp_sql_path(prefix: &str) -> PathBuf {
    env::temp_dir().join(format!("{prefix}-{}.sql", Uuid::new_v4()))
}
