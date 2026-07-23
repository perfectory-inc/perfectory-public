use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use collection_domain::validate_bronze_object_key_contract;
use foundation_outbox::{object_storage::R2InventoryRequest, R2ObjectStorage};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::r2_command_support::{
    canonical_path, env_bool, env_path, normalize_windows_verbatim_path, optional_env,
    r2_config_from_env_file, read_json, resolve_path, utc_now, write_json_file,
};
use crate::r2_layout::{
    is_bronze_catalog_recovery_evidence_key, PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT,
    VECTOR_TILE_ARTIFACT_ROOT, VECTOR_TILE_MANIFEST_ROOT,
};

const AUDIT_SCHEMA_VERSION: &str = "foundation-platform.r2_inventory_audit.v1";
const DELETE_CANDIDATES_SCHEMA_VERSION: &str = "foundation-platform.r2_delete_candidates.v1";
const DEFAULT_OUTPUT_DIR: &str = "target/r2-inventory-audit";
const DEFAULT_ENV_FILE: &str = ".env.local";
const REPORT_FILE_NAME: &str = "r2-inventory-audit.json";
const DELETE_CANDIDATES_FILE_NAME: &str = "r2-delete-candidates.json";
const PREFIX: &str = "FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let (objects, source, list_request_count) = read_inventory(&config).await?;
    let estimated_cost = optional_estimated_cost(
        config.estimated_list_request_unit_cost_usd.as_deref(),
        list_request_count,
    )?;
    let bronze_orphan_evidence = load_bronze_orphan_evidence(&config).await?;
    let report = build_audit_report(
        objects,
        source,
        list_request_count,
        estimated_cost,
        &bronze_orphan_evidence,
    );

    let report_path = config.output_dir.join(REPORT_FILE_NAME);
    let delete_candidates_path = config.output_dir.join(DELETE_CANDIDATES_FILE_NAME);
    write_usage_metrics(config.usage_metrics_path.as_deref(), &report)?;
    write_json_file(&report_path, &report)?;
    write_json_file(
        &delete_candidates_path,
        &delete_candidate_manifest(&report_path, &report),
    )?;

    write_summary(config.quiet, &report_path, &delete_candidates_path, &report)?;
    Ok(())
}

struct Config {
    input_json: Option<PathBuf>,
    output_dir: PathBuf,
    env_file: PathBuf,
    prefix: Option<String>,
    max_keys: i32,
    usage_metrics_path: Option<PathBuf>,
    estimated_list_request_unit_cost_usd: Option<String>,
    bronze_orphan_min_age_hours: u64,
    quiet: bool,
}

#[derive(Clone, Debug, Serialize)]
struct AuditObject {
    key: String,
    size_bytes: i64,
    last_modified: Option<String>,
    e_tag: Option<String>,
    classification: &'static str,
    action: &'static str,
    reason: &'static str,
}

#[derive(Clone, Debug)]
struct RawObject {
    key: String,
    size_bytes: i64,
    last_modified: Option<String>,
    e_tag: Option<String>,
}

#[derive(Debug, Serialize)]
struct AuditReport {
    schema_version: &'static str,
    generated_at_utc: String,
    source: &'static str,
    list_request_count: usize,
    estimated_list_request_cost_usd: Option<f64>,
    object_count: usize,
    total_size_bytes: i64,
    keep_count: usize,
    delete_candidate_count: usize,
    review_count: usize,
    classification_summary: Vec<ClassificationSummary>,
    objects: Vec<AuditObject>,
}

#[derive(Debug, Serialize)]
struct ClassificationSummary {
    classification: &'static str,
    count: usize,
    size_bytes: i64,
}

#[derive(Debug, Serialize)]
struct DeleteCandidateManifest<'a> {
    schema_version: &'static str,
    generated_at_utc: &'a str,
    source_report: String,
    object_count: usize,
    total_size_bytes: i64,
    objects: Vec<AuditObject>,
}

#[derive(Debug, Deserialize)]
struct S3Object {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "LastModified")]
    last_modified: Option<String>,
    #[serde(rename = "ETag", default)]
    e_tag: Option<String>,
}

struct Classification {
    name: &'static str,
    action: &'static str,
    reason: &'static str,
}

struct BronzeOrphanEvidence {
    db_keys: HashSet<String>,
    min_age_hours: u64,
    object_ages: HashMap<String, u64>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let input_json = optional_env_path(&format!("{PREFIX}_INPUT_JSON"))?;
        let output_dir = env_path(&format!("{PREFIX}_OUTPUT_DIR"), DEFAULT_OUTPUT_DIR)?;
        let env_file = env_path(&format!("{PREFIX}_ENV_FILE"), DEFAULT_ENV_FILE)?;
        let prefix = optional_env(&format!("{PREFIX}_PREFIX"))?;
        let max_keys = optional_env(&format!("{PREFIX}_MAX_KEYS"))?
            .map(|raw| parse_i32(&raw, "MaxKeys"))
            .transpose()?
            .unwrap_or(1000);

        Ok(Self {
            input_json: input_json.map(|path| resolve_path(&root, &path)),
            output_dir: resolve_path(&root, &output_dir),
            env_file: resolve_path(&root, &env_file),
            prefix,
            max_keys,
            usage_metrics_path: optional_env_path(&format!("{PREFIX}_USAGE_METRICS_PATH"))?
                .map(|path| resolve_path(&root, &path)),
            estimated_list_request_unit_cost_usd: optional_env(&format!(
                "{PREFIX}_ESTIMATED_LIST_REQUEST_UNIT_COST_USD"
            ))?,
            bronze_orphan_min_age_hours: optional_env(&format!(
                "{PREFIX}_BRONZE_ORPHAN_MIN_AGE_HOURS"
            ))?
            .map(|raw| parse_u64(&raw, "BronzeOrphanMinAgeHours"))
            .transpose()?
            .unwrap_or(24),
            quiet: env_bool(&format!("{PREFIX}_QUIET"), false)?,
        })
    }
}

async fn read_inventory(config: &Config) -> anyhow::Result<(Vec<RawObject>, &'static str, usize)> {
    if let Some(path) = &config.input_json {
        return Ok((read_inventory_from_json(path)?, "input_json", 1));
    }

    let request = R2InventoryRequest::new(config.prefix.as_deref(), Some(config.max_keys))
        .context("invalid R2 inventory audit request")?;
    let storage = R2ObjectStorage::from_config(r2_config_from_env_file(&config.env_file)?);
    let report = storage.inventory_audit(request).await?;
    let objects = report
        .objects()
        .iter()
        .map(|object| RawObject {
            key: object.key.clone(),
            size_bytes: object.size_bytes,
            last_modified: object.last_modified.clone(),
            e_tag: object.e_tag.clone(),
        })
        .collect();
    Ok((objects, "live_r2_read_only", report.list_request_count()))
}

fn read_inventory_from_json(path: &Path) -> anyhow::Result<Vec<RawObject>> {
    if !path.is_file() {
        bail!("InputJson not found: {}", path.display());
    }
    let raw = read_json(path, "R2 inventory input JSON")?;
    let values = if let Some(contents) = raw.get("Contents") {
        contents
            .as_array()
            .context("InputJson Contents must be an array")?
            .clone()
    } else if let Some(array) = raw.as_array() {
        array.clone()
    } else {
        vec![raw]
    };

    values
        .into_iter()
        .map(|value| {
            let object: S3Object =
                serde_json::from_value(value).context("failed to parse R2 inventory object")?;
            if object.key.trim().is_empty() {
                bail!("R2 inventory object omitted Key.");
            }
            Ok(RawObject {
                key: object.key,
                size_bytes: object.size,
                last_modified: object.last_modified,
                e_tag: object.e_tag,
            })
        })
        .collect()
}

fn build_audit_report(
    mut objects: Vec<RawObject>,
    source: &'static str,
    list_request_count: usize,
    estimated_list_request_cost_usd: Option<f64>,
    bronze_orphan_evidence: &BronzeOrphanEvidence,
) -> AuditReport {
    let r2_keys = objects
        .iter()
        .map(|object| object.key.clone())
        .collect::<Vec<_>>();
    let bronze_orphan_keys = find_bronze_orphans(
        &r2_keys,
        &bronze_orphan_evidence.db_keys,
        bronze_orphan_evidence.min_age_hours,
        &bronze_orphan_evidence.object_ages,
    )
    .into_iter()
    .collect::<HashSet<_>>();
    objects.sort_by(|left, right| left.key.cmp(&right.key));
    let audited_objects: Vec<_> = objects
        .into_iter()
        .map(|object| {
            let classification = classify_key_with_bronze_evidence(
                &object.key,
                &bronze_orphan_evidence.db_keys,
                &bronze_orphan_keys,
            );
            AuditObject {
                key: object.key,
                size_bytes: object.size_bytes,
                last_modified: object.last_modified,
                e_tag: object.e_tag,
                classification: classification.name,
                action: classification.action,
                reason: classification.reason,
            }
        })
        .collect();

    let keep_count = audited_objects
        .iter()
        .filter(|object| object.action == "keep")
        .count();
    let delete_candidate_count = audited_objects
        .iter()
        .filter(|object| object.action == "delete_candidate")
        .count();
    let review_count = audited_objects
        .iter()
        .filter(|object| object.action == "review")
        .count();

    AuditReport {
        schema_version: AUDIT_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        source,
        list_request_count,
        estimated_list_request_cost_usd,
        object_count: audited_objects.len(),
        total_size_bytes: size_sum(&audited_objects),
        keep_count,
        delete_candidate_count,
        review_count,
        classification_summary: classification_summary(&audited_objects),
        objects: audited_objects,
    }
}

fn delete_candidate_manifest<'a>(
    report_path: &Path,
    report: &'a AuditReport,
) -> DeleteCandidateManifest<'a> {
    let objects: Vec<_> = report
        .objects
        .iter()
        .filter(|object| object.action == "delete_candidate")
        .cloned()
        .collect();
    DeleteCandidateManifest {
        schema_version: DELETE_CANDIDATES_SCHEMA_VERSION,
        generated_at_utc: &report.generated_at_utc,
        source_report: canonical_path(report_path),
        object_count: objects.len(),
        total_size_bytes: size_sum(&objects),
        objects,
    }
}

fn classification_summary(objects: &[AuditObject]) -> Vec<ClassificationSummary> {
    let mut summary = BTreeMap::<&'static str, (usize, i64)>::new();
    for object in objects {
        let entry = summary.entry(object.classification).or_default();
        entry.0 += 1;
        entry.1 += object.size_bytes;
    }
    summary
        .into_iter()
        .map(
            |(classification, (count, size_bytes))| ClassificationSummary {
                classification,
                count,
                size_bytes,
            },
        )
        .collect()
}

async fn load_bronze_orphan_evidence(config: &Config) -> anyhow::Result<BronzeOrphanEvidence> {
    let database_url = bronze_orphan_database_url(config)?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to catalog database for R2 Bronze orphan evidence")?;
    let db_keys = load_bronze_object_keys(&pool).await?;
    let object_ages = load_displaced_bronze_object_ages(&pool).await?;
    Ok(BronzeOrphanEvidence {
        db_keys,
        min_age_hours: config.bronze_orphan_min_age_hours,
        object_ages,
    })
}

fn bronze_orphan_database_url(config: &Config) -> anyhow::Result<String> {
    let env_file_values = read_env_file_values(&config.env_file)?;
    bronze_orphan_database_url_from_values(
        optional_env(&format!("{PREFIX}_DATABASE_URL"))?,
        optional_env("DATABASE_URL")?,
        env_file_values
            .get(&format!("{PREFIX}_DATABASE_URL"))
            .cloned(),
        env_file_values.get("DATABASE_URL").cloned(),
    )
}

fn bronze_orphan_database_url_from_values(
    audit_url: Option<String>,
    database_url: Option<String>,
    env_file_audit_url: Option<String>,
    env_file_database_url: Option<String>,
) -> anyhow::Result<String> {
    audit_url
        .or(database_url)
        .or(env_file_audit_url)
        .or(env_file_database_url)
        .with_context(|| {
            "R2 inventory audit requires DATABASE_URL or FOUNDATION_PLATFORM_R2_INVENTORY_AUDIT_DATABASE_URL for Bronze orphan DB evidence"
        })
}

async fn load_bronze_object_keys(pool: &PgPool) -> anyhow::Result<HashSet<String>> {
    let keys = sqlx::query_scalar::<_, String>("SELECT object_key FROM catalog.bronze_object")
        .fetch_all(pool)
        .await
        .context("failed to load catalog.bronze_object keys for R2 audit")?;
    Ok(keys.into_iter().collect())
}

async fn load_displaced_bronze_object_ages(pool: &PgPool) -> anyhow::Result<HashMap<String, u64>> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT payload->>'displaced_object_key' AS object_key,
                GREATEST(
                    FLOOR(EXTRACT(EPOCH FROM (now() - occurred_at)) / 3600),
                    0
                )::bigint AS age_hours
         FROM catalog.outbox_event
         WHERE type = 'catalog.bronze_object.orphan_candidate.displaced.v1'
           AND payload ? 'displaced_object_key'",
    )
    .fetch_all(pool)
    .await
    .context("failed to load displaced Bronze object ages for R2 audit")?;

    let mut ages = HashMap::new();
    for (key, age_hours) in rows {
        if key.trim().is_empty() {
            continue;
        }
        let age_hours = u64::try_from(age_hours).unwrap_or(0);
        ages.entry(key)
            .and_modify(|existing: &mut u64| *existing = (*existing).max(age_hours))
            .or_insert(age_hours);
    }
    Ok(ages)
}

fn classify_key_with_bronze_evidence(
    key: &str,
    catalog_keys: &HashSet<String>,
    bronze_orphan_keys: &HashSet<String>,
) -> Classification {
    let classification = classify_key(key);
    if classification.name != "current_bronze_contract" {
        return classification;
    }
    if catalog_keys.contains(key) {
        return classification;
    }
    if bronze_orphan_keys.contains(key) {
        return Classification {
            name: "bronze_orphan_candidate",
            action: "delete_candidate",
            reason: "Bronze object is absent from catalog.bronze_object and older than the orphan safety window.",
        };
    }
    Classification {
        name: "bronze_catalog_metadata_missing",
        action: "review",
        reason: "Bronze bytes exist in R2 but catalog.bronze_object has no metadata row; reconcile or re-collect before treating the object as current.",
    }
}

fn classify_key(key: &str) -> Classification {
    if key.starts_with('/') || key.contains("..") {
        return Classification {
            name: "invalid_object_key",
            action: "review",
            reason: "Object key is not a clean provider-relative key.",
        };
    }
    if key.starts_with("__r2_data_catalog/") {
        return Classification {
            name: "managed_iceberg_catalog",
            action: "keep",
            reason: "Cloudflare R2 Data Catalog / Iceberg metadata is catalog-managed.",
        };
    }
    if is_bronze_catalog_recovery_evidence_key(key) {
        return Classification {
            name: "bronze_catalog_recovery_evidence",
            action: "keep",
            reason: "Immutable content-addressed evidence for a Bronze Catalog recovery apply.",
        };
    }
    if key == "gold/manifest.json" {
        return Classification {
            name: "runtime_manifest_pointer",
            action: "keep",
            reason: "Canonical runtime manifest pointer.",
        };
    }
    if is_current_spatial_artifact(key) {
        return Classification {
            name: "runtime_spatial_artifact",
            action: "keep",
            reason: "Canonical immutable spatial artifact or manifest.",
        };
    }
    if is_legacy_semantic_versioned_gold_key(key) || is_legacy_spatial_artifact(key) {
        return Classification {
            name: "legacy_gold_artifact",
            action: "review",
            reason: "Legacy Gold layout must be checked against Catalog pointers before migration or deletion.",
        };
    }
    if key.starts_with("silver-handoff/") {
        return Classification {
            name: "silver_handoff_artifact",
            action: "keep",
            reason:
                "Silver handoff artifact is a replayable national promotion input or audit output.",
        };
    }
    if is_legacy_date_partitioned_bronze_key(key) {
        return Classification {
            name: "legacy_date_partitioned_bronze",
            action: "review",
            reason: "Date-partitioned Bronze key is legacy; copy to the source/run_id/partition contract before deleting the old object.",
        };
    }
    if key.starts_with("bronze/source=") {
        if validate_bronze_object_key_contract(key).is_err() {
            return Classification {
                name: "legacy_bronze_path_contract",
                action: "review",
                reason: "Bronze object key contains legacy or non-identity path segments; migrate metadata to Catalog and preserve bytes before deleting the old key.",
            };
        }
        return Classification {
            name: "current_bronze_contract",
            action: "keep",
            reason: "Foundation Platform source-partitioned Bronze object contract.",
        };
    }
    if key.starts_with("warehouse/") {
        if key.contains("smoke") {
            return Classification {
                name: "iceberg_catalog_cleanup_required",
                action: "review",
                reason: "Iceberg smoke table files must be removed through the Catalog, never by raw R2 deletion.",
            };
        }
        return Classification {
            name: "managed_iceberg_table",
            action: "keep",
            reason: "Iceberg owns table data and metadata lifecycle under warehouse/.",
        };
    }
    if key.starts_with("gold/staging/") {
        return Classification {
            name: "staging_gold_artifact",
            action: "delete_candidate",
            reason: "Staging Gold artifact should not be a long-lived runtime pointer.",
        };
    }
    if is_legacy_date_only_bronze_key(key) {
        return Classification {
            name: "legacy_uncontracted_bronze",
            action: "delete_candidate",
            reason: "Legacy date-only Bronze prefix is outside the source-partitioned contract.",
        };
    }
    if key.contains("smoke") {
        return Classification {
            name: "smoke_artifact",
            action: "delete_candidate",
            reason: "Smoke artifacts are disposable when no active run references them.",
        };
    }

    Classification {
        name: "unknown",
        action: "review",
        reason: "No Foundation Platform R2 ownership rule matched this object.",
    }
}

pub fn find_bronze_orphans(
    r2_keys: &[String],
    db_keys: &HashSet<String>,
    min_age_hours: u64,
    object_ages: &HashMap<String, u64>,
) -> Vec<String> {
    r2_keys
        .iter()
        .filter(|key| !db_keys.contains(*key))
        .filter(|key| {
            object_ages
                .get(*key)
                .is_some_and(|age_hours| *age_hours > min_age_hours)
        })
        .cloned()
        .collect()
}

fn write_usage_metrics(path: Option<&Path>, report: &AuditReport) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create metrics directory {}", parent.display()))?;
    }
    let source = prometheus_label_value(report.source);
    let mut lines = vec![
        "# HELP foundation_platform_r2_inventory_object_count Objects observed by the latest R2 inventory audit.".to_owned(),
        "# TYPE foundation_platform_r2_inventory_object_count gauge".to_owned(),
        format!("foundation_platform_r2_inventory_object_count{{source=\"{source}\"}} {}", report.object_count),
        "# HELP foundation_platform_r2_inventory_total_size_bytes Total object bytes observed by the latest R2 inventory audit.".to_owned(),
        "# TYPE foundation_platform_r2_inventory_total_size_bytes gauge".to_owned(),
        format!("foundation_platform_r2_inventory_total_size_bytes{{source=\"{source}\"}} {}", report.total_size_bytes),
        "# HELP foundation_platform_r2_inventory_delete_candidate_count R2 objects classified as delete candidates by the latest inventory audit.".to_owned(),
        "# TYPE foundation_platform_r2_inventory_delete_candidate_count gauge".to_owned(),
        format!("foundation_platform_r2_inventory_delete_candidate_count{{source=\"{source}\"}} {}", report.delete_candidate_count),
        "# HELP foundation_platform_r2_inventory_review_count R2 objects requiring owner review by the latest inventory audit.".to_owned(),
        "# TYPE foundation_platform_r2_inventory_review_count gauge".to_owned(),
        format!("foundation_platform_r2_inventory_review_count{{source=\"{source}\"}} {}", report.review_count),
        "# HELP foundation_platform_r2_inventory_list_request_count Estimated R2 list-objects request count used by the latest inventory audit.".to_owned(),
        "# TYPE foundation_platform_r2_inventory_list_request_count gauge".to_owned(),
        format!("foundation_platform_r2_inventory_list_request_count{{source=\"{source}\"}} {}", report.list_request_count),
    ];

    if let Some(estimated_cost) = report.estimated_list_request_cost_usd {
        lines.push("# HELP foundation_platform_r2_inventory_estimated_list_request_cost_usd Estimated R2 list-objects request cost for the latest inventory audit, using the caller-provided unit price.".to_owned());
        lines.push(
            "# TYPE foundation_platform_r2_inventory_estimated_list_request_cost_usd gauge"
                .to_owned(),
        );
        lines.push(format!(
            "foundation_platform_r2_inventory_estimated_list_request_cost_usd{{source=\"{source}\"}} {}",
            prometheus_number(estimated_cost)
        ));
    }

    fs::write(path, format!("{}\n", lines.join("\n")))
        .with_context(|| format!("failed to write usage metrics {}", path.display()))
}

fn write_summary(
    quiet: bool,
    report_path: &Path,
    delete_candidates_path: &Path,
    report: &AuditReport,
) -> anyhow::Result<()> {
    if quiet {
        return Ok(());
    }

    let mut stdout = io::stdout().lock();
    writeln!(stdout, "R2 inventory audit wrote:")?;
    writeln!(stdout, "  report: {}", report_path.display())?;
    writeln!(
        stdout,
        "  delete candidates: {}",
        delete_candidates_path.display()
    )?;
    writeln!(stdout, "Summary:")?;
    writeln!(stdout, "  objects: {}", report.object_count)?;
    writeln!(stdout, "  keep: {}", report.keep_count)?;
    writeln!(
        stdout,
        "  delete_candidate: {}",
        report.delete_candidate_count
    )?;
    writeln!(stdout, "  review: {}", report.review_count)?;
    Ok(())
}

fn optional_estimated_cost(raw: Option<&str>, request_count: usize) -> anyhow::Result<Option<f64>> {
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let unit_cost = raw
        .trim()
        .parse::<f64>()
        .context("EstimatedListRequestUnitCostUsd must be a number")?;
    if unit_cost < 0.0 {
        bail!("EstimatedListRequestUnitCostUsd must not be negative.");
    }
    let request_count = u32::try_from(request_count)
        .context("ListRequestCount exceeded the supported cost-estimation range")?;
    Ok(Some(unit_cost * f64::from(request_count)))
}

fn is_legacy_semantic_versioned_gold_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("gold/v") else {
        return false;
    };
    let Some((version, tail)) = rest.split_once('/') else {
        return false;
    };
    !version.is_empty() && !tail.is_empty() && version.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_current_spatial_artifact(key: &str) -> bool {
    [
        VECTOR_TILE_ARTIFACT_ROOT,
        VECTOR_TILE_MANIFEST_ROOT,
        PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT,
    ]
    .iter()
    .any(|root| has_path_root(key, root))
}

fn is_legacy_spatial_artifact(key: &str) -> bool {
    key.starts_with("gold/parcel-marker-anchor-pbf/")
        || key.starts_with("gold/parcel-marker-anchor-aggregate-pbf/")
        || key.starts_with("gold/parcel-marker-anchor-runtime/")
        || (key.starts_with("gold/parcel-marker-anchors/")
            && !has_path_root(key, PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT))
}

fn has_path_root(key: &str, root: &str) -> bool {
    key.strip_prefix(root)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

fn is_legacy_date_partitioned_bronze_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("bronze/source=") else {
        return false;
    };
    let Some((source, rest)) = rest.split_once("/ingest_date=") else {
        return false;
    };
    if source.is_empty() || source.contains('/') {
        return false;
    }
    let Some(date) = rest.get(..10) else {
        return false;
    };
    if !valid_date(date) {
        return false;
    }
    let Some(tail) = rest.get(10..).and_then(|value| value.strip_prefix('/')) else {
        return false;
    };
    tail.starts_with("run_id=") && tail.contains("/partition=")
}

fn is_legacy_date_only_bronze_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("bronze/") else {
        return false;
    };
    let Some(date) = rest.get(..7) else {
        return false;
    };
    date.len() == 7
        && date.as_bytes()[4] == b'-'
        && date
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 4 || byte.is_ascii_digit())
        && rest.get(7..).is_some_and(|tail| tail.starts_with('/'))
}

fn valid_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn prometheus_number(value: f64) -> String {
    let formatted = format!("{value:.28}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

fn size_sum(objects: &[AuditObject]) -> i64 {
    objects.iter().map(|object| object.size_bytes).sum()
}

fn parse_i32(raw: &str, label: &str) -> anyhow::Result<i32> {
    raw.parse::<i32>()
        .with_context(|| format!("{label} must be an integer"))
}

fn parse_u64(raw: &str, label: &str) -> anyhow::Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("{label} must be an integer"))
}

fn read_env_file_values(path: &Path) -> anyhow::Result<HashMap<String, String>> {
    if !path.is_file() {
        return Ok(HashMap::new());
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read env file {}", path.display()))?;
    let mut values = HashMap::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        values.insert(name.trim().to_owned(), unquote_env_value(value.trim()));
    }
    Ok(values)
}

fn unquote_env_value(value: &str) -> String {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}

fn optional_env_path(name: &str) -> anyhow::Result<Option<PathBuf>> {
    optional_env(name).map(|value| value.map(PathBuf::from))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn no_bronze_orphans() -> BronzeOrphanEvidence {
        BronzeOrphanEvidence {
            db_keys: HashSet::new(),
            min_age_hours: 24,
            object_ages: HashMap::new(),
        }
    }

    #[test]
    fn audit_report_classifies_runtime_legacy_and_unknown_keys() {
        let evidence = no_bronze_orphans();
        let report = build_audit_report(
            vec![
                RawObject {
                    key: "gold/manifest.json".to_owned(),
                    size_bytes: 10,
                    last_modified: None,
                    e_tag: None,
                },
                RawObject {
                    key: "bronze/source=molit/ingest_date=2026-05-24/run_id=run-1/partition=part-000.jsonl".to_owned(),
                    size_bytes: 20,
                    last_modified: Some("2026-05-24T00:00:00Z".to_owned()),
                    e_tag: Some("legacy-etag".to_owned()),
                },
                RawObject {
                    key: "bronze/2026-05/raw.jsonl".to_owned(),
                    size_bytes: 30,
                    last_modified: None,
                    e_tag: None,
                },
                RawObject {
                    key: "../escape.json".to_owned(),
                    size_bytes: 40,
                    last_modified: None,
                    e_tag: None,
                },
            ],
            "input_json",
            1,
            None,
            &evidence,
        );

        assert_eq!(report.object_count, 4);
        assert_eq!(report.total_size_bytes, 100);
        assert_eq!(report.keep_count, 1);
        assert_eq!(report.delete_candidate_count, 1);
        assert_eq!(report.review_count, 2);
        assert_eq!(report.objects[0].classification, "invalid_object_key");
        assert_eq!(
            report.objects[1].classification,
            "legacy_uncontracted_bronze"
        );
        assert_eq!(
            report.objects[2].classification,
            "legacy_date_partitioned_bronze"
        );
        assert_eq!(report.objects[3].classification, "runtime_manifest_pointer");
    }

    #[test]
    fn spatial_inventory_keeps_only_canonical_layouts_and_reviews_legacy_layouts() {
        let canonical = [
            "gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json",
            "gold/vector-tiles/manifests/018f0000-0000-7000-8000-000000000002.json",
            "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000003/manifest.json",
        ];
        for key in canonical {
            let classification = classify_key(key);
            assert_eq!(classification.name, "runtime_spatial_artifact");
            assert_eq!(classification.action, "keep");
        }

        let legacy = [
            "gold/v1/parcels/0/0/0.pbf",
            "gold/parcel-marker-anchor-pbf/legacy/manifest.json",
            "gold/parcel-marker-anchor-aggregate-pbf/legacy/manifest.json",
            "gold/parcel-marker-anchor-runtime/legacy/manifest.json",
            "gold/parcel-marker-anchors/legacy/manifest.json",
        ];
        for key in legacy {
            let classification = classify_key(key);
            assert_eq!(classification.name, "legacy_gold_artifact");
            assert_eq!(classification.action, "review");
        }
    }

    #[test]
    fn iceberg_table_files_are_never_raw_delete_candidates() {
        let current = classify_key("warehouse/silver/buildings/data/part-00001.parquet");
        assert_eq!(current.name, "managed_iceberg_table");
        assert_eq!(current.action, "keep");

        let smoke = classify_key("warehouse/silver/buildings_smoke/metadata/00001.metadata.json");
        assert_eq!(smoke.name, "iceberg_catalog_cleanup_required");
        assert_eq!(smoke.action, "review");

        let raw_smoke = classify_key("smoke/manual-proof.json");
        assert_eq!(raw_smoke.name, "smoke_artifact");
        assert_eq!(raw_smoke.action, "delete_candidate");
    }

    #[test]
    fn bronze_catalog_recovery_control_evidence_is_retained() {
        let classification = classify_key(
            "control/evidence/bronze-catalog-recovery/manifests/sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
        );

        assert_eq!(classification.name, "bronze_catalog_recovery_evidence");
        assert_eq!(classification.action, "keep");
    }

    #[test]
    fn audit_reviews_current_bronze_without_catalog_metadata() {
        let key = "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json";
        let report = build_audit_report(
            vec![RawObject {
                key: key.to_owned(),
                size_bytes: 42,
                last_modified: Some("2026-07-02T00:00:00Z".to_owned()),
                e_tag: Some("current-etag".to_owned()),
            }],
            "input_json",
            1,
            None,
            &no_bronze_orphans(),
        );

        assert_eq!(
            report.objects[0].classification,
            "bronze_catalog_metadata_missing"
        );
        assert_eq!(report.objects[0].action, "review");
        assert_eq!(report.objects[0].e_tag.as_deref(), Some("current-etag"));
        assert_eq!(report.keep_count, 0);
        assert_eq!(report.review_count, 1);
    }

    #[test]
    fn audit_reviews_metadata_and_redundant_operation_in_legacy_bronze_paths() {
        let metadata = classify_key(
            "bronze/source=hubgokr__building_register_main/provider_file_period=2026-04/OPN209912310000000002.zip",
        );
        assert_eq!(metadata.name, "legacy_bronze_path_contract");
        assert_eq!(metadata.action, "review");

        let repeated_operation = classify_key(
            "bronze/source=datagokr__building_register_main/operation=getBrTitleInfo/sigungu=11680/bjdong=10300/page-000001.json",
        );
        assert_eq!(repeated_operation.name, "legacy_bronze_path_contract");
        assert_eq!(repeated_operation.action, "review");
    }

    #[test]
    fn inventory_json_reader_accepts_s3_contents_shape() -> anyhow::Result<()> {
        let dir = PathBuf::from("target/r2-inventory-audit-tests");
        fs::create_dir_all(&dir)?;
        let path = dir.join("contents.json");
        fs::write(
            &path,
            r#"{
                "Contents": [
                    {
                        "Key": "bronze/source=molit/run_id=run-1/partition=part-000.jsonl",
                        "Size": 42,
                        "LastModified": "2026-06-07T00:00:00Z",
                        "ETag": "inventory-etag"
                    }
                ]
            }"#,
        )?;

        let objects = read_inventory_from_json(&path)?;

        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].key,
            "bronze/source=molit/run_id=run-1/partition=part-000.jsonl"
        );
        assert_eq!(objects[0].size_bytes, 42);
        assert_eq!(
            objects[0].last_modified.as_deref(),
            Some("2026-06-07T00:00:00Z")
        );
        assert_eq!(objects[0].e_tag.as_deref(), Some("inventory-etag"));
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn bronze_orphan_detection_uses_db_keys_and_safety_window() {
        let current_key =
            "bronze/source=data-go-kr/run_id=run-1/partition=part-current.json".to_owned();
        let old_orphan_key =
            "bronze/source=data-go-kr/run_id=run-1/partition=part-old.json".to_owned();
        let young_orphan_key =
            "bronze/source=data-go-kr/run_id=run-1/partition=part-young.json".to_owned();
        let r2_keys = vec![
            current_key.clone(),
            old_orphan_key.clone(),
            young_orphan_key.clone(),
        ];
        let db_keys = HashSet::from([current_key.clone()]);
        let object_ages = HashMap::from([
            (current_key, 48),
            (old_orphan_key.clone(), 25),
            (young_orphan_key, 2),
        ]);

        let orphans = find_bronze_orphans(&r2_keys, &db_keys, 24, &object_ages);

        assert_eq!(orphans, vec![old_orphan_key]);
    }

    #[test]
    fn audit_report_marks_old_db_unreferenced_bronze_key_as_orphan_candidate() {
        let current_key = "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json".to_owned();
        let orphan_key = "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000002.json".to_owned();
        let evidence = BronzeOrphanEvidence {
            db_keys: HashSet::from([current_key.clone()]),
            min_age_hours: 24,
            object_ages: HashMap::from([(orphan_key.clone(), 25)]),
        };

        let report = build_audit_report(
            vec![
                RawObject {
                    key: current_key,
                    size_bytes: 10,
                    last_modified: None,
                    e_tag: None,
                },
                RawObject {
                    key: orphan_key.clone(),
                    size_bytes: 20,
                    last_modified: None,
                    e_tag: None,
                },
            ],
            "input_json",
            1,
            None,
            &evidence,
        );

        let orphan = report
            .objects
            .iter()
            .find(|object| object.key == orphan_key)
            .expect("orphan object should be present");

        assert_eq!(orphan.classification, "bronze_orphan_candidate");
        assert_eq!(orphan.action, "delete_candidate");
        assert_eq!(report.delete_candidate_count, 1);
    }

    #[test]
    fn bronze_orphan_database_url_prefers_process_audit_url_then_database_url_then_env_file() {
        let url = bronze_orphan_database_url_from_values(
            Some("postgres://audit".to_owned()),
            Some("postgres://database".to_owned()),
            Some("postgres://env-file-audit".to_owned()),
            Some("postgres://env-file-database".to_owned()),
        )
        .expect("audit url should win");
        assert_eq!(url, "postgres://audit");

        let url = bronze_orphan_database_url_from_values(
            None,
            Some("postgres://database".to_owned()),
            Some("postgres://env-file-audit".to_owned()),
            Some("postgres://env-file-database".to_owned()),
        )
        .expect("DATABASE_URL should win after audit url");
        assert_eq!(url, "postgres://database");

        let url = bronze_orphan_database_url_from_values(
            None,
            None,
            Some("postgres://env-file-audit".to_owned()),
            Some("postgres://env-file-database".to_owned()),
        )
        .expect("env-file audit url should win after process env");
        assert_eq!(url, "postgres://env-file-audit");
    }

    #[test]
    fn bronze_orphan_database_url_missing_is_fail_closed() {
        let error = bronze_orphan_database_url_from_values(None, None, None, None)
            .expect_err("missing DB evidence must fail closed");

        assert!(error.to_string().contains("requires DATABASE_URL"));
    }
}
