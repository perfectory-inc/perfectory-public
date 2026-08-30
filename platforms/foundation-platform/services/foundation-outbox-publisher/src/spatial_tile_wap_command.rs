use std::{
    collections::BTreeMap,
    fs,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::public_data_control_support::{
    optional_env_value, repo_relative_path, resolve_repo_path,
};

const ACTION_ENV: &str = "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_ACTION";
const RELEASE_ID_ENV: &str = "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_RELEASE_ID";
const EXPECTED_BASE_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_EXPECTED_BASE_SNAPSHOT";
const PLAN_PATH_ENV: &str = "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_PLAN_PATH";
const EVIDENCE_PATH_ENV: &str = "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_EVIDENCE_PATH";
const VALIDATED_EVIDENCE_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_VALIDATED_EVIDENCE_PATH";
const REPO_ROOT_ENV: &str = "FOUNDATION_PLATFORM_REPO_ROOT";
const CONFIG_ENV_NAMES: [&str; 7] = [
    REPO_ROOT_ENV,
    ACTION_ENV,
    RELEASE_ID_ENV,
    EXPECTED_BASE_SNAPSHOT_ENV,
    PLAN_PATH_ENV,
    EVIDENCE_PATH_ENV,
    VALIDATED_EVIDENCE_PATH_ENV,
];

const HOST_PLAN_SCHEMA_VERSION: &str =
    "foundation-platform.spatial_tile_wap_host_execution_plan.v1";
const ARTIFACT_ROOT: &str = "target/spatial-tile-publication";
const CONTAINER_ARTIFACT_ROOT: &str = "/workspace/target/spatial-tile-publication";
const SPARK_JOB_PATH: &str =
    "/workspace/infra/lakehouse/spark/jobs/spatial_tile_publication_wap.py";
const FOUNDATION_SPARK_JOB_PATH: &str =
    "infra/lakehouse/spark/jobs/spatial_tile_publication_wap.py";

const CATALOG_ENV_NAMES: [&str; 3] = [
    "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
    "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
    "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
];
const OFFLINE_CAPABILITY: &str = "not_proven_offline";
const OFFLINE_CAPABILITY_LINE: &str = "provider_capability=not_proven_offline";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpatialTileWapAction {
    Plan,
    Validate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SpatialTileWapConfig {
    action: SpatialTileWapAction,
    root: PathBuf,
    artifact_root: PathBuf,
    release_id: Uuid,
    expected_base_snapshot: Option<u64>,
    plan_path: PathBuf,
    evidence_path: PathBuf,
    validated_evidence_path: PathBuf,
}

impl SpatialTileWapConfig {
    fn from_env() -> anyhow::Result<Self> {
        let mut values = BTreeMap::new();
        for name in CONFIG_ENV_NAMES {
            if let Some(value) = optional_env_value(name)? {
                values.insert(name, value);
            }
        }
        Self::from_lookup(|name| values.get(name).cloned())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let action = parse_action(optional_lookup(&mut lookup, ACTION_ENV).as_deref())?;
        let release_id = parse_release_id(&required_lookup(&mut lookup, RELEASE_ID_ENV)?)?;
        let root = PathBuf::from(
            optional_lookup(&mut lookup, REPO_ROOT_ENV).unwrap_or_else(|| ".".to_owned()),
        );
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve Foundation root {}", root.display()))?;
        validate_foundation_root(&root)?;

        let artifact_root = resolve_artifact_root(&root)?;
        let release_hex = release_id.simple();
        let default_plan = format!("{ARTIFACT_ROOT}/spatial-tile-wap-{release_hex}-host-plan.json");
        let default_evidence = format!("{ARTIFACT_ROOT}/spatial-tile-wap-{release_hex}-probe.json");
        let default_validated =
            format!("{ARTIFACT_ROOT}/spatial-tile-wap-{release_hex}-validated.json");
        let plan_path = resolve_artifact_path(
            &root,
            &artifact_root,
            optional_lookup(&mut lookup, PLAN_PATH_ENV)
                .unwrap_or(default_plan)
                .as_str(),
            "plan path",
        )?;
        let evidence_path = resolve_artifact_path(
            &root,
            &artifact_root,
            optional_lookup(&mut lookup, EVIDENCE_PATH_ENV)
                .unwrap_or(default_evidence)
                .as_str(),
            "evidence path",
        )?;
        let validated_evidence_path = resolve_artifact_path(
            &root,
            &artifact_root,
            optional_lookup(&mut lookup, VALIDATED_EVIDENCE_PATH_ENV)
                .unwrap_or(default_validated)
                .as_str(),
            "validated evidence path",
        )?;
        if plan_path == evidence_path
            || plan_path == validated_evidence_path
            || evidence_path == validated_evidence_path
        {
            bail!("plan, evidence, and validated evidence paths must be distinct");
        }
        let expected_base_snapshot = match action {
            SpatialTileWapAction::Plan => None,
            SpatialTileWapAction::Validate => Some(parse_positive_snapshot(&required_lookup(
                &mut lookup,
                EXPECTED_BASE_SNAPSHOT_ENV,
            )?)?),
        };

        Ok(Self {
            action,
            root,
            artifact_root,
            release_id,
            expected_base_snapshot,
            plan_path,
            evidence_path,
            validated_evidence_path,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct HostExecutionPlan {
    schema_version: String,
    working_directory: String,
    program: String,
    args: Vec<String>,
    forwarded_environment: Vec<String>,
    logical_contract: String,
    physical_table: String,
    catalog_bucket: String,
    release_id: String,
    historical_branch_name: String,
    branch_name: String,
    expected_evidence_path: String,
    provider_capability: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SparkWapEvidence {
    schema_version: String,
    logical_contract: String,
    physical_table: String,
    catalog_bucket: String,
    historical_base_snapshot: u64,
    base_snapshot: u64,
    historical_branch_snapshot: u64,
    branch_snapshot: u64,
    historical_branch_name: String,
    branch_name: String,
    result: String,
    provider: String,
    historical_base_isolation: String,
    branch_isolation: String,
    retention: String,
    fast_forward: String,
}

pub fn run() -> anyhow::Result<()> {
    let config = SpatialTileWapConfig::from_env()?;
    execute_action(&config)?;
    println!("{OFFLINE_CAPABILITY_LINE}");
    Ok(())
}

fn execute_action(config: &SpatialTileWapConfig) -> anyhow::Result<()> {
    match config.action {
        SpatialTileWapAction::Plan => {
            let plan = build_host_execution_plan(config)?;
            write_json_create_new(&config.plan_path, &plan, "host execution plan")
        }
        SpatialTileWapAction::Validate => {
            let raw = fs::read(&config.evidence_path).with_context(|| {
                format!(
                    "failed to read Spark WAP evidence {}",
                    config.evidence_path.display()
                )
            })?;
            let expected_base_snapshot = config
                .expected_base_snapshot
                .context("validate action requires an expected base snapshot")?;
            let evidence =
                validate_spark_evidence(&raw, config.release_id, expected_base_snapshot)?;
            write_json_create_new(
                &config.validated_evidence_path,
                &evidence,
                "validated Spark WAP evidence",
            )
        }
    }
}

#[path = "spatial_tile_wap_evidence_contract.rs"]
mod spatial_tile_wap_evidence_contract;

use spatial_tile_wap_evidence_contract::{
    branch_prefix, evidence_contract, property_const, validate_evidence_payload_against_contract,
};
#[cfg(test)]
use spatial_tile_wap_evidence_contract::{string_list, validate_evidence_contract};

fn build_host_execution_plan(config: &SpatialTileWapConfig) -> anyhow::Result<HostExecutionPlan> {
    let contract = evidence_contract()?;
    let logical_contract = property_const(&contract, "logical_contract")?;
    let physical_table = property_const(&contract, "physical_table")?;
    let catalog_bucket = property_const(&contract, "catalog_bucket")?;
    let target = physical_table.split('.').collect::<Vec<_>>();
    if target.len() != 3 || target.iter().any(|value| value.is_empty()) {
        bail!("physical_table const must contain catalog.namespace.table");
    }
    let evidence_relative = config
        .evidence_path
        .strip_prefix(&config.artifact_root)
        .context("evidence path must stay below the spatial tile publication artifact root")?;
    let evidence_relative = evidence_relative.to_string_lossy().replace('\\', "/");
    let container_evidence = format!("{CONTAINER_ARTIFACT_ROOT}/{evidence_relative}");
    let mount = format!(
        "{}:{CONTAINER_ARTIFACT_ROOT}",
        host_path(&config.artifact_root)
    );
    let release_id = config.release_id.to_string();
    let mut args = vec![
        "compose".to_owned(),
        "-f".to_owned(),
        "compose.lakehouse.yml".to_owned(),
        "--profile".to_owned(),
        "lakehouse-batch".to_owned(),
        "run".to_owned(),
        "--rm".to_owned(),
        "--volume".to_owned(),
        mount,
    ];
    for name in CATALOG_ENV_NAMES {
        args.extend(["-e".to_owned(), name.to_owned()]);
    }
    args.extend([
        "spark".to_owned(),
        "spark-submit".to_owned(),
        "--conf".to_owned(),
        "spark.jars.ivy=/tmp/.ivy2".to_owned(),
        "--packages".to_owned(),
        crate::lakehouse_engine_contract::iceberg_packages()?.to_owned(),
        SPARK_JOB_PATH.to_owned(),
        "probe".to_owned(),
        "--catalog".to_owned(),
        target[0].to_owned(),
        "--namespace".to_owned(),
        target[1].to_owned(),
        "--table".to_owned(),
        target[2].to_owned(),
        "--release-id".to_owned(),
        release_id.clone(),
        "--evidence-output".to_owned(),
        container_evidence,
    ]);

    Ok(HostExecutionPlan {
        schema_version: HOST_PLAN_SCHEMA_VERSION.to_owned(),
        working_directory: host_path(&config.root),
        program: "docker".to_owned(),
        args,
        forwarded_environment: CATALOG_ENV_NAMES.map(str::to_owned).to_vec(),
        logical_contract,
        physical_table,
        catalog_bucket,
        release_id,
        historical_branch_name: historical_branch_name(config.release_id)?,
        branch_name: branch_name(config.release_id)?,
        expected_evidence_path: repo_relative_path(&config.root, &config.evidence_path),
        provider_capability: OFFLINE_CAPABILITY.to_owned(),
    })
}

fn validate_spark_evidence(
    raw: &[u8],
    release_id: Uuid,
    expected_base_snapshot: u64,
) -> anyhow::Result<SparkWapEvidence> {
    let payload: JsonValue =
        serde_json::from_slice(raw).context("failed to parse strict Spark WAP evidence")?;
    let contract = evidence_contract()?;
    validate_evidence_payload_against_contract(&payload, &contract)?;
    let evidence: SparkWapEvidence =
        serde_json::from_value(payload).context("failed to type strict Spark WAP evidence")?;
    if evidence.base_snapshot != expected_base_snapshot {
        bail!("base_snapshot does not match expected base snapshot");
    }
    if evidence.historical_branch_name != historical_branch_name(release_id)? {
        bail!("historical_branch_name mismatch");
    }
    if evidence.branch_name != branch_name(release_id)? {
        bail!("branch_name mismatch");
    }
    if evidence.result != "probe_ok" {
        bail!("result must be probe_ok");
    }
    Ok(evidence)
}

fn parse_action(value: Option<&str>) -> anyhow::Result<SpatialTileWapAction> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("plan") => Ok(SpatialTileWapAction::Plan),
        Some("validate") => Ok(SpatialTileWapAction::Validate),
        Some(other) => bail!("{ACTION_ENV} must be plan or validate, got {other}"),
    }
}

fn parse_release_id(value: &str) -> anyhow::Result<Uuid> {
    let release_id = Uuid::parse_str(value.trim())
        .with_context(|| format!("release id in {RELEASE_ID_ENV} must be a UUID"))?;
    if release_id.is_nil() {
        bail!("{RELEASE_ID_ENV} release UUID must not be nil");
    }
    Ok(release_id)
}

fn parse_positive_snapshot(value: &str) -> anyhow::Result<u64> {
    let snapshot = value
        .trim()
        .parse::<u64>()
        .with_context(|| format!("{EXPECTED_BASE_SNAPSHOT_ENV} must be a positive integer"))?;
    if snapshot == 0 {
        bail!("{EXPECTED_BASE_SNAPSHOT_ENV} must be a positive integer");
    }
    Ok(snapshot)
}

fn branch_name(release_id: Uuid) -> anyhow::Result<String> {
    let contract = evidence_contract()?;
    Ok(format!(
        "{}{}",
        branch_prefix(&contract, "publication")?,
        release_id.simple()
    ))
}

fn historical_branch_name(release_id: Uuid) -> anyhow::Result<String> {
    let contract = evidence_contract()?;
    Ok(format!(
        "{}{}",
        branch_prefix(&contract, "historical")?,
        release_id.simple()
    ))
}

fn host_path(path: &Path) -> String {
    normalize_host_path(&path.to_string_lossy())
}

fn normalize_host_path(value: &str) -> String {
    if let Some(path) = value.strip_prefix(r"\\?\UNC\") {
        return format!(r"\\{path}");
    }
    value.strip_prefix(r"\\?\").unwrap_or(value).to_owned()
}

fn validate_foundation_root(root: &Path) -> anyhow::Result<()> {
    let compose = root.join("compose.lakehouse.yml");
    let job = root.join(FOUNDATION_SPARK_JOB_PATH);
    if !compose.is_file() || !job.is_file() {
        bail!(
            "Foundation root must contain compose.lakehouse.yml and {FOUNDATION_SPARK_JOB_PATH}: {}",
            root.display()
        );
    }
    Ok(())
}

fn resolve_artifact_path(
    root: &Path,
    artifact_root: &Path,
    value: &str,
    label: &str,
) -> anyhow::Result<PathBuf> {
    let resolved = resolve_repo_path(root, Path::new(value), label)
        .with_context(|| format!("{label} must stay below {ARTIFACT_ROOT}"))?;
    let relative = resolved
        .strip_prefix(artifact_root)
        .map_err(|_| anyhow::anyhow!("{label} must stay below {ARTIFACT_ROOT}"))?;
    if relative.as_os_str().is_empty() {
        bail!("{label} must name a file below {ARTIFACT_ROOT}");
    }
    if resolved
        .extension()
        .and_then(|extension| extension.to_str())
        != Some("json")
    {
        bail!("{label} must be a JSON file below {ARTIFACT_ROOT}");
    }
    reject_existing_symlink_components(artifact_root, &resolved, label)?;
    Ok(resolved)
}

fn resolve_artifact_root(root: &Path) -> anyhow::Result<PathBuf> {
    let artifact_root = root.join(ARTIFACT_ROOT);
    if fs::symlink_metadata(&artifact_root).is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        bail!("{ARTIFACT_ROOT} must not be a symlink");
    }
    fs::create_dir_all(&artifact_root).with_context(|| {
        format!(
            "failed to create spatial tile publication artifact root {}",
            artifact_root.display()
        )
    })?;
    let canonical = fs::canonicalize(&artifact_root).with_context(|| {
        format!(
            "failed to resolve spatial tile publication artifact root {}",
            artifact_root.display()
        )
    })?;
    if canonical != artifact_root || canonical.strip_prefix(root).is_err() {
        bail!("{ARTIFACT_ROOT} must not escape the Foundation root through a symlink");
    }
    Ok(canonical)
}

fn reject_existing_symlink_components(
    artifact_root: &Path,
    candidate: &Path,
    label: &str,
) -> anyhow::Result<()> {
    let relative = candidate
        .strip_prefix(artifact_root)
        .with_context(|| format!("{label} must stay below {ARTIFACT_ROOT}"))?;
    let mut current = artifact_root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(component) = component else {
            bail!("{label} contains an invalid path component");
        };
        current.push(component);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to inspect {label} component {}", current.display())
                });
            }
        };
        if metadata.file_type().is_symlink() {
            bail!("{label} must not traverse a symlink: {}", current.display());
        }
        let canonical = fs::canonicalize(&current).with_context(|| {
            format!("failed to resolve {label} component {}", current.display())
        })?;
        if canonical.strip_prefix(artifact_root).is_err() {
            bail!(
                "{label} must not escape {ARTIFACT_ROOT} through a symlink: {}",
                current.display()
            );
        }
    }
    Ok(())
}

fn optional_lookup(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name).and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn required_lookup(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<String> {
    optional_lookup(lookup, name).with_context(|| format!("{name} is required"))
}

fn write_json_create_new<T: Serialize>(path: &Path, value: &T, label: &str) -> anyhow::Result<()> {
    write_json_create_new_with_installer(path, value, label, |source, destination| {
        fs::hard_link(source, destination)
    })
}

fn write_json_create_new_with_installer<T, F>(
    path: &Path,
    value: &T,
    label: &str,
    installer: F,
) -> anyhow::Result<()>
where
    T: Serialize,
    F: FnOnce(&Path, &Path) -> std::io::Result<()>,
{
    let mut bytes =
        serde_json::to_vec_pretty(value).with_context(|| format!("failed to serialize {label}"))?;
    bytes.push(b'\n');
    let parent = path
        .parent()
        .with_context(|| format!("{label} path must have a parent"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {label} directory {}", parent.display()))?;
    let name = path
        .file_name()
        .context("atomic JSON destination must have a file name")?
        .to_string_lossy();
    let temporary = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4().simple()));
    let outcome = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| format!("failed to create temporary {label}"))?;
        file.write_all(&bytes)
            .with_context(|| format!("failed to write temporary {label}"))?;
        file.sync_all()
            .with_context(|| format!("failed to sync temporary {label}"))?;
        installer(&temporary, path)
            .map_err(|error| anyhow::anyhow!("failed to create {label} atomically: {error}"))
    })();
    let cleanup = if temporary.exists() {
        fs::remove_file(&temporary)
    } else {
        Ok(())
    };
    if let Err(error) = cleanup {
        return Err(anyhow::anyhow!(
            "failed to remove temporary {label} {}: {error}",
            temporary.display()
        ));
    }
    outcome
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        env, fs,
        path::{Path, PathBuf},
    };

    use serde_json::{json, Value as JsonValue};
    use uuid::Uuid;

    use super::*;

    const RELEASE_ID: &str = "018f1111-1111-7111-8111-111111111111";
    const RELEASE_HEX: &str = "018f1111111171118111111111111111";
    const SENTINEL_URI: &str = "https://sentinel-account.example.test/catalog";
    const SENTINEL_WAREHOUSE: &str = "sentinel-warehouse-secret";
    const SENTINEL_TOKEN: &str = "sentinel-catalog-token-secret";

    fn test_root() -> anyhow::Result<PathBuf> {
        let root = env::current_dir()?
            .join("target")
            .join("spatial-tile-wap-command-tests")
            .join(Uuid::new_v4().simple().to_string());
        let jobs = root
            .join("infra")
            .join("lakehouse")
            .join("spark")
            .join("jobs");
        fs::create_dir_all(&jobs)?;
        fs::write(
            root.join("compose.lakehouse.yml"),
            "# test compose marker\n",
        )?;
        fs::write(
            jobs.join("spatial_tile_publication_wap.py"),
            "# test Spark job marker\n",
        )?;
        Ok(fs::canonicalize(root)?)
    }

    fn base_env(root: &Path, action: &str) -> HashMap<String, String> {
        HashMap::from([
            (
                "FOUNDATION_PLATFORM_REPO_ROOT".to_owned(),
                root.display().to_string(),
            ),
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_ACTION".to_owned(),
                action.to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_RELEASE_ID".to_owned(),
                RELEASE_ID.to_owned(),
            ),
        ])
    }

    fn config_from(values: &HashMap<String, String>) -> anyhow::Result<SpatialTileWapConfig> {
        SpatialTileWapConfig::from_lookup(|name| values.get(name).cloned())
    }

    fn valid_evidence() -> JsonValue {
        json!({
            "schema_version": "foundation-platform.spatial_tile_wap_evidence.v1",
            "logical_contract": "silver.parcel_boundaries",
            "physical_table": "r2.tiles_slice_proof.parcel_boundaries_wap_probe",
            "catalog_bucket": "perfectory-tiles-slice-proof",
            "historical_base_snapshot": 40,
            "base_snapshot": 41,
            "historical_branch_snapshot": 40,
            "branch_snapshot": 42,
            "historical_branch_name": format!("history_{RELEASE_HEX}"),
            "branch_name": format!("tile_{RELEASE_HEX}"),
            "result": "probe_ok",
            "provider": "cloudflare-r2-data-catalog",
            "historical_base_isolation": "ok",
            "branch_isolation": "ok",
            "retention": "ok",
            "fast_forward": "ok"
        })
    }

    #[test]
    fn spatial_tile_wap_contract_fails_closed_on_unknown_custom_rule() -> anyhow::Result<()> {
        let mut contract = evidence_contract()?;
        contract["x-perfectory-cross-field-invariants"]
            .as_array_mut()
            .expect("contract invariants must be an array")
            .push(json!({"op": "invented", "fields": ["base_snapshot"]}));
        let error = validate_evidence_contract(&contract)
            .expect_err("unknown custom contract operations must fail closed");
        assert!(error.to_string().contains("invented"));
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_contract_rejects_malformed_required_fields() -> anyhow::Result<()> {
        let mut contract = evidence_contract()?;
        contract["required"]
            .as_array_mut()
            .expect("contract required must be an array")
            .retain(|field| field != "schema_version");
        let error = validate_evidence_contract(&contract)
            .expect_err("required and properties must remain identical");
        assert!(error.to_string().contains("required"));
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_contract_rejects_malformed_rule_shapes() -> anyhow::Result<()> {
        let mutations: [fn(&mut JsonValue); 5] = [
            |contract| contract["title"] = json!(7),
            |contract| contract["allOf"][0]["if"]["description"] = json!("ignored"),
            |contract| contract["properties"]["historical_base_snapshot"]["const"] = json!(true),
            |contract| {
                contract["x-perfectory-branch-pair"]["historical"]["field"] =
                    json!("base_snapshot");
            },
            |contract| {
                contract["x-perfectory-cross-field-invariants"][0]["fields"] =
                    json!(["branch_name", "historical_base_snapshot"]);
            },
        ];
        for mutate in mutations {
            let mut contract = evidence_contract()?;
            mutate(&mut contract);
            validate_evidence_contract(&contract)
                .expect_err("malformed contract shapes must fail closed");
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_action_is_typed_and_defaults_to_plan() -> anyhow::Result<()> {
        assert_eq!(parse_action(None)?, SpatialTileWapAction::Plan);
        assert_eq!(parse_action(Some(""))?, SpatialTileWapAction::Plan);
        assert_eq!(parse_action(Some("plan"))?, SpatialTileWapAction::Plan);
        assert_eq!(
            parse_action(Some("validate"))?,
            SpatialTileWapAction::Validate
        );
        let error = parse_action(Some("execute")).expect_err("execute must not be an action");
        assert!(error.to_string().contains("plan or validate"));
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_release_uuid_is_non_nil_and_branch_is_derived() -> anyhow::Result<()> {
        let release_id = parse_release_id(RELEASE_ID)?;
        assert_eq!(release_id.to_string(), RELEASE_ID);
        assert_eq!(branch_name(release_id)?, format!("tile_{RELEASE_HEX}"));
        assert_eq!(
            historical_branch_name(release_id)?,
            format!("history_{RELEASE_HEX}")
        );

        for invalid in ["not-a-uuid", "00000000-0000-0000-0000-000000000000"] {
            let error = parse_release_id(invalid).expect_err("invalid UUID must fail");
            assert!(error.to_string().contains("release"));
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_expected_base_snapshot_is_strictly_positive() -> anyhow::Result<()> {
        assert_eq!(parse_positive_snapshot("41")?, 41);
        for invalid in ["0", "-1", "not-a-snapshot"] {
            let error =
                parse_positive_snapshot(invalid).expect_err("invalid snapshot must fail closed");
            assert!(error.to_string().contains("positive"));
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_plan_fixes_contract_target_branch_and_paths() -> anyhow::Result<()> {
        let root = test_root()?;
        let config = config_from(&base_env(&root, "plan"))?;
        let plan = build_host_execution_plan(&config)?;

        assert_eq!(plan.schema_version, HOST_PLAN_SCHEMA_VERSION);
        assert_eq!(plan.logical_contract, "silver.parcel_boundaries");
        assert_eq!(
            plan.physical_table,
            "r2.tiles_slice_proof.parcel_boundaries_wap_probe"
        );
        assert_eq!(plan.catalog_bucket, "perfectory-tiles-slice-proof");
        assert_eq!(plan.release_id, RELEASE_ID);
        assert_eq!(
            plan.historical_branch_name,
            format!("history_{RELEASE_HEX}")
        );
        assert_eq!(plan.branch_name, format!("tile_{RELEASE_HEX}"));
        assert_eq!(plan.program, "docker");
        assert_eq!(plan.working_directory, host_path(&config.root));
        assert_eq!(
            plan.expected_evidence_path,
            format!("target/spatial-tile-publication/spatial-tile-wap-{RELEASE_HEX}-probe.json")
        );
        assert_eq!(plan.provider_capability, OFFLINE_CAPABILITY);
        let serialized = serde_json::to_value(&plan)?;
        assert_eq!(
            serialized["catalog_bucket"],
            json!("perfectory-tiles-slice-proof")
        );
        assert_eq!(
            property_const(&evidence_contract()?, "catalog_bucket")?,
            plan.catalog_bucket
        );
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_evidence_fields_are_derived_from_the_schema() -> anyhow::Result<()> {
        let contract = evidence_contract()?;
        let rust_evidence: SparkWapEvidence = serde_json::from_value(valid_evidence())?;
        let serialized = serde_json::to_value(rust_evidence)?;
        let rust_fields = serialized
            .as_object()
            .expect("SparkWapEvidence must serialize as an object")
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let contract_fields = string_list(&contract["required"], "required")?
            .into_iter()
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            rust_fields, contract_fields,
            "Rust SparkWapEvidence and the JSON contract drifted"
        );
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_plan_pins_exact_host_command_and_narrow_mount() -> anyhow::Result<()> {
        let root = test_root()?;
        let config = config_from(&base_env(&root, "plan"))?;
        let plan = build_host_execution_plan(&config)?;
        let mount = format!(
            "{}:{CONTAINER_ARTIFACT_ROOT}",
            host_path(&config.artifact_root)
        );
        let container_evidence =
            format!("{CONTAINER_ARTIFACT_ROOT}/spatial-tile-wap-{RELEASE_HEX}-probe.json");

        assert_eq!(
            plan.forwarded_environment,
            CATALOG_ENV_NAMES.map(str::to_owned)
        );
        assert_eq!(
            plan.args,
            vec![
                "compose",
                "-f",
                "compose.lakehouse.yml",
                "--profile",
                "lakehouse-batch",
                "run",
                "--rm",
                "--volume",
                mount.as_str(),
                "-e",
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
                "-e",
                "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
                "-e",
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN",
                "spark",
                "spark-submit",
                "--conf",
                "spark.jars.ivy=/tmp/.ivy2",
                "--packages",
                crate::lakehouse_engine_contract::iceberg_packages()?,
                "/workspace/infra/lakehouse/spark/jobs/spatial_tile_publication_wap.py",
                "probe",
                "--catalog",
                "r2",
                "--namespace",
                "tiles_slice_proof",
                "--table",
                "parcel_boundaries_wap_probe",
                "--release-id",
                RELEASE_ID,
                "--evidence-output",
                container_evidence.as_str(),
            ]
        );
        assert_eq!(
            plan.args
                .iter()
                .filter(|argument| argument.as_str() == "--volume")
                .count(),
            1
        );
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_windows_verbatim_paths_are_docker_compatible() {
        assert_eq!(
            normalize_host_path(r"\\?\C:\repo\target\spatial-tile-publication"),
            r"C:\repo\target\spatial-tile-publication"
        );
        assert_eq!(
            normalize_host_path(r"\\?\UNC\server\share\foundation"),
            r"\\server\share\foundation"
        );
        assert_eq!(
            normalize_host_path("/workspace/platforms/foundation-platform"),
            "/workspace/platforms/foundation-platform"
        );
    }

    #[test]
    fn spatial_tile_wap_plan_never_reads_or_serializes_secret_values() -> anyhow::Result<()> {
        let root = test_root()?;
        let mut values = base_env(&root, "plan");
        values.extend([
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI".to_owned(),
                SENTINEL_URI.to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE".to_owned(),
                SENTINEL_WAREHOUSE.to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN".to_owned(),
                SENTINEL_TOKEN.to_owned(),
            ),
        ]);
        let config = config_from(&values)?;
        let plan = build_host_execution_plan(&config)?;
        let raw = serde_json::to_string(&plan)?;

        for sentinel in [SENTINEL_URI, SENTINEL_WAREHOUSE, SENTINEL_TOKEN] {
            assert!(!raw.contains(sentinel));
        }
        for name in CATALOG_ENV_NAMES {
            assert!(raw.contains(name));
        }

        let source = include_str!("spatial_tile_wap_command.rs");
        let forbidden_process_module = ["pro", "cess::"].concat();
        let forbidden_command_constructor = ["Command", "::new"].concat();
        assert!(!source.contains(&forbidden_process_module));
        assert!(!source.contains(&forbidden_command_constructor));
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validation_accepts_only_complete_probe_evidence() -> anyhow::Result<()> {
        let evidence = validate_spark_evidence(
            &serde_json::to_vec(&valid_evidence())?,
            parse_release_id(RELEASE_ID)?,
            41,
        )?;
        assert_eq!(evidence.logical_contract, "silver.parcel_boundaries");
        assert_eq!(
            evidence.catalog_bucket,
            property_const(&evidence_contract()?, "catalog_bucket")?
        );
        assert_eq!(evidence.historical_base_snapshot, 40);
        assert_eq!(evidence.base_snapshot, 41);
        assert_eq!(evidence.historical_branch_snapshot, 40);
        assert_eq!(evidence.branch_snapshot, 42);
        assert_eq!(
            evidence.historical_branch_name,
            format!("history_{RELEASE_HEX}")
        );
        assert_eq!(evidence.result, "probe_ok");
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validation_rejects_every_identity_or_status_mismatch() -> anyhow::Result<()>
    {
        let cases = [
            (
                "schema_version",
                json!("foundation-platform.spatial_tile_wap_evidence.v2"),
            ),
            ("logical_contract", json!("silver.other")),
            ("physical_table", json!("r2.silver.parcel_boundaries")),
            (
                "catalog_bucket",
                json!("foundation-platform-lakehouse-prod"),
            ),
            ("historical_base_snapshot", json!(41)),
            ("base_snapshot", json!(40)),
            ("historical_branch_snapshot", json!(41)),
            ("historical_branch_name", json!("history_wrong")),
            ("branch_name", json!("tile_wrong")),
            ("result", json!("validated")),
            ("provider", json!("local")),
            ("historical_base_isolation", json!("not_proven")),
            ("branch_isolation", json!("not_proven")),
            ("retention", json!("not_proven")),
            ("fast_forward", json!("not_requested")),
        ];
        for (field, value) in cases {
            let mut payload = valid_evidence();
            payload[field] = value;
            let error = validate_spark_evidence(
                &serde_json::to_vec(&payload)?,
                parse_release_id(RELEASE_ID)?,
                41,
            )
            .expect_err("mismatched evidence must fail");
            assert!(
                error.to_string().contains(field),
                "unexpected error for {field}: {error:#}"
            );
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validation_rejects_nonpositive_or_equal_snapshots() -> anyhow::Result<()> {
        for (field, value) in [
            ("historical_base_snapshot", json!(0)),
            ("base_snapshot", json!(0)),
            ("historical_branch_snapshot", json!(0)),
            ("branch_snapshot", json!(0)),
            ("branch_snapshot", json!(41)),
            ("branch_snapshot", json!(40)),
        ] {
            let mut payload = valid_evidence();
            payload[field] = value;
            let error = validate_spark_evidence(
                &serde_json::to_vec(&payload)?,
                parse_release_id(RELEASE_ID)?,
                41,
            )
            .expect_err("invalid snapshot must fail");
            assert!(error.to_string().contains("snapshot"));
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validation_rejects_unknown_or_token_fields() -> anyhow::Result<()> {
        for field in ["unexpected", "catalog_token"] {
            let mut payload = valid_evidence();
            payload[field] = json!("must-not-be-accepted");
            let error = validate_spark_evidence(
                &serde_json::to_vec(&payload)?,
                parse_release_id(RELEASE_ID)?,
                41,
            )
            .expect_err("unknown evidence field must fail");
            assert!(error.to_string().contains("strict schema"));
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validation_rejects_legacy_evidence() -> anyhow::Result<()> {
        for field in [
            "schema_version",
            "catalog_bucket",
            "historical_base_snapshot",
            "historical_branch_snapshot",
            "historical_branch_name",
            "historical_base_isolation",
            "retention",
        ] {
            let mut payload = valid_evidence();
            payload
                .as_object_mut()
                .expect("test evidence must be an object")
                .remove(field);
            let error = validate_spark_evidence(
                &serde_json::to_vec(&payload)?,
                parse_release_id(RELEASE_ID)?,
                41,
            )
            .expect_err("legacy evidence without the strict schema must fail");
            assert!(
                error.to_string().contains("strict schema"),
                "unexpected error for missing {field}: {error:#}"
            );
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_failed_atomic_install_leaves_no_final_or_temporary_file(
    ) -> anyhow::Result<()> {
        let root = test_root()?;
        let destination = root
            .join("target")
            .join("spatial-tile-publication")
            .join("failed-install.json");
        let error = write_json_create_new_with_installer(
            &destination,
            &valid_evidence(),
            "test evidence",
            |_temporary, _destination| Err(std::io::Error::other("sentinel install failure")),
        )
        .expect_err("failed atomic installation must propagate");
        assert!(error.to_string().contains("sentinel install failure"));
        assert!(!destination.exists());
        let parent = destination.parent().expect("destination has parent");
        let temporary_count = fs::read_dir(parent)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".failed-install.json.")
            })
            .count();
        assert_eq!(temporary_count, 0);
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validation_rejects_non_integer_json_snapshots() -> anyhow::Result<()> {
        for field in [
            "historical_base_snapshot",
            "base_snapshot",
            "historical_branch_snapshot",
            "branch_snapshot",
        ] {
            for value in [json!(41.0), json!(41.5), json!("41"), json!(true)] {
                let mut payload = valid_evidence();
                payload[field] = value;
                let error = validate_spark_evidence(
                    &serde_json::to_vec(&payload)?,
                    parse_release_id(RELEASE_ID)?,
                    41,
                )
                .expect_err("non-integer JSON snapshots must fail");
                assert!(
                    error.to_string().contains(field) && error.to_string().contains("JSON type"),
                    "unexpected error for {field}: {error:#}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_artifact_paths_are_confined_to_the_dedicated_root() -> anyhow::Result<()> {
        let root = test_root()?;
        let outside = root
            .parent()
            .expect("test root has a parent")
            .join("outside.json")
            .display()
            .to_string();
        for (name, value) in [
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_PLAN_PATH",
                "../outside.json".to_owned(),
            ),
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_EVIDENCE_PATH",
                outside,
            ),
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_VALIDATED_EVIDENCE_PATH",
                "target/not-spatial-proof.json".to_owned(),
            ),
        ] {
            let mut values = base_env(&root, "plan");
            values.insert(name.to_owned(), value);
            let error = config_from(&values).expect_err("outside path must fail");
            assert!(error
                .to_string()
                .contains("target/spatial-tile-publication"));
        }
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_artifact_paths_must_be_distinct() -> anyhow::Result<()> {
        let root = test_root()?;
        let shared = "target/spatial-tile-publication/shared.json";
        for (first, second) in [
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_PLAN_PATH",
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_EVIDENCE_PATH",
            ),
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_PLAN_PATH",
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_VALIDATED_EVIDENCE_PATH",
            ),
            (
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_EVIDENCE_PATH",
                "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_VALIDATED_EVIDENCE_PATH",
            ),
        ] {
            let mut values = base_env(&root, "plan");
            values.insert(first.to_owned(), shared.to_owned());
            values.insert(second.to_owned(), shared.to_owned());
            let error = config_from(&values).expect_err("artifact paths must not collide");
            assert!(error.to_string().contains("distinct"));
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn spatial_tile_wap_artifact_paths_reject_symlink_escape() -> anyhow::Result<()> {
        use std::os::unix::fs::symlink;

        let root = test_root()?;
        let outside = root
            .parent()
            .expect("test root has a parent")
            .join(format!("outside-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&outside)?;
        fs::create_dir_all(root.join("target"))?;
        symlink(&outside, root.join(ARTIFACT_ROOT))?;
        let error =
            config_from(&base_env(&root, "plan")).expect_err("symlinked proof root must fail");
        assert!(error.to_string().contains("symlink"));

        let nested_root = test_root()?;
        let nested_outside = nested_root
            .parent()
            .expect("test root has a parent")
            .join(format!("outside-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&nested_outside)?;
        let artifact_root = nested_root.join(ARTIFACT_ROOT);
        fs::create_dir_all(&artifact_root)?;
        symlink(&nested_outside, artifact_root.join("escape"))?;
        let mut values = base_env(&nested_root, "plan");
        values.insert(
            EVIDENCE_PATH_ENV.to_owned(),
            format!("{ARTIFACT_ROOT}/escape/evidence.json"),
        );
        let error = config_from(&values).expect_err("nested symlink ancestor must fail");
        assert!(error.to_string().contains("symlink"));
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_root_must_be_the_foundation_root() -> anyhow::Result<()> {
        let root = env::current_dir()?
            .join("target")
            .join("spatial-tile-wap-command-tests")
            .join(Uuid::new_v4().simple().to_string());
        fs::create_dir_all(&root)?;
        let root = fs::canonicalize(root)?;
        let values = base_env(&root, "plan");
        let error = config_from(&values).expect_err("root without markers must fail");
        assert!(error.to_string().contains("Foundation root"));
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validate_requires_expected_base_snapshot() -> anyhow::Result<()> {
        let root = test_root()?;
        let values = base_env(&root, "validate");
        let error = config_from(&values).expect_err("validate must require base snapshot");
        assert!(error
            .to_string()
            .contains("FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_EXPECTED_BASE_SNAPSHOT"));
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_validated_evidence_is_canonical_and_create_only() -> anyhow::Result<()> {
        let root = test_root()?;
        let mut values = base_env(&root, "validate");
        values.insert(
            "FOUNDATION_PLATFORM_SPATIAL_TILE_WAP_EXPECTED_BASE_SNAPSHOT".to_owned(),
            "41".to_owned(),
        );
        let config = config_from(&values)?;
        fs::create_dir_all(
            config
                .evidence_path
                .parent()
                .expect("evidence path has parent"),
        )?;
        fs::write(
            &config.evidence_path,
            serde_json::to_vec(&valid_evidence())?,
        )?;

        execute_action(&config)?;
        let written: JsonValue =
            serde_json::from_slice(&fs::read(&config.validated_evidence_path)?)?;
        assert_eq!(written, valid_evidence());
        let original = fs::read(&config.validated_evidence_path)?;

        let error = execute_action(&config).expect_err("validated evidence must not overwrite");
        assert!(error.to_string().contains("create"));
        assert_eq!(fs::read(&config.validated_evidence_path)?, original);
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_plan_is_written_create_only_without_running_docker() -> anyhow::Result<()> {
        let root = test_root()?;
        let config = config_from(&base_env(&root, "plan"))?;
        execute_action(&config)?;
        let written: HostExecutionPlan = serde_json::from_slice(&fs::read(&config.plan_path)?)?;
        assert_eq!(written, build_host_execution_plan(&config)?);
        assert!(!config.evidence_path.exists());
        let original = fs::read(&config.plan_path)?;

        let error = execute_action(&config).expect_err("plan must not overwrite");
        assert!(error.to_string().contains("create"));
        assert_eq!(fs::read(&config.plan_path)?, original);
        Ok(())
    }

    #[test]
    fn spatial_tile_wap_offline_output_never_claims_provider_capability() {
        assert_eq!(
            OFFLINE_CAPABILITY_LINE,
            "provider_capability=not_proven_offline"
        );
    }
}
