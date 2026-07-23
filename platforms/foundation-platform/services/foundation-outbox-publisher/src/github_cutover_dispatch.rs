use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context};
use reqwest::{header, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::loopback_http::LoopbackRetrySend;
use crate::public_data_control_support::{
    env_path, optional_env_value, repo_relative_path, required_secrets_for_mode, resolve_repo_path,
    utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.github_cutover_workflow_dispatch.v1";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/github-cutover-workflow-dispatch.json";
const DEFAULT_API_BASE_URL: &str = "https://api.github.com";
const DEFAULT_WORKFLOW_FILE: &str = "ci.yml";
const DEFAULT_REF: &str = "main";
const DEFAULT_SCHEDULER_RUNTIME: &str = "github-actions";
const DEFAULT_BACKPRESSURE_STATUS: &str = "observed";
const USER_AGENT: &str = "foundation-platform-cutover-dispatcher";
const RECEIVER_PATH: &str = "/foundation-platform/events";
const EXPECTED_RECEIVER_SLUGS: &[&str] = &["gongzzang", "dawneer"];

pub(crate) async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let report = dispatch(&config).await?;
    let status = report.status;
    let mode = report.mode.clone();
    let report_path = repo_relative_path(&config.root, &config.output_path);
    write_json_file(&config.output_path, &report)?;
    println!(
        "github-cutover-workflow-dispatch-ok status={status} mode={mode} report={report_path}"
    );
    Ok(())
}

async fn dispatch(config: &Config) -> anyhow::Result<DispatchReport> {
    let mode_config = mode_config(config)?;
    let request_path = format!(
        "/repos/{}/actions/workflows/{}/dispatches",
        config.owner_repo,
        percent_encode_path_segment(config.workflow_file.as_str())
    );
    let body = json!({
        "ref": config.git_ref,
        "inputs": mode_config.inputs,
    });
    let mut report = DispatchReport {
        schema_version: SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        scope: "dispatch_request_only",
        completion_claim_allowed: false,
        status: "planned",
        mode: config.mode.clone(),
        blocker_id: mode_config.blocker_id,
        execute: config.execute,
        owner_repo: config.owner_repo.clone(),
        workflow_file: config.workflow_file.clone(),
        git_ref: config.git_ref.clone(),
        api_base_url: config
            .api_base_url
            .as_str()
            .trim_end_matches('/')
            .to_owned(),
        request: RequestReport {
            method: "POST",
            path: request_path.clone(),
        },
        inputs: mode_config.inputs.clone(),
        final_evidence_artifact: mode_config.final_evidence_artifact,
        token_variable: None,
        response: None,
        evidence_limitations: [
            "does_not_create_completion_artifacts",
            "workflow_completion_and_artifact_download_still_required",
            "completion_evidence_must_be_attached_separately",
        ],
    };

    if !config.execute {
        return Ok(report);
    }
    if !config.confirm {
        bail!("Execute requires -ConfirmGitHubWorkflowDispatch");
    }

    let token = github_token()?;
    report.token_variable = Some(token.name.clone());
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("failed to build GitHub workflow dispatch HTTP client")?;
    assert_required_secrets_present(&client, config, &token, config.mode.as_str()).await?;
    let request_url = config
        .api_base_url
        .join(request_path.trim_start_matches('/'))
        .context("failed to build GitHub workflow dispatch URL")?;
    let body_json =
        serde_json::to_string(&body).context("failed to serialize GitHub dispatch request body")?;
    let response = client
        .post(request_url)
        .bearer_auth(token.value)
        .header(header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body_json)
        .send_with_loopback_connect_retry()
        .await
        .context("failed to dispatch GitHub Actions workflow")?;
    report.status = "dispatched";
    report.response = Some(ResponseReport {
        status_code: response.status().as_u16(),
        status_description: response
            .status()
            .canonical_reason()
            .unwrap_or_default()
            .to_owned(),
    });
    Ok(report)
}

async fn assert_required_secrets_present(
    client: &reqwest::Client,
    config: &Config,
    token: &Token,
    mode: &str,
) -> anyhow::Result<()> {
    let required = required_secrets_for_mode(mode);
    if required.is_empty() {
        return Ok(());
    }
    let request_path = format!("/repos/{}/actions/secrets", config.owner_repo);
    let request_url = config
        .api_base_url
        .join(request_path.trim_start_matches('/'))
        .context("failed to build GitHub Actions secrets URL")?;
    let response = client
        .get(request_url)
        .bearer_auth(token.value.clone())
        .header(header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send_with_loopback_connect_retry()
        .await
        .context("failed to query GitHub Actions secrets")?;
    let payload: SecretsPayload = response
        .json()
        .await
        .context("failed to parse GitHub Actions secrets")?;
    let present = payload
        .secrets
        .into_iter()
        .map(|secret| secret.name)
        .filter(|name| !name.trim().is_empty())
        .collect::<BTreeSet<_>>();
    let missing = required
        .iter()
        .filter(|secret| !present.contains(**secret))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "missing required GitHub Actions secrets for {mode} dispatch: {}",
            missing.join(",")
        );
    }
    Ok(())
}

fn mode_config(config: &Config) -> anyhow::Result<ModeConfig> {
    match config.mode.as_str() {
        "ProductionOrchestrator" => {
            if config.scheduler_runtime.trim().is_empty() {
                bail!("SchedulerRuntime is required for ProductionOrchestrator dispatch");
            }
            if !matches!(config.backpressure_status.as_str(), "observed" | "passed") {
                bail!("BackpressureStatus must be observed or passed");
            }
            Ok(ModeConfig {
                blocker_id: "production-orchestrator",
                final_evidence_artifact: "target/cutover/production-orchestrator-run.json",
                inputs: json!({
                    "production_orchestrator_cutover_evidence": true,
                    "production_orchestrator_runtime": config.scheduler_runtime,
                    "production_orchestrator_backpressure_status": config.backpressure_status,
                }),
            })
        }
        "ConsumerReceiverE2E" => {
            assert_consumer_receiver_endpoints(config.consumer_receiver_endpoints.as_str())?;
            Ok(ModeConfig {
                blocker_id: "consumer-deployed-receiver-e2e",
                final_evidence_artifact: "target/cutover/consumer-deployed-receiver-e2e.json",
                inputs: json!({
                    "consumer_receiver_e2e": true,
                    "consumer_receiver_endpoints": config.consumer_receiver_endpoints,
                }),
            })
        }
        "SupplyChainReleaseGates" => {
            let mut inputs = serde_json::Map::new();
            inputs.insert("supply_chain_cutover_evidence".to_owned(), json!(true));
            if !config.signature_digest.trim().is_empty() {
                assert_signature_digest(config.signature_digest.as_str())?;
                inputs.insert(
                    "supply_chain_signature_digest".to_owned(),
                    json!(config.signature_digest),
                );
            }
            Ok(ModeConfig {
                blocker_id: "supply-chain-release-gates",
                final_evidence_artifact: "target/cutover/supply-chain-release-gates.json",
                inputs: JsonValue::Object(inputs),
            })
        }
        _ => bail!(
            "Mode must be ProductionOrchestrator, ConsumerReceiverE2E, or SupplyChainReleaseGates"
        ),
    }
}

fn assert_consumer_receiver_endpoints(raw: &str) -> anyhow::Result<()> {
    if raw.trim().is_empty() {
        bail!("ConsumerReceiverEndpoints is required for ConsumerReceiverE2E dispatch");
    }
    let map = endpoint_map(raw)?;
    for slug in EXPECTED_RECEIVER_SLUGS {
        let Some(endpoint) = map.get(*slug) else {
            bail!("missing consumer receiver endpoint: {slug}");
        };
        let url = Url::parse(endpoint)
            .with_context(|| format!("consumer receiver endpoint URL invalid: {slug}"))?;
        if url.scheme() != "https" {
            bail!("consumer receiver endpoint must use HTTPS: {slug}");
        }
        if is_placeholder_host(url.host_str().unwrap_or_default()) {
            bail!("consumer receiver endpoint host must not be a placeholder: {slug}");
        }
        if url.path() != RECEIVER_PATH {
            bail!("consumer receiver endpoint path mismatch: {slug} expected={RECEIVER_PATH}");
        }
    }
    Ok(())
}

fn endpoint_map(raw: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for entry in raw.split(';').filter(|entry| !entry.trim().is_empty()) {
        let Some((slug, url)) = entry.split_once('=') else {
            bail!("ConsumerReceiverEndpoints must use slug=url syntax: {entry}");
        };
        let slug = slug.trim();
        let url = url.trim();
        if slug.is_empty() || url.is_empty() {
            bail!("ConsumerReceiverEndpoints must include non-empty slug and URL: {entry}");
        }
        if map.insert(slug.to_owned(), url.to_owned()).is_some() {
            bail!("duplicate consumer receiver endpoint slug: {slug}");
        }
    }
    Ok(map)
}

fn assert_signature_digest(digest: &str) -> anyhow::Result<()> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        bail!("signature digest must use sha256:<64-hex> format");
    };
    if hex.len() != 64 || !hex.chars().all(|value| value.is_ascii_hexdigit()) {
        bail!("signature digest must use sha256:<64-hex> format");
    }
    let lowercase = hex.to_ascii_lowercase();
    let mut chars = lowercase.chars();
    let first = chars.next().unwrap_or_default();
    if chars.all(|value| value == first) {
        bail!("signature digest must not be a repeated-hex placeholder");
    }
    Ok(())
}

fn github_token() -> anyhow::Result<Token> {
    for name in ["GH_TOKEN", "GITHUB_TOKEN"] {
        if let Some(value) = optional_env_value(name)? {
            return Ok(Token {
                name: name.to_owned(),
                value,
            });
        }
    }
    github_cli_token_value().map_or_else(
        || {
            bail!("GH_TOKEN or GITHUB_TOKEN is required for GitHub workflow dispatch, or authenticate GitHub CLI with gh auth login")
        },
        Ok,
    )
}

fn github_cli_token_value() -> Option<Token> {
    if env::var("FOUNDATION_PLATFORM_DISABLE_GH_CLI_AUTH")
        .ok()
        .as_deref()
        == Some("1")
    {
        return None;
    }
    for candidate in gh_cli_candidates() {
        let Ok(output) = Command::new(&candidate).args(["auth", "token"]).output() else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let token = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if token.is_empty() {
            continue;
        }
        return Some(Token {
            name: "gh auth token".to_owned(),
            value: token,
        });
    }
    None
}

fn gh_cli_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path_var) = env::var_os("PATH") {
        for directory in env::split_paths(&path_var) {
            for file_name in ["gh.exe", "gh.cmd", "gh.bat", "gh"] {
                let candidate = directory.join(file_name);
                if candidate.is_file() {
                    candidates.push(candidate);
                }
            }
        }
    }
    let program_files = PathBuf::from(r"C:\Program Files\GitHub CLI\gh.exe");
    if program_files.is_file() {
        candidates.push(program_files);
    }
    candidates
}

fn resolve_github_url(raw: &str, field: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw.trim_end_matches('/'))
        .with_context(|| format!("{field} must be an absolute URL"))?;
    if url.scheme() == "https" {
        return Ok(url);
    }
    let is_loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
    if url.scheme() == "http" && is_loopback {
        return Ok(url);
    }
    bail!("{field} must use HTTPS unless it is a loopback test URL");
}

fn owner_repo_from_git(root: &Path) -> anyhow::Result<String> {
    let output = Command::new("git")
        .args(["-C", &root.to_string_lossy(), "remote", "get-url", "origin"])
        .output()
        .context("failed to read git origin remote")?;
    if !output.status.success() {
        bail!("OwnerRepo is required when git origin remote is missing");
    }
    let remote = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let Some(stripped) = remote.strip_prefix("https://github.com/") else {
        bail!("origin remote must be an HTTPS GitHub repository URL");
    };
    let owner_repo = stripped.strip_suffix(".git").unwrap_or(stripped).to_owned();
    validate_owner_repo(owner_repo.as_str())?;
    Ok(owner_repo)
}

fn validate_owner_repo(owner_repo: &str) -> anyhow::Result<()> {
    let parts = owner_repo.split('/').collect::<Vec<_>>();
    if parts.len() != 2
        || parts
            .iter()
            .any(|part| part.is_empty() || !part.chars().all(is_github_name_char))
    {
        bail!("OwnerRepo must use owner/repo format");
    }
    Ok(())
}

fn is_github_name_char(value: char) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, '_' | '.' | '-')
}

fn is_placeholder_host(host: &str) -> bool {
    if host.trim().is_empty() {
        return true;
    }
    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "example.com" | "example.net" | "example.org" | "localhost" | "127.0.0.1" | "::1"
    ) || normalized.ends_with(".example")
        || normalized.ends_with(".test")
        || normalized.ends_with(".invalid")
        || normalized.ends_with(".localhost")
}

fn percent_encode_path_segment(raw: &str) -> String {
    let mut encoded = String::new();
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(format!("%{byte:02X}").as_str());
        }
    }
    encoded
}

struct Config {
    root: PathBuf,
    output_path: PathBuf,
    mode: String,
    owner_repo: String,
    workflow_file: String,
    git_ref: String,
    api_base_url: Url,
    scheduler_runtime: String,
    backpressure_status: String,
    consumer_receiver_endpoints: String,
    signature_digest: String,
    execute: bool,
    confirm: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("Root does not exist: {}", root.display()))?;
        let mode = optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_MODE")?
            .context("Mode is required")?;
        let owner_repo =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_OWNER_REPO")?
                .map_or_else(|| owner_repo_from_git(&root), Ok)?;
        validate_owner_repo(owner_repo.as_str())?;
        let workflow_file =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_WORKFLOW_FILE")?
                .unwrap_or_else(|| DEFAULT_WORKFLOW_FILE.to_owned());
        if workflow_file.trim().is_empty() {
            bail!("WorkflowFile is required");
        }
        let git_ref = optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_REF")?
            .unwrap_or_else(|| DEFAULT_REF.to_owned());
        if git_ref.trim().is_empty() {
            bail!("Ref is required");
        }
        let api_base_url = resolve_github_url(
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_API_BASE_URL")?
                .unwrap_or_else(|| DEFAULT_API_BASE_URL.to_owned())
                .as_str(),
            "GitHubApiBaseUrl",
        )?;
        let output_path = resolve_repo_path(
            &root,
            &env_path(
                "FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_OUTPUT_PATH",
                DEFAULT_OUTPUT_PATH,
            )?,
            "OutputPath",
        )?;
        let scheduler_runtime =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_SCHEDULER_RUNTIME")?
                .unwrap_or_else(|| DEFAULT_SCHEDULER_RUNTIME.to_owned());
        let backpressure_status =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_BACKPRESSURE_STATUS")?
                .unwrap_or_else(|| DEFAULT_BACKPRESSURE_STATUS.to_owned());
        let consumer_receiver_endpoints = optional_env_value(
            "FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_CONSUMER_RECEIVER_ENDPOINTS",
        )?
        .unwrap_or_default();
        let signature_digest =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_SIGNATURE_DIGEST")?
                .unwrap_or_default();
        let execute =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_DISPATCH_EXECUTE")?.is_some();
        let confirm =
            optional_env_value("FOUNDATION_PLATFORM_CONFIRM_GITHUB_CUTOVER_WORKFLOW_DISPATCH")?
                .is_some();
        Ok(Self {
            root,
            output_path,
            mode,
            owner_repo,
            workflow_file,
            git_ref,
            api_base_url,
            scheduler_runtime,
            backpressure_status,
            consumer_receiver_endpoints,
            signature_digest,
            execute,
            confirm,
        })
    }
}

struct ModeConfig {
    blocker_id: &'static str,
    final_evidence_artifact: &'static str,
    inputs: JsonValue,
}

#[derive(Clone)]
struct Token {
    name: String,
    value: String,
}

#[derive(Deserialize)]
struct SecretsPayload {
    #[serde(default)]
    secrets: Vec<SecretPayload>,
}

#[derive(Deserialize)]
struct SecretPayload {
    name: String,
}

#[derive(Serialize)]
struct DispatchReport {
    schema_version: &'static str,
    generated_at_utc: String,
    scope: &'static str,
    completion_claim_allowed: bool,
    status: &'static str,
    mode: String,
    blocker_id: &'static str,
    execute: bool,
    owner_repo: String,
    workflow_file: String,
    #[serde(rename = "ref")]
    git_ref: String,
    api_base_url: String,
    request: RequestReport,
    inputs: JsonValue,
    final_evidence_artifact: &'static str,
    token_variable: Option<String>,
    response: Option<ResponseReport>,
    evidence_limitations: [&'static str; 3],
}

#[derive(Serialize)]
struct RequestReport {
    method: &'static str,
    path: String,
}

#[derive(Serialize)]
struct ResponseReport {
    status_code: u16,
    status_description: String,
}

#[cfg(test)]
mod tests {
    use super::{
        assert_consumer_receiver_endpoints, assert_signature_digest, mode_config,
        percent_encode_path_segment, required_secrets_for_mode, Config,
    };
    use reqwest::Url;
    use std::path::PathBuf;

    #[test]
    fn mode_config_maps_dispatch_inputs_and_artifacts() -> anyhow::Result<()> {
        let config = Config {
            root: PathBuf::from("."),
            output_path: PathBuf::from("target/audit/report.json"),
            mode: "ProductionOrchestrator".to_owned(),
            owner_repo: "acme/foundation-platform".to_owned(),
            workflow_file: "ci.yml".to_owned(),
            git_ref: "main".to_owned(),
            api_base_url: Url::parse("https://api.github.com")?,
            scheduler_runtime: "github-actions".to_owned(),
            backpressure_status: "passed".to_owned(),
            consumer_receiver_endpoints: String::new(),
            signature_digest: String::new(),
            execute: false,
            confirm: false,
        };
        let mode = mode_config(&config)?;
        assert_eq!(mode.blocker_id, "production-orchestrator");
        assert_eq!(
            mode.final_evidence_artifact,
            "target/cutover/production-orchestrator-run.json"
        );
        assert_eq!(
            mode.inputs["production_orchestrator_backpressure_status"],
            "passed"
        );
        Ok(())
    }

    #[test]
    fn consumer_receiver_endpoints_require_real_https_foundation_platform_events_urls() {
        assert!(assert_consumer_receiver_endpoints("gongzzang=https://gongzzang-receiver.operator-owned.net/foundation-platform/events;dawneer=https://dawneer-receiver.operator-owned.net/foundation-platform/events").is_ok());
        assert!(assert_consumer_receiver_endpoints(
            "gongzzang=http://gongzzang-receiver.operator-owned.net/foundation-platform/events;dawneer=https://dawneer-receiver.operator-owned.net/foundation-platform/events"
        )
        .is_err());
        assert!(assert_consumer_receiver_endpoints(
            "gongzzang=https://example.com/foundation-platform/events;dawneer=https://dawneer-receiver.operator-owned.net/foundation-platform/events"
        )
        .is_err());
    }

    #[test]
    fn signature_digest_rejects_repeated_placeholder_hex() {
        assert!(assert_signature_digest(&format!("sha256:{}", "f".repeat(64))).is_err());
        assert!(assert_signature_digest(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        )
        .is_ok());
    }

    #[test]
    fn required_secrets_are_selected_by_dispatch_mode() {
        assert!(required_secrets_for_mode("ProductionOrchestrator")
            .contains(&"FOUNDATION_PLATFORM_DATABASE_URL"));
        assert!(!required_secrets_for_mode("ProductionOrchestrator")
            .contains(&"FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET"));
        assert_eq!(
            required_secrets_for_mode("ConsumerReceiverE2E"),
            vec!["FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET"]
        );
        assert!(required_secrets_for_mode("SupplyChainReleaseGates").is_empty());
    }

    #[test]
    fn workflow_file_is_encoded_as_one_path_segment() {
        assert_eq!(percent_encode_path_segment("ci.yml"), "ci.yml");
        assert_eq!(
            percent_encode_path_segment("release/cutover.yml"),
            "release%2Fcutover.yml"
        );
    }
}
