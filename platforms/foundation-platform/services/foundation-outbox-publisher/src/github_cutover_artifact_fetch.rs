use std::{
    env, fs,
    fs::File,
    io::{self},
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context};
use reqwest::{header, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;
use zip::ZipArchive;

use crate::loopback_http::LoopbackRetrySend;
use crate::public_data_control_support::{
    env_path, optional_env_value, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.github_cutover_artifact_fetch.v1";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/github-cutover-artifact-fetch.json";
const DEFAULT_API_BASE_URL: &str = "https://api.github.com";
const USER_AGENT: &str = "foundation-platform-cutover-artifact-fetcher";

pub(crate) async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let report = fetch(&config).await?;
    let status = report.status;
    let blocker_id = report.blocker_id.clone();
    let report_path = repo_relative_path(&config.root, &config.output_path);
    write_json_file(&config.output_path, &report)?;
    println!(
        "github-cutover-artifact-fetch-ok status={status} blocker={blocker_id} report={report_path}"
    );
    Ok(())
}

async fn fetch(config: &Config) -> anyhow::Result<FetchReport> {
    let spec = artifact_spec(config.blocker_id.as_str())?;
    let request_path = format!(
        "/repos/{}/actions/runs/{}/artifacts?name={}",
        config.owner_repo, config.run_id, spec.artifact_name
    );
    let mut report = FetchReport::new(
        "planned",
        config.execute,
        config.blocker_id.clone(),
        config.owner_repo.clone(),
        config.run_id.clone(),
        config
            .api_base_url
            .as_str()
            .trim_end_matches('/')
            .to_owned(),
        request_path.clone(),
        spec.artifact_name.to_owned(),
        spec.expected_artifact_path.to_owned(),
    );

    if !config.execute {
        return Ok(report);
    }
    if !config.confirm {
        bail!("Execute requires -ConfirmGitHubArtifactFetch");
    }
    if config.expected_artifact_path.is_file() {
        bail!(
            "cutover artifact already exists: {}",
            spec.expected_artifact_path
        );
    }

    let token = github_token()?;
    report.token_variable = Some(token.name.clone());
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("failed to build GitHub artifact fetch HTTP client")?;
    let list_url = config
        .api_base_url
        .join(request_path.as_str().trim_start_matches('/'))
        .context("failed to build GitHub artifact list URL")?;
    let list_response = client
        .get(list_url)
        .bearer_auth(token.value)
        .header(header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send_with_loopback_connect_retry()
        .await
        .context("failed to query GitHub Actions artifacts")?;
    let list_status = list_response.status().as_u16();
    let list_payload: ArtifactList = list_response
        .json()
        .await
        .context("failed to parse GitHub Actions artifact list")?;
    let matches = list_payload
        .artifacts
        .into_iter()
        .filter(|artifact| artifact.name == spec.artifact_name && !artifact.expired)
        .collect::<Vec<_>>();

    if matches.len() != 1 {
        let diagnostics =
            failure_diagnostics(&client, config, request_path.as_str(), matches.len()).await?;
        report.status = "blocked";
        report.failure_diagnostics = Some(diagnostics.clone());
        write_json_file(&config.output_path, &report)?;
        bail!(
            "{}: expected exactly one non-expired GitHub artifact named {}, found {}. {}",
            diagnostics.category,
            spec.artifact_name,
            matches.len(),
            diagnostics.message
        );
    }

    let selected = matches
        .into_iter()
        .next()
        .context("GitHub artifact match disappeared")?;
    let archive_url = resolve_github_url(
        selected.archive_download_url.as_str(),
        "archive_download_url",
    )?;
    let staging_root = resolve_repo_path(
        &config.root,
        &PathBuf::from(format!(
            "target/github-cutover-artifact-fetch/{}",
            Uuid::new_v4().simple()
        )),
        "staging_path",
    )?;
    let zip_path = staging_root.join(format!("{}.zip", spec.artifact_name));
    let extract_path = staging_root.join("extracted");
    fs::create_dir_all(&extract_path)
        .with_context(|| format!("failed to create staging path {}", extract_path.display()))?;

    let download_response = client
        .get(archive_url.clone())
        .bearer_auth(github_token_for_download(&report.token_variable)?)
        .header(header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send_with_loopback_connect_retry()
        .await
        .context("failed to download GitHub Actions artifact archive")?;
    let download_status = download_response.status().as_u16();
    let archive_bytes = download_response
        .bytes()
        .await
        .context("failed to read GitHub artifact archive body")?;
    fs::write(&zip_path, &archive_bytes).with_context(|| {
        format!(
            "failed to write GitHub artifact archive {}",
            zip_path.display()
        )
    })?;
    extract_zip(&zip_path, &extract_path)?;

    let expected_leaf = Path::new(spec.expected_artifact_path)
        .file_name()
        .and_then(|name| name.to_str())
        .context("expected artifact path must have a file name")?;
    let candidates = find_files_by_leaf_name(&extract_path, expected_leaf)?;
    if candidates.len() != 1 {
        bail!(
            "expected exactly one artifact file named {expected_leaf} in GitHub artifact archive, found {}",
            candidates.len()
        );
    }

    if let Some(parent) = config.expected_artifact_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create cutover artifact directory {}",
                parent.display()
            )
        })?;
    }
    fs::copy(&candidates[0], &config.expected_artifact_path).with_context(|| {
        format!(
            "failed to copy cutover artifact {} to {}",
            candidates[0].display(),
            config.expected_artifact_path.display()
        )
    })?;

    let copied_supporting_artifact_paths =
        copy_supporting_artifacts(&config.root, &extract_path, spec)?;
    report.status = "fetched";
    report.github_artifact = Some(GitHubArtifactReport {
        id: selected.id,
        name: selected.name,
        archive_download_url: archive_url.to_string(),
        expired: selected.expired,
    });
    report.download = Some(DownloadReport {
        list_status_code: list_status,
        download_status_code: download_status,
        archive_path: zip_path.to_string_lossy().into_owned(),
        extracted_file_path: candidates[0].to_string_lossy().into_owned(),
        copied_artifact_path: config.expected_artifact_path.to_string_lossy().into_owned(),
        copied_supporting_artifact_paths,
    });
    Ok(report)
}

fn github_token_for_download(token_variable: &Option<String>) -> anyhow::Result<String> {
    match token_variable.as_deref() {
        Some("GH_TOKEN") => env::var("GH_TOKEN").context("GH_TOKEN disappeared during fetch"),
        Some("GITHUB_TOKEN") => {
            env::var("GITHUB_TOKEN").context("GITHUB_TOKEN disappeared during fetch")
        }
        Some("gh auth token") => github_cli_token_value()
            .map(|token| token.value)
            .context("gh auth token disappeared during fetch"),
        _ => bail!("GitHub token source was not resolved"),
    }
}

async fn failure_diagnostics(
    client: &reqwest::Client,
    config: &Config,
    request_path: &str,
    found_count: usize,
) -> anyhow::Result<FailureDiagnostics> {
    let token = github_token()?;
    let jobs_path = format!(
        "/repos/{}/actions/runs/{}/jobs?per_page=100",
        config.owner_repo, config.run_id
    );
    let jobs_url = config
        .api_base_url
        .join(jobs_path.trim_start_matches('/'))
        .context("failed to build GitHub Actions jobs URL")?;
    let jobs_response = client
        .get(jobs_url)
        .bearer_auth(token.value.clone())
        .header(header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send_with_loopback_connect_retry()
        .await
        .context("failed to query GitHub Actions jobs for artifact diagnostics")?;
    let jobs_payload: JobsPayload = jobs_response
        .json()
        .await
        .context("failed to parse GitHub Actions jobs diagnostics")?;
    let mut annotations = Vec::new();

    for job in jobs_payload.jobs {
        if job.conclusion.as_deref() != Some("failure") {
            continue;
        }
        let annotations_path = format!(
            "/repos/{}/check-runs/{}/annotations?per_page=100",
            config.owner_repo, job.id
        );
        let annotations_url = config
            .api_base_url
            .join(annotations_path.trim_start_matches('/'))
            .context("failed to build GitHub check-run annotations URL")?;
        let annotation_response = client
            .get(annotations_url)
            .bearer_auth(token.value.clone())
            .header(header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send_with_loopback_connect_retry()
            .await
            .context("failed to query GitHub check-run annotations")?;
        let job_annotations: Vec<AnnotationPayload> = annotation_response
            .json()
            .await
            .context("failed to parse GitHub check-run annotations")?;
        for annotation in job_annotations {
            let annotation_report = AnnotationReport {
                job_id: job.id.to_string(),
                job_name: job.name.clone().unwrap_or_default(),
                runner_name: job.runner_name.clone().unwrap_or_default(),
                step_count: job.steps.as_ref().map_or(0, Vec::len),
                annotation_level: annotation.annotation_level.unwrap_or_default(),
                message: annotation.message.unwrap_or_default(),
            };
            let billing_blocker = annotation_report
                .message
                .contains("recent account payments have failed")
                || annotation_report
                    .message
                    .contains("spending limit needs to be increased")
                || annotation_report.message.contains("Billing & plans");
            annotations.push(annotation_report.clone());
            if billing_blocker {
                return Ok(FailureDiagnostics {
                    category: "github_actions_billing_or_spending_limit",
                    message: annotation_report.message,
                    next_action: "Open GitHub Settings > Billing & plans, fix the failed payment or increase the Actions spending limit, then rerun the cutover workflow.".to_owned(),
                    evidence_source: EvidenceSource {
                        jobs_path: jobs_path.clone(),
                        annotations_path: Some(annotations_path),
                        request_path: Some(request_path.to_owned()),
                    },
                    job: Some(JobReport {
                        id: job.id.to_string(),
                        name: job.name.unwrap_or_default(),
                        conclusion: job.conclusion.unwrap_or_default(),
                        runner_name: job.runner_name.unwrap_or_default(),
                        step_count: job.steps.map_or(0, |steps| steps.len()),
                    }),
                    annotations,
                    found_artifact_count: found_count,
                });
            }
        }
    }

    Ok(FailureDiagnostics {
        category: "github_actions_artifact_missing",
        message: "GitHub artifact was not found and no known runner account blocker annotation was detected.".to_owned(),
        next_action: "Open the GitHub Actions run, inspect failed jobs, fix the workflow or runner failure, rerun it, then fetch the cutover artifact again.".to_owned(),
        evidence_source: EvidenceSource {
            jobs_path,
            annotations_path: None,
            request_path: Some(request_path.to_owned()),
        },
        job: None,
        annotations,
        found_artifact_count: found_count,
    })
}

fn copy_supporting_artifacts(
    root: &Path,
    extract_path: &Path,
    spec: &ArtifactSpec,
) -> anyhow::Result<Vec<String>> {
    let mut copied = Vec::new();
    for supporting in spec.supporting_artifact_files {
        let source = resolve_extracted_archive_path(extract_path, supporting.archive_path)?;
        if !source.is_file() {
            bail!(
                "supporting artifact file missing in GitHub artifact archive: {}",
                supporting.archive_path
            );
        }
        let destination = resolve_repo_path(
            root,
            &PathBuf::from(supporting.destination_path),
            "supporting_artifact.destination_path",
        )?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create supporting artifact destination directory {}",
                    parent.display()
                )
            })?;
        }
        fs::copy(&source, &destination).with_context(|| {
            format!(
                "failed to copy supporting artifact {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        copied.push(supporting.destination_path.replace('\\', "/"));
    }
    Ok(copied)
}

fn resolve_extracted_archive_path(
    extract_path: &Path,
    archive_path: &str,
) -> anyhow::Result<PathBuf> {
    if archive_path.trim().is_empty() || Path::new(archive_path).is_absolute() {
        bail!("supporting archive path must be a safe relative path");
    }
    for segment in archive_path.replace('\\', "/").split('/') {
        if segment.trim().is_empty() || segment == "." || segment == ".." {
            bail!("supporting archive path must be a safe relative path");
        }
    }
    let resolved = extract_path.join(archive_path.replace('/', std::path::MAIN_SEPARATOR_STR));
    if !resolved.starts_with(extract_path) {
        bail!("supporting archive path must stay within extracted archive");
    }
    Ok(resolved)
}

fn extract_zip(zip_path: &Path, extract_path: &Path) -> anyhow::Result<()> {
    let file = File::open(zip_path).with_context(|| {
        format!(
            "failed to open GitHub artifact archive {}",
            zip_path.display()
        )
    })?;
    let mut archive =
        ZipArchive::new(file).context("failed to read GitHub artifact zip archive")?;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .with_context(|| format!("failed to read zip entry {index}"))?;
        let Some(enclosed_name) = entry.enclosed_name() else {
            bail!("GitHub artifact zip entry contains an unsafe path");
        };
        let output_path = extract_path.join(enclosed_name);
        if entry.is_dir() {
            fs::create_dir_all(&output_path).with_context(|| {
                format!("failed to create zip directory {}", output_path.display())
            })?;
            continue;
        }
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create zip output directory {}", parent.display())
            })?;
        }
        let mut output = File::create(&output_path).with_context(|| {
            format!("failed to create extracted file {}", output_path.display())
        })?;
        io::copy(&mut entry, &mut output)
            .with_context(|| format!("failed to extract zip file {}", output_path.display()))?;
    }
    Ok(())
}

fn find_files_by_leaf_name(root: &Path, leaf_name: &str) -> anyhow::Result<Vec<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];
    let mut matches = Vec::new();
    while let Some(current) = stack.pop() {
        for entry in fs::read_dir(&current)
            .with_context(|| format!("failed to read extracted directory {}", current.display()))?
        {
            let entry = entry.with_context(|| {
                format!(
                    "failed to inspect extracted directory {}",
                    current.display()
                )
            })?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .with_context(|| format!("failed to inspect extracted file {}", path.display()))?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file()
                && path.file_name().and_then(|name| name.to_str()) == Some(leaf_name)
            {
                matches.push(path);
            }
        }
    }
    Ok(matches)
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
            bail!("GH_TOKEN or GITHUB_TOKEN is required for GitHub artifact fetch, or authenticate GitHub CLI with gh auth login")
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

fn artifact_spec(id: &str) -> anyhow::Result<&'static ArtifactSpec> {
    ARTIFACT_SPECS
        .iter()
        .find(|spec| spec.blocker_id == id)
        .with_context(|| format!("unsupported BlockerId: {id}"))
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

struct Config {
    root: PathBuf,
    output_path: PathBuf,
    expected_artifact_path: PathBuf,
    blocker_id: String,
    owner_repo: String,
    run_id: String,
    api_base_url: Url,
    execute: bool,
    confirm: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path(
            "FOUNDATION_PLATFORM_GITHUB_CUTOVER_ARTIFACT_FETCH_ROOT",
            ".",
        )?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("Root does not exist: {}", root.display()))?;
        let blocker_id =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_ARTIFACT_FETCH_BLOCKER_ID")?
                .context("BlockerId is required")?;
        let spec = artifact_spec(blocker_id.as_str())?;
        let owner_repo =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_ARTIFACT_FETCH_OWNER_REPO")?
                .map_or_else(|| owner_repo_from_git(&root), Ok)?;
        validate_owner_repo(owner_repo.as_str())?;
        let run_id =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_ARTIFACT_FETCH_RUN_ID")?
                .context("RunId is required")?;
        if run_id == "0" || !run_id.chars().all(|value| value.is_ascii_digit()) {
            bail!("RunId must be a numeric GitHub Actions run id");
        }
        let api_base_url = resolve_github_url(
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_ARTIFACT_FETCH_API_BASE_URL")?
                .unwrap_or_else(|| DEFAULT_API_BASE_URL.to_owned())
                .as_str(),
            "GitHubApiBaseUrl",
        )?;
        let output_path = resolve_repo_path(
            &root,
            &env_path(
                "FOUNDATION_PLATFORM_GITHUB_CUTOVER_ARTIFACT_FETCH_OUTPUT_PATH",
                DEFAULT_OUTPUT_PATH,
            )?,
            "OutputPath",
        )?;
        let expected_artifact_path = resolve_repo_path(
            &root,
            &PathBuf::from(spec.expected_artifact_path),
            "expected_artifact_path",
        )?;
        let execute =
            optional_env_value("FOUNDATION_PLATFORM_GITHUB_CUTOVER_ARTIFACT_FETCH_EXECUTE")?
                .is_some();
        let confirm =
            optional_env_value("FOUNDATION_PLATFORM_CONFIRM_GITHUB_CUTOVER_ARTIFACT_FETCH")?
                .is_some();
        Ok(Self {
            root,
            output_path,
            expected_artifact_path,
            blocker_id,
            owner_repo,
            run_id,
            api_base_url,
            execute,
            confirm,
        })
    }
}

#[derive(Clone)]
struct Token {
    name: String,
    value: String,
}

struct ArtifactSpec {
    blocker_id: &'static str,
    artifact_name: &'static str,
    expected_artifact_path: &'static str,
    supporting_artifact_files: &'static [SupportingArtifactFile],
}

struct SupportingArtifactFile {
    archive_path: &'static str,
    destination_path: &'static str,
}

const SUPPLY_CHAIN_SUPPORTING_ARTIFACTS: &[SupportingArtifactFile] = &[
    SupportingArtifactFile {
        archive_path: "supply-chain-tools/cyclonedx-bom.json",
        destination_path: "target/supply-chain-tools/cyclonedx-bom.json",
    },
    SupportingArtifactFile {
        archive_path: "supply-chain-tools/supply-chain-tool-run.json",
        destination_path: "target/supply-chain-tools/supply-chain-tool-run.json",
    },
    SupportingArtifactFile {
        archive_path: "release-artifacts/foundation-platform-release.sha256",
        destination_path: "target/release-artifacts/foundation-platform-release.sha256",
    },
    SupportingArtifactFile {
        archive_path: "release-artifacts/foundation-platform-release.sha256.sigstore.json",
        destination_path:
            "target/release-artifacts/foundation-platform-release.sha256.sigstore.json",
    },
    SupportingArtifactFile {
        archive_path: "release-artifacts/foundation-platform-release.provenance.json",
        destination_path: "target/release-artifacts/foundation-platform-release.provenance.json",
    },
    SupportingArtifactFile {
        archive_path: "release-artifacts/foundation-platform-release.provenance.sigstore.json",
        destination_path:
            "target/release-artifacts/foundation-platform-release.provenance.sigstore.json",
    },
];

const ARTIFACT_SPECS: &[ArtifactSpec] = &[
    ArtifactSpec {
        blocker_id: "production-orchestrator",
        artifact_name: "production-orchestrator-run",
        expected_artifact_path: "target/cutover/production-orchestrator-run.json",
        supporting_artifact_files: &[],
    },
    ArtifactSpec {
        blocker_id: "consumer-deployed-receiver-e2e",
        artifact_name: "consumer-deployed-receiver-e2e",
        expected_artifact_path: "target/cutover/consumer-deployed-receiver-e2e.json",
        supporting_artifact_files: &[],
    },
    ArtifactSpec {
        blocker_id: "supply-chain-release-gates",
        artifact_name: "supply-chain-release-gates",
        expected_artifact_path: "target/cutover/supply-chain-release-gates.json",
        supporting_artifact_files: SUPPLY_CHAIN_SUPPORTING_ARTIFACTS,
    },
];

#[derive(Deserialize)]
struct ArtifactList {
    artifacts: Vec<GitHubArtifactPayload>,
}

#[derive(Deserialize)]
struct GitHubArtifactPayload {
    id: i64,
    name: String,
    archive_download_url: String,
    #[serde(default)]
    expired: bool,
}

#[derive(Deserialize)]
struct JobsPayload {
    jobs: Vec<JobPayload>,
}

#[derive(Deserialize)]
struct JobPayload {
    id: i64,
    name: Option<String>,
    conclusion: Option<String>,
    runner_name: Option<String>,
    steps: Option<Vec<JsonValue>>,
}

#[derive(Deserialize)]
struct AnnotationPayload {
    annotation_level: Option<String>,
    message: Option<String>,
}

#[derive(Serialize)]
struct FetchReport {
    schema_version: &'static str,
    generated_at_utc: String,
    scope: &'static str,
    completion_claim_allowed: bool,
    status: &'static str,
    blocker_id: String,
    execute: bool,
    owner_repo: String,
    run_id: String,
    api_base_url: String,
    request: RequestReport,
    artifact_name: String,
    expected_artifact_path: String,
    token_variable: Option<String>,
    github_artifact: Option<GitHubArtifactReport>,
    download: Option<DownloadReport>,
    failure_diagnostics: Option<FailureDiagnostics>,
    evidence_limitations: [&'static str; 3],
}

impl FetchReport {
    fn new(
        status: &'static str,
        execute: bool,
        blocker_id: String,
        owner_repo: String,
        run_id: String,
        api_base_url: String,
        request_path: String,
        artifact_name: String,
        expected_artifact_path: String,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            scope: "artifact_fetch_only",
            completion_claim_allowed: false,
            status,
            blocker_id,
            execute,
            owner_repo,
            run_id,
            api_base_url,
            request: RequestReport {
                method: "GET",
                path: request_path,
            },
            artifact_name,
            expected_artifact_path,
            token_variable: None,
            github_artifact: None,
            download: None,
            failure_diagnostics: None,
            evidence_limitations: [
                "does_not_validate_cutover_artifact_semantics",
                "does_not_attach_completion_evidence",
                "does_not_approve_or_mark_blocker_complete",
            ],
        }
    }
}

#[derive(Serialize)]
struct RequestReport {
    method: &'static str,
    path: String,
}

#[derive(Serialize)]
struct GitHubArtifactReport {
    id: i64,
    name: String,
    archive_download_url: String,
    expired: bool,
}

#[derive(Serialize)]
struct DownloadReport {
    list_status_code: u16,
    download_status_code: u16,
    archive_path: String,
    extracted_file_path: String,
    copied_artifact_path: String,
    copied_supporting_artifact_paths: Vec<String>,
}

#[derive(Clone, Serialize)]
struct FailureDiagnostics {
    category: &'static str,
    message: String,
    next_action: String,
    evidence_source: EvidenceSource,
    job: Option<JobReport>,
    annotations: Vec<AnnotationReport>,
    found_artifact_count: usize,
}

#[derive(Clone, Serialize)]
struct EvidenceSource {
    jobs_path: String,
    annotations_path: Option<String>,
    request_path: Option<String>,
}

#[derive(Clone, Serialize)]
struct JobReport {
    id: String,
    name: String,
    conclusion: String,
    runner_name: String,
    step_count: usize,
}

#[derive(Clone, Serialize)]
struct AnnotationReport {
    job_id: String,
    job_name: String,
    runner_name: String,
    step_count: usize,
    annotation_level: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::{
        artifact_spec, resolve_extracted_archive_path, resolve_github_url, validate_owner_repo,
    };
    use std::path::Path;

    #[test]
    fn artifact_spec_maps_blockers_to_expected_artifacts() -> anyhow::Result<()> {
        let production = artifact_spec("production-orchestrator")?;
        assert_eq!(production.artifact_name, "production-orchestrator-run");
        assert_eq!(
            production.expected_artifact_path,
            "target/cutover/production-orchestrator-run.json"
        );

        let supply_chain = artifact_spec("supply-chain-release-gates")?;
        assert_eq!(supply_chain.supporting_artifact_files.len(), 6);
        assert!(supply_chain
            .supporting_artifact_files
            .iter()
            .any(|artifact| {
                artifact.destination_path
                == "target/release-artifacts/foundation-platform-release.provenance.sigstore.json"
            }));
        Ok(())
    }

    #[test]
    fn github_url_allows_https_and_loopback_http_only() {
        assert!(resolve_github_url("https://api.github.com", "GitHubApiBaseUrl").is_ok());
        assert!(resolve_github_url("http://127.0.0.1:3000", "GitHubApiBaseUrl").is_ok());
        assert!(resolve_github_url("http://example.com", "GitHubApiBaseUrl").is_err());
    }

    #[test]
    fn supporting_archive_path_must_be_safe_relative_path() {
        assert!(
            resolve_extracted_archive_path(Path::new("target/extracted"), "../escape.json")
                .is_err()
        );
        assert!(resolve_extracted_archive_path(
            Path::new("target/extracted"),
            "release-artifacts/foundation-platform-release.sha256"
        )
        .is_ok());
    }

    #[test]
    fn owner_repo_uses_github_owner_repo_shape() {
        assert!(validate_owner_repo("acme/foundation-platform").is_ok());
        assert!(validate_owner_repo("acme").is_err());
        assert!(validate_owner_repo("acme/platform/core").is_err());
    }
}
