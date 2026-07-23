use std::{
    env, fs,
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.regional_data_serving_load_evidence.v1";
const PRIOR_PROOF_SCHEMA_VERSION: &str = "foundation-platform.postgis_anchor_pbf_regional_proof.v1";
const DEFAULT_API_BASE_URL: &str = "http://127.0.0.1:18080";
const DEFAULT_PRIOR_PROOF_PATH: &str = "target/audit/postgis-anchor-pbf-regional-proof.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/regional-data-serving-load-evidence.json";
const DEFAULT_DURATION: &str = "30s";
const DEFAULT_READ_RPS: i64 = 5;
const DEFAULT_HEALTH_RPS: i64 = 2;
const DEFAULT_TIMEOUT_SEC: u64 = 5;
const MARKER_CONTRACT_PATH: &str = "/map/v1/marker-tiles/contract";
const MARKER_TILE_PATH: &str =
    "/map/v1/marker-tiles/parcel_anchor/12/3489/1588.pbf?filter_hash=all-active-v1";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    if !config.prior_proof_path.is_file() {
        write_skipped_report(&config)?;
        println!(
            "regional-data-serving-load-ok status=skipped report={}",
            config.output_path.display()
        );
        return Ok(());
    }

    let prior_proof = read_json(
        &config.prior_proof_path,
        "postgis anchor PBF regional proof",
    )?;
    let mut blockers = validate_prior_proof(&prior_proof);
    if !blockers.is_empty() {
        write_prior_blocked_report(&config, blockers.clone())?;
        print_blocked(&config, blockers);
        bail!("regional data serving load blocked");
    }

    let prior_status = json_string(&prior_proof, "status");
    if prior_status == "skipped" {
        write_skipped_report(&config)?;
        println!(
            "regional-data-serving-load-ok status=skipped report={}",
            config.output_path.display()
        );
        return Ok(());
    }
    if prior_status != "ready" {
        blockers.push("prior regional proof status must be ready".to_owned());
        write_prior_blocked_report(&config, blockers.clone())?;
        print_blocked(&config, blockers);
        bail!("regional data serving load blocked");
    }

    let health = invoke_http_probe(&config, "/healthz", false)?;
    let ready = invoke_http_probe(&config, "/readyz", false)?;
    let marker_contract = invoke_http_probe(&config, MARKER_CONTRACT_PATH, false)?;
    let marker_tile = invoke_http_probe(&config, MARKER_TILE_PATH, true)?;

    validate_probe_responses(
        &mut blockers,
        &health,
        &ready,
        &marker_contract,
        &marker_tile,
    );
    let contract_json = parse_marker_contract(&marker_contract.body, &mut blockers);

    // A k6 read-smoke summary is produced out-of-band by an operator. When its
    // `--summary-export` JSON is provided, fold its thresholds into the evidence; otherwise the
    // native HTTP probes and assertions above stand on their own.
    let mut resolved_load_summary_path = None;
    let mut load_metrics = LoadMetrics::default();
    if let Some(summary_path_raw) = &config.load_summary_path_raw {
        let path = resolve_repo_path(&config.root, summary_path_raw, "LoadSummaryPath", true)?;
        let load_summary = read_json(&path, "regional data serving load summary")?;
        load_metrics = inspect_load_summary(&load_summary, &mut blockers);
        resolved_load_summary_path = Some(path);
    }

    let metrics = invoke_http_probe(&config, "/metrics", false)?;
    let metrics_body = metrics.body.as_str();
    let metrics_summary = MetricsSummary {
        http_status: metrics.status_code,
        database_ready: metrics_body.contains("foundation_api_database_ready 1"),
        marker_contract_route_seen: metrics_contains_route(
            metrics_body,
            "/map/v1/marker-tiles/contract",
        ),
        parcel_anchor_tile_route_seen: metrics_contains_route(
            metrics_body,
            "/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf",
        ),
    };
    validate_metrics(&mut blockers, &metrics_summary);

    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let report = FullReport {
        schema_version: SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        status: status.to_owned(),
        completion_claim_allowed: false,
        national_rollout_allowed: false,
        national_rollout_blocked_reason: "regional_serving_load_proof_only",
        api_base_url: config.api_base_url.clone(),
        scope: "bounded_regional_postgis_anchor_pbf_serving_only",
        evidence_paths: FullEvidencePaths {
            postgis_anchor_pbf_regional_proof: repo_relative_path(
                &config.root,
                &config.prior_proof_path,
            ),
            load_summary: resolved_load_summary_path
                .as_ref()
                .map(|path| repo_relative_path(&config.root, path))
                .unwrap_or_default(),
            output: repo_relative_path(&config.root, &config.output_path),
        },
        load: LoadReport {
            duration: config.duration.clone(),
            read_rps: config.read_rps,
            health_rps: config.health_rps,
            error_rate: load_metrics.failed_rate,
            p95_ms: load_metrics.p95,
            p99_ms: load_metrics.p99,
            checks_rate: load_metrics.checks_rate,
            load_summary_provided: resolved_load_summary_path.is_some(),
        },
        marker_contract: MarkerContractReport {
            endpoint: MARKER_CONTRACT_PATH,
            http_status: marker_contract.status_code,
            response: contract_json.unwrap_or(JsonValue::Null),
            content_type: marker_contract.content_type.clone(),
        },
        marker_tile: MarkerTileReport {
            endpoint: MARKER_TILE_PATH,
            http_status: marker_tile.status_code,
            content_type: marker_tile.content_type.clone(),
            cache_control: marker_tile.cache_control.clone(),
            body_length: marker_tile.body_length,
        },
        metrics: metrics_summary,
        blockers: blockers.clone(),
        next_gates: vec!["explicit-national-rollout-approval"],
        evidence_limitations: vec![
            "does_not_run_national_collection",
            "does_not_approve_production_cutover",
            "does_not_prove_deployed_aws_runtime",
            "regional_dataset_only",
        ],
    };
    write_json_file(&config.output_path, &report)?;

    if status != "ready" {
        print_blocked(&config, blockers);
        bail!("regional data serving load blocked");
    }

    println!(
        "regional-data-serving-load-ok status=ready p95_ms={} error_rate={} marker_tile_bytes={} report={}",
        load_metrics.p95,
        load_metrics.failed_rate,
        marker_tile.body_length,
        config.output_path.display()
    );
    Ok(())
}

fn validate_prior_proof(prior_proof: &JsonValue) -> Vec<String> {
    let mut blockers = Vec::new();
    add_if(
        &mut blockers,
        json_string(prior_proof, "schema_version") != PRIOR_PROOF_SCHEMA_VERSION,
        "prior regional proof schema mismatch",
    );
    add_if(
        &mut blockers,
        json_bool(prior_proof, "completion_claim_allowed", false),
        "prior regional proof must not allow completion claims",
    );
    add_if(
        &mut blockers,
        json_bool(prior_proof, "national_rollout_allowed", false),
        "prior regional proof must keep national rollout blocked",
    );
    blockers
}

fn validate_probe_responses(
    blockers: &mut Vec<String>,
    health: &HttpProbe,
    ready: &HttpProbe,
    marker_contract: &HttpProbe,
    marker_tile: &HttpProbe,
) {
    add_if(
        blockers,
        health.status_code != 200,
        "health probe must return 200",
    );
    add_if(
        blockers,
        health.status_code == 200 && !health_probe_is_foundation_platform(health),
        "health probe service must be foundation-api",
    );
    add_if(
        blockers,
        ready.status_code != 200,
        "ready probe must return 200",
    );
    add_if(
        blockers,
        marker_contract.status_code != 200,
        "marker tile contract must return 200",
    );
    add_if(
        blockers,
        marker_tile.status_code != 200,
        "parcel anchor marker tile must return 200",
    );
    add_if(
        blockers,
        !marker_tile.content_type.contains("application/x-protobuf"),
        "parcel anchor marker tile content-type must be application/x-protobuf",
    );
    add_if(
        blockers,
        !(marker_tile.cache_control.contains("public")
            && marker_tile.cache_control.contains("max-age")),
        "parcel anchor marker tile must be HTTP cacheable",
    );
    add_if(
        blockers,
        marker_tile.body_length < 1,
        "parcel anchor marker tile must be non-empty for the bounded regional proof",
    );
}

fn health_probe_is_foundation_platform(health: &HttpProbe) -> bool {
    serde_json::from_str::<JsonValue>(&health.body)
        .ok()
        .and_then(|body| {
            body.get("service")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some("foundation-api")
}

fn parse_marker_contract(body: &str, blockers: &mut Vec<String>) -> Option<JsonValue> {
    let contract = match serde_json::from_str::<JsonValue>(body) {
        Ok(contract) => Some(contract),
        Err(_) => {
            blockers.push("marker tile contract response must be JSON".to_owned());
            None
        }
    };
    let contract_json = contract.as_ref()?;
    add_if(
        blockers,
        json_string(contract_json, "response_format") != "mvt_pbf",
        "marker tile contract response_format must be mvt_pbf",
    );
    add_if(
        blockers,
        json_string(contract_json, "position_source") != "pnu_anchor",
        "marker tile contract position_source must be pnu_anchor",
    );
    add_if(
        blockers,
        !json_bool(contract_json, "bbox_marker_runtime_forbidden", false),
        "marker tile contract must forbid bbox marker runtime",
    );
    contract
}

fn inspect_load_summary(summary: &JsonValue, blockers: &mut Vec<String>) -> LoadMetrics {
    let failed_values = metric_values(summary, "http_req_failed");
    let duration_values = metric_values(summary, "http_req_duration");
    let checks_values = metric_values(summary, "checks");

    let failed_rate = json_f64_opt(
        failed_values,
        &["rate", "value"],
        "load.http_req_failed.rate",
        blockers,
    );
    let p95 = json_f64_opt(
        duration_values,
        &["p(95)"],
        "load.http_req_duration.p95",
        blockers,
    );
    let p99 = json_f64_opt(
        duration_values,
        &["p(99)"],
        "load.http_req_duration.p99",
        blockers,
    );
    let checks_rate = json_f64_opt(
        checks_values,
        &["rate", "value"],
        "load.checks.rate",
        blockers,
    );

    add_if(
        blockers,
        failed_rate >= 0.01,
        "load error rate must be below 1%",
    );
    add_if(blockers, p95 >= 500.0, "load p95 must be below 500ms");
    add_if(blockers, p99 >= 1500.0, "load p99 must be below 1500ms");
    add_if(blockers, checks_rate < 1.0, "all k6 checks must pass");

    LoadMetrics {
        failed_rate,
        p95,
        p99,
        checks_rate,
    }
}

fn validate_metrics(blockers: &mut Vec<String>, metrics: &MetricsSummary) {
    add_if(
        blockers,
        metrics.http_status != 200,
        "metrics probe must return 200",
    );
    add_if(
        blockers,
        !metrics.database_ready,
        "metrics must expose database readiness",
    );
    add_if(
        blockers,
        !metrics.marker_contract_route_seen,
        "metrics must include marker tile contract reads",
    );
    add_if(
        blockers,
        !metrics.parcel_anchor_tile_route_seen,
        "metrics must include parcel anchor marker tile reads",
    );
}

fn invoke_http_probe(config: &Config, path: &str, binary: bool) -> anyhow::Result<HttpProbe> {
    let uri = parse_http_uri(&join_api_uri(&config.api_base_url, path))?;
    if uri.scheme != "http" {
        bail!("regional serving load probe supports http base URLs only");
    }
    let addr = format!("{}:{}", uri.host, uri.port);
    let socket = addr
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {addr}"))?
        .next()
        .with_context(|| format!("no socket address resolved for {addr}"))?;
    let mut stream = TcpStream::connect_timeout(&socket, Duration::from_secs(config.timeout_sec))
        .with_context(|| format!("failed to connect to {addr}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(config.timeout_sec)))?;
    stream.set_write_timeout(Some(Duration::from_secs(config.timeout_sec)))?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
        uri.path_and_query, uri.host_header
    );
    stream.write_all(request.as_bytes())?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    parse_http_response(&bytes, binary)
}

fn parse_http_response(bytes: &[u8], binary: bool) -> anyhow::Result<HttpProbe> {
    let separator = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("HTTP response missing header separator")?;
    let headers_raw = String::from_utf8_lossy(&bytes[..separator]);
    let body = &bytes[separator + 4..];
    let mut lines = headers_raw.lines();
    let status_line = lines.next().unwrap_or_default();
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    let mut content_type = String::new();
    let mut cache_control = String::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            match name.trim().to_ascii_lowercase().as_str() {
                "content-type" => content_type = value.trim().to_owned(),
                "cache-control" => cache_control = value.trim().to_owned(),
                _ => {}
            }
        }
    }
    Ok(HttpProbe {
        status_code,
        body: if binary {
            String::new()
        } else {
            String::from_utf8_lossy(body).to_string()
        },
        content_type,
        cache_control,
        body_length: body.len() as i64,
    })
}

fn write_skipped_report(config: &Config) -> anyhow::Result<()> {
    let report = BasicReport {
        schema_version: SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        status: "skipped",
        completion_claim_allowed: false,
        national_rollout_allowed: false,
        national_rollout_blocked_reason: "postgis_anchor_pbf_regional_proof_not_produced",
        evidence_paths: BasicEvidencePaths {
            postgis_anchor_pbf_regional_proof: repo_relative_path(
                &config.root,
                &config.prior_proof_path,
            ),
            output: repo_relative_path(&config.root, &config.output_path),
        },
        blockers: vec![
            "postgis/anchor/PBF regional proof evidence has not been produced".to_owned(),
        ],
        next_gates: vec![
            "postgis-anchor-pbf-regional-proof",
            "regional-data-serving-load",
            "explicit-national-rollout-approval",
        ],
        evidence_limitations: vec![
            "does_not_run_regional_load",
            "does_not_run_national_collection",
            "does_not_approve_production_cutover",
        ],
    };
    write_json_file(&config.output_path, &report)
}

fn write_prior_blocked_report(config: &Config, blockers: Vec<String>) -> anyhow::Result<()> {
    let report = BasicReport {
        schema_version: SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        status: "blocked",
        completion_claim_allowed: false,
        national_rollout_allowed: false,
        national_rollout_blocked_reason: "postgis_anchor_pbf_regional_proof_not_ready",
        evidence_paths: BasicEvidencePaths {
            postgis_anchor_pbf_regional_proof: repo_relative_path(
                &config.root,
                &config.prior_proof_path,
            ),
            output: repo_relative_path(&config.root, &config.output_path),
        },
        blockers,
        next_gates: vec![
            "postgis-anchor-pbf-regional-proof",
            "regional-data-serving-load",
        ],
        evidence_limitations: vec![
            "does_not_run_regional_load",
            "does_not_run_national_collection",
            "does_not_approve_production_cutover",
        ],
    };
    write_json_file(&config.output_path, &report)
}

fn print_blocked(config: &Config, blockers: Vec<String>) {
    println!(
        "regional-data-serving-load-blocked status=blocked blockers={} report={}",
        blockers.len(),
        config.output_path.display()
    );
    for blocker in blockers {
        println!("blocker={blocker}");
    }
}

fn metric_values<'a>(summary: &'a JsonValue, metric_name: &str) -> Option<&'a JsonValue> {
    let metric = summary.get("metrics")?.get(metric_name)?;
    metric.get("values").or(Some(metric))
}

fn json_f64_opt(
    value: Option<&JsonValue>,
    fields: &[&str],
    label: &str,
    blockers: &mut Vec<String>,
) -> f64 {
    let parsed = value.and_then(|value| {
        fields.iter().find_map(|field| {
            value
                .get(*field)
                .and_then(|value| value.as_f64().or_else(|| value.as_str()?.parse().ok()))
        })
    });
    if let Some(parsed) = parsed {
        parsed
    } else {
        blockers.push(format!("{label} must be numeric"));
        0.0
    }
}

fn metrics_contains_route(metrics_body: &str, route: &str) -> bool {
    metrics_body.contains(&format!(
        "foundation_api_http_requests_total{{method=\"GET\",route=\"{route}\",status=\"200\"}}"
    ))
}

fn join_api_uri(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn parse_http_uri(uri: &str) -> anyhow::Result<ParsedHttpUri> {
    let (scheme, rest) = uri
        .split_once("://")
        .context("ApiBaseUrl must be an absolute http(s) URL")?;
    let (authority, path_and_query) = rest
        .split_once('/')
        .map(|(authority, path)| (authority, format!("/{path}")))
        .unwrap_or((rest, "/".to_owned()));
    let (host, port) = parse_authority(authority, scheme)?;
    Ok(ParsedHttpUri {
        scheme: scheme.to_ascii_lowercase(),
        host_header: authority.to_owned(),
        host,
        port,
        path_and_query,
    })
}

fn parse_authority(authority: &str, scheme: &str) -> anyhow::Result<(String, u16)> {
    if authority.trim().is_empty() {
        bail!("ApiBaseUrl host is required");
    }
    if let Some((host, port)) = authority.rsplit_once(':') {
        if !host.contains(']') {
            return Ok((
                host.trim_matches(['[', ']']).to_owned(),
                port.parse().context("ApiBaseUrl port must be numeric")?,
            ));
        }
    }
    Ok((
        authority.trim_matches(&['[', ']'][..]).to_owned(),
        if scheme.eq_ignore_ascii_case("https") {
            443
        } else {
            80
        },
    ))
}

fn json_string(value: &JsonValue, field: &str) -> String {
    value
        .get(field)
        .and_then(|value| match value {
            JsonValue::String(text) => Some(text.to_owned()),
            JsonValue::Number(number) => Some(number.to_string()),
            JsonValue::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_bool(value: &JsonValue, field: &str, default: bool) -> bool {
    value
        .get(field)
        .and_then(|value| match value {
            JsonValue::Bool(flag) => Some(*flag),
            JsonValue::String(text) => text.parse().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn add_if(blockers: &mut Vec<String>, condition: bool, message: &str) {
    if condition {
        blockers.push(message.to_owned());
    }
}

struct Config {
    root: PathBuf,
    api_base_url: String,
    prior_proof_path: PathBuf,
    load_summary_path_raw: Option<PathBuf>,
    output_path: PathBuf,
    duration: String,
    read_rps: i64,
    health_rps: i64,
    timeout_sec: u64,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = repo_root()?;
        let api_base_url = env_string(
            "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_API_BASE_URL",
            DEFAULT_API_BASE_URL,
        )?;
        validate_api_base_url(&api_base_url)?;
        let read_rps = env_i64(
            "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_READ_RPS",
            DEFAULT_READ_RPS,
        )?;
        let health_rps = env_i64(
            "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_HEALTH_RPS",
            DEFAULT_HEALTH_RPS,
        )?;
        if read_rps < 1 {
            bail!("ReadRps must be positive");
        }
        if health_rps < 1 {
            bail!("HealthRps must be positive");
        }

        Ok(Self {
            prior_proof_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_POSTGIS_ANCHOR_PBF_PROOF_PATH",
                    DEFAULT_PRIOR_PROOF_PATH,
                )?,
                "PostgisAnchorPbfProofPath",
                false,
            )?,
            load_summary_path_raw: env_optional_path(
                "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_SUMMARY_PATH",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_OUTPUT_PATH",
                    DEFAULT_OUTPUT_PATH,
                )?,
                "OutputPath",
                false,
            )?,
            duration: env_string(
                "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_DURATION",
                DEFAULT_DURATION,
            )?,
            timeout_sec: env_i64(
                "FOUNDATION_PLATFORM_REGIONAL_DATA_SERVING_LOAD_TIMEOUT_SEC",
                DEFAULT_TIMEOUT_SEC as i64,
            )? as u64,
            root,
            api_base_url,
            read_rps,
            health_rps,
        })
    }
}

fn validate_api_base_url(value: &str) -> anyhow::Result<()> {
    let (scheme, _) = value
        .split_once("://")
        .context("ApiBaseUrl must be an absolute http(s) URL")?;
    if !matches!(scheme, "http" | "https") {
        bail!("ApiBaseUrl must be an absolute http(s) URL");
    }
    Ok(())
}

fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    Ok(match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value,
        Ok(_) | Err(env::VarError::NotPresent) => default.to_owned(),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    })
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<i64>()
            .with_context(|| format!("{name} must be an integer")),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_optional_path(name: &str) -> anyhow::Result<Option<PathBuf>> {
    Ok(match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(PathBuf::from(value)),
        Ok(_) | Err(env::VarError::NotPresent) => None,
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    })
}

fn resolve_repo_path(
    root: &Path,
    path: &Path,
    label: &str,
    must_exist: bool,
) -> anyhow::Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("{label} is required");
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{label} must stay within Root");
    }
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    if !resolved.starts_with(root) {
        bail!("{label} must stay within Root");
    }
    if must_exist && !resolved.is_file() {
        bail!("{label} does not exist: {}", resolved.display());
    }
    Ok(resolved)
}

fn repo_root() -> anyhow::Result<PathBuf> {
    let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
    let root = fs::canonicalize(&root)
        .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
    Ok(normalize_windows_verbatim_path(root))
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    const VERBATIM_PREFIX: &str = r"\\?\";
    let raw = path.to_string_lossy();
    if let Some(stripped) = raw.strip_prefix(VERBATIM_PREFIX) {
        PathBuf::from(stripped)
    } else {
        path
    }
}

#[derive(Default)]
struct LoadMetrics {
    failed_rate: f64,
    p95: f64,
    p99: f64,
    checks_rate: f64,
}

struct HttpProbe {
    status_code: i32,
    body: String,
    content_type: String,
    cache_control: String,
    body_length: i64,
}

struct ParsedHttpUri {
    scheme: String,
    host_header: String,
    host: String,
    port: u16,
    path_and_query: String,
}

#[derive(Serialize)]
struct BasicReport {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
    national_rollout_blocked_reason: &'static str,
    evidence_paths: BasicEvidencePaths,
    blockers: Vec<String>,
    next_gates: Vec<&'static str>,
    evidence_limitations: Vec<&'static str>,
}

#[derive(Serialize)]
struct BasicEvidencePaths {
    postgis_anchor_pbf_regional_proof: String,
    output: String,
}

#[derive(Serialize)]
struct FullReport {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: String,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
    national_rollout_blocked_reason: &'static str,
    api_base_url: String,
    scope: &'static str,
    evidence_paths: FullEvidencePaths,
    load: LoadReport,
    marker_contract: MarkerContractReport,
    marker_tile: MarkerTileReport,
    metrics: MetricsSummary,
    blockers: Vec<String>,
    next_gates: Vec<&'static str>,
    evidence_limitations: Vec<&'static str>,
}

#[derive(Serialize)]
struct FullEvidencePaths {
    postgis_anchor_pbf_regional_proof: String,
    load_summary: String,
    output: String,
}

#[derive(Serialize)]
struct LoadReport {
    duration: String,
    read_rps: i64,
    health_rps: i64,
    error_rate: f64,
    p95_ms: f64,
    p99_ms: f64,
    checks_rate: f64,
    load_summary_provided: bool,
}

#[derive(Serialize)]
struct MarkerContractReport {
    endpoint: &'static str,
    http_status: i32,
    response: JsonValue,
    content_type: String,
}

#[derive(Serialize)]
struct MarkerTileReport {
    endpoint: &'static str,
    http_status: i32,
    content_type: String,
    cache_control: String,
    body_length: i64,
}

#[derive(Serialize)]
struct MetricsSummary {
    http_status: i32,
    database_ready: bool,
    marker_contract_route_seen: bool,
    parcel_anchor_tile_route_seen: bool,
}

#[cfg(test)]
mod tests {
    use super::{validate_probe_responses, HttpProbe};

    #[test]
    fn probe_validation_rejects_wrong_health_service_identity() {
        let mut blockers = Vec::new();
        validate_probe_responses(
            &mut blockers,
            &HttpProbe {
                status_code: 200,
                body: r#"{"status":"ok","project":"newoncity","environment":"dev"}"#.to_owned(),
                content_type: "application/json".to_owned(),
                cache_control: String::new(),
                body_length: 57,
            },
            &HttpProbe {
                status_code: 200,
                body: r#"{"service":"foundation-api","status":"ready","database":"ok"}"#.to_owned(),
                content_type: "application/json".to_owned(),
                cache_control: String::new(),
                body_length: 64,
            },
            &HttpProbe {
                status_code: 200,
                body: String::new(),
                content_type: "application/json".to_owned(),
                cache_control: String::new(),
                body_length: 128,
            },
            &HttpProbe {
                status_code: 200,
                body: String::new(),
                content_type: "application/x-protobuf".to_owned(),
                cache_control: "public, max-age=30".to_owned(),
                body_length: 42,
            },
        );
        assert!(blockers
            .iter()
            .any(|blocker| blocker == "health probe service must be foundation-api"));
    }
}
