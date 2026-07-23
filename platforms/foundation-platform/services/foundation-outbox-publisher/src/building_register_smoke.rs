use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use serde_json::Value as JsonValue;

use crate::{
    public_api_metric_writer,
    public_data_control_support::{env_path, resolve_repo_path},
};

const BASE_URI: &str = "https://apis.data.go.kr/1613000/BldRgstHubService";
const PREFIX: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE";
const MODE: &str = "read_only_smoke";

pub fn run() -> anyhow::Result<()> {
    let config = SmokeConfig::from_env()?;
    if config.show_required {
        println!("DATA_GO_KR_SERVICE_KEY");
        return Ok(());
    }

    if !config.live_call {
        write_setup_instructions();
        return Ok(());
    }
    if !config.confirm_public_api_quota_impact {
        bail!("Public API quota impact must be confirmed with -ConfirmPublicApiQuotaImpact");
    }
    config.validate()?;

    let service_key = required_env("DATA_GO_KR_SERVICE_KEY")?;
    let uri = config.smoke_uri(&service_key);
    write_quota_metric(
        &config.quota_metrics_path,
        "data.go.kr",
        &config.operation,
        1,
    )?;

    let started = Instant::now();
    let total_count = match run_live_smoke(&config, &uri) {
        Ok(total_count) => {
            write_dependency_metric(
                &config.quota_metrics_path,
                "data.go.kr",
                &config.operation,
                started.elapsed(),
                "succeeded",
                None,
            )?;
            total_count
        }
        Err(error) => {
            write_dependency_metric(
                &config.quota_metrics_path,
                "data.go.kr",
                &config.operation,
                started.elapsed(),
                "failed",
                Some("smoke_error"),
            )?;
            return Err(error);
        }
    };

    println!(
        "data-go-kr-building-register-smoke-ok operation={} sigunguCd={} bjdongCd={} totalCount={total_count}",
        config.operation, config.sigungu_cd, config.bjdong_cd
    );
    println!("No secret values or raw payload were printed.");
    Ok(())
}

struct SmokeConfig {
    root: PathBuf,
    operation: String,
    sigungu_cd: String,
    bjdong_cd: String,
    page_no: i64,
    num_of_rows: i64,
    live_call: bool,
    confirm_public_api_quota_impact: bool,
    quota_metrics_path: Option<PathBuf>,
    show_required: bool,
    curl_exe: String,
}

impl SmokeConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let quota_metrics_path = env_string(&format!("{PREFIX}_QUOTA_METRICS_PATH"), "")?;
        let quota_metrics_path = if quota_metrics_path.trim().is_empty() {
            None
        } else {
            Some(resolve_repo_path(
                &root,
                Path::new(&quota_metrics_path),
                "QuotaMetricsPath",
            )?)
        };

        Ok(Self {
            root,
            operation: env_string(&format!("{PREFIX}_OPERATION"), "getBrTitleInfo")?,
            sigungu_cd: env_string(&format!("{PREFIX}_SIGUNGU_CD"), "")?,
            bjdong_cd: env_string(&format!("{PREFIX}_BJDONG_CD"), "")?,
            page_no: env_i64(&format!("{PREFIX}_PAGE_NO"), 1)?,
            num_of_rows: env_i64(&format!("{PREFIX}_NUM_OF_ROWS"), 1)?,
            live_call: env_bool(&format!("{PREFIX}_LIVE_CALL"), false)?,
            confirm_public_api_quota_impact: env_bool(
                &format!("{PREFIX}_CONFIRM_PUBLIC_API_QUOTA_IMPACT"),
                false,
            )?,
            quota_metrics_path,
            show_required: env_bool(&format!("{PREFIX}_SHOW_REQUIRED"), false)?,
            curl_exe: env_string(&format!("{PREFIX}_CURL_EXE"), "")?,
        })
    }

    fn validate(&self) -> anyhow::Result<()> {
        if !simple_identifier(&self.operation) {
            bail!(
                "Operation must be a simple API operation identifier: {}",
                self.operation
            );
        }
        if !fixed_digits(&self.sigungu_cd, 5) {
            bail!("SigunguCd must be exactly 5 digits: {}", self.sigungu_cd);
        }
        if !fixed_digits(&self.bjdong_cd, 5) {
            bail!("BjdongCd must be exactly 5 digits: {}", self.bjdong_cd);
        }
        if self.page_no < 1 {
            bail!("PageNo must be greater than zero");
        }
        if self.num_of_rows < 1 {
            bail!("NumOfRows must be greater than zero");
        }
        Ok(())
    }

    fn smoke_uri(&self, service_key: &str) -> String {
        let service_key = service_key_for_query(service_key);
        format!(
            "{BASE_URI}/{}?serviceKey={service_key}&sigunguCd={}&bjdongCd={}&pageNo={}&numOfRows={}&_type=json",
            self.operation,
            url_encode_component(&self.sigungu_cd),
            url_encode_component(&self.bjdong_cd),
            self.page_no,
            self.num_of_rows
        )
    }
}

fn write_setup_instructions() {
    println!("Run a read-only building register API smoke against data.go.kr:");
    println!();
    println!("  DATA_GO_KR_SERVICE_KEY=<decoded-service-key> \\");
    println!("  FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE_LIVE_CALL=true \\");
    println!(
        "  FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE_CONFIRM_PUBLIC_API_QUOTA_IMPACT=true \\"
    );
    println!("  FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE_SIGUNGU_CD=<5-digit-code> \\");
    println!("  FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE_BJDONG_CD=<5-digit-code> \\");
    println!("  FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE_QUOTA_METRICS_PATH=target/public-api-quota.prom \\");
    println!("  cargo run -p foundation-outbox-publisher -- building-register-smoke");
    println!();
    println!("Default operation is getBrTitleInfo with JSON output.");
    println!(
        "No live API is touched unless live-call and quota-impact confirmation are both present."
    );
    println!("Requires curl on PATH (or FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE_CURL_EXE).");
}

fn run_live_smoke(config: &SmokeConfig, uri: &str) -> anyhow::Result<i64> {
    let payload = invoke_data_go_kr_json(config, uri)?;
    let response = payload
        .get("response")
        .context("data.go.kr response omitted response envelope")?;
    let header = response
        .get("header")
        .context("data.go.kr response omitted response.header")?;
    let result_code = string_property(header, "resultCode");
    let result_msg = string_property(header, "resultMsg");
    if result_code != "00" {
        bail!(
            "data.go.kr building register smoke failed with resultCode={result_code} resultMsg={result_msg}"
        );
    }

    Ok(response
        .get("body")
        .and_then(|body| body.get("totalCount"))
        .and_then(json_i64)
        .unwrap_or(0))
}

fn invoke_data_go_kr_json(config: &SmokeConfig, uri: &str) -> anyhow::Result<JsonValue> {
    let curl = resolve_curl(&config.curl_exe)?;
    let mut command = Command::new(&curl);
    command.current_dir(&config.root).args([
        "-sS",
        "-L",
        "-A",
        "foundation-platform-data-go-kr-smoke/1.0",
        uri,
    ]);
    let output = command
        .output()
        .with_context(|| format!("failed to invoke curl executable {}", curl.display()))?;
    if !output.status.success() {
        bail!(
            "data.go.kr HTTP request failed with curl exit code {}",
            output.status.code().unwrap_or(1)
        );
    }
    if output.stdout.is_empty() {
        bail!("data.go.kr response body was empty");
    }
    serde_json::from_slice(&output.stdout).context("data.go.kr response body was not valid JSON")
}

fn required_env(name: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        Ok(_) | Err(env::VarError::NotPresent) => {
            bail!("Missing required environment variable: {name}")
        }
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn resolve_curl(explicit: &str) -> anyhow::Result<PathBuf> {
    if !explicit.trim().is_empty() {
        return Ok(PathBuf::from(explicit));
    }
    if let Some(path) = env::var_os("PATH") {
        for dir in env::split_paths(&path) {
            for candidate in ["curl.exe", "curl"] {
                let path = dir.join(candidate);
                if path.is_file() {
                    return Ok(path);
                }
            }
        }
    }
    bail!("curl is required for this smoke; install it on PATH or set FOUNDATION_PLATFORM_BUILDING_REGISTER_SMOKE_CURL_EXE")
}

fn write_quota_metric(
    path: &Option<PathBuf>,
    provider: &str,
    endpoint: &str,
    count: i64,
) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    public_api_metric_writer::write_quota_metric(path, provider, endpoint, count, "attempted", MODE)
}

fn write_dependency_metric(
    path: &Option<PathBuf>,
    provider: &str,
    endpoint: &str,
    duration: Duration,
    outcome: &str,
    error_kind: Option<&str>,
) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    public_api_metric_writer::write_dependency_metric_duration(
        path, provider, endpoint, duration, outcome, MODE, error_kind,
    )
}

fn service_key_for_query(service_key: &str) -> String {
    if contains_percent_escape(service_key) {
        service_key.to_owned()
    } else {
        url_encode_component(service_key)
    }
}

fn contains_percent_escape(value: &str) -> bool {
    value.as_bytes().windows(3).any(|window| {
        window[0] == b'%' && window[1].is_ascii_hexdigit() && window[2].is_ascii_hexdigit()
    })
}

fn url_encode_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        // Present-but-empty behaves like unset, so callers can pass empty to mean default.
        Ok(_) | Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    env_string(name, &default.to_string())?
        .parse::<i64>()
        .with_context(|| format!("invalid {name} environment variable"))
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => bail!("invalid {name} environment variable"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn string_property(value: &JsonValue, name: &str) -> String {
    value
        .get(name)
        .map(|property| match property {
            JsonValue::String(text) => text.clone(),
            JsonValue::Null => String::new(),
            JsonValue::Bool(flag) => flag.to_string(),
            JsonValue::Number(number) => number.to_string(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

fn json_i64(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::Number(number) => number.as_i64(),
        JsonValue::String(text) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn simple_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_alphabetic() && bytes.all(|byte| byte.is_ascii_alphanumeric())
}

fn fixed_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let value = path.to_string_lossy();
        if let Some(rest) = value.strip_prefix("\\\\?\\UNC\\") {
            return PathBuf::from(format!("\\\\{rest}"));
        }
        if let Some(rest) = value.strip_prefix("\\\\?\\") {
            return PathBuf::from(rest);
        }
    }
    path
}
