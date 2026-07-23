use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    public_data_control_support::{
        env_path, optional_env_value, read_json, repo_relative_path, required_env_value,
        write_json_file,
    },
    public_provider_rate_policy::{
        is_throttle_signal, new_lane_state, update_lane_state, LanePolicy, LaneState,
        ProviderOutcome, ProviderRatePolicyDocument,
    },
};

const DEFAULT_POLICY_PATH: &str = "docs/catalog/provider-rate-policy.v1.json";

pub fn run() -> anyhow::Result<()> {
    let config = ControllerConfig::from_env()?;
    let policy: ProviderRatePolicyDocument =
        serde_json::from_value(read_json(&config.policy_path, "provider rate policy")?)
            .context("failed to parse provider rate policy")?;
    let lane = find_lane(&policy, &config.lane_id)?;

    match config.mode {
        ControllerMode::Initialize => {
            let state = new_lane_state(lane)?;
            emit_result(&config, &state)?;
        }
        ControllerMode::Update => {
            let state_path = config
                .state_path
                .as_ref()
                .context("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_STATE_PATH is required")?;
            let state: LaneState =
                serde_json::from_value(read_json(state_path, "provider rate lane state")?)
                    .context("failed to parse provider rate lane state")?;
            let outcome = config
                .outcome
                .context("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_OUTCOME is required")?;
            let observed_at_utc = config.observed_at_utc.context(
                "FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_OBSERVED_AT_UTC is required",
            )?;
            let next =
                update_lane_state(lane, &state, outcome, config.latency_ms, observed_at_utc)?;
            emit_result(&config, &next)?;
        }
        ControllerMode::DetectThrottle => {
            let result = ThrottleSignalResult {
                lane_id: lane.lane_id.clone(),
                is_throttle_signal: is_throttle_signal(
                    lane,
                    config.http_status_code,
                    &config.provider_error_code,
                    &config.body_text,
                ),
            };
            emit_result(&config, &result)?;
        }
    }

    if let Some(output_path) = &config.output_path {
        writeln!(
            io::stdout().lock(),
            "provider-rate-controller-ok mode={} output={}",
            config.mode.as_str(),
            repo_relative_path(&config.root, output_path)
        )?;
    }
    Ok(())
}

struct ControllerConfig {
    root: PathBuf,
    policy_path: PathBuf,
    mode: ControllerMode,
    lane_id: String,
    state_path: Option<PathBuf>,
    outcome: Option<ProviderOutcome>,
    latency_ms: u32,
    observed_at_utc: Option<DateTime<Utc>>,
    http_status_code: u16,
    provider_error_code: String,
    body_text: String,
    output_path: Option<PathBuf>,
}

impl ControllerConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        let policy_path = resolve_existing_repo_path(
            &root,
            &env_path(
                "FOUNDATION_PLATFORM_PROVIDER_RATE_POLICY_PATH",
                DEFAULT_POLICY_PATH,
            )?,
            "policy path",
        )?;
        let mode = optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_MODE")?
            .unwrap_or_else(|| "initialize".to_owned())
            .parse()?;
        let lane_id = required_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_LANE_ID")?;
        let state_path =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_STATE_PATH")?
                .map(PathBuf::from)
                .map(|path| resolve_existing_repo_path(&root, &path, "state path"))
                .transpose()?;
        let outcome = optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_OUTCOME")?
            .map(|value| value.parse())
            .transpose()?;
        let latency_ms =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_LATENCY_MS")?
                .map(|value| parse_u32(&value, "latency ms"))
                .transpose()?
                .unwrap_or(0);
        let observed_at_utc =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_OBSERVED_AT_UTC")?
                .map(|value| parse_timestamp(&value))
                .transpose()?;
        let http_status_code =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_HTTP_STATUS_CODE")?
                .map(|value| parse_u16(&value, "HTTP status code"))
                .transpose()?
                .unwrap_or(0);
        let provider_error_code =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_PROVIDER_ERROR_CODE")?
                .unwrap_or_default();
        let body_text =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_BODY_TEXT")?
                .unwrap_or_default();
        let output_path =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_RATE_CONTROLLER_OUTPUT_PATH")?
                .map(PathBuf::from)
                .map(|path| resolve_output_repo_path(&root, &path, "output path"))
                .transpose()?;

        Ok(Self {
            root,
            policy_path,
            mode,
            lane_id,
            state_path,
            outcome,
            latency_ms,
            observed_at_utc,
            http_status_code,
            provider_error_code,
            body_text,
            output_path,
        })
    }
}

fn resolve_existing_repo_path(
    root: &std::path::Path,
    path: &std::path::Path,
    label: &str,
) -> anyhow::Result<PathBuf> {
    reject_parent_segments(path, label)?;
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let canonical = fs::canonicalize(&resolved)
        .with_context(|| format!("failed to resolve {label} {}", resolved.display()))?;
    if !canonical.starts_with(root) {
        bail!("{label} must stay within repo root");
    }
    Ok(canonical)
}

fn resolve_output_repo_path(
    root: &std::path::Path,
    path: &std::path::Path,
    label: &str,
) -> anyhow::Result<PathBuf> {
    reject_parent_segments(path, label)?;
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let parent = resolved
        .parent()
        .with_context(|| format!("{label} must have a parent directory"))?;
    let file_name = resolved
        .file_name()
        .with_context(|| format!("{label} must have a file name"))?;
    let canonical_parent = if parent.exists() {
        fs::canonicalize(parent)
            .with_context(|| format!("failed to resolve {label} parent {}", parent.display()))?
    } else {
        parent.to_path_buf()
    };
    if parent.exists() && !canonical_parent.starts_with(root) {
        bail!("{label} must stay within repo root");
    }
    Ok(canonical_parent.join(file_name))
}

fn reject_parent_segments(path: &std::path::Path, label: &str) -> anyhow::Result<()> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{label} must not contain parent directory segments");
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ControllerMode {
    Initialize,
    Update,
    DetectThrottle,
}

impl ControllerMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Initialize => "initialize",
            Self::Update => "update",
            Self::DetectThrottle => "detect-throttle",
        }
    }
}

impl std::str::FromStr for ControllerMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value.trim() {
            "initialize" => Ok(Self::Initialize),
            "update" => Ok(Self::Update),
            "detect-throttle" => Ok(Self::DetectThrottle),
            other => bail!("unknown provider rate controller mode '{other}'"),
        }
    }
}

impl std::str::FromStr for ProviderOutcome {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value.trim() {
            "success" => Ok(Self::Success),
            "throttle" => Ok(Self::Throttle),
            "timeout" => Ok(Self::Timeout),
            "error" => Ok(Self::Error),
            other => bail!("unknown provider outcome '{other}'"),
        }
    }
}

fn find_lane<'a>(
    policy: &'a ProviderRatePolicyDocument,
    lane_id: &str,
) -> anyhow::Result<&'a LanePolicy> {
    policy
        .lanes
        .iter()
        .find(|lane| lane.lane_id == lane_id)
        .with_context(|| format!("provider rate lane missing: {lane_id}"))
}

fn emit_result<T: Serialize>(config: &ControllerConfig, value: &T) -> anyhow::Result<()> {
    if let Some(path) = &config.output_path {
        write_json_file(path, value)
    } else {
        writeln!(
            io::stdout().lock(),
            "{}",
            serde_json::to_string_pretty(value)
                .context("failed to serialize provider rate controller result")?
        )?;
        Ok(())
    }
}

fn parse_timestamp(value: &str) -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid observed timestamp: {value}"))?
        .with_timezone(&Utc))
}

fn parse_u32(value: &str, label: &str) -> anyhow::Result<u32> {
    value
        .parse::<u32>()
        .with_context(|| format!("{label} must be an unsigned integer"))
}

fn parse_u16(value: &str, label: &str) -> anyhow::Result<u16> {
    value
        .parse::<u16>()
        .with_context(|| format!("{label} must be an unsigned integer"))
}

#[derive(Serialize)]
struct ThrottleSignalResult {
    lane_id: String,
    is_throttle_signal: bool,
}
