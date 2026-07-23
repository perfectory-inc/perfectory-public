use std::{
    env, fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use anyhow::{bail, Context};

use crate::public_data_control_support::optional_env_value;

const PREFIX: &str = "FOUNDATION_PLATFORM_PUBLIC_API_METRIC_WRITER";
const QUOTA_METRIC: MetricHeader = MetricHeader {
    name: "foundation_platform_public_api_quota_request_total",
    help: "Public API quota-impacting requests attempted by Foundation Platform smoke tooling.",
    kind: "counter",
};
const DURATION_METRIC: MetricHeader = MetricHeader {
    name: "foundation_platform_public_api_dependency_request_duration_seconds",
    help: "Wall-clock duration observed by Foundation Platform public API smoke tooling.",
    kind: "gauge",
};
const ERROR_METRIC: MetricHeader = MetricHeader {
    name: "foundation_platform_public_api_dependency_error_total",
    help: "Public API smoke dependency errors observed by Foundation Platform tooling.",
    kind: "counter",
};

pub fn run_quota() -> anyhow::Result<()> {
    let Some(path) = metric_path()? else {
        return Ok(());
    };
    let metric = QuotaMetric {
        provider: required_label("PROVIDER")?,
        endpoint: required_label("ENDPOINT")?,
        request_count: required_i64("REQUEST_COUNT")?,
        outcome: optional_label("OUTCOME", "attempted")?,
        mode: optional_label("MODE", "read_only_smoke")?,
    };
    if metric.request_count < 0 {
        bail!("RequestCount must be non-negative");
    }

    write_quota_metric(
        &path,
        &metric.provider,
        &metric.endpoint,
        metric.request_count,
        &metric.outcome,
        &metric.mode,
    )?;
    if !quiet()? {
        println!(
            "public-api-quota-metric-ok provider={} endpoint={} request_count={}",
            metric.provider, metric.endpoint, metric.request_count
        );
    }
    Ok(())
}

pub fn run_dependency() -> anyhow::Result<()> {
    let Some(path) = metric_path()? else {
        return Ok(());
    };
    let metric = DependencyMetric {
        provider: required_label("PROVIDER")?,
        endpoint: required_label("ENDPOINT")?,
        duration_seconds: required_nonnegative_f64("DURATION_SECONDS")?,
        outcome: optional_label("OUTCOME", "succeeded")?,
        mode: optional_label("MODE", "read_only_smoke")?,
        error_kind: optional_env_value(&key("ERROR_KIND"))?
            .map(|value| required_label_value("ERROR_KIND", value))
            .transpose()?,
    };

    write_dependency_metric_seconds(
        &path,
        &metric.provider,
        &metric.endpoint,
        metric.duration_seconds,
        &metric.outcome,
        &metric.mode,
        metric.error_kind.as_deref(),
    )?;
    if !quiet()? {
        println!(
            "public-api-dependency-metric-ok provider={} endpoint={} outcome={}",
            metric.provider, metric.endpoint, metric.outcome
        );
    }
    Ok(())
}

pub(crate) fn write_quota_metric(
    path: &Path,
    provider: &str,
    endpoint: &str,
    request_count: i64,
    outcome: &str,
    mode: &str,
) -> anyhow::Result<()> {
    if request_count < 0 {
        bail!("RequestCount must be non-negative");
    }
    let provider = required_label_value("Provider", provider.to_owned())?;
    let endpoint = required_label_value("Endpoint", endpoint.to_owned())?;
    let outcome = required_label_value("Outcome", outcome.to_owned())?;
    let mode = required_label_value("Mode", mode.to_owned())?;

    append_metric_lines(
        path,
        &[QUOTA_METRIC],
        &[format!(
            "{}{{provider=\"{}\",endpoint=\"{}\",outcome=\"{}\",mode=\"{}\"}} {}",
            QUOTA_METRIC.name,
            prometheus_label(&provider),
            prometheus_label(&endpoint),
            prometheus_label(&outcome),
            prometheus_label(&mode),
            request_count
        )],
    )
}

pub(crate) fn write_dependency_metric_duration(
    path: &Path,
    provider: &str,
    endpoint: &str,
    duration: Duration,
    outcome: &str,
    mode: &str,
    error_kind: Option<&str>,
) -> anyhow::Result<()> {
    write_dependency_metric_seconds(
        path,
        provider,
        endpoint,
        duration.as_secs_f64(),
        outcome,
        mode,
        error_kind,
    )
}

fn write_dependency_metric_seconds(
    path: &Path,
    provider: &str,
    endpoint: &str,
    duration_seconds: f64,
    outcome: &str,
    mode: &str,
    error_kind: Option<&str>,
) -> anyhow::Result<()> {
    if !duration_seconds.is_finite() || duration_seconds < 0.0 {
        bail!("DurationSeconds must be a non-negative finite number");
    }
    let provider = required_label_value("Provider", provider.to_owned())?;
    let endpoint = required_label_value("Endpoint", endpoint.to_owned())?;
    let outcome = required_label_value("Outcome", outcome.to_owned())?;
    let mode = required_label_value("Mode", mode.to_owned())?;
    let error_kind = error_kind
        .map(|value| required_label_value("ErrorKind", value.to_owned()))
        .transpose()?;

    let mut sample_lines = vec![format!(
        "{}{{provider=\"{}\",endpoint=\"{}\",outcome=\"{}\",mode=\"{}\"}} {}",
        DURATION_METRIC.name,
        prometheus_label(&provider),
        prometheus_label(&endpoint),
        prometheus_label(&outcome),
        prometheus_label(&mode),
        prometheus_number(duration_seconds)
    )];
    let mut headers = vec![DURATION_METRIC];
    if let Some(error_kind) = &error_kind {
        headers.push(ERROR_METRIC);
        sample_lines.push(format!(
            "{}{{provider=\"{}\",endpoint=\"{}\",error_kind=\"{}\",mode=\"{}\"}} 1",
            ERROR_METRIC.name,
            prometheus_label(&provider),
            prometheus_label(&endpoint),
            prometheus_label(error_kind),
            prometheus_label(&mode)
        ));
    }

    append_metric_lines(path, &headers, &sample_lines)
}

struct QuotaMetric {
    provider: String,
    endpoint: String,
    request_count: i64,
    outcome: String,
    mode: String,
}

struct DependencyMetric {
    provider: String,
    endpoint: String,
    duration_seconds: f64,
    outcome: String,
    mode: String,
    error_kind: Option<String>,
}

#[derive(Clone, Copy)]
struct MetricHeader {
    name: &'static str,
    help: &'static str,
    kind: &'static str,
}

fn append_metric_lines(
    path: &Path,
    headers: &[MetricHeader],
    sample_lines: &[String],
) -> anyhow::Result<()> {
    if sample_lines.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create metric directory {}", parent.display())
            })?;
        }
    }

    let _guard = MetricFileLock::acquire(path)?;
    let existing = fs::read_to_string(path).unwrap_or_default();
    let mut lines = Vec::new();
    for header in headers {
        if !existing.contains(&format!("# HELP {} ", header.name)) {
            lines.push(format!("# HELP {} {}", header.name, header.help));
            lines.push(format!("# TYPE {} {}", header.name, header.kind));
        }
    }
    lines.extend(sample_lines.iter().cloned());

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open metric file {}", path.display()))?;
    file.write_all((lines.join("\n") + "\n").as_bytes())
        .with_context(|| format!("failed to append metric file {}", path.display()))
}

struct MetricFileLock {
    path: PathBuf,
}

impl MetricFileLock {
    fn acquire(metric_path: &Path) -> anyhow::Result<Self> {
        let lock_path = lock_path(metric_path);
        for attempt in 0..10 {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(_) => return Ok(Self { path: lock_path }),
                Err(error) if error.kind() == ErrorKind::AlreadyExists && attempt < 9 => {
                    thread::sleep(Duration::from_millis(50 * (attempt + 1) as u64));
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to acquire metric lock {}", lock_path.display())
                    });
                }
            }
        }
        bail!("failed to acquire metric lock {}", lock_path.display())
    }
}

impl Drop for MetricFileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn lock_path(metric_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.lock", metric_path.to_string_lossy()))
}

fn metric_path() -> anyhow::Result<Option<PathBuf>> {
    optional_env_value(&key("PATH"))?
        .map(|raw| {
            let path = PathBuf::from(raw.trim());
            if path.as_os_str().is_empty() {
                bail!("Path must not be blank");
            }
            Ok(if path.is_absolute() {
                path
            } else {
                env::current_dir()
                    .context("failed to resolve current directory")?
                    .join(path)
            })
        })
        .transpose()
}

fn required_label(name: &str) -> anyhow::Result<String> {
    let value = optional_env_value(&key(name))?
        .map_or_else(|| bail!("{name} is required for public API metric"), Ok)?;
    required_label_value(name, value)
}

fn optional_label(name: &str, default: &str) -> anyhow::Result<String> {
    optional_env_value(&key(name))?
        .map(|value| required_label_value(name, value))
        .transpose()
        .map(|value| value.unwrap_or_else(|| default.to_owned()))
}

fn required_label_value(name: &str, value: String) -> anyhow::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{name} must not be blank");
    }
    Ok(value.to_owned())
}

fn required_i64(name: &str) -> anyhow::Result<i64> {
    let raw = optional_env_value(&key(name))?
        .map_or_else(|| bail!("{name} is required for public API metric"), Ok)?;
    raw.trim()
        .parse::<i64>()
        .with_context(|| format!("{name} must be an integer"))
}

fn required_nonnegative_f64(name: &str) -> anyhow::Result<f64> {
    let raw = optional_env_value(&key(name))?
        .map_or_else(|| bail!("{name} is required for public API metric"), Ok)?;
    let value = raw
        .trim()
        .parse::<f64>()
        .with_context(|| format!("{name} must be a number"))?;
    if !value.is_finite() || value < 0.0 {
        bail!("{name} must be a non-negative finite number");
    }
    Ok(value)
}

fn quiet() -> anyhow::Result<bool> {
    let Some(raw) = optional_env_value(&key("QUIET"))? else {
        return Ok(false);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => bail!("QUIET must be a boolean"),
    }
}

fn prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn prometheus_number(value: f64) -> String {
    let mut text = format!("{value:.6}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn key(name: &str) -> String {
    format!("{PREFIX}_{name}")
}
