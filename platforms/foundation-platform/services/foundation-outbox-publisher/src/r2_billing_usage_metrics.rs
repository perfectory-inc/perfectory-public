use std::{
    collections::{BTreeMap, HashSet},
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value;

use crate::r2_command_support::{optional_env, read_json, utc_now, write_json_file};

const PREFIX: &str = "FOUNDATION_PLATFORM_R2_BILLING_USAGE_METRICS";
const REPORT_SCHEMA_VERSION: &str = "foundation-platform.r2_billing_usage.v1";
const DEFAULT_OUTPUT_DIR: &str = "target/r2-billing-usage-metrics";
const REPORT_FILE_NAME: &str = "r2-billing-usage-report.json";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let rows = read_billing_rows(&config)?;
    let entries = billing_entries(rows, config.bucket_name.as_deref())?;
    if entries.is_empty() {
        bail!("No R2 billing rows matched the requested filter.");
    }

    fs::create_dir_all(&config.output_dir).with_context(|| {
        format!(
            "failed to create billing usage output directory {}",
            config.output_dir.display()
        )
    })?;
    let report = build_usage_report(&entries, config.bucket_name.as_deref().unwrap_or_default());
    let report_path = config.output_dir.join(REPORT_FILE_NAME);
    write_json_file(&report_path, &report)?;
    if let Some(metrics_path) = &config.metrics_path {
        write_usage_metrics(metrics_path, &report)?;
    }

    write_usage_summary(
        config.quiet,
        &report_path,
        config.metrics_path.as_deref(),
        &report,
    )?;
    Ok(())
}

struct Config {
    input_csv: Option<PathBuf>,
    input_json: Option<PathBuf>,
    output_dir: PathBuf,
    metrics_path: Option<PathBuf>,
    bucket_name: Option<String>,
    quiet: bool,
}

#[derive(Debug)]
struct BillingRow {
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecimalAmount {
    units: i128,
    scale: u32,
}

#[derive(Debug)]
struct BillingEntry {
    bucket: String,
    operation: String,
    operation_class: &'static str,
    request_count: i64,
    usage_bytes: i64,
    cost_usd: DecimalAmount,
    currency: String,
}

#[derive(Serialize)]
struct UsageReport {
    schema_version: &'static str,
    generated_at_utc: String,
    source: &'static str,
    bucket_filter: String,
    bucket_count: usize,
    operation_class_count: usize,
    total_request_count: i64,
    total_usage_bytes: i64,
    total_cost_usd: DecimalAmount,
    usage: Vec<UsageAggregate>,
}

#[derive(Clone, Serialize)]
struct UsageAggregate {
    bucket: String,
    operation_class: &'static str,
    currency: String,
    request_count: i64,
    usage_bytes: i64,
    cost_usd: DecimalAmount,
    operations: Vec<String>,
}

impl Serialize for DecimalAmount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_plain_string())
    }
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let input_csv = optional_env(&key("INPUT_CSV"))?
            .map(|raw| resolve_path(&raw, "InputCsv"))
            .transpose()?;
        let input_json = optional_env(&key("INPUT_JSON"))?
            .map(|raw| resolve_path(&raw, "InputJson"))
            .transpose()?;
        if input_csv.is_some() && input_json.is_some() {
            bail!("Specify only one of -InputCsv or -InputJson.");
        }
        if input_csv.is_none() && input_json.is_none() {
            bail!("Specify -InputCsv or -InputJson.");
        }

        let output_dir = match optional_env(&key("OUTPUT_DIR"))? {
            Some(raw) => resolve_path(&raw, "OutputDir")?,
            None => resolve_path(DEFAULT_OUTPUT_DIR, "OutputDir")?,
        };

        Ok(Self {
            input_csv,
            input_json,
            output_dir,
            metrics_path: optional_env(&key("METRICS_PATH"))?
                .map(|raw| resolve_path(&raw, "MetricsPath"))
                .transpose()?,
            bucket_name: optional_env(&key("BUCKET_NAME"))?,
            quiet: env_bool("QUIET", false)?,
        })
    }
}

fn read_billing_rows(config: &Config) -> anyhow::Result<Vec<BillingRow>> {
    if let Some(input_csv) = &config.input_csv {
        if !input_csv.is_file() {
            bail!("InputCsv not found: {}", input_csv.display());
        }
        let bytes = fs::read(input_csv)
            .with_context(|| format!("failed to read InputCsv {}", input_csv.display()))?;
        let text = String::from_utf8(strip_utf8_bom(&bytes).to_vec())
            .with_context(|| format!("InputCsv must be UTF-8: {}", input_csv.display()))?;
        return parse_csv(&text);
    }

    let Some(input_json) = config.input_json.as_ref() else {
        bail!("Specify -InputCsv or -InputJson.");
    };
    if !input_json.is_file() {
        bail!("InputJson not found: {}", input_json.display());
    }
    let raw = read_json(input_json, "R2 billing usage JSON")?;
    rows_from_json(raw)
}

fn rows_from_json(raw: Value) -> anyhow::Result<Vec<BillingRow>> {
    let values = if let Some(rows) = raw.get("rows") {
        rows.as_array()
            .context("InputJson rows must be an array")?
            .clone()
    } else if let Some(usage) = raw.get("usage") {
        usage
            .as_array()
            .context("InputJson usage must be an array")?
            .clone()
    } else if let Some(array) = raw.as_array() {
        array.clone()
    } else {
        vec![raw]
    };

    values
        .into_iter()
        .map(|value| {
            let object = value
                .as_object()
                .context("billing usage JSON rows must be objects")?;
            Ok(BillingRow {
                fields: object
                    .iter()
                    .map(|(key, value)| (key.clone(), json_field_value(value)))
                    .collect(),
            })
        })
        .collect()
}

fn parse_csv(text: &str) -> anyhow::Result<Vec<BillingRow>> {
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let Some(header_line) = lines.next() else {
        bail!("InputCsv must contain a header row");
    };
    let headers = parse_csv_record(header_line)?;
    if headers.is_empty() {
        bail!("InputCsv must contain a header row");
    }

    let mut rows = Vec::new();
    for (index, line) in lines.enumerate() {
        let fields = parse_csv_record(line)
            .with_context(|| format!("failed to parse CSV data row {}", index + 2))?;
        if fields.len() != headers.len() {
            bail!(
                "CSV data row {} has {} fields, expected {}",
                index + 2,
                fields.len(),
                headers.len()
            );
        }
        rows.push(BillingRow {
            fields: headers.iter().cloned().zip(fields).collect(),
        });
    }
    Ok(rows)
}

fn parse_csv_record(line: &str) -> anyhow::Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.trim_start_matches('\u{feff}').chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(field.clone());
                field.clear();
            }
            _ => field.push(ch),
        }
    }
    if in_quotes {
        bail!("CSV row has an unterminated quoted field");
    }
    fields.push(field);
    Ok(fields)
}

fn billing_entries(
    rows: Vec<BillingRow>,
    bucket_filter: Option<&str>,
) -> anyhow::Result<Vec<BillingEntry>> {
    let mut entries = Vec::new();
    for row in rows {
        let service = field(&row, "service");
        if !service.trim().is_empty() && !service.eq_ignore_ascii_case("r2") {
            continue;
        }

        let bucket = field(&row, "bucket");
        if bucket.trim().is_empty() {
            bail!("billing export row missing bucket");
        }
        if bucket_filter.is_some_and(|filter| filter != bucket) {
            continue;
        }

        let operation = field(&row, "operation");
        if operation.trim().is_empty() {
            bail!("billing export row missing operation");
        }
        let currency = match field(&row, "currency").trim() {
            "" => "USD".to_owned(),
            value => value.to_owned(),
        };
        entries.push(BillingEntry {
            bucket: bucket.to_owned(),
            operation: operation.to_owned(),
            operation_class: operation_class(operation),
            request_count: parse_i64_field(field(&row, "request_count"), "request_count")?,
            usage_bytes: parse_i64_field(field(&row, "usage_bytes"), "usage_bytes")?,
            cost_usd: DecimalAmount::parse_nonnegative(field(&row, "cost_usd"), "cost_usd")?,
            currency,
        });
    }
    Ok(entries)
}

fn build_usage_report(entries: &[BillingEntry], bucket_filter: &str) -> UsageReport {
    let mut aggregates = BTreeMap::<(String, &'static str, String), UsageAggregate>::new();
    for entry in entries {
        let key = (
            entry.bucket.clone(),
            entry.operation_class,
            entry.currency.clone(),
        );
        let aggregate = aggregates.entry(key).or_insert_with(|| UsageAggregate {
            bucket: entry.bucket.clone(),
            operation_class: entry.operation_class,
            currency: entry.currency.clone(),
            request_count: 0,
            usage_bytes: 0,
            cost_usd: DecimalAmount::zero(),
            operations: Vec::new(),
        });
        aggregate.request_count += entry.request_count;
        aggregate.usage_bytes += entry.usage_bytes;
        aggregate.cost_usd.add_assign(&entry.cost_usd);
        if !aggregate.operations.contains(&entry.operation) {
            aggregate.operations.push(entry.operation.clone());
        }
    }

    let usage: Vec<_> = aggregates.into_values().collect();
    let bucket_count = usage
        .iter()
        .map(|entry| entry.bucket.as_str())
        .collect::<HashSet<_>>()
        .len();
    let operation_class_count = usage
        .iter()
        .map(|entry| entry.operation_class)
        .collect::<HashSet<_>>()
        .len();
    UsageReport {
        schema_version: REPORT_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        source: "cloudflare_billing_export",
        bucket_filter: bucket_filter.to_owned(),
        bucket_count,
        operation_class_count,
        total_request_count: usage.iter().map(|entry| entry.request_count).sum(),
        total_usage_bytes: usage.iter().map(|entry| entry.usage_bytes).sum(),
        total_cost_usd: usage
            .iter()
            .fold(DecimalAmount::zero(), |mut total, entry| {
                total.add_assign(&entry.cost_usd);
                total
            }),
        usage,
    }
}

fn write_usage_metrics(path: &Path, report: &UsageReport) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create metrics directory {}", parent.display())
            })?;
        }
    }
    let mut lines = vec![
        "# HELP foundation_platform_r2_billing_request_total R2 request count from the latest billing export.".to_owned(),
        "# TYPE foundation_platform_r2_billing_request_total counter".to_owned(),
        "# HELP foundation_platform_r2_billing_usage_bytes R2 usage bytes from the latest billing export.".to_owned(),
        "# TYPE foundation_platform_r2_billing_usage_bytes gauge".to_owned(),
        "# HELP foundation_platform_r2_billing_cost_usd R2 cost in USD from the latest billing export.".to_owned(),
        "# TYPE foundation_platform_r2_billing_cost_usd gauge".to_owned(),
    ];

    for entry in &report.usage {
        let labels = format!(
            "bucket=\"{}\",operation_class=\"{}\",currency=\"{}\"",
            prometheus_label(&entry.bucket),
            prometheus_label(entry.operation_class),
            prometheus_label(&entry.currency)
        );
        lines.push(format!(
            "foundation_platform_r2_billing_request_total{{{labels}}} {}",
            entry.request_count
        ));
        lines.push(format!(
            "foundation_platform_r2_billing_usage_bytes{{{labels}}} {}",
            entry.usage_bytes
        ));
        lines.push(format!(
            "foundation_platform_r2_billing_cost_usd{{{labels}}} {}",
            prometheus_number(&entry.cost_usd)
        ));
    }
    fs::write(path, lines.join("\n") + "\n")
        .with_context(|| format!("failed to write metrics file {}", path.display()))
}

fn operation_class(operation: &str) -> &'static str {
    let normalized = operation.to_ascii_lowercase();
    if normalized.contains("storage")
        || normalized.contains("byte-hour")
        || normalized.contains("gb-month")
    {
        return "storage";
    }
    if normalized.contains("class a")
        || normalized.contains("put")
        || normalized.contains("write")
        || normalized.contains("list")
        || normalized.contains("copy")
        || normalized.contains("delete")
        || normalized.contains("multipart")
    {
        return "class_a_write";
    }
    if normalized.contains("class b")
        || normalized.contains("get")
        || normalized.contains("read")
        || normalized.contains("head")
    {
        return "class_b_read";
    }
    "other"
}

fn parse_i64_field(raw: &str, field_name: &str) -> anyhow::Result<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    if let Some((whole, fraction)) = trimmed.split_once('.') {
        if !fraction.chars().all(|ch| ch == '0') {
            bail!("{field_name} must be an integer");
        }
        let value = whole
            .parse::<i64>()
            .with_context(|| format!("{field_name} must be an integer"))?;
        if value < 0 {
            bail!("{field_name} must not be negative");
        }
        return Ok(value);
    }
    let value = trimmed
        .parse::<i64>()
        .with_context(|| format!("{field_name} must be an integer"))?;
    if value < 0 {
        bail!("{field_name} must not be negative");
    }
    Ok(value)
}

fn field<'a>(row: &'a BillingRow, name: &str) -> &'a str {
    row.fields.get(name).map(String::as_str).unwrap_or_default()
}

fn json_field_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        other => other.to_string(),
    }
}

fn prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn prometheus_number(value: &DecimalAmount) -> String {
    value.to_plain_string()
}

fn write_usage_summary(
    quiet: bool,
    report_path: &Path,
    metrics_path: Option<&Path>,
    report: &UsageReport,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    if !quiet {
        writeln!(stdout, "R2 billing usage metrics wrote:")?;
        writeln!(stdout, "  report: {}", report_path.display())?;
        if let Some(metrics_path) = metrics_path {
            writeln!(stdout, "  metrics: {}", metrics_path.display())?;
        }
    }
    writeln!(
        stdout,
        "r2-billing-usage-metrics-ok buckets={} operation_classes={}",
        report.bucket_count, report.operation_class_count
    )?;
    Ok(())
}

fn resolve_path(raw: &str, label: &str) -> anyhow::Result<PathBuf> {
    if raw.trim().is_empty() {
        bail!("{label} must not be blank");
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return Ok(path);
    }
    Ok(env::current_dir()
        .context("failed to resolve current directory")?
        .join(path))
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    let Some(raw) = optional_env(&key(name))? else {
        return Ok(default);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => bail!("{name} must be a boolean"),
    }
}

fn key(name: &str) -> String {
    format!("{PREFIX}_{name}")
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

impl DecimalAmount {
    const fn zero() -> Self {
        Self { units: 0, scale: 0 }
    }

    fn parse_nonnegative(raw: &str, field_name: &str) -> anyhow::Result<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::zero());
        }
        if trimmed.starts_with('-') {
            bail!("{field_name} must not be negative");
        }

        let (whole, fraction) = trimmed.split_once('.').unwrap_or((trimmed, ""));
        if whole.is_empty() && fraction.is_empty() {
            bail!("{field_name} must be a number");
        }
        if !whole.chars().all(|ch| ch.is_ascii_digit()) {
            bail!("{field_name} must be a number");
        }
        if !fraction.chars().all(|ch| ch.is_ascii_digit()) {
            bail!("{field_name} must be a number");
        }

        let whole_units = if whole.is_empty() {
            0
        } else {
            whole
                .parse::<i128>()
                .with_context(|| format!("{field_name} must be a number"))?
        };
        let scale = u32::try_from(fraction.len())
            .with_context(|| format!("{field_name} decimal scale is too large"))?;
        let fraction_units = if fraction.is_empty() {
            0
        } else {
            fraction
                .parse::<i128>()
                .with_context(|| format!("{field_name} must be a number"))?
        };
        Ok(Self {
            units: whole_units
                .checked_mul(pow10(scale)?)
                .and_then(|value| value.checked_add(fraction_units))
                .with_context(|| format!("{field_name} is too large"))?,
            scale,
        })
    }

    fn add_assign(&mut self, other: &Self) {
        let target_scale = self.scale.max(other.scale);
        self.units = self.units * pow10_lossless(target_scale - self.scale)
            + other.units * pow10_lossless(target_scale - other.scale);
        self.scale = target_scale;
    }

    fn to_plain_string(&self) -> String {
        if self.units == 0 {
            return "0".to_owned();
        }
        if self.scale == 0 {
            return self.units.to_string();
        }

        let divisor = pow10_lossless(self.scale);
        let whole = self.units / divisor;
        let fraction = self.units % divisor;
        let mut fraction_text = format!("{fraction:0width$}", width = self.scale as usize);
        while fraction_text.ends_with('0') {
            fraction_text.pop();
        }
        if fraction_text.is_empty() {
            whole.to_string()
        } else {
            format!("{whole}.{fraction_text}")
        }
    }
}

fn pow10(scale: u32) -> anyhow::Result<i128> {
    let mut value = 1_i128;
    for _ in 0..scale {
        value = value
            .checked_mul(10)
            .context("decimal scale is too large")?;
    }
    Ok(value)
}

fn pow10_lossless(scale: u32) -> i128 {
    let mut value = 1_i128;
    for _ in 0..scale {
        value *= 10;
    }
    value
}
