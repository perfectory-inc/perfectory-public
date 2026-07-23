//! Plan writer for `rt.molit.go.kr` real-transaction CSV export collection jobs.

use std::{collections::BTreeSet, env, fs, path::PathBuf};

use anyhow::{bail, Context};
use chrono::{Datelike, Duration, NaiveDate};
use collection_application::{
    plan_rt_molit_real_transaction_export, RtMolitExportScope,
    RtMolitRealTransactionExportPlanInput, RtMolitRealTransactionExportRequest,
};
use serde::{Deserialize, Serialize};

use crate::public_data_control_support::{
    env_path, optional_env_value, optional_usize_env, repo_relative_path, resolve_repo_path,
    utc_now, write_json_file,
};
use crate::rt_molit_real_transaction_export_ingest::{run_input, RtMolitExportIngestInput};

const REPORT_SCHEMA_VERSION: &str =
    "foundation-platform.rt_molit_real_transaction_export_collection_plan.v1";
const SCOPE_ROW_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope_row.v1";
const PROVIDER: &str = "rt.molit.go.kr";
const DEFAULT_SCOPE_PATH: &str = "target/audit/national-data-collection-scope.jsonl";
const DEFAULT_OUTPUT_PATH: &str =
    "target/audit/rt-molit-real-transaction-export-collection-plan.json";
const DEFAULT_EXECUTION_EVIDENCE_PATH: &str =
    "target/audit/rt-molit-real-transaction-export-execution-evidence.json";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let mut report = match config.scope_mode {
        PlanScopeMode::Nationwide if config.period_mode == PlanPeriodMode::RollingDays => {
            compile_nationwide_rolling_days_plan(
                config
                    .rolling_as_of_date
                    .context("rolling_days plan requires as_of_date")?,
                config
                    .rolling_day_count
                    .context("rolling_days plan requires rolling_day_count")?,
            )?
        }
        PlanScopeMode::Nationwide => compile_nationwide_plan_with_period_mode(
            config
                .contract_month_from
                .as_deref()
                .context("month/year plan requires contract_month_from")?,
            config
                .contract_month_to
                .as_deref()
                .context("month/year plan requires contract_month_to")?,
            config.period_mode,
        )?,
        PlanScopeMode::Sigungu if config.period_mode == PlanPeriodMode::RollingDays => {
            bail!(
                "rt.molit rolling_days export plan is nationwide-only; use bulk nationwide refresh"
            )
        }
        PlanScopeMode::Sigungu => {
            let scope_jsonl = fs::read_to_string(&config.scope_path)
                .with_context(|| format!("failed to read {}", config.scope_path.display()))?;
            compile_plan_from_scope_jsonl_with_period_mode(
                &scope_jsonl,
                config
                    .contract_month_from
                    .as_deref()
                    .context("month/year plan requires contract_month_from")?,
                config
                    .contract_month_to
                    .as_deref()
                    .context("month/year plan requires contract_month_to")?,
                config.period_mode,
            )?
        }
    };
    report.generated_at_utc = utc_now();
    report.scope_path = if config.scope_mode == PlanScopeMode::Sigungu {
        repo_relative_path(&config.root, &config.scope_path)
    } else {
        String::new()
    };
    report.output_path = repo_relative_path(&config.root, &config.output_path);
    write_json_file(&config.output_path, &report)?;
    println!(
        "rt-molit-real-transaction-export-plan-written status=ready jobs={} scope_kind={} scope_units={} months={} period_kind={} periods={} path={}",
        report.job_count,
        report.scope_kind,
        report.scope_unit_count,
        report.month_count,
        report.period_kind,
        report.period_count,
        report.output_path
    );
    Ok(())
}

pub async fn run_execute() -> anyhow::Result<()> {
    let config = ExecuteConfig::from_env()?;
    let bytes = fs::read(&config.plan_path)
        .with_context(|| format!("failed to read {}", config.plan_path.display()))?;
    let report: RtMolitExportCollectionPlanReport =
        serde_json::from_slice(bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&bytes))
            .with_context(|| format!("failed to parse {}", config.plan_path.display()))?;
    validate_execution_report(&report)?;
    let jobs = select_execution_jobs(&report, config.start_index, config.max_jobs)?;

    let mut succeeded = 0_u64;
    for job in &jobs {
        let input = ingest_input_from_job(
            job,
            config.base_uri.clone(),
            config.user_agent.clone(),
            config.live_write.clone(),
        )
        .with_context(|| format!("failed to prepare rt.molit job {}", job.object_key))?;
        run_input(input)
            .await
            .with_context(|| format!("rt.molit job failed object_key={}", job.object_key))?;
        succeeded += 1;
    }

    let evidence = RtMolitExportExecutionEvidence {
        schema_version:
            "foundation-platform.rt_molit_real_transaction_export_execution_evidence.v1",
        generated_at_utc: utc_now(),
        status: "succeeded",
        plan_path: repo_relative_path(&config.root, &config.plan_path),
        plan_job_count: report.jobs.len() as u64,
        start_index: config.start_index as u64,
        selected_job_count: jobs.len() as u64,
        succeeded_job_count: succeeded,
        first_object_key: jobs.first().map(|job| job.object_key.clone()),
        last_object_key: jobs.last().map(|job| job.object_key.clone()),
        live_write_enabled: config.live_write.as_deref() == Some("1"),
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
    };
    write_json_file(&config.evidence_path, &evidence)?;
    println!(
        "rt-molit-real-transaction-export-plan-executed status=succeeded jobs={} live_write={} evidence={}",
        evidence.succeeded_job_count,
        evidence.live_write_enabled,
        repo_relative_path(&config.root, &config.evidence_path)
    );
    Ok(())
}

struct Config {
    root: PathBuf,
    scope_path: PathBuf,
    output_path: PathBuf,
    contract_month_from: Option<String>,
    contract_month_to: Option<String>,
    rolling_as_of_date: Option<NaiveDate>,
    rolling_day_count: Option<u32>,
    scope_mode: PlanScopeMode,
    period_mode: PlanPeriodMode,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let scope_path = resolve_repo_path(
            &root,
            &env_path(
                "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_SCOPE_PATH",
                DEFAULT_SCOPE_PATH,
            )?,
            "RtMolitExportPlanScopePath",
        )?;
        let output_path = resolve_repo_path(
            &root,
            &env_path(
                "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_OUTPUT_PATH",
                DEFAULT_OUTPUT_PATH,
            )?,
            "RtMolitExportPlanOutputPath",
        )?;
        let period_mode = PlanPeriodMode::from_env()?;
        let (contract_month_from, contract_month_to, rolling_as_of_date, rolling_day_count) =
            if period_mode == PlanPeriodMode::RollingDays {
                (
                    None,
                    None,
                    Some(parse_date_env(
                        "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_AS_OF_DATE",
                    )?),
                    Some(parse_u32_env(
                        "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_ROLLING_DAYS",
                    )?),
                )
            } else {
                (
                    Some(required_env(
                        "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_MONTH_FROM",
                    )?),
                    Some(required_env(
                        "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_MONTH_TO",
                    )?),
                    None,
                    None,
                )
            };

        Ok(Self {
            root,
            scope_path,
            output_path,
            contract_month_from,
            contract_month_to,
            rolling_as_of_date,
            rolling_day_count,
            scope_mode: PlanScopeMode::from_env()?,
            period_mode,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanScopeMode {
    Nationwide,
    Sigungu,
}

impl PlanScopeMode {
    fn from_env() -> anyhow::Result<Self> {
        match optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_SCOPE_KIND")?
            .unwrap_or_else(|| "nationwide".to_owned())
            .as_str()
        {
            "nationwide" => Ok(Self::Nationwide),
            "sigungu" => Ok(Self::Sigungu),
            value => bail!(
                "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_SCOPE_KIND must be nationwide or sigungu, got {value}"
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanPeriodMode {
    Month,
    Year,
    RollingDays,
}

impl PlanPeriodMode {
    fn from_env() -> anyhow::Result<Self> {
        match optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_PERIOD_KIND")?
            .unwrap_or_else(|| "year".to_owned())
            .as_str()
        {
            "month" => Ok(Self::Month),
            "year" => Ok(Self::Year),
            "rolling_days" => Ok(Self::RollingDays),
            value => bail!(
                "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_PERIOD_KIND must be month, year, or rolling_days, got {value}"
            ),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Month => "month",
            Self::Year => "year",
            Self::RollingDays => "rolling_days",
        }
    }
}

struct ExecuteConfig {
    root: PathBuf,
    plan_path: PathBuf,
    evidence_path: PathBuf,
    max_jobs: usize,
    start_index: usize,
    base_uri: Option<String>,
    user_agent: Option<String>,
    live_write: Option<String>,
}

impl ExecuteConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = required_env("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_CONFIRM")?;
        if confirm != "1" {
            bail!("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_CONFIRM must be 1");
        }
        let live_write =
            optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_LIVE_WRITE")?;
        if live_write.as_deref().is_some_and(|value| value != "1") {
            bail!("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_LIVE_WRITE must be 1 when set");
        }
        let max_jobs =
            optional_usize_env("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_MAX_JOBS")?
                .context("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_MAX_JOBS is required")?;
        if max_jobs == 0 {
            bail!(
                "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_MAX_JOBS must be greater than zero"
            );
        }
        let start_index =
            optional_usize_env("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_START_INDEX")?
                .unwrap_or(0);
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        Ok(Self {
            plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_PLAN_PATH",
                    DEFAULT_OUTPUT_PATH,
                )?,
                "RtMolitExportExecutionPlanPath",
            )?,
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_EVIDENCE_PATH",
                    DEFAULT_EXECUTION_EVIDENCE_PATH,
                )?,
                "RtMolitExportExecutionEvidencePath",
            )?,
            base_uri: optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_BASE_URI")?,
            user_agent: optional_env_value(
                "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EXECUTION_USER_AGENT",
            )?,
            live_write,
            max_jobs,
            start_index,
            root,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct RtMolitExportCollectionPlanReport {
    schema_version: String,
    generated_at_utc: String,
    status: String,
    provider: String,
    scope_kind: String,
    scope_path: String,
    output_path: String,
    contract_month_from: String,
    contract_month_to: String,
    scope_unit_count: u64,
    month_count: u64,
    #[serde(default = "default_month_period_kind")]
    period_kind: String,
    #[serde(default)]
    period_count: u64,
    dataset_count: u64,
    job_count: u64,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    jobs: Vec<RtMolitExportCollectionJob>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct RtMolitExportCollectionJob {
    source_slug: String,
    dataset_slug: String,
    thing_code: String,
    deal_type_code: String,
    period: String,
    contract_from: String,
    contract_to: String,
    scope_kind: String,
    sido_code: Option<String>,
    sigungu_code: Option<String>,
    source_identity_key: String,
    source_partition_key: String,
    object_key: String,
}

#[derive(Serialize)]
struct RtMolitExportExecutionEvidence {
    schema_version: &'static str,
    generated_at_utc: String,
    status: &'static str,
    plan_path: String,
    plan_job_count: u64,
    start_index: u64,
    selected_job_count: u64,
    succeeded_job_count: u64,
    first_object_key: Option<String>,
    last_object_key: Option<String>,
    live_write_enabled: bool,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DatasetSpec {
    dataset_slug: &'static str,
    thing_code: &'static str,
    deal_type_code: &'static str,
}

const DATASETS: &[DatasetSpec] = &[
    DatasetSpec {
        dataset_slug: "real_transaction_apartment_trade",
        thing_code: "A",
        deal_type_code: "1",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_apartment_rent",
        thing_code: "A",
        deal_type_code: "2",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_row_house_trade",
        thing_code: "B",
        deal_type_code: "1",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_row_house_rent",
        thing_code: "B",
        deal_type_code: "2",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_detached_house_trade",
        thing_code: "C",
        deal_type_code: "1",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_detached_house_rent",
        thing_code: "C",
        deal_type_code: "2",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_officetel_trade",
        thing_code: "D",
        deal_type_code: "1",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_officetel_rent",
        thing_code: "D",
        deal_type_code: "2",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_apartment_presale",
        thing_code: "E",
        deal_type_code: "1",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_commercial_trade",
        thing_code: "F",
        deal_type_code: "1",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_land_trade",
        thing_code: "G",
        deal_type_code: "1",
    },
    DatasetSpec {
        dataset_slug: "real_transaction_industrial_trade",
        thing_code: "H",
        deal_type_code: "1",
    },
];

#[cfg(test)]
fn compile_plan_from_scope_jsonl(
    scope_jsonl: &str,
    contract_month_from: &str,
    contract_month_to: &str,
) -> anyhow::Result<RtMolitExportCollectionPlanReport> {
    compile_plan_from_scope_jsonl_with_period_mode(
        scope_jsonl,
        contract_month_from,
        contract_month_to,
        PlanPeriodMode::Month,
    )
}

fn compile_plan_from_scope_jsonl_with_period_mode(
    scope_jsonl: &str,
    contract_month_from: &str,
    contract_month_to: &str,
    period_mode: PlanPeriodMode,
) -> anyhow::Result<RtMolitExportCollectionPlanReport> {
    let months = month_range(contract_month_from, contract_month_to)?;
    let periods = contract_periods(contract_month_from, contract_month_to, period_mode)?;
    let sigungu_codes = sigungu_codes_from_scope_jsonl(scope_jsonl)?;
    let mut jobs = Vec::with_capacity(sigungu_codes.len() * periods.len() * DATASETS.len());
    for period in &periods {
        for sigungu_code in &sigungu_codes {
            let sido_code = sido_code_from_sigungu(sigungu_code)?;
            for dataset in DATASETS {
                jobs.push(plan_job(
                    dataset,
                    &period.label,
                    &period.contract_from,
                    &period.contract_to,
                    RtMolitExportScope::Sigungu {
                        sido_code: sido_code.clone(),
                        sigungu_code: sigungu_code.clone(),
                    },
                )?);
            }
        }
    }

    Ok(RtMolitExportCollectionPlanReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: String::new(),
        status: "ready".to_owned(),
        provider: PROVIDER.to_owned(),
        scope_kind: "sigungu".to_owned(),
        scope_path: String::new(),
        output_path: String::new(),
        contract_month_from: contract_month_from.to_owned(),
        contract_month_to: contract_month_to.to_owned(),
        scope_unit_count: sigungu_codes.len() as u64,
        month_count: months.len() as u64,
        period_kind: period_mode.as_str().to_owned(),
        period_count: periods.len() as u64,
        dataset_count: DATASETS.len() as u64,
        job_count: jobs.len() as u64,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        jobs,
    })
}

#[cfg(test)]
fn compile_nationwide_plan(
    contract_month_from: &str,
    contract_month_to: &str,
) -> anyhow::Result<RtMolitExportCollectionPlanReport> {
    compile_nationwide_plan_with_period_mode(
        contract_month_from,
        contract_month_to,
        PlanPeriodMode::Month,
    )
}

fn compile_nationwide_plan_with_period_mode(
    contract_month_from: &str,
    contract_month_to: &str,
    period_mode: PlanPeriodMode,
) -> anyhow::Result<RtMolitExportCollectionPlanReport> {
    let months = month_range(contract_month_from, contract_month_to)?;
    let periods = contract_periods(contract_month_from, contract_month_to, period_mode)?;
    let mut jobs = Vec::with_capacity(periods.len() * DATASETS.len());
    for period in &periods {
        for dataset in DATASETS {
            jobs.push(plan_job(
                dataset,
                &period.label,
                &period.contract_from,
                &period.contract_to,
                RtMolitExportScope::Nationwide,
            )?);
        }
    }
    Ok(RtMolitExportCollectionPlanReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: String::new(),
        status: "ready".to_owned(),
        provider: PROVIDER.to_owned(),
        scope_kind: "nationwide".to_owned(),
        scope_path: String::new(),
        output_path: String::new(),
        contract_month_from: contract_month_from.to_owned(),
        contract_month_to: contract_month_to.to_owned(),
        scope_unit_count: 1,
        month_count: months.len() as u64,
        period_kind: period_mode.as_str().to_owned(),
        period_count: periods.len() as u64,
        dataset_count: DATASETS.len() as u64,
        job_count: jobs.len() as u64,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        jobs,
    })
}

fn compile_nationwide_rolling_days_plan(
    as_of_date: NaiveDate,
    day_count: u32,
) -> anyhow::Result<RtMolitExportCollectionPlanReport> {
    let period = rolling_days_contract_period(as_of_date, day_count)?;
    let contract_month_from = YearMonth::from_date(period.contract_from).to_string();
    let contract_month_to = YearMonth::from_date(period.contract_to).to_string();
    let months = month_range(&contract_month_from, &contract_month_to)?;
    let periods = [period];
    let mut jobs = Vec::with_capacity(periods.len() * DATASETS.len());
    for period in &periods {
        for dataset in DATASETS {
            jobs.push(plan_job(
                dataset,
                &period.label,
                &period.contract_from,
                &period.contract_to,
                RtMolitExportScope::Nationwide,
            )?);
        }
    }
    Ok(RtMolitExportCollectionPlanReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: String::new(),
        status: "ready".to_owned(),
        provider: PROVIDER.to_owned(),
        scope_kind: "nationwide".to_owned(),
        scope_path: String::new(),
        output_path: String::new(),
        contract_month_from,
        contract_month_to,
        scope_unit_count: 1,
        month_count: months.len() as u64,
        period_kind: "rolling_days".to_owned(),
        period_count: periods.len() as u64,
        dataset_count: DATASETS.len() as u64,
        job_count: jobs.len() as u64,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        jobs,
    })
}

fn plan_job(
    dataset: &DatasetSpec,
    period_label: &str,
    contract_from: &NaiveDate,
    contract_to: &NaiveDate,
    scope: RtMolitExportScope,
) -> anyhow::Result<RtMolitExportCollectionJob> {
    let source_slug = collection_domain::source_slug(PROVIDER, dataset.dataset_slug)?;
    let request = RtMolitRealTransactionExportRequest {
        thing_code: dataset.thing_code.to_owned(),
        deal_type_code: dataset.deal_type_code.to_owned(),
        contract_from: *contract_from,
        contract_to: *contract_to,
        scope: scope.clone(),
        response_format: "csv".to_owned(),
    };
    let (scope_kind, sido_code, sigungu_code) = job_scope_fields(&scope);
    let plan = plan_rt_molit_real_transaction_export(RtMolitRealTransactionExportPlanInput {
        source_slug: &source_slug,
        request,
        raw_payload: Vec::new(),
    })?;
    Ok(RtMolitExportCollectionJob {
        source_slug,
        dataset_slug: dataset.dataset_slug.to_owned(),
        thing_code: dataset.thing_code.to_owned(),
        deal_type_code: dataset.deal_type_code.to_owned(),
        period: period_label.to_owned(),
        contract_from: contract_from.to_string(),
        contract_to: contract_to.to_string(),
        scope_kind,
        sido_code,
        sigungu_code,
        source_identity_key: plan.source_identity_key,
        source_partition_key: plan.source_partition_key,
        object_key: plan.object_key.as_str().to_owned(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ContractPeriod {
    label: String,
    contract_from: NaiveDate,
    contract_to: NaiveDate,
}

fn contract_periods(
    contract_month_from: &str,
    contract_month_to: &str,
    period_mode: PlanPeriodMode,
) -> anyhow::Result<Vec<ContractPeriod>> {
    let months = month_range(contract_month_from, contract_month_to)?;
    match period_mode {
        PlanPeriodMode::Month => months
            .into_iter()
            .map(|month| {
                let (contract_from, contract_to) = month.date_range()?;
                Ok(ContractPeriod {
                    label: month.to_string(),
                    contract_from,
                    contract_to,
                })
            })
            .collect(),
        PlanPeriodMode::Year => yearly_contract_periods(&months),
        PlanPeriodMode::RollingDays => {
            bail!("rolling_days periods are compiled from as_of_date, not month range")
        }
    }
}

fn yearly_contract_periods(months: &[YearMonth]) -> anyhow::Result<Vec<ContractPeriod>> {
    let first = months
        .first()
        .copied()
        .context("month range must not be empty")?;
    let last = months
        .last()
        .copied()
        .context("month range must not be empty")?;
    let mut periods = Vec::new();
    let mut cursor = first;
    while cursor <= last {
        let mut end = YearMonth {
            year: cursor.year,
            month: 12,
        };
        if end > last {
            end = last;
        }
        let (contract_from, _) = cursor.date_range()?;
        let (_, contract_to) = end.date_range()?;
        let label = if cursor.month == 1 && end.month == 12 {
            format!("{:04}", cursor.year)
        } else {
            format!("{cursor}..{end}")
        };
        periods.push(ContractPeriod {
            label,
            contract_from,
            contract_to,
        });
        cursor = end.next();
    }
    Ok(periods)
}

fn rolling_days_contract_period(
    as_of_date: NaiveDate,
    day_count: u32,
) -> anyhow::Result<ContractPeriod> {
    if day_count == 0 {
        bail!("rolling day count must be greater than zero");
    }
    let days_before = i64::from(day_count - 1);
    let contract_from = as_of_date
        .checked_sub_signed(Duration::days(days_before))
        .context("rolling day window underflowed")?;
    Ok(ContractPeriod {
        label: format!("{contract_from}..{as_of_date}"),
        contract_from,
        contract_to: as_of_date,
    })
}

fn default_month_period_kind() -> String {
    "month".to_owned()
}

fn job_scope_fields(scope: &RtMolitExportScope) -> (String, Option<String>, Option<String>) {
    match scope {
        RtMolitExportScope::Nationwide => ("nationwide".to_owned(), None, None),
        RtMolitExportScope::Sigungu {
            sido_code,
            sigungu_code,
        } => (
            "sigungu".to_owned(),
            Some(sido_code.clone()),
            Some(sigungu_code.clone()),
        ),
        RtMolitExportScope::Sido { sido_code } => {
            ("sido".to_owned(), Some(sido_code.clone()), None)
        }
        RtMolitExportScope::Emd {
            sido_code,
            sigungu_code,
            ..
        } => (
            "emd".to_owned(),
            Some(sido_code.clone()),
            Some(sigungu_code.clone()),
        ),
    }
}

fn validate_execution_report(report: &RtMolitExportCollectionPlanReport) -> anyhow::Result<()> {
    if report.schema_version != REPORT_SCHEMA_VERSION {
        bail!("rt.molit execution plan schema_version mismatch");
    }
    if report.status != "ready" {
        bail!("rt.molit execution plan status must be ready");
    }
    if report.provider != PROVIDER {
        bail!("rt.molit execution plan provider mismatch");
    }
    if report.national_rollout_allowed {
        bail!("rt.molit execution plan must not self-approve national rollout");
    }
    if report.jobs.is_empty() {
        bail!("rt.molit execution plan contains no jobs");
    }
    Ok(())
}

fn select_execution_jobs(
    report: &RtMolitExportCollectionPlanReport,
    start_index: usize,
    max_jobs: usize,
) -> anyhow::Result<Vec<&RtMolitExportCollectionJob>> {
    validate_execution_report(report)?;
    if max_jobs == 0 {
        bail!("max_jobs must be greater than zero");
    }
    if start_index >= report.jobs.len() {
        bail!(
            "start_index must be smaller than planned job count: start_index={} job_count={}",
            start_index,
            report.jobs.len()
        );
    }
    Ok(report
        .jobs
        .iter()
        .skip(start_index)
        .take(max_jobs)
        .collect())
}

fn ingest_input_from_job(
    job: &RtMolitExportCollectionJob,
    base_uri: Option<String>,
    user_agent: Option<String>,
    live_write: Option<String>,
) -> anyhow::Result<RtMolitExportIngestInput> {
    let scope = match job.scope_kind.as_str() {
        "nationwide" => RtMolitExportScope::Nationwide,
        "sigungu" => RtMolitExportScope::Sigungu {
            sido_code: job
                .sido_code
                .clone()
                .context("sigungu job requires sido_code")?,
            sigungu_code: job
                .sigungu_code
                .clone()
                .context("sigungu job requires sigungu_code")?,
        },
        _ => bail!("rt.molit execution supports only nationwide and sigungu jobs"),
    };
    Ok(RtMolitExportIngestInput {
        request: RtMolitRealTransactionExportRequest {
            thing_code: job.thing_code.clone(),
            deal_type_code: job.deal_type_code.clone(),
            contract_from: NaiveDate::parse_from_str(&job.contract_from, "%Y-%m-%d")
                .context("contract_from must use YYYY-MM-DD")?,
            contract_to: NaiveDate::parse_from_str(&job.contract_to, "%Y-%m-%d")
                .context("contract_to must use YYYY-MM-DD")?,
            scope,
            response_format: "csv".to_owned(),
        },
        base_uri,
        user_agent,
        live_write,
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct YearMonth {
    year: i32,
    month: u32,
}

impl YearMonth {
    fn from_date(date: NaiveDate) -> Self {
        Self {
            year: date.year(),
            month: date.month(),
        }
    }

    fn parse(raw: &str) -> anyhow::Result<Self> {
        let (year, month) = raw
            .split_once('-')
            .with_context(|| format!("month must use YYYY-MM format: {raw}"))?;
        let year = year
            .parse::<i32>()
            .with_context(|| format!("month year must be numeric: {raw}"))?;
        let month = month
            .parse::<u32>()
            .with_context(|| format!("month value must be numeric: {raw}"))?;
        if !(1..=12).contains(&month) {
            bail!("month must be between 01 and 12: {raw}");
        }
        Ok(Self { year, month })
    }

    fn next(self) -> Self {
        if self.month == 12 {
            Self {
                year: self.year + 1,
                month: 1,
            }
        } else {
            Self {
                year: self.year,
                month: self.month + 1,
            }
        }
    }

    fn date_range(self) -> anyhow::Result<(NaiveDate, NaiveDate)> {
        let first = NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .with_context(|| format!("invalid month: {self}"))?;
        let last =
            NaiveDate::from_ymd_opt(self.year, self.month, days_in_month(self.year, self.month))
                .with_context(|| format!("invalid month: {self}"))?;
        Ok((first, last))
    }
}

impl std::fmt::Display for YearMonth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:04}-{:02}", self.year, self.month)
    }
}

fn month_range(from: &str, to: &str) -> anyhow::Result<Vec<YearMonth>> {
    let from = YearMonth::parse(from)?;
    let to = YearMonth::parse(to)?;
    if from > to {
        bail!("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_PLAN_MONTH_FROM must be on or before *_TO");
    }
    let mut months = Vec::new();
    let mut cursor = from;
    while cursor <= to {
        months.push(cursor);
        cursor = cursor.next();
    }
    Ok(months)
}

#[derive(Deserialize)]
struct ScopeRow {
    schema_version: String,
    sigungu_cd: String,
}

fn sigungu_codes_from_scope_jsonl(scope_jsonl: &str) -> anyhow::Result<Vec<String>> {
    let mut codes = BTreeSet::new();
    for (index, raw_line) in scope_jsonl.lines().enumerate() {
        let line_number = index + 1;
        let line = if line_number == 1 {
            raw_line.trim_start_matches('\u{feff}')
        } else {
            raw_line
        }
        .trim();
        if line.is_empty() {
            continue;
        }
        let row: ScopeRow = serde_json::from_str(line)
            .with_context(|| format!("scope line {line_number} is not valid JSON"))?;
        if row.schema_version != SCOPE_ROW_SCHEMA_VERSION {
            bail!("scope line {line_number} has unsupported schema_version");
        }
        validate_sigungu_code(&row.sigungu_cd)?;
        codes.insert(row.sigungu_cd);
    }
    if codes.is_empty() {
        bail!("scope must contain at least one legal-dong row");
    }
    Ok(codes.into_iter().collect())
}

fn validate_sigungu_code(value: &str) -> anyhow::Result<()> {
    if value.len() == 5 && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(());
    }
    bail!("sigungu_cd must be five digits");
}

fn sido_code_from_sigungu(sigungu_code: &str) -> anyhow::Result<String> {
    validate_sigungu_code(sigungu_code)?;
    Ok(format!("{}000", &sigungu_code[0..2]))
}

fn required_env(name: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        Ok(_) | Err(env::VarError::NotPresent) => bail!("{name} is required"),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn parse_date_env(name: &str) -> anyhow::Result<NaiveDate> {
    let value = required_env(name)?;
    NaiveDate::parse_from_str(&value, "%Y-%m-%d")
        .with_context(|| format!("{name} must use YYYY-MM-DD"))
}

fn parse_u32_env(name: &str) -> anyhow::Result<u32> {
    let value = required_env(name)?;
    let parsed = value
        .parse::<u32>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
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

const fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

const fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    #[test]
    fn compile_plan_deduplicates_scope_to_sigungu_monthly_jobs() -> TestResult {
        let scope_jsonl = r#"
{"schema_version":"foundation-platform.national_data_collection_scope_row.v1","sigungu_cd":"11680","bjdong_cd":"10300","bjdong_code":"1168010300"}
{"schema_version":"foundation-platform.national_data_collection_scope_row.v1","sigungu_cd":"11680","bjdong_cd":"10400","bjdong_code":"1168010400"}
{"schema_version":"foundation-platform.national_data_collection_scope_row.v1","sigungu_cd":"30170","bjdong_cd":"10100","bjdong_code":"3017010100"}
"#;

        let report = compile_plan_from_scope_jsonl(scope_jsonl, "2026-05", "2026-06")?;

        assert_eq!(
            report.schema_version,
            "foundation-platform.rt_molit_real_transaction_export_collection_plan.v1"
        );
        assert_eq!(report.status, "ready");
        assert_eq!(report.scope_kind, "sigungu");
        assert_eq!(report.scope_unit_count, 2);
        assert_eq!(report.month_count, 2);
        assert_eq!(report.dataset_count, 12);
        assert_eq!(report.job_count, 48);
        assert!(!report.national_rollout_allowed);

        let first = report.jobs.first().ok_or("missing first job")?;
        assert_eq!(first.dataset_slug, "real_transaction_apartment_trade");
        assert_eq!(
            first.source_slug,
            "rtmolitkr__real_transaction_apartment_trade"
        );
        assert_eq!(first.thing_code, "A");
        assert_eq!(first.deal_type_code, "1");
        assert_eq!(first.period, "2026-05");
        assert_eq!(first.contract_from, "2026-05-01");
        assert_eq!(first.contract_to, "2026-05-31");
        assert_eq!(first.sido_code.as_deref(), Some("11000"));
        assert_eq!(first.sigungu_code.as_deref(), Some("11680"));
        assert_eq!(
            first.object_key,
            "bronze/source=rtmolitkr__real_transaction_apartment_trade/period=2026-05/sido=11000/sigungu=11680/export.csv"
        );

        let last = report.jobs.last().ok_or("missing last job")?;
        assert_eq!(last.dataset_slug, "real_transaction_industrial_trade");
        assert_eq!(last.period, "2026-06");
        assert_eq!(last.sido_code.as_deref(), Some("30000"));
        assert_eq!(last.sigungu_code.as_deref(), Some("30170"));
        Ok(())
    }

    #[test]
    fn compile_nationwide_plan_uses_coarsest_provider_export_scope() -> TestResult {
        let report = compile_nationwide_plan("2026-06", "2026-06")?;

        assert_eq!(report.scope_kind, "nationwide");
        assert_eq!(report.scope_unit_count, 1);
        assert_eq!(report.month_count, 1);
        assert_eq!(report.dataset_count, 12);
        assert_eq!(report.job_count, 12);

        let first = report.jobs.first().ok_or("missing first job")?;
        assert_eq!(first.dataset_slug, "real_transaction_apartment_trade");
        assert_eq!(first.scope_kind, "nationwide");
        assert_eq!(first.sido_code, None);
        assert_eq!(first.sigungu_code, None);
        assert_eq!(
            first.object_key,
            "bronze/source=rtmolitkr__real_transaction_apartment_trade/period=2026-06/scope=nationwide/export.csv"
        );
        Ok(())
    }

    #[test]
    fn compile_nationwide_yearly_plan_uses_coarser_provider_export_ranges() -> TestResult {
        let report =
            compile_nationwide_plan_with_period_mode("2025-01", "2026-06", PlanPeriodMode::Year)?;

        assert_eq!(report.scope_kind, "nationwide");
        assert_eq!(report.period_kind, "year");
        assert_eq!(report.scope_unit_count, 1);
        assert_eq!(report.month_count, 18);
        assert_eq!(report.period_count, 2);
        assert_eq!(report.dataset_count, 12);
        assert_eq!(report.job_count, 24);

        let first = report.jobs.first().ok_or("missing first job")?;
        assert_eq!(first.period, "2025");
        assert_eq!(first.contract_from, "2025-01-01");
        assert_eq!(first.contract_to, "2025-12-31");
        assert_eq!(
            first.object_key,
            "bronze/source=rtmolitkr__real_transaction_apartment_trade/contract_from=2025-01-01/contract_to=2025-12-31/scope=nationwide/export.csv"
        );

        let last = report.jobs.last().ok_or("missing last job")?;
        assert_eq!(last.period, "2026-01..2026-06");
        assert_eq!(last.dataset_slug, "real_transaction_industrial_trade");
        assert_eq!(last.contract_from, "2026-01-01");
        assert_eq!(last.contract_to, "2026-06-30");
        assert_eq!(
            last.object_key,
            "bronze/source=rtmolitkr__real_transaction_industrial_trade/contract_from=2026-01-01/contract_to=2026-06-30/scope=nationwide/export.csv"
        );
        Ok(())
    }

    #[test]
    fn compile_nationwide_rolling_days_plan_uses_daily_refresh_window() -> TestResult {
        let report = compile_nationwide_rolling_days_plan(
            NaiveDate::from_ymd_opt(2026, 7, 1).ok_or("valid date")?,
            90,
        )?;

        assert_eq!(report.scope_kind, "nationwide");
        assert_eq!(report.period_kind, "rolling_days");
        assert_eq!(report.contract_month_from, "2026-04");
        assert_eq!(report.contract_month_to, "2026-07");
        assert_eq!(report.month_count, 4);
        assert_eq!(report.period_count, 1);
        assert_eq!(report.dataset_count, 12);
        assert_eq!(report.job_count, 12);

        let first = report.jobs.first().ok_or("missing first job")?;
        assert_eq!(first.period, "2026-04-03..2026-07-01");
        assert_eq!(first.contract_from, "2026-04-03");
        assert_eq!(first.contract_to, "2026-07-01");
        assert_eq!(
            first.object_key,
            "bronze/source=rtmolitkr__real_transaction_apartment_trade/contract_from=2026-04-03/contract_to=2026-07-01/scope=nationwide/export.csv"
        );
        Ok(())
    }

    #[test]
    fn compile_plan_rejects_invalid_sigungu_code() {
        let scope_jsonl = r#"{"schema_version":"foundation-platform.national_data_collection_scope_row.v1","sigungu_cd":"1168X"}"#;

        let error = compile_plan_from_scope_jsonl(scope_jsonl, "2026-05", "2026-05")
            .err()
            .expect("invalid sigungu must fail");

        assert!(
            error.to_string().contains("sigungu_cd must be five digits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn execution_input_maps_plan_job_to_ingest_input() -> TestResult {
        let scope_jsonl = r#"{"schema_version":"foundation-platform.national_data_collection_scope_row.v1","sigungu_cd":"11680","bjdong_cd":"10300","bjdong_code":"1168010300"}"#;
        let report = compile_plan_from_scope_jsonl(scope_jsonl, "2026-06", "2026-06")?;
        let job = report.jobs.first().ok_or("missing planned job")?;

        let input = ingest_input_from_job(
            job,
            Some("https://example.test".to_owned()),
            Some("test-agent".to_owned()),
            Some("1".to_owned()),
        )?;

        assert_eq!(input.request.thing_code, "A");
        assert_eq!(input.request.deal_type_code, "1");
        assert_eq!(input.request.contract_from.to_string(), "2026-06-01");
        assert_eq!(input.request.contract_to.to_string(), "2026-06-30");
        assert_eq!(input.base_uri.as_deref(), Some("https://example.test"));
        assert_eq!(input.user_agent.as_deref(), Some("test-agent"));
        assert_eq!(input.live_write.as_deref(), Some("1"));
        match input.request.scope {
            RtMolitExportScope::Sigungu {
                sido_code,
                sigungu_code,
            } => {
                assert_eq!(sido_code, "11000");
                assert_eq!(sigungu_code, "11680");
            }
            other => panic!("expected sigungu scope, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn select_execution_jobs_respects_explicit_max_jobs() -> TestResult {
        let scope_jsonl = r#"
{"schema_version":"foundation-platform.national_data_collection_scope_row.v1","sigungu_cd":"11680","bjdong_cd":"10300","bjdong_code":"1168010300"}
{"schema_version":"foundation-platform.national_data_collection_scope_row.v1","sigungu_cd":"30170","bjdong_cd":"10100","bjdong_code":"3017010100"}
"#;
        let report = compile_plan_from_scope_jsonl(scope_jsonl, "2026-06", "2026-06")?;

        let selected = select_execution_jobs(&report, 0, 3)?;

        assert_eq!(selected.len(), 3);
        assert_eq!(selected[0].object_key, report.jobs[0].object_key);
        assert_eq!(selected[2].object_key, report.jobs[2].object_key);
        Ok(())
    }

    #[test]
    fn select_execution_jobs_respects_start_index_for_chunked_resume() -> TestResult {
        let report = compile_nationwide_plan("2026-05", "2026-06")?;

        let selected = select_execution_jobs(&report, 12, 5)?;

        assert_eq!(selected.len(), 5);
        assert_eq!(selected[0].object_key, report.jobs[12].object_key);
        assert_eq!(selected[4].object_key, report.jobs[16].object_key);
        Ok(())
    }

    #[test]
    fn select_execution_jobs_rejects_start_index_beyond_plan() -> TestResult {
        let report = compile_nationwide_plan("2026-06", "2026-06")?;

        let error = select_execution_jobs(&report, 12, 1)
            .err()
            .expect("out-of-range start index must fail");

        assert!(
            error
                .to_string()
                .contains("start_index must be smaller than planned job count"),
            "unexpected error: {error}"
        );
        Ok(())
    }
}
