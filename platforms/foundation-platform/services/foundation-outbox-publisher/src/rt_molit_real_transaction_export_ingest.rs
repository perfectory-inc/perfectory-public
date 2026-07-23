//! `rt.molit.go.kr` real-transaction CSV export Bronze ingestion command.

use anyhow::{bail, Context};
use chrono::{NaiveDate, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    plan_rt_molit_real_transaction_export, BronzeCommitter, BronzePayload, PlannedBronzeObject,
    RtMolitExportScope, RtMolitRealTransactionExportPlan, RtMolitRealTransactionExportPlanInput,
    RtMolitRealTransactionExportRequest,
};
use collection_domain::{
    BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use reqwest::header::{ACCEPT, CONTENT_TYPE, COOKIE, REFERER, SET_COOKIE, USER_AGENT};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::{
    bronze_object_storage_from_env, live_write_target_preflight, BronzeObjectStorageWriter,
};
use crate::public_data_control_support::{optional_env_value, required_env_value};

const PROVIDER: &str = "rt.molit.go.kr";
const DEFAULT_BASE_URI: &str = "https://rt.molit.go.kr";
const DEFAULT_THING_CODE: &str = "A";
const DEFAULT_DEAL_TYPE_CODE: &str = "1";
const DEFAULT_USER_AGENT: &str = "foundation-platform-rt-molit-real-transaction-export/1.0";
const BRONZE_CONTENT_TYPE: &str = "text/csv";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const TERMS_URL: &str = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt=";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();

/// Runs one `rt.molit.go.kr` real-transaction CSV export Bronze ingestion.
pub async fn run() -> anyhow::Result<()> {
    let config = RtMolitExportIngestConfig::from_env()?;
    run_config(config).await
}

pub(crate) async fn run_input(input: RtMolitExportIngestInput) -> anyhow::Result<()> {
    let config = RtMolitExportIngestConfig::from_input(input)?;
    run_config(config).await
}

async fn run_config(config: RtMolitExportIngestConfig) -> anyhow::Result<()> {
    let client = RtMolitExportClient::new(config.base_uri.clone(), config.user_agent.clone())?;
    let fetched = client
        .fetch_csv_export(&config.request)
        .await
        .context("failed to fetch rt.molit.go.kr real-transaction CSV export")?;
    let plan = plan_rt_molit_real_transaction_export(RtMolitRealTransactionExportPlanInput {
        source_slug: &config.source_slug,
        request: config.request.clone(),
        raw_payload: fetched.raw_payload,
    })
    .context("failed to plan rt.molit.go.kr real-transaction CSV Bronze export")?;

    if !live_write_enabled(config.live_write.as_deref()) {
        tracing::info!(
            source_slug = %config.source_slug,
            source_identity_key = %plan.source_identity_key,
            object_key = %plan.object_key.as_str(),
            provider_count = fetched.provider_count,
            size_bytes = plan.size_bytes,
            "rt.molit.go.kr real-transaction CSV export dry run succeeded"
        );
        return Ok(());
    }

    live_write_target_preflight()
        .context("rt.molit.go.kr real-transaction export live-write target preflight failed")?;
    persist_plan(&config, fetched.provider_count, plan).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RtMolitExportIngestInput {
    pub request: RtMolitRealTransactionExportRequest,
    pub base_uri: Option<String>,
    pub user_agent: Option<String>,
    pub live_write: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RtMolitExportIngestConfig {
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    user_agent: String,
    request: RtMolitRealTransactionExportRequest,
    live_write: Option<String>,
}

impl RtMolitExportIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        let thing_code = optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_THING_CODE")?
            .unwrap_or_else(|| DEFAULT_THING_CODE.to_owned());
        let deal_type_code =
            optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_DEAL_TYPE_CODE")?
                .unwrap_or_else(|| DEFAULT_DEAL_TYPE_CODE.to_owned());
        let dataset_slug = rt_molit_dataset_slug(&thing_code, &deal_type_code)?;
        let source_slug = crate::public_data_control_support::resolve_canonical_source_slug(
            "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SOURCE_SLUG",
            optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SOURCE_SLUG")?,
            PROVIDER,
            dataset_slug,
        )?;
        Ok(Self {
            source_slug,
            source_name: format!("rt.molit.go.kr Real Transaction ({dataset_slug})"),
            dataset_name: dataset_slug.replace('_', "-"),
            base_uri: optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            user_agent: optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request: RtMolitRealTransactionExportRequest {
                thing_code,
                deal_type_code,
                contract_from: parse_date_env("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_CONTRACT_FROM")?,
                contract_to: parse_date_env("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_CONTRACT_TO")?,
                scope: rt_molit_scope_from_env()?,
                response_format: "csv".to_owned(),
            },
            live_write: optional_env_value("FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_LIVE_WRITE")?,
        })
    }

    fn from_input(input: RtMolitExportIngestInput) -> anyhow::Result<Self> {
        let dataset_slug =
            rt_molit_dataset_slug(&input.request.thing_code, &input.request.deal_type_code)?;
        let source_slug = crate::public_data_control_support::resolve_canonical_source_slug(
            "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SOURCE_SLUG",
            None,
            PROVIDER,
            dataset_slug,
        )?;
        Ok(Self {
            source_slug,
            source_name: format!("rt.molit.go.kr Real Transaction ({dataset_slug})"),
            dataset_name: dataset_slug.replace('_', "-"),
            base_uri: input
                .base_uri
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            user_agent: input
                .user_agent
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request: input.request,
            live_write: input.live_write,
        })
    }
}

fn rt_molit_scope_from_env() -> anyhow::Result<RtMolitExportScope> {
    let sido_code = nonblank(optional_env_value(
        "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SIDO_CODE",
    )?);
    let sigungu_code = nonblank(optional_env_value(
        "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SIGUNGU_CODE",
    )?);
    let emd_code = nonblank(optional_env_value(
        "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EMD_CODE",
    )?);

    match (sido_code, sigungu_code, emd_code) {
        (None, None, None) => Ok(RtMolitExportScope::Nationwide),
        (Some(sido_code), None, None) => Ok(RtMolitExportScope::Sido { sido_code }),
        (Some(sido_code), Some(sigungu_code), None) => Ok(RtMolitExportScope::Sigungu {
            sido_code,
            sigungu_code,
        }),
        (Some(sido_code), Some(sigungu_code), Some(emd_code)) => Ok(RtMolitExportScope::Emd {
            sido_code,
            sigungu_code,
            emd_code,
        }),
        (None, Some(_), _) => bail!(
            "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SIGUNGU_CODE requires FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SIDO_CODE"
        ),
        (_, None, Some(_)) => bail!(
            "FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_EMD_CODE requires FOUNDATION_PLATFORM_RT_MOLIT_EXPORT_SIGUNGU_CODE"
        ),
    }
}

fn nonblank(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn rt_molit_dataset_slug(thing_code: &str, deal_type_code: &str) -> anyhow::Result<&'static str> {
    match (thing_code, deal_type_code) {
        ("A", "1") => Ok("real_transaction_apartment_trade"),
        ("A", "2") => Ok("real_transaction_apartment_rent"),
        ("B", "1") => Ok("real_transaction_row_house_trade"),
        ("B", "2") => Ok("real_transaction_row_house_rent"),
        ("C", "1") => Ok("real_transaction_detached_house_trade"),
        ("C", "2") => Ok("real_transaction_detached_house_rent"),
        ("D", "1") => Ok("real_transaction_officetel_trade"),
        ("D", "2") => Ok("real_transaction_officetel_rent"),
        ("E", "1") => Ok("real_transaction_apartment_presale"),
        ("F", "1") => Ok("real_transaction_commercial_trade"),
        ("G", "1") => Ok("real_transaction_land_trade"),
        ("H", "1") => Ok("real_transaction_industrial_trade"),
        _ => bail!(
            "unsupported rt.molit.go.kr thing/deal code pair: thing={thing_code:?} deal_type={deal_type_code:?}"
        ),
    }
}

#[derive(Clone, Debug)]
struct RtMolitExportClient {
    base_uri: String,
    user_agent: String,
    client: reqwest::Client,
}

impl RtMolitExportClient {
    fn new(base_uri: String, user_agent: String) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(user_agent.clone())
            .build()
            .context("failed to build rt.molit.go.kr HTTP client")?;
        Ok(Self {
            base_uri: base_uri.trim_end_matches('/').to_owned(),
            user_agent,
            client,
        })
    }

    async fn fetch_csv_export(
        &self,
        request: &RtMolitRealTransactionExportRequest,
    ) -> anyhow::Result<RtMolitFetchedExport> {
        let landing_url = format!("{}/pt/xls/xls.do?mobileAt=", self.base_uri);
        let landing = self
            .client
            .get(&landing_url)
            .header(USER_AGENT, &self.user_agent)
            .send()
            .await
            .context("failed to open rt.molit.go.kr CSV export form")?;
        let cookie_header = cookie_header_from_set_cookie(landing.headers());
        let form = request_form(request);

        let count_url = format!("{}/pt/xls/ptXlsDownDataCheck.do", self.base_uri);
        let mut count_request = self
            .client
            .post(count_url)
            .header(USER_AGENT, &self.user_agent)
            .header(REFERER, &landing_url)
            .header(ACCEPT, "application/json, text/javascript, */*; q=0.01")
            .form(&form);
        if let Some(cookie) = cookie_header.as_deref() {
            count_request = count_request.header(COOKIE, cookie);
        }
        let count_response = count_request
            .send()
            .await
            .context("failed to check rt.molit.go.kr export row count")?;
        if !count_response.status().is_success() {
            bail!(
                "rt.molit.go.kr count check returned HTTP {}",
                count_response.status()
            );
        }
        let count_body = count_response
            .text()
            .await
            .context("failed to read rt.molit.go.kr count response")?;
        let provider_count = parse_count_response(&count_body)?;

        let csv_url = format!("{}/pt/xls/ptXlsCSVDown.do", self.base_uri);
        let mut csv_request = self
            .client
            .post(csv_url)
            .header(USER_AGENT, &self.user_agent)
            .header(REFERER, &landing_url)
            .header(ACCEPT, "text/csv, */*; q=0.01")
            .form(&form);
        if let Some(cookie) = cookie_header.as_deref() {
            csv_request = csv_request.header(COOKIE, cookie);
        }
        let csv_response = csv_request
            .send()
            .await
            .context("failed to download rt.molit.go.kr CSV export")?;
        if !csv_response.status().is_success() {
            bail!(
                "rt.molit.go.kr CSV export returned HTTP {}",
                csv_response.status()
            );
        }
        let content_type = csv_response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let raw_payload = csv_response
            .bytes()
            .await
            .context("failed to read rt.molit.go.kr CSV export body")?
            .to_vec();
        reject_html_payload(&raw_payload, content_type.as_deref())?;

        Ok(RtMolitFetchedExport {
            provider_count,
            raw_payload,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RtMolitFetchedExport {
    provider_count: u64,
    raw_payload: Vec<u8>,
}

async fn persist_plan(
    config: &RtMolitExportIngestConfig,
    provider_count: u64,
    plan: RtMolitRealTransactionExportPlan,
) -> anyhow::Result<()> {
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for rt.molit.go.kr export ingest")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = bronze_object_storage_from_env()
        .await
        .context("failed to configure object storage for rt.molit.go.kr export ingest")?;
    let started_at = Utc::now();
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(config, started_at))
        .await
        .context("failed to upsert rt.molit.go.kr source catalog entry")?;
    let run = uow
        .create_ingestion_run(&ingestion_run(
            source.id,
            run_id,
            started_at,
            run_request_params(config, provider_count),
        ))
        .await
        .context("failed to create rt.molit.go.kr export ingestion run")?;
    let writer = BronzeObjectStorageWriter::new(storage.as_ref());
    let record = bronze_object_record(&source, &run, started_at, provider_count, &plan);
    let outcome = BRONZE_COMMITTER
        .commit(
            &writer,
            &uow,
            PlannedBronzeObject {
                object_key: plan.object_key.as_str().to_owned(),
                payload: BronzePayload::InMemory(plan.raw_payload),
                content_type: BRONZE_CONTENT_TYPE.to_owned(),
                cache_control: BRONZE_CACHE_CONTROL.to_owned(),
                checksum_sha256: plan.checksum_sha256.clone(),
                record,
            },
        )
        .await
        .context("failed to commit rt.molit.go.kr real-transaction export Bronze object")?;

    let completed = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run.id,
            status: IngestionRunStatus::Succeeded,
            finished_at: Utc::now(),
            logical_records_seen: provider_count,
            objects_written: 1,
            error_message: None,
        })
        .await
        .context("failed to complete rt.molit.go.kr export ingestion run")?;

    tracing::info!(
        run_id = %completed.id,
        object_key = %outcome.object_key,
        checksum_sha256 = %outcome.checksum_sha256,
        provider_count,
        "rt.molit.go.kr real-transaction CSV export live write succeeded"
    );
    Ok(())
}

fn bronze_object_record(
    source: &SourceCatalogEntry,
    run: &IngestionRun,
    collected_at: chrono::DateTime<Utc>,
    provider_count: u64,
    plan: &RtMolitRealTransactionExportPlan,
) -> BronzeObject {
    BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id: source.id,
        ingestion_run_id: run.id,
        source_record_id: None,
        source_partition_key: Some(plan.source_partition_key.clone()),
        source_identity_key: plan.source_identity_key.clone(),
        dedupe_key: plan.dedupe_key.clone(),
        request_params: plan.request_params.clone(),
        object_key: plan.object_key.clone(),
        checksum_sha256: plan.checksum_sha256.clone(),
        content_type: BRONZE_CONTENT_TYPE.to_owned(),
        size_bytes: plan.size_bytes,
        logical_record_count: Some(provider_count),
        collected_at,
        snapshot_period: plan.snapshot_period.clone(),
        snapshot_date: plan.snapshot_date,
        snapshot_granularity: plan.snapshot_granularity,
        snapshot_basis: plan.snapshot_basis,
        provider_file_id: None,
        provider_file_name: None,
        provider_updated_at: None,
        effective_date: None,
        created_at: collected_at,
    }
}

fn source_catalog_entry(
    config: &RtMolitExportIngestConfig,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: config.source_slug.clone(),
        name: config.source_name.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: config.dataset_name.clone(),
        base_url: Some(config.base_uri.clone()),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Csv,
        license_name: None,
        license_url: None,
        terms_url: Some(TERMS_URL.to_owned()),
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

const fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    run_id: IngestionRunId,
    now: chrono::DateTime<Utc>,
    request_params: JsonValue,
) -> IngestionRun {
    IngestionRun {
        id: run_id,
        source_catalog_id,
        trigger: IngestionTrigger::Manual,
        status: IngestionRunStatus::Running,
        request_params,
        started_at: now,
        finished_at: None,
        logical_records_seen: 0,
        objects_written: 0,
        error_message: None,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

fn run_request_params(config: &RtMolitExportIngestConfig, provider_count: u64) -> JsonValue {
    json!({
        "source_slug": config.source_slug,
        "contract_from": config.request.contract_from.to_string(),
        "contract_to": config.request.contract_to.to_string(),
        "thing_code": config.request.thing_code,
        "deal_type_code": config.request.deal_type_code,
        "scope": scope_summary(&config.request.scope),
        "format": config.request.response_format,
        "provider_count": provider_count,
    })
}

fn request_form(request: &RtMolitRealTransactionExportRequest) -> Vec<(&'static str, String)> {
    let (sido_code, sigungu_code, emd_code) = scope_form_values(&request.scope);
    vec![
        ("srhThingNo", request.thing_code.clone()),
        ("srhDelngSecd", request.deal_type_code.clone()),
        ("srhAddrGbn", "1".to_owned()),
        ("srhLfstsSecd", "1".to_owned()),
        ("srhFromDt", request.contract_from.to_string()),
        ("srhToDt", request.contract_to.to_string()),
        ("srhNewRonSecd", String::new()),
        ("srhSidoCd", sido_code),
        ("srhSggCd", sigungu_code),
        ("srhEmdCd", emd_code),
        ("srhRoadNm", String::new()),
        ("srhLoadCd", String::new()),
        ("srhHsmpCd", String::new()),
        ("srhArea", String::new()),
        ("srhFromAmount", String::new()),
        ("srhToAmount", String::new()),
        ("srhLrArea", String::new()),
        ("mobileAt", String::new()),
    ]
}

fn scope_form_values(scope: &RtMolitExportScope) -> (String, String, String) {
    match scope {
        RtMolitExportScope::Nationwide => (String::new(), String::new(), String::new()),
        RtMolitExportScope::Sido { sido_code } => (sido_code.clone(), String::new(), String::new()),
        RtMolitExportScope::Sigungu {
            sido_code,
            sigungu_code,
        } => (sido_code.clone(), sigungu_code.clone(), String::new()),
        RtMolitExportScope::Emd {
            sido_code,
            sigungu_code,
            emd_code,
        } => (sido_code.clone(), sigungu_code.clone(), emd_code.clone()),
    }
}

fn scope_summary(scope: &RtMolitExportScope) -> JsonValue {
    match scope {
        RtMolitExportScope::Nationwide => json!({ "kind": "nationwide" }),
        RtMolitExportScope::Sido { sido_code } => {
            json!({ "kind": "sido", "sido_code": sido_code })
        }
        RtMolitExportScope::Sigungu {
            sido_code,
            sigungu_code,
        } => json!({
            "kind": "sigungu",
            "sido_code": sido_code,
            "sigungu_code": sigungu_code,
        }),
        RtMolitExportScope::Emd {
            sido_code,
            sigungu_code,
            emd_code,
        } => json!({
            "kind": "emd",
            "sido_code": sido_code,
            "sigungu_code": sigungu_code,
            "emd_code": emd_code,
        }),
    }
}

fn cookie_header_from_set_cookie(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let cookies = headers
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if cookies.is_empty() {
        None
    } else {
        Some(cookies.join("; "))
    }
}

fn parse_count_response(body: &str) -> anyhow::Result<u64> {
    let response: JsonValue =
        serde_json::from_str(body).context("rt.molit.go.kr count response must be JSON")?;
    if let Some(message) = response.get("error").and_then(JsonValue::as_str) {
        bail!("rt.molit.go.kr provider rejected count check: {message}");
    }
    response
        .get("cnt")
        .and_then(JsonValue::as_u64)
        .context("rt.molit.go.kr count response is missing numeric cnt")
}

fn reject_html_payload(raw_payload: &[u8], content_type: Option<&str>) -> anyhow::Result<()> {
    if content_type.is_some_and(|value| value.to_ascii_lowercase().contains("text/html")) {
        bail!("rt.molit.go.kr CSV endpoint returned HTML content-type");
    }
    let prefix = String::from_utf8_lossy(&raw_payload[..raw_payload.len().min(128)]);
    if prefix
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("<html")
    {
        bail!("rt.molit.go.kr CSV endpoint returned an HTML document");
    }
    Ok(())
}

fn parse_date_env(name: &str) -> anyhow::Result<NaiveDate> {
    let value = required_env_value(name)?;
    NaiveDate::parse_from_str(&value, "%Y-%m-%d")
        .with_context(|| format!("{name} must be a date in YYYY-MM-DD format"))
}

fn live_write_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn request_form_matches_rt_molit_field_names() -> anyhow::Result<()> {
        let request = RtMolitRealTransactionExportRequest {
            thing_code: "A".to_owned(),
            deal_type_code: "1".to_owned(),
            contract_from: NaiveDate::from_ymd_opt(2026, 6, 1).context("valid date")?,
            contract_to: NaiveDate::from_ymd_opt(2026, 6, 30).context("valid date")?,
            scope: RtMolitExportScope::Nationwide,
            response_format: "csv".to_owned(),
        };

        let form = request_form(&request);

        assert!(form.contains(&("srhThingNo", "A".to_owned())));
        assert!(form.contains(&("srhDelngSecd", "1".to_owned())));
        assert!(form.contains(&("srhFromDt", "2026-06-01".to_owned())));
        assert!(form.contains(&("srhToDt", "2026-06-30".to_owned())));
        Ok(())
    }

    #[test]
    fn request_form_includes_sigungu_scope_fields() -> anyhow::Result<()> {
        let request = RtMolitRealTransactionExportRequest {
            thing_code: "A".to_owned(),
            deal_type_code: "1".to_owned(),
            contract_from: NaiveDate::from_ymd_opt(2026, 6, 1).context("valid date")?,
            contract_to: NaiveDate::from_ymd_opt(2026, 6, 30).context("valid date")?,
            scope: RtMolitExportScope::Sigungu {
                sido_code: "11000".to_owned(),
                sigungu_code: "11680".to_owned(),
            },
            response_format: "csv".to_owned(),
        };

        let form = request_form(&request);

        assert!(form.contains(&("srhSidoCd", "11000".to_owned())));
        assert!(form.contains(&("srhSggCd", "11680".to_owned())));
        assert!(form.contains(&("srhEmdCd", String::new())));
        Ok(())
    }

    #[test]
    fn cookie_header_keeps_only_cookie_pairs() -> anyhow::Result<()> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.append(
            SET_COOKIE,
            HeaderValue::from_static("JSESSIONID=abc; Path=/; HttpOnly"),
        );
        headers.append(
            SET_COOKIE,
            HeaderValue::from_static("other=def; Path=/; Secure"),
        );

        assert_eq!(
            cookie_header_from_set_cookie(&headers).as_deref(),
            Some("JSESSIONID=abc; other=def")
        );
        Ok(())
    }

    #[test]
    fn html_payload_is_rejected() {
        assert!(reject_html_payload(b"  <html><body>error</body></html>", None).is_err());
        assert!(reject_html_payload(b"csv,data\n", Some("text/html;charset=utf-8")).is_err());
    }

    #[test]
    fn count_response_rejects_provider_error_payloads_with_actionable_message() {
        let error = parse_count_response(r#"{"error":"일일 다운로드 횟수는 최대 100건 입니다."}"#)
            .err()
            .expect("provider error payload must fail");

        assert!(
            error.to_string().contains("provider rejected count check"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("일일 다운로드 횟수는 최대 100건 입니다."),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn count_response_parses_provider_count() -> anyhow::Result<()> {
        assert_eq!(parse_count_response(r#"{"cnt":123}"#)?, 123);
        Ok(())
    }

    #[test]
    fn thing_and_deal_codes_derive_dataset_slug_as_single_source() -> anyhow::Result<()> {
        let cases = [
            (("A", "1"), "real_transaction_apartment_trade"),
            (("A", "2"), "real_transaction_apartment_rent"),
            (("B", "1"), "real_transaction_row_house_trade"),
            (("B", "2"), "real_transaction_row_house_rent"),
            (("C", "1"), "real_transaction_detached_house_trade"),
            (("C", "2"), "real_transaction_detached_house_rent"),
            (("D", "1"), "real_transaction_officetel_trade"),
            (("D", "2"), "real_transaction_officetel_rent"),
            (("E", "1"), "real_transaction_apartment_presale"),
            (("F", "1"), "real_transaction_commercial_trade"),
            (("G", "1"), "real_transaction_land_trade"),
            (("H", "1"), "real_transaction_industrial_trade"),
        ];
        for ((thing, deal), expected) in cases {
            assert_eq!(rt_molit_dataset_slug(thing, deal)?, expected);
        }
        assert!(rt_molit_dataset_slug("H", "2").is_err());
        Ok(())
    }

    #[test]
    fn explicit_input_derives_canonical_ingest_config_without_env_mutation() -> anyhow::Result<()> {
        let input = RtMolitExportIngestInput {
            request: RtMolitRealTransactionExportRequest {
                thing_code: "A".to_owned(),
                deal_type_code: "1".to_owned(),
                contract_from: NaiveDate::from_ymd_opt(2026, 6, 1).context("valid date")?,
                contract_to: NaiveDate::from_ymd_opt(2026, 6, 30).context("valid date")?,
                scope: RtMolitExportScope::Sigungu {
                    sido_code: "11000".to_owned(),
                    sigungu_code: "11680".to_owned(),
                },
                response_format: "csv".to_owned(),
            },
            base_uri: Some("https://example.test".to_owned()),
            user_agent: Some("test-agent".to_owned()),
            live_write: Some("1".to_owned()),
        };

        let config = RtMolitExportIngestConfig::from_input(input)?;

        assert_eq!(
            config.source_slug,
            "rtmolitkr__real_transaction_apartment_trade"
        );
        assert_eq!(
            config.source_name,
            "rt.molit.go.kr Real Transaction (real_transaction_apartment_trade)"
        );
        assert_eq!(config.dataset_name, "real-transaction-apartment-trade");
        assert_eq!(config.base_uri, "https://example.test");
        assert_eq!(config.user_agent, "test-agent");
        assert_eq!(config.live_write.as_deref(), Some("1"));
        Ok(())
    }
}
