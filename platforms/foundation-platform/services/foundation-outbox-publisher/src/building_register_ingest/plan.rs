//! Live page planning and the page-count probe.
//!
//! Fetches provider pages within the configured window, plans each into a
//! `BuildingRegisterBronzePagePlan`, asserts the page-window slice is complete, and (separately)
//! probes a single page to report how many pages a scope needs.

use std::{fs, path::Path};

use anyhow::{bail, Context};
use collection_application::{
    plan_building_register_bronze_page, BuildingRegisterBronzePagePlan,
    BuildingRegisterBronzePagePlanInput, BuildingRegisterPageRequest,
};
use collection_infrastructure::DataGoKrBuildingRegisterClient;
use foundation_shared_kernel::ids::IngestionRunId;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::pagination_guard::assert_page_window_slice_complete;

use super::config::BuildingRegisterIngestConfig;

/// One fetched building-register page: the compiled Bronze plan plus the RAW page identity +
/// parsed payload the [`BronzeCommitter`](collection_application::BronzeCommitter) needs to OWN the key-compile.
///
/// The compiled `plan` drives the page-window slice assertion and the dry-run summary here in the
/// planning stage; the persist stage hands the raw `request` + `raw_payload` + `payload` to the
/// committer, which re-runs the building-register Bronze plan as its owned compile step (ADR 0016).
#[derive(Clone, Debug)]
pub(super) struct BuildingRegisterPlannedPage {
    pub(super) plan: BuildingRegisterBronzePagePlan,
    pub(super) request: BuildingRegisterPageRequest,
    pub(super) raw_payload: Vec<u8>,
    pub(super) payload: JsonValue,
}

pub(super) async fn plan_pages(
    config: &BuildingRegisterIngestConfig,
    client: &DataGoKrBuildingRegisterClient,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
) -> anyhow::Result<Vec<BuildingRegisterPlannedPage>> {
    let mut pages = Vec::new();
    let mut last_page_observation = None;
    for (request_index, request) in page_requests_for_batch(&config.request, config.max_pages)?
        .into_iter()
        .enumerate()
    {
        if let Some(spacing) = config.request_spacing {
            spacing.wait_before_request(request_index).await;
        }
        let fetched_page = client.fetch_page(&request).await.with_context(|| {
            format!(
                "failed to fetch data.go.kr building-register page {}",
                request.page_no
            )
        })?;
        let provider_total_count =
            json_u64_pointer(&fetched_page.payload, "/response/body/totalCount")?;
        let effective_page_size =
            effective_page_size_from_response_metadata(&fetched_page.payload, request.num_of_rows)?;
        let plan = plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload.clone(),
            payload: fetched_page.payload.clone(),
        })
        .with_context(|| {
            format!(
                "failed to plan building-register Bronze page {}",
                request.page_no
            )
        })?;
        let logical_record_count = plan.logical_record_count;
        pages.push(BuildingRegisterPlannedPage {
            plan,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload,
            payload: fetched_page.payload,
        });
        last_page_observation = Some((
            request.page_no,
            effective_page_size,
            logical_record_count,
            provider_total_count,
        ));
        if should_stop_after_page(
            request.page_no,
            effective_page_size,
            logical_record_count,
            provider_total_count,
        ) {
            break;
        }
    }

    if let Some((last_page, page_size, logical_record_count, provider_total_count)) =
        last_page_observation
    {
        assert_page_window_slice_complete(
            "building-register",
            last_page,
            page_size,
            logical_record_count,
            pages
                .iter()
                .map(|page| page.plan.logical_record_count)
                .sum(),
            provider_total_count,
            config.max_pages,
            config.allow_partial_page_window,
        )?;
    }

    Ok(pages)
}

pub(super) fn effective_page_size_from_response_metadata(
    payload: &JsonValue,
    requested_page_size: u32,
) -> anyhow::Result<u32> {
    let Some(raw_page_size) = json_u64_pointer(payload, "/response/body/numOfRows")? else {
        return Ok(requested_page_size);
    };
    if raw_page_size == 0 {
        bail!("building-register response body numOfRows must be greater than zero");
    }
    u32::try_from(raw_page_size)
        .with_context(|| "building-register response body numOfRows must fit in u32")
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct BuildingRegisterPageCountProbe {
    pub(crate) operation: String,
    pub(crate) sigungu_cd: String,
    pub(crate) bjdong_cd: String,
    pub(crate) requested_page_size: u32,
    pub(crate) effective_page_size: u32,
    pub(crate) provider_total_count: u64,
    pub(crate) required_pages: u32,
}

pub(crate) fn page_count_probe_from_response_metadata(
    request: &BuildingRegisterPageRequest,
    payload: &JsonValue,
) -> anyhow::Result<BuildingRegisterPageCountProbe> {
    let effective_page_size =
        effective_page_size_from_response_metadata(payload, request.num_of_rows)?;
    let provider_total_count = json_u64_pointer(payload, "/response/body/totalCount")?
        .context("building-register response body totalCount is required for page count probe")?;
    let required_pages = required_pages_for_total_count(provider_total_count, effective_page_size)?;
    Ok(BuildingRegisterPageCountProbe {
        operation: request.operation.clone(),
        sigungu_cd: request.sigungu_cd.clone(),
        bjdong_cd: request.bjdong_cd.clone(),
        requested_page_size: request.num_of_rows,
        effective_page_size,
        provider_total_count,
        required_pages,
    })
}

fn required_pages_for_total_count(
    total_count: u64,
    effective_page_size: u32,
) -> anyhow::Result<u32> {
    if effective_page_size == 0 {
        bail!("building-register effective page size must be greater than zero");
    }
    let pages = if total_count == 0 {
        1
    } else {
        total_count.div_ceil(u64::from(effective_page_size))
    };
    u32::try_from(pages).context("building-register required page count must fit in u32")
}

pub(crate) fn write_page_count_probe_output(
    path: &Path,
    probe: &BuildingRegisterPageCountProbe,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("building-register page count probe output path must have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create building-register page count probe output directory {}",
            parent.display()
        )
    })?;
    let payload = serde_json::to_vec_pretty(probe)
        .context("failed to serialize building-register page count probe output")?;
    fs::write(path, payload).with_context(|| {
        format!(
            "failed to write building-register page count probe output {}",
            path.display()
        )
    })
}

pub(super) fn should_stop_after_page(
    page: u32,
    page_size: u32,
    logical_record_count: u64,
    provider_total_count: Option<u64>,
) -> bool {
    if let Some(total_count) = provider_total_count {
        return u64::from(page).saturating_mul(u64::from(page_size)) >= total_count;
    }
    logical_record_count < u64::from(page_size)
}

pub(super) fn json_u64_pointer(payload: &JsonValue, pointer: &str) -> anyhow::Result<Option<u64>> {
    let Some(value) = payload.pointer(pointer) else {
        return Ok(None);
    };
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Number(number) => number
            .as_u64()
            .with_context(|| {
                format!("building-register JSON field {pointer} must be an unsigned integer")
            })
            .map(Some),
        JsonValue::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<u64>()
                .with_context(|| {
                    format!("building-register JSON field {pointer} must be an unsigned integer")
                })
                .map(Some)
        }
        _ => bail!("building-register JSON field {pointer} must be an unsigned integer"),
    }
}

pub(super) fn page_requests_for_batch(
    base_request: &BuildingRegisterPageRequest,
    max_pages: u32,
) -> anyhow::Result<Vec<BuildingRegisterPageRequest>> {
    (0..max_pages)
        .map(|offset| {
            let page_no = base_request
                .page_no
                .checked_add(offset)
                .context("building-register pageNo window exceeds u32")?;
            Ok(BuildingRegisterPageRequest {
                operation: base_request.operation.clone(),
                sigungu_cd: base_request.sigungu_cd.clone(),
                bjdong_cd: base_request.bjdong_cd.clone(),
                page_no,
                num_of_rows: base_request.num_of_rows,
            })
        })
        .collect()
}
