//! Planning helpers for the data.go.kr 행정표준코드 (`getStanReginCdList`) Bronze snapshot pages.
//!
//! `getStanReginCdList` carries no date filter — it always returns the *current* nationwide
//! 법정동코드 list — so each periodic fetch is a distinct temporal coverage slice. We record that
//! slice as a `snapshot=YYYYMMDD` partition, which keeps every snapshot's raw bytes as its own
//! immutable Bronze object (append-only, no overwrite) and is exactly the "request-scope partition
//! that distinguishes the requested coverage slice" the Bronze object-lake convention keeps in the
//! API-page key (FP-ADR-0019). The cross-snapshot difference is what the registry job turns into a
//! 시군구 crosswalk (ADR-0104), so retaining every snapshot is a correctness requirement, not just
//! lineage.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use foundation_shared_kernel::ids::IngestionRunId;
use foundation_shared_kernel::ObjectKey;
use serde_json::Value as JsonValue;

use crate::{
    build_public_data_bronze_object_key, plan_public_data_bronze_page, PublicDataBronzePagePlan,
    PublicDataBronzePagePlanInput, PublicDataBronzePageRequest, PublicDataBronzePlanError,
    PublicDataFixedQueryParam, PublicDataPageRequest, PublicDataPartitionField,
    PublicDataSchemaObservation,
};

// getStanReginCdList returns the list as a paged JSON body shaped
// `{"StanReginCd": [{"head": [...]}, {"row": [...]}]}`; the logical records are the `row` array,
// which sits at index 1 of the `StanReginCd` array.
const LOGICAL_ITEMS_POINTER: &str = "/StanReginCd/1/row";

/// Request parameters for one 행정표준코드 (`getStanReginCdList`) snapshot page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandardCodePageRequest {
    /// 행정표준코드 API operation, i.e. `getStanReginCdList`.
    pub operation: String,
    /// The snapshot's coverage date in `YYYYMMDD` form (the fetch's as-of day).
    pub snapshot_id: String,
    /// One-based page number.
    pub page_no: u32,
    /// Requested page size. Pinned to the operation's canonical size (1000) at plan time.
    pub num_of_rows: u32,
}

impl StandardCodePageRequest {
    /// Returns the canonical provider partition key represented by this request.
    ///
    /// # Errors
    ///
    /// Returns `StandardCodeBronzePlanError` when any request parameter is invalid.
    pub fn source_partition_key(&self) -> Result<String, StandardCodeBronzePlanError> {
        self.to_public_data_request()?.source_partition_key()
    }

    /// Converts this 행정표준코드 request into the generic public-data page request.
    ///
    /// The `snapshot` partition is a coverage-slice label we assign, not a provider query parameter:
    /// the API takes no date, so `query_params` is empty and only `type=json` / `pageNo` / `numOfRows`
    /// reach the provider.
    ///
    /// # Errors
    ///
    /// Returns `StandardCodeBronzePlanError` when any request parameter is invalid.
    pub fn to_public_data_request(
        &self,
    ) -> Result<PublicDataBronzePageRequest, StandardCodeBronzePlanError> {
        validate_request(self)?;
        Ok(PublicDataBronzePageRequest {
            operation: self.operation.clone(),
            partition_fields: vec![PublicDataPartitionField {
                name: "snapshot".to_owned(),
                value: self.snapshot_id.clone(),
            }],
            query_params: BTreeMap::new(),
            format_query_param: Some(PublicDataFixedQueryParam {
                name: "type".to_owned(),
                value: "json".to_owned(),
            }),
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: self.page_no,
            num_of_rows: self.num_of_rows,
        })
    }
}

/// Input required to plan one immutable 행정표준코드 Bronze snapshot page object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandardCodeBronzePagePlanInput<'a> {
    /// Stable lowercase source slug (`datagokr__legal_dong_code`).
    pub source_slug: &'a str,
    /// Ingestion date recorded as run context and as the snapshot's canonical as-of date. The
    /// provider states no dataset-level reference date, so this is the `collected_at_fallback` basis.
    pub ingest_date: NaiveDate,
    /// Ingestion run id recorded on the `bronze_object` row.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters.
    pub request: StandardCodePageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// Planned metadata for one immutable 행정표준코드 Bronze snapshot page.
pub type StandardCodeBronzePagePlan = PublicDataBronzePagePlan;

/// Observed field statistics for one 행정표준코드 payload field path.
pub type StandardCodeSchemaObservation = PublicDataSchemaObservation;

/// Error returned while planning a 행정표준코드 Bronze page.
pub type StandardCodeBronzePlanError = PublicDataBronzePlanError;

/// Builds the canonical Bronze object key for one 행정표준코드 API snapshot page.
///
/// # Errors
///
/// Returns `StandardCodeBronzePlanError` when request parameters or key parts are invalid.
pub fn build_standard_code_bronze_object_key(
    source_slug: &str,
    request: &StandardCodePageRequest,
) -> Result<ObjectKey, StandardCodeBronzePlanError> {
    build_public_data_bronze_object_key(source_slug, &request.to_public_data_request()?)
}

/// Plans object metadata for one 행정표준코드 raw response page.
///
/// # Errors
///
/// Returns `StandardCodeBronzePlanError` when request parameters cannot be represented in the
/// canonical Bronze object layout.
pub fn plan_standard_code_bronze_page(
    input: StandardCodeBronzePagePlanInput<'_>,
) -> Result<StandardCodeBronzePagePlan, StandardCodeBronzePlanError> {
    plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug: input.source_slug,
        ingest_date: input.ingest_date,
        ingestion_run_id: input.ingestion_run_id,
        request: input.request.to_public_data_request()?,
        raw_payload: input.raw_payload,
        payload: input.payload,
        logical_items_pointer: LOGICAL_ITEMS_POINTER,
        candidate_key_field_suffixes: vec!["region_cd".to_owned()],
    })
}

impl PublicDataPageRequest for StandardCodePageRequest {
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
        plan_standard_code_bronze_page(StandardCodeBronzePagePlanInput {
            source_slug,
            ingest_date,
            ingestion_run_id,
            request: self.clone(),
            raw_payload,
            payload,
        })
    }
}

fn validate_request(request: &StandardCodePageRequest) -> Result<(), StandardCodeBronzePlanError> {
    if request.snapshot_id.len() != 8
        || !request
            .snapshot_id
            .bytes()
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(PublicDataBronzePlanError::InvalidRequest(
            "snapshot_id must be exactly 8 digits (YYYYMMDD)".to_owned(),
        ));
    }
    Ok(())
}
