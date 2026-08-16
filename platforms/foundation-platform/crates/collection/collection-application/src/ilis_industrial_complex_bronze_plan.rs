//! Planning helpers for ILIS (industryland.or.kr) industrial-complex Bronze ingestion pages.
//!
//! ILIS serves its list endpoints as `POST` with a JSON body, and it honours a page size large
//! enough to return the whole result set in one response. That is why this lane's request carries a
//! `pageSize` parameter name rather than data.go.kr's `numOfRows`, and no format parameter at all:
//! the recorded lineage has to say what was actually sent, not what a neighbouring lane sends.

use chrono::NaiveDate;
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::Value as JsonValue;

use crate::{
    plan_public_data_bronze_page, PublicDataBronzePagePlan, PublicDataBronzePagePlanInput,
    PublicDataBronzePageRequest, PublicDataBronzePlanError, PublicDataPageRequest,
    PublicDataPartitionField,
};

/// Request parameters for one ILIS page.
///
/// The partition is carried on the request because this lane serves two shapes: the national bulk
/// endpoints (`scope=national`, one object for the whole country) and the per-complex detail
/// endpoint (`complex=<code>`, one object each). Both go through the same key compile, so neither
/// can invent its own object layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IlisIndustrialComplexPageRequest {
    /// Provider-native operation, for example `danjiList`.
    pub operation: String,
    /// Provider-neutral partition name included in the Bronze object key.
    pub partition_name: String,
    /// Provider-neutral partition value included in the Bronze object key.
    pub partition_value: String,
    /// One-based page number.
    pub page_no: u32,
    /// Requested page size. The bulk lane sets this above the known total row count on purpose.
    pub page_size: u32,
    /// JSON pointer to the logical records array inside the provider response.
    pub logical_items_pointer: String,
    /// Field-path suffixes scored as candidate source keys when non-null.
    pub candidate_key_field_suffixes: Vec<String>,
}

impl IlisIndustrialComplexPageRequest {
    /// Converts the ILIS request into the provider-neutral public-data planner request.
    ///
    /// # Errors
    /// Returns [`PublicDataBronzePlanError`] when fields cannot be represented canonically.
    pub fn to_public_data_request(
        &self,
    ) -> Result<PublicDataBronzePageRequest, PublicDataBronzePlanError> {
        Ok(PublicDataBronzePageRequest {
            operation: self.operation.clone(),
            partition_fields: vec![PublicDataPartitionField {
                name: self.partition_name.clone(),
                value: self.partition_value.clone(),
            }],
            query_params: std::collections::BTreeMap::new(),
            format_query_param: None,
            page_param_name: "pageNo".to_owned(),
            size_param_name: "pageSize".to_owned(),
            page_no: self.page_no,
            num_of_rows: self.page_size,
        })
    }
}

/// Input required to plan one immutable ILIS Bronze page object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IlisIndustrialComplexBronzePagePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run that owns this object.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters.
    pub request: IlisIndustrialComplexPageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// Planned metadata for one immutable ILIS raw response page.
pub type IlisIndustrialComplexBronzePagePlan = PublicDataBronzePagePlan;

/// Plans object metadata for one immutable ILIS raw response page.
///
/// # Errors
/// Returns [`PublicDataBronzePlanError`] when request parameters cannot be represented in the
/// canonical Bronze object layout.
pub fn plan_ilis_industrial_complex_bronze_page(
    input: IlisIndustrialComplexBronzePagePlanInput<'_>,
) -> Result<IlisIndustrialComplexBronzePagePlan, PublicDataBronzePlanError> {
    plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug: input.source_slug,
        ingest_date: input.ingest_date,
        ingestion_run_id: input.ingestion_run_id,
        logical_items_pointer: &input.request.logical_items_pointer,
        candidate_key_field_suffixes: input.request.candidate_key_field_suffixes.clone(),
        request: input.request.to_public_data_request()?,
        raw_payload: input.raw_payload,
        payload: input.payload,
    })
}

impl PublicDataPageRequest for IlisIndustrialComplexPageRequest {
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
        plan_ilis_industrial_complex_bronze_page(IlisIndustrialComplexBronzePagePlanInput {
            source_slug,
            ingest_date,
            ingestion_run_id,
            request: self.clone(),
            raw_payload,
            payload,
        })
    }
}
