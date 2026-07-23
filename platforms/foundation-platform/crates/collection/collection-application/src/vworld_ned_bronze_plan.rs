//! Planning helpers for generic `VWorld` NED attribute API Bronze ingestion pages.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::Value as JsonValue;

use crate::{
    plan_public_data_bronze_page, PublicDataBronzePagePlan, PublicDataBronzePagePlanInput,
    PublicDataBronzePageRequest, PublicDataBronzePlanError, PublicDataFixedQueryParam,
    PublicDataPageRequest, PublicDataPartitionField, PublicDataSchemaObservation,
};

/// Request parameters for one generic `VWorld` NED attribute page.
///
/// Unlike the fixed-shape lanes (building-register / real-transaction / land-register), the generic
/// NED lane serves many operations whose logical-records pointer and candidate-key suffixes vary per
/// operation, so the request CARRIES `logical_items_pointer` + `candidate_key_field_suffixes`
/// (resolved by the ingest from its per-operation spec / env) rather than the planner hard-coding
/// them as constants. This is what lets the request supply the entire per-lane key-compile input
/// through `&self`, so the lane plugs into the [`PublicDataPageRequest`] seam the
/// [`BronzeCommitter`](crate::bronze_committer::BronzeCommitter) owns (ADR 0016).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldNedPageRequest {
    /// `VWorld` NED operation, for example `getLandCharacteristic`.
    pub operation: String,
    /// Provider-neutral partition name included in the Bronze object key.
    pub partition_name: String,
    /// Provider-neutral partition value included in the Bronze object key.
    pub partition_value: String,
    /// Provider query parameters other than key, format, pageNo, and numOfRows.
    pub query_params: BTreeMap<String, String>,
    /// One-based page number.
    pub page_no: u32,
    /// Requested page size.
    pub num_of_rows: u32,
    /// JSON pointer to the logical records array or object inside the provider response. Carried on
    /// the request (not a planner constant) because it varies per NED operation.
    pub logical_items_pointer: String,
    /// Field-path suffixes that should be scored as candidate source keys when non-null. Carried on
    /// the request (not a planner constant) because it varies per NED operation.
    pub candidate_key_field_suffixes: Vec<String>,
}

impl VWorldNedPageRequest {
    /// Converts the `VWorld` request into the provider-neutral public-data planner request.
    ///
    /// # Errors
    ///
    /// Returns `PublicDataBronzePlanError` when fields cannot be represented canonically.
    pub fn to_public_data_request(
        &self,
    ) -> Result<PublicDataBronzePageRequest, PublicDataBronzePlanError> {
        Ok(PublicDataBronzePageRequest {
            operation: self.operation.clone(),
            partition_fields: vec![PublicDataPartitionField {
                name: self.partition_name.clone(),
                value: self.partition_value.clone(),
            }],
            query_params: self.query_params.clone(),
            format_query_param: Some(PublicDataFixedQueryParam {
                name: "format".to_owned(),
                value: "json".to_owned(),
            }),
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: self.page_no,
            num_of_rows: self.num_of_rows,
        })
    }
}

/// Input required to plan one immutable generic `VWorld` NED Bronze page object.
///
/// The logical-records pointer and candidate-key suffixes are carried on the `request`
/// ([`VWorldNedPageRequest`]) — not as separate input fields — so the entire per-lane key-compile
/// input flows through `request`, matching the [`PublicDataPageRequest`] seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldNedBronzePagePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run id used in the object key.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters, including the operation's `logical_items_pointer` and
    /// `candidate_key_field_suffixes`.
    pub request: VWorldNedPageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// Planned metadata for one immutable generic `VWorld` NED raw response page.
pub type VWorldNedBronzePagePlan = PublicDataBronzePagePlan;

/// Observed field statistics for one generic `VWorld` NED payload field path.
pub type VWorldNedSchemaObservation = PublicDataSchemaObservation;

/// Error returned while planning a generic `VWorld` NED Bronze page.
pub type VWorldNedBronzePlanError = PublicDataBronzePlanError;

/// Plans object metadata for one generic `VWorld` NED raw response page.
///
/// # Errors
///
/// Returns `PublicDataBronzePlanError` when request parameters cannot be represented in the
/// canonical Bronze object layout.
pub fn plan_vworld_ned_bronze_page(
    input: VWorldNedBronzePagePlanInput<'_>,
) -> Result<VWorldNedBronzePagePlan, VWorldNedBronzePlanError> {
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

impl PublicDataPageRequest for VWorldNedPageRequest {
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
        plan_vworld_ned_bronze_page(VWorldNedBronzePagePlanInput {
            source_slug,
            ingest_date,
            ingestion_run_id,
            request: self.clone(),
            raw_payload,
            payload,
        })
    }
}
