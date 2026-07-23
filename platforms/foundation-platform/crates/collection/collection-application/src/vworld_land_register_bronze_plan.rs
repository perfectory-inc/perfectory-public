//! Planning helpers for `VWorld` land-register Bronze ingestion pages.

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

const LOGICAL_ITEMS_POINTER: &str = "/ladfrlVOList/ladfrlVOList";
const VWORLD_LAND_REGISTER_KEY_SUFFIX: &str = "pnu";

/// Request parameters for one `VWorld` land-register page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldLandRegisterPageRequest {
    /// `VWorld` NED operation, usually `ladfrlList`.
    pub operation: String,
    /// Ten-digit legal-dong PNU prefix or nineteen-digit parcel number.
    pub pnu: String,
    /// One-based page number.
    pub page_no: u32,
    /// Requested page size.
    pub num_of_rows: u32,
}

impl VWorldLandRegisterPageRequest {
    /// Returns the canonical provider partition key represented by this request.
    ///
    /// # Errors
    ///
    /// Returns `VWorldLandRegisterBronzePlanError` when any request parameter is invalid.
    pub fn source_partition_key(&self) -> Result<String, VWorldLandRegisterBronzePlanError> {
        self.to_public_data_request()?.source_partition_key()
    }

    fn to_public_data_request(
        &self,
    ) -> Result<PublicDataBronzePageRequest, VWorldLandRegisterBronzePlanError> {
        validate_request(self)?;
        Ok(PublicDataBronzePageRequest {
            operation: self.operation.clone(),
            partition_fields: vec![PublicDataPartitionField {
                name: "pnu".to_owned(),
                value: self.pnu.clone(),
            }],
            query_params: BTreeMap::from([("pnu".to_owned(), self.pnu.clone())]),
            format_query_param: Some(PublicDataFixedQueryParam {
                name: "_type".to_owned(),
                value: "json".to_owned(),
            }),
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: self.page_no,
            num_of_rows: self.num_of_rows,
        })
    }
}

/// Input required to plan one immutable `VWorld` land-register Bronze page object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldLandRegisterBronzePagePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run id used in the object key.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters.
    pub request: VWorldLandRegisterPageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// Planned metadata for one immutable `VWorld` land-register Bronze page.
pub type VWorldLandRegisterBronzePagePlan = PublicDataBronzePagePlan;

/// Observed field statistics for one `VWorld` land-register payload field path.
pub type VWorldLandRegisterSchemaObservation = PublicDataSchemaObservation;

/// Error returned while planning a `VWorld` land-register Bronze page.
pub type VWorldLandRegisterBronzePlanError = PublicDataBronzePlanError;

/// Builds the canonical Bronze object key for one `VWorld` land-register API page.
///
/// # Errors
///
/// Returns `VWorldLandRegisterBronzePlanError` when request parameters or key parts are invalid.
pub fn build_vworld_land_register_bronze_object_key(
    source_slug: &str,
    request: &VWorldLandRegisterPageRequest,
) -> Result<ObjectKey, VWorldLandRegisterBronzePlanError> {
    build_public_data_bronze_object_key(source_slug, &request.to_public_data_request()?)
}

/// Plans object metadata for one `VWorld` land-register raw response page.
///
/// # Errors
///
/// Returns `VWorldLandRegisterBronzePlanError` when request parameters cannot be represented in
/// the canonical Bronze object layout.
pub fn plan_vworld_land_register_bronze_page(
    input: VWorldLandRegisterBronzePagePlanInput<'_>,
) -> Result<VWorldLandRegisterBronzePagePlan, VWorldLandRegisterBronzePlanError> {
    plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug: input.source_slug,
        ingest_date: input.ingest_date,
        ingestion_run_id: input.ingestion_run_id,
        request: input.request.to_public_data_request()?,
        raw_payload: input.raw_payload,
        payload: input.payload,
        logical_items_pointer: LOGICAL_ITEMS_POINTER,
        candidate_key_field_suffixes: vec![VWORLD_LAND_REGISTER_KEY_SUFFIX.to_owned()],
    })
}

impl PublicDataPageRequest for VWorldLandRegisterPageRequest {
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
        plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
            source_slug,
            ingest_date,
            ingestion_run_id,
            request: self.clone(),
            raw_payload,
            payload,
        })
    }
}

fn validate_request(
    request: &VWorldLandRegisterPageRequest,
) -> Result<(), VWorldLandRegisterBronzePlanError> {
    if matches!(request.pnu.len(), 10 | 19) && request.pnu.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(
        "pnu must be either a 10-digit legal-dong prefix or exactly 19 digits".to_owned(),
    ))
}
