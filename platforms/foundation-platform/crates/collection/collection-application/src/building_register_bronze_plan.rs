//! Planning helpers for building-register Bronze ingestion pages.

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

const LOGICAL_ITEMS_POINTER: &str = "/response/body/items/item";
const BUILDING_REGISTER_KEY_SUFFIX: &str = "mgmBldrgstPk";

/// Request parameters for one building-register page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterPageRequest {
    /// Building-register API operation, for example `getBrTitleInfo`.
    pub operation: String,
    /// Five-digit city/county/district code.
    pub sigungu_cd: String,
    /// Five-digit legal-dong code.
    pub bjdong_cd: String,
    /// One-based page number.
    pub page_no: u32,
    /// Requested page size.
    pub num_of_rows: u32,
}

impl BuildingRegisterPageRequest {
    /// Returns the canonical provider partition key represented by this request.
    ///
    /// # Errors
    ///
    /// Returns `BuildingRegisterBronzePlanError` when any request parameter is invalid.
    pub fn source_partition_key(&self) -> Result<String, BuildingRegisterBronzePlanError> {
        self.to_public_data_request()?.source_partition_key()
    }

    fn to_public_data_request(
        &self,
    ) -> Result<PublicDataBronzePageRequest, PublicDataBronzePlanError> {
        validate_request(self)?;
        Ok(PublicDataBronzePageRequest {
            operation: self.operation.clone(),
            partition_fields: vec![
                PublicDataPartitionField {
                    name: "sigungu".to_owned(),
                    value: self.sigungu_cd.clone(),
                },
                PublicDataPartitionField {
                    name: "bjdong".to_owned(),
                    value: self.bjdong_cd.clone(),
                },
            ],
            query_params: BTreeMap::from([
                ("sigunguCd".to_owned(), self.sigungu_cd.clone()),
                ("bjdongCd".to_owned(), self.bjdong_cd.clone()),
            ]),
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

/// Input required to plan one immutable Bronze page object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterBronzePagePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run id used in the object key.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters.
    pub request: BuildingRegisterPageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// Planned metadata for one immutable building-register Bronze page.
pub type BuildingRegisterBronzePagePlan = PublicDataBronzePagePlan;

/// Observed field statistics for one building-register payload field path.
pub type BuildingRegisterSchemaObservation = PublicDataSchemaObservation;

/// Error returned while planning a building-register Bronze page.
pub type BuildingRegisterBronzePlanError = PublicDataBronzePlanError;

/// Builds the canonical Bronze object key for one building-register API page.
///
/// # Errors
///
/// Returns `BuildingRegisterBronzePlanError` when request parameters or key parts are invalid.
pub fn build_building_register_bronze_object_key(
    source_slug: &str,
    request: &BuildingRegisterPageRequest,
) -> Result<ObjectKey, BuildingRegisterBronzePlanError> {
    build_public_data_bronze_object_key(source_slug, &request.to_public_data_request()?)
}

/// Plans object metadata for one building-register raw response page.
///
/// # Errors
///
/// Returns `BuildingRegisterBronzePlanError` when request parameters cannot be represented in the
/// canonical Bronze object layout.
pub fn plan_building_register_bronze_page(
    input: BuildingRegisterBronzePagePlanInput<'_>,
) -> Result<BuildingRegisterBronzePagePlan, BuildingRegisterBronzePlanError> {
    plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug: input.source_slug,
        ingest_date: input.ingest_date,
        ingestion_run_id: input.ingestion_run_id,
        request: input.request.to_public_data_request()?,
        raw_payload: input.raw_payload,
        payload: input.payload,
        logical_items_pointer: LOGICAL_ITEMS_POINTER,
        candidate_key_field_suffixes: vec![BUILDING_REGISTER_KEY_SUFFIX.to_owned()],
    })
}

impl PublicDataPageRequest for BuildingRegisterPageRequest {
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
        plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
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
    request: &BuildingRegisterPageRequest,
) -> Result<(), BuildingRegisterBronzePlanError> {
    validate_fixed_digits("sigunguCd", &request.sigungu_cd, 5)?;
    validate_fixed_digits("bjdongCd", &request.bjdong_cd, 5)?;
    Ok(())
}

fn validate_fixed_digits(
    name: &'static str,
    value: &str,
    len: usize,
) -> Result<(), BuildingRegisterBronzePlanError> {
    if value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(format!(
        "{name} must be exactly {len} digits"
    )))
}
