//! Catalog/Bronze entity builders shared by the persist and reconcile paths.
//!
//! Pure mappers from a `BuildingRegisterBronzePagePlan` (+ run/source identity) to the catalog
//! domain entities (`SourceCatalogEntry`, `IngestionRun`, `BronzeObject`, `SchemaProfile`) plus the
//! batch request-params snapshot. No I/O.

use chrono::Utc;
use collection_application::BuildingRegisterBronzePagePlan;
use collection_domain::{
    BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SchemaProfile,
    SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

use crate::bronze_schema_profile::{
    schema_profiles_for_plans as shared_schema_profiles_for_plans, CandidateKeyOverride,
};

use super::config::BuildingRegisterIngestConfig;
use super::{BRONZE_JSON_CONTENT_TYPE, DATASET_NAME, PROVIDER, SOURCE_NAME};

pub(crate) fn source_catalog_entry(
    config: &BuildingRegisterIngestConfig,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: config.source_slug.clone(),
        name: SOURCE_NAME.to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: DATASET_NAME.to_owned(),
        base_url: Some(config.base_uri.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: Some("https://www.data.go.kr/data/15134735/openapi.do".to_owned()),
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

pub(crate) const fn ingestion_run(
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

pub(crate) fn bronze_object(
    source_catalog_id: SourceCatalogId,
    ingestion_run_id: IngestionRunId,
    now: chrono::DateTime<Utc>,
    plan: &BuildingRegisterBronzePagePlan,
) -> BronzeObject {
    BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id,
        ingestion_run_id,
        source_record_id: None,
        source_partition_key: Some(plan.source_partition_key.clone()),
        source_identity_key: plan.source_identity_key.clone(),
        dedupe_key: plan.dedupe_key.clone(),
        request_params: plan.request_params.clone(),
        object_key: plan.object_key.clone(),
        checksum_sha256: plan.checksum_sha256.clone(),
        content_type: BRONZE_JSON_CONTENT_TYPE.to_owned(),
        size_bytes: plan.size_bytes,
        logical_record_count: Some(plan.logical_record_count),
        collected_at: now,
        snapshot_period: plan.snapshot_period.clone(),
        snapshot_date: plan.snapshot_date,
        snapshot_granularity: plan.snapshot_granularity,
        snapshot_basis: plan.snapshot_basis,
        provider_file_id: None,
        provider_file_name: None,
        provider_updated_at: None,
        effective_date: None,
        created_at: now,
    }
}

pub(crate) fn batch_request_params(
    config: &BuildingRegisterIngestConfig,
    plans: &[BuildingRegisterBronzePagePlan],
) -> JsonValue {
    json!({
        "operation": config.request.operation,
        "sigunguCd": config.request.sigungu_cd,
        "bjdongCd": config.request.bjdong_cd,
        "startPageNo": config.request.page_no,
        "numOfRows": config.request.num_of_rows,
        "maxPages": config.max_pages,
        "pagesPlanned": plans.len(),
        "_type": "json"
    })
}

/// Builds schema profiles for one run, keying building-register pages on the
/// `mgmBldrgstPk` management number (the data.go.kr building primary key).
pub(crate) fn schema_profiles_for_plans(
    source_catalog_id: SourceCatalogId,
    ingestion_run_id: IngestionRunId,
    now: chrono::DateTime<Utc>,
    plans: &[BuildingRegisterBronzePagePlan],
) -> Vec<SchemaProfile> {
    shared_schema_profiles_for_plans(
        source_catalog_id,
        ingestion_run_id,
        now,
        plans,
        CandidateKeyOverride::EndsWith("mgmBldrgstPk"),
    )
}

pub(crate) fn total_logical_record_count(plans: &[BuildingRegisterBronzePagePlan]) -> u64 {
    plans.iter().map(|plan| plan.logical_record_count).sum()
}

pub(crate) fn total_size_bytes(plans: &[BuildingRegisterBronzePagePlan]) -> u64 {
    plans.iter().map(|plan| plan.size_bytes).sum()
}
