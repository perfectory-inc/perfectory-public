//! Contract tests for public-data bulk file Bronze planning.

use chrono::NaiveDate;
use collection_application::{
    plan_public_data_bulk_file, plan_public_data_bulk_file_metadata,
    plan_public_data_bulk_file_storage_location, public_data_bulk_file_source_partition_key,
    PublicDataBulkFileIdentity, PublicDataBulkFileMetadataInput, PublicDataBulkFilePlanInput,
    PublicDataBulkFileSourcePartitionKeyInput, PublicDataBulkFileStorageLocationInput,
};
use foundation_shared_kernel::ids::IngestionRunId;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn bulk_file_plan_preserves_provider_bytes_and_uses_provider_file_identity() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000101")?);
    let raw_payload = b"PK\x03\x04provider zip bytes".to_vec();

    let plan = plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: "hubgokr__building_register_main",
        ingest_date: NaiveDate::from_ymd_opt(2026, 6, 4).ok_or("invalid test date")?,
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: "building_register_main".to_owned(),
            provider_file_period: Some("2026-05".to_owned()),
            provider_snapshot_date: None,
            provider_file_id: "OPN209912310000000008".to_owned(),
            provider_file_name: "building_register_main_202605.zip".to_owned(),
            provider_updated_at: None,
        },
        raw_payload: raw_payload.clone(),
        content_type: "application/zip".to_owned(),
    })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip"
    );
    assert_eq!(
        plan.source_identity_key,
        "provider_file_id=OPN209912310000000008"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=building_register_main/provider_file_id=OPN209912310000000008"
    );
    assert_eq!(plan.snapshot_period.as_deref(), Some("2026-05"));
    assert_eq!(
        plan.snapshot_date,
        NaiveDate::from_ymd_opt(2026, 5, 1).ok_or("invalid snapshot date")?
    );
    assert_eq!(plan.snapshot_granularity.as_str(), "month");
    assert_eq!(plan.snapshot_basis.as_str(), "provider_file_period");
    assert_eq!(plan.raw_payload, raw_payload);
    assert_eq!(plan.content_type, "application/zip");
    assert_eq!(plan.size_bytes, 22);
    assert!(plan.dedupe_key.starts_with(
        "hubgokr__building_register_main:provider_file_id=OPN209912310000000008:sha256="
    ));
    assert_eq!(
        plan.request_params["provider_file_name"],
        "building_register_main_202605.zip"
    );
    Ok(())
}

#[test]
fn bulk_file_plan_rejects_path_like_provider_file_identity() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000102")?);

    let error = plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: "hubgokr__building_register_main",
        ingest_date: NaiveDate::from_ymd_opt(2026, 6, 4).ok_or("invalid test date")?,
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: "building_register_main".to_owned(),
            provider_file_period: Some("2026-05".to_owned()),
            provider_snapshot_date: None,
            provider_file_id: "../OPN209912310000000008".to_owned(),
            provider_file_name: "building_register_main_202605.zip".to_owned(),
            provider_updated_at: None,
        },
        raw_payload: b"PK\x03\x04provider zip bytes".to_vec(),
        content_type: "application/zip".to_owned(),
    })
    .err()
    .ok_or("expected invalid provider file id failure")?;

    assert!(
        error.to_string().contains("provider_file_id"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn plans_bulk_file_metadata_from_streamed_digest_without_raw_payload() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000901")?);
    let ingest_date = NaiveDate::from_ymd_opt(2026, 6, 10).ok_or("invalid ingest date")?;

    let plan = plan_public_data_bulk_file_metadata(PublicDataBulkFileMetadataInput {
        source_slug: "hubgokr__building_register_basis_outline",
        ingest_date,
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: "building_register_basis_outline".to_owned(),
            provider_file_period: Some("2026-05".to_owned()),
            provider_snapshot_date: None,
            provider_file_id: "OPN209912310000000007".to_owned(),
            provider_file_name: "building_register_202605.zip".to_owned(),
            provider_updated_at: None,
        },
        checksum_sha256: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
            .to_owned(),
        size_bytes: 4,
        content_type: "application/zip".to_owned(),
    })?;

    assert_eq!(
        plan.checksum_sha256,
        "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    );
    assert_eq!(plan.size_bytes, 4);
    assert!(!serde_json::to_string(&plan.request_params)?.contains("raw_payload"));
    Ok(())
}

#[test]
fn plans_bulk_file_storage_location_before_streamed_digest_is_known() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000902")?);
    let ingest_date = NaiveDate::from_ymd_opt(2026, 6, 10).ok_or("invalid ingest date")?;

    let plan =
        plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
            source_slug: "vworldkr__boundary_census_emd",
            ingest_date,
            ingestion_run_id: run_id,
            identity: PublicDataBulkFileIdentity {
                operation: "boundary_census_emd".to_owned(),
                provider_file_period: Some("2026-05".to_owned()),
                provider_snapshot_date: None,
                provider_file_id: "20991231DS99994-9007".to_owned(),
                provider_file_name: "SYNTHETIC_BOUNDARY_ARCHIVE.zip".to_owned(),
                provider_updated_at: None,
            },
        })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__boundary_census_emd/20991231DS99994-9007.zip"
    );
    assert_eq!(
        plan.source_identity_key,
        "provider_file_id=20991231DS99994-9007"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=boundary_census_emd/provider_file_id=20991231DS99994-9007"
    );
    Ok(())
}

#[test]
fn bulk_file_identity_keys_do_not_encode_snapshot_period() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000904")?);
    let ingest_date = NaiveDate::from_ymd_opt(2026, 6, 10).ok_or("invalid ingest date")?;

    let plan =
        plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
            source_slug: "vworldkr__land_characteristic",
            ingest_date,
            ingestion_run_id: run_id,
            identity: PublicDataBulkFileIdentity {
                operation: "land_characteristic".to_owned(),
                provider_file_period: None,
                provider_snapshot_date: Some(
                    NaiveDate::from_ymd_opt(2026, 5, 20).ok_or("invalid snapshot date")?,
                ),
                provider_file_id: "20991231DS99992-9003".to_owned(),
                provider_file_name: "SYNTHETIC_REGION.zip".to_owned(),
                provider_updated_at: Some(
                    NaiveDate::from_ymd_opt(2026, 5, 21).ok_or("invalid updated date")?,
                ),
            },
        })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__land_characteristic/20991231DS99992-9003.zip"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=land_characteristic/provider_file_id=20991231DS99992-9003"
    );
    assert_eq!(plan.snapshot_period.as_deref(), Some("2026-05"));
    assert_eq!(
        plan.snapshot_date,
        NaiveDate::from_ymd_opt(2026, 5, 20).ok_or("invalid snapshot date")?
    );
    Ok(())
}

#[test]
fn bulk_file_without_provider_period_uses_provider_updated_at_snapshot() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000903")?);
    let ingest_date = NaiveDate::from_ymd_opt(2026, 6, 10).ok_or("invalid ingest date")?;

    let plan =
        plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
            source_slug: "vworldkr__boundary_sido",
            ingest_date,
            ingestion_run_id: run_id,
            identity: PublicDataBulkFileIdentity {
                operation: "boundary_sido".to_owned(),
                provider_file_period: None,
                provider_snapshot_date: None,
                provider_file_id: "20991231DS99997-9010".to_owned(),
                provider_file_name: "SYNTHETIC_BOUNDARY_SIDO.zip".to_owned(),
                provider_updated_at: Some(
                    NaiveDate::from_ymd_opt(2099, 12, 31).ok_or("invalid updated date")?,
                ),
            },
        })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__boundary_sido/20991231DS99997-9010.zip"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=boundary_sido/provider_file_id=20991231DS99997-9010"
    );
    assert_eq!(plan.snapshot_period.as_deref(), Some("2099-12"));
    assert_eq!(
        plan.snapshot_date,
        NaiveDate::from_ymd_opt(2099, 12, 31).ok_or("invalid snapshot date")?
    );
    assert_eq!(plan.snapshot_granularity.as_str(), "day");
    assert_eq!(plan.snapshot_basis.as_str(), "provider_updated_at");
    Ok(())
}

#[test]
fn source_partition_key_is_available_without_provider_file_name() -> TestResult {
    let partition_key =
        public_data_bulk_file_source_partition_key(PublicDataBulkFileSourcePartitionKeyInput {
            operation: "building_register_main",
            provider_file_id: "OPN209912310000000008",
        })?;

    assert_eq!(
        partition_key,
        "operation=building_register_main/provider_file_id=OPN209912310000000008"
    );
    Ok(())
}
