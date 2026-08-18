//! Use-case tests for writing source-side industrial-complex rows into Catalog.

use std::sync::Mutex;

use async_trait::async_trait;
use catalog_application::{
    ports::{
        CatalogUnitOfWork, UpsertIndustrialComplexCommand, UpsertIndustrialComplexEffect,
        UpsertIndustrialComplexOutcome,
    },
    IndustrialComplexCatalogRow, UpsertIndustrialComplexCatalogRows,
    UpsertIndustrialComplexCatalogRowsInput,
};
use catalog_domain::{
    CatalogError, ComplexMutation, IndustrialComplex, IndustrialComplexKind, Parcel, ParcelKind,
    VectorTileManifest,
};
use foundation_shared_kernel::ids::{ComplexId, ParcelId, StaffId};
use uuid::Uuid;

#[derive(Default)]
struct RecordingCatalogUnitOfWork {
    commands: Mutex<Vec<UpsertIndustrialComplexCommand>>,
}

#[async_trait]
impl CatalogUnitOfWork for RecordingCatalogUnitOfWork {
    async fn create_complex(&self, _complex: &IndustrialComplex) -> Result<(), CatalogError> {
        Err(unexpected_call("create_complex"))
    }

    async fn update_complex(
        &self,
        _id: ComplexId,
        _expected_version: i64,
        _mutate: ComplexMutation,
    ) -> Result<IndustrialComplex, CatalogError> {
        Err(unexpected_call("update_complex"))
    }

    async fn upsert_complexes_by_official_code(
        &self,
        commands: &[UpsertIndustrialComplexCommand],
    ) -> Result<Vec<UpsertIndustrialComplexOutcome>, CatalogError> {
        self.commands
            .lock()
            .map_err(|_| CatalogError::Infrastructure("commands mutex poisoned".to_owned()))?
            .extend(commands.iter().cloned());
        Ok(commands
            .iter()
            .map(|command| UpsertIndustrialComplexOutcome {
                complex: IndustrialComplex {
                    id: ComplexId::new(Uuid::now_v7()),
                    official_complex_code: command.official_complex_code.clone(),
                    name: command.name.clone(),
                    kind: command.kind,
                    primary_bjdong_code: command.primary_bjdong_code.clone(),
                    area_m2: command.area_m2,
                    lakehouse_complex_id: None,
                    status: None,
                    sido_code: None,
                    sigungu_code: None,
                    address_text: None,
                    management_agency_name: None,
                    developer_name: None,
                    designated_date: None,
                    construction_start_date: None,
                    completion_date: None,
                    development_progress_percent: None,
                    lot_sales_status: None,
                    business_period_raw: None,
                    business_period_start_month: None,
                    business_period_end_month: None,
                    designation_basis_law_raw: None,
                    development_method_raw: None,
                    development_purpose_raw: None,
                    invited_industries_raw: None,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                    archived_at: None,
                    version: 1,
                },
                effect: UpsertIndustrialComplexEffect::Inserted,
            })
            .collect())
    }

    async fn archive_complex(
        &self,
        _id: ComplexId,
        _expected_version: i64,
        _operator_staff_id: StaffId,
        _reason: Option<String>,
        _request_id: Option<String>,
    ) -> Result<IndustrialComplex, CatalogError> {
        Err(unexpected_call("archive_complex"))
    }

    async fn update_parcel_kind(
        &self,
        _id: ParcelId,
        _expected_version: i64,
        _new_kind: ParcelKind,
        _applied_by: StaffId,
    ) -> Result<Parcel, CatalogError> {
        Err(unexpected_call("update_parcel_kind"))
    }

    async fn rollback_vector_tile_manifest(
        &self,
        _command: catalog_application::ports::VectorTileManifestRollbackCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        Err(unexpected_call("rollback_vector_tile_manifest"))
    }

    async fn promote_vector_tile_manifest(
        &self,
        _command: catalog_application::ports::VectorTileManifestPromotionCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        Err(unexpected_call("promote_vector_tile_manifest"))
    }
}

fn unexpected_call(method: &'static str) -> CatalogError {
    CatalogError::Infrastructure(format!("unexpected CatalogUnitOfWork::{method} call"))
}

#[tokio::test]
async fn writes_valid_source_side_rows() -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = UpsertIndustrialComplexCatalogRows::new(uow.clone());

    let report = use_case
        .execute(UpsertIndustrialComplexCatalogRowsInput {
            rows: vec![
                IndustrialComplexCatalogRow {
                    official_complex_code: "SYNTHETIC-COMPLEX-001".to_owned(),
                    name: "Synthetic Industrial Complex Alpha".to_owned(),
                    kind: IndustrialComplexKind::General,
                    primary_bjdong_code: Some("9999900101".to_owned()),
                    area_m2: 123_456,
                    lakehouse_complex_id: None,
                    status: None,
                    sido_code: None,
                    sigungu_code: None,
                    address_text: None,
                    management_agency_name: None,
                    developer_name: None,
                    designated_date: None,
                    construction_start_date: None,
                    completion_date: None,
                    development_progress_percent: None,
                    lot_sales_status: None,
                    business_period_raw: None,
                    business_period_start_month: None,
                    business_period_end_month: None,
                    designation_basis_law_raw: None,
                    development_method_raw: None,
                    development_purpose_raw: None,
                    invited_industries_raw: None,
                },
                IndustrialComplexCatalogRow {
                    official_complex_code: "111010".to_owned(),
                    name: "Sourced Industrial Complex".to_owned(),
                    kind: IndustrialComplexKind::National,
                    primary_bjdong_code: None,
                    area_m2: 3_708_451,
                    lakehouse_complex_id: None,
                    status: None,
                    sido_code: None,
                    sigungu_code: None,
                    address_text: None,
                    management_agency_name: None,
                    developer_name: None,
                    designated_date: None,
                    construction_start_date: None,
                    completion_date: None,
                    development_progress_percent: None,
                    lot_sales_status: None,
                    business_period_raw: None,
                    business_period_start_month: None,
                    business_period_end_month: None,
                    designation_basis_law_raw: None,
                    development_method_raw: None,
                    development_purpose_raw: None,
                    invited_industries_raw: None,
                },
            ],
        })
        .await?;

    assert_eq!(report.written_count(), 2);
    assert_eq!(
        report.count_with(UpsertIndustrialComplexEffect::Inserted),
        2
    );
    let commands = {
        let commands = uow
            .commands
            .lock()
            .map_err(|_| CatalogError::Infrastructure("commands mutex poisoned".to_owned()))?;
        commands.clone()
    };
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0].official_complex_code, "SYNTHETIC-COMPLEX-001");
    assert_eq!(
        commands[0].primary_bjdong_code.as_deref(),
        Some("9999900101")
    );
    // A complex whose source resolved no legal-dong code reaches the write path unchanged rather
    // than being rejected or filled in (root ADR-0040).
    assert_eq!(commands[1].primary_bjdong_code, None);
    Ok(())
}

#[tokio::test]
async fn rejects_placeholder_official_codes_before_writing() {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = UpsertIndustrialComplexCatalogRows::new(uow);

    let result = use_case
        .execute(UpsertIndustrialComplexCatalogRowsInput {
            rows: vec![IndustrialComplexCatalogRow {
                official_complex_code: "foundation-platform:00000000-0000-7000-8000-000000000001"
                    .to_owned(),
                name: "Synthetic Industrial Complex Alpha".to_owned(),
                kind: IndustrialComplexKind::General,
                primary_bjdong_code: Some("9999900101".to_owned()),
                area_m2: 123_456,
                lakehouse_complex_id: None,
                status: None,
                sido_code: None,
                sigungu_code: None,
                address_text: None,
                management_agency_name: None,
                developer_name: None,
                designated_date: None,
                construction_start_date: None,
                completion_date: None,
                development_progress_percent: None,
                lot_sales_status: None,
                business_period_raw: None,
                business_period_start_month: None,
                business_period_end_month: None,
                designation_basis_law_raw: None,
                development_method_raw: None,
                development_purpose_raw: None,
                invited_industries_raw: None,
            }],
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
}
