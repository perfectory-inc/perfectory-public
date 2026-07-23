//! Use-case tests for importing source-side industrial-complex seed rows into Catalog.

use std::sync::Mutex;

use async_trait::async_trait;
use catalog_application::{
    ports::{CatalogUnitOfWork, UpsertIndustrialComplexCommand},
    ImportIndustrialComplexCatalogSeed, ImportIndustrialComplexCatalogSeedInput,
    IndustrialComplexCatalogSeedRow,
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
    ) -> Result<Vec<IndustrialComplex>, CatalogError> {
        self.commands
            .lock()
            .map_err(|_| CatalogError::Infrastructure("commands mutex poisoned".to_owned()))?
            .extend(commands.iter().cloned());
        Ok(commands
            .iter()
            .map(|command| IndustrialComplex {
                id: ComplexId::new(Uuid::now_v7()),
                official_complex_code: command.official_complex_code.clone(),
                name: command.name.clone(),
                kind: command.kind,
                primary_bjdong_code: command.primary_bjdong_code.clone(),
                area_m2: command.area_m2,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                archived_at: None,
                version: 1,
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
async fn imports_valid_source_side_seed_rows() -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = ImportIndustrialComplexCatalogSeed::new(uow.clone());

    let report = use_case
        .execute(ImportIndustrialComplexCatalogSeedInput {
            rows: vec![IndustrialComplexCatalogSeedRow {
                official_complex_code: "SYNTHETIC-COMPLEX-001".to_owned(),
                name: "Synthetic Industrial Complex Alpha".to_owned(),
                kind: IndustrialComplexKind::General,
                primary_bjdong_code: "9999900101".to_owned(),
                area_m2: 123_456,
            }],
        })
        .await?;

    assert_eq!(report.imported_count, 1);
    let commands = {
        let commands = uow
            .commands
            .lock()
            .map_err(|_| CatalogError::Infrastructure("commands mutex poisoned".to_owned()))?;
        commands.clone()
    };
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].official_complex_code, "SYNTHETIC-COMPLEX-001");
    assert_eq!(commands[0].primary_bjdong_code, "9999900101");
    Ok(())
}

#[tokio::test]
async fn rejects_placeholder_official_codes_before_writing() {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = ImportIndustrialComplexCatalogSeed::new(uow);

    let result = use_case
        .execute(ImportIndustrialComplexCatalogSeedInput {
            rows: vec![IndustrialComplexCatalogSeedRow {
                official_complex_code: "foundation-platform:00000000-0000-7000-8000-000000000001"
                    .to_owned(),
                name: "Synthetic Industrial Complex Alpha".to_owned(),
                kind: IndustrialComplexKind::General,
                primary_bjdong_code: "9999900101".to_owned(),
                area_m2: 123_456,
            }],
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
}
