//! Industrial-complex update use-case tests.

use std::sync::Mutex;

use async_trait::async_trait;
use catalog_application::ports::CatalogUnitOfWork;
use catalog_application::{
    ArchiveIndustrialComplex, ArchiveIndustrialComplexInput, UpdateIndustrialComplex,
    UpdateIndustrialComplexInput,
};
use catalog_domain::{
    CatalogError, ComplexMutation, IndustrialComplex, IndustrialComplexKind, Parcel, ParcelKind,
    VectorTileManifest,
};
use foundation_shared_kernel::ids::{ComplexId, ParcelId, StaffId};
use uuid::Uuid;

#[derive(Clone, Debug)]
struct RecordedUpdate {
    id: ComplexId,
    expected_version: i64,
    mutation: ComplexMutation,
}

#[derive(Clone, Debug)]
struct RecordedArchive {
    id: ComplexId,
    expected_version: i64,
    operator_staff_id: StaffId,
    reason: Option<String>,
    request_id: Option<String>,
}

#[derive(Default)]
struct RecordingCatalogUnitOfWork {
    archives: Mutex<Vec<RecordedArchive>>,
    updates: Mutex<Vec<RecordedUpdate>>,
}

impl RecordingCatalogUnitOfWork {
    fn recorded_archives(&self) -> Result<Vec<RecordedArchive>, CatalogError> {
        self.archives
            .lock()
            .map(|records| records.clone())
            .map_err(|_| CatalogError::Infrastructure("archives mutex poisoned".to_owned()))
    }

    fn recorded_updates(&self) -> Result<Vec<RecordedUpdate>, CatalogError> {
        self.updates
            .lock()
            .map(|records| records.clone())
            .map_err(|_| CatalogError::Infrastructure("updates mutex poisoned".to_owned()))
    }
}

#[async_trait]
impl CatalogUnitOfWork for RecordingCatalogUnitOfWork {
    async fn create_complex(&self, _complex: &IndustrialComplex) -> Result<(), CatalogError> {
        Err(unexpected_call("create_complex"))
    }

    async fn upsert_complexes_by_official_code(
        &self,
        _commands: &[catalog_application::ports::UpsertIndustrialComplexCommand],
    ) -> Result<Vec<IndustrialComplex>, CatalogError> {
        Err(unexpected_call("upsert_complexes_by_official_code"))
    }

    async fn update_complex(
        &self,
        id: ComplexId,
        expected_version: i64,
        mutation: ComplexMutation,
    ) -> Result<IndustrialComplex, CatalogError> {
        self.updates
            .lock()
            .map_err(|_| CatalogError::Infrastructure("updates mutex poisoned".to_owned()))?
            .push(RecordedUpdate {
                id,
                expected_version,
                mutation: mutation.clone(),
            });

        Ok(IndustrialComplex {
            id,
            official_complex_code: "SYNTHETIC-COMPLEX-001".to_owned(),
            name: mutation
                .name
                .unwrap_or_else(|| "Synthetic Industrial Complex Alpha".to_owned()),
            kind: IndustrialComplexKind::General,
            primary_bjdong_code: "9999900101".to_owned(),
            area_m2: mutation.area_m2.unwrap_or(123_456),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            archived_at: None,
            version: expected_version + 1,
        })
    }

    async fn archive_complex(
        &self,
        id: ComplexId,
        expected_version: i64,
        operator_staff_id: StaffId,
        reason: Option<String>,
        request_id: Option<String>,
    ) -> Result<IndustrialComplex, CatalogError> {
        self.archives
            .lock()
            .map_err(|_| CatalogError::Infrastructure("archives mutex poisoned".to_owned()))?
            .push(RecordedArchive {
                id,
                expected_version,
                operator_staff_id,
                reason,
                request_id,
            });

        Ok(IndustrialComplex {
            id,
            official_complex_code: "SYNTHETIC-COMPLEX-001".to_owned(),
            name: "Synthetic Industrial Complex Alpha".to_owned(),
            kind: IndustrialComplexKind::General,
            primary_bjdong_code: "9999900101".to_owned(),
            area_m2: 123_456,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            archived_at: Some(chrono::Utc::now()),
            version: expected_version + 1,
        })
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
async fn archives_complex_with_operator_reason_and_expected_version() -> Result<(), CatalogError> {
    let complex_id = ComplexId::new(Uuid::now_v7());
    let operator_staff_id = StaffId::new(Uuid::now_v7());
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = ArchiveIndustrialComplex::new(uow.clone());

    let archived_complex = use_case
        .execute(ArchiveIndustrialComplexInput {
            complex_id,
            expected_version: 4,
            operator_staff_id,
            reason: Some("duplicate source record".to_owned()),
            request_id: Some("archive-req-1".to_owned()),
        })
        .await?;

    assert_eq!(archived_complex.id, complex_id);
    assert_eq!(archived_complex.version, 5);
    assert!(archived_complex.archived_at.is_some());

    let archive_records = uow.recorded_archives()?;
    assert_eq!(archive_records.len(), 1);
    assert_eq!(archive_records[0].id, complex_id);
    assert_eq!(archive_records[0].expected_version, 4);
    assert_eq!(archive_records[0].operator_staff_id, operator_staff_id);
    assert_eq!(
        archive_records[0].reason.as_deref(),
        Some("duplicate source record")
    );
    assert_eq!(
        archive_records[0].request_id.as_deref(),
        Some("archive-req-1")
    );
    Ok(())
}

#[tokio::test]
async fn rejects_archive_with_non_positive_expected_version_before_writing(
) -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = ArchiveIndustrialComplex::new(uow.clone());

    let result = use_case
        .execute(ArchiveIndustrialComplexInput {
            complex_id: ComplexId::new(Uuid::now_v7()),
            expected_version: 0,
            operator_staff_id: StaffId::new(Uuid::now_v7()),
            reason: Some("duplicate source record".to_owned()),
            request_id: None,
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
    assert!(uow.recorded_archives()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejects_archive_with_blank_reason_before_writing() -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = ArchiveIndustrialComplex::new(uow.clone());

    let result = use_case
        .execute(ArchiveIndustrialComplexInput {
            complex_id: ComplexId::new(Uuid::now_v7()),
            expected_version: 3,
            operator_staff_id: StaffId::new(Uuid::now_v7()),
            reason: Some(" ".to_owned()),
            request_id: None,
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
    assert!(uow.recorded_archives()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn updates_name_and_area_with_expected_version() -> Result<(), CatalogError> {
    let complex_id = ComplexId::new(Uuid::now_v7());
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = UpdateIndustrialComplex::new(uow.clone());

    let updated_complex = use_case
        .execute(UpdateIndustrialComplexInput {
            complex_id,
            expected_version: 3,
            name: Some("Synthetic Industrial Complex Gamma".to_owned()),
            area_m2: Some(9_600_000),
        })
        .await?;

    assert_eq!(updated_complex.id, complex_id);
    assert_eq!(updated_complex.name, "Synthetic Industrial Complex Gamma");
    assert_eq!(updated_complex.area_m2, 9_600_000);
    assert_eq!(updated_complex.version, 4);

    let update_records = uow.recorded_updates()?;
    assert_eq!(update_records.len(), 1);
    assert_eq!(update_records[0].id, complex_id);
    assert_eq!(update_records[0].expected_version, 3);
    assert_eq!(
        update_records[0].mutation.name.as_deref(),
        Some("Synthetic Industrial Complex Gamma")
    );
    assert_eq!(update_records[0].mutation.area_m2, Some(9_600_000));
    Ok(())
}

#[tokio::test]
async fn rejects_empty_update_before_writing() -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = UpdateIndustrialComplex::new(uow.clone());

    let result = use_case
        .execute(UpdateIndustrialComplexInput {
            complex_id: ComplexId::new(Uuid::now_v7()),
            expected_version: 3,
            name: None,
            area_m2: None,
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
    assert!(uow.recorded_updates()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejects_blank_name_before_writing() -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = UpdateIndustrialComplex::new(uow.clone());

    let result = use_case
        .execute(UpdateIndustrialComplexInput {
            complex_id: ComplexId::new(Uuid::now_v7()),
            expected_version: 3,
            name: Some(" ".to_owned()),
            area_m2: Some(9_600_000),
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
    assert!(uow.recorded_updates()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejects_non_positive_expected_version_before_writing() -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = UpdateIndustrialComplex::new(uow.clone());

    let result = use_case
        .execute(UpdateIndustrialComplexInput {
            complex_id: ComplexId::new(Uuid::now_v7()),
            expected_version: 0,
            name: Some("Synthetic Industrial Complex Gamma".to_owned()),
            area_m2: None,
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
    assert!(uow.recorded_updates()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejects_zero_area_before_writing() -> Result<(), CatalogError> {
    let uow = std::sync::Arc::new(RecordingCatalogUnitOfWork::default());
    let use_case = UpdateIndustrialComplex::new(uow.clone());

    let result = use_case
        .execute(UpdateIndustrialComplexInput {
            complex_id: ComplexId::new(Uuid::now_v7()),
            expected_version: 3,
            name: None,
            area_m2: Some(0),
        })
        .await;

    assert!(matches!(
        result,
        Err(CatalogError::InvalidIndustrialComplexInput(_))
    ));
    assert!(uow.recorded_updates()?.is_empty());
    Ok(())
}
