//! `PostgreSQL` round-trip tests for Collection Bronze ingestion metadata.

#![allow(clippy::expect_used, clippy::print_stderr, clippy::unwrap_used)]

use chrono::{NaiveDate, Utc};
use collection_application::ports::{
    BronzeIngestRepository, BronzeIngestUnitOfWork, CompleteIngestionRunCommand,
};
use collection_domain::{
    BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SchemaObservedType,
    SchemaProfile, SnapshotBasis, SnapshotGranularity, SourceAuthKind, SourceCatalogEntry,
    SourcePayloadFormat,
};
use collection_infrastructure::{PgBronzeIngestRepository, PgBronzeIngestUnitOfWork};
use foundation_shared_kernel::ids::{
    BronzeObjectId, IngestionRunId, SchemaProfileId, SourceCatalogId,
};
use foundation_shared_kernel::ObjectKey;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    match PgPool::connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skipping - could not connect to DATABASE_URL: {e}");
            None
        }
    }
}

#[tokio::test]
#[ignore = "requires local docker stack"]
#[allow(clippy::too_many_lines)]
async fn bronze_ingest_round_trip_preserves_source_run_object_batch_and_schema_profile() {
    let Some(pool) = pool().await else {
        return;
    };
    let repo = PgBronzeIngestRepository::new(pool.clone());
    let uow = PgBronzeIngestUnitOfWork::new(pool.clone());
    let fixture = BronzeFixture::new();

    let source = uow
        .upsert_source_catalog_entry(&fixture.source)
        .await
        .expect("upsert source catalog entry");
    assert_eq!(source.slug, fixture.source.slug);
    assert_eq!(source.auth_kind, SourceAuthKind::ServiceKey);
    assert_eq!(source.payload_format, SourcePayloadFormat::Json);

    let run = uow
        .create_ingestion_run(&BronzeFixture::run(source.id))
        .await
        .expect("create ingestion run");
    assert_eq!(run.status, IngestionRunStatus::Running);
    assert_eq!(run.logical_records_seen, 0);

    let raw = uow
        .record_bronze_object(&fixture.bronze_object(source.id, run.id, Uuid::now_v7()))
        .await
        .expect("record bronze object");
    let duplicate = uow
        .record_bronze_object(&fixture.bronze_object(source.id, run.id, Uuid::now_v7()))
        .await
        .expect("record duplicate bronze object");
    assert_eq!(duplicate.id, raw.id);
    assert_eq!(duplicate.dedupe_key, raw.dedupe_key);
    assert_eq!(duplicate.object_key, raw.object_key);
    assert_eq!(duplicate.logical_record_count, Some(3));

    let mut corrected_duplicate_object = fixture.bronze_object(source.id, run.id, Uuid::now_v7());
    corrected_duplicate_object.logical_record_count = Some(4);
    corrected_duplicate_object.request_params =
        json!({ "pageNo": 1, "numOfRows": 100, "metadataRevision": 2 });
    let corrected_duplicate = uow
        .record_bronze_object(&corrected_duplicate_object)
        .await
        .expect("record corrected duplicate bronze object metadata");
    assert_eq!(corrected_duplicate.id, raw.id);
    assert_eq!(corrected_duplicate.logical_record_count, Some(4));
    assert_eq!(corrected_duplicate.request_params["metadataRevision"], 2);

    let first_profile = uow
        .upsert_schema_profile(&BronzeFixture::schema_profile(
            source.id,
            run.id,
            "items[].pnu",
            SchemaObservedType::String,
            3,
            0,
        ))
        .await
        .expect("upsert first schema profile");
    let second_profile = uow
        .upsert_schema_profile(&BronzeFixture::schema_profile(
            source.id,
            run.id,
            "items[].area",
            SchemaObservedType::Number,
            2,
            1,
        ))
        .await
        .expect("upsert second schema profile");
    assert_eq!(first_profile.field_path, "items[].pnu");
    assert_eq!(second_profile.observed_type, SchemaObservedType::Number);

    let completed = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run.id,
            status: IngestionRunStatus::Succeeded,
            finished_at: Utc::now(),
            logical_records_seen: 4,
            objects_written: 1,
            error_message: None,
        })
        .await
        .expect("complete ingestion run");
    assert_eq!(completed.status, IngestionRunStatus::Succeeded);
    assert_eq!(completed.logical_records_seen, 4);
    assert_eq!(completed.objects_written, 1);
    assert!(completed.finished_at.is_some());

    let found_source = repo
        .find_source_catalog_by_slug(&fixture.source.slug)
        .await
        .expect("find source by slug")
        .expect("source exists");
    assert_eq!(found_source.id, source.id);
    assert_eq!(found_source.provider, "data.go.kr");

    let found_run = repo
        .find_ingestion_run(run.id)
        .await
        .expect("find run")
        .expect("run exists");
    assert_eq!(found_run.status, IngestionRunStatus::Succeeded);
    assert_eq!(found_run.request_params["sigunguCd"], "11110");

    let bronze_objects = repo
        .list_bronze_objects_by_run(run.id)
        .await
        .expect("list bronze objects");
    assert_eq!(bronze_objects.len(), 1);
    assert_eq!(
        bronze_objects[0].source_partition_key.as_deref(),
        Some("sigungu=11110/month=202605")
    );
    assert_eq!(
        bronze_objects[0].source_identity_key,
        "sigungu=11110/month=202605/page=000001/page_size=100"
    );
    assert_eq!(
        bronze_objects[0].snapshot_period.as_deref(),
        Some("2026-05")
    );
    assert_eq!(
        bronze_objects[0].snapshot_date,
        NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()
    );
    assert_eq!(
        bronze_objects[0].snapshot_granularity,
        SnapshotGranularity::Month
    );
    assert_eq!(
        bronze_objects[0].snapshot_basis,
        SnapshotBasis::RequestMonth
    );
    assert_eq!(
        bronze_objects[0].object_key.as_str(),
        fixture.bronze_object_key.as_str()
    );
    assert_eq!(bronze_objects[0].logical_record_count, Some(4));

    let profiles = repo
        .list_schema_profiles_by_run(run.id)
        .await
        .expect("list schema profiles");
    assert_eq!(profiles.len(), 2);
    assert_eq!(profiles[0].field_path, "items[].area");
    assert_eq!(profiles[1].field_path, "items[].pnu");

    cleanup(&pool, source.id).await;
}

// Audit finding 6 / Codex F6: a re-ingest that hits the dedupe conflict must adopt the latest
// upload's object_key and ingestion_run_id, so the row points at the object the new run wrote
// instead of a stale key (the bulk path uploads before this upsert). Otherwise the freshly
// uploaded object is orphaned and the row references the original.
#[tokio::test]
#[ignore = "requires local docker stack"]
async fn bronze_dedupe_conflict_adopts_latest_object_key_and_run() {
    let Some(pool) = pool().await else {
        return;
    };
    let repo = PgBronzeIngestRepository::new(pool.clone());
    let uow = PgBronzeIngestUnitOfWork::new(pool.clone());
    let fixture = BronzeFixture::new();

    let source = uow
        .upsert_source_catalog_entry(&fixture.source)
        .await
        .expect("upsert source catalog entry");

    let first_run = uow
        .create_ingestion_run(&BronzeFixture::run(source.id))
        .await
        .expect("create first ingestion run");
    let original = uow
        .record_bronze_object(&fixture.bronze_object(source.id, first_run.id, Uuid::now_v7()))
        .await
        .expect("record original bronze object");

    // A later run re-ingests the same dedupe_key (identical bytes) but writes to a new object
    // key; the bulk path has already uploaded this new object before recording it.
    let second_run = uow
        .create_ingestion_run(&BronzeFixture::run(source.id))
        .await
        .expect("create second ingestion run");
    let mut reingest = fixture.bronze_object(source.id, second_run.id, Uuid::now_v7());
    reingest.object_key = ObjectKey::parse(&format!("{}-reingest", reingest.object_key.as_str()))
        .expect("valid re-ingest object key");

    let adopted = uow
        .record_bronze_object(&reingest)
        .await
        .expect("record re-ingested bronze object");

    // Same logical row (dedupe conflict), but now pointing at the latest upload + run.
    assert_eq!(adopted.id, original.id);
    assert_eq!(adopted.object_key.as_str(), reingest.object_key.as_str());
    assert_ne!(adopted.object_key.as_str(), original.object_key.as_str());
    assert_eq!(adopted.ingestion_run_id, second_run.id);

    // The row is now associated with the second run, not the first.
    let second_run_objects = repo
        .list_bronze_objects_by_run(second_run.id)
        .await
        .expect("list bronze objects for second run");
    assert_eq!(second_run_objects.len(), 1);
    assert_eq!(
        second_run_objects[0].object_key.as_str(),
        reingest.object_key.as_str()
    );
    let first_run_objects = repo
        .list_bronze_objects_by_run(first_run.id)
        .await
        .expect("list bronze objects for first run");
    assert!(first_run_objects.is_empty());

    cleanup(&pool, source.id).await;
}

// A live smoke exposed this gap: if the object-key contract stays stable but the identity/dedupe
// format changes (for example adding page_size to the identity), `record_bronze_object` must converge
// onto the existing physical object row instead of creating a second catalog row pointing at the same
// R2 object.
#[tokio::test]
#[ignore = "requires local docker stack"]
async fn bronze_object_key_conflict_adopts_latest_identity_without_duplicate_row() {
    let Some(pool) = pool().await else {
        return;
    };
    let uow = PgBronzeIngestUnitOfWork::new(pool.clone());
    let fixture = BronzeFixture::new();

    let source = uow
        .upsert_source_catalog_entry(&fixture.source)
        .await
        .expect("upsert source catalog entry");

    let first_run = uow
        .create_ingestion_run(&BronzeFixture::run(source.id))
        .await
        .expect("create first ingestion run");
    let original = uow
        .record_bronze_object(&fixture.bronze_object(source.id, first_run.id, Uuid::now_v7()))
        .await
        .expect("record original bronze object");

    let second_run = uow
        .create_ingestion_run(&BronzeFixture::run(source.id))
        .await
        .expect("create second ingestion run");
    let mut revised_identity = fixture.bronze_object(source.id, second_run.id, Uuid::now_v7());
    revised_identity.source_identity_key =
        "sigungu=11110/month=202605/page=000001/page_size=100/revision=2".to_owned();
    revised_identity.dedupe_key = "parcel-registry:11110-202605:page-1:v2".to_owned();
    revised_identity.logical_record_count = Some(5);

    let adopted = uow
        .record_bronze_object(&revised_identity)
        .await
        .expect("record revised identity for the same bronze object key");

    assert_eq!(adopted.id, original.id);
    assert_eq!(adopted.object_key, original.object_key);
    assert_eq!(
        adopted.source_identity_key,
        revised_identity.source_identity_key
    );
    assert_eq!(adopted.dedupe_key, revised_identity.dedupe_key);
    assert_eq!(adopted.ingestion_run_id, second_run.id);
    assert_eq!(adopted.logical_record_count, Some(5));

    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM catalog.bronze_object
         WHERE source_catalog_id = $1 AND object_key = $2",
    )
    .bind(source.id.as_uuid())
    .bind(original.object_key.as_str())
    .fetch_one(&pool)
    .await
    .expect("count bronze objects by physical key");
    assert_eq!(row_count, 1);

    cleanup(&pool, source.id).await;
}

struct BronzeFixture {
    source: SourceCatalogEntry,
    bronze_object_key: ObjectKey,
}

impl BronzeFixture {
    fn new() -> Self {
        let now = Utc::now();
        let suffix = Uuid::new_v4().simple().to_string();
        let slug = format!("datagokr__parcel_registry_{suffix}");
        let bronze_object_key = ObjectKey::parse(&format!(
            "bronze/datagokr/parcel-registry/{suffix}/page-1.json"
        ))
        .expect("valid object key");
        Self {
            source: SourceCatalogEntry {
                id: SourceCatalogId::new(Uuid::now_v7()),
                slug,
                name: "data.go.kr parcel registry API".to_owned(),
                provider: "data.go.kr".to_owned(),
                dataset_name: "parcel_registry".to_owned(),
                base_url: Some("https://apis.data.go.kr/1613000/BldRgstService_v2".to_owned()),
                auth_kind: SourceAuthKind::ServiceKey,
                payload_format: SourcePayloadFormat::Json,
                license_name: Some("public-data-portal".to_owned()),
                license_url: Some("https://www.data.go.kr".to_owned()),
                terms_url: Some("https://www.data.go.kr/ugs/selectPortalPolicyView.do".to_owned()),
                collection_frequency: Some("daily".to_owned()),
                is_active: true,
                created_at: now,
                updated_at: now,
                version: 1,
            },
            bronze_object_key,
        }
    }

    fn run(source_catalog_id: SourceCatalogId) -> IngestionRun {
        let now = Utc::now();
        IngestionRun {
            id: IngestionRunId::new(Uuid::now_v7()),
            source_catalog_id,
            trigger: IngestionTrigger::Manual,
            status: IngestionRunStatus::Running,
            request_params: json!({ "sigunguCd": "11110", "bjdongCd": "10100" }),
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

    fn bronze_object(
        &self,
        source_catalog_id: SourceCatalogId,
        ingestion_run_id: IngestionRunId,
        id: Uuid,
    ) -> BronzeObject {
        BronzeObject {
            id: BronzeObjectId::new(id),
            source_catalog_id,
            ingestion_run_id,
            source_record_id: None,
            source_partition_key: Some("sigungu=11110/month=202605".to_owned()),
            source_identity_key: "sigungu=11110/month=202605/page=000001/page_size=100".to_owned(),
            dedupe_key: "parcel-registry:11110-202605:page-1".to_owned(),
            request_params: json!({ "pageNo": 1, "numOfRows": 100 }),
            object_key: self.bronze_object_key.clone(),
            checksum_sha256: "a".repeat(64),
            content_type: "application/json".to_owned(),
            size_bytes: 4096,
            logical_record_count: Some(3),
            collected_at: Utc::now(),
            snapshot_period: Some("2026-05".to_owned()),
            snapshot_date: NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
            snapshot_granularity: SnapshotGranularity::Month,
            snapshot_basis: SnapshotBasis::RequestMonth,
            provider_file_id: None,
            provider_file_name: None,
            provider_updated_at: None,
            effective_date: Some(NaiveDate::from_ymd_opt(2026, 5, 13).unwrap()),
            created_at: Utc::now(),
        }
    }

    fn schema_profile(
        source_catalog_id: SourceCatalogId,
        ingestion_run_id: IngestionRunId,
        field_path: &str,
        observed_type: SchemaObservedType,
        nonnull_count: u64,
        null_count: u64,
    ) -> SchemaProfile {
        let now = Utc::now();
        SchemaProfile {
            id: SchemaProfileId::new(Uuid::now_v7()),
            source_catalog_id,
            ingestion_run_id,
            field_path: field_path.to_owned(),
            observed_type,
            nonnull_count,
            null_count,
            sample_values: json!(["sample"]),
            candidate_key_score: 0.8,
            profiled_at: now,
            created_at: now,
            updated_at: now,
            version: 1,
        }
    }
}

async fn cleanup(pool: &PgPool, source_catalog_id: SourceCatalogId) {
    sqlx::query("DELETE FROM catalog.schema_profile WHERE source_catalog_id = $1")
        .bind(source_catalog_id.as_uuid())
        .execute(pool)
        .await
        .expect("cleanup schema profile");
    sqlx::query("DELETE FROM catalog.bronze_object WHERE source_catalog_id = $1")
        .bind(source_catalog_id.as_uuid())
        .execute(pool)
        .await
        .expect("cleanup bronze object");
    sqlx::query("DELETE FROM catalog.ingestion_run WHERE source_catalog_id = $1")
        .bind(source_catalog_id.as_uuid())
        .execute(pool)
        .await
        .expect("cleanup ingestion run");
    sqlx::query("DELETE FROM catalog.source_catalog WHERE id = $1")
        .bind(source_catalog_id.as_uuid())
        .execute(pool)
        .await
        .expect("cleanup source catalog");
}
