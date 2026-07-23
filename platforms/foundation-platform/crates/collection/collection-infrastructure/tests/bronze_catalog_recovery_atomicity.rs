//! `PostgreSQL` atomicity proof for evidence-driven Bronze Catalog recovery.

use chrono::{NaiveDate, Utc};
use collection_application::{
    bronze_catalog_recovery::{
        ApplyBronzeCatalogRecoveryCommand, BronzeCatalogRecoveryCatalogWriter,
    },
    ports::CompleteIngestionRunCommand,
};
use collection_domain::{
    BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SnapshotBasis,
    SnapshotGranularity, SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::ObjectKey;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn late_object_failure_rolls_back_source_run_and_all_objects() -> TestResult {
    let pool = pool().await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let source_slug = format!("testprovider__bronze_recovery_{suffix}");
    let command = recovery_command(&source_slug)?;
    let run_id = command.run.id;
    let second_object_key = command.objects[1].object_key.as_str().to_owned();
    let trigger = FailureTrigger::install(&pool, &suffix, &second_object_key).await?;
    let writer = PgBronzeIngestUnitOfWork::new(pool.clone());

    let result = writer.apply_recovery(command).await;
    trigger.remove(&pool).await?;

    let source_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM catalog.source_catalog WHERE slug = $1")
            .bind(&source_slug)
            .fetch_one(&pool)
            .await?;
    let run_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM catalog.ingestion_run WHERE id = $1")
            .bind(run_id.as_uuid())
            .fetch_one(&pool)
            .await?;
    let object_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.bronze_object WHERE ingestion_run_id = $1",
    )
    .bind(run_id.as_uuid())
    .fetch_one(&pool)
    .await?;
    sqlx::query("DELETE FROM catalog.source_catalog WHERE slug = $1")
        .bind(&source_slug)
        .execute(&pool)
        .await?;

    assert!(
        result.is_err(),
        "injected second-object failure must surface"
    );
    assert_eq!(source_count, 0, "source upsert leaked from failed batch");
    assert_eq!(run_count, 0, "recovery run leaked from failed batch");
    assert_eq!(
        object_count, 0,
        "partial recovered objects leaked from failed batch"
    );
    Ok(())
}

async fn pool() -> TestResult<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is required for the ignored PostgreSQL atomicity test")?;
    Ok(PgPool::connect(&url).await?)
}

struct FailureTrigger {
    trigger_name: String,
    function_name: String,
}

impl FailureTrigger {
    async fn install(pool: &PgPool, suffix: &str, object_key: &str) -> TestResult<Self> {
        let trigger_name = format!("fail_bronze_recovery_{suffix}");
        let function_name = format!("fail_bronze_recovery_{suffix}");
        let sql = format!(
            "CREATE FUNCTION catalog.{function_name}() RETURNS trigger AS $$
             BEGIN
                 IF NEW.object_key = '{object_key}' THEN
                     RAISE EXCEPTION 'forced second recovery object failure';
                 END IF;
                 RETURN NEW;
             END;
             $$ LANGUAGE plpgsql;
             CREATE TRIGGER {trigger_name}
             BEFORE INSERT OR UPDATE ON catalog.bronze_object
             FOR EACH ROW EXECUTE FUNCTION catalog.{function_name}();"
        );
        sqlx::raw_sql(&sql).execute(pool).await?;
        Ok(Self {
            trigger_name,
            function_name,
        })
    }

    async fn remove(self, pool: &PgPool) -> TestResult {
        let sql = format!(
            "DROP TRIGGER IF EXISTS {} ON catalog.bronze_object;
             DROP FUNCTION IF EXISTS catalog.{}();",
            self.trigger_name, self.function_name
        );
        sqlx::raw_sql(&sql).execute(pool).await?;
        Ok(())
    }
}

fn recovery_command(source_slug: &str) -> TestResult<ApplyBronzeCatalogRecoveryCommand> {
    let now = Utc::now();
    let source_id = SourceCatalogId::new(Uuid::now_v7());
    let run_id = IngestionRunId::new(Uuid::now_v7());
    let source = SourceCatalogEntry {
        id: source_id,
        slug: source_slug.to_owned(),
        name: "Bronze recovery atomicity fixture".to_owned(),
        provider: "testprovider".to_owned(),
        dataset_name: "bronze_recovery".to_owned(),
        base_url: Some("https://example.invalid".to_owned()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    };
    let run = IngestionRun {
        id: run_id,
        source_catalog_id: source_id,
        trigger: IngestionTrigger::Replay,
        status: IngestionRunStatus::Running,
        request_params: json!({"catalog_recovery": {"kind": "evidence_rehydration"}}),
        started_at: now,
        finished_at: None,
        logical_records_seen: 0,
        objects_written: 0,
        error_message: None,
        created_at: now,
        updated_at: now,
        version: 1,
    };
    let objects = [1_u32, 2]
        .into_iter()
        .map(|page| recovered_object(source_slug, source_id, run_id, page, now))
        .collect::<TestResult<Vec<_>>>()?;

    Ok(ApplyBronzeCatalogRecoveryCommand {
        source,
        run,
        objects,
        completion: CompleteIngestionRunCommand {
            id: run_id,
            status: IngestionRunStatus::Succeeded,
            finished_at: now,
            logical_records_seen: 2,
            objects_written: 0,
            error_message: None,
        },
    })
}

fn recovered_object(
    source_slug: &str,
    source_id: SourceCatalogId,
    run_id: IngestionRunId,
    page: u32,
    now: chrono::DateTime<Utc>,
) -> TestResult<BronzeObject> {
    let object_key =
        format!("bronze/source={source_slug}/sigungu=11680/bjdong=10300/page-{page:06}.json");
    Ok(BronzeObject {
        id: BronzeObjectId::new(Uuid::now_v7()),
        source_catalog_id: source_id,
        ingestion_run_id: run_id,
        source_record_id: None,
        source_partition_key: Some(format!("sigungu=11680/bjdong=10300/page={page}")),
        source_identity_key: format!("sigungu=11680/bjdong=10300/page={page}"),
        dedupe_key: format!("{source_slug}:page={page}:sha256={}", "a".repeat(64)),
        request_params: json!({
            "pageNo": page,
            "catalog_recovery": {"kind": "evidence_rehydration"}
        }),
        object_key: ObjectKey::parse(&object_key)?,
        checksum_sha256: "a".repeat(64),
        content_type: "application/json".to_owned(),
        size_bytes: 12,
        logical_record_count: Some(1),
        collected_at: now,
        snapshot_period: Some("2026-07".to_owned()),
        snapshot_date: NaiveDate::parse_from_str("2026-07-01", "%Y-%m-%d")?,
        snapshot_granularity: SnapshotGranularity::Month,
        snapshot_basis: SnapshotBasis::CollectedAtFallback,
        provider_file_id: None,
        provider_file_name: None,
        provider_updated_at: None,
        effective_date: None,
        created_at: now,
    })
}
