//! `PostgreSQL` atomicity and concurrency proofs for Gold publication.

use std::sync::Arc;

use lakehouse_application::ports::{
    IndustrialComplexGoldPointerReader, LakehousePublicationUnitOfWork,
};
use lakehouse_application::PublishIndustrialComplexGoldPointerCommand;
use lakehouse_domain::LakehouseError;
use lakehouse_infrastructure::{
    PgIndustrialComplexGoldPointerReader, PgLakehousePublicationUnitOfWork,
};
use sqlx::PgPool;
use tokio::sync::Barrier;
use uuid::Uuid;

mod support;

use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");
const GOLD_EVENT_TYPE: &str = "catalog.industrial_complex.gold_pointer.published.v1";

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn outbox_failure_rolls_back_pointer_source_files_and_event() -> TestResult {
    let suffix = Uuid::now_v7().simple().to_string();
    let label = format!("lakehouse_gold_atomicity_{}", &suffix[..12]);
    run_in_disposable_database(&label, move |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex_id = insert_complex(&pool, &suffix).await?;
        let command = publish_command(complex_id, &suffix, "gold-v1", None);
        let trigger = OutboxFailureTrigger::install(&pool, &suffix).await?;
        let unit_of_work = PgLakehousePublicationUnitOfWork::new(pool.clone());

        let result = unit_of_work
            .publish_industrial_complex_gold_pointer(command.clone())
            .await;
        trigger.remove(&pool).await?;

        assert!(result.is_err(), "injected outbox failure must surface");
        assert_eq!(count_pointer(&pool, complex_id).await?, 0);
        assert_eq!(
            count_source(&pool, command.source_external_id.as_deref()).await?,
            0
        );
        assert_eq!(count_file(&pool, &command.profile_object_key).await?, 0);
        assert_eq!(count_event(&pool, complex_id).await?, 0);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn concurrent_publish_with_same_expectation_allows_exactly_one_winner() -> TestResult {
    let suffix = Uuid::now_v7().simple().to_string();
    let label = format!("lakehouse_gold_concurrency_{}", &suffix[..12]);
    run_in_disposable_database(&label, move |pool| async move {
        MIGRATOR.run(&pool).await?;
        let complex_id = insert_complex(&pool, &suffix).await?;
        let unit_of_work = Arc::new(PgLakehousePublicationUnitOfWork::new(pool.clone()));
        let barrier = Arc::new(Barrier::new(3));
        let first = spawn_publish(
            unit_of_work.clone(),
            barrier.clone(),
            publish_command(complex_id, &format!("{suffix}a"), "gold-v1", None),
        );
        let second = spawn_publish(
            unit_of_work,
            barrier.clone(),
            publish_command(complex_id, &format!("{suffix}b"), "gold-v2", None),
        );

        barrier.wait().await;
        let (first, second) = tokio::join!(first, second);
        let results = [first?, second?];

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(LakehouseError::IndustrialComplexGoldPointerVersionConflict { .. })
                ))
                .count(),
            1
        );
        assert_eq!(count_pointer(&pool, complex_id).await?, 1);
        assert_eq!(count_event(&pool, complex_id).await?, 1);
        let reader = PgIndustrialComplexGoldPointerReader::new(pool.clone());
        let found = reader
            .find_industrial_complex_gold_pointer(complex_id)
            .await?
            .ok_or_else(|| LakehouseError::Persistence("Gold pointer missing".to_owned()))?;
        let listed = reader
            .list_industrial_complex_gold_pointers(&[complex_id])
            .await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].current_version, found.current_version);
        Ok(())
    })
    .await
}

fn spawn_publish(
    unit_of_work: Arc<PgLakehousePublicationUnitOfWork>,
    barrier: Arc<Barrier>,
    command: PublishIndustrialComplexGoldPointerCommand,
) -> tokio::task::JoinHandle<Result<lakehouse_domain::IndustrialComplexGoldPointer, LakehouseError>>
{
    tokio::spawn(async move {
        barrier.wait().await;
        unit_of_work
            .publish_industrial_complex_gold_pointer(command)
            .await
    })
}

fn publish_command(
    complex_id: foundation_shared_kernel::ids::ComplexId,
    suffix: &str,
    current_version: &str,
    expected_current_version: Option<String>,
) -> PublishIndustrialComplexGoldPointerCommand {
    PublishIndustrialComplexGoldPointerCommand {
        complex_id,
        current_version: current_version.to_owned(),
        expected_current_version,
        profile_object_key: format!("gold/industrial-complex/profiles/{suffix}.json"),
        spatial_locator_object_key: Some(format!(
            "gold/industrial-complex/spatial-locators/{suffix}.parquet"
        )),
        source: "foundation-platform.test.gold".to_owned(),
        source_url: None,
        source_external_id: Some(format!("gold-test-{suffix}")),
        source_snapshot_id: format!("source-{suffix}"),
        iceberg_snapshot_id: format!("iceberg-{suffix}"),
        profile_row_count: 1,
        profile_size_bytes: 512,
        spatial_locator_size_bytes: Some(1024),
        profile_checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_owned(),
        published_at: chrono::Utc::now(),
    }
}

async fn insert_complex(
    pool: &PgPool,
    suffix: &str,
) -> TestResult<foundation_shared_kernel::ids::ComplexId> {
    let id = Uuid::now_v7();
    let digits = suffix
        .bytes()
        .filter(u8::is_ascii_digit)
        .take(10)
        .map(char::from)
        .collect::<String>();
    let primary_bjdong_code = format!("{digits:0<10}");
    sqlx::query(
        "INSERT INTO catalog.industrial_complex
         (id, official_complex_code, name, kind, primary_bjdong_code, area_m2)
         VALUES ($1, $2, $3, 'general', $4, 1000)",
    )
    .bind(id)
    .bind(format!("GOLD-{suffix}"))
    .bind(format!("Gold test {suffix}"))
    .bind(primary_bjdong_code)
    .execute(pool)
    .await?;
    Ok(foundation_shared_kernel::ids::ComplexId::new(id))
}

async fn count_pointer(
    pool: &PgPool,
    complex_id: foundation_shared_kernel::ids::ComplexId,
) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM catalog.industrial_complex_gold_pointer WHERE complex_id = $1",
    )
    .bind(complex_id.as_uuid())
    .fetch_one(pool)
    .await?)
}

async fn count_source(pool: &PgPool, external_id: Option<&str>) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM catalog.source_record WHERE external_id IS NOT DISTINCT FROM $1",
    )
    .bind(external_id)
    .fetch_one(pool)
    .await?)
}

async fn count_file(pool: &PgPool, object_key: &str) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM catalog.file_asset WHERE object_key = $1")
            .bind(object_key)
            .fetch_one(pool)
            .await?,
    )
}

async fn count_event(
    pool: &PgPool,
    complex_id: foundation_shared_kernel::ids::ComplexId,
) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM catalog.outbox_event
         WHERE type = $1 AND payload->>'complex_id' = $2",
    )
    .bind(GOLD_EVENT_TYPE)
    .bind(complex_id.to_string())
    .fetch_one(pool)
    .await?)
}

struct OutboxFailureTrigger {
    trigger_name: String,
    function_name: String,
}

impl OutboxFailureTrigger {
    async fn install(pool: &PgPool, suffix: &str) -> TestResult<Self> {
        let trigger_name = format!("fail_gold_outbox_{}", &suffix[..12]);
        let function_name = format!("fail_gold_outbox_fn_{}", &suffix[..12]);
        let sql = format!(
            "CREATE FUNCTION catalog.{function_name}() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.type = '{GOLD_EVENT_TYPE}' THEN
                     RAISE EXCEPTION 'forced Gold outbox failure' USING ERRCODE = 'P0001';
                 END IF;
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER {trigger_name}
             BEFORE INSERT ON catalog.outbox_event
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
            "DROP TRIGGER IF EXISTS {} ON catalog.outbox_event;
             DROP FUNCTION IF EXISTS catalog.{}();",
            self.trigger_name, self.function_name
        );
        sqlx::raw_sql(&sql).execute(pool).await?;
        Ok(())
    }
}
