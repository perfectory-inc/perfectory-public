//! `PostgreSQL` rollback proof for atomic Registry artifact registration.

use foundation_shared_kernel::ids::LakehouseStorageNamespaceId;
use lakehouse_application::ports::{LakehouseRegistryRepository, LakehouseRegistryUnitOfWork};
use lakehouse_application::RegisterLakehouseObjectArtifactCommand;
use lakehouse_domain::{
    LakehouseArtifactFormat, LakehouseAssetKind, LakehouseCatalogProvider, LakehouseEnvironment,
    LakehouseNamespaceStatus, LakehouseOwnerService, LakehouseRegistryLayer,
    LakehouseStorageNamespace, LakehouseStorageProvider,
};
use lakehouse_infrastructure::{PgLakehouseRegistryRepository, PgLakehouseRegistryUnitOfWork};
use sqlx::PgPool;
use uuid::Uuid;

mod support;

use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn artifact_insert_failure_rolls_back_asset_version_and_artifact() -> TestResult {
    let suffix = Uuid::now_v7().simple().to_string();
    let label = format!("lakehouse_registry_atomicity_{}", &suffix[..12]);
    run_in_disposable_database(&label, move |pool| async move {
        MIGRATOR.run(&pool).await?;
        let qualified_name = format!("gongzzang.gold.rollback_{}", &suffix[..12]);
        let bucket_name = format!("gongzzang-lakehouse-rollback-{}", &suffix[..12]);
        let object_key = format!("gold/rollback-{}/manifest.json", &suffix[..12]);
        let repository = PgLakehouseRegistryRepository::new(pool.clone());
        let unit_of_work = PgLakehouseRegistryUnitOfWork::new(pool.clone());
        let namespace = LakehouseStorageNamespace::new(
            LakehouseStorageNamespaceId::new(Uuid::now_v7()),
            LakehouseStorageProvider::R2,
            LakehouseEnvironment::Staging,
            LakehouseOwnerService::Gongzzang,
            bucket_name,
            None,
            LakehouseCatalogProvider::None,
            LakehouseNamespaceStatus::Active,
        )?;
        repository.upsert_storage_namespace(&namespace).await?;
        let trigger = ArtifactFailureTrigger::install(&pool, &suffix, &object_key).await?;

        let result = unit_of_work
            .register_object_artifact(RegisterLakehouseObjectArtifactCommand {
                qualified_name: qualified_name.clone(),
                owner_service: LakehouseOwnerService::Gongzzang,
                environment: LakehouseEnvironment::Staging,
                layer: LakehouseRegistryLayer::Gold,
                asset_kind: LakehouseAssetKind::Manifest,
                schema_contract_ref: "test.registry.rollback.v1".to_owned(),
                dataset_version: "rollback-v1".to_owned(),
                schema_version: "test.registry.rollback.v1".to_owned(),
                artifact_format: LakehouseArtifactFormat::Json,
                created_by_ingestion_run_id: None,
                object_key: object_key.clone(),
                content_type: "application/json".to_owned(),
                checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                size_bytes: 128,
                logical_record_count: Some(1),
            })
            .await;
        trigger.remove(&pool).await?;

        let asset_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM catalog.lakehouse_data_asset WHERE qualified_name = $1",
        )
        .bind(&qualified_name)
        .fetch_one(&pool)
        .await?;
        let version_count: i64 = sqlx::query_scalar(
            "SELECT count(*)
             FROM catalog.lakehouse_dataset_version version
             JOIN catalog.lakehouse_data_asset asset ON asset.id = version.data_asset_id
             WHERE asset.qualified_name = $1",
        )
        .bind(&qualified_name)
        .fetch_one(&pool)
        .await?;
        let artifact_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM catalog.lakehouse_object_artifact WHERE object_key = $1",
        )
        .bind(&object_key)
        .fetch_one(&pool)
        .await?;

        assert!(result.is_err(), "injected artifact failure must surface");
        assert_eq!(asset_count, 0, "asset upsert leaked from failed commit");
        assert_eq!(
            version_count, 0,
            "dataset version leaked from failed commit"
        );
        assert_eq!(artifact_count, 0, "artifact leaked from failed commit");
        Ok(())
    })
    .await
}

struct ArtifactFailureTrigger {
    trigger_name: String,
    function_name: String,
}

impl ArtifactFailureTrigger {
    async fn install(pool: &PgPool, suffix: &str, object_key: &str) -> TestResult<Self> {
        let trigger_name = format!("fail_lakehouse_artifact_{}", &suffix[..12]);
        let function_name = format!("fail_lakehouse_artifact_fn_{}", &suffix[..12]);
        let sql = format!(
            "CREATE FUNCTION catalog.{function_name}() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.object_key = '{object_key}' THEN
                     RAISE EXCEPTION 'forced lakehouse artifact failure' USING ERRCODE = 'P0001';
                 END IF;
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER {trigger_name}
             BEFORE INSERT ON catalog.lakehouse_object_artifact
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
            "DROP TRIGGER IF EXISTS {} ON catalog.lakehouse_object_artifact;
             DROP FUNCTION IF EXISTS catalog.{}();",
            self.trigger_name, self.function_name
        );
        sqlx::raw_sql(&sql).execute(pool).await?;
        Ok(())
    }
}
