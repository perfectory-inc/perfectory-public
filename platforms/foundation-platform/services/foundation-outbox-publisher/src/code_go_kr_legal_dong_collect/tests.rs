use std::sync::Mutex;

use async_trait::async_trait;
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, SchemaProfile, SourceCatalogEntry,
};
use foundation_outbox::FileObjectStorage;
use foundation_shared_kernel::ids::SourceCatalogId;
use uuid::Uuid;

use super::{
    dated_file_id, persist, plan, request_spacing, source_contract, CollectConfig, Fetched,
    DATASET_SLUG, OPERATION, PREFIX,
};

fn at() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2099-01-02T03:04:05Z")
        .map(|value| value.with_timezone(&chrono::Utc))
        .unwrap_or_default()
}

fn table(at: chrono::DateTime<chrono::Utc>, bytes: &[u8]) -> Fetched {
    Fetched {
        provider_file_id: dated_file_id(at),
        provider_file_name: format!("{}.html", dated_file_id(at)),
        content_type: "text/html;charset=UTF-8".to_owned(),
        bytes: bytes.to_vec(),
    }
}

#[test]
fn the_contract_spacing_is_a_floor_a_setting_cannot_lower() -> anyhow::Result<()> {
    let contract = source_contract()?;
    assert!(contract.request_spacing_seconds >= 15, "root ADR-0143 §6");
    let floor = u128::from(contract.request_spacing_seconds) * 1000;
    assert_eq!(request_spacing(&contract, None)?.interval_millis(), floor);
    assert_eq!(
        request_spacing(&contract, Some(1))?.interval_millis(),
        floor
    );
    assert_eq!(
        request_spacing(&contract, Some(60_000))?.interval_millis(),
        60_000
    );
    Ok(())
}

#[test]
fn the_notice_board_is_not_a_source() -> anyhow::Result<()> {
    // ADR-0144: only downloaded data is a source. The contract names the full table and
    // nothing of the 코드변경안내 board, so no code path can be pointed at it again by configuration.
    let contract: serde_json::Value = serde_json::from_str(
        foundation_outbox_publisher::code_go_kr_legal_dong_contract::CONTRACT_JSON,
    )?;
    let endpoints = contract
        .as_object()
        .map(|object| object.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    for board in ["notice_list", "notice_detail", "attachment"] {
        assert!(
            !endpoints.iter().any(|key| key == board),
            "{board} is back in the contract"
        );
    }
    assert!(
        !foundation_outbox_publisher::code_go_kr_legal_dong_contract::CONTRACT_JSON
            .contains("bbsmng")
    );
    Ok(())
}

#[test]
fn the_object_key_follows_the_bulk_layout_and_names_the_second_it_was_taken() -> anyhow::Result<()>
{
    let run = foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4());
    // The snapshot loader recognises a code.go.kr table by this prefix
    // (legal_dong_code_snapshot_to_reference.py, CODE_GO_KR_TABLE_PREFIX).
    assert_eq!(
        plan(&table(at(), b"<table></table>"), run, at())?.object_key.as_str(),
        "bronze/source=codegokr__legal_dong_code_table/operation=regCodeL/regcode-20990102T030405Z.html"
    );
    Ok(())
}

#[test]
fn the_dataset_is_registered_in_the_endpoint_catalog() -> anyhow::Result<()> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/catalog/public-source-endpoint-catalog.v1.json");
    let catalog: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let endpoints = catalog["endpoints"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let slug = collection_domain::source_slug("code.go.kr", DATASET_SLUG)?;
    let entry = endpoints
        .iter()
        .find(|entry| entry["bronze"]["source_slug"] == slug.as_str());
    assert_eq!(
        entry.map(|entry| entry["operation"].clone()),
        Some(serde_json::Value::from(OPERATION)),
        "{slug}"
    );
    // The catalog lists this one code.go.kr dataset and no other.
    let code_go_kr = endpoints
        .iter()
        .filter(|entry| {
            entry["bronze"]["source_slug"]
                .as_str()
                .is_some_and(|value| value.starts_with("codegokr__"))
        })
        .count();
    assert_eq!(code_go_kr, 1);
    Ok(())
}

#[derive(Default)]
struct RecordingUow {
    runs: Mutex<Vec<IngestionRun>>,
    completions: Mutex<Vec<CompleteIngestionRunCommand>>,
    objects: Mutex<Vec<BronzeObject>>,
}

fn poisoned(_: impl std::fmt::Debug) -> CollectionError {
    CollectionError::Infrastructure("lock poisoned".to_owned())
}

#[async_trait]
impl BronzeIngestUnitOfWork for RecordingUow {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        let mut source = entry.clone();
        source.id = SourceCatalogId::new(Uuid::from_u128(u128::from(
            entry.slug.bytes().map(u32::from).sum::<u32>(),
        )));
        Ok(source)
    }

    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError> {
        self.runs.lock().map_err(poisoned)?.push(run.clone());
        Ok(run.clone())
    }

    async fn complete_ingestion_run(
        &self,
        command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        self.completions
            .lock()
            .map_err(poisoned)?
            .push(command.clone());
        self.runs
            .lock()
            .map_err(poisoned)?
            .iter()
            .find(|run| run.id == command.id)
            .cloned()
            .ok_or_else(|| CollectionError::IngestionRunNotFound(command.id.to_string()))
    }

    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(self
            .objects
            .lock()
            .map_err(poisoned)?
            .iter()
            .find(|o| {
                o.source_catalog_id == source_catalog_id && o.object_key.as_str() == object_key
            })
            .cloned())
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        self.objects.lock().map_err(poisoned)?.push(object.clone());
        Ok(object.clone())
    }

    async fn upsert_schema_profile(
        &self,
        profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError> {
        Ok(profile.clone())
    }
}

#[tokio::test]
async fn a_table_is_committed_once_and_a_local_copy_kept() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("code-go-kr-{}", Uuid::new_v4()));
    let config = CollectConfig {
        base_uri: "http://127.0.0.1:9".to_owned(),
        request_spacing: request_spacing(&source_contract()?, None)?,
        live_write: true,
        output_dir: root.join("out"),
    };
    let fetched = table(at(), b"<table><tr><td>synthetic</td></tr></table>");
    let storage = FileObjectStorage::new(root.join("bronze"))?;
    let uow = RecordingUow::default();
    let object = persist(&config, &fetched, at(), &uow, &storage).await?;
    assert!(object.written);
    assert_eq!(std::fs::read(&object.local_path)?, fetched.bytes);
    assert!(root.join("bronze").join(&object.object_key).exists());
    assert!(uow
        .completions
        .lock()
        .map_err(|_| anyhow::anyhow!("lock"))?
        .iter()
        .all(|done| done.objects_written == 1));
    // A retry of the same bytes is the committer's idempotent success, not a second object.
    persist(&config, &fetched, at(), &uow, &storage).await?;
    assert_eq!(
        uow.objects
            .lock()
            .map_err(|_| anyhow::anyhow!("lock"))?
            .len(),
        1
    );
    // Different bytes under one run's key are refused, never overwritten.
    let changed = table(at(), b"<table>changed</table>");
    assert!(persist(&config, &changed, at(), &uow, &storage)
        .await
        .is_err());
    // The next day's table is its own object beside the first.
    let later = at() + chrono::Duration::days(1);
    let next = persist(
        &config,
        &table(later, b"<table>next</table>"),
        later,
        &uow,
        &storage,
    )
    .await?;
    assert_ne!(next.object_key, object.object_key);
    std::fs::remove_dir_all(&root)?;
    assert!(PREFIX.starts_with("FOUNDATION_PLATFORM_"));
    Ok(())
}
