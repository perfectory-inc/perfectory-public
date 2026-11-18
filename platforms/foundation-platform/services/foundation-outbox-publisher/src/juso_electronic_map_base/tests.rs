use std::{path::Path, sync::Mutex};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, IngestionRunStatus, SchemaProfile,
    SourceCatalogEntry,
};
use foundation_outbox::FileObjectStorage;
use foundation_shared_kernel::ids::SourceCatalogId;
use uuid::Uuid;

use super::{all_kinds, land, pending, read_layout, scan, BaseContract, Ports};
use crate::juso_electronic_map_collect::source_contract;

// Synthetic only: made-up file names, the synthetic 시도 folders 99000 and 98000, and layer parts
// holding no real rows (public repository).

fn at() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2099-01-02T03:04:05Z")
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_default()
}

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("juso-base-{}", Uuid::new_v4()))
}

fn place(inbox: &Path, relative: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let path = inbox.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

fn zip(tag: &str) -> Vec<u8> {
    let mut bytes = b"PK\x03\x04".to_vec();
    bytes.extend_from_slice(tag.as_bytes());
    bytes
}

/// A zip laid out as the provider's 시도 zip: one `<시도코드>/` folder of every layer's parts, the
/// projection and a document beside it. `skip` names one member to leave out.
fn region_zip(base: &BaseContract, folder: &str, skip: Option<&str>) -> anyhow::Result<Vec<u8>> {
    use std::io::Write as _;
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default();
    let mut names = vec![
        "GRS80_UTMK.prj".to_owned(),
        "synthetic-layout.pdf".to_owned(),
    ];
    for layer in base.layers.values().chain(&base.unmatched_layers) {
        names.extend(
            base.layer_parts
                .iter()
                .map(|part| format!("{folder}/{layer}{part}")),
        );
    }
    for name in names.iter().filter(|name| Some(name.as_str()) != skip) {
        writer.start_file(name.as_str(), options)?;
        writer.write_all(name.as_bytes())?;
    }
    Ok(writer.finish()?.into_inner())
}

#[test]
fn the_base_is_the_national_set_of_all_kinds_and_the_replay_starts_a_month_early(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let base = BaseContract::of(&contract)?;
    assert_eq!(base.item.rtl_dta_dtl_sn, 26);
    assert_eq!(all_kinds(&contract)?.len(), 11);
    // The provider's base has no layer for 상세주소 동 (building_group): ten kinds start from it.
    assert_eq!(base.layers.len(), 10);
    assert!(!base.layers.contains_key("building_group"));
    assert_eq!(
        base.replay_from("209910")?,
        NaiveDate::from_ymd_opt(2099, 9, 1).unwrap_or_default()
    );
    assert_eq!(
        base.replay_from("209901")?,
        NaiveDate::from_ymd_opt(2098, 12, 1).unwrap_or_default()
    );
    assert!(base.month_start("2099-10").is_none());
    assert!(base.month_start("209913").is_none());
    Ok(())
}

#[test]
fn an_empty_or_missing_inbox_is_inert() -> anyhow::Result<()> {
    let base = BaseContract::of(&source_contract()?)?;
    let root = temp();
    let found = scan(&base, &root.join("missing"))?;
    assert!(found.files.is_empty() && found.refused.is_empty());
    std::fs::create_dir_all(root.join("inbox"))?;
    let found = scan(&base, &root.join("inbox"))?;
    assert!(found.files.is_empty() && found.refused.is_empty());
    Ok(())
}

#[test]
fn the_inbox_is_a_month_folder_of_files_and_partial_transfers_wait() -> anyhow::Result<()> {
    let base = BaseContract::of(&source_contract()?)?;
    let inbox = temp();
    place(&inbox, "209910/synthetic-a.zip", &zip("a"))?;
    place(&inbox, "209910/synthetic-b.zip.crdownload", &zip("b"))?;
    place(&inbox, "209910/nested/synthetic-c.zip", &zip("c"))?;
    place(&inbox, "2099-10/synthetic-d.zip", &zip("d"))?;
    let found = scan(&base, &inbox)?;
    assert_eq!(found.files.len(), 1);
    assert_eq!(found.files[0].month, "209910");
    assert_eq!(found.files[0].name, "synthetic-a.zip");
    assert_eq!(found.refused.len(), 2, "{:?}", found.refused);
    Ok(())
}

#[test]
fn a_zip_is_read_by_its_folder_and_must_hold_every_layer() -> anyhow::Result<()> {
    let base = BaseContract::of(&source_contract()?)?;
    let root = temp();
    place(&root, "whole.zip", &region_zip(&base, "99000", None)?)?;
    let layout =
        read_layout(&root.join("whole.zip"), &base)?.map_err(|reason| anyhow::anyhow!(reason))?;
    assert_eq!(layout.folder, "99000");
    assert_eq!(layout.member_count, 35);
    assert_eq!(
        layout.members["building"],
        [
            "99000/TL_SPBD_BULD.shp",
            "99000/TL_SPBD_BULD.shx",
            "99000/TL_SPBD_BULD.dbf"
        ]
    );

    place(
        &root,
        "short.zip",
        &region_zip(&base, "99000", Some("99000/TL_SPRD_RW.dbf"))?,
    )?;
    let refused = read_layout(&root.join("short.zip"), &base)?;
    assert_eq!(refused, Err("lacks 99000/TL_SPRD_RW.dbf".to_owned()));
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
async fn each_placed_zip_lands_byte_for_byte_once_not_once_per_kind() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let base = BaseContract::of(&contract)?;
    let root = temp();
    let (inbox, state) = (root.join("inbox"), root.join("state"));
    let bytes = region_zip(&base, "99000", None)?;
    place(&inbox, "209910/합성.zip", &bytes)?;
    place(
        &inbox,
        "209910/synthetic-map.zip",
        &region_zip(&base, "98000", None)?,
    )?;
    let storage = FileObjectStorage::new(root.join("bronze"))?;
    let uow = RecordingUow::default();
    let ports = || {
        Some(Ports {
            uow: &uow,
            storage: &storage,
        })
    };

    let (manifest, refused) = land(&contract, &base, &inbox, &state, ports(), at()).await?;
    assert!(refused.is_empty());
    let landed = manifest["landed"].as_array().cloned().unwrap_or_default();
    // One object per zip, in the base's one source, the provider's bytes as they are.
    assert_eq!(landed.len(), 2);
    let first = landed
        .iter()
        .find(|entry| entry["folder"] == "99000")
        .cloned()
        .unwrap_or_default();
    let key = first["object_key"].as_str().unwrap_or_default();
    assert!(
        key.starts_with("bronze/source=jusogokr__electronic_map_base/"),
        "{key}"
    );
    assert!(key.contains("base-209910-99000"), "{key}");
    assert_eq!(std::fs::read(root.join("bronze").join(key))?, bytes);
    assert_eq!(first["replay_from"], "2099-09-01");
    // Each kind is addressed inside the zip by its members.
    assert_eq!(first["members"]["legal_ri"][2], "99000/TL_SCCO_LI.dbf");
    let completions = uow.completions.lock().map_err(poisoned)?.clone();
    assert!(completions
        .iter()
        .all(|done| done.status == IngestionRunStatus::Succeeded));
    assert_eq!(uow.objects.lock().map_err(poisoned)?.len(), 2);
    let runs = uow.runs.lock().map_err(poisoned)?.clone();
    assert_eq!(runs.len(), 2);
    assert!(runs.iter().all(
        |run| run.request_params["juso_base"]["members"]["road_width"][0]
            .as_str()
            .is_some_and(|member| member.ends_with("/TL_SPRD_RW.shp"))
    ));

    // The next run opens nothing: the marker holds the file's size and modification time.
    assert!(pending(&base, &inbox, &state)?.files.is_empty());
    let (again, _) = land(&contract, &base, &inbox, &state, ports(), at()).await?;
    assert_eq!(again["landed"].as_array().map(Vec::len), Some(0));
    assert_eq!(uow.objects.lock().map_err(poisoned)?.len(), 2);
    // The inbox is never emptied by the collector.
    assert!(inbox.join("209910/합성.zip").exists());
    Ok(())
}

#[tokio::test]
async fn a_file_that_is_not_a_zip_of_the_layers_is_refused_and_lands_nothing() -> anyhow::Result<()>
{
    let contract = source_contract()?;
    let base = BaseContract::of(&contract)?;
    let root = temp();
    let (inbox, state) = (root.join("inbox"), root.join("state"));
    place(&inbox, "209910/synthetic.html", b"<html>login</html>")?;
    place(&inbox, "209910/synthetic-other.zip", &zip("no directory"))?;
    let storage = FileObjectStorage::new(root.join("bronze"))?;
    let uow = RecordingUow::default();
    let ports = Some(Ports {
        uow: &uow,
        storage: &storage,
    });
    let (_, refused) = land(&contract, &base, &inbox, &state, ports, at()).await?;
    assert_eq!(refused.len(), 2, "{refused:?}");
    assert!(uow.objects.lock().map_err(poisoned)?.is_empty());
    assert_eq!(
        pending(&base, &inbox, &state)?.files.len(),
        2,
        "it is asked again next run"
    );
    Ok(())
}
