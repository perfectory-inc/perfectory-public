use std::io::{Read as _, Write as _};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_domain::{
    source_slug, BronzeObject, CollectionError, IngestionRun, SchemaProfile, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_outbox::FileObjectStorage;
use foundation_shared_kernel::ids::SourceCatalogId;
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

use super::{close_after_seconds, JsonLinesWriter, LineObject, LineObjects, Target, CONTRACT_JSON};

// Synthetic only: no real case numbers, PNUs or addresses (public repository).
fn at() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2099-01-02T16:05:04Z")
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_default()
}

fn source() -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug("courtauction.go.kr", "synthetic_lines").unwrap(),
        name: "synthetic_lines".to_owned(),
        provider: "courtauction.go.kr".to_owned(),
        dataset_name: "synthetic_lines".to_owned(),
        base_url: None,
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: None,
        is_active: true,
        created_at: at(),
        updated_at: at(),
        version: 1,
    }
}

/// The court lane's key form: `<dataset>-partNNNN` (no run stamp, root ADR-0626 G5).
fn part_id(n: u32) -> String {
    format!("synthetic-lines-part{:04}", n + 1)
}

fn no_params() -> JsonValue {
    JsonValue::Null
}

fn lane(
    file_id: impl Fn(u32) -> String + Send + Sync + 'static,
    params: impl Fn() -> JsonValue + Send + Sync + 'static,
    content_qualified: bool,
) -> LineObjects<'static> {
    LineObjects {
        source: source(),
        operation: "syntheticOperation",
        collected_at: at(),
        file_id: Box::new(file_id),
        content_qualified,
        params: Box::new(params),
    }
}

type DryRun<'a> = Target<'a, RecordingUow, FileObjectStorage>;

/// The court and onbid helpers this writer replaces: each line, then a newline, into one gzip
/// stream at the default level.
fn old_text_helper(lines: &[String]) -> Vec<u8> {
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for line in lines {
        gzip.write_all(line.as_bytes()).unwrap();
        gzip.write_all(b"\n").unwrap();
    }
    gzip.finish().unwrap()
}

/// The industrial notice helper this writer replaces: each value serialized into the stream.
fn old_value_helper(lines: &[JsonValue]) -> Vec<u8> {
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for line in lines {
        serde_json::to_writer(&mut gzip, line).unwrap();
        gzip.write_all(b"\n").unwrap();
    }
    gzip.finish().unwrap()
}

fn read(dir: &Path, object: &LineObject) -> Vec<u8> {
    std::fs::read(dir.join(object.local_path.as_deref().unwrap_or_default())).unwrap()
}

fn text_of(bytes: &[u8]) -> String {
    let mut text = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut text)
        .unwrap();
    text
}

/// Lines shaped like the collectors' envelopes, long enough that the gzip stream spans many
/// deflate blocks.
fn fixture_values() -> Vec<JsonValue> {
    (0..2_000)
        .map(|n| {
            json!({
                "operation": "syntheticOperation",
                "request": {"page": n, "key": format!("SYN-{n:05}")},
                "requested_at_utc": "2099-01-02T16:05:04+00:00",
                "http_status": 200,
                "body": json!({"data": {"rows": [{"id": n, "name": format!("합성 {n}"), "note": "x".repeat(n % 97)}]}}).to_string(),
            })
        })
        .collect()
}

#[test]
fn the_contract_closes_an_object_after_a_positive_interval() {
    let seconds = close_after_seconds().unwrap();
    assert!(seconds > 0);
    let contract: JsonValue = serde_json::from_str(CONTRACT_JSON).unwrap();
    assert_eq!(contract["close_after_seconds"].as_u64(), Some(seconds));
}

#[tokio::test]
async fn lines_land_as_the_text_helpers_bytes_and_as_the_value_helpers_lines() {
    let values = fixture_values();
    let lines: Vec<String> = values
        .iter()
        .map(|value| serde_json::to_string(value).unwrap())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = JsonLinesWriter::new(
        lane(part_id, no_params, false),
        DryRun::of(None, dir.path()),
    )
    .unwrap();
    writer.push_group(&lines).await.unwrap();
    let objects = writer.finish().await.unwrap();
    assert_eq!(objects.len(), 1);
    let bytes = read(dir.path(), &objects[0]);
    assert_eq!(
        bytes,
        old_text_helper(&lines),
        "the court and onbid helpers"
    );
    // The industrial notice helper serialized each value into the gzip stream token by token, and
    // the deflate stream flate2 writes depends on how its input arrives in writes: the same lines,
    // other bytes. The text is what Bronze holds for its readers, and it is the same.
    assert_eq!(
        text_of(&bytes),
        text_of(&old_value_helper(&values)),
        "the industrial notice helper's lines"
    );
    assert_eq!(objects[0].lines, lines.len());
    assert_eq!(objects[0].size_bytes, bytes.len() as u64);
    assert!(!objects[0].written);
}

#[tokio::test]
async fn groups_pushed_one_at_a_time_land_as_the_same_bytes_as_one_group() {
    let lines: Vec<String> = fixture_values()
        .iter()
        .map(|value| serde_json::to_string(value).unwrap())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = JsonLinesWriter::new(
        lane(part_id, no_params, false),
        DryRun::of(None, dir.path()),
    )
    .unwrap();
    for line in &lines {
        writer.push_group(std::slice::from_ref(line)).await.unwrap();
    }
    let objects = writer.finish().await.unwrap();
    assert_eq!(objects.len(), 1, "an object stays open inside its interval");
    assert_eq!(read(dir.path(), &objects[0]), old_text_helper(&lines));
}

#[tokio::test]
async fn an_object_closes_at_the_first_group_boundary_after_its_interval_and_never_inside_a_group()
{
    let dir = tempfile::tempdir().unwrap();
    // An interval already passed: every group closes the object it went into.
    let mut writer = JsonLinesWriter::closing_after(
        lane(part_id, no_params, false),
        DryRun::of(None, dir.path()),
        Duration::ZERO,
    );
    let search: Vec<String> = (0..500).map(|n| json!({"page": n}).to_string()).collect();
    writer.push_group(&search).await.unwrap();
    writer
        .push_group(&[json!({"page": "next"}).to_string()])
        .await
        .unwrap();
    let objects = writer.finish().await.unwrap();
    // A whole search is one group, so one object, however long it took.
    assert_eq!(
        objects
            .iter()
            .map(|object| object.lines)
            .collect::<Vec<_>>(),
        vec![500, 1]
    );
    assert!(objects[0]
        .object_key
        .ends_with("/synthetic-lines-part0001.gz"));
    assert!(objects[1]
        .object_key
        .ends_with("/synthetic-lines-part0002.gz"));
    assert_eq!(read(dir.path(), &objects[0]), old_text_helper(&search));
}

#[tokio::test]
async fn a_line_that_holds_a_newline_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = JsonLinesWriter::new(
        lane(part_id, no_params, false),
        DryRun::of(None, dir.path()),
    )
    .unwrap();
    let error = writer
        .push_group(&["{\"a\":1}\n{\"b\":2}"])
        .await
        .err()
        .map(|error| format!("{error:#}"))
        .unwrap_or_default();
    assert!(error.contains("holds a newline"), "{error}");
}

#[tokio::test]
async fn nothing_pushed_is_no_object() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = JsonLinesWriter::new(
        lane(part_id, no_params, false),
        DryRun::of(None, dir.path()),
    )
    .unwrap();
    writer.push_group::<String>(&[]).await.unwrap();
    assert!(writer.finish().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_content_qualified_key_carries_the_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let id = |n: u32| format!("synthetic-lines-{n:03}");
    let mut writer =
        JsonLinesWriter::new(lane(id, no_params, true), DryRun::of(None, dir.path())).unwrap();
    writer.push_group(&["{}"]).await.unwrap();
    let objects = writer.finish().await.unwrap();
    assert!(
        objects[0].object_key.contains(&objects[0].checksum_sha256),
        "{}",
        objects[0].object_key
    );
}

#[tokio::test]
async fn a_live_object_is_committed_in_a_run_of_its_own_with_the_lanes_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileObjectStorage::new(dir.path().join("bronze")).unwrap();
    let uow = RecordingUow::default();
    let params = || json!({"night": {"requests_sent": 3}});
    let lines = vec![
        r#"{"operation":"syntheticOperation","body":"{}"}"#.to_owned(),
        "{}".to_owned(),
    ];
    let mut writer = JsonLinesWriter::new(
        lane(part_id, params, false),
        Target::of(Some((&uow, &storage)), dir.path()),
    )
    .unwrap();
    writer.push_group(&lines).await.unwrap();
    let objects = writer.finish().await.unwrap();
    assert_eq!(objects.len(), 1);
    let object = &objects[0];
    assert!(object.written && object.new);
    assert_eq!(object.local_path, None, "a live run keeps no local copy");
    let stored = std::fs::read(dir.path().join("bronze").join(&object.object_key)).unwrap();
    assert_eq!(stored, old_text_helper(&lines));
    assert_eq!(text_of(&stored), format!("{}\n{}\n", lines[0], lines[1]));

    let recorded = uow.objects.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].logical_record_count, Some(2));
    assert_eq!(recorded[0].request_params["night"]["requests_sent"], 3);
    assert_eq!(
        recorded[0].request_params["provider_file_id"],
        "synthetic-lines-part0001"
    );
    let runs = uow.runs.lock().unwrap().clone();
    assert_eq!(
        runs[0].request_params,
        json!({"operation": "syntheticOperation", "object": 0, "lines": 2, "night": {"requests_sent": 3}})
    );

    // A retry of the same run is the committer's idempotent success, not a second object.
    let mut again = JsonLinesWriter::new(
        lane(part_id, params, false),
        Target::of(Some((&uow, &storage)), dir.path()),
    )
    .unwrap();
    again.push_group(&lines).await.unwrap();
    let retried = again.finish().await.unwrap();
    assert_eq!(retried[0].object_key, object.object_key);
    assert!(!retried[0].new, "Bronze held it before this run");
    assert_eq!(uow.objects.lock().unwrap().len(), 1);
}

#[derive(Default)]
struct RecordingUow {
    runs: Mutex<Vec<IngestionRun>>,
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
        // An upsert returns the source's one id, as the database does.
        let mut source = entry.clone();
        source.id = SourceCatalogId::new(Uuid::new_v5(&Uuid::NAMESPACE_OID, entry.slug.as_bytes()));
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
