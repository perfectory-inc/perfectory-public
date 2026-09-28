//! Saving a polygon edit (ADR-0112): what this layer owns is that nothing invalid reaches the store.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catalog_application::ports::{MapEditAppended, MapEditRecord, MapEditStore, MapEditStoreError};
use catalog_application::{AppendMapEdit, AppendMapEditError, AppendMapEditInput};
use catalog_domain::{MapEditError, MapEditOperation};
use foundation_shared_kernel::ids::StaffId;
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Default)]
struct RecordingStore {
    records: Mutex<Vec<MapEditRecord>>,
}

#[async_trait]
impl MapEditStore for RecordingStore {
    async fn append(&self, record: &MapEditRecord) -> Result<MapEditAppended, MapEditStoreError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MapEditStoreError::Unavailable("poisoned".to_owned()))?;
        records.push(record.clone());
        Ok(MapEditAppended {
            change_seq: u64::try_from(records.len()).unwrap_or(u64::MAX),
            replayed: false,
        })
    }
}

// The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py.
fn square() -> Value {
    json!({"type": "Polygon", "coordinates": [[
        [127.123, 36.123], [127.1239, 36.123], [127.1239, 36.1239], [127.123, 36.1239], [127.123, 36.123]
    ]]})
}

fn input(
    operation: &str,
    geometry: Option<Value>,
    properties: Option<Value>,
) -> AppendMapEditInput {
    AppendMapEditInput {
        unit: "complex".to_owned(),
        feature_id: "00000000-0000-5000-8000-000000000001".to_owned(),
        operation: operation.to_owned(),
        geometry,
        properties,
        editor: StaffId::new(Uuid::nil()),
        idempotency_key: "synthetic-key".to_owned(),
    }
}

#[tokio::test]
async fn a_valid_upsert_and_delete_reach_the_store_as_checked() -> Result<(), AppendMapEditError> {
    let store = Arc::new(RecordingStore::default());
    let use_case = AppendMapEdit::new(store.clone());
    let upsert = use_case
        .execute(input(
            "upsert",
            Some(square()),
            Some(json!({"official_complex_code": "SYN"})),
        ))
        .await?;
    assert_eq!(upsert.change_seq, 1);
    use_case.execute(input("delete", None, None)).await?;
    let records = store
        .records
        .lock()
        .map_err(|_| MapEditStoreError::Unavailable("poisoned".to_owned()))?
        .clone();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].operation, MapEditOperation::Upsert);
    assert_eq!(
        records[0]
            .geometry
            .as_ref()
            .map(|geometry| geometry.as_json().clone()),
        Some(square())
    );
    assert_eq!(records[1].operation, MapEditOperation::Delete);
    assert!(records[1].geometry.is_none());
    Ok(())
}

#[tokio::test]
async fn refused_edits_never_reach_the_store() {
    let store = Arc::new(RecordingStore::default());
    let use_case = AppendMapEdit::new(store.clone());
    let bow_tie = json!({"type": "Polygon", "coordinates": [[
        [127.123, 36.123], [127.1239, 36.1239], [127.1239, 36.123], [127.123, 36.1239], [127.123, 36.123]
    ]]});
    for (edit, expected) in [
        (input("update", Some(square()), None), "UnknownOperation"),
        (input("upsert", None, None), "GeometryDoesNotMatchOperation"),
        (
            input("delete", Some(square()), None),
            "GeometryDoesNotMatchOperation",
        ),
        (
            input("delete", None, Some(json!({}))),
            "GeometryDoesNotMatchOperation",
        ),
        (input("upsert", Some(bow_tie), None), "Invalid"),
    ] {
        let result = use_case.execute(edit).await;
        let name = match &result {
            Err(AppendMapEditError::Invalid(MapEditError::UnknownOperation)) => "UnknownOperation",
            Err(AppendMapEditError::Invalid(MapEditError::GeometryDoesNotMatchOperation)) => {
                "GeometryDoesNotMatchOperation"
            }
            Err(AppendMapEditError::Invalid(MapEditError::Invalid(_))) => "Invalid",
            _ => "unexpected",
        };
        assert_eq!(name, expected, "{result:?}");
    }
    assert!(store.records.lock().is_ok_and(|records| records.is_empty()));
}
