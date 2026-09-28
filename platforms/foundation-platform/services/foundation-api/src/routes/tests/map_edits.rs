//! The admin map-edit route (root ADR-0112): who may call it, and what each outcome answers.

use super::*;
use catalog_application::ports::{MapEditAppended, MapEditRecord, MapEditStore, MapEditStoreError};

/// Grants exactly the staff map-edit capability and records what it was asked.
#[derive(Default)]
struct StaffMapEditAuthorization {
    asked: Mutex<Vec<(String, String, Option<String>)>>,
}

#[async_trait]
impl IdentityAuthorization for StaffMapEditAuthorization {
    async fn authorize(
        &self,
        _bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        self.asked.lock().await.push((
            resource.to_owned(),
            action.to_owned(),
            resource_id.map(ToOwned::to_owned),
        ));
        if required_principal_kind == RequiredPrincipalKind::Staff
            && (resource, action) == ("foundation.spatial", "map_edit")
        {
            return Ok(AuthorizedPrincipal {
                principal_id: uuid::Uuid::nil(),
                trace_id: trace_id.to_owned(),
            });
        }
        Err(IdentityAuthorizationError::Forbidden)
    }
}

#[derive(Default)]
struct RecordingMapEditStore {
    records: Mutex<Vec<MapEditRecord>>,
}

#[async_trait]
impl MapEditStore for RecordingMapEditStore {
    async fn append(&self, record: &MapEditRecord) -> Result<MapEditAppended, MapEditStoreError> {
        let mut records = self.records.lock().await;
        records.push(record.clone());
        Ok(MapEditAppended {
            change_seq: u64::try_from(records.len()).unwrap_or(u64::MAX),
            replayed: false,
        })
    }
}

// The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py; the range
// is half-open, so every corner stays strictly inside it.
const SQUARE: &str = r#"{"type":"Polygon","coordinates":[[[127.1231,36.1231],[127.1234,36.1231],[127.1234,36.1234],[127.1231,36.1234],[127.1231,36.1231]]]}"#;
const BOW_TIE: &str = r#"{"type":"Polygon","coordinates":[[[127.1231,36.1231],[127.1234,36.1234],[127.1234,36.1231],[127.1231,36.1234],[127.1231,36.1231]]]}"#;

fn edit(geometry: &str) -> String {
    format!(
        r#"{{"feature_id":"00000000-0000-5000-8000-000000000001","op":"upsert","geometry":{geometry},"properties":{{"official_complex_code":"SYN"}},"idempotency_key":"synthetic-key"}}"#
    )
}

fn post(body: String, bearer: bool) -> Result<Request<Body>, axum::http::Error> {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/catalog/v1/map-edits/complex")
        .header(header::CONTENT_TYPE, "application/json");
    if bearer {
        request = request.header(header::AUTHORIZATION, "Bearer synthetic-staff-token");
    }
    request.body(Body::from(body))
}

#[tokio::test]
async fn a_map_edit_without_a_token_is_refused_before_anything_runs() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(RecordingMapEditStore::default());
    let state = AppState::bootstrap_for_test_with_identity_authorization(Arc::new(
        StaffMapEditAuthorization::default(),
    ))?
    .with_map_edit_store(store.clone());
    let response = router(Arc::new(state))
        .oneshot(post(edit(SQUARE), false)?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(store.records.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn an_authorized_edit_is_saved_under_the_staff_spatial_map_edit_capability(
) -> Result<(), Box<dyn Error>> {
    let authorization = Arc::new(StaffMapEditAuthorization::default());
    let store = Arc::new(RecordingMapEditStore::default());
    let state = AppState::bootstrap_for_test_with_identity_authorization(authorization.clone())?
        .with_map_edit_store(store.clone());
    let response = router(Arc::new(state))
        .oneshot(post(edit(SQUARE), true)?)
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        body,
        serde_json::json!({"unit": "complex", "change_seq": 1, "replayed": false})
    );
    assert_eq!(
        authorization.asked.lock().await.as_slice(),
        [(
            "foundation.spatial".to_owned(),
            "map_edit".to_owned(),
            Some("complex".to_owned())
        )]
    );
    let editors: Vec<String> = store
        .records
        .lock()
        .await
        .iter()
        .map(|record| record.editor.to_string())
        .collect();
    assert_eq!(editors, [uuid::Uuid::nil().to_string()]);
    Ok(())
}

#[tokio::test]
async fn an_invalid_polygon_answers_422_and_never_reaches_the_store() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(RecordingMapEditStore::default());
    let state = AppState::bootstrap_for_test_with_identity_authorization(Arc::new(
        StaffMapEditAuthorization::default(),
    ))?
    .with_map_edit_store(store.clone());
    let response = router(Arc::new(state))
        .oneshot(post(edit(BOW_TIE), true)?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(store.records.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn without_a_configured_store_an_edit_answers_503_not_success() -> Result<(), Box<dyn Error>>
{
    let state = AppState::bootstrap_for_test_with_identity_authorization(Arc::new(
        StaffMapEditAuthorization::default(),
    ))?;
    let response = router(Arc::new(state))
        .oneshot(post(edit(SQUARE), true)?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}
