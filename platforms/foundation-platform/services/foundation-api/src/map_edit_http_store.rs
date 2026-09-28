//! Bounded HTTP adapter for the map edit store Worker (ADR-0112).
//!
//! The Worker (`foundation-map-edit-gateway`) owns the store and its contract checks; this adapter
//! only carries an edit that already passed authorization and the topology checks, with the
//! writer token only this API holds.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use catalog_application::ports::{MapEditAppended, MapEditRecord, MapEditStore, MapEditStoreError};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};

use crate::identity_token_verifier::parse_secure_endpoint_url;

pub const MAP_EDIT_GATEWAY_BASE_URL_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL";
pub const MAP_EDIT_WRITE_TOKEN_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The Worker refuses a shorter secret as misconfigured; refusing it here fails at startup instead.
const MIN_WRITE_TOKEN_LENGTH: usize = 32;

/// Where the edit store is and the secret that lets this API write to it.
#[derive(Clone, Eq, PartialEq)]
pub struct MapEditStoreConfig {
    base_url: String,
    write_token: String,
}

impl fmt::Debug for MapEditStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MapEditStoreConfig")
            .field("base_url", &self.base_url)
            .field("write_token", &"<redacted>")
            .finish()
    }
}

impl MapEditStoreConfig {
    /// Reads both variables. Neither set means the store is not configured (edits answer 503);
    /// only one set, a short token, or a non-https address is a startup error.
    pub fn from_vars(lookup: &impl Fn(&str) -> Option<String>) -> anyhow::Result<Option<Self>> {
        let read = |key: &str| {
            lookup(key)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        match (read(MAP_EDIT_GATEWAY_BASE_URL_ENV), read(MAP_EDIT_WRITE_TOKEN_ENV)) {
            (None, None) => Ok(None),
            (Some(base_url), Some(write_token)) => {
                parse_secure_endpoint_url(&base_url).map_err(|_| {
                    anyhow::anyhow!("{MAP_EDIT_GATEWAY_BASE_URL_ENV} must be an https URL (http only on loopback)")
                })?;
                if write_token.len() < MIN_WRITE_TOKEN_LENGTH {
                    anyhow::bail!("{MAP_EDIT_WRITE_TOKEN_ENV} must be at least {MIN_WRITE_TOKEN_LENGTH} characters");
                }
                Ok(Some(Self { base_url, write_token }))
            }
            _ => anyhow::bail!(
                "{MAP_EDIT_GATEWAY_BASE_URL_ENV} and {MAP_EDIT_WRITE_TOKEN_ENV} must be set together"
            ),
        }
    }
}

/// The edit store reached over HTTPS.
pub struct HttpMapEditStore {
    client: Client,
    base_url: Url,
    write_token: String,
}

impl HttpMapEditStore {
    pub fn new(config: MapEditStoreConfig) -> anyhow::Result<Self> {
        let base_url = parse_secure_endpoint_url(&config.base_url)
            .map_err(|_| anyhow::anyhow!("{MAP_EDIT_GATEWAY_BASE_URL_ENV} is not a secure URL"))?;
        let client = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            base_url,
            write_token: config.write_token,
        })
    }

    fn edits_url(&self, unit: &str) -> Result<Url, MapEditStoreError> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|()| {
                MapEditStoreError::Unavailable("edit store URL cannot carry a path".to_owned())
            })?
            .pop_if_empty()
            .extend(["edits", unit]);
        Ok(url)
    }
}

#[derive(Serialize)]
struct AppendRequest<'a> {
    feature_id: &'a str,
    op: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    geometry: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<&'a serde_json::Value>,
    editor: String,
    idempotency_key: &'a str,
}

#[derive(Deserialize)]
struct AppendResponse {
    change_seq: u64,
}

#[derive(Deserialize)]
struct StoreError {
    error: String,
}

#[async_trait]
impl MapEditStore for HttpMapEditStore {
    async fn append(&self, record: &MapEditRecord) -> Result<MapEditAppended, MapEditStoreError> {
        let body = AppendRequest {
            feature_id: &record.feature_id,
            op: record.operation.as_str(),
            geometry: record
                .geometry
                .as_ref()
                .map(catalog_domain::MapEditGeometry::as_json),
            properties: record.properties.as_ref(),
            editor: record.editor.to_string(),
            idempotency_key: &record.idempotency_key,
        };
        let response = self
            .client
            .post(self.edits_url(&record.unit)?)
            .bearer_auth(&self.write_token)
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                MapEditStoreError::Unavailable(format!("edit store unreachable: {error}"))
            })?;
        let status = response.status();
        if status == StatusCode::CREATED || status == StatusCode::OK {
            let appended: AppendResponse = response.json().await.map_err(|_| {
                MapEditStoreError::Unavailable("edit store answered an unreadable body".to_owned())
            })?;
            return Ok(MapEditAppended {
                change_seq: appended.change_seq,
                replayed: status == StatusCode::OK,
            });
        }
        let code = response.json::<StoreError>().await.map_or_else(
            |_| format!("status {}", status.as_u16()),
            |error| error.error,
        );
        Err(match status {
            StatusCode::BAD_REQUEST
            | StatusCode::NOT_FOUND
            | StatusCode::PAYLOAD_TOO_LARGE
            | StatusCode::UNPROCESSABLE_ENTITY => MapEditStoreError::Refused(code),
            StatusCode::CONFLICT => MapEditStoreError::Conflict(code),
            // 401/403 mean this API's own token or wiring is wrong; the caller cannot fix that.
            _ => MapEditStoreError::Unavailable(format!(
                "edit store answered {}: {code}",
                status.as_u16()
            )),
        })
    }
}

/// The store when this deployment has no edit store configured: every edit is refused as
/// unavailable, so a missing variable can never look like a saved edit.
pub struct UnconfiguredMapEditStore;

#[async_trait]
impl MapEditStore for UnconfiguredMapEditStore {
    async fn append(&self, _record: &MapEditRecord) -> Result<MapEditAppended, MapEditStoreError> {
        Err(MapEditStoreError::Unavailable(
            "the map edit store is not configured".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog_domain::{MapEditGeometry, MapEditOperation};
    use foundation_shared_kernel::ids::StaffId;
    use serde_json::json;
    use wiremock::matchers::{bearer_token, body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TOKEN: &str = "synthetic-writer-token-0123456789abcdef";

    fn vars<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    fn record() -> Result<MapEditRecord, Box<dyn std::error::Error>> {
        // The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py.
        let geometry = MapEditGeometry::parse(json!({"type": "Polygon", "coordinates": [[
            [127.123, 36.123], [127.1239, 36.123], [127.1239, 36.1239], [127.123, 36.1239], [127.123, 36.123]
        ]]}))?;
        Ok(MapEditRecord {
            unit: "complex".to_owned(),
            feature_id: "00000000-0000-5000-8000-000000000001".to_owned(),
            operation: MapEditOperation::Upsert,
            geometry: Some(geometry),
            properties: Some(json!({"official_complex_code": "SYN"})),
            editor: StaffId::new(uuid::Uuid::nil()),
            idempotency_key: "synthetic-key".to_owned(),
        })
    }

    fn store(server: &MockServer) -> Result<HttpMapEditStore, Box<dyn std::error::Error>> {
        let config = MapEditStoreConfig::from_vars(&vars(&[
            (MAP_EDIT_GATEWAY_BASE_URL_ENV, &server.uri()),
            (MAP_EDIT_WRITE_TOKEN_ENV, TOKEN),
        ]))?
        .ok_or("configured store expected")?;
        Ok(HttpMapEditStore::new(config)?)
    }

    #[test]
    fn configuration_is_both_or_neither_and_never_prints_the_token() -> anyhow::Result<()> {
        assert_eq!(MapEditStoreConfig::from_vars(&vars(&[]))?, None);
        assert!(
            MapEditStoreConfig::from_vars(&vars(&[(MAP_EDIT_WRITE_TOKEN_ENV, TOKEN)])).is_err()
        );
        assert!(MapEditStoreConfig::from_vars(&vars(&[
            (MAP_EDIT_GATEWAY_BASE_URL_ENV, "http://edits.example.test"),
            (MAP_EDIT_WRITE_TOKEN_ENV, TOKEN),
        ]))
        .is_err());
        assert!(MapEditStoreConfig::from_vars(&vars(&[
            (MAP_EDIT_GATEWAY_BASE_URL_ENV, "https://edits.example.test"),
            (MAP_EDIT_WRITE_TOKEN_ENV, "short"),
        ]))
        .is_err());
        let config = MapEditStoreConfig::from_vars(&vars(&[
            (MAP_EDIT_GATEWAY_BASE_URL_ENV, "https://edits.example.test"),
            (MAP_EDIT_WRITE_TOKEN_ENV, TOKEN),
        ]))?;
        assert!(!format!("{config:?}").contains(TOKEN));
        Ok(())
    }

    #[tokio::test]
    async fn an_edit_is_sent_with_the_writer_token_and_its_sequence_is_returned(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let record = record()?;
        Mock::given(method("POST"))
            .and(path("/edits/complex"))
            .and(bearer_token(TOKEN))
            .and(body_json(json!({
                "feature_id": record.feature_id,
                "op": "upsert",
                "geometry": record.geometry.as_ref().map(MapEditGeometry::as_json),
                "properties": {"official_complex_code": "SYN"},
                "editor": uuid::Uuid::nil().to_string(),
                "idempotency_key": "synthetic-key",
            })))
            .respond_with(
                ResponseTemplate::new(201)
                    .set_body_json(json!({"unit": "complex", "change_seq": 7})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let appended = store(&server)?.append(&record).await?;
        assert_eq!(
            appended,
            MapEditAppended {
                change_seq: 7,
                replayed: false
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn store_answers_map_to_refused_conflict_and_unavailable(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (status, expected) in [
            (200, None),
            (
                422,
                Some(MapEditStoreError::Refused("out_of_bounds".to_owned())),
            ),
            (
                409,
                Some(MapEditStoreError::Conflict("out_of_bounds".to_owned())),
            ),
            (
                401,
                Some(MapEditStoreError::Unavailable(
                    "edit store answered 401: out_of_bounds".to_owned(),
                )),
            ),
            (
                500,
                Some(MapEditStoreError::Unavailable(
                    "edit store answered 500: out_of_bounds".to_owned(),
                )),
            ),
        ] {
            let server = MockServer::start().await;
            let body = if status == 200 {
                json!({"change_seq": 3})
            } else {
                json!({"error": "out_of_bounds"})
            };
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status).set_body_json(body))
                .mount(&server)
                .await;
            let result = store(&server)?.append(&record()?).await;
            match expected {
                None => assert_eq!(
                    result,
                    Ok(MapEditAppended {
                        change_seq: 3,
                        replayed: true
                    })
                ),
                Some(error) => assert_eq!(result, Err(error), "status {status}"),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn an_unconfigured_store_never_reports_a_saved_edit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            UnconfiguredMapEditStore.append(&record()?).await,
            Err(MapEditStoreError::Unavailable(_))
        ));
        Ok(())
    }
}
