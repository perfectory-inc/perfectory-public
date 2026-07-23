//! Foundation Platform Catalog-backed building reader.
//!
//! Gongzzang owns the `/api/buildings` route shape, but Foundation Platform owns
//! canonical catalog building data. This adapter is the only translation layer
//! between the Foundation Platform public Catalog API and the Gongzzang route-facing
//! `BuildingRegisterRecord`.

#![allow(clippy::disallowed_types, clippy::module_name_repetitions)]

use std::sync::Arc;

use shared_kernel::pnu::Pnu;
use thiserror::Error;

use crate::routes::buildings::{
    BuildingRegisterError, BuildingRegisterReader, BuildingRegisterRecord,
};
use foundation_platform_client::{
    CatalogBuildingResponse, FoundationCatalogClient, FoundationCatalogClientConfigError,
    FoundationServiceAuth,
};

/// Configuration error for the Foundation Platform building reader.
#[derive(Debug, Error)]
pub enum FoundationPlatformBuildingReaderConfigError {
    /// Foundation Platform Catalog client configuration failed.
    #[error(transparent)]
    CatalogClient(#[from] FoundationCatalogClientConfigError),
}

#[derive(Debug, Error)]
enum FoundationPlatformBuildingReaderError {
    #[error("Foundation Platform building lookup HTTP request failed: {source}")]
    Request {
        #[source]
        source: reqwest::Error,
    },
    #[error("Foundation Platform building lookup returned status {status}")]
    Status { status: reqwest::StatusCode },
    #[error(
        "Foundation Platform building stories value is outside Gongzzang route contract: id={id} stories={stories}"
    )]
    InvalidStories { id: String, stories: i16 },
    #[error(
        "Foundation Platform building below-ground floor count is outside Gongzzang route contract: id={id} below_ground_floors={below_ground_floors}"
    )]
    InvalidBelowGroundFloors {
        id: String,
        below_ground_floors: i16,
    },
}

/// Building reader that consumes Foundation Platform Catalog's public HTTP API.
pub struct FoundationPlatformBuildingRegisterReader {
    catalog_client: FoundationCatalogClient,
}

impl FoundationPlatformBuildingRegisterReader {
    /// Creates a Foundation Platform HTTP-backed building reader.
    ///
    /// # Errors
    ///
    /// Returns an error when `base_url` is empty or the HTTP client cannot be
    /// constructed.
    pub fn new(
        base_url: &str,
        auth: Option<FoundationServiceAuth>,
    ) -> Result<Self, FoundationPlatformBuildingReaderConfigError> {
        let catalog_client = FoundationCatalogClient::new(base_url, auth)?;
        Ok(Self { catalog_client })
    }
}

impl BuildingRegisterReader for FoundationPlatformBuildingRegisterReader {
    fn list_by_pnu<'a>(
        &'a self,
        pnu: &'a Pnu,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<BuildingRegisterRecord>, BuildingRegisterError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let response = self
                .catalog_client
                .list_buildings_by_pnu_response(pnu.as_str())
                .await
                .map_err(|source| Box::new(source) as BuildingRegisterError)?;
            let status = response.status();
            if !status.is_success() {
                return Err(
                    Box::new(FoundationPlatformBuildingReaderError::Status { status })
                        as BuildingRegisterError,
                );
            }

            let buildings = response
                .json::<Vec<CatalogBuildingResponse>>()
                .await
                .map_err(|source| {
                    Box::new(FoundationPlatformBuildingReaderError::Request { source })
                        as BuildingRegisterError
                })?;

            buildings
                .into_iter()
                .map(building_record_from_response)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| Box::new(source) as BuildingRegisterError)
        })
    }
}

/// Builds the Foundation Platform Catalog-backed building reader.
///
/// # Errors
///
/// Returns an error when `base_url` is invalid for reader construction.
pub fn build_foundation_platform_building_register_reader(
    base_url: &str,
    auth: Option<FoundationServiceAuth>,
) -> Result<Arc<dyn BuildingRegisterReader>, FoundationPlatformBuildingReaderConfigError> {
    Ok(Arc::new(FoundationPlatformBuildingRegisterReader::new(
        base_url, auth,
    )?))
}

fn building_record_from_response(
    value: CatalogBuildingResponse,
) -> Result<BuildingRegisterRecord, FoundationPlatformBuildingReaderError> {
    let above_ground_floors = u8::try_from(value.stories).map_err(|_| {
        FoundationPlatformBuildingReaderError::InvalidStories {
            id: value.id.clone(),
            stories: value.stories,
        }
    })?;
    let below_ground_floors = u8::try_from(value.below_ground_floors).map_err(|_| {
        FoundationPlatformBuildingReaderError::InvalidBelowGroundFloors {
            id: value.id.clone(),
            below_ground_floors: value.below_ground_floors,
        }
    })?;

    Ok(BuildingRegisterRecord {
        id: value.id,
        name: String::new(),
        address: None,
        purpose: value.purpose_code,
        structure: value.structure_code,
        plot_area_m2: None,
        building_area_m2: None,
        building_coverage_ratio: None,
        total_area_m2: value.floor_area_m2,
        floor_area_ratio: None,
        above_ground_floors,
        below_ground_floors,
        has_rooftop: value.has_rooftop,
        rooftop_area_m2: value.rooftop_area_m2,
        rooftop_usage: value.rooftop_usage,
        height_m: None,
        passenger_elevators: None,
        emergency_elevators: None,
        indoor_self_parking: None,
        outdoor_self_parking: None,
        annex_building_count: None,
        annex_building_area_m2: None,
        permitted_at: None,
        started_at: None,
        approved_at: None,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::{self, Receiver};
    use std::thread;
    use std::time::Duration;

    use foundation_platform_client::FoundationServiceAuth;

    use super::*;

    #[test]
    fn constructor_enforces_foundation_endpoint_security() {
        assert!(
            FoundationPlatformBuildingRegisterReader::new("https://foundation.example", None)
                .is_ok()
        );
        assert!(FoundationPlatformBuildingRegisterReader::new("http://[::1]:8080", None).is_ok());
        for invalid in [
            "http://foundation.example",
            "https://user:password@foundation.example",
            "https://foundation.example?tenant=other",
            "https://foundation.example#fragment",
        ] {
            assert!(FoundationPlatformBuildingRegisterReader::new(invalid, None).is_err());
        }
    }

    #[tokio::test]
    async fn reads_buildings_from_foundation_platform_catalog_by_pnu() {
        let body = r#"
[
  {
    "id": "building-01",
    "parcel_id": "parcel-01",
    "purpose_code": "factory",
    "structure_code": "steel",
    "floor_area_m2": 1234.5,
    "stories": 7,
    "below_ground_floors": 2,
    "has_rooftop": true,
    "rooftop_area_m2": 13.87,
    "rooftop_usage": "기타제2종근린생활시설 · 주차장",
    "built_year": 2020,
    "updated_at": "2026-05-28T00:00:00Z"
  }
]
"#;
        let (base_url, request_line) = spawn_foundation_platform_response("HTTP/1.1 200 OK", body);
        let reader =
            FoundationPlatformBuildingRegisterReader::new(&base_url, None).expect("reader");
        let pnu = Pnu::try_new("9999900501107370000").expect("valid pnu");

        let records = reader.list_by_pnu(&pnu).await.expect("records");

        assert_eq!(
            request_line
                .recv_timeout(Duration::from_secs(2))
                .expect("request line"),
            "GET /catalog/v1/parcels/by-pnu/9999900501107370000/buildings HTTP/1.1"
        );
        assert_eq!(
            records,
            vec![BuildingRegisterRecord {
                id: "building-01".to_owned(),
                name: String::new(),
                address: None,
                purpose: "factory".to_owned(),
                structure: "steel".to_owned(),
                plot_area_m2: None,
                building_area_m2: None,
                building_coverage_ratio: None,
                total_area_m2: 1234.5,
                floor_area_ratio: None,
                above_ground_floors: 7,
                below_ground_floors: 2,
                has_rooftop: true,
                rooftop_area_m2: Some(13.87),
                rooftop_usage: "기타제2종근린생활시설 · 주차장".to_owned(),
                height_m: None,
                passenger_elevators: None,
                emergency_elevators: None,
                indoor_self_parking: None,
                outdoor_self_parking: None,
                annex_building_count: None,
                annex_building_area_m2: None,
                permitted_at: None,
                started_at: None,
                approved_at: None,
            }]
        );
    }

    #[tokio::test]
    async fn returns_error_for_foundation_platform_non_success_status() {
        let (base_url, _request_line) =
            spawn_foundation_platform_responses("HTTP/1.1 503 Service Unavailable", "{}", 2);
        let reader =
            FoundationPlatformBuildingRegisterReader::new(&base_url, None).expect("reader");
        let pnu = Pnu::try_new("9999900501107370000").expect("valid pnu");

        let error = reader.list_by_pnu(&pnu).await.expect_err("status error");

        assert!(error.to_string().contains("503"));
    }

    #[tokio::test]
    async fn rejects_foundation_platform_building_story_count_outside_route_contract() {
        let body = r#"
[
  {
    "id": "building-01",
    "parcel_id": "parcel-01",
    "purpose_code": "factory",
    "structure_code": "steel",
    "floor_area_m2": 1234.5,
    "stories": -1,
    "below_ground_floors": 0,
    "has_rooftop": false,
    "built_year": 2020,
    "updated_at": "2026-05-28T00:00:00Z"
  }
]
"#;
        let (base_url, _request_line) = spawn_foundation_platform_response("HTTP/1.1 200 OK", body);
        let reader =
            FoundationPlatformBuildingRegisterReader::new(&base_url, None).expect("reader");
        let pnu = Pnu::try_new("9999900501107370000").expect("valid pnu");

        let error = reader.list_by_pnu(&pnu).await.expect_err("invalid stories");

        assert!(error.to_string().contains("stories"));
    }

    #[tokio::test]
    async fn sends_foundation_workload_bearer_token() {
        let body = r#"
[
  {
    "id": "building-01",
    "parcel_id": "parcel-01",
    "purpose_code": "factory",
    "structure_code": "steel",
    "floor_area_m2": 1234.5,
    "stories": 7,
    "below_ground_floors": 1,
    "has_rooftop": false,
    "built_year": 2020,
    "updated_at": "2026-05-28T00:00:00Z"
  }
]
"#;
        let (base_url, requests) =
            spawn_foundation_platform_request_capture("HTTP/1.1 200 OK", body);
        let auth = test_service_auth();
        let reader =
            FoundationPlatformBuildingRegisterReader::new(&base_url, Some(auth)).expect("reader");
        let pnu = Pnu::try_new("9999900501107370000").expect("valid pnu");

        reader.list_by_pnu(&pnu).await.expect("records");

        let request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("captured request");
        assert!(
            request.contains("\r\nauthorization: Bearer zitadel-workload-token-32-valid\r\n")
                || request
                    .contains("\r\nAuthorization: Bearer zitadel-workload-token-32-valid\r\n"),
            "request missing service bearer token: {request}"
        );
    }

    fn spawn_foundation_platform_response(
        status_line: &str,
        body: &str,
    ) -> (String, Receiver<String>) {
        spawn_foundation_platform_responses(status_line, body, 1)
    }

    fn test_service_auth() -> FoundationServiceAuth {
        FoundationServiceAuth::from_bearer_token("zitadel-workload-token-32-valid")
            .expect("service auth")
    }

    fn spawn_foundation_platform_responses(
        status_line: &str,
        body: &str,
        request_count: usize,
    ) -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let status_line = status_line.to_owned();
        let body = body.to_owned();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut request = [0_u8; 2048];
                let read = stream.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..read]);
                tx.send(request.lines().next().unwrap_or_default().to_owned())
                    .expect("send request line");
                let response = format!(
                    "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
            }
        });

        (format!("http://{addr}"), rx)
    }

    fn spawn_foundation_platform_request_capture(
        status_line: &str,
        body: &str,
    ) -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let status_line = status_line.to_owned();
        let body = body.to_owned();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let read = stream.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            tx.send(request.to_string()).expect("send request");
            let response = format!(
                "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });

        (format!("http://{addr}"), rx)
    }
}
