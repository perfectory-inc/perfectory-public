//! Foundation Platform Catalog-backed industrial-complex reader.
//!
//! Gongzzang owns the `/api/complexes` route shape; Foundation Platform owns the canonical
//! industrial-complex record. This adapter is the only translation layer between the Foundation
//! Platform public Catalog API and the Gongzzang route-facing [`ComplexCatalogRecord`].

#![allow(clippy::disallowed_types, clippy::module_name_repetitions)]

use std::sync::Arc;

use shared_kernel::lakehouse_complex_id::LakehouseComplexId;
use thiserror::Error;

use crate::routes::complexes::search::ComplexSearchRequest;
use crate::routes::complexes::{
    ComplexCatalogError, ComplexCatalogListRecord, ComplexCatalogPage, ComplexCatalogReader,
    ComplexCatalogRecord, ComplexGoldProfileRef, ComplexSearchRejected,
};
use foundation_platform_client::{
    CatalogIndustrialComplexListResponse, CatalogIndustrialComplexResponse,
    FoundationCatalogClient, FoundationCatalogClientConfigError, FoundationServiceAuth,
};

/// Configuration error for the Foundation Platform complex reader.
#[derive(Debug, Error)]
pub enum FoundationPlatformComplexReaderConfigError {
    /// Foundation Platform Catalog client configuration failed.
    #[error(transparent)]
    CatalogClient(#[from] FoundationCatalogClientConfigError),
}

#[derive(Debug, Error)]
enum FoundationPlatformComplexReaderError {
    #[error("Foundation Platform complex lookup HTTP request failed: {source}")]
    Request {
        #[source]
        source: reqwest::Error,
    },
    #[error("Foundation Platform complex lookup returned status {status}")]
    Status { status: reqwest::StatusCode },
    /// The provider answered with a complex carrying a different lakehouse id than was asked for.
    ///
    /// Checked rather than assumed: the whole reason this route is keyed on the lakehouse id is
    /// that two id spaces name the same complex, and a silently mismatched answer is exactly the
    /// failure that motivated the key.
    #[error(
        "Foundation Platform complex lookup identity mismatch: requested={requested} response={response}"
    )]
    IdentityMismatch { requested: String, response: String },
}

/// Complex reader that consumes Foundation Platform Catalog's public HTTP API.
pub struct FoundationPlatformComplexCatalogReader {
    catalog_client: FoundationCatalogClient,
}

impl FoundationPlatformComplexCatalogReader {
    /// Creates a Foundation Platform HTTP-backed complex reader.
    ///
    /// # Errors
    ///
    /// Returns an error when `base_url` is invalid or the HTTP client cannot be constructed.
    pub fn new(
        base_url: &str,
        auth: Option<FoundationServiceAuth>,
    ) -> Result<Self, FoundationPlatformComplexReaderConfigError> {
        let catalog_client = FoundationCatalogClient::new(base_url, auth)?;
        Ok(Self { catalog_client })
    }
}

impl ComplexCatalogReader for FoundationPlatformComplexCatalogReader {
    fn find_by_lakehouse_id<'a>(
        &'a self,
        lakehouse_complex_id: &'a LakehouseComplexId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<ComplexCatalogRecord>, ComplexCatalogError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let response = self
                .catalog_client
                .get_complex_by_lakehouse_id_response(lakehouse_complex_id.as_str())
                .await
                .map_err(|source| Box::new(source) as ComplexCatalogError)?;
            let status = response.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                return Err(
                    Box::new(FoundationPlatformComplexReaderError::Status { status })
                        as ComplexCatalogError,
                );
            }

            let complex = response
                .json::<CatalogIndustrialComplexResponse>()
                .await
                .map_err(|source| {
                    Box::new(FoundationPlatformComplexReaderError::Request { source })
                        as ComplexCatalogError
                })?;

            complex_record_from_response(lakehouse_complex_id, complex)
                .map(Some)
                .map_err(|source| Box::new(source) as ComplexCatalogError)
        })
    }

    fn search<'a>(
        &'a self,
        query: &'a ComplexSearchRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ComplexCatalogPage, ComplexCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let response = self
                .catalog_client
                .list_complexes_response(&query.to_catalog_query())
                .await
                .map_err(|source| Box::new(source) as ComplexCatalogError)?;
            let status = response.status();
            if status.is_client_error() {
                // 뒷단이 요청을 거절한 것이지 답을 못 준 것이 아니에요. 라우트가 이것을 `400` 으로
                // 옮겨, 고칠 수 있는 입력 오류가 "잠시 후 다시 시도해 주세요"로 끝나지 않게 해요.
                return Err(Box::new(ComplexSearchRejected {
                    detail: format!("catalog answered {status}"),
                }) as ComplexCatalogError);
            }
            if !status.is_success() {
                return Err(
                    Box::new(FoundationPlatformComplexReaderError::Status { status })
                        as ComplexCatalogError,
                );
            }

            let page = response
                .json::<CatalogIndustrialComplexListResponse>()
                .await
                .map_err(|source| {
                    Box::new(FoundationPlatformComplexReaderError::Request { source })
                        as ComplexCatalogError
                })?;

            Ok(complex_page_from_response(page))
        })
    }
}

fn complex_page_from_response(page: CatalogIndustrialComplexListResponse) -> ComplexCatalogPage {
    ComplexCatalogPage {
        complexes: page
            .complexes
            .into_iter()
            .map(|complex| ComplexCatalogListRecord {
                lakehouse_complex_id: complex.lakehouse_complex_id,
                official_complex_code: complex.official_complex_code,
                name: complex.name,
                kind: complex.kind,
                status: complex.status,
                address_text: complex.address_text,
            })
            .collect(),
        total: page.total,
        page: page.page,
        size: page.size,
        has_next: page.has_next,
    }
}

/// Builds the Foundation Platform Catalog-backed complex reader behind the trait-object port.
///
/// # Errors
///
/// Returns a config error when `base_url` is invalid.
pub fn build_foundation_platform_complex_catalog_reader(
    base_url: &str,
    auth: Option<FoundationServiceAuth>,
) -> Result<Arc<dyn ComplexCatalogReader>, FoundationPlatformComplexReaderConfigError> {
    Ok(Arc::new(FoundationPlatformComplexCatalogReader::new(
        base_url, auth,
    )?))
}

fn complex_record_from_response(
    requested: &LakehouseComplexId,
    response: CatalogIndustrialComplexResponse,
) -> Result<ComplexCatalogRecord, FoundationPlatformComplexReaderError> {
    match response.lakehouse_complex_id.as_deref() {
        Some(answered) if answered == requested.as_str() => {}
        other => {
            return Err(FoundationPlatformComplexReaderError::IdentityMismatch {
                requested: requested.as_str().to_owned(),
                response: other.unwrap_or("null").to_owned(),
            })
        }
    }

    Ok(ComplexCatalogRecord {
        official_complex_code: response.official_complex_code,
        name: response.name,
        kind: response.kind,
        status: response.status,
        address_text: response.address_text,
        area_m2: response.area_m2,
        management_agency_name: response.management_agency_name,
        developer_name: response.developer_name,
        designated_date: response.designated_date,
        construction_start_date: response.construction_start_date,
        completion_date: response.completion_date,
        development_progress_percent: response.development_progress_percent,
        lot_sales_status: response.lot_sales_status,
        business_period_raw: response.business_period_raw,
        designation_basis_law_raw: response.designation_basis_law_raw,
        development_method_raw: response.development_method_raw,
        development_purpose_raw: response.development_purpose_raw,
        invited_industries_raw: response.invited_industries_raw,
        // A pointer whose template carries no substitution point resolves to no URL, and a
        // profile reference without one is dropped rather than passed on half-built: the panel's
        // contract is "here is where the rest is", and a reference that cannot be fetched makes
        // that a promise the response cannot keep. Dropping it lands the panel in the
        // no-pointer state, which is the state it already has to handle.
        gold_profile: response.gold_pointer.and_then(|pointer| {
            pointer.profile_url().map(|url| ComplexGoldProfileRef {
                url,
                version: pointer.current_version,
                checksum_sha256: pointer.profile_checksum_sha256,
            })
        }),
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

    use super::*;

    const REQUEST_ID: &str = "7df3859c-1e0a-51fa-8b7d-9a1c2e3f4a5b";
    const OTHER_ID: &str = "9a8b7c6d-5e4f-53a2-9b1c-0d1e2f3a4b5c";

    fn requested() -> LakehouseComplexId {
        LakehouseComplexId::try_new(REQUEST_ID).expect("valid lakehouse id")
    }

    #[test]
    fn constructor_enforces_foundation_endpoint_security() {
        assert!(
            FoundationPlatformComplexCatalogReader::new("https://foundation.example", None).is_ok()
        );
        assert!(FoundationPlatformComplexCatalogReader::new("http://127.0.0.1:8080", None).is_ok());
        for invalid in [
            "http://foundation.example",
            "https://user:password@foundation.example",
            "https://foundation.example?tenant=other",
            "https://foundation.example#fragment",
        ] {
            assert!(FoundationPlatformComplexCatalogReader::new(invalid, None).is_err());
        }
    }

    #[tokio::test]
    async fn lookup_success_maps_the_sourced_description() {
        let base_url = spawn_foundation_response(
            "HTTP/1.1 200 OK",
            &complex_json(
                REQUEST_ID,
                r#""status":"operating","completion_date":"1998-12-31","#,
            ),
        );
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let record = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.official_complex_code, "111010");
        assert_eq!(record.name, "테스트산업단지");
        assert_eq!(record.kind, "national");
        assert_eq!(record.status.as_deref(), Some("operating"));
        assert_eq!(record.completion_date.as_deref(), Some("1998-12-31"));
        assert_eq!(record.area_m2, 1_234_567);
    }

    #[tokio::test]
    async fn absent_optional_columns_stay_absent() {
        // No `status`, no `completion_date` in the payload. The reader must carry the absence
        // through rather than substituting a placeholder the source never published.
        let base_url = spawn_foundation_response("HTTP/1.1 200 OK", &complex_json(REQUEST_ID, ""));
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let record = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap()
            .unwrap();

        assert!(record.status.is_none());
        assert!(record.completion_date.is_none());
        assert!(record.development_progress_percent.is_none());
    }

    #[tokio::test]
    async fn zero_progress_is_a_value_not_an_absence() {
        let base_url = spawn_foundation_response(
            "HTTP/1.1 200 OK",
            &complex_json(REQUEST_ID, r#""development_progress_percent":"0.00","#),
        );
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let record = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.development_progress_percent.as_deref(), Some("0.00"));
    }

    #[tokio::test]
    async fn lookup_404_returns_none() {
        let base_url =
            spawn_foundation_response("HTTP/1.1 404 Not Found", r#"{"error":"not found"}"#);
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        assert!(reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn lookup_rejects_a_response_naming_a_different_complex() {
        let base_url = spawn_foundation_response("HTTP/1.1 200 OK", &complex_json(OTHER_ID, ""));
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let error = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("identity mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn lookup_rejects_a_response_carrying_no_lakehouse_id() {
        // A complex registered through the write API carries `lakehouse_complex_id: null`. Such a
        // row cannot be the answer to a lakehouse-id lookup, so accepting it would mean serving a
        // different complex than the one clicked.
        let base_url = spawn_foundation_response(
            "HTTP/1.1 200 OK",
            r#"{"lakehouse_complex_id":null,"official_complex_code":"111010","name":"x","kind":"national","area_m2":1}"#,
        );
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let error = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("identity mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn lookup_requests_the_published_by_lakehouse_id_path() {
        let (base_url, requests) =
            spawn_foundation_response_capture("HTTP/1.1 200 OK", &complex_json(REQUEST_ID, ""));
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        reader.find_by_lakehouse_id(&requested()).await.unwrap();

        let request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("captured request");
        assert!(
            request.starts_with(&format!(
                "GET /catalog/v1/complexes/by-lakehouse-id/{REQUEST_ID} "
            )),
            "request path mismatch: {request}"
        );
    }

    fn spawn_foundation_response(status_line: &str, body: &str) -> String {
        let (base_url, _requests) = spawn_foundation_response_capture(status_line, body);
        base_url
    }

    fn spawn_foundation_response_capture(
        status_line: &str,
        body: &str,
    ) -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let status_line = status_line.to_owned();
        let body = body.to_owned();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let read = stream.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            let _ = tx.send(request.to_string());
            let response = format!(
                "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });

        (format!("http://{addr}"), rx)
    }

    fn complex_json(lakehouse_complex_id: &str, extra_fields: &str) -> String {
        format!(
            r#"{{
                "id":"01a0136d-2b3c-7e61-8f90-a1b2c3d4e5f6",
                "lakehouse_complex_id":"{lakehouse_complex_id}",
                "official_complex_code":"111010",
                "name":"테스트산업단지",
                "kind":"national",
                {extra_fields}
                "area_m2":1234567,
                "version":1,
                "updated_at":"2026-08-01T00:00:00Z"
            }}"#
        )
    }

    /// A `gold_pointer` block as Catalog serializes it, with the address template under test.
    fn gold_pointer_json(template: &str) -> String {
        format!(
            r#""gold_pointer":{{
                "current_version":"018f0000-0000-7000-8000-000000000001",
                "previous_version":null,
                "profile_object_key":"gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json",
                "profile_url_template":"{template}",
                "spatial_locator_object_key":null,
                "source_record_id":"018f0000-0000-7000-8000-0000000000ff",
                "source_snapshot_id":"snapshot-1",
                "iceberg_snapshot_id":"999990000000000001",
                "profile_row_count":1,
                "profile_checksum_sha256":"{checksum}",
                "published_at":"2026-08-01T00:00:00Z"
            }},"#,
            checksum = "a".repeat(64)
        )
    }

    /// The state every one of the 1,448 canonical rows is in today: nothing has been exported, so
    /// no pointer exists. The panel's contract has to survive that, because that is the only state
    /// it has ever been observed in.
    #[tokio::test]
    async fn a_complex_without_a_published_profile_maps_to_no_reference() {
        let base_url = spawn_foundation_response("HTTP/1.1 200 OK", &complex_json(REQUEST_ID, ""));
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let record = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.gold_profile, None);
        assert_eq!(record.name, "테스트산업단지");
    }

    #[tokio::test]
    async fn a_published_pointer_becomes_a_fetchable_reference() {
        let base_url = spawn_foundation_response(
            "HTTP/1.1 200 OK",
            &complex_json(
                REQUEST_ID,
                &gold_pointer_json("https://lakehouse.example.com/{object_key}"),
            ),
        );
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let record = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap()
            .unwrap();

        let profile = record
            .gold_profile
            .expect("a published pointer maps through");
        assert_eq!(
            profile.url,
            "https://lakehouse.example.com/gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json"
        );
        assert_eq!(profile.version, "018f0000-0000-7000-8000-000000000001");
        assert_eq!(profile.checksum_sha256, "a".repeat(64));
    }

    /// A template that names no substitution point cannot address this complex's object, so the
    /// reference is dropped and the panel lands in the no-profile state rather than being handed a
    /// URL that would fetch some other complex's description.
    #[tokio::test]
    async fn a_pointer_whose_template_cannot_address_the_object_is_dropped() {
        let base_url = spawn_foundation_response(
            "HTTP/1.1 200 OK",
            &complex_json(
                REQUEST_ID,
                &gold_pointer_json("https://lakehouse.example.com/profile.json"),
            ),
        );
        let reader = FoundationPlatformComplexCatalogReader::new(&base_url, None).unwrap();

        let record = reader
            .find_by_lakehouse_id(&requested())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.gold_profile, None);
    }
}
