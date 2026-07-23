//! Foundation Platform Catalog-backed parcel lookup adapter.
//!
//! `parcel-lookup` owns only the Gongzzang port. This module owns the HTTP
//! integration because Foundation Platform is an external runtime dependency of the
//! API service, not a domain crate concern.

#![allow(clippy::disallowed_types, clippy::module_name_repetitions)]

use std::sync::Arc;

use async_trait::async_trait;
use parcel_lookup::{LookupError, ParcelInfo, ParcelInfoLookup};
use reqwest::StatusCode;
use shared_kernel::admin_division::{AdminDivision, EupmyeondongCode, SidoCode, SigunguCode};
use shared_kernel::land_use_type::LandUseType;
use shared_kernel::pnu::Pnu;
use thiserror::Error;
use tracing::instrument;

use foundation_platform_client::{
    CatalogParcelResponse, FoundationCatalogClient, FoundationCatalogClientConfigError,
    FoundationServiceAuth,
};

/// Foundation Platform HTTP-backed parcel lookup adapter.
pub struct FoundationPlatformParcelInfoLookup {
    catalog_client: FoundationCatalogClient,
}

impl FoundationPlatformParcelInfoLookup {
    /// Build a Foundation Platform lookup from an API base URL.
    ///
    /// # Errors
    ///
    /// Returns a config error when the URL is empty, invalid, or the HTTP
    /// client cannot be constructed.
    pub fn new(
        base_url: &str,
        auth: Option<FoundationServiceAuth>,
    ) -> Result<Self, FoundationPlatformParcelLookupConfigError> {
        let catalog_client = FoundationCatalogClient::new(base_url, auth)?;
        Ok(Self { catalog_client })
    }
}

#[async_trait]
impl ParcelInfoLookup for FoundationPlatformParcelInfoLookup {
    #[instrument(skip(self), fields(pnu = %pnu.as_str()))]
    async fn lookup_by_pnu(&self, pnu: &Pnu) -> Result<Option<ParcelInfo>, LookupError> {
        let response = self
            .catalog_client
            .get_parcel_by_pnu_response(pnu.as_str())
            .await
            .map_err(|error| {
                LookupError::Backend(format!("Foundation Platform parcel lookup failed: {error}"))
            })?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(LookupError::Backend(format!(
                "Foundation Platform parcel lookup returned {}",
                response.status()
            )));
        }

        let parcel = response
            .json::<CatalogParcelResponse>()
            .await
            .map_err(|error| LookupError::Parse(error.to_string()))?;

        parcel_info_from_response(pnu, &parcel).map(Some)
    }
}

/// Build a Foundation Platform lookup behind the trait-object port.
///
/// # Errors
///
/// Returns a config error when `base_url` is invalid.
pub fn build_foundation_platform_parcel_info_lookup(
    base_url: &str,
    auth: Option<FoundationServiceAuth>,
) -> Result<Arc<dyn ParcelInfoLookup>, FoundationPlatformParcelLookupConfigError> {
    Ok(Arc::new(FoundationPlatformParcelInfoLookup::new(
        base_url, auth,
    )?))
}

/// Configuration errors for the Foundation Platform parcel lookup adapter.
#[derive(Debug, Error)]
pub enum FoundationPlatformParcelLookupConfigError {
    /// Foundation Platform Catalog client configuration failed.
    #[error(transparent)]
    CatalogClient(#[from] FoundationCatalogClientConfigError),
}

fn parcel_info_from_response(
    requested_pnu: &Pnu,
    response: &CatalogParcelResponse,
) -> Result<ParcelInfo, LookupError> {
    let response_pnu = Pnu::try_new(&response.pnu)
        .map_err(|error| LookupError::Parse(format!("invalid response PNU: {error}")))?;
    if response_pnu.as_str() != requested_pnu.as_str() {
        return Err(LookupError::Parse(format!(
            "response PNU mismatch: requested={}, response={}",
            requested_pnu.as_str(),
            response_pnu.as_str()
        )));
    }

    Ok(ParcelInfo {
        admin: admin_from_pnu(requested_pnu)?,
        land_use_type: land_use_type_from_foundation_platform_kind(&response.kind)?,
        zoning: None,
        official_land_price_per_m2: None,
        gosi_year_month: None,
    })
}

fn admin_from_pnu(pnu: &Pnu) -> Result<AdminDivision, LookupError> {
    let sido = SidoCode::try_new(pnu.sido_code())
        .map_err(|error| LookupError::Parse(format!("invalid PNU sido code: {error}")))?;
    let sigungu = SigunguCode::try_new(pnu.sigungu_code())
        .map_err(|error| LookupError::Parse(format!("invalid PNU sigungu code: {error}")))?;
    let eupmyeondong = EupmyeondongCode::try_new(pnu.eupmyeondong_code())
        .map_err(|error| LookupError::Parse(format!("invalid PNU eupmyeondong code: {error}")))?;

    AdminDivision::try_new(sido, sigungu, eupmyeondong)
        .map_err(|error| LookupError::Parse(format!("invalid PNU admin hierarchy: {error}")))
}

fn land_use_type_from_foundation_platform_kind(kind: &str) -> Result<LandUseType, LookupError> {
    match kind {
        "factory" => Ok(LandUseType::FactorySite),
        "support" => Ok(LandUseType::Building),
        "public" | "river" | "other" => Ok(LandUseType::Other),
        other => Err(LookupError::Parse(format!(
            "unknown Foundation Platform parcel kind: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::{self, Receiver};
    use std::thread;
    use std::time::Duration;

    use shared_kernel::land_use_type::LandUseType;

    use foundation_platform_client::FoundationServiceAuth;

    use super::*;

    const REQUEST_PNU: &str = "9999900501107370000";
    const OTHER_PNU: &str = "9999900101100010000";

    #[test]
    fn constructor_enforces_foundation_endpoint_security() {
        assert!(
            FoundationPlatformParcelInfoLookup::new("https://foundation.example", None).is_ok()
        );
        assert!(FoundationPlatformParcelInfoLookup::new("http://127.0.0.1:8080", None).is_ok());
        for invalid in [
            "http://foundation.example",
            "https://user:password@foundation.example",
            "https://foundation.example?tenant=other",
            "https://foundation.example#fragment",
        ] {
            assert!(FoundationPlatformParcelInfoLookup::new(invalid, None).is_err());
        }
    }

    #[tokio::test]
    async fn lookup_success_maps_foundation_platform_kind_and_pnu_admin() {
        let base_url = spawn_foundation_platform_response(
            REQUEST_PNU,
            "HTTP/1.1 200 OK",
            &foundation_platform_parcel_json(REQUEST_PNU, "factory"),
        );
        let lookup =
            FoundationPlatformParcelInfoLookup::new(&base_url, None).expect("valid base url");
        let pnu = Pnu::try_new(REQUEST_PNU).unwrap();

        let info = lookup.lookup_by_pnu(&pnu).await.unwrap().unwrap();

        assert_eq!(info.admin.sido.as_str(), "99");
        assert_eq!(info.admin.sigungu.as_str(), "99999");
        assert_eq!(info.admin.eupmyeondong.as_str(), "99999005");
        assert_eq!(info.land_use_type, LandUseType::FactorySite);
        assert!(info.zoning.is_none());
        assert!(info.official_land_price_per_m2.is_none());
        assert!(info.gosi_year_month.is_none());
    }

    #[tokio::test]
    async fn lookup_404_returns_none() {
        let base_url = spawn_foundation_platform_response(
            REQUEST_PNU,
            "HTTP/1.1 404 Not Found",
            r#"{"error":"not found"}"#,
        );
        let lookup =
            FoundationPlatformParcelInfoLookup::new(&base_url, None).expect("valid base url");
        let pnu = Pnu::try_new(REQUEST_PNU).unwrap();

        assert!(lookup.lookup_by_pnu(&pnu).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lookup_rejects_mismatched_response_pnu() {
        let base_url = spawn_foundation_platform_response(
            REQUEST_PNU,
            "HTTP/1.1 200 OK",
            &foundation_platform_parcel_json(OTHER_PNU, "factory"),
        );
        let lookup =
            FoundationPlatformParcelInfoLookup::new(&base_url, None).expect("valid base url");
        let pnu = Pnu::try_new(REQUEST_PNU).unwrap();

        match lookup.lookup_by_pnu(&pnu).await.unwrap_err() {
            LookupError::Parse(message) => assert!(message.contains("response PNU mismatch")),
            other @ LookupError::Backend(_) => panic!("expected parse error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lookup_sends_foundation_workload_bearer_token() {
        let (base_url, requests) = spawn_foundation_platform_response_capture(
            REQUEST_PNU,
            "HTTP/1.1 200 OK",
            &foundation_platform_parcel_json(REQUEST_PNU, "factory"),
        );
        let auth = test_service_auth();
        let lookup =
            FoundationPlatformParcelInfoLookup::new(&base_url, Some(auth)).expect("valid base url");
        let pnu = Pnu::try_new(REQUEST_PNU).unwrap();

        lookup.lookup_by_pnu(&pnu).await.unwrap();

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
        expected_pnu: &str,
        status_line: &str,
        body: &str,
    ) -> String {
        let (base_url, _requests) =
            spawn_foundation_platform_response_capture(expected_pnu, status_line, body);
        base_url
    }

    fn test_service_auth() -> FoundationServiceAuth {
        FoundationServiceAuth::from_bearer_token("zitadel-workload-token-32-valid")
            .expect("service auth")
    }

    fn spawn_foundation_platform_response_capture(
        expected_pnu: &str,
        status_line: &str,
        body: &str,
    ) -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let expected_path = format!("GET /catalog/v1/parcels/by-pnu/{expected_pnu} ");
        let status_line = status_line.to_owned();
        let body = body.to_owned();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let read = stream.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(
                request.starts_with(&expected_path),
                "request path mismatch: {request}"
            );
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

    fn foundation_platform_parcel_json(pnu: &str, kind: &str) -> String {
        format!(
            r#"{{
                "id":"018f2ec8-7f3a-79db-8f7f-3d65f4277f00",
                "complex_id":"018f2ec8-7f3a-79db-8f7f-3d65f4277f01",
                "pnu":"{pnu}",
                "kind":"{kind}",
                "area_m2":1200,
                "version":3,
                "updated_at":"2026-05-28T00:00:00Z"
            }}"#
        )
    }
}
