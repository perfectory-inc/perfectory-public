//! Typed Foundation Platform Catalog v1 transport primitives.

use circuit_breaker::{execute, Breaker, BreakerError, Policy};
use serde::Deserialize;
use thiserror::Error;

use crate::{
    parse_foundation_endpoint_url, FoundationEndpointUrlError, FoundationServiceAuth,
    FoundationServiceAuthError,
};

const PARCEL_BY_PNU_PATH_PREFIX: &str = "catalog/v1/parcels/by-pnu/";

/// Foundation Catalog parcel wire response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CatalogParcelResponse {
    /// Standard 19-digit parcel identity.
    pub pnu: String,
    /// Foundation-owned parcel kind wire value.
    pub kind: String,
}

/// Foundation Catalog building wire response.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CatalogBuildingResponse {
    /// Stable Foundation building identifier.
    pub id: String,
    /// Stable Foundation parcel identifier.
    pub parcel_id: String,
    /// Source building purpose code.
    pub purpose_code: String,
    /// Source building structure code.
    pub structure_code: String,
    /// Official total floor area in square meters.
    pub floor_area_m2: f64,
    /// Above-ground floor count.
    pub stories: i16,
    /// Below-ground floor count.
    pub below_ground_floors: i16,
    /// Whether the source reports a rooftop floor or structure.
    pub has_rooftop: bool,
    /// Optional rooftop area in square meters.
    #[serde(default)]
    pub rooftop_area_m2: Option<f64>,
    /// Source rooftop usage description.
    #[serde(default)]
    pub rooftop_usage: String,
    /// Source construction year.
    pub built_year: i32,
    /// Foundation Catalog update timestamp.
    pub updated_at: String,
}

/// Shared HTTP transport for Foundation Catalog v1 reads.
pub struct FoundationCatalogClient {
    base_url: reqwest::Url,
    /// Every send using this client is owned by `execute_get` below.
    #[allow(clippy::disallowed_types)]
    client: reqwest::Client,
    auth: Option<FoundationServiceAuth>,
    breaker: Breaker,
    policy: Policy,
}

impl FoundationCatalogClient {
    /// Creates a Catalog client from one validated Foundation endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint or HTTP client configuration is invalid.
    pub fn new(
        base_url: &str,
        auth: Option<FoundationServiceAuth>,
    ) -> Result<Self, FoundationCatalogClientConfigError> {
        let base_url = parse_foundation_endpoint_url(base_url)?;
        #[allow(clippy::disallowed_types)]
        let client = reqwest::Client::builder()
            .build()
            .map_err(|source| FoundationCatalogClientConfigError::HttpClient { source })?;
        Ok(Self {
            base_url,
            client,
            auth,
            breaker: Breaker::new(),
            policy: Policy::foundation_platform_default(),
        })
    }

    /// Sends one parcel-by-PNU request through the published Catalog v1 path.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures, invalid workload credentials, and retriable status.
    pub async fn get_parcel_by_pnu_response(
        &self,
        pnu: &str,
    ) -> Result<reqwest::Response, FoundationCatalogClientRequestError> {
        self.execute_get(
            "foundation_platform.catalog.get_parcel_by_pnu",
            &format!("{PARCEL_BY_PNU_PATH_PREFIX}{pnu}"),
        )
        .await
    }

    /// Sends one building-list-by-PNU request through the published Catalog v1 path.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures, invalid workload credentials, and retriable status.
    pub async fn list_buildings_by_pnu_response(
        &self,
        pnu: &str,
    ) -> Result<reqwest::Response, FoundationCatalogClientRequestError> {
        self.execute_get(
            "foundation_platform.catalog.list_parcel_buildings_by_pnu",
            &format!("{PARCEL_BY_PNU_PATH_PREFIX}{pnu}/buildings"),
        )
        .await
    }

    async fn execute_get(
        &self,
        operation_name: &'static str,
        relative_path: &str,
    ) -> Result<reqwest::Response, FoundationCatalogClientRequestError> {
        execute(&self.breaker, &self.policy, operation_name, || {
            self.send_get_attempt(relative_path)
        })
        .await
        .map_err(|source| FoundationCatalogClientRequestError::Circuit { source })
    }

    async fn send_get_attempt(
        &self,
        relative_path: &str,
    ) -> Result<reqwest::Response, FoundationCatalogHttpError> {
        let url = self.base_url.join(relative_path).map_err(|source| {
            FoundationCatalogHttpError::BuildUrl {
                detail: source.to_string(),
            }
        })?;
        let request = self.client.get(url);
        let request = if let Some(auth) = &self.auth {
            auth.apply(request)
                .map_err(|source| FoundationCatalogHttpError::ServiceAuth { source })?
        } else {
            request
        };
        let response = request
            .send()
            .await
            .map_err(|source| FoundationCatalogHttpError::Request { source })?;
        let status = response.status();
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(FoundationCatalogHttpError::RetriableStatus { status });
        }
        Ok(response)
    }
}

/// A guarded Catalog request exhausted or was rejected by its circuit policy.
#[derive(Debug, Error)]
pub enum FoundationCatalogClientRequestError {
    /// The shared circuit-breaker policy rejected or exhausted the request.
    #[error("Foundation Platform Catalog guarded request failed: {source}")]
    Circuit {
        /// Circuit-breaker failure with the final redacted HTTP reason.
        #[source]
        source: BreakerError<FoundationCatalogHttpError>,
    },
}

/// Invalid Catalog client configuration.
#[derive(Debug, Error)]
pub enum FoundationCatalogClientConfigError {
    /// Foundation endpoint validation failed.
    #[error(transparent)]
    FoundationEndpoint(#[from] FoundationEndpointUrlError),
    /// HTTP client construction failed.
    #[error("build Foundation Platform Catalog HTTP client: {source}")]
    HttpClient {
        /// Underlying HTTP client construction error.
        source: reqwest::Error,
    },
}

/// One Foundation Catalog HTTP attempt failed before domain translation.
#[derive(Debug, Error)]
pub enum FoundationCatalogHttpError {
    /// The endpoint URL could not be joined with the contract path.
    #[error("build Foundation Platform Catalog URL: {detail}")]
    BuildUrl {
        /// URL parser detail without credentials.
        detail: String,
    },
    /// The HTTP request failed.
    #[error("Foundation Platform Catalog request failed: {source}")]
    Request {
        /// Underlying request error.
        #[source]
        source: reqwest::Error,
    },
    /// Foundation returned a status eligible for retry.
    #[error("Foundation Platform Catalog returned retriable status {status}")]
    RetriableStatus {
        /// Retriable HTTP status.
        status: reqwest::StatusCode,
    },
    /// Workload authentication could not be attached.
    #[error("Foundation Platform Catalog workload authentication failed: {source}")]
    ServiceAuth {
        /// Workload token failure.
        #[source]
        source: FoundationServiceAuthError,
    },
}
