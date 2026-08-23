//! Typed Foundation Platform Catalog v1 transport primitives.

use circuit_breaker::{execute, Breaker, BreakerError, Policy};
use serde::Deserialize;
use thiserror::Error;

use crate::{
    parse_foundation_endpoint_url, FoundationEndpointUrlError, FoundationServiceAuth,
    FoundationServiceAuthError,
};

const PARCEL_BY_PNU_PATH_PREFIX: &str = "catalog/v1/parcels/by-pnu/";
/// Keyed on the lakehouse id, not on `catalog.industrial_complex.id`.
///
/// The two are different identities and neither is computable from the other. What a Gongzzang
/// caller holds is the lakehouse one: it is what the `complex` vector tile publishes as its feature
/// id and what every Gold artifact key is derived from.
const COMPLEX_BY_LAKEHOUSE_ID_PATH_PREFIX: &str = "catalog/v1/complexes/by-lakehouse-id/";
/// Paged industrial-complex collection.
const COMPLEXES_PATH: &str = "catalog/v1/complexes";

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

/// Where the pre-rendered Gold profile for one complex lives, and what it must hash to.
///
/// Root ADR-0006 serves catalog point-lookups from pre-rendered JSON on R2/CDN, so the description
/// a panel draws is split in two: the columns the canonical table carries come back on this
/// response, and the rest are in an object this pointer addresses. Only the fields a consumer needs
/// to *fetch and trust* that object are deserialized — the pointer's lineage columns
/// (`source_record_id`, `iceberg_snapshot_id`, …) are Catalog's own bookkeeping.
///
/// `profile_checksum_sha256` is not decoration. A consumer that fetches the object is reading bytes
/// from a CDN rather than from the API it authenticated to, and the checksum is what lets it tell
/// the published artifact from a stale or substituted one.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CatalogIndustrialComplexGoldPointer {
    /// Active Gold artifact version, which is also the profile object's `artifact_id`.
    pub current_version: String,
    /// Provider-neutral object key for the profile artifact.
    pub profile_object_key: String,
    /// Address template the object key is substituted into.
    ///
    /// The pointer carries a template rather than a materialized URL so that moving the serving
    /// host does not require rewriting immutable objects (root ADR-0037).
    pub profile_url_template: String,
    /// SHA-256 the fetched profile bytes must hash to.
    pub profile_checksum_sha256: String,
}

impl CatalogIndustrialComplexGoldPointer {
    /// Substitution token the address template carries.
    const OBJECT_KEY_TOKEN: &'static str = "{object_key}";

    /// Resolves the fetchable profile URL, or `None` when the template has no substitution point.
    ///
    /// A template that does not name `{object_key}` addresses something other than this pointer's
    /// object, and returning its literal text would hand a consumer a URL that silently fetches the
    /// wrong artifact — or the same artifact for every complex.
    #[must_use]
    pub fn profile_url(&self) -> Option<String> {
        self.profile_url_template
            .contains(Self::OBJECT_KEY_TOKEN)
            .then(|| {
                self.profile_url_template
                    .replace(Self::OBJECT_KEY_TOKEN, self.profile_object_key.as_str())
            })
    }
}

/// Foundation Catalog industrial-complex wire response.
///
/// Carries the description Gongzzang shows, not the whole provider contract: `version`,
/// `updated_at` and `archived_at` are Catalog's own bookkeeping, and a consumer that deserialized
/// them would be claiming a use it does not have.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CatalogIndustrialComplexResponse {
    /// Lakehouse identity of the complex, echoed back by the provider.
    pub lakehouse_complex_id: Option<String>,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Industrial complex kind wire value.
    pub kind: String,
    /// Development lifecycle wire value. `unknown` and absent mean different things.
    #[serde(default)]
    pub status: Option<String>,
    /// Address the source stated, in the source's own wording.
    #[serde(default)]
    pub address_text: Option<String>,
    /// Official complex area in square meters.
    pub area_m2: u64,
    /// Organization that manages the complex.
    #[serde(default)]
    pub management_agency_name: Option<String>,
    /// Organization that developed the complex.
    #[serde(default)]
    pub developer_name: Option<String>,
    /// Designation date as `YYYY-MM-DD`.
    #[serde(default)]
    pub designated_date: Option<String>,
    /// Site-works start date as `YYYY-MM-DD`.
    #[serde(default)]
    pub construction_start_date: Option<String>,
    /// Site-formation completion date as `YYYY-MM-DD`.
    #[serde(default)]
    pub completion_date: Option<String>,
    /// Site-formation progress as exact decimal text. `"0.00"` is a real answer.
    #[serde(default)]
    pub development_progress_percent: Option<String>,
    /// Lot sales/lease progress wire value.
    #[serde(default)]
    pub lot_sales_status: Option<String>,
    /// Business period exactly as the source wrote it.
    #[serde(default)]
    pub business_period_raw: Option<String>,
    /// Statute the designation was made under, verbatim.
    #[serde(default)]
    pub designation_basis_law_raw: Option<String>,
    /// Development method, verbatim.
    #[serde(default)]
    pub development_method_raw: Option<String>,
    /// Stated development purpose, verbatim.
    #[serde(default)]
    pub development_purpose_raw: Option<String>,
    /// Industry types the complex set out to attract, verbatim.
    #[serde(default)]
    pub invited_industries_raw: Option<String>,
    /// Where the rest of the description lives, when it has been published.
    ///
    /// Absent until an export has run and its pointer has been published. Every consumer therefore
    /// has to work without it, which is the state the whole catalog is in today.
    #[serde(default)]
    pub gold_pointer: Option<CatalogIndustrialComplexGoldPointer>,
}

/// Foundation Catalog paged industrial-complex collection response.
///
/// The envelope, not the array: a page without `total` cannot tell a screen how much of the
/// collection it is showing, and `has_next` alone cannot tell it how much is left.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CatalogIndustrialComplexListResponse {
    /// Complexes on this page, in the order the provider was asked for.
    pub complexes: Vec<CatalogIndustrialComplexResponse>,
    /// Complexes the filters match in total.
    pub total: u64,
    /// Zero-indexed page number the provider served.
    pub page: u32,
    /// Page size the provider served.
    pub size: u32,
    /// Whether a further page exists.
    pub has_next: bool,
}

/// Query parameters for the Foundation Catalog industrial-complex collection.
///
/// The provider's parameter *names* live here and nowhere else in Gongzzang. A route that spelled
/// them itself would be a second copy of somebody else's contract, and the copy is what goes stale.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogComplexListQuery {
    /// Substring of the complex name or official complex code.
    pub q: Option<String>,
    /// Two-digit province code.
    pub sido_code: Option<String>,
    /// Comma-separated development lifecycle filter.
    pub status: Option<String>,
    /// Zero-indexed page number.
    pub page: Option<u32>,
    /// Page size.
    pub size: Option<u32>,
    /// Row order.
    pub sort: Option<String>,
}

impl CatalogComplexListQuery {
    /// Renders the stated parameters as URL query pairs, omitting the absent ones.
    ///
    /// Absent rather than empty: `q=` and no `q` are the same request to the provider today, but
    /// sending a parameter the caller did not state asserts a filter they did not ask for.
    fn to_query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::with_capacity(6);
        if let Some(q) = &self.q {
            pairs.push(("q", q.clone()));
        }
        if let Some(sido_code) = &self.sido_code {
            pairs.push(("sido_code", sido_code.clone()));
        }
        if let Some(status) = &self.status {
            pairs.push(("status", status.clone()));
        }
        if let Some(page) = self.page {
            pairs.push(("page", page.to_string()));
        }
        if let Some(size) = self.size {
            pairs.push(("size", size.to_string()));
        }
        if let Some(sort) = &self.sort {
            pairs.push(("sort", sort.clone()));
        }
        pairs
    }
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
            &[],
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
            &[],
        )
        .await
    }

    /// Sends one complex-by-lakehouse-id request through the published Catalog v1 path.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures, invalid workload credentials, and retriable status.
    pub async fn get_complex_by_lakehouse_id_response(
        &self,
        lakehouse_complex_id: &str,
    ) -> Result<reqwest::Response, FoundationCatalogClientRequestError> {
        self.execute_get(
            "foundation_platform.catalog.get_complex_by_lakehouse_id",
            &format!("{COMPLEX_BY_LAKEHOUSE_ID_PATH_PREFIX}{lakehouse_complex_id}"),
            &[],
        )
        .await
    }

    /// Sends one industrial-complex collection request through the published Catalog v1 path.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures, invalid workload credentials, and retriable status.
    pub async fn list_complexes_response(
        &self,
        query: &CatalogComplexListQuery,
    ) -> Result<reqwest::Response, FoundationCatalogClientRequestError> {
        let pairs = query.to_query_pairs();
        self.execute_get(
            "foundation_platform.catalog.list_complexes",
            COMPLEXES_PATH,
            &pairs,
        )
        .await
    }

    async fn execute_get(
        &self,
        operation_name: &'static str,
        relative_path: &str,
        query: &[(&'static str, String)],
    ) -> Result<reqwest::Response, FoundationCatalogClientRequestError> {
        execute(&self.breaker, &self.policy, operation_name, || {
            self.send_get_attempt(relative_path, query)
        })
        .await
        .map_err(|source| FoundationCatalogClientRequestError::Circuit { source })
    }

    async fn send_get_attempt(
        &self,
        relative_path: &str,
        query: &[(&'static str, String)],
    ) -> Result<reqwest::Response, FoundationCatalogHttpError> {
        let mut url = self.base_url.join(relative_path).map_err(|source| {
            FoundationCatalogHttpError::BuildUrl {
                detail: source.to_string(),
            }
        })?;
        if !query.is_empty() {
            // `query_pairs_mut` percent-encodes, so a Korean `q` reaches the provider intact
            // instead of arriving as whatever the caller happened to paste into a path.
            let mut serializer = url.query_pairs_mut();
            for (name, value) in query {
                serializer.append_pair(name, value);
            }
        }
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

#[cfg(test)]
mod tests {
    use super::CatalogIndustrialComplexGoldPointer;

    fn pointer(template: &str) -> CatalogIndustrialComplexGoldPointer {
        CatalogIndustrialComplexGoldPointer {
            current_version: "018f0000-0000-7000-8000-000000000001".to_owned(),
            profile_object_key:
                "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json"
                    .to_owned(),
            profile_url_template: template.to_owned(),
            profile_checksum_sha256: "a".repeat(64),
        }
    }

    #[test]
    fn resolves_the_object_key_into_the_address_template() {
        assert_eq!(
            pointer("https://lakehouse.example.com/{object_key}").profile_url(),
            Some(
                "https://lakehouse.example.com/gold/industrial-complex/profiles/\
                 018f0000-0000-7000-8000-000000000001.json"
                    .to_owned()
            )
        );
    }

    /// A template with no substitution point would resolve to the same address for every complex.
    /// Returning its literal text would hand a consumer a URL that fetches the wrong artifact
    /// while looking entirely well-formed, so there is no URL rather than a plausible wrong one.
    #[test]
    fn a_template_without_a_substitution_point_resolves_to_no_url() {
        assert_eq!(
            pointer("https://lakehouse.example.com/profile.json").profile_url(),
            None
        );
        assert_eq!(pointer("").profile_url(), None);
    }
}
