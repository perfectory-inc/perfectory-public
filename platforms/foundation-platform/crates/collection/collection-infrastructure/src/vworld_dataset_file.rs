//! HTTP client and parser for `VWorld` provider dataset files.

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use collection_domain::CollectionError;
use futures_util::{stream::BoxStream, StreamExt as _};
use reqwest::header::{HeaderMap, CONTENT_DISPOSITION, CONTENT_TYPE, SET_COOKIE};

use outbound_http_infrastructure::{
    classify_response, execute_retryable, execute_single, execute_streaming_handshake,
    redact_transport_error, shared_http_client, AttemptError, ResilienceAudit, ResilienceCtx,
    RetryDecision, VWORLD_FILE,
};

/// Provider label for the dataset file/inventory client (audit + error messages).
const FILE_PROVIDER: &str = "VWorld dataset file";
/// Provider label for the dataset login client (audit + error messages).
const LOGIN_PROVIDER: &str = "VWorld dataset login";

const DATASET_DETAIL_PATH: &str = "dtmk/dtmk_ntads_s002.do";
const SINGLE_FILE_DOWNLOAD_PATH: &str = "dtmk/downloadResourceFile.do";
const SELECTION_DOWNLOAD_PAGE_PATH: &str = "dtmk/downloadDtnaResourceFile.do";
const LOGIN_PATH: &str = "v4po_usrlogin_a004.do";
const MAX_DATASET_FILE_PAGE_COUNT: u64 = 10_000;
const LARGE_FILE_THRESHOLD_KIB: u64 = 512_000;
const SESSION_BEARING_LOGIN_RESULTS: &[&str] = &["success", "expirePw"];

/// Configuration for a `VWorld` provider dataset-file client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetFileConfig {
    /// Base URI for `VWorld` or a compatible test server.
    pub base_uri: String,
    /// User-Agent header sent to the provider.
    pub user_agent: String,
    /// Provider file list page size. The live site accepts 100.
    pub page_size: u64,
    /// Optional provider session cookie header supplied by the operator.
    pub cookie_header: Option<String>,
}

/// Configuration for a `VWorld` provider dataset login client.
#[derive(Clone, Eq, PartialEq)]
pub struct VWorldDatasetLoginConfig {
    /// Base URI for `VWorld` or a compatible test server.
    pub base_uri: String,
    /// User-Agent header sent to the provider.
    pub user_agent: String,
    /// Provider account username.
    pub username: String,
    /// Provider account password.
    pub password: String,
}

/// Selector that identifies one provider dataset detail page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetFileInventorySelector {
    /// `VWorld` service code, for example `MK` or `NA`.
    pub svc_cde: String,
    /// `VWorld` dataset id.
    pub ds_id: String,
}

/// Download path kind implied by the official `VWorld` page script.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VWorldDatasetFileKind {
    /// Single file path: `/dtmk/downloadResourceFile.do?ds_id={ds_id}&fileNo={file_no}`.
    SingleResourceFile,
    /// Large file selection flow exposed by the provider page. Live `VWorld` opens a
    /// RAON/KUpload desktop-agent page for this flow, not a server-side downloadable ZIP.
    SelectionArchive,
}

/// One downloadable provider file listed on a `VWorld` dataset detail page.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VWorldDatasetFileInventoryItem {
    /// `VWorld` service code.
    pub svc_cde: String,
    /// `VWorld` dataset id.
    pub ds_id: String,
    /// Provider dataset id passed to the official download script.
    pub download_ds_id: String,
    /// Provider file sequence passed to the official download script.
    pub file_no: String,
    /// Provider file name shown on the detail page.
    pub provider_file_name: String,
    /// Provider file format label, for example `SHP`.
    pub file_format: String,
    /// Human label from the page's MB column.
    pub size_mb_label: String,
    /// Provider size argument passed to `listFnc.download`, in KiB.
    pub size_kib: u64,
    /// Provider file kind label shown by the dataset page.
    pub provider_file_kind: String,
    /// Provider base year-month label.
    pub base_ym: String,
    /// Provider updated-at label.
    pub updated_at: String,
    /// Official download path kind.
    pub download_kind: VWorldDatasetFileKind,
}

/// Request for one official `VWorld` provider file download.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetFileDownloadRequest {
    /// Provider dataset id passed to the official download script.
    pub download_ds_id: String,
    /// Provider file sequence.
    pub file_no: String,
    /// Official download path kind.
    pub download_kind: VWorldDatasetFileKind,
}

/// Raw file downloaded from `VWorld`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetFile {
    /// Raw provider file bytes, suitable for Bronze storage.
    pub raw_payload: Vec<u8>,
    /// Response content type.
    pub content_type: String,
    /// Provider file name from the response content-disposition header.
    pub provider_file_name: String,
}

/// Streaming file response opened from a `VWorld` provider dataset.
pub struct VWorldDatasetFileStream {
    /// Response content type.
    pub content_type: String,
    /// Provider file name from the response content-disposition header.
    pub provider_file_name: String,
    /// Exact response byte length when the provider declares one.
    pub expected_size_bytes: Option<u64>,
    body: BoxStream<'static, Result<Bytes, CollectionError>>,
}

impl VWorldDatasetFileStream {
    /// Creates a streaming file wrapper from validated provider metadata and a byte stream.
    #[must_use]
    pub fn from_body_stream(
        content_type: String,
        provider_file_name: String,
        expected_size_bytes: Option<u64>,
        body: BoxStream<'static, Result<Bytes, CollectionError>>,
    ) -> Self {
        Self {
            content_type,
            provider_file_name,
            expected_size_bytes,
            body,
        }
    }

    /// Reads the next provider byte chunk without buffering the full file.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the provider response stream fails.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, CollectionError> {
        self.body.next().await.transpose()
    }

    /// Consumes this file and returns its provider byte stream.
    #[must_use]
    pub fn into_body_stream(self) -> BoxStream<'static, Result<Bytes, CollectionError>> {
        self.body
    }
}

/// One parsed provider file inventory page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetFileInventoryPage {
    /// Current provider page index.
    pub current_page_index: u64,
    /// Total provider page count.
    pub total_page_count: u64,
    /// Total provider file count displayed by the page.
    pub total_file_count: u64,
    /// Files listed on this page.
    pub files: Vec<VWorldDatasetFileInventoryItem>,
}

/// `reqwest` backed client for `VWorld` provider dataset-file inventories.
#[derive(Clone, Debug)]
pub struct VWorldDatasetFileClient {
    base_uri: reqwest::Url,
    user_agent: String,
    page_size: u64,
    cookie_header: Option<String>,
    http: reqwest::Client,
    audit: ResilienceAudit,
}

/// `reqwest` backed client for acquiring a `VWorld` provider login session.
#[derive(Clone)]
pub struct VWorldDatasetLoginClient {
    base_uri: reqwest::Url,
    user_agent: String,
    username: String,
    password: String,
    http: reqwest::Client,
    audit: ResilienceAudit,
}

impl VWorldDatasetLoginClient {
    /// Creates a login client from explicit provider credentials.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when base URI, User-Agent, username, or password are invalid.
    pub fn new(config: &VWorldDatasetLoginConfig) -> Result<Self, CollectionError> {
        let base_uri = parse_provider_base_uri("VWorld dataset login", &config.base_uri)?;
        let user_agent = validate_required_secretless_field(
            "VWorld dataset login user_agent",
            &config.user_agent,
        )?;
        let username =
            validate_required_secretless_field("VWorld dataset login username", &config.username)?;
        let password =
            validate_required_secretless_field("VWorld dataset login password", &config.password)?;
        let http = shared_http_client(LOGIN_PROVIDER, &VWORLD_FILE)
            .map_err(crate::outbound_http_error::into_collection_error)?;

        Ok(Self {
            base_uri,
            user_agent,
            username,
            password,
            http,
            audit: ResilienceAudit::new(LOGIN_PROVIDER),
        })
    }

    /// Logs in and returns a provider `Cookie` header suitable for dataset file downloads.
    ///
    /// Executed through [`execute_single`]: the login POST is non-idempotent, so it is never
    /// retried (and the breaker is not applied — client/job-local scope, spec §8).
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the provider rejects login, omits session cookies, or the
    /// response cannot be parsed.
    pub async fn fetch_cookie_header(&self) -> Result<String, CollectionError> {
        let ctx = ResilienceCtx {
            breaker: None,
            policy: &VWORLD_FILE,
            audit: &self.audit,
        };
        execute_single(&ctx, || self.fetch_cookie_header_once())
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_cookie_header_once(&self) -> Result<String, AttemptError> {
        let login_url = self.base_uri.join(LOGIN_PATH).map_err(|error| {
            outbound_http_infrastructure::OutboundHttpError::new(format!(
                "invalid VWorld dataset login URL: {error}"
            ))
        })?;
        let referer_url = self.base_uri.join("anyId/login.do").map_err(|error| {
            outbound_http_infrastructure::OutboundHttpError::new(format!(
                "invalid VWorld dataset login referer: {error}"
            ))
        })?;
        let form = [
            ("usrIdeE", BASE64_STANDARD.encode(self.username.as_bytes())),
            ("usrPwdE", BASE64_STANDARD.encode(self.password.as_bytes())),
            ("nextUrl", "/v4po_main.do".to_owned()),
        ];
        let response = self
            .http
            .post(login_url)
            .header("user-agent", &self.user_agent)
            .header("x-requested-with", "XMLHttpRequest")
            .header("origin", self.base_uri.origin().ascii_serialization())
            .header("referer", referer_url.as_str())
            .form(&form)
            .send()
            .await
            // Transient-class transport failure; execute_single reports it without retrying
            // as "VWorld dataset login request failed: {error}" (legacy message preserved).
            .map_err(|error| AttemptError::Retryable {
                message: redact_transport_error(&error),
                retry_after: None,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(AttemptError::Fatal(
                outbound_http_infrastructure::OutboundHttpError::new(format!(
                    "VWorld dataset login returned HTTP {status}"
                )),
            ));
        }
        let headers = response.headers().clone();
        let login_result = response
            .json::<VWorldDatasetLoginResponse>()
            .await
            .map_err(|error| {
                outbound_http_infrastructure::OutboundHttpError::new(format!(
                    "VWorld dataset login response parse failed: {error}"
                ))
            })?;
        if !login_result
            .result_map
            .as_ref()
            .is_some_and(|result| SESSION_BEARING_LOGIN_RESULTS.contains(&result.result.as_str()))
        {
            return Err(AttemptError::Fatal(
                outbound_http_infrastructure::OutboundHttpError::new(
                    vworld_dataset_login_failure_message(
                        &login_result,
                        &self.username,
                        &self.password,
                    ),
                ),
            ));
        }

        cookie_header_from_set_cookie_headers(&headers)
            .map_err(crate::outbound_http_error::into_fatal_attempt)
    }
}

impl VWorldDatasetFileClient {
    /// Creates a client from explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid, the User-Agent is empty, or the
    /// page size is outside the provider's expected range.
    pub fn new(config: &VWorldDatasetFileConfig) -> Result<Self, CollectionError> {
        let base_uri = parse_provider_base_uri("VWorld dataset", &config.base_uri)?;
        let user_agent =
            validate_required_secretless_field("VWorld dataset user_agent", &config.user_agent)?;
        if config.page_size == 0 || config.page_size > 100 {
            return Err(CollectionError::Infrastructure(
                "VWorld dataset page_size must be between 1 and 100".to_owned(),
            ));
        }
        let cookie_header = config
            .cookie_header
            .as_deref()
            .map(validate_cookie_header)
            .transpose()?;
        let http = shared_http_client(FILE_PROVIDER, &VWORLD_FILE)
            .map_err(crate::outbound_http_error::into_collection_error)?;

        Ok(Self {
            base_uri,
            user_agent,
            page_size: config.page_size,
            cookie_header,
            http,
            audit: ResilienceAudit::new(FILE_PROVIDER),
        })
    }

    /// Fetches all file inventory rows for one provider dataset.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when a page request fails, a page cannot be parsed, or the parsed
    /// file count does not match the provider page summary.
    pub async fn fetch_dataset_file_inventory(
        &self,
        selector: &VWorldDatasetFileInventorySelector,
    ) -> Result<Vec<VWorldDatasetFileInventoryItem>, CollectionError> {
        validate_selector(selector)?;
        let first_page = self.fetch_inventory_page(selector, 1).await?;
        let first_page = parse_vworld_dataset_file_inventory_page(selector, &first_page)?;
        if first_page.total_page_count > MAX_DATASET_FILE_PAGE_COUNT {
            return Err(CollectionError::Infrastructure(format!(
                "VWorld dataset {} page count exceeded safety cap {}",
                selector.ds_id, MAX_DATASET_FILE_PAGE_COUNT
            )));
        }

        let expected_file_count = first_page.total_file_count;
        let total_page_count = first_page.total_page_count;
        let mut files = first_page.files;
        for page_index in 2..=total_page_count {
            let page = self.fetch_inventory_page(selector, page_index).await?;
            let parsed = parse_vworld_dataset_file_inventory_page(selector, &page)?;
            if parsed.total_file_count != expected_file_count {
                return Err(CollectionError::Infrastructure(format!(
                    "VWorld dataset {} total file count changed while scanning pages",
                    selector.ds_id
                )));
            }
            files.extend(parsed.files);
        }

        if files.len() as u64 != expected_file_count {
            return Err(CollectionError::Infrastructure(format!(
                "VWorld dataset {} expected {} files, parsed {}",
                selector.ds_id,
                expected_file_count,
                files.len()
            )));
        }
        Ok(files)
    }

    /// Downloads one official provider file.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the request identity is invalid, the provider request fails, or
    /// the response is not a downloadable provider file.
    pub async fn fetch_file(
        &self,
        request: &VWorldDatasetFileDownloadRequest,
    ) -> Result<VWorldDatasetFile, CollectionError> {
        let response = self.send_file_request(request).await?;
        let headers = response.headers().clone();
        let content_type = header_value(&headers, CONTENT_TYPE.as_str())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let raw_payload = response
            .bytes()
            .await
            .map_err(|error| {
                CollectionError::Infrastructure(format!(
                    "VWorld dataset file response body read failed: {error}"
                ))
            })?
            .to_vec();
        if raw_payload.is_empty() {
            return Err(self.empty_file_response_error(request).await);
        }
        if is_html_response(&content_type, &raw_payload) {
            return Err(CollectionError::Infrastructure(
                "VWorld dataset file request returned HTML instead of a provider file".to_owned(),
            ));
        }
        let provider_file_name = provider_file_name_from_headers(&headers)?;

        Ok(VWorldDatasetFile {
            raw_payload,
            content_type,
            provider_file_name,
        })
    }

    /// Opens one official provider file as a bounded-memory byte stream.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the request identity is invalid, the provider request fails, or
    /// the response is not a downloadable provider file.
    pub async fn open_file_stream(
        &self,
        request: &VWorldDatasetFileDownloadRequest,
    ) -> Result<VWorldDatasetFileStream, CollectionError> {
        self.open_file_stream_with_optional_provider_file_name_fallback(request, None)
            .await
    }

    /// Opens one official provider file as a bounded-memory byte stream, using the already parsed
    /// inventory file name when the provider download response omits `Content-Disposition`.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the request identity is invalid, the provider request fails, the
    /// response is not a downloadable provider file, or the fallback file name is invalid.
    pub async fn open_file_stream_with_provider_file_name_fallback(
        &self,
        request: &VWorldDatasetFileDownloadRequest,
        provider_file_name_fallback: &str,
    ) -> Result<VWorldDatasetFileStream, CollectionError> {
        self.open_file_stream_with_optional_provider_file_name_fallback(
            request,
            Some(provider_file_name_fallback),
        )
        .await
    }

    async fn open_file_stream_with_optional_provider_file_name_fallback(
        &self,
        request: &VWorldDatasetFileDownloadRequest,
        provider_file_name_fallback: Option<&str>,
    ) -> Result<VWorldDatasetFileStream, CollectionError> {
        let response = self.send_file_request(request).await?;
        let expected_size_bytes = response.content_length();
        if expected_size_bytes == Some(0) {
            return Err(self.empty_file_response_error(request).await);
        }
        let headers = response.headers().clone();
        let content_type = header_value(&headers, CONTENT_TYPE.as_str())
            .unwrap_or("application/octet-stream")
            .to_owned();
        if is_html_content_type(&content_type) {
            return Err(CollectionError::Infrastructure(
                "VWorld dataset file request returned HTML instead of a provider file".to_owned(),
            ));
        }
        let provider_file_name =
            provider_file_name_from_headers_or_fallback(&headers, provider_file_name_fallback)?;
        let body = response
            .bytes_stream()
            .map(|chunk| {
                chunk.map_err(|error| {
                    CollectionError::Infrastructure(format!(
                        "VWorld dataset file response body stream failed: {error}"
                    ))
                })
            })
            .boxed();

        Ok(VWorldDatasetFileStream {
            content_type,
            provider_file_name,
            expected_size_bytes,
            body,
        })
    }

    /// Sends one download request through [`execute_streaming_handshake`]: transient failures
    /// are retried up to the status-validated `Response`; callers open the body (buffered or
    /// streamed) after this returns, with no retry once the stream starts. The breaker is not
    /// applied — ingest creates a client per job, so its state would be cosmetic (spec §8).
    async fn send_file_request(
        &self,
        request: &VWorldDatasetFileDownloadRequest,
    ) -> Result<reqwest::Response, CollectionError> {
        validate_download_request(request)?;
        if request.download_kind == VWorldDatasetFileKind::SelectionArchive {
            return Err(selection_archive_requires_raon_agent_error(request));
        }
        let path = SINGLE_FILE_DOWNLOAD_PATH;
        let mut url = self.base_uri.join(path).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid VWorld dataset download URL: {error}"))
        })?;
        url.query_pairs_mut()
            .append_pair("ds_id", request.download_ds_id.as_str())
            .append_pair("fileNo", request.file_no.as_str());
        let ctx = ResilienceCtx {
            breaker: None,
            policy: &VWORLD_FILE,
            audit: &self.audit,
        };
        execute_streaming_handshake(&ctx, || self.send_file_request_once(&url))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn empty_file_response_error(
        &self,
        request: &VWorldDatasetFileDownloadRequest,
    ) -> CollectionError {
        match self
            .selection_download_page_requires_raon_agent(request)
            .await
        {
            Ok(true) => selection_archive_requires_raon_agent_error(request),
            Ok(false) => CollectionError::Infrastructure(
                "VWorld dataset file response body was empty".to_owned(),
            ),
            Err(error) => CollectionError::Infrastructure(format!(
                "VWorld dataset file response body was empty; RAON fallback probe failed: {error}"
            )),
        }
    }

    async fn selection_download_page_requires_raon_agent(
        &self,
        request: &VWorldDatasetFileDownloadRequest,
    ) -> Result<bool, CollectionError> {
        validate_download_request(request)?;
        let mut url = self
            .base_uri
            .join(SELECTION_DOWNLOAD_PAGE_PATH)
            .map_err(|error| {
                CollectionError::Infrastructure(format!(
                    "invalid VWorld dataset selection download URL: {error}"
                ))
            })?;
        let ds_file_sq = format!("{}{}", request.download_ds_id, request.file_no);
        url.query_pairs_mut()
            .append_pair("ds_file_sq", ds_file_sq.as_str());
        let ctx = ResilienceCtx {
            breaker: None,
            policy: &VWORLD_FILE,
            audit: &self.audit,
        };
        let html = execute_retryable(&ctx, || self.fetch_selection_download_page_once(&url))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)?;
        Ok(selection_download_page_mentions_raon_agent(&html, request))
    }

    async fn send_file_request_once(
        &self,
        url: &reqwest::Url,
    ) -> Result<reqwest::Response, AttemptError> {
        let mut request_builder = self
            .http
            .get(url.clone())
            .header("user-agent", &self.user_agent);
        if let Some(cookie_header) = &self.cookie_header {
            request_builder = request_builder.header("cookie", cookie_header);
        }
        let response = request_builder
            .send()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: redact_transport_error(&error),
                retry_after: None,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(match classify_response(status, response.headers()) {
                RetryDecision::Retryable { retry_after } => AttemptError::Retryable {
                    message: format!("HTTP {status}"),
                    retry_after,
                },
                RetryDecision::NotRetryable => {
                    AttemptError::Fatal(outbound_http_infrastructure::OutboundHttpError::new(
                        format!("VWorld dataset file request returned HTTP {status}"),
                    ))
                }
            });
        }
        Ok(response)
    }

    async fn fetch_selection_download_page_once(
        &self,
        url: &reqwest::Url,
    ) -> Result<String, AttemptError> {
        let mut request_builder = self
            .http
            .get(url.clone())
            .header("user-agent", &self.user_agent);
        if let Some(cookie_header) = &self.cookie_header {
            request_builder = request_builder.header("cookie", cookie_header);
        }
        let response = request_builder
            .send()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: redact_transport_error(&error),
                retry_after: None,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(match classify_response(status, response.headers()) {
                RetryDecision::Retryable { retry_after } => AttemptError::Retryable {
                    message: format!("HTTP {status}"),
                    retry_after,
                },
                RetryDecision::NotRetryable => {
                    AttemptError::Fatal(outbound_http_infrastructure::OutboundHttpError::new(
                        format!("VWorld dataset selection download request returned HTTP {status}"),
                    ))
                }
            });
        }
        response
            .text()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: format!(
                    "selection download response body read failed: {}",
                    redact_transport_error(&error)
                ),
                retry_after: None,
            })
    }

    /// Fetches one inventory detail page through [`execute_retryable`] (idempotent GET;
    /// spec §3.2). No circuit breaker — client/job-local scope (spec §8).
    async fn fetch_inventory_page(
        &self,
        selector: &VWorldDatasetFileInventorySelector,
        page_index: u64,
    ) -> Result<String, CollectionError> {
        let mut url = self.base_uri.join(DATASET_DETAIL_PATH).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid VWorld dataset detail URL: {error}"))
        })?;
        url.query_pairs_mut()
            .append_pair("svcCde", selector.svc_cde.as_str())
            .append_pair("dsId", selector.ds_id.as_str())
            .append_pair("datPageIndex", &page_index.to_string())
            .append_pair("datPageSize", &self.page_size.to_string());
        let ctx = ResilienceCtx {
            breaker: None,
            policy: &VWORLD_FILE,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || self.fetch_inventory_page_once(&url))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_inventory_page_once(&self, url: &reqwest::Url) -> Result<String, AttemptError> {
        let mut request_builder = self
            .http
            .get(url.clone())
            .header("user-agent", &self.user_agent);
        if let Some(cookie_header) = &self.cookie_header {
            request_builder = request_builder.header("cookie", cookie_header);
        }
        let response = request_builder
            .send()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: redact_transport_error(&error),
                retry_after: None,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(match classify_response(status, response.headers()) {
                RetryDecision::Retryable { retry_after } => AttemptError::Retryable {
                    message: format!("HTTP {status}"),
                    retry_after,
                },
                RetryDecision::NotRetryable => {
                    AttemptError::Fatal(outbound_http_infrastructure::OutboundHttpError::new(
                        format!("VWorld dataset inventory request returned HTTP {status}"),
                    ))
                }
            });
        }
        response
            .text()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: format!(
                    "inventory response body read failed: {}",
                    redact_transport_error(&error)
                ),
                retry_after: None,
            })
    }
}

#[derive(Debug, serde::Deserialize)]
struct VWorldDatasetLoginResponse {
    #[serde(rename = "resultMap")]
    result_map: Option<VWorldDatasetLoginResult>,
}

#[derive(Debug, serde::Deserialize)]
struct VWorldDatasetLoginResult {
    result: String,
    msg: Option<String>,
}

fn vworld_dataset_login_failure_message(
    login_result: &VWorldDatasetLoginResponse,
    username: &str,
    password: &str,
) -> String {
    let Some(result_map) = &login_result.result_map else {
        return "VWorld dataset login failed result=missing".to_owned();
    };
    let result = redact_provider_login_detail(&result_map.result, username, password);
    let mut message = format!("VWorld dataset login failed result={result}");
    if let Some(provider_message) = result_map.msg.as_deref() {
        let provider_message = clean_provider_login_message(provider_message);
        if !provider_message.is_empty() {
            let provider_message =
                redact_provider_login_detail(&provider_message, username, password);
            message.push_str(" message=");
            message.push_str(&provider_message);
        }
    }
    message
}

fn clean_provider_login_message(raw: &str) -> String {
    let without_breaks = raw
        .replace("<br/>", " ")
        .replace("<br />", " ")
        .replace("<br>", " ");
    clean_html_text(&without_breaks)
}

fn redact_provider_login_detail(value: &str, username: &str, password: &str) -> String {
    let mut redacted = value.to_owned();
    for sensitive in [username, password] {
        if !sensitive.is_empty() {
            redacted = redacted.replace(sensitive, "[redacted]");
        }
    }
    redacted
}

/// Parses one official `VWorld` dataset detail page.
///
/// # Errors
///
/// Returns `CollectionError` when provider file rows or page count fields are malformed.
pub fn parse_vworld_dataset_file_inventory_page(
    selector: &VWorldDatasetFileInventorySelector,
    html: &str,
) -> Result<VWorldDatasetFileInventoryPage, CollectionError> {
    validate_selector(selector)?;
    let (total_file_count, current_page_index, total_page_count) = parse_count_summary(html)?;
    let mut files = Vec::new();

    for segment in html.split("<li") {
        if !segment.contains("listFnc.download(") {
            continue;
        }
        let args = single_quoted_args_after(segment, "listFnc.download(").ok_or_else(|| {
            CollectionError::Infrastructure(
                "VWorld dataset file row omitted listFnc.download arguments".to_owned(),
            )
        })?;
        if args.len() != 3 {
            return Err(CollectionError::Infrastructure(format!(
                "VWorld dataset file row expected 3 download arguments, got {}",
                args.len()
            )));
        }
        if args[0].trim().is_empty() {
            return Err(CollectionError::Infrastructure(
                "VWorld dataset file row omitted download dataset id".to_owned(),
            ));
        }
        let size_kib = args[2].parse::<u64>().map_err(|error| {
            CollectionError::Infrastructure(format!(
                "VWorld dataset file row size argument must be u64: {error}"
            ))
        })?;
        let file_no = args[1].clone();
        let checkbox_file_no = value_after_marker(segment, "name=\"chkDs\" value=\"", "\"")
            .ok_or_else(|| {
                CollectionError::Infrastructure(
                    "VWorld dataset file row omitted chkDs value".to_owned(),
                )
            })?;
        if checkbox_file_no != file_no {
            return Err(CollectionError::Infrastructure(format!(
                "VWorld dataset file row chkDs value {checkbox_file_no} did not match fileNo {file_no}"
            )));
        }
        let em_values = em_text_values(segment);
        if em_values.len() < 4 {
            return Err(CollectionError::Infrastructure(
                "VWorld dataset file row omitted one or more metadata values".to_owned(),
            ));
        }

        files.push(VWorldDatasetFileInventoryItem {
            svc_cde: selector.svc_cde.clone(),
            ds_id: selector.ds_id.clone(),
            download_ds_id: args[0].clone(),
            file_no,
            provider_file_name: text_after_class_marker(segment, "class=\"tit", "</div>")
                .ok_or_else(|| {
                    CollectionError::Infrastructure(
                        "VWorld dataset file row omitted provider file name".to_owned(),
                    )
                })?,
            file_format: first_span_text_after(segment, "class=\"format").ok_or_else(|| {
                CollectionError::Infrastructure(
                    "VWorld dataset file row omitted file format".to_owned(),
                )
            })?,
            size_mb_label: em_values[0].clone(),
            size_kib,
            provider_file_kind: em_values[1].clone(),
            base_ym: em_values[2].clone(),
            updated_at: em_values[3].clone(),
            download_kind: if size_kib > LARGE_FILE_THRESHOLD_KIB {
                VWorldDatasetFileKind::SelectionArchive
            } else {
                VWorldDatasetFileKind::SingleResourceFile
            },
        });
    }

    Ok(VWorldDatasetFileInventoryPage {
        current_page_index,
        total_page_count,
        total_file_count,
        files,
    })
}

fn parse_provider_base_uri(name: &str, raw: &str) -> Result<reqwest::Url, CollectionError> {
    let base_uri_raw = format!("{}/", raw.trim().trim_end_matches('/'));
    reqwest::Url::parse(&base_uri_raw).map_err(|error| {
        CollectionError::Infrastructure(format!("invalid {name} base URI: {error}"))
    })
}

fn cookie_header_from_set_cookie_headers(headers: &HeaderMap) -> Result<String, CollectionError> {
    let mut cookie_pairs = Vec::new();
    for value in headers.get_all(SET_COOKIE) {
        let value = value.to_str().map_err(|error| {
            CollectionError::Infrastructure(format!(
                "VWorld dataset login set-cookie header was not valid text: {error}"
            ))
        })?;
        let Some(pair) = value.split(';').next().map(str::trim) else {
            continue;
        };
        if pair.is_empty() {
            continue;
        }
        validate_cookie_pair(pair)?;
        cookie_pairs.push(pair.to_owned());
    }
    if cookie_pairs.is_empty() {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset login did not return session cookies".to_owned(),
        ));
    }
    Ok(cookie_pairs.join("; "))
}

fn validate_cookie_pair(value: &str) -> Result<(), CollectionError> {
    if value.contains('\r') || value.contains('\n') || !value.contains('=') {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset login returned an invalid session cookie".to_owned(),
        ));
    }
    Ok(())
}

fn validate_required_secretless_field(name: &str, value: &str) -> Result<String, CollectionError> {
    if value.trim().is_empty() {
        return Err(CollectionError::Infrastructure(format!(
            "{name} is required"
        )));
    }
    if value.trim() != value || value.contains('\r') || value.contains('\n') {
        return Err(CollectionError::Infrastructure(format!(
            "{name} must not contain leading/trailing whitespace or line breaks"
        )));
    }
    Ok(value.to_owned())
}

fn validate_cookie_header(value: &str) -> Result<String, CollectionError> {
    if value.trim().is_empty() {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset cookie header must not be empty".to_owned(),
        ));
    }
    if value.trim() != value || value.contains('\r') || value.contains('\n') {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset cookie header must not contain leading/trailing whitespace or line breaks"
                .to_owned(),
        ));
    }
    Ok(value.to_owned())
}

fn validate_selector(selector: &VWorldDatasetFileInventorySelector) -> Result<(), CollectionError> {
    if selector.svc_cde.trim().is_empty() || selector.ds_id.trim().is_empty() {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset selector svc_cde and ds_id are required".to_owned(),
        ));
    }
    if selector.svc_cde.trim() != selector.svc_cde
        || selector.ds_id.trim() != selector.ds_id
        || !selector
            .svc_cde
            .bytes()
            .all(|byte| byte.is_ascii_uppercase())
        || !selector.ds_id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset selector must use uppercase svc_cde and numeric ds_id".to_owned(),
        ));
    }
    Ok(())
}

fn validate_download_request(
    request: &VWorldDatasetFileDownloadRequest,
) -> Result<(), CollectionError> {
    validate_download_token("VWorld download dataset id", &request.download_ds_id)?;
    validate_download_token("VWorld fileNo", &request.file_no)
}

fn selection_archive_requires_raon_agent_error(
    request: &VWorldDatasetFileDownloadRequest,
) -> CollectionError {
    CollectionError::ProviderAcquisitionBlocked(format!(
        "VWorld dataset selection archive requires RAON/KUpload desktop agent and is not available through automated server-side HTTP download (download_ds_id={} file_no={})",
        request.download_ds_id, request.file_no
    ))
}

fn selection_download_page_mentions_raon_agent(
    html: &str,
    request: &VWorldDatasetFileDownloadRequest,
) -> bool {
    let provider_file_identity = format!("{}|{}", request.download_ds_id, request.file_no);
    html.contains("RAONKUPLOAD")
        && html.contains("AddUploadedFile")
        && html.contains(&provider_file_identity)
}

fn validate_download_token(name: &'static str, value: &str) -> Result<(), CollectionError> {
    if value.is_empty() {
        return Err(CollectionError::Infrastructure(format!(
            "{name} must not be empty"
        )));
    }
    if value.trim() != value || value.contains('/') || value.contains('\\') || value.contains("..")
    {
        return Err(CollectionError::Infrastructure(format!(
            "{name} must not contain whitespace, path separators, or traversal markers"
        )));
    }
    if !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(CollectionError::Infrastructure(format!(
            "{name} must contain only ASCII letters and digits"
        )));
    }
    Ok(())
}

fn parse_count_summary(html: &str) -> Result<(u64, u64, u64), CollectionError> {
    let summary = count_summary_segment(html).ok_or_else(|| {
        CollectionError::Infrastructure("VWorld dataset page omitted count summary".to_owned())
    })?;
    let b_values = bold_tag_values(summary);
    if b_values.len() < 2 {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset page count summary omitted total/current values".to_owned(),
        ));
    }
    let total_file_count = parse_clean_u64(&b_values[0], "total file count")?;
    let current_page_index = parse_clean_u64(&b_values[1], "current page index")?;
    let total_page_count = value_between(summary, "/ ", " page")
        .or_else(|| value_between(summary, "/", " page"))
        .ok_or_else(|| {
            CollectionError::Infrastructure(
                "VWorld dataset page count summary omitted total page count".to_owned(),
            )
        })
        .and_then(|value| parse_clean_u64(&value, "total page count"))?;
    if current_page_index == 0 || total_page_count == 0 {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset page indexes must be greater than zero".to_owned(),
        ));
    }
    Ok((total_file_count, current_page_index, total_page_count))
}

fn count_summary_segment(html: &str) -> Option<&str> {
    let count_index = html.find("class=\"count")?;
    Some(&html[count_index..html.len().min(count_index + 500)])
}

fn bold_tag_values(segment: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut rest = segment;
    while let Some(start) = rest.find("<b>") {
        rest = &rest[start + "<b>".len()..];
        let Some(end) = rest.find("</b>") else {
            break;
        };
        values.push(clean_html_text(&rest[..end]));
        rest = &rest[end + "</b>".len()..];
    }
    values
}

fn parse_clean_u64(value: &str, field: &str) -> Result<u64, CollectionError> {
    value
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
        .parse::<u64>()
        .map_err(|error| {
            CollectionError::Infrastructure(format!("VWorld dataset {field} must be u64: {error}"))
        })
}

fn single_quoted_args_after(segment: &str, function_marker: &str) -> Option<Vec<String>> {
    let start = segment.find(function_marker)? + function_marker.len();
    let rest = &segment[start..];
    let end = rest.find(')')?;
    Some(single_quoted_values(&rest[..end]))
}

fn single_quoted_values(source: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut in_quote = false;
    let mut current = String::new();
    for ch in source.chars() {
        if ch == '\'' {
            if in_quote {
                values.push(std::mem::take(&mut current));
            }
            in_quote = !in_quote;
            continue;
        }
        if in_quote {
            current.push(ch);
        }
    }
    values
}

fn value_after_marker(segment: &str, marker: &str, closing: &str) -> Option<String> {
    let start = segment.find(marker)? + marker.len();
    let rest = &segment[start..];
    let end = rest.find(closing)?;
    Some(clean_html_text(&rest[..end]))
}

fn value_between(segment: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start = segment.find(start_marker)? + start_marker.len();
    let rest = &segment[start..];
    let end = rest.find(end_marker)?;
    Some(clean_html_text(&rest[..end]))
}

fn text_after_class_marker(segment: &str, marker: &str, closing_tag: &str) -> Option<String> {
    let marker_start = segment.find(marker)?;
    let after_marker = &segment[marker_start..];
    let text_start = after_marker.find('>')? + 1;
    let text_rest = &after_marker[text_start..];
    let text_end = text_rest.find(closing_tag)?;
    Some(clean_html_text(&text_rest[..text_end]))
}

fn first_span_text_after(segment: &str, marker: &str) -> Option<String> {
    let marker_start = segment.find(marker)?;
    let after_marker = &segment[marker_start..];
    let span_start = after_marker.find("<span")?;
    let span = &after_marker[span_start..];
    let text_start = span.find('>')? + 1;
    let text_rest = &span[text_start..];
    let text_end = text_rest.find("</span>")?;
    Some(clean_html_text(&text_rest[..text_end]))
}

fn em_text_values(segment: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut rest = segment;
    while let Some(em_start) = rest.find("<em") {
        rest = &rest[em_start..];
        let Some(text_start) = rest.find('>') else {
            break;
        };
        let text_rest = &rest[text_start + 1..];
        let Some(text_end) = text_rest.find("</em>") else {
            break;
        };
        values.push(clean_html_text(&text_rest[..text_end]));
        rest = &text_rest[text_end + "</em>".len()..];
    }
    values
}

fn clean_html_text(raw: &str) -> String {
    raw.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn provider_file_name_from_headers(headers: &HeaderMap) -> Result<String, CollectionError> {
    provider_file_name_from_headers_or_fallback(headers, None)
}

fn provider_file_name_from_headers_or_fallback(
    headers: &HeaderMap,
    fallback: Option<&str>,
) -> Result<String, CollectionError> {
    let Some(disposition) = header_value(headers, CONTENT_DISPOSITION.as_str()) else {
        return fallback_provider_file_name(fallback);
    };
    let Some(file_name) = parse_content_disposition_file_name(disposition) else {
        return fallback_provider_file_name(fallback);
    };
    validate_provider_file_name(&file_name)?;
    Ok(file_name)
}

fn fallback_provider_file_name(fallback: Option<&str>) -> Result<String, CollectionError> {
    let Some(file_name) = fallback else {
        return Err(CollectionError::Infrastructure(
            "VWorld dataset file response omitted content-disposition filename".to_owned(),
        ));
    };
    validate_provider_file_name(file_name)?;
    Ok(file_name.to_owned())
}

fn parse_content_disposition_file_name(disposition: &str) -> Option<String> {
    for part in disposition.split(';').map(str::trim) {
        if let Some(encoded) = part.strip_prefix("filename*=") {
            return decode_rfc5987_filename(encoded);
        }
        if let Some(file_name) = part.strip_prefix("filename=") {
            return Some(strip_optional_quotes(file_name).to_owned());
        }
    }
    None
}

fn decode_rfc5987_filename(value: &str) -> Option<String> {
    let value = strip_optional_quotes(value);
    let encoded = value
        .strip_prefix("UTF-8''")
        .or_else(|| value.strip_prefix("utf-8''"))
        .unwrap_or(value);
    percent_decode_utf8(encoded).ok()
}

fn percent_decode_utf8(value: &str) -> Result<String, CollectionError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let Some(hex) = bytes.get(index + 1..index + 3) else {
                return Err(CollectionError::Infrastructure(
                    "invalid percent-encoded VWorld filename".to_owned(),
                ));
            };
            let hex = std::str::from_utf8(hex).map_err(|error| {
                CollectionError::Infrastructure(format!(
                    "invalid percent-encoded VWorld filename: {error}"
                ))
            })?;
            let byte = u8::from_str_radix(hex, 16).map_err(|error| {
                CollectionError::Infrastructure(format!(
                    "invalid percent-encoded VWorld filename: {error}"
                ))
            })?;
            decoded.push(byte);
            index += 3;
            continue;
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(decoded).map_err(|error| {
        CollectionError::Infrastructure(format!(
            "invalid UTF-8 percent-decoded VWorld filename: {error}"
        ))
    })
}

fn strip_optional_quotes(value: &str) -> &str {
    let value = value.trim();
    value
        .strip_prefix('"')
        .and_then(|without_prefix| without_prefix.strip_suffix('"'))
        .unwrap_or(value)
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn is_html_response(content_type: &str, raw_payload: &[u8]) -> bool {
    is_html_content_type(content_type)
        || raw_payload
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            == Some(b'<')
}

fn is_html_content_type(content_type: &str) -> bool {
    let normalized = content_type.to_ascii_lowercase();
    normalized.contains("text/html") || normalized.contains("application/xhtml")
}

fn validate_provider_file_name(value: &str) -> Result<(), CollectionError> {
    if value.is_empty() {
        return Err(CollectionError::Infrastructure(
            "VWorld provider file name must not be empty".to_owned(),
        ));
    }
    if value.trim() != value || value.contains('/') || value.contains('\\') || value.contains("..")
    {
        return Err(CollectionError::Infrastructure(
            "VWorld provider file name must not contain whitespace, path separators, or traversal markers"
                .to_owned(),
        ));
    }
    if value.rsplit_once('.').is_none() {
        return Err(CollectionError::Infrastructure(
            "VWorld provider file name must include a file extension".to_owned(),
        ));
    }
    Ok(())
}
