//! HTTP client and inventory parser for `hub.go.kr` bulk files.

use bytes::Bytes;
use collection_domain::CollectionError;
use futures_util::{stream::BoxStream, StreamExt as _};
use reqwest::header::{HeaderMap, CONTENT_DISPOSITION, CONTENT_TYPE};

use outbound_http_infrastructure::{
    classify_response, execute_retryable, execute_streaming_handshake, redact_transport_error,
    shared_http_client, AttemptError, ResilienceAudit, ResilienceCtx, RetryDecision, HUB,
};

/// Provider label for audit events and error messages.
const PROVIDER: &str = "hub.go.kr bulk file";

const FILE_DOWNLOAD_PATH: &str = "cmm/fms/fileOpnDown.do";
const INVENTORY_PATH: &str = "portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do";
const MAX_INVENTORY_PAGE_COUNT: usize = 1_000;

/// Configuration for a `hub.go.kr` bulk-file client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkConfig {
    /// Base URI for `hub.go.kr` or a compatible test server.
    pub base_uri: String,
    /// User-Agent header sent to the public-data provider.
    pub user_agent: String,
}

/// One downloadable item parsed from the `hub.go.kr` public inventory page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkInventoryItem {
    /// Provider category label shown on the official page.
    pub category_name: String,
    /// Provider service name without the period suffix.
    pub service_name: String,
    /// Provider period label extracted from the title, for example `2026-04`.
    pub service_period_label: String,
    /// Provider file period label shown by the official page, for example `2026-05`.
    pub provider_file_period: String,
    /// First argument passed to the official `fnDownloadPop` call.
    pub task_group_code: String,
    /// Second argument passed to the official `fnDownloadPop` call.
    pub task_code: String,
    /// Stable provider server file id passed as `srvrFileNm`.
    pub file_id: String,
}

/// Request for a single `hub.go.kr` bulk-file download.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkDownloadRequest {
    /// Stable provider server file id passed as `srvrFileNm`.
    pub file_id: String,
}

/// Raw file downloaded from `hub.go.kr`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkFile {
    /// Raw provider file bytes, suitable for Bronze storage.
    pub raw_payload: Vec<u8>,
    /// Response content type.
    pub content_type: String,
    /// Provider file name from the response content-disposition header.
    pub provider_file_name: String,
}

/// Streaming file response opened from `hub.go.kr`.
pub struct BuildingHubBulkFileStream {
    /// Response content type.
    pub content_type: String,
    /// Provider file name from the response content-disposition header.
    pub provider_file_name: String,
    /// Exact response byte length when the provider declares one.
    pub expected_size_bytes: Option<u64>,
    body: BoxStream<'static, Result<Bytes, CollectionError>>,
}

impl BuildingHubBulkFileStream {
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

/// `reqwest` backed client for `hub.go.kr` bulk files.
#[derive(Clone, Debug)]
pub struct BuildingHubBulkClient {
    base_uri: reqwest::Url,
    user_agent: String,
    http: reqwest::Client,
    audit: ResilienceAudit,
}

impl BuildingHubBulkClient {
    /// Creates a `hub.go.kr` bulk-file client from explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid or the User-Agent is empty.
    pub fn new(config: &BuildingHubBulkConfig) -> Result<Self, CollectionError> {
        let base_uri_raw = format!("{}/", config.base_uri.trim().trim_end_matches('/'));
        let base_uri = reqwest::Url::parse(&base_uri_raw).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid hub.go.kr bulk base URI: {error}"))
        })?;
        let user_agent = config.user_agent.trim().to_owned();
        if user_agent.is_empty() {
            return Err(CollectionError::Infrastructure(
                "hub.go.kr bulk user_agent is required".to_owned(),
            ));
        }
        let http = shared_http_client(PROVIDER, &HUB)
            .map_err(crate::outbound_http_error::into_collection_error)?;

        Ok(Self {
            base_uri,
            user_agent,
            http,
            audit: ResilienceAudit::new(PROVIDER),
        })
    }

    /// Fetches the complete official downloadable inventory exposed by `hub.go.kr`.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the provider inventory cannot be read or parsed.
    pub async fn fetch_inventory(
        &self,
    ) -> Result<Vec<BuildingHubBulkInventoryItem>, CollectionError> {
        let first_page = self.fetch_inventory_page(1).await?;
        let max_page = inventory_max_page(&first_page)?;
        let mut items = parse_building_hub_bulk_inventory(&first_page)?;
        for page_index in 2..=max_page {
            let page = self.fetch_inventory_page(page_index).await?;
            items.extend(parse_building_hub_bulk_inventory(&page)?);
        }
        Ok(items)
    }

    /// Downloads one official bulk file by server file id.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the provider id is invalid, the request fails, or the
    /// response is not a downloadable provider file.
    pub async fn fetch_file(
        &self,
        request: &BuildingHubBulkDownloadRequest,
    ) -> Result<BuildingHubBulkFile, CollectionError> {
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
                    "hub.go.kr bulk file response body read failed: {error}"
                ))
            })?
            .to_vec();
        if raw_payload.is_empty() {
            return Err(CollectionError::Infrastructure(
                "hub.go.kr bulk file response body was empty".to_owned(),
            ));
        }
        if is_html_response(&content_type, &raw_payload) {
            return Err(CollectionError::Infrastructure(
                "hub.go.kr bulk file request returned HTML instead of a provider file".to_owned(),
            ));
        }
        let provider_file_name = provider_file_name_from_headers(&headers)?;

        Ok(BuildingHubBulkFile {
            raw_payload,
            content_type,
            provider_file_name,
        })
    }

    /// Opens one official bulk file by server file id as a byte stream.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the provider id is invalid, the request fails, or the
    /// response is not a downloadable provider file.
    pub async fn open_file_stream(
        &self,
        request: &BuildingHubBulkDownloadRequest,
    ) -> Result<BuildingHubBulkFileStream, CollectionError> {
        let response = self.send_file_request(request).await?;
        let expected_size_bytes = response.content_length();
        if expected_size_bytes == Some(0) {
            return Err(CollectionError::Infrastructure(
                "hub.go.kr bulk file response body was empty".to_owned(),
            ));
        }
        let headers = response.headers().clone();
        let content_type = header_value(&headers, CONTENT_TYPE.as_str())
            .unwrap_or("application/octet-stream")
            .to_owned();
        if is_html_content_type(&content_type) {
            return Err(CollectionError::Infrastructure(
                "hub.go.kr bulk file request returned HTML instead of a provider file".to_owned(),
            ));
        }
        let provider_file_name = provider_file_name_from_headers(&headers)?;
        let body = response
            .bytes_stream()
            .map(|chunk| {
                chunk.map_err(|error| {
                    CollectionError::Infrastructure(format!(
                        "hub.go.kr bulk file response body stream failed: {error}"
                    ))
                })
            })
            .boxed();

        Ok(BuildingHubBulkFileStream {
            content_type,
            provider_file_name,
            expected_size_bytes,
            body,
        })
    }

    /// Sends one download request through [`execute_streaming_handshake`]: transient failures
    /// are retried up to the status-validated `Response`; callers open the body (buffered or
    /// streamed) after this returns, with no retry once the stream starts. The download POST
    /// is a semantically idempotent read (form carries only the provider file id). No circuit
    /// breaker — ingest creates a client per job, so its state would be cosmetic (spec §8).
    async fn send_file_request(
        &self,
        request: &BuildingHubBulkDownloadRequest,
    ) -> Result<reqwest::Response, CollectionError> {
        validate_provider_file_id(&request.file_id)?;
        let url = self.base_uri.join(FILE_DOWNLOAD_PATH).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid hub.go.kr bulk file URL: {error}"))
        })?;
        let ctx = ResilienceCtx {
            breaker: None,
            policy: &HUB,
            audit: &self.audit,
        };
        execute_streaming_handshake(&ctx, || self.send_file_request_once(&url, request))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn send_file_request_once(
        &self,
        url: &reqwest::Url,
        request: &BuildingHubBulkDownloadRequest,
    ) -> Result<reqwest::Response, AttemptError> {
        let response = self
            .http
            .post(url.clone())
            .header("user-agent", &self.user_agent)
            .form(&[("srvrFileNm", request.file_id.as_str())])
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
                        format!("{PROVIDER} request returned HTTP {status}"),
                    ))
                }
            });
        }
        Ok(response)
    }

    /// Fetches one inventory page through [`execute_retryable`] (idempotent GET; spec §3.2).
    /// No circuit breaker — same client/job-local scope rationale as downloads (spec §8).
    async fn fetch_inventory_page(&self, page_index: usize) -> Result<String, CollectionError> {
        let mut url = self.base_uri.join(INVENTORY_PATH).map_err(|error| {
            CollectionError::Infrastructure(format!(
                "invalid hub.go.kr bulk inventory URL: {error}"
            ))
        })?;
        url.query_pairs_mut()
            .append_pair("pageIndex", &page_index.to_string());
        let ctx = ResilienceCtx {
            breaker: None,
            policy: &HUB,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || self.fetch_inventory_page_once(&url))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_inventory_page_once(&self, url: &reqwest::Url) -> Result<String, AttemptError> {
        let response = self
            .http
            .get(url.clone())
            .header("user-agent", &self.user_agent)
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
                        format!("hub.go.kr bulk inventory request returned HTTP {status}"),
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

/// Parses the official `hub.go.kr` bulk-file inventory page.
///
/// # Errors
///
/// Returns `CollectionError` when a downloadable list item has an invalid `fnDownloadPop` call.
pub fn parse_building_hub_bulk_inventory(
    html: &str,
) -> Result<Vec<BuildingHubBulkInventoryItem>, CollectionError> {
    let mut items = Vec::new();
    for segment in html.split("<li") {
        if !segment.contains("fnDownloadPop(") {
            continue;
        }
        let args = single_quoted_args_after(segment, "fnDownloadPop(").ok_or_else(|| {
            CollectionError::Infrastructure(
                "hub.go.kr bulk inventory item omitted fnDownloadPop arguments".to_owned(),
            )
        })?;
        if args.is_empty() {
            continue;
        }
        if args.len() != 3 {
            return Err(CollectionError::Infrastructure(format!(
                "hub.go.kr bulk inventory fnDownloadPop expected 3 arguments, got {}",
                args.len()
            )));
        }
        let category_name =
            text_after_class_marker(segment, "class=\"tagset", "</p>").ok_or_else(|| {
                CollectionError::Infrastructure(
                    "hub.go.kr bulk inventory item omitted category name".to_owned(),
                )
            })?;
        let title = text_after_class_marker(segment, "class=\"tit\"", "</p>").ok_or_else(|| {
            CollectionError::Infrastructure(
                "hub.go.kr bulk inventory item omitted service title".to_owned(),
            )
        })?;
        let provider_file_period = text_after_class_marker(segment, "class=\"detail\"", "</span>")
            .ok_or_else(|| {
                CollectionError::Infrastructure(
                    "hub.go.kr bulk inventory item omitted provider file period".to_owned(),
                )
            })?;
        let (service_name, service_period_label) = split_service_title(&title);

        items.push(BuildingHubBulkInventoryItem {
            category_name,
            service_name,
            service_period_label,
            provider_file_period,
            task_group_code: args[0].clone(),
            task_code: args[1].clone(),
            file_id: args[2].clone(),
        });
    }
    Ok(items)
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

fn text_after_class_marker(segment: &str, marker: &str, closing_tag: &str) -> Option<String> {
    let marker_start = segment.find(marker)?;
    let after_marker = &segment[marker_start..];
    let text_start = after_marker.find('>')? + 1;
    let text_rest = &after_marker[text_start..];
    let text_end = text_rest.find(closing_tag)?;
    Some(clean_html_text(&text_rest[..text_end]))
}

fn clean_html_text(raw: &str) -> String {
    raw.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .trim()
        .to_owned()
}

fn split_service_title(title: &str) -> (String, String) {
    let title = title.trim();
    if let Some((service_name, period_label)) = title.rsplit_once(" (") {
        if let Some(period_label) = period_label.strip_suffix(')') {
            return (
                service_name.trim().to_owned(),
                period_label.trim().to_owned(),
            );
        }
    }
    (title.to_owned(), String::new())
}

fn inventory_max_page(html: &str) -> Result<usize, CollectionError> {
    let mut max_page = 1_usize;
    let mut rest = html;
    while let Some(index) = rest.find("pageIndex=") {
        rest = &rest[index + "pageIndex=".len()..];
        let digits = rest
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>();
        if digits.is_empty() {
            continue;
        }
        let page = digits.parse::<usize>().map_err(|error| {
            CollectionError::Infrastructure(format!(
                "invalid hub.go.kr inventory pageIndex value: {error}"
            ))
        })?;
        max_page = max_page.max(page);
        if max_page > MAX_INVENTORY_PAGE_COUNT {
            return Err(CollectionError::Infrastructure(format!(
                "hub.go.kr inventory page count exceeded safety cap {MAX_INVENTORY_PAGE_COUNT}"
            )));
        }
    }
    Ok(max_page)
}

fn provider_file_name_from_headers(headers: &HeaderMap) -> Result<String, CollectionError> {
    let disposition = header_value(headers, CONTENT_DISPOSITION.as_str()).ok_or_else(|| {
        CollectionError::Infrastructure(
            "hub.go.kr bulk file response omitted content-disposition filename".to_owned(),
        )
    })?;
    let file_name = parse_content_disposition_file_name(disposition).ok_or_else(|| {
        CollectionError::Infrastructure(
            "hub.go.kr bulk file response omitted content-disposition filename".to_owned(),
        )
    })?;
    validate_provider_file_name(&file_name)?;
    Ok(file_name)
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
                    "invalid percent-encoded hub.go.kr filename".to_owned(),
                ));
            };
            let hex = std::str::from_utf8(hex).map_err(|error| {
                CollectionError::Infrastructure(format!(
                    "invalid percent-encoded hub.go.kr filename: {error}"
                ))
            })?;
            let byte = u8::from_str_radix(hex, 16).map_err(|error| {
                CollectionError::Infrastructure(format!(
                    "invalid percent-encoded hub.go.kr filename: {error}"
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
            "invalid UTF-8 percent-decoded hub.go.kr filename: {error}"
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

fn validate_provider_file_id(value: &str) -> Result<(), CollectionError> {
    if value.is_empty() {
        return Err(CollectionError::Infrastructure(
            "hub.go.kr provider file id must not be empty".to_owned(),
        ));
    }
    if value.trim() != value || value.contains('/') || value.contains('\\') || value.contains("..")
    {
        return Err(CollectionError::Infrastructure(
            "hub.go.kr provider file id must not contain whitespace, path separators, or traversal markers"
                .to_owned(),
        ));
    }
    if !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(CollectionError::Infrastructure(
            "hub.go.kr provider file id must contain only ASCII letters and digits".to_owned(),
        ));
    }
    Ok(())
}

fn validate_provider_file_name(value: &str) -> Result<(), CollectionError> {
    if value.is_empty() {
        return Err(CollectionError::Infrastructure(
            "hub.go.kr provider file name must not be empty".to_owned(),
        ));
    }
    if value.trim() != value || value.contains('/') || value.contains('\\') || value.contains("..")
    {
        return Err(CollectionError::Infrastructure(
            "hub.go.kr provider file name must not contain whitespace, path separators, or traversal markers"
                .to_owned(),
        ));
    }
    if value.rsplit_once('.').is_none() {
        return Err(CollectionError::Infrastructure(
            "hub.go.kr provider file name must include a file extension".to_owned(),
        ));
    }
    Ok(())
}
