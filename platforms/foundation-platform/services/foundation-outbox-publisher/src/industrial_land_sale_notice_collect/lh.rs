//! LH 토지 공고 through data.go.kr (`B552555`): the notice list of kind `01` (토지) over the whole
//! history window, newest first; per notice its detail (`dsAhflInfo` lists the attachments) and its
//! supply list (per 필지: 용도·소재지·지번·면적·예정가격), kept together as one read (root ADR-0603
//! §1). The shapes follow LH's usage guides (공고문 조회 2026-07, 상세정보 2026-02, 공급정보 2026-02);
//! no answer has been seen: the owner's usage application for the three services comes first, and
//! until then the gateway refuses and the run ends with [`Refused`] before writing anything.

use std::time::Duration;

use anyhow::{bail, Context};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use super::{
    contract_date, Detail, DetailRead, Downloaded, FileRef, FileRequest, ListPage, Listed,
};
use crate::media_store::spool_response;
use crate::public_data_control_support::optional_env_value;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct List {
    pub(crate) path: String,
    pub(crate) notice_kind_code: String,
    pub(crate) page_size: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Endpoint {
    pub(crate) path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Files {
    pub(crate) url_host: String,
    pub(crate) list_fields: Vec<String>,
    pub(crate) name_field: String,
    pub(crate) url_field: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Rules {
    pub(crate) base_url: String,
    pub(crate) service_key_env: String,
    pub(crate) list: List,
    pub(crate) detail: Endpoint,
    pub(crate) supply: Endpoint,
    pub(crate) attachments: Files,
    pub(crate) refusal_codes: Vec<String>,
}

/// The gateway refused the lane's access (no usage application for the service, or no key): an
/// owner's decision, not a failure a later run gets past by asking again.
#[derive(Debug)]
pub(crate) struct Refused(pub(crate) String);

impl std::fmt::Display for Refused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "data.go.kr refused the LH land notice services: {}",
            self.0
        )
    }
}

impl std::error::Error for Refused {}

pub(crate) struct Lh {
    http: reqwest::Client,
    rules: Rules,
    service_key: Option<String>,
    from: NaiveDate,
    until: NaiveDate,
}

/// The gateway's refusal a body names, if it names one of `codes`.
pub(crate) fn refusal(codes: &[String], body: &str) -> Option<String> {
    codes
        .iter()
        .find(|code| body.contains(code.as_str()))
        .cloned()
}

/// The datasets named `name` across the answer's elements (an LH answer is an array of objects,
/// each holding some of its `ds…` lists).
fn datasets<'a>(answer: &'a JsonValue, name: &str) -> Vec<&'a JsonValue> {
    answer
        .as_array()
        .map(|elements| {
            elements
                .iter()
                .filter_map(|element| element.get(name).and_then(JsonValue::as_array))
                .flatten()
                .collect()
        })
        .unwrap_or_default()
}

fn text(value: &JsonValue, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// The notices of one list answer, in its order (the provider's newest first), and its count.
pub(crate) fn list_page(
    rules: &Rules,
    page_no: u32,
    answer: &JsonValue,
) -> anyhow::Result<ListPage> {
    let header_ok = datasets(answer, "resHeader")
        .iter()
        .any(|header| text(header, "SS_CODE").as_deref() == Some("Y"));
    if !header_ok {
        bail!("the LH list answer carries no successful resHeader");
    }
    let rows = datasets(answer, "dsList")
        .into_iter()
        .filter_map(|row| {
            Some(Listed {
                id: text(row, "PAN_ID")?,
                row: row.clone(),
            })
        })
        .collect::<Vec<_>>();
    let total = rows
        .first()
        .and_then(|first| text(&first.row, "ALL_CNT"))
        .and_then(|count| count.replace(',', "").parse::<u64>().ok());
    let size = u64::from(rules.list.page_size);
    let last =
        (rows.len() as u64) < size || total.is_some_and(|total| u64::from(page_no) * size >= total);
    Ok(ListPage { rows, total, last })
}

/// The attachments a detail answer lists: the `list_fields` datasets' rows, in order. A URL on
/// another host than `url_host` is recorded and not asked for.
pub(crate) fn files_of(rules: &Rules, detail: &JsonValue) -> Vec<FileRef> {
    let files = &rules.attachments;
    files
        .list_fields
        .iter()
        .flat_map(|field| datasets(detail, field))
        .map(|file| {
            let url = text(file, &files.url_field);
            let parsed = url.as_deref().and_then(|url| reqwest::Url::parse(url).ok());
            let on_host = parsed.as_ref().is_some_and(|url| {
                url.scheme() == "https" && url.host_str() == Some(files.url_host.as_str())
            });
            FileRef {
                title: text(file, &files.name_field),
                provider_file_id: parsed.as_ref().and_then(|url| {
                    url.query_pairs()
                        .find(|(name, _)| name == "fileid")
                        .map(|(_, value)| value.into_owned())
                }),
                provider_file_size: None,
                request: url.clone().filter(|_| on_host).map(FileRequest::Url),
                source_url: url,
                withdrawn: false,
            }
        })
        .collect()
}

/// The detail and supply parameters of a listed notice (`AIS_TP_CD` when it has one).
pub(crate) fn notice_params(listed: &Listed) -> anyhow::Result<Vec<(String, String)>> {
    let mut params = Vec::new();
    for field in [
        "SPL_INF_TP_CD",
        "CCR_CNNT_SYS_DS_CD",
        "PAN_ID",
        "UPP_AIS_TP_CD",
    ] {
        params.push((
            field.to_owned(),
            text(&listed.row, field)
                .with_context(|| format!("the LH notice {} lists no {field}", listed.id))?,
        ));
    }
    if let Some(kind) = text(&listed.row, "AIS_TP_CD") {
        params.push(("AIS_TP_CD".to_owned(), kind));
    }
    Ok(params)
}

impl Lh {
    pub(crate) fn new(json: &JsonValue, today: NaiveDate) -> anyhow::Result<Self> {
        let rules: Rules = serde_json::from_value(json.clone())
            .context("the lh source of the lane contract does not parse")?;
        let from = contract_date(&json["list"], "history_from")?;
        let http = reqwest::Client::builder()
            .user_agent(super::USER_AGENT)
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()
            .context("failed to build the LH HTTP client")?;
        let service_key = optional_env_value(&rules.service_key_env)?
            .map(|key| key.trim().to_owned())
            .filter(|key| !key.is_empty());
        Ok(Self {
            http,
            rules,
            service_key,
            from,
            until: today,
        })
    }

    /// One gateway call; the key is in the URL, so no error carries the URL.
    async fn call(&self, path: &str, params: &[(String, String)]) -> anyhow::Result<(u16, String)> {
        let Some(key) = &self.service_key else {
            return Err(Refused(format!("{} is not set", self.rules.service_key_env)).into());
        };
        let mut query = vec![("serviceKey".to_owned(), key.clone())];
        query.extend(params.iter().cloned());
        let response = self
            .http
            .get(format!(
                "{}/{}",
                self.rules.base_url.trim_end_matches('/'),
                path.trim_start_matches('/')
            ))
            .query(&query)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("LH request failed: {}", error.without_url()))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|error| anyhow::anyhow!("LH body failed: {}", error.without_url()))?;
        if let Some(code) = refusal(&self.rules.refusal_codes, &body) {
            return Err(Refused(code).into());
        }
        Ok((status, body))
    }

    /// One list page, and its raw answer for Bronze (`list.keep_in_bronze`: the statuses it lists
    /// change, and the answers are their history).
    pub(crate) async fn page(
        &mut self,
        page_no: u32,
    ) -> anyhow::Result<(ListPage, Option<Vec<u8>>)> {
        let params = vec![
            ("PG_SZ".to_owned(), self.rules.list.page_size.to_string()),
            ("PAGE".to_owned(), page_no.to_string()),
            (
                "UPP_AIS_TP_CD".to_owned(),
                self.rules.list.notice_kind_code.clone(),
            ),
            (
                "PAN_ST_DT".to_owned(),
                self.from.format("%Y%m%d").to_string(),
            ),
            (
                "PAN_ED_DT".to_owned(),
                self.until.format("%Y%m%d").to_string(),
            ),
        ];
        let (status, body) = self.call(&self.rules.list.path, &params).await?;
        if !(200..300).contains(&status) {
            bail!("the LH list answered HTTP {status}");
        }
        let answer: JsonValue =
            serde_json::from_str(&body).context("the LH list answer is not JSON")?;
        Ok((
            list_page(&self.rules, page_no, &answer)?,
            Some(body.into_bytes()),
        ))
    }

    /// One notice's detail and supply list. A 4xx from the gateway is [`DetailRead::Gone`]; a refusal
    /// of either service is [`Refused`], which ends the run.
    pub(crate) async fn detail(&mut self, listed: &Listed) -> anyhow::Result<DetailRead> {
        let params = notice_params(listed)?;
        let (status, detail) = self.call(&self.rules.detail.path, &params).await?;
        if (400..500).contains(&status) {
            return Ok(DetailRead::Gone(status));
        }
        if !(200..300).contains(&status) {
            bail!("the LH detail answered HTTP {status}");
        }
        let detail: JsonValue =
            serde_json::from_str(&detail).context("the LH detail answer is not JSON")?;
        let (supply_status, supply) = self.call(&self.rules.supply.path, &params).await?;
        let supply =
            serde_json::from_str::<JsonValue>(&supply).unwrap_or(JsonValue::String(supply));
        Ok(DetailRead::Found(Detail {
            http_status: Some(status),
            files: files_of(&self.rules, &detail),
            body: json!({
                "detail": detail,
                "supply": {"http_status": supply_status, "answer": supply},
            }),
        }))
    }

    pub(crate) async fn download(
        &mut self,
        request: &FileRequest,
        max_bytes: u64,
    ) -> anyhow::Result<Downloaded> {
        let FileRequest::Url(url) = request else {
            bail!("an LH attachment is asked by its URL");
        };
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("LH download failed: {error}"))?;
        let status = response.status();
        if status.is_client_error() {
            return Ok(Downloaded::Gone(status.as_u16()));
        }
        if !status.is_success() {
            bail!("LH answered HTTP {status} for a file");
        }
        Ok(Downloaded::Spooled(
            spool_response(response, max_bytes)
                .await
                .context("LH file body failed")?,
        ))
    }
}
