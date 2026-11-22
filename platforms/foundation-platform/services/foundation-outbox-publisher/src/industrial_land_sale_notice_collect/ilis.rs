//! ILIS 분양공고 (`il/parcel`): the whole list in one POST, the detail of one notice by GET, the
//! attachments through the download path the ILIS notice collector uses (root ADR-0603 §1).

use anyhow::Context;
use collection_infrastructure::{
    IlisFileDownload, IlisIndustrialComplexApiClient, IlisIndustrialComplexApiConfig,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::{pointed_text, Detail, DetailRead, Downloaded, FileRef, FileRequest, ListPage, Listed};

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct List {
    pub(crate) path: String,
    pub(crate) page_size: u32,
    pub(crate) records_pointer: String,
    pub(crate) total_pointer: String,
    pub(crate) key_field: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DetailRule {
    pub(crate) path: String,
    pub(crate) result_pointer: String,
    pub(crate) files_field: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Files {
    pub(crate) download_path: String,
    pub(crate) path_field: String,
    pub(crate) name_field: String,
    pub(crate) title_field: String,
    pub(crate) size_field: String,
    pub(crate) in_use_field: String,
    pub(crate) in_use_value: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Rules {
    pub(crate) list: List,
    pub(crate) detail: DetailRule,
    pub(crate) attachments: Files,
}

pub(crate) struct Ilis {
    client: IlisIndustrialComplexApiClient,
    base_url: String,
    rules: Rules,
}

fn whole(value: &JsonValue) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
}

fn text(value: &JsonValue, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// The listed notices of a list answer, newest (largest `par_seq`) first, and the count it states.
pub(crate) fn list_page(list: &List, payload: &JsonValue) -> anyhow::Result<ListPage> {
    let rows = payload
        .pointer(&list.records_pointer)
        .and_then(JsonValue::as_array)
        .with_context(|| {
            format!(
                "the ILIS sale notice list carries no records at {}",
                list.records_pointer
            )
        })?;
    let mut listed = rows
        .iter()
        .filter_map(|row| {
            let id = whole(&row[list.key_field.as_str()])?;
            Some((
                id,
                Listed {
                    id: id.to_string(),
                    row: row.clone(),
                },
            ))
        })
        .collect::<Vec<_>>();
    listed.sort_by_key(|(id, _)| std::cmp::Reverse(*id));
    Ok(ListPage {
        rows: listed.into_iter().map(|(_, listed)| listed).collect(),
        total: payload
            .pointer(&list.total_pointer)
            .and_then(whole)
            .and_then(|total| u64::try_from(total).ok()),
        // One request carries the whole list.
        last: true,
    })
}

/// The 4xx `code` an answer with no result carries: the provider saying the notice is not there.
pub(crate) fn gone_code(payload: &JsonValue) -> Option<u16> {
    whole(&payload["code"])
        .and_then(|code| u16::try_from(code).ok())
        .filter(|code| (400..500).contains(code))
}

/// The attachments a detail result lists, in its order.
pub(crate) fn files_of(base_url: &str, rules: &Rules, payload: &JsonValue) -> Vec<FileRef> {
    let files = &rules.attachments;
    payload
        .pointer(&format!(
            "{}/{}",
            rules.detail.result_pointer, rules.detail.files_field
        ))
        .and_then(JsonValue::as_array)
        .map(|listed| {
            listed
                .iter()
                .map(|file| {
                    let query = match (text(file, &files.name_field), text(file, &files.path_field))
                    {
                        (Some(name), Some(path)) => Some(vec![
                            (files.name_field.clone(), name),
                            (files.path_field.clone(), path),
                        ]),
                        _ => None,
                    };
                    let source_url = query.as_ref().and_then(|query| {
                        reqwest::Url::parse_with_params(
                            &format!(
                                "{}/{}",
                                base_url.trim_end_matches('/'),
                                files.download_path.trim_start_matches('/')
                            ),
                            query,
                        )
                        .ok()
                        .map(String::from)
                    });
                    FileRef {
                        title: text(file, &files.title_field),
                        provider_file_id: text(file, &files.name_field),
                        provider_file_size: whole(&file[files.size_field.as_str()]),
                        request: query.map(FileRequest::IlisQuery),
                        source_url,
                        withdrawn: file.get(&files.in_use_field).and_then(JsonValue::as_str)
                            != Some(files.in_use_value.as_str()),
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

impl Ilis {
    pub(crate) fn new(json: &JsonValue) -> anyhow::Result<Self> {
        let rules: Rules = serde_json::from_value(json.clone())
            .context("the ilis source of the lane contract does not parse")?;
        let base_url = pointed_text(
            json["base_url_from"]
                .as_str()
                .context("the ilis source names no base_url_from")?,
        )?;
        let client = IlisIndustrialComplexApiClient::new(&IlisIndustrialComplexApiConfig {
            base_uri: base_url.clone(),
            user_agent: super::USER_AGENT.to_owned(),
        })?;
        Ok(Self {
            client,
            base_url,
            rules,
        })
    }

    /// The list, and its raw answer for Bronze (`list.keep_in_bronze`: ILIS takes notices down).
    pub(crate) async fn page(
        &mut self,
        page_no: u32,
    ) -> anyhow::Result<(ListPage, Option<Vec<u8>>)> {
        let page = self
            .client
            .fetch_page(&self.rules.list.path, page_no, self.rules.list.page_size)
            .await
            .context("failed to fetch the ILIS sale notice list")?;
        Ok((
            list_page(&self.rules.list, &page.payload)?,
            Some(page.raw_payload),
        ))
    }

    /// One notice's detail. A notice the provider took down answers HTTP 200 with `code` 400 and
    /// no result (measured 2026-11-22 on par_seq 832): that is [`DetailRead::Gone`].
    pub(crate) async fn detail(&mut self, listed: &Listed) -> anyhow::Result<DetailRead> {
        let page = self
            .client
            .fetch_detail(&self.rules.detail.path, &listed.id)
            .await?;
        if !page
            .payload
            .pointer(&self.rules.detail.result_pointer)
            .is_some_and(JsonValue::is_object)
        {
            if let Some(code) = gone_code(&page.payload) {
                return Ok(DetailRead::Gone(code));
            }
            anyhow::bail!("no object at {}", self.rules.detail.result_pointer);
        }
        Ok(DetailRead::Found(Detail {
            http_status: Some(200),
            files: files_of(&self.base_url, &self.rules, &page.payload),
            body: page.payload,
        }))
    }

    pub(crate) async fn download(&mut self, request: &FileRequest) -> anyhow::Result<Downloaded> {
        let FileRequest::IlisQuery(query) = request else {
            anyhow::bail!("an ILIS attachment is asked by its download query");
        };
        let pairs = query
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        Ok(
            match self
                .client
                .download_file(&self.rules.attachments.download_path, &pairs)
                .await?
            {
                IlisFileDownload::Bytes(bytes) => Downloaded::Bytes(bytes),
                IlisFileDownload::Gone(status) => Downloaded::Gone(status),
            },
        )
    }
}
