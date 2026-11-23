//! 공장설립온라인지원시스템(팩토리온) 분양 및 처분신청 공고 (`bbs/lttotNdDspsPblancList.do`): ten notices
//! a page, newest first, and one page per notice with its fields and attachments (root ADR-0603 §8).
//! The owner decided to collect it knowing its robots.txt disallows `/bbs/`, at the least load: the
//! contract's spacing (seven seconds), the whole history once, then the first page daily. Any 4xx,
//! 429 or a page that is not the one asked for is [`Blocked`]: the run stops at once and fails, so
//! it is seen, and asks nothing more.

use std::time::Duration;

use anyhow::{bail, Context};
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use super::{Blocked, Detail, DetailRead, Downloaded, FileRef, FileRequest, ListPage, Listed};
use crate::media_store::spool_response;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct List {
    pub(crate) path: String,
    pub(crate) page_field: String,
    pub(crate) page_rows: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DetailRule {
    pub(crate) path: String,
    pub(crate) id_field: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Files {
    pub(crate) download_path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Rules {
    pub(crate) base_url: String,
    pub(crate) user_agent: String,
    pub(crate) list: List,
    pub(crate) detail: DetailRule,
    pub(crate) attachments: Files,
}

pub(crate) struct Factoryon {
    http: reqwest::Client,
    rules: Rules,
}

/// The page shapes this reads. Built for each page read and dropped with it, so the run's future
/// stays `Send`.
pub(crate) struct Patterns {
    list_table: Selector,
    row: Selector,
    cell: Selector,
    link: Selector,
    file_icon: Selector,
    detail_row: Selector,
    header: Selector,
    data: Selector,
    file_link: Selector,
    view_call: Regex,
}

impl Patterns {
    pub(crate) fn new() -> anyhow::Result<Self> {
        let css = |text: &str| {
            Selector::parse(text).map_err(|error| anyhow::anyhow!("selector {text:?}: {error:?}"))
        };
        Ok(Self {
            list_table: css("table.cellType_b")?,
            row: css("table.cellType_b tbody tr")?,
            cell: css("td")?,
            link: css("a[href]")?,
            file_icon: css("img")?,
            detail_row: css("tr")?,
            header: css("th")?,
            data: css("td")?,
            file_link: css("a.txt_file[href]")?,
            view_call: Regex::new(r"fnSelectView\(\s*([0-9]+)\s*\)")?,
        })
    }
}

/// An element's text with its whitespace runs folded to one space.
fn folded(element: ElementRef<'_>) -> String {
    element
        .text()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The notices of one list page, newest first, and (on any page) the count its first row's number
/// states (the board numbers its notices from 1).
///
/// # Errors
/// [`Blocked`] when the page is not the board's list (a block or error page).
pub(crate) fn list_page(patterns: &Patterns, rules: &List, html: &str) -> anyhow::Result<ListPage> {
    let document = Html::parse_document(html);
    if document.select(&patterns.list_table).next().is_none() {
        return Err(Blocked(
            "factoryon.go.kr answered a page that is not the notice list".to_owned(),
        )
        .into());
    }
    let mut numbers = Vec::new();
    let rows = document
        .select(&patterns.row)
        .filter_map(|row| {
            let cells = row.select(&patterns.cell).collect::<Vec<_>>();
            let link = cells.get(2)?.select(&patterns.link).next()?;
            let id = patterns
                .view_call
                .captures(link.value().attr("href")?)?
                .get(1)?
                .as_str()
                .to_owned();
            let number = cells.first().map(|cell| folded(*cell));
            if let Some(number) = number.as_deref().and_then(|text| text.parse::<u64>().ok()) {
                numbers.push(number);
            }
            let text = |index: usize| cells.get(index).map(|cell| folded(*cell));
            Some(Listed {
                row: json!({
                    "bbs_sn": id,
                    "number": number,
                    "kind": text(1),
                    "title": folded(link),
                    "publisher": text(3),
                    "notice_date": text(4),
                    "has_attachment": cells
                        .get(5)
                        .is_some_and(|cell| cell.select(&patterns.file_icon).next().is_some()),
                    "period": text(6),
                }),
                id,
            })
        })
        .collect::<Vec<_>>();
    let total = numbers.iter().max().copied();
    let last = rows.is_empty() || rows.len() < rules.page_rows || numbers.contains(&1);
    Ok(ListPage { rows, total, last })
}

/// The attachments one notice page lists, in its order.
///
/// # Errors
/// [`Blocked`] when the page holds no notice (a block or error page).
pub(crate) fn files_of(
    patterns: &Patterns,
    rules: &Rules,
    html: &str,
) -> anyhow::Result<Vec<FileRef>> {
    let document = Html::parse_document(html);
    let has_title = document.select(&patterns.detail_row).any(|row| {
        row.select(&patterns.header)
            .next()
            .is_some_and(|header| folded(header) == "제목")
            && row.select(&patterns.data).next().is_some()
    });
    if !has_title {
        return Err(
            Blocked("factoryon.go.kr answered a page that holds no notice".to_owned()).into(),
        );
    }
    let base = rules.base_url.trim_end_matches('/');
    let path = rules.attachments.download_path.trim_matches('/');
    Ok(document
        .select(&patterns.file_link)
        .filter_map(|link| {
            let href = link.value().attr("href")?;
            let relative = href.trim_start_matches('/');
            if !relative.starts_with(path) {
                return None;
            }
            let url = format!("{base}/{relative}");
            let parsed = reqwest::Url::parse(&url).ok()?;
            let value = |name: &str| {
                parsed
                    .query_pairs()
                    .find(|(key, _)| key == name)
                    .map(|(_, value)| value.into_owned())
            };
            Some(FileRef {
                title: Some(folded(link)).filter(|title| !title.is_empty()),
                provider_file_id: match (value("atchmnflManageNo"), value("atchmnflSn")) {
                    (Some(manage), Some(sequence)) => Some(format!("{manage}/{sequence}")),
                    _ => None,
                },
                provider_file_size: None,
                request: Some(FileRequest::Url(url.clone())),
                source_url: Some(url),
                withdrawn: false,
            })
        })
        .collect())
}

/// A 4xx (429 included) is the site refusing the collector: [`Blocked`].
fn refused(status: reqwest::StatusCode, what: &str) -> anyhow::Result<()> {
    if status.is_client_error() {
        return Err(Blocked(format!("factoryon.go.kr answered HTTP {status} for {what}")).into());
    }
    if !status.is_success() {
        bail!("factoryon.go.kr answered HTTP {status} for {what}");
    }
    Ok(())
}

impl Factoryon {
    pub(crate) fn new(json: &JsonValue) -> anyhow::Result<Self> {
        let rules: Rules = serde_json::from_value(json.clone())
            .context("the factoryon source of the lane contract does not parse")?;
        let http = reqwest::Client::builder()
            .user_agent(rules.user_agent.clone())
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()
            .context("failed to build the Factory-on HTTP client")?;
        // The patterns are this file's constants: a broken one fails the run before any request.
        Patterns::new()?;
        Ok(Self { http, rules })
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.rules.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    async fn page_text(
        &self,
        url: &str,
        query: &[(String, String)],
        what: &str,
    ) -> anyhow::Result<String> {
        let response = self
            .http
            .get(url)
            .query(query)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("factoryon.go.kr request failed: {error}"))?;
        refused(response.status(), what)?;
        let text = response
            .text()
            .await
            .map_err(|error| anyhow::anyhow!("factoryon.go.kr body failed: {error}"))?;
        Ok(text)
    }

    pub(crate) async fn page(&mut self, page_no: u32) -> anyhow::Result<ListPage> {
        let query = vec![(self.rules.list.page_field.clone(), page_no.to_string())];
        let html = self
            .page_text(
                &self.url(&self.rules.list.path),
                &query,
                &format!("list page {page_no}"),
            )
            .await?;
        list_page(&Patterns::new()?, &self.rules.list, &html)
    }

    pub(crate) async fn detail(&mut self, listed: &Listed) -> anyhow::Result<DetailRead> {
        let query = vec![(self.rules.detail.id_field.clone(), listed.id.clone())];
        let html = self
            .page_text(
                &self.url(&self.rules.detail.path),
                &query,
                &format!("notice {}", listed.id),
            )
            .await?;
        let files = files_of(&Patterns::new()?, &self.rules, &html)?;
        Ok(DetailRead::Found(Detail {
            http_status: Some(200),
            body: json!({ "html": html }),
            files,
        }))
    }

    pub(crate) async fn download(
        &mut self,
        request: &FileRequest,
        max_bytes: u64,
    ) -> anyhow::Result<Downloaded> {
        let FileRequest::Url(url) = request else {
            bail!("a Factory-on attachment is asked by its URL");
        };
        anyhow::ensure!(
            url.starts_with(&format!("{}/", self.rules.base_url.trim_end_matches('/'))),
            "{url} is not on the site's host"
        );
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("factoryon.go.kr download failed: {error}"))?;
        refused(response.status(), "an attachment")?;
        Ok(Downloaded::Spooled(
            spool_response(response, max_bytes)
                .await
                .context("factoryon.go.kr file body failed")?,
        ))
    }
}
