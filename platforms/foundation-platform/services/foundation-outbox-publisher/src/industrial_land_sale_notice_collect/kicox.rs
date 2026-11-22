//! 한국산업단지공단 임대/분양 게시판 (`boardList/1003`): ten posts a page, newest first, and one
//! page per post that carries its title, date, body and attachments (root ADR-0603 §1). The pages
//! are HTML; `scraper` (Servo's `html5ever` and `selectors`) finds the rows and the attachment
//! buttons, and the ids are read from the `onclick` calls the page itself makes
//! (`bbsArticleDet('<post>')`, `fnFileDownload('<file>', '<id>')`). Nothing else of a page is read
//! here: the page is kept whole in Bronze.

use std::time::Duration;

use anyhow::{bail, ensure, Context};
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use super::{Detail, DetailRead, Downloaded, FileRef, FileRequest, ListPage, Listed};

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct List {
    pub(crate) path: String,
    pub(crate) query: std::collections::BTreeMap<String, String>,
    pub(crate) page_field: String,
    pub(crate) page_rows: u64,
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
    pub(crate) list: List,
    pub(crate) detail: DetailRule,
    pub(crate) attachments: Files,
}

pub(crate) struct Kicox {
    http: reqwest::Client,
    rules: Rules,
}

/// The page shapes this reads. Built for each page read and dropped with it: the parsed document
/// and its selectors never live across a request, so the run's future stays `Send`.
pub(crate) struct Patterns {
    row: Selector,
    row_link: Selector,
    row_date: Selector,
    row_views: Selector,
    row_file_icon: Selector,
    total: Selector,
    article_title: Selector,
    file_item: Selector,
    file_title: Selector,
    file_button: Selector,
    post_call: Regex,
    file_call: Regex,
}

impl Patterns {
    pub(crate) fn new() -> anyhow::Result<Self> {
        let css = |text: &str| {
            Selector::parse(text).map_err(|error| anyhow::anyhow!("selector {text:?}: {error:?}"))
        };
        Ok(Self {
            row: css("table.board tbody tr")?,
            row_link: css("td.cont a")?,
            row_date: css("td.b_date")?,
            row_views: css(r#"td[data-cell-header="조회"]"#)?,
            row_file_icon: css("img[alt]")?,
            total: css("p.totalBoard span.num")?,
            article_title: css("div.thead p.tit")?,
            file_item: css("div.fileDwn li")?,
            file_title: css("p.tit")?,
            file_button: css("button[onclick]")?,
            post_call: Regex::new(r"bbsArticleDet\(\s*'([0-9]+)'\s*\)")?,
            file_call: Regex::new(r"fnFileDownload\(\s*'([0-9]+)'\s*,\s*'([0-9A-Za-z-]+)'\s*\)")?,
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

/// The posts of one list page, in the page's order (newest first), and the count the page states.
pub(crate) fn list_page(patterns: &Patterns, rules: &List, page_no: u32, html: &str) -> ListPage {
    let document = Html::parse_document(html);
    let rows = document
        .select(&patterns.row)
        .filter_map(|row| {
            let link = row.select(&patterns.row_link).next()?;
            let id = patterns
                .post_call
                .captures(link.value().attr("onclick")?)?
                .get(1)?
                .as_str()
                .to_owned();
            let has_attachment = row
                .select(&patterns.row_file_icon)
                .any(|icon| icon.value().attr("alt") == Some("첨부파일"));
            Some(Listed {
                row: json!({
                    "bbs_seq": id,
                    "title": folded(link),
                    "registered_on": row.select(&patterns.row_date).next().map(folded),
                    "has_attachment": has_attachment,
                    "views": row.select(&patterns.row_views).next().map(folded),
                }),
                id,
            })
        })
        .collect::<Vec<_>>();
    let total = document
        .select(&patterns.total)
        .next()
        .map(folded)
        .and_then(|text| text.replace(',', "").trim().parse::<u64>().ok());
    let last =
        rows.is_empty() || total.is_some_and(|total| u64::from(page_no) * rules.page_rows >= total);
    ListPage { rows, total, last }
}

/// The attachments one post page lists, in its order.
pub(crate) fn files_of(
    patterns: &Patterns,
    rules: &Rules,
    html: &str,
) -> anyhow::Result<Vec<FileRef>> {
    let document = Html::parse_document(html);
    ensure!(
        document.select(&patterns.article_title).next().is_some(),
        "the page holds no post"
    );
    let base = rules.base_url.trim_end_matches('/');
    let path = rules.attachments.download_path.trim_matches('/');
    Ok(document
        .select(&patterns.file_item)
        .map(|item| {
            let call = item
                .select(&patterns.file_button)
                .filter_map(|button| button.value().attr("onclick"))
                .find_map(|onclick| patterns.file_call.captures(onclick));
            let reference = call.and_then(|call| {
                Some((
                    call.get(1)?.as_str().to_owned(),
                    call.get(2)?.as_str().to_owned(),
                ))
            });
            let url = reference
                .as_ref()
                .map(|(file, id)| format!("{base}/{path}/{file}/{id}"));
            FileRef {
                title: item.select(&patterns.file_title).next().map(folded),
                provider_file_id: reference.map(|(file, id)| format!("{file}/{id}")),
                provider_file_size: None,
                request: url.clone().map(FileRequest::Url),
                source_url: url,
                withdrawn: false,
            }
        })
        .collect())
}

impl Kicox {
    pub(crate) fn new(json: &JsonValue) -> anyhow::Result<Self> {
        let rules: Rules = serde_json::from_value(json.clone())
            .context("the kicox source of the lane contract does not parse")?;
        let http = reqwest::Client::builder()
            .user_agent(super::USER_AGENT)
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()
            .context("failed to build the KICOX HTTP client")?;
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

    /// One page's text, or the 4xx the site answered with.
    async fn page_text(
        &self,
        url: &str,
        query: &[(String, String)],
    ) -> anyhow::Result<Result<String, u16>> {
        let response = self
            .http
            .get(url)
            .query(query)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("kicox.or.kr request failed: {error}"))?;
        let status = response.status();
        if status.is_client_error() {
            return Ok(Err(status.as_u16()));
        }
        if !status.is_success() {
            bail!("kicox.or.kr answered HTTP {status}");
        }
        let text = response
            .text()
            .await
            .map_err(|error| anyhow::anyhow!("kicox.or.kr body failed: {error}"))?;
        Ok(Ok(text))
    }

    pub(crate) async fn page(&mut self, page_no: u32) -> anyhow::Result<ListPage> {
        let mut query = self
            .rules
            .list
            .query
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        query.push((self.rules.list.page_field.clone(), page_no.to_string()));
        let html = self
            .page_text(&self.url(&self.rules.list.path), &query)
            .await?
            .map_err(|status| {
                anyhow::anyhow!("kicox.or.kr answered HTTP {status} for list page {page_no}")
            })?;
        Ok(list_page(
            &Patterns::new()?,
            &self.rules.list,
            page_no,
            &html,
        ))
    }

    /// One post's page; a 4xx is [`DetailRead::Gone`]. A page without a post (the site's own error
    /// page) is a failed read, asked again later: it may be the site, not the post.
    pub(crate) async fn detail(&mut self, listed: &Listed) -> anyhow::Result<DetailRead> {
        let mut query = self
            .rules
            .list
            .query
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        query.push((self.rules.detail.id_field.clone(), listed.id.clone()));
        let html = match self
            .page_text(&self.url(&self.rules.detail.path), &query)
            .await?
        {
            Ok(html) => html,
            Err(status) => return Ok(DetailRead::Gone(status)),
        };
        let files = files_of(&Patterns::new()?, &self.rules, &html)?;
        Ok(DetailRead::Found(Detail {
            http_status: Some(200),
            body: json!({ "html": html }),
            files,
        }))
    }

    pub(crate) async fn download(&mut self, request: &FileRequest) -> anyhow::Result<Downloaded> {
        let FileRequest::Url(url) = request else {
            bail!("a KICOX attachment is asked by its URL");
        };
        ensure!(
            url.starts_with(&format!("{}/", self.rules.base_url.trim_end_matches('/'))),
            "{url} is not on the board's host"
        );
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("kicox.or.kr download failed: {error}"))?;
        let status = response.status();
        if status.is_client_error() {
            return Ok(Downloaded::Gone(status.as_u16()));
        }
        if !status.is_success() {
            bail!("kicox.or.kr answered HTTP {status} for a file");
        }
        Ok(Downloaded::Bytes(
            response
                .bytes()
                .await
                .map_err(|error| anyhow::anyhow!("kicox.or.kr file body failed: {error}"))?
                .to_vec(),
        ))
    }
}
