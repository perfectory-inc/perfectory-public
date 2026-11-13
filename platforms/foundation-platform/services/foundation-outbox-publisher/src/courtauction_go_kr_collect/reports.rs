//! 법원경매 감정평가서 PDF, from the viewer the site's 감정평가 screen opens (root ADR-0540 §1, §3).
//!
//! Which reports is computed each run (root ADR-0352): the 감정평가 rounds the lakehouse holds
//! (`silver.court_auction_appraisal`) whose (court, case, round, number, written) has no stored
//! report (`silver.court_auction_appraisal_report`) and whose last read
//! (`silver.court_auction_appraisal_report_read`) is not a recent `gone`, newest written first, at
//! most the contract's per-run limit. A report is one GET of the viewer page, whose file key leads
//! to one GET of the PDF, stored once in the shared content-addressed store (`media_store`).
//!
//! The viewer sometimes asks for a reCAPTCHA. That report is skipped and recorded as `captcha`, the
//! asking waits the contract's next step and stops for the run after its bound in a row: a CAPTCHA
//! is never solved or bypassed. Each request is one Bronze line: its key, outcome and stored hash.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use chrono::{DateTime, NaiveDate, Utc};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ObjectKeyPrefix;
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::time::Instant;

use super::{may_ask, Asking, Search, Stop};
use crate::media_store::{store_bytes, Stored};

/// The contract's `appraisal_reports`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AppraisalReports {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) base_url: String,
    pub(crate) view_path: String,
    /// The court office code's prefix the viewer's path leaves out (`B000210` -> `000210`).
    pub(crate) court_code_prefix: String,
    pub(crate) file_key_pattern: String,
    pub(crate) captcha_marker: String,
    pub(crate) display_path: String,
    pub(crate) key_param: String,
    pub(crate) headers: std::collections::BTreeMap<String, String>,
    pub(crate) user_agent: String,
    pub(crate) read_from: String,
    pub(crate) reports_from: String,
    pub(crate) reads_from: String,
    pub(crate) object_prefix_from: String,
    pub(crate) max_file_bytes: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) per_run_limit: u32,
    pub(crate) concurrency: usize,
    pub(crate) run_time_budget_seconds: u64,
    pub(crate) captcha_waits_seconds: Vec<u64>,
    pub(crate) captcha_stop_after: u32,
}

impl AppraisalReports {
    /// The reports as the Bronze persist step names a search.
    pub(crate) fn as_search(&self) -> Search {
        Search {
            dataset_slug: self.dataset_slug.clone(),
            operation: self.operation.clone(),
            path: self.view_path.clone(),
            page_size: 0,
            pace_group: String::new(),
            headers: self.headers.clone(),
            conditions: Vec::new(),
            date_window: None,
        }
    }

    /// The viewer URL of one report, as the site's screen builds it (PGJ15BP03): each part of
    /// `view_path` one path segment, encoded as a browser encodes it.
    pub(crate) fn view_url(&self, report: &WantedReport) -> anyhow::Result<String> {
        let court = report
            .court_office_code
            .strip_prefix(self.court_code_prefix.as_str())
            .unwrap_or(&report.court_office_code);
        let round = report.round_seq.to_string();
        let written = report.written.format("%Y%m%d").to_string();
        let mut url = reqwest::Url::parse(&self.base_url)
            .context("appraisal_reports.base_url is not a URL")?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| anyhow::anyhow!("appraisal_reports.base_url cannot carry a path"))?;
            segments.clear();
            for part in self.view_path.trim_start_matches('/').split('/') {
                segments.push(match part {
                    "{court}" => court,
                    "{case}" => &report.case_no,
                    "{round}" => &round,
                    "{appraisal_no}" => &report.appraisal_no,
                    "{written}" => &written,
                    literal => literal,
                });
            }
        }
        Ok(url.to_string())
    }

    /// The file key a viewer page carries, or none (the page then asks for a CAPTCHA or names no
    /// report).
    pub(crate) fn file_key(&self, page: &str) -> anyhow::Result<Option<String>> {
        let pattern = regex::Regex::new(&self.file_key_pattern)
            .context("appraisal_reports.file_key_pattern is not a valid pattern")?;
        Ok(pattern
            .captures(page)
            .and_then(|found| found.get(1))
            .map(|key| key.as_str().to_owned()))
    }
}

/// One 감정평가 round whose report is wanted: the viewer's key.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct WantedReport {
    pub(crate) court_office_code: String,
    pub(crate) case_no: String,
    pub(crate) round_seq: i64,
    pub(crate) appraisal_no: String,
    pub(crate) written: NaiveDate,
}

fn text(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        JsonValue::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// A date column as the lakehouse scan gives it: `YYYY-MM-DD`, or days since 1970-01-01.
fn day(value: &JsonValue) -> Option<NaiveDate> {
    match value {
        JsonValue::Number(days) => days.as_i64().and_then(|days| {
            NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(chrono::Duration::days(days))
        }),
        JsonValue::String(text) => NaiveDate::parse_from_str(text.get(..10)?, "%Y-%m-%d").ok(),
        _ => None,
    }
}

fn report_of(row: &serde_json::Map<String, JsonValue>) -> Option<WantedReport> {
    Some(WantedReport {
        court_office_code: text(&row["court_office_code"])?,
        case_no: text(&row["case_no"])?,
        round_seq: row["round_seq"].as_i64()?,
        appraisal_no: text(&row["appraisal_no"])?,
        written: day(&row["written_date"])?,
    })
}

/// The reports this run asks: every round of `rounds` with a number and a written date, less the
/// stored ones and those whose last read was `gone` on or after `gone_since`, newest written first,
/// then by key; at most `limit`. The same inputs always give the same list (root ADR-0352 §1).
pub(crate) fn frontier<'a>(
    rounds: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
    stored: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
    reads: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
    gone_since: NaiveDate,
    limit: u32,
) -> Vec<WantedReport> {
    let stored: HashSet<WantedReport> = stored.into_iter().filter_map(report_of).collect();
    // The newest read of each report.
    let mut last: HashMap<WantedReport, (String, String, Option<NaiveDate>)> = HashMap::new();
    for row in reads {
        let (Some(report), Some(snapshot)) = (report_of(row), text(&row["source_snapshot_id"]))
        else {
            continue;
        };
        let outcome = text(&row["outcome"]).unwrap_or_default();
        let at = super::detail::run_day(&snapshot);
        match last.get(&report) {
            Some((seen, ..)) if *seen >= snapshot => {}
            _ => {
                last.insert(report, (snapshot, outcome, at));
            }
        }
    }
    let mut wanted: Vec<WantedReport> = rounds
        .into_iter()
        .filter_map(report_of)
        .collect::<HashSet<_>>()
        .into_iter()
        .filter(|report| !stored.contains(report))
        .filter(|report| {
            !last.get(report).is_some_and(|(_, outcome, at)| {
                outcome == "gone" && at.is_some_and(|at| at >= gone_since)
            })
        })
        .collect();
    wanted.sort_by(|a, b| {
        (
            b.written,
            &a.court_office_code,
            &a.case_no,
            a.round_seq,
            &a.appraisal_no,
        )
            .cmp(&(
                a.written,
                &b.court_office_code,
                &b.case_no,
                b.round_seq,
                &b.appraisal_no,
            ))
    });
    wanted.truncate(limit as usize);
    wanted
}

/// Reads the three tables the frontier needs from the lakehouse (their current snapshots; a table
/// with none yet holds no row).
pub(crate) async fn frontier_from_lakehouse(
    reports: &AppraisalReports,
    gone_since: NaiveDate,
) -> anyhow::Result<Vec<WantedReport>> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::{
        LakehouseTableContract, SILVER_COURT_AUCTION_APPRAISAL,
        SILVER_COURT_AUCTION_APPRAISAL_REPORT, SILVER_COURT_AUCTION_APPRAISAL_REPORT_READ,
    };
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    let tables: [(&str, &LakehouseTableContract); 3] = [
        (&reports.read_from, &SILVER_COURT_AUCTION_APPRAISAL),
        (
            &reports.reports_from,
            &SILVER_COURT_AUCTION_APPRAISAL_REPORT,
        ),
        (
            &reports.reads_from,
            &SILVER_COURT_AUCTION_APPRAISAL_REPORT_READ,
        ),
    ];
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let lakehouse = LakehouseObjectReader::from_env()?;
    let mut rows = Vec::new();
    for (named, contract) in tables {
        anyhow::ensure!(
            named == contract.table_name,
            "appraisal_reports names {named}, the collector reads {}",
            contract.table_name
        );
        let Some(snapshot) = catalog
            .load_current_snapshot_manifest_list(named)
            .await
            .with_context(|| format!("failed to resolve {named}"))?
        else {
            rows.push(Vec::new());
            continue;
        };
        rows.push(
            scan_snapshot_rows(contract, &lakehouse, &snapshot)
                .await?
                .rows,
        );
    }
    Ok(frontier(
        rows[0].iter(),
        rows[1].iter(),
        rows[2].iter(),
        gone_since,
        reports.per_run_limit,
    ))
}

/// What one report came to.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    /// Stored now, or already in the store (same bytes).
    Stored,
    /// The viewer asked for a reCAPTCHA: skipped, never solved.
    Captcha,
    /// The viewer page carried no file key and no CAPTCHA.
    NoKey,
    /// The viewer or the file answered 4xx: asking again will not bring it back soon.
    Gone,
    /// The file is not a PDF the store takes (over the cap, or not a known format).
    NotStored,
    /// A network failure or 5xx: asked again next run.
    Failed,
}

/// What the reports of one run came to.
#[derive(Debug, Default, serde::Serialize)]
pub(crate) struct ReportsResult {
    #[serde(skip)]
    pub(crate) lines: Vec<String>,
    pub(crate) asked: u32,
    pub(crate) stored: u32,
    pub(crate) deduplicated: u32,
    pub(crate) captcha: u32,
    pub(crate) failed: u32,
    pub(crate) bytes_stored: u64,
}

/// The run-wide state of the asking: the client, the store and the CAPTCHA tally.
pub(crate) struct ReportAsking<'a> {
    pub(crate) reports: &'a AppraisalReports,
    pub(crate) http: reqwest::Client,
    pub(crate) prefix: ObjectKeyPrefix,
    pub(crate) storage: Option<&'a dyn ObjectStorageService>,
    pub(crate) captchas_in_a_row: AtomicU32,
}

enum Fetched {
    Line {
        line: String,
        outcome: Outcome,
        stored: Option<Stored>,
    },
    Stopped(Stop),
}

async fn get(asking: &ReportAsking<'_>, url: &str) -> Result<reqwest::Response, String> {
    let mut request = asking.http.get(url);
    for (name, value) in &asking.reports.headers {
        request = request.header(name, value);
    }
    request
        .send()
        .await
        .map_err(|error| format!("GET failed: {error}"))
}

/// One report: the viewer page, then the file. Never asks the CAPTCHA endpoint.
async fn fetch_one(
    asking: &ReportAsking<'_>,
    report: &WantedReport,
) -> anyhow::Result<(Outcome, Option<Stored>, JsonValue)> {
    let reports = asking.reports;
    let view = reports.view_url(report)?;
    let mut facts = json!({"view_url": view});
    let page = match get(asking, &view).await {
        Ok(response) if response.status().is_client_error() => {
            facts["http_status"] = json!(response.status().as_u16());
            return Ok((Outcome::Gone, None, facts));
        }
        Ok(response) if !response.status().is_success() => {
            facts["http_status"] = json!(response.status().as_u16());
            return Ok((Outcome::Failed, None, facts));
        }
        Ok(response) => {
            facts["http_status"] = json!(response.status().as_u16());
            match response.text().await {
                Ok(page) => page,
                Err(error) => {
                    facts["failure"] = json!(format!("reading the viewer page failed: {error}"));
                    return Ok((Outcome::Failed, None, facts));
                }
            }
        }
        Err(reason) => {
            facts["failure"] = json!(reason);
            return Ok((Outcome::Failed, None, facts));
        }
    };
    let Some(key) = reports.file_key(&page)? else {
        let outcome = if page.contains(&reports.captcha_marker) {
            Outcome::Captcha
        } else {
            Outcome::NoKey
        };
        return Ok((outcome, None, facts));
    };
    let file = format!(
        "{}{}?{}={}",
        reports.base_url.trim_end_matches('/'),
        reports.display_path,
        reports.key_param,
        key
    );
    let response = match get(asking, &file).await {
        Ok(response) => response,
        Err(reason) => {
            facts["failure"] = json!(reason);
            return Ok((Outcome::Failed, None, facts));
        }
    };
    facts["file_http_status"] = json!(response.status().as_u16());
    if response.status().is_client_error() {
        return Ok((Outcome::Gone, None, facts));
    }
    if !response.status().is_success() {
        return Ok((Outcome::Failed, None, facts));
    }
    // An over-cap file is refused by its declared length before its body is read.
    if response
        .content_length()
        .is_some_and(|len| len > reports.max_file_bytes)
    {
        return Ok((Outcome::NotStored, Some(Stored::Oversize), facts));
    }
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            facts["failure"] = json!(format!("reading the file failed: {error}"));
            return Ok((Outcome::Failed, None, facts));
        }
    };
    // The file answer can be the viewer's CAPTCHA page too: HTML is never a report.
    if String::from_utf8_lossy(&bytes[..bytes.len().min(4096)]).contains(&reports.captcha_marker) {
        return Ok((Outcome::Captcha, None, facts));
    }
    let Some(storage) = asking.storage else {
        let sniffed = foundation_shared_kernel::MediaFormat::sniff(&bytes);
        facts["media_format"] = json!(sniffed.map(|format| format.extension()));
        facts["size_bytes"] = json!(bytes.len());
        return Ok((
            if sniffed.is_some() {
                Outcome::Stored
            } else {
                Outcome::NotStored
            },
            None,
            facts,
        ));
    };
    let stored = store_bytes(&bytes, &asking.prefix, storage, reports.max_file_bytes).await?;
    let outcome = match &stored {
        Stored::New(_) | Stored::Deduplicated(_) => Outcome::Stored,
        Stored::Gone => Outcome::Gone,
        Stored::Oversize | Stored::UnknownFormat => Outcome::NotStored,
    };
    Ok((outcome, Some(stored), facts))
}

/// Asks one report unless the run must stop, and waits the contract's step after a CAPTCHA.
async fn read_one(
    asking: &ReportAsking<'_>,
    run: &Asking<'_>,
    deadline: Instant,
    report: &WantedReport,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
) -> anyhow::Result<Fetched> {
    let reports = asking.reports;
    if Instant::now() >= deadline {
        return Ok(Fetched::Stopped(Stop::TimeBudget));
    }
    if asking.captchas_in_a_row.load(Ordering::SeqCst) >= reports.captcha_stop_after {
        return Ok(Fetched::Stopped(Stop::TimeBudget));
    }
    let at = now();
    if !may_ask(&run.switches, at)? {
        return Ok(Fetched::Stopped(Stop::WindowEnded));
    }
    let (outcome, stored, facts) = fetch_one(asking, report).await?;
    if outcome == Outcome::Captcha {
        let in_a_row = asking.captchas_in_a_row.fetch_add(1, Ordering::SeqCst) + 1;
        let step = reports
            .captcha_waits_seconds
            .get(in_a_row.saturating_sub(1) as usize)
            .or(reports.captcha_waits_seconds.last())
            .copied()
            .unwrap_or(0);
        tracing::warn!(
            court = %report.court_office_code, case = %report.case_no, in_a_row,
            "courtauction.go.kr appraisal report: the viewer asked for a CAPTCHA; skipped, waiting {step} s"
        );
        let wake = Instant::now() + Duration::from_secs(step);
        tokio::time::sleep_until(wake.min(deadline)).await;
    } else {
        asking.captchas_in_a_row.store(0, Ordering::SeqCst);
    }
    let media = match &stored {
        Some(Stored::New(media) | Stored::Deduplicated(media)) => Some(media),
        _ => None,
    };
    let line = json!({
        "operation": reports.operation,
        "request": {
            "court_office_code": report.court_office_code,
            "case_no": report.case_no,
            "round_seq": report.round_seq,
            "appraisal_no": report.appraisal_no,
            "written_date": report.written.format("%Y%m%d").to_string(),
        },
        "requested_at_utc": at.to_rfc3339(),
        "outcome": outcome,
        "sha256": media.map(|media| media.sha256.clone()),
        "media_format": media.map(|media| media.media_format.clone()),
        "size_bytes": media.map(|media| media.size_bytes),
        "facts": facts,
    });
    Ok(Fetched::Line {
        line: serde_json::to_string(&line)?,
        outcome,
        stored,
    })
}

/// Asks `wanted` until `deadline`, `concurrency` at a time, and keeps the lines in the order asked.
pub(crate) async fn collect_reports(
    asking: &ReportAsking<'_>,
    run: &Asking<'_>,
    deadline: Instant,
    wanted: &[WantedReport],
    now: impl Fn() -> DateTime<Utc> + Sync,
) -> anyhow::Result<(ReportsResult, Option<Stop>)> {
    // Built here, run only as the stream polls them (as `detail::collect_details` notes).
    let reads: Vec<_> = wanted
        .iter()
        .map(|report| read_one(asking, run, deadline, report, &now))
        .collect();
    let mut answers = stream::iter(reads).buffered(asking.reports.concurrency.max(1));
    let mut result = ReportsResult::default();
    let mut stop = None;
    while let Some(read) = answers.next().await {
        match read? {
            Fetched::Stopped(why) => {
                stop.get_or_insert(why);
            }
            Fetched::Line {
                line,
                outcome,
                stored,
            } => {
                result.asked += 1;
                match (&outcome, &stored) {
                    (Outcome::Stored, Some(Stored::New(media))) => {
                        result.stored += 1;
                        result.bytes_stored += media.size_bytes;
                    }
                    (Outcome::Stored, Some(Stored::Deduplicated(_))) => result.deduplicated += 1,
                    (Outcome::Captcha, _) => result.captcha += 1,
                    (Outcome::Failed, _) => result.failed += 1,
                    _ => {}
                }
                result.lines.push(line);
            }
        }
    }
    Ok((result, stop))
}

/// The client the reports are asked with: our own name, the contract's timeout.
pub(crate) fn http_client(reports: &AppraisalReports) -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(reports.user_agent.clone())
        .timeout(Duration::from_secs(reports.request_timeout_seconds))
        .build()
        .context("failed to build the kapanet HTTP client")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    fn contract() -> super::super::SourceContract {
        super::super::source_contract().expect("the contract parses")
    }

    fn report() -> WantedReport {
        WantedReport {
            court_office_code: "B999999".into(),
            case_no: "20990130000001".into(),
            round_seq: 1,
            appraisal_no: "합성-01".into(),
            written: NaiveDate::from_ymd_opt(2099, 1, 12).unwrap(),
        }
    }

    fn row(value: JsonValue) -> serde_json::Map<String, JsonValue> {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn the_viewer_url_is_the_sites_and_the_file_key_is_read_from_its_page() {
        let contract = contract();
        let reports = &contract.appraisal_reports;
        assert_eq!(
            reports.view_url(&report()).unwrap(),
            "https://ca.kapanet.or.kr/view/999999/20990130000001/1/%ED%95%A9%EC%84%B1-01/20990112"
        );
        let page = r#"<iframe id="pdfFrame" src="/auction/display?key=5a7739f9-e45c-4004-8fa2-d4c8c438f394#toolbar=1"></iframe>"#;
        assert_eq!(
            reports.file_key(page).unwrap().as_deref(),
            Some("5a7739f9-e45c-4004-8fa2-d4c8c438f394")
        );
        let captcha = r#"<div id="captcha_section"><div id="html_element"></div></div><iframe src="/auction/display?key="></iframe>"#;
        assert_eq!(
            reports.file_key(captcha).unwrap(),
            None,
            "no key: the CAPTCHA"
        );
        assert!(captcha.contains(&reports.captcha_marker));
        assert!(reports.captcha_stop_after >= 1 && !reports.captcha_waits_seconds.is_empty());
    }

    #[test]
    fn the_frontier_skips_stored_and_recently_gone_reports_newest_first() {
        let round = |case: &str, written: &str| {
            row(
                json!({"court_office_code": "B999999", "case_no": case, "round_seq": 1,
                       "appraisal_no": "합성-01", "written_date": written}),
            )
        };
        let rounds = [
            round("OLD", "2099-01-01"),
            round("NEW", "2099-01-20"),
            round("STORED", "2099-01-15"),
            round("GONE", "2099-01-16"),
            round("CAPTCHA", "2099-01-10"),
            row(
                json!({"court_office_code": "B999999", "case_no": "NO-NUMBER", "round_seq": 1,
                       "written_date": "2099-01-10"}),
            ),
        ];
        let stored = [round("STORED", "2099-01-15")];
        let read = |case: &str, outcome: &str| {
            let mut r = round(
                case,
                if case == "GONE" {
                    "2099-01-16"
                } else {
                    "2099-01-10"
                },
            );
            r.insert("outcome".into(), json!(outcome));
            r.insert("source_snapshot_id".into(), json!("20990110T160500Z"));
            r
        };
        let reads = [read("GONE", "gone"), read("CAPTCHA", "captcha")];
        let since = NaiveDate::from_ymd_opt(2099, 1, 1).unwrap();
        let asked: Vec<String> = frontier(rounds.iter(), stored.iter(), reads.iter(), since, 10)
            .into_iter()
            .map(|r| r.case_no)
            .collect();
        assert_eq!(asked, ["NEW", "CAPTCHA", "OLD"], "a CAPTCHA is asked again");
        let later = NaiveDate::from_ymd_opt(2099, 3, 1).unwrap();
        assert!(
            frontier(rounds.iter(), stored.iter(), reads.iter(), later, 10)
                .iter()
                .any(|r| r.case_no == "GONE"),
            "a gone report is asked again after the backstop"
        );
        assert_eq!(
            frontier(rounds.iter(), stored.iter(), reads.iter(), since, 1).len(),
            1
        );
    }

    #[test]
    fn a_date_reads_from_text_or_days() {
        assert_eq!(
            day(&json!("2099-01-12")),
            NaiveDate::from_ymd_opt(2099, 1, 12)
        );
        assert_eq!(day(&json!(0)), NaiveDate::from_ymd_opt(1970, 1, 1));
    }
}
