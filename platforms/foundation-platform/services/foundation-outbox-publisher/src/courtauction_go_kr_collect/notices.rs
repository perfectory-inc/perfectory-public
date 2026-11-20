//! 법원경매 매각공고, every court's of this month and the next, asked each run as the lists are
//! (root ADR-0575 §6).
//!
//! The site answers a 매각공고 from today on only (measured 2026-11-19: past months answer none), so
//! each run reads the courts (`courts_path`), then each court's notice list for each bid kind and
//! month, then each listed notice's detail: its properties, its 정정공고 and its 취소·변경공고. A
//! notice list is small (one court, one month: a few sale dates), so the whole of it is read each
//! run as the item search is; the Silver load appends only what changed. Each request is one Bronze
//! line with the answer as received. The notices name no person (measured: 매각공고 목록·상세).

use std::collections::BTreeMap;

use anyhow::Context as _;
use chrono::{DateTime, Datelike as _, Months, NaiveDate, Utc};
use collection_application::{ask, Asked, PaceGate, SiteMethod, SiteRequest};
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::time::Instant;

use super::{may_ask, Asking, Search, SourceContract, Stop};

/// The contract's `sale_notices`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SaleNotices {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) pace_group: String,
    pub(crate) courts_path: String,
    pub(crate) courts_operation: String,
    pub(crate) courts_pointer: String,
    pub(crate) court_field: String,
    pub(crate) list_path: String,
    pub(crate) list_headers: BTreeMap<String, String>,
    pub(crate) list_request_object: String,
    pub(crate) month_field: String,
    pub(crate) bid_kind_field: String,
    pub(crate) bid_kinds: Vec<String>,
    pub(crate) list_constants: BTreeMap<String, String>,
    pub(crate) rows_pointer: String,
    pub(crate) months_ahead: u32,
    pub(crate) detail_operation: String,
    pub(crate) detail_path: String,
    pub(crate) detail_headers: BTreeMap<String, String>,
    pub(crate) detail_request_object: String,
    /// The list row's fields the detail is asked with, as the screen sends the row back.
    pub(crate) detail_fields: Vec<String>,
    pub(crate) concurrency: usize,
    pub(crate) run_time_budget_seconds: u64,
}

impl SaleNotices {
    /// The notices as the Bronze persist step names a search.
    pub(crate) fn as_search(&self) -> Search {
        Search {
            dataset_slug: self.dataset_slug.clone(),
            operation: self.operation.clone(),
            path: self.list_path.clone(),
            page_size: 0,
            pace_group: self.pace_group.clone(),
            headers: self.list_headers.clone(),
            conditions: Vec::new(),
            date_window: None,
        }
    }

    /// The months a run asks (`YYYYMM`): this month and the next `months_ahead`.
    pub(crate) fn months(&self, today: NaiveDate) -> Vec<String> {
        let first = today.with_day(1).unwrap_or(today);
        (0..=self.months_ahead)
            .filter_map(|ahead| first.checked_add_months(Months::new(ahead)))
            .map(|month| format!("{:04}{:02}", month.year(), month.month()))
            .collect()
    }

    /// A notice list's request body.
    pub(crate) fn list_body(&self, court: &str, bid_kind: &str, month: &str) -> JsonValue {
        let mut object = serde_json::Map::new();
        object.insert(self.court_field.clone(), json!(court));
        object.insert(self.bid_kind_field.clone(), json!(bid_kind));
        object.insert(self.month_field.clone(), json!(month));
        for (field, value) in &self.list_constants {
            object.insert(field.clone(), json!(value));
        }
        json!({ self.list_request_object.as_str(): object })
    }

    /// A listed notice's detail request: the list row's fields, sent back as the screen sends them.
    pub(crate) fn detail_body(&self, row: &JsonValue) -> JsonValue {
        let object: serde_json::Map<String, JsonValue> = self
            .detail_fields
            .iter()
            .map(|field| {
                (
                    field.clone(),
                    row.get(field).cloned().unwrap_or(JsonValue::Null),
                )
            })
            .collect();
        json!({ self.detail_request_object.as_str(): object })
    }
}

/// What the notices of one run came to.
#[derive(Debug, Default, serde::Serialize)]
pub(crate) struct NoticesResult {
    #[serde(skip)]
    pub(crate) lines: Vec<String>,
    pub(crate) courts: usize,
    /// The court office codes the court list named, for the 배당요구종기공고 sweep (root ADR-0575 §6).
    #[serde(skip)]
    pub(crate) court_codes: Vec<String>,
    pub(crate) lists: u32,
    pub(crate) notices: u32,
    pub(crate) failed: u32,
}

/// One request's line and, when it was read, its answer.
pub(super) struct Answered {
    pub(super) line: String,
    pub(super) answer: Option<JsonValue>,
}

pub(super) enum Sent {
    Answered(Answered),
    Stopped(Stop),
}

/// The asking one request shares with the run.
pub(super) struct Ask<'a, 'b> {
    pub(super) asking: &'a Asking<'b>,
    pub(super) gate: &'a PaceGate,
    pub(super) deadline: Instant,
    pub(super) session_headers: &'a BTreeMap<String, String>,
}

pub(super) async fn send(
    on: &Ask<'_, '_>,
    operation: &str,
    path: &str,
    headers: &BTreeMap<String, String>,
    body: JsonValue,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
) -> anyhow::Result<Sent> {
    let at = now();
    if !may_ask(&on.asking.switches, at)? {
        return Ok(Sent::Stopped(Stop::WindowEnded));
    }
    let request = SiteRequest {
        method: SiteMethod::Post,
        path: path.to_owned(),
        headers: on
            .session_headers
            .iter()
            .chain(headers)
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        body: Some(body.clone()),
    };
    let asked = ask(
        on.asking.sessions,
        on.gate,
        &on.asking.rules,
        &on.asking.tally,
        &request,
        on.deadline,
    )
    .await;
    let mut line = json!({
        "operation": operation,
        "request": body,
        "requested_at_utc": at.to_rfc3339(),
    });
    let answer = match asked {
        Asked::Unfinished => return Ok(Sent::Stopped(Stop::TimeBudget)),
        Asked::Data { reply, .. } => {
            line["http_status"] = json!(reply.status);
            line["read_outcome"] = json!("read");
            let answer = serde_json::from_str::<JsonValue>(&reply.body).ok();
            line["body"] = json!(reply.body);
            answer
        }
        Asked::Failed { reason, reply } => {
            line["http_status"] = json!(reply.as_ref().map(|reply| reply.status));
            line["read_outcome"] = json!("failed");
            line["failure"] = json!(reason);
            line["body"] = json!(reply.map(|reply| reply.body));
            None
        }
    };
    Ok(Sent::Answered(Answered {
        line: serde_json::to_string(&line)?,
        answer,
    }))
}

/// The rows an answer lists at `pointer`, none when it lists none.
pub(super) fn rows_at(answer: Option<&JsonValue>, pointer: &str) -> Vec<JsonValue> {
    answer
        .and_then(|answer| answer.pointer(pointer))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Reads the courts, each court's notice lists for the run's months and bid kinds, then every
/// listed notice's detail, `concurrency` at a time, until `deadline`. A failed request is a line
/// like any other; it never fails the run.
pub(crate) async fn collect_notices(
    contract: &SourceContract,
    asking: &Asking<'_>,
    deadline: Instant,
    today: NaiveDate,
    now: impl Fn() -> DateTime<Utc> + Sync,
) -> anyhow::Result<(NoticesResult, Option<Stop>)> {
    let notices = &contract.sale_notices;
    let gate = asking
        .gates
        .get(&notices.pace_group)
        .with_context(|| format!("no collection.pace group {:?}", notices.pace_group))?;
    let on = Ask {
        asking,
        gate,
        deadline,
        session_headers: &contract.collection.session.headers,
    };
    let mut result = NoticesResult::default();
    let courts = match send(
        &on,
        &notices.courts_operation,
        &notices.courts_path,
        &BTreeMap::new(),
        json!({}),
        &now,
    )
    .await?
    {
        Sent::Stopped(why) => return Ok((result, Some(why))),
        Sent::Answered(answered) => {
            result.failed += u32::from(answered.answer.is_none());
            result.lines.push(answered.line);
            rows_at(answered.answer.as_ref(), &notices.courts_pointer)
        }
    };
    let courts: Vec<String> = courts
        .iter()
        .filter_map(|court| court.get(&notices.court_field)?.as_str().map(str::to_owned))
        .collect();
    result.courts = courts.len();
    result.court_codes.clone_from(&courts);
    let mut lists = Vec::new();
    for court in &courts {
        for bid_kind in &notices.bid_kinds {
            for month in notices.months(today) {
                lists.push(notices.list_body(court, bid_kind, &month));
            }
        }
    }
    let asked: Vec<_> = lists
        .into_iter()
        .map(|body| {
            send(
                &on,
                &notices.operation,
                &notices.list_path,
                &notices.list_headers,
                body,
                &now,
            )
        })
        .collect();
    let mut answers = stream::iter(asked).buffered(notices.concurrency.max(1));
    let mut listed = Vec::new();
    let mut stop = None;
    while let Some(sent) = answers.next().await {
        match sent? {
            Sent::Stopped(why) => {
                stop.get_or_insert(why);
            }
            Sent::Answered(answered) => {
                result.lists += 1;
                result.failed += u32::from(answered.answer.is_none());
                listed.extend(rows_at(answered.answer.as_ref(), &notices.rows_pointer));
                result.lines.push(answered.line);
            }
        }
    }
    drop(answers);
    if stop.is_some() {
        return Ok((result, stop));
    }
    let asked: Vec<_> = listed
        .iter()
        .map(|row| {
            send(
                &on,
                &notices.detail_operation,
                &notices.detail_path,
                &notices.detail_headers,
                notices.detail_body(row),
                &now,
            )
        })
        .collect();
    let mut answers = stream::iter(asked).buffered(notices.concurrency.max(1));
    while let Some(sent) = answers.next().await {
        match sent? {
            Sent::Stopped(why) => {
                stop.get_or_insert(why);
            }
            Sent::Answered(answered) => {
                result.notices += 1;
                result.failed += u32::from(answered.answer.is_none());
                result.lines.push(answered.line);
            }
        }
    }
    Ok((result, stop))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    fn contract() -> SourceContract {
        super::super::source_contract().expect("the contract parses")
    }

    #[test]
    fn a_run_asks_this_month_and_the_next() {
        let contract = contract();
        let notices = &contract.sale_notices;
        let today = NaiveDate::from_ymd_opt(2099, 12, 31).unwrap();
        assert_eq!(notices.months(today), ["209912", "210001"]);
    }

    #[test]
    fn the_list_and_the_detail_are_asked_as_the_screen_asks_them() {
        let contract = contract();
        let notices = &contract.sale_notices;
        // The shapes PGJ143M01 and PGJ143M02 send, measured 2026-11-19.
        assert_eq!(
            notices.list_body("B999999", "000331", "209911"),
            json!({"dma_srchDspslPbanc": {"cortOfcCd": "B999999", "bidDvsCd": "000331",
                   "srchYmd": "209911", "srchBtnYn": "Y"}})
        );
        let row = json!({"cortOfcCd": "B999999", "dspslDxdyYmd": "20991119", "jdbnCd": "1002",
                         "cortAuctnJdbnNm": "경매2계", "bidDvsCd": "000331", "fstDspslHm": "1000",
                         "corCnt": 1, "dspslRealId": "B999999_1002_20991119"});
        let body = notices.detail_body(&row);
        let detail = &body["dma_srchGnrlPbanc"];
        assert_eq!(detail["jdbnCd"], json!("1002"));
        assert_eq!(detail["bidBgngYmd"], JsonValue::Null);
        assert!(detail.get("corCnt").is_none(), "only the screen's fields");
    }
}
