//! 법원경매 배당요구종기공고, every court department's, asked each run after the 매각공고 (root
//! ADR-0575 §6).
//!
//! Each run reads each court's departments (`depts_path`), then each department's notice list (a
//! case per row, from the 개시결정 to the 배당요구종기; measured 2026-11-19: 32 rows in one
//! department), whole as the lists are. A listed case's 물건 list (`objects_path`: each property's
//! address and its 등기 고유번호) is asked once: only for the cases the lakehouse's object table does
//! not hold yet, computed each run (root ADR-0352). The list names its creditors, debtors and owners
//! unmasked: those fields stay in Bronze; the Silver load never reads them (root ADR-0575 §6).

use std::collections::{BTreeMap, HashSet};

use anyhow::Context as _;
use chrono::{DateTime, Utc};
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::time::Instant;

use super::notices::{rows_at, send, Ask, Sent};
use super::{Asking, Search, SourceContract, Stop};

/// The contract's `dividend_notices`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DividendNotices {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) pace_group: String,
    pub(crate) depts_operation: String,
    pub(crate) depts_path: String,
    pub(crate) depts_court_field: String,
    pub(crate) depts_pointer: String,
    pub(crate) dept_code_field: String,
    pub(crate) list_path: String,
    pub(crate) list_headers: BTreeMap<String, String>,
    pub(crate) list_request_object: String,
    pub(crate) list_court_field: String,
    pub(crate) list_dept_field: String,
    pub(crate) list_constants: BTreeMap<String, String>,
    pub(crate) rows_pointer: String,
    pub(crate) objects_operation: String,
    pub(crate) objects_path: String,
    pub(crate) objects_headers: BTreeMap<String, String>,
    pub(crate) objects_request_object: String,
    /// The list row's fields the 물건 list is asked with.
    pub(crate) objects_fields: Vec<String>,
    /// The list row's court and case fields (the case a row names).
    pub(crate) case_fields: Vec<String>,
    pub(crate) objects_from: String,
    pub(crate) concurrency: usize,
    pub(crate) run_time_budget_seconds: u64,
}

impl DividendNotices {
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

    /// A department's notice list request.
    pub(crate) fn list_body(&self, court: &str, dept: &str) -> JsonValue {
        let mut object = serde_json::Map::new();
        for (field, value) in &self.list_constants {
            object.insert(field.clone(), json!(value));
        }
        object.insert(self.list_court_field.clone(), json!(court));
        object.insert(self.list_dept_field.clone(), json!(dept));
        json!({ self.list_request_object.as_str(): object })
    }

    /// A listed case's 물건 list request: the row's fields, as the screen sends them.
    pub(crate) fn objects_body(&self, row: &JsonValue) -> JsonValue {
        let object: serde_json::Map<String, JsonValue> = self
            .objects_fields
            .iter()
            .map(|field| {
                (
                    field.clone(),
                    row.get(field).cloned().unwrap_or(JsonValue::Null),
                )
            })
            .collect();
        json!({ self.objects_request_object.as_str(): object })
    }

    /// The (court, case) a list row names, when it names both.
    pub(crate) fn case_of(&self, row: &JsonValue) -> Option<(String, String)> {
        let [court, case] = self.case_fields.as_slice() else {
            return None;
        };
        Some((
            row.get(court)?.as_str()?.to_owned(),
            row.get(case)?.as_str()?.to_owned(),
        ))
    }
}

/// What the 배당요구종기공고 of one run came to.
#[derive(Debug, Default, serde::Serialize)]
pub(crate) struct DividendsResult {
    #[serde(skip)]
    pub(crate) lines: Vec<String>,
    pub(crate) departments: u32,
    pub(crate) lists: u32,
    pub(crate) listed_cases: usize,
    pub(crate) object_lists: u32,
    pub(crate) failed: u32,
}

/// The cases the lakehouse's object table holds (its current snapshot; none yet holds none).
pub(crate) async fn known_from_lakehouse(
    notices: &DividendNotices,
) -> anyhow::Result<HashSet<(String, String)>> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::SILVER_COURT_AUCTION_DIVIDEND_OBJECT as OBJECTS;
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    anyhow::ensure!(
        notices.objects_from == OBJECTS.table_name,
        "dividend_notices names {}, the collector reads {}",
        notices.objects_from,
        OBJECTS.table_name
    );
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let Some(snapshot) = catalog
        .load_current_snapshot_manifest_list(&notices.objects_from)
        .await
        .with_context(|| format!("failed to resolve {}", notices.objects_from))?
    else {
        return Ok(HashSet::new());
    };
    let lakehouse = LakehouseObjectReader::from_env()?;
    Ok(scan_snapshot_rows(&OBJECTS, &lakehouse, &snapshot)
        .await?
        .rows
        .iter()
        .filter_map(|row| {
            Some((
                row.get("court_office_code")?.as_str()?.to_owned(),
                row.get("case_no")?.as_str()?.to_owned(),
            ))
        })
        .collect())
}

/// Reads every court's departments, each department's notice list, then the 물건 list of each listed
/// case `known` does not hold, `concurrency` at a time, until `deadline`. `known` is `None` when the
/// lakehouse could not be read: the lists are still read, no 물건 list is asked blind.
pub(crate) async fn collect_dividends(
    contract: &SourceContract,
    asking: &Asking<'_>,
    deadline: Instant,
    courts: &[String],
    known: Option<&HashSet<(String, String)>>,
    now: impl Fn() -> DateTime<Utc> + Sync,
) -> anyhow::Result<(DividendsResult, Option<Stop>)> {
    let notices = &contract.dividend_notices;
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
    let mut result = DividendsResult::default();
    let mut stop = None;
    // The departments of each court (the screen sends no headers of its own for them).
    let no_headers = BTreeMap::new();
    let asked: Vec<_> = courts
        .iter()
        .map(|court| {
            send(
                &on,
                &notices.depts_operation,
                &notices.depts_path,
                &no_headers,
                json!({ notices.depts_court_field.as_str(): court }),
                &now,
            )
        })
        .collect();
    let mut answers = stream::iter(asked).buffered(notices.concurrency.max(1));
    let mut departments = Vec::new();
    let mut court_of = courts.iter();
    while let Some(sent) = answers.next().await {
        let court = court_of.next().cloned().unwrap_or_default();
        match sent? {
            Sent::Stopped(why) => {
                stop.get_or_insert(why);
            }
            Sent::Answered(answered) => {
                result.failed += u32::from(answered.answer.is_none());
                for dept in rows_at(answered.answer.as_ref(), &notices.depts_pointer) {
                    if let Some(code) = dept
                        .get(&notices.dept_code_field)
                        .and_then(JsonValue::as_str)
                    {
                        departments.push((court.clone(), code.to_owned()));
                    }
                }
                result.lines.push(answered.line);
            }
        }
    }
    drop(answers);
    result.departments = u32::try_from(departments.len()).unwrap_or(u32::MAX);
    if stop.is_some() {
        return Ok((result, stop));
    }
    // Each department's notice list.
    let asked: Vec<_> = departments
        .iter()
        .map(|(court, dept)| {
            send(
                &on,
                &notices.operation,
                &notices.list_path,
                &notices.list_headers,
                notices.list_body(court, dept),
                &now,
            )
        })
        .collect();
    let mut answers = stream::iter(asked).buffered(notices.concurrency.max(1));
    let mut listed = Vec::new();
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
    result.listed_cases = listed.len();
    let Some(known) = known else {
        return Ok((result, stop));
    };
    if stop.is_some() {
        return Ok((result, stop));
    }
    // The 물건 list of each case not read yet, each case once.
    let mut seen = HashSet::new();
    let wanted: Vec<&JsonValue> = listed
        .iter()
        .filter(|row| {
            notices
                .case_of(row)
                .is_some_and(|case| !known.contains(&case) && seen.insert(case))
        })
        .collect();
    let asked: Vec<_> = wanted
        .iter()
        .map(|row| {
            send(
                &on,
                &notices.objects_operation,
                &notices.objects_path,
                &notices.objects_headers,
                notices.objects_body(row),
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
                result.object_lists += 1;
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
    fn the_list_and_the_objects_are_asked_as_the_screen_asks_them() {
        let contract = contract();
        let notices = &contract.dividend_notices;
        // The shapes PGJ142M01 and PGJ142P01 send, measured 2026-11-19.
        let list = notices.list_body("B999999", "1002");
        let asked = &list["dma_srchLstprdPbanc"];
        assert_eq!(asked["jdbnDeptCortOfcCd"], json!("B999999"));
        assert_eq!(asked["jdbnCd"], json!("1002"));
        assert_eq!(asked["dvsFlag"], json!("C"));
        let row = json!({"cortOfcCd": "B999999", "csNo": "20990130000001", "orddcsDvsCd": "021",
                         "dstrtDemnLstprdYmd": "20991126", "rprsCrdtrNm": "합성채권자"});
        assert_eq!(
            notices.objects_body(&row),
            json!({"dma_srchDstrtDemnLstDts": {"cortOfcCd": "B999999", "csNo": "20990130000001",
                   "orddcsDvsCd": "021", "dstrtDemnLstprdYmd": "20991126"}})
        );
        assert_eq!(
            notices.case_of(&row),
            Some(("B999999".to_owned(), "20990130000001".to_owned()))
        );
        assert_eq!(notices.objects_from, "silver.court_auction_dividend_object");
    }
}
