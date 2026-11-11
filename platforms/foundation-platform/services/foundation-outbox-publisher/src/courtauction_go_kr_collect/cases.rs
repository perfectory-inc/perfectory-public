//! 법원경매 사건 문서: 사건상세, 현황조사 and 감정평가, asked per case after the 물건상세 phase (root
//! ADR-0540 §1, §3).
//!
//! Which cases is computed each run, never stored (root ADR-0352): the cases of this run's
//! in-progress list rows against the last read `silver.court_auction_case_document_read` records,
//! never-read first, then what the contract's recrawl rule says is due, soonest sale date first, at
//! most the contract's per-run limit. A due case is asked every document of the contract's
//! `case_documents`; a document with rounds (현황조사·감정평가 회차) is asked for its newest round,
//! then for every other round its answer lists. Each request is one Bronze line with the answer as
//! received, one object per document: the names it holds stay in Bronze (root ADR-0540 §4).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Context as _;
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::{ask, Asked, PaceGate, SiteMethod, SiteRequest};
use collection_domain::court_auction::{due, CaseView, RecrawlRule};
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use sha2::{Digest as _, Sha256};
use tokio::time::Instant;

use super::detail::{run_day, LastRead, Wanted};
use super::{may_ask, Asking, Search, SourceContract, Stop};

/// One of a case's documents (`case_documents.documents[]`).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CaseDocument {
    /// The name the manifest and the read record give it.
    pub(crate) role: String,
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
    /// The headers the site's own screen sends to this endpoint.
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) request_object: String,
    pub(crate) request_constants: BTreeMap<String, String>,
    /// The request field naming a round, for a document with rounds.
    #[serde(default)]
    pub(crate) round_field: Option<String>,
    /// Where an answer lists the case's rounds (each `{round_field: n}`).
    #[serde(default)]
    pub(crate) rounds_pointer: Option<String>,
    /// Where an answer says which round it is.
    #[serde(default)]
    pub(crate) answered_round_pointer: Option<String>,
}

impl CaseDocument {
    /// The document as the Bronze persist step names a search: its dataset, operation and path.
    pub(crate) fn as_search(&self, pace_group: &str) -> Search {
        Search {
            dataset_slug: self.dataset_slug.clone(),
            operation: self.operation.clone(),
            path: self.path.clone(),
            page_size: 0,
            pace_group: pace_group.to_owned(),
            headers: self.headers.clone(),
            conditions: Vec::new(),
            date_window: None,
        }
    }
}

/// The contract's `case_documents`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CaseDocuments {
    pub(crate) pace_group: String,
    /// The request fields of a case's key, in key order (court office, case).
    pub(crate) key_fields: Vec<String>,
    pub(crate) per_run_limit: u32,
    /// What the 물건상세 phase leaves these before the ladder step's budget (root ADR-0427 §7).
    pub(crate) run_time_budget_seconds: u64,
    pub(crate) read_from: String,
    pub(crate) documents: Vec<CaseDocument>,
}

/// A case: (court office, case).
pub(crate) type CaseKey = Vec<String>;

/// One in-progress case of this run's list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WantedCase {
    pub(crate) key: CaseKey,
    /// Its soonest sale date.
    pub(crate) sale_date: Option<NaiveDate>,
    pub(crate) status_code: String,
    /// The hash of its properties' list fingerprints: a property's change is the case's.
    pub(crate) fingerprint: String,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn text(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        JsonValue::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// The cases of this run's in-progress properties: one per (court office, case), with its soonest
/// sale date and the hash of its properties' fingerprints in key order.
pub(crate) fn cases_of(wanted: &[Wanted]) -> Vec<WantedCase> {
    let mut found: BTreeMap<CaseKey, (Option<NaiveDate>, String, Vec<(Vec<String>, String)>)> =
        BTreeMap::new();
    for property in wanted {
        let Some(case) = property.key.get(..2) else {
            continue;
        };
        let entry = found
            .entry(case.to_vec())
            .or_insert_with(|| (None, property.status_code.clone(), Vec::new()));
        entry.0 = match (entry.0, property.sale_date) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        entry
            .2
            .push((property.key.clone(), property.fingerprint.clone()));
    }
    found
        .into_iter()
        .map(|(key, (sale_date, status_code, mut properties))| {
            properties.sort();
            let mut hasher = Sha256::new();
            for (property, fingerprint) in &properties {
                hasher.update(property.join("|").as_bytes());
                hasher.update(b"=");
                hasher.update(fingerprint.as_bytes());
                hasher.update(b"\n");
            }
            WantedCase {
                key,
                sale_date,
                status_code,
                fingerprint: hex(&hasher.finalize()),
            }
        })
        .collect()
}

/// The cases this run asks, in order: never read first, then the due ones, each soonest sale date
/// first (an undated one last), then by key; at most `limit`. The same inputs always give the same
/// list (root ADR-0352 §1).
pub(crate) fn frontier(
    cases: Vec<WantedCase>,
    last: &HashMap<CaseKey, LastRead>,
    rule: &RecrawlRule,
    today: NaiveDate,
    limit: u32,
) -> Vec<WantedCase> {
    let mut due_now: Vec<(bool, WantedCase)> = cases
        .into_iter()
        .filter_map(|case| {
            let read = last.get(&case.key);
            let view = CaseView {
                status_code: &case.status_code,
                sale_date: case.sale_date,
                last_read: read.map(|read| read.day),
                row_changed: read
                    .is_some_and(|read| read.fingerprint.as_deref() != Some(&case.fingerprint)),
            };
            due(rule, &view, today).then_some((read.is_some(), case))
        })
        .collect();
    due_now.sort_by(|(a_read, a), (b_read, b)| {
        (a_read, a.sale_date.is_none(), a.sale_date, &a.key).cmp(&(
            b_read,
            b.sale_date.is_none(),
            b.sale_date,
            &b.key,
        ))
    });
    due_now
        .into_iter()
        .take(limit as usize)
        .map(|(_, case)| case)
        .collect()
}

/// The last read of each case from rows of `silver.court_auction_case_document_read`: its newest
/// run, any document.
pub(crate) fn last_reads_of<'a>(
    rows: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
) -> HashMap<CaseKey, LastRead> {
    let mut out: HashMap<CaseKey, (String, LastRead)> = HashMap::new();
    for row in rows {
        let key: Option<CaseKey> = ["court_office_code", "case_no"]
            .iter()
            .map(|column| text(&row[*column]))
            .collect();
        let (Some(key), Some(snapshot)) = (key, text(&row["source_snapshot_id"])) else {
            continue;
        };
        let Some(day) = run_day(&snapshot) else {
            continue;
        };
        let read = LastRead {
            day,
            fingerprint: text(&row["list_fingerprint"]),
        };
        match out.get(&key) {
            Some((seen, _)) if *seen >= snapshot => {}
            _ => {
                out.insert(key, (snapshot, read));
            }
        }
    }
    out.into_iter()
        .map(|(key, (_, read))| (key, read))
        .collect()
}

/// Reads the last reads from the lakehouse (`read_from`'s current snapshot); none when the table
/// holds no snapshot yet.
pub(crate) async fn last_reads_from_lakehouse(
    documents: &CaseDocuments,
) -> anyhow::Result<HashMap<CaseKey, LastRead>> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::SILVER_COURT_AUCTION_CASE_DOCUMENT_READ;
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    anyhow::ensure!(
        documents.read_from == SILVER_COURT_AUCTION_CASE_DOCUMENT_READ.table_name,
        "case_documents.read_from names {}, the collector reads {}",
        documents.read_from,
        SILVER_COURT_AUCTION_CASE_DOCUMENT_READ.table_name
    );
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let Some(snapshot) = catalog
        .load_current_snapshot_manifest_list(&documents.read_from)
        .await
        .with_context(|| format!("failed to resolve {}", documents.read_from))?
    else {
        return Ok(HashMap::new());
    };
    let lakehouse = LakehouseObjectReader::from_env()?;
    let scanned = scan_snapshot_rows(
        &SILVER_COURT_AUCTION_CASE_DOCUMENT_READ,
        &lakehouse,
        &snapshot,
    )
    .await?;
    Ok(last_reads_of(scanned.rows.iter()))
}

/// The request body of one case's document, for `round` (empty: the newest).
pub(crate) fn request_body(
    documents: &CaseDocuments,
    document: &CaseDocument,
    key: &CaseKey,
    round: Option<&str>,
) -> JsonValue {
    let mut object = serde_json::Map::new();
    for (field, value) in documents.key_fields.iter().zip(key) {
        object.insert(field.clone(), json!(value));
    }
    for (field, value) in &document.request_constants {
        object.insert(field.clone(), json!(value));
    }
    if let Some(field) = &document.round_field {
        object.insert(field.clone(), json!(round.unwrap_or_default()));
    }
    json!({ document.request_object.as_str(): object })
}

/// The rounds an answer lists other than the one it answered, in order.
pub(crate) fn other_rounds(document: &CaseDocument, answer: &JsonValue) -> Vec<String> {
    let (Some(list), Some(field)) = (&document.rounds_pointer, &document.round_field) else {
        return Vec::new();
    };
    let answered = document
        .answered_round_pointer
        .as_deref()
        .and_then(|pointer| answer.pointer(pointer))
        .and_then(text);
    let listed: BTreeSet<String> = answer
        .pointer(list)
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(|round| text(&round[field.as_str()]))
        .collect();
    listed
        .into_iter()
        .filter(|round| Some(round) != answered.as_ref())
        .collect()
}

/// What one request came to: its Bronze line and, for a document with rounds, the other rounds its
/// answer lists.
struct Read {
    line: String,
    failed: bool,
    rounds: Vec<String>,
}

enum Sent {
    Read(Read),
    Stopped(Stop),
}

/// The asking one request shares with the run.
struct Ask<'a, 'b> {
    asking: &'a Asking<'b>,
    gate: &'a PaceGate,
    deadline: Instant,
    documents: &'a CaseDocuments,
    session_headers: &'a BTreeMap<String, String>,
}

/// Asks one case's document for `round` unless the operator's window is shut.
async fn read_one(
    on: &Ask<'_, '_>,
    document: &CaseDocument,
    case: &WantedCase,
    round: Option<String>,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
) -> anyhow::Result<Sent> {
    let at = now();
    if !may_ask(&on.asking.switches, at)? {
        return Ok(Sent::Stopped(Stop::WindowEnded));
    }
    let body = request_body(on.documents, document, &case.key, round.as_deref());
    let request = SiteRequest {
        method: SiteMethod::Post,
        path: document.path.clone(),
        headers: on
            .session_headers
            .iter()
            .chain(&document.headers)
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
        "operation": document.operation,
        "request": body,
        "requested_at_utc": at.to_rfc3339(),
        "list_fingerprint": case.fingerprint,
    });
    let mut read = Read {
        line: String::new(),
        failed: false,
        rounds: Vec::new(),
    };
    match asked {
        Asked::Unfinished => return Ok(Sent::Stopped(Stop::TimeBudget)),
        Asked::Data { data, reply } => {
            line["http_status"] = json!(reply.status);
            line["read_outcome"] = json!("read");
            if round.is_none() {
                read.rounds = other_rounds(document, &json!({ "data": data }));
            }
            line["body"] = json!(reply.body);
        }
        Asked::Failed { reason, reply } => {
            line["http_status"] = json!(reply.as_ref().map(|reply| reply.status));
            line["read_outcome"] = json!("failed");
            line["failure"] = json!(reason);
            line["body"] = json!(reply.map(|reply| reply.body));
            read.failed = true;
        }
    }
    read.line = serde_json::to_string(&line)?;
    Ok(Sent::Read(read))
}

/// One case's documents: each document's newest round, then its other rounds.
async fn read_case(
    on: &Ask<'_, '_>,
    case: &WantedCase,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
) -> anyhow::Result<(Vec<(usize, Read)>, Option<Stop>)> {
    let mut reads = Vec::new();
    for (index, document) in on.documents.documents.iter().enumerate() {
        let first = match read_one(on, document, case, None, now).await? {
            Sent::Read(read) => read,
            Sent::Stopped(why) => return Ok((reads, Some(why))),
        };
        let others = first.rounds.clone();
        reads.push((index, first));
        for round in others {
            match read_one(on, document, case, Some(round), now).await? {
                Sent::Read(read) => reads.push((index, read)),
                Sent::Stopped(why) => return Ok((reads, Some(why))),
            }
        }
    }
    Ok((reads, None))
}

/// What the case documents of one run came to.
#[derive(Debug, Default)]
pub(crate) struct CasesResult {
    /// Per document (contract order), its Bronze lines.
    pub(crate) lines: Vec<Vec<String>>,
    pub(crate) cases: u32,
    pub(crate) requests: u32,
    pub(crate) failed: u32,
}

/// Asks the documents of `cases` until `deadline`, as many cases in flight as the pace group allows,
/// and keeps each document's lines in the order asked. A case cut short by the deadline keeps the
/// lines it got; it is due again next run (its read record is the newest of what landed).
pub(crate) async fn collect_case_documents(
    contract: &SourceContract,
    asking: &Asking<'_>,
    deadline: Instant,
    cases: &[WantedCase],
    now: impl Fn() -> DateTime<Utc> + Sync,
) -> anyhow::Result<(CasesResult, Option<Stop>)> {
    let documents = &contract.case_documents;
    let rule = contract
        .collection
        .pace
        .get(&documents.pace_group)
        .with_context(|| format!("no collection.pace group {:?}", documents.pace_group))?;
    let gate = asking
        .gates
        .get(&documents.pace_group)
        .with_context(|| format!("no collection.pace group {:?}", documents.pace_group))?;
    let on = Ask {
        asking,
        gate,
        deadline,
        documents,
        session_headers: &contract.collection.session.headers,
    };
    // Built here, run only as the stream polls them: no closure lives in the stream (one broke the
    // binary's Send bound, as `detail::collect_details` notes).
    let reads: Vec<_> = cases
        .iter()
        .map(|case| read_case(&on, case, &now))
        .collect();
    let mut answers = stream::iter(reads).buffered(rule.max.max(1) as usize);
    let mut result = CasesResult {
        lines: vec![Vec::new(); documents.documents.len()],
        ..CasesResult::default()
    };
    let mut stop = None;
    while let Some(read) = answers.next().await {
        let (reads, stopped) = read?;
        for (index, read) in reads {
            result.requests += 1;
            result.failed += u32::from(read.failed);
            result.lines[index].push(read.line);
        }
        if let Some(why) = stopped {
            stop.get_or_insert(why);
            break;
        }
        result.cases += 1;
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

    fn property(case: &str, seq: &str, sale: Option<(u32, u32)>, fingerprint: &str) -> Wanted {
        Wanted {
            key: vec!["B999999".into(), case.into(), seq.into()],
            sale_date: sale.and_then(|(m, d)| NaiveDate::from_ymd_opt(2099, m, d)),
            status_code: "0002100001".into(),
            fingerprint: fingerprint.into(),
        }
    }

    #[test]
    fn the_contract_names_three_documents_read_by_case() {
        let contract = contract();
        let documents = &contract.case_documents;
        assert_eq!(
            documents.read_from,
            "silver.court_auction_case_document_read"
        );
        let roles: Vec<&str> = documents
            .documents
            .iter()
            .map(|d| d.role.as_str())
            .collect();
        assert_eq!(roles, ["case_detail", "case_survey", "case_appraisal"]);
        assert!(contract.collection.pace.contains_key(&documents.pace_group));
        assert!(documents.per_run_limit >= 40_000);
        let key = vec!["B999999".to_owned(), "20990130000001".to_owned()];
        // The shapes the site's screens send, measured 2026-11-12 (root ADR-0540).
        assert_eq!(
            request_body(documents, &documents.documents[0], &key, None),
            json!({"dma_srchCsDtlInf": {"cortOfcCd": "B999999", "csNo": "20990130000001"}})
        );
        assert_eq!(
            request_body(documents, &documents.documents[2], &key, Some("2")),
            json!({"dma_srchAeeWevl": {"cortOfcCd": "B999999", "csNo": "20990130000001",
                   "auctnInfOriginDvsCd": "4", "pgmId": "PGJ15BP03", "ordTsCnt": "2"}})
        );
        assert_eq!(
            request_body(documents, &documents.documents[1], &key, None)
                ["dma_srchCurstExmndcRletIndctLst"]["ordTsCnt"],
            json!(""),
            "an empty round asks the newest"
        );
    }

    #[test]
    fn a_case_is_its_properties_soonest_sale_and_their_fingerprints() {
        let cases = cases_of(&[
            property("20990130000001", "2", Some((2, 1)), "b"),
            property("20990130000001", "1", Some((1, 20)), "a"),
            property("20990130000002", "1", None, "c"),
        ]);
        assert_eq!(cases.len(), 2);
        assert_eq!(cases[0].sale_date, NaiveDate::from_ymd_opt(2099, 1, 20));
        let again = cases_of(&[
            property("20990130000001", "1", Some((1, 20)), "a"),
            property("20990130000001", "2", Some((2, 1)), "b"),
        ]);
        assert_eq!(
            cases[0].fingerprint, again[0].fingerprint,
            "the order read does not matter"
        );
        let changed = cases_of(&[
            property("20990130000001", "1", Some((1, 20)), "a"),
            property("20990130000001", "2", Some((2, 1)), "B"),
        ]);
        assert_ne!(
            cases[0].fingerprint, changed[0].fingerprint,
            "a property's change is the case's"
        );
    }

    #[test]
    fn the_frontier_asks_never_read_cases_first_and_skips_an_unchanged_one() {
        let contract = contract();
        let today = NaiveDate::from_ymd_opt(2099, 1, 10).unwrap();
        let cases = cases_of(&[
            property("NEW", "1", Some((2, 1)), "a"),
            property("SAME", "1", Some((1, 20)), "a"),
            property("CHANGED", "1", Some((1, 12)), "a"),
        ]);
        let mut last = HashMap::new();
        for case in &cases {
            let fingerprint = if case.key[1] == "CHANGED" {
                "other".to_owned()
            } else {
                case.fingerprint.clone()
            };
            if case.key[1] != "NEW" {
                last.insert(
                    case.key.clone(),
                    LastRead {
                        day: today,
                        fingerprint: Some(fingerprint),
                    },
                );
            }
        }
        let asked: Vec<String> = frontier(cases, &last, &contract.collection.recrawl, today, 10)
            .into_iter()
            .map(|case| case.key[1].clone())
            .collect();
        assert_eq!(asked, ["NEW", "CHANGED"]);
    }

    #[test]
    fn the_other_rounds_are_the_listed_ones_not_answered() {
        let contract = contract();
        let survey = &contract.case_documents.documents[1];
        let answer = json!({"data": {"dlt_rletCsCurstExmnTs": [{"ordTsCnt": 1}, {"ordTsCnt": 2}],
                                     "dma_ordTsCurstExmnMngInf": {"ordTsCnt": "2"}}});
        assert_eq!(other_rounds(survey, &answer), ["1"]);
        let one = json!({"data": {"dlt_rletCsCurstExmnTs": [{"ordTsCnt": 1}],
                                  "dma_ordTsCurstExmnMngInf": {"ordTsCnt": 1}}});
        assert!(other_rounds(survey, &one).is_empty());
        assert!(
            other_rounds(&contract.case_documents.documents[0], &answer).is_empty(),
            "a document without rounds"
        );
    }

    #[test]
    fn the_last_read_of_a_case_is_its_newest_run() {
        let rows: Vec<serde_json::Map<String, JsonValue>> = [
            ("20990101T160500Z", "a", "case_detail"),
            ("20990103T160500Z", "b", "case_survey"),
        ]
        .iter()
        .map(|(snapshot, fp, document)| {
            json!({"court_office_code": "B999999", "case_no": "20990130000001", "document": document,
                   "source_snapshot_id": snapshot, "list_fingerprint": fp})
            .as_object()
            .cloned()
            .unwrap()
        })
        .collect();
        let last = last_reads_of(rows.iter());
        let read = &last[&vec!["B999999".to_owned(), "20990130000001".to_owned()]];
        assert_eq!(read.day, NaiveDate::from_ymd_opt(2099, 1, 4).unwrap());
        assert_eq!(read.fingerprint.as_deref(), Some("b"));
    }
}
