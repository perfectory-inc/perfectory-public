use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, NaiveDate, Utc};
use collection_application::{AskCounts, CourtAuctionSessions};
use serde_json::{json, Value};
use tokio::time::Instant;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use super::{
    asked_condition, collect, in_window, may_ask, night_summary, operator_window, session_pool,
    source_contract, with_night, Asking, FailedPage, SourceContract, Stop, Window,
};

// Synthetic only: made-up case numbers and court codes (public repository).

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_default()
}

/// 02:00 KST on 2099-01-02.
fn night() -> DateTime<Utc> {
    at("2099-01-01T17:00:00Z")
}

fn page_body(rows: usize, total: usize) -> Value {
    let rows = (0..rows)
        .map(|n| json!({"boCd": "B999999", "saNo": format!("2099013000{n:04}"), "maemulSer": "1", "mokmulSer": "1"}))
        .collect::<Vec<_>>();
    json!({"status": 200, "data": {"ipcheck": true, "dma_pageInfo": {"totalCnt": total.to_string()}, "dlt_srchResult": rows}})
}

/// The site's 200 block, as it answered on 2026-10-21 (synthetic message).
fn block_body() -> Value {
    json!({"status": 200, "message": "blocked", "data": {"ipcheck": false}})
}

/// A site whose first page opens a session (a fresh cookie each time) and that answers every
/// search with `pages` pages of `page_size` rows, blocking the first `blocks` searches with
/// `ipcheck=false`.
async fn site(contract: &SourceContract, pages: usize, blocks: usize) -> MockServer {
    let server = MockServer::start().await;
    let opened = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path(contract.collection.session.opening_path.as_str()))
        .respond_with(move |_: &Request| {
            let n = opened.fetch_add(1, Ordering::Relaxed);
            ResponseTemplate::new(200)
                .insert_header("set-cookie", format!("JSESSIONID=s{n}; Path=/").as_str())
        })
        .mount(&server)
        .await;
    let page_size = contract.item_search.page_size as usize;
    let total = pages * page_size;
    let asked = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .respond_with(move |request: &Request| {
            if asked.fetch_add(1, Ordering::Relaxed) < blocks {
                return ResponseTemplate::new(200).set_body_json(block_body());
            }
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let page = body["dma_pageInfo"]["pageNo"].as_u64().unwrap_or(1) as usize;
            let rows = if page <= pages { page_size } else { 0 };
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json;charset=UTF-8")
                .set_body_json(page_body(rows, total))
        })
        .mount(&server)
        .await;
    server
}

/// The contract's asking, with the waits shortened so a test does not sleep for minutes.
fn asking<'a>(
    contract: &SourceContract,
    sessions: &'a dyn CourtAuctionSessions,
    budget: Duration,
) -> Asking<'a> {
    let mut asking = Asking::new(
        contract,
        sessions,
        Instant::now() + budget,
        PathBuf::from("/nonexistent-switches"),
    );
    asking.rules.block_waits = vec![Duration::from_millis(1)];
    asking.rules.failure_wait = Duration::from_millis(1);
    asking
}

fn conditions(contract: &SourceContract) -> usize {
    contract.item_search.conditions.len() + contract.sale_result_search.conditions.len()
}

#[test]
fn the_contract_holds_every_value_and_the_collector_restates_none() -> anyhow::Result<()> {
    // Root ADR-0289 §7: the endpoints, the opening page and the browser profile live in the
    // contract only; the collector's source naming one of them is a second copy.
    let contract = source_contract()?;
    let source = include_str!("../courtauction_go_kr_collect.rs");
    let restated = |source: &str| -> Vec<String> {
        let mut values = vec![
            contract.base_url.clone(),
            contract.collection.session.opening_path.clone(),
            contract.collection.session.browser_profile.clone(),
        ];
        for search in [&contract.item_search, &contract.sale_result_search] {
            values.push(search.path.clone());
            values.extend(search.headers.values().cloned());
        }
        values.extend(contract.collection.session.headers.values().cloned());
        values
            .into_iter()
            .filter(|value| source.contains(&format!("\"{value}\"")))
            .collect()
    };
    assert_eq!(restated(source), Vec::<String>::new());
    let planted = format!("const PATH: &str = \"{}\";", contract.item_search.path);
    assert_eq!(restated(&planted), vec![contract.item_search.path.clone()]);
    for search in [&contract.item_search, &contract.sale_result_search] {
        assert!(
            contract.collection.pace.contains_key(&search.pace_group),
            "{} names an unknown pace group",
            search.dataset_slug
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_run_reads_every_condition_to_its_last_page_on_browser_sessions() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = site(&contract, 3, 0).await;
    let pool = session_pool(&contract, &server.uri())?;
    let asking = asking(&contract, &pool, Duration::from_secs(60));
    let (results, stop) = collect(&contract, &asking, night).await?;
    assert_eq!(stop, None);
    assert!(results
        .values()
        .all(|result| result.complete && result.failed.is_empty()));
    let page_size = u64::from(contract.item_search.page_size);
    assert_eq!(
        results["item_search"].rows,
        3 * page_size * contract.item_search.conditions.len() as u64
    );
    let counts = asking.tally.counts();
    assert_eq!(counts.requests as usize, 3 * conditions(&contract));
    assert_eq!((counts.blocks, counts.renewals, counts.failed), (0, 0, 0));
    for request in server.received_requests().await.unwrap_or_default() {
        if request.method.as_str() != "POST" {
            continue;
        }
        assert!(
            request.headers.contains_key("cookie"),
            "a search rides an open session"
        );
        for (name, value) in &contract.collection.session.headers {
            assert_eq!(
                request
                    .headers
                    .get(name.as_str())
                    .and_then(|v| v.to_str().ok()),
                Some(value.as_str())
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn ipcheck_false_renews_the_session_and_the_run_still_completes() -> anyhow::Result<()> {
    // 2026-11-06: the old collector read ipcheck=false as a refusal after 1,528 requests, stopped,
    // and every later run refused to start until a person removed refused.json.
    let contract = source_contract()?;
    let blocks = 7;
    let server = site(&contract, 2, blocks).await;
    let pool = session_pool(&contract, &server.uri())?;
    let asking = asking(&contract, &pool, Duration::from_secs(60));
    let (results, stop) = collect(&contract, &asking, night).await?;
    assert_eq!(stop, None);
    assert!(results.values().all(|result| result.complete));
    let counts = asking.tally.counts();
    assert_eq!(
        (counts.blocks, counts.renewals),
        (blocks as u64, blocks as u64)
    );
    assert_eq!(counts.answered as usize, 2 * conditions(&contract));
    // Every renewal opened a fresh session on the site's first page.
    let sessions = pool.sessions();
    assert!(sessions.iter().filter(|s| s.blocked_at.is_some()).count() >= 1);
    let opened = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| request.url.path() == contract.collection.session.opening_path)
        .count();
    assert_eq!(opened, sessions.len());
    Ok(())
}

#[tokio::test]
async fn the_time_budget_ends_the_run_incomplete_not_refused() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = site(&contract, 2, usize::MAX).await;
    let pool = session_pool(&contract, &server.uri())?;
    let asking = asking(&contract, &pool, Duration::from_millis(200));
    let (results, stop) = collect(&contract, &asking, night).await?;
    assert_eq!(stop, Some(Stop::TimeBudget));
    assert!(!results["item_search"].complete);
    let counts = asking.tally.counts();
    assert!(counts.blocks > 0 && counts.answered == 0 && counts.failed == 0);
    Ok(())
}

#[tokio::test]
async fn a_page_that_keeps_failing_is_recorded_and_leaves_the_pass_incomplete() -> anyhow::Result<()>
{
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("content-type", "application/json;charset=UTF-8"))
        .respond_with(ResponseTemplate::new(500).set_body_string("oops"))
        .mount(&server)
        .await;
    let pool = session_pool(&contract, &server.uri())?;
    let asking = asking(&contract, &pool, Duration::from_secs(60));
    let (results, stop) = collect(&contract, &asking, night).await?;
    assert_eq!(stop, None);
    let item = &results["item_search"];
    assert!(!item.complete);
    assert_eq!(item.failed.len(), contract.item_search.conditions.len());
    assert!(item.failed.iter().all(|page| page.page_no == 1));
    assert_eq!(asking.tally.counts().failed as usize, conditions(&contract));
    Ok(())
}

#[test]
fn the_item_search_asks_the_contracts_fixed_date_range_and_the_result_search_none(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let window = contract
        .item_search
        .date_window
        .clone()
        .ok_or_else(|| anyhow::anyhow!("the item search has no date_window"))?;
    let day = NaiveDate::from_ymd_opt(2099, 1, 2).ok_or_else(|| anyhow::anyhow!("date"))?;
    let ymd = |days: i64| {
        (day + chrono::Duration::days(days))
            .format("%Y%m%d")
            .to_string()
    };
    for condition in &contract.item_search.conditions {
        let asked = asked_condition(&contract.item_search, condition, day);
        assert_eq!(
            asked.get(&window.start_field),
            Some(&ymd(-window.days_before))
        );
        assert_eq!(asked.get(&window.end_field), Some(&ymd(window.days_after)));
        assert!(condition
            .iter()
            .all(|(name, value)| asked.get(name) == Some(value)));
    }
    for condition in &contract.sale_result_search.conditions {
        assert_eq!(
            &asked_condition(&contract.sale_result_search, condition, day),
            condition
        );
    }
    Ok(())
}

#[tokio::test]
async fn every_item_search_page_of_a_run_asks_the_same_date_range() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = site(&contract, 2, 0).await;
    let pool = session_pool(&contract, &server.uri())?;
    collect(
        &contract,
        &asking(&contract, &pool, Duration::from_secs(60)),
        night,
    )
    .await?;
    let window = contract
        .item_search
        .date_window
        .clone()
        .ok_or_else(|| anyhow::anyhow!("the item search has no date_window"))?;
    let day = NaiveDate::from_ymd_opt(2099, 1, 2).ok_or_else(|| anyhow::anyhow!("date"))?;
    let expected = asked_condition(
        &contract.item_search,
        &contract.item_search.conditions[0],
        day,
    );
    let mut item_pages = 0;
    for request in server.received_requests().await.unwrap_or_default() {
        if request.url.path() != contract.item_search.path {
            continue;
        }
        let body: Value = serde_json::from_slice(&request.body)?;
        let asked = &body["dma_srchGdsDtlSrchInfo"];
        assert_eq!(
            asked[&window.start_field],
            json!(expected[&window.start_field])
        );
        assert_eq!(asked[&window.end_field], json!(expected[&window.end_field]));
        item_pages += 1;
    }
    assert!(item_pages > 0);
    Ok(())
}

#[test]
fn without_an_operators_window_the_job_asks_at_any_hour_and_with_one_only_inside_it(
) -> anyhow::Result<()> {
    // Root ADR-0289 §5: no quiet hours of our own; root ADR-0282: an operator may narrow them.
    let dir = tempfile::tempdir()?;
    let noon_kst = at("2099-01-01T03:00:00Z");
    assert!(operator_window(dir.path(), noon_kst).is_none());
    assert!(may_ask(dir.path(), noon_kst)? && may_ask(dir.path(), night())?);
    std::fs::write(
        dir.path().join("court_auction_collect.window"),
        "start=09:00\nend=18:00\nuntil=2099-01-01T06:00:00+00:00\nrequest=x\nactor=staff-a\nreason=day once\n",
    )?;
    let (window, by) = operator_window(dir.path(), noon_kst)
        .ok_or_else(|| anyhow::anyhow!("the window is read"))?;
    assert_eq!(
        (window.start.as_str(), window.end.as_str()),
        ("09:00", "18:00")
    );
    assert!(by.contains("staff-a: day once"));
    assert!(may_ask(dir.path(), noon_kst)?);
    assert!(
        !may_ask(dir.path(), at("2098-12-31T23:00:00Z"))?,
        "08:00 KST, before the window opens"
    );
    assert!(
        operator_window(dir.path(), at("2099-01-01T06:00:00Z")).is_none(),
        "an ended window is not one"
    );
    std::fs::write(
        dir.path().join("court_auction_collect.window"),
        "start=9:00\nend=18:00\nuntil=2099-01-02T00:00:00+00:00\n",
    )?;
    assert!(
        operator_window(dir.path(), noon_kst).is_none(),
        "an unreadable window is not one"
    );
    let whole_day = Window {
        start: "00:00".to_owned(),
        end: "00:00".to_owned(),
    };
    assert!(in_window(&whole_day, noon_kst)? && in_window(&whole_day, night())?);
    Ok(())
}

#[test]
fn every_object_records_the_runs_requests_blocks_renewals_and_sessions() {
    // 2026-10-19: the first night's request count had to be estimated from its duration.
    let counts = AskCounts {
        requests: 1_540,
        answered: 1_528,
        blocks: 12,
        renewals: 12,
        ..AskCounts::default()
    };
    let failed_page = FailedPage {
        condition: BTreeMap::new(),
        page_no: 7,
        reason: "Status(500)".to_owned(),
    };
    let summary = night_summary(
        counts,
        &[],
        &BTreeMap::from([("item_search", std::slice::from_ref(&failed_page))]),
        Some(&Stop::TimeBudget),
    );
    assert_eq!(
        summary["failed_pages"]["item_search"][0]["page_no"].as_u64(),
        Some(7)
    );
    assert_eq!(summary["requests_sent"].as_u64(), Some(1_540));
    assert_eq!(summary["asking"]["blocks"].as_u64(), Some(12));
    assert_eq!(summary["asking"]["renewals"].as_u64(), Some(12));
    assert_eq!(summary["stopped"], json!("TimeBudget"));
    let params = with_night(
        &json!({"operation": "searchControllerMain"}),
        1_125,
        &summary,
    );
    assert_eq!(params["operation"], "searchControllerMain");
    assert_eq!(params["requests_in_object"].as_u64(), Some(1_125));
    assert_eq!(params["night"]["requests_sent"].as_u64(), Some(1_540));
}

/// A site that opens sessions and answers every 물건상세 with one synthetic photo, blocking the
/// first `blocks` with `ipcheck=false`.
async fn detail_site(contract: &SourceContract, blocks: usize) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(contract.collection.session.opening_path.as_str()))
        .respond_with(
            ResponseTemplate::new(200).insert_header("set-cookie", "JSESSIONID=d; Path=/"),
        )
        .mount(&server)
        .await;
    let asked = Arc::new(AtomicUsize::new(0));
    // A JPEG start (the sniff reads the magic bytes), base64 as the site sends it.
    let photo = "/9j/4AAQ";
    Mock::given(method("POST"))
        .and(path(contract.item_detail.path.as_str()))
        .respond_with(move |_: &Request| {
            if asked.fetch_add(1, Ordering::Relaxed) < blocks {
                return ResponseTemplate::new(200).set_body_json(block_body());
            }
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": 200, "data": {"ipcheck": true,
                "dma_result": {"csPicLst": [{"picFile": photo, "cortAuctnPicSeq": "1"}]}}}))
        })
        .mount(&server)
        .await;
    server
}

fn wanted(sa: &str) -> super::detail::Wanted {
    super::detail::Wanted {
        key: vec!["B999999".into(), sa.into(), "1".into()],
        sale_date: NaiveDate::from_ymd_opt(2099, 1, 15),
        status_code: "0002100001".into(),
        fingerprint: "f".into(),
    }
}

#[tokio::test]
async fn the_details_ride_the_same_pool_a_block_renews_and_photos_become_hashes(
) -> anyhow::Result<()> {
    // Before root ADR-0289 the detail phase stopped the night on its first ipcheck=false.
    let contract = source_contract()?;
    let server = detail_site(&contract, 1).await;
    let pool = session_pool(&contract, &server.uri())?;
    let asking = asking(&contract, &pool, Duration::from_secs(60));
    let store = super::detail::PhotoStore {
        prefix: super::detail::photo_prefix(&contract.item_detail)?,
        storage: None,
    };
    let asked = [wanted("20990130000001"), wanted("20990130000002")];
    let (result, stop) =
        super::detail::collect_details(&contract, &asking, asking.deadline, &asked, &store, night)
            .await?;
    assert_eq!(stop, None);
    assert_eq!(
        (result.asked, result.failed, result.photos.photos),
        (2, 0, 2)
    );
    assert_eq!(asking.tally.counts().blocks, 1);
    assert!(result
        .lines
        .iter()
        .all(|line| line.contains("sha256:") && !line.contains("/9j/4AAQ")));

    let (result, stop) =
        super::detail::collect_details(&contract, &asking, Instant::now(), &asked, &store, night)
            .await?;
    assert_eq!(
        (stop, result.asked),
        (Some(Stop::TimeBudget), 0),
        "the detail deadline ends the asking; the rest is due again next run"
    );
    Ok(())
}
