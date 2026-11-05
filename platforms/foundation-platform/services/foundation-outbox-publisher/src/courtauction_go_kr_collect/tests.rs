use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{json, Value};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use super::{
    asked_condition, collect, in_quiet_hours, night_summary, read_page, read_robots,
    refusal_on_record, run_window, source_contract, take_quiet_hours_waiver, user_agent,
    with_night, Collector, Page, Stop, PREFIX, QUIET_HOURS_WAIVER_FILE, REFUSED_FILE,
};
use crate::provider_request_spacing::ProviderRequestSpacing;

// Synthetic only: made-up case numbers and court codes (public repository).
const CONTACT: &str = "mailto:ops@example.invalid";

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_default()
}

/// 02:00 KST, inside the quiet hours.
fn night() -> DateTime<Utc> {
    at("2099-01-01T17:00:00Z")
}

fn page_body(rows: usize, total: usize) -> Value {
    let rows = (0..rows)
        .map(|n| json!({"boCd": "B999999", "saNo": format!("2099013000{n:04}"), "maemulSer": "1", "mokmulSer": "1"}))
        .collect::<Vec<_>>();
    json!({"status": 200, "data": {"ipcheck": true, "dma_pageInfo": {"totalCnt": total.to_string()}, "dlt_srchResult": rows}})
}

fn spacing() -> anyhow::Result<ProviderRequestSpacing> {
    ProviderRequestSpacing::try_new(Duration::from_millis(1))
}

/// A site with no robots.txt that answers every search with `pages` pages of two rows each.
async fn site(pages: usize) -> anyhow::Result<MockServer> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/robots.txt"))
        .respond_with(ResponseTemplate::new(404).set_body_string("<html>시스템안내</html>"))
        .mount(&server)
        .await;
    let total = pages * 2;
    Mock::given(method("POST"))
        .respond_with(move |request: &Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let page = body["dma_pageInfo"]["pageNo"].as_u64().unwrap_or(1) as usize;
            let rows = if page <= pages { 2 } else { 0 };
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json;charset=UTF-8")
                .set_body_json(page_body(rows, total))
        })
        .mount(&server)
        .await;
    Ok(server)
}

#[test]
fn the_contract_keeps_the_manners_adr_0281_names() -> anyhow::Result<()> {
    let contract = source_contract()?;
    assert!(contract.manners.request_spacing_seconds >= 3);
    assert!(contract.manners.max_requests_per_run <= 3500);
    assert!(contract.manners.network_retries <= 2);
    for status in [403, 429, 503] {
        assert!(contract.refusal.http_statuses.contains(&status));
    }
    assert!(
        !in_quiet_hours(
            &contract.manners.quiet_hours_kst,
            at("2099-01-01T03:00:00Z")
        )?,
        "noon KST"
    );
    assert!(
        in_quiet_hours(
            &contract.manners.quiet_hours_kst,
            at("2099-01-01T16:00:00Z")
        )?,
        "01:00 KST"
    );
    assert!(
        !in_quiet_hours(
            &contract.manners.quiet_hours_kst,
            at("2099-01-01T21:00:00Z")
        )?,
        "06:00 KST"
    );
    assert!(PREFIX.starts_with("FOUNDATION_PLATFORM_"));
    Ok(())
}

#[test]
fn the_collector_source_holds_no_disguise_proxy_or_captcha() -> anyhow::Result<()> {
    // ADR-0281 §4: read the module itself, the way a reviewer would.
    let source = include_str!("../courtauction_go_kr_collect.rs");
    for name in source_contract()?.forbidden_in_collector.names {
        assert!(!source.contains(&name), "the collector contains {name:?}");
    }
    assert!(
        source.contains(".no_proxy()"),
        "environment proxies must stay off"
    );
    Ok(())
}

#[test]
fn every_request_says_who_we_are_and_a_contact_only_when_set() -> anyhow::Result<()> {
    let contract = source_contract()?;
    // Root ADR-0283: the product names us; a contact is optional and never blocks a run.
    assert_eq!(
        user_agent(&contract, None)?,
        "perfectory-foundation-court-auction/1"
    );
    assert_eq!(
        user_agent(&contract, Some("  "))?,
        "perfectory-foundation-court-auction/1"
    );
    assert_eq!(
        user_agent(&contract, Some(CONTACT))?,
        "perfectory-foundation-court-auction/1 (+mailto:ops@example.invalid)"
    );
    assert!(user_agent(&contract, Some("x)\r\nInjected: 1")).is_err());
    Ok(())
}

#[test]
fn robots_rules_for_us_or_everyone_are_kept() {
    let robots = read_robots(
        "User-agent: other\nDisallow: /\n\nUser-agent: *\nDisallow: /pgj/pgjsearch/\nCrawl-delay: 4.5\n",
        "perfectory-foundation-court-auction/1",
    );
    assert!(!robots.allows("/pgj/pgjsearch/searchControllerMain.on"));
    assert!(robots.allows("/pgj/other.on"));
    assert_eq!(robots.crawl_delay_seconds, Some(5));
    assert!(read_robots("", "x/1").allows("/anything"));
}

#[test]
fn every_refusal_signal_reads_as_a_refusal() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let json = "application/json;charset=UTF-8";
    let ok = page_body(1, 1).to_string();
    assert!(matches!(
        read_page(&contract, 200, false, json, &ok),
        Page::Rows { total: 1, .. }
    ));
    for (status, retry, kind, body) in [
        (429, false, json, ok.as_str()),
        (403, false, json, ok.as_str()),
        (503, false, json, ok.as_str()),
        (200, true, json, ok.as_str()),
        (
            200,
            false,
            "text/html",
            "<html>접속이 차단되었습니다</html>",
        ),
        (200, false, json, r#"{"data": {"ipcheck": false}}"#),
        (200, false, json, r#"{"data": {}}"#),
    ] {
        assert!(
            matches!(
                read_page(&contract, status, retry, kind, body),
                Page::Refused(_)
            ),
            "{status} {retry} {kind} {body}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_night_reads_every_condition_to_its_last_page_with_our_identity() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = site(3).await?;
    let agent = user_agent(&contract, None)?;
    let mut collector = Collector::new(&contract, &server.uri(), &agent, spacing()?)?;
    let (results, stop) = collect(&contract, &mut collector, night, |at| {
        in_quiet_hours(&contract.manners.quiet_hours_kst, at)
    })
    .await?;
    assert_eq!(stop, None);
    let conditions =
        contract.item_search.conditions.len() + contract.sale_result_search.conditions.len();
    // robots, then three pages per condition.
    assert_eq!(collector.sent as usize, 1 + 3 * conditions);
    assert!(results.values().all(|result| result.complete));
    assert_eq!(
        results["item_search"].rows,
        6 * contract.item_search.conditions.len() as u64
    );
    for request in server.received_requests().await.unwrap_or_default() {
        assert_eq!(
            request
                .headers
                .get("user-agent")
                .and_then(|v| v.to_str().ok()),
            Some(agent.as_str())
        );
        for disguise in ["origin", "referer", "sec-fetch-dest", "cookie"] {
            assert!(!request.headers.contains_key(disguise), "{disguise} sent");
        }
    }
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
async fn every_item_search_page_of_a_night_asks_the_same_date_range() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = site(2).await?;
    let agent = user_agent(&contract, None)?;
    let mut collector = Collector::new(&contract, &server.uri(), &agent, spacing()?)?;
    collect(&contract, &mut collector, night, |_| Ok(true)).await?;
    let window = contract
        .item_search
        .date_window
        .clone()
        .ok_or_else(|| anyhow::anyhow!("the item search has no date_window"))?;
    // night() is 02:00 KST on 2099-01-02.
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

#[tokio::test]
async fn a_refusal_stops_the_night_at_once() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/robots.txt"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"dma_pageInfo": {"pageNo": 2}})))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "3600"))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("content-type", "application/json;charset=UTF-8"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(page_body(2, 10)),
        )
        .with_priority(5)
        .mount(&server)
        .await;
    let mut collector = Collector::new(
        &contract,
        &server.uri(),
        &user_agent(&contract, None)?,
        spacing()?,
    )?;
    let (results, stop) = collect(&contract, &mut collector, night, |at| {
        in_quiet_hours(&contract.manners.quiet_hours_kst, at)
    })
    .await?;
    assert!(matches!(stop, Some(Stop::Refused(_))), "{stop:?}");
    assert_eq!(
        collector.sent, 3,
        "robots, page 1, the refused page 2, and nothing after"
    );
    assert_eq!(
        results["item_search"].lines.len(),
        2,
        "what was collected is kept"
    );
    assert!(!results["item_search"].complete);
    Ok(())
}

#[tokio::test]
async fn the_request_bound_and_the_end_of_the_night_stop_without_a_refusal() -> anyhow::Result<()> {
    let mut contract = source_contract()?;
    let server = site(50).await?;
    contract.manners.max_requests_per_run = 4;
    let mut collector = Collector::new(
        &contract,
        &server.uri(),
        &user_agent(&contract, None)?,
        spacing()?,
    )?;
    let (_, stop) = collect(&contract, &mut collector, night, |at| {
        in_quiet_hours(&contract.manners.quiet_hours_kst, at)
    })
    .await?;
    assert_eq!((stop, collector.sent), (Some(Stop::RequestBound), 4));

    let contract = source_contract()?;
    let mut collector = Collector::new(
        &contract,
        &server.uri(),
        &user_agent(&contract, None)?,
        spacing()?,
    )?;
    let calls = std::sync::atomic::AtomicUsize::new(0);
    // The quiet hours end after the second page is asked.
    let clock = || {
        if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
            night()
        } else {
            at("2099-01-01T21:00:00Z")
        }
    };
    let (_, stop) = collect(&contract, &mut collector, clock, |at| {
        in_quiet_hours(&contract.manners.quiet_hours_kst, at)
    })
    .await?;
    assert_eq!((stop, collector.sent), (Some(Stop::QuietHoursEnded), 3));
    Ok(())
}

#[tokio::test]
async fn robots_can_forbid_a_search_and_slow_us_down() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/robots.txt"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("User-agent: *\nDisallow: /pgj/pgjsearch/\n"),
        )
        .mount(&server)
        .await;
    let mut collector = Collector::new(
        &contract,
        &server.uri(),
        &user_agent(&contract, None)?,
        spacing()?,
    )?;
    let (_, stop) = collect(&contract, &mut collector, night, |at| {
        in_quiet_hours(&contract.manners.quiet_hours_kst, at)
    })
    .await?;
    assert!(matches!(stop, Some(Stop::RobotsDisallow(_))));
    assert_eq!(collector.sent, 1, "only robots.txt was asked");
    Ok(())
}

#[test]
fn a_refusal_on_record_is_read_back() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("court-auction-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    assert_eq!(refusal_on_record(&dir)?, None);
    std::fs::write(dir.join(REFUSED_FILE), r#"{"reason": "HTTP 429"}"#)?;
    assert_eq!(refusal_on_record(&dir)?.as_deref(), Some("HTTP 429"));
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

#[tokio::test]
async fn an_attended_waiver_lets_one_run_ask_by_day_and_nothing_else_changes() -> anyhow::Result<()>
{
    let mut contract = source_contract()?;
    let server = site(50).await?;
    contract.manners.max_requests_per_run = 4;
    let midday = || at("2099-01-01T03:00:00Z"); // 12:00 KST
    let mut collector = Collector::new(
        &contract,
        &server.uri(),
        &user_agent(&contract, None)?,
        spacing()?,
    )?;
    let (_, stop) = collect(&contract, &mut collector, midday, |at| {
        in_quiet_hours(&contract.manners.quiet_hours_kst, at)
    })
    .await?;
    assert_eq!(
        (stop, collector.sent),
        (Some(Stop::QuietHoursEnded), 1),
        "without a waiver only robots.txt is asked"
    );

    let mut collector = Collector::new(
        &contract,
        &server.uri(),
        &user_agent(&contract, None)?,
        spacing()?,
    )?;
    collector.quiet_hours_waived = Some("owner decision".to_owned());
    let (_, stop) = collect(&contract, &mut collector, midday, |at| {
        in_quiet_hours(&contract.manners.quiet_hours_kst, at)
    })
    .await?;
    assert_eq!(
        (stop, collector.sent),
        (Some(Stop::RequestBound), 4),
        "the request bound still holds"
    );
    Ok(())
}

#[test]
fn a_waiver_serves_one_fresh_start_and_needs_a_reason() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("court-auction-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    let now = at("2099-01-01T03:00:00Z");
    let write = |requested_at: i64, reason: &str| {
        std::fs::write(
            dir.join(QUIET_HOURS_WAIVER_FILE),
            json!({"who": "operator", "requested_at": requested_at, "reason": reason}).to_string(),
        )
    };
    assert_eq!(take_quiet_hours_waiver(&dir, now)?, None);

    write(now.timestamp() - 30, "owner decision 2099-01-01 first run")?;
    let waiver = take_quiet_hours_waiver(&dir, now)?;
    assert!(waiver.is_some_and(|reason| reason.starts_with("owner decision 2099-01-01 first run")));
    assert_eq!(
        take_quiet_hours_waiver(&dir, now)?,
        None,
        "a waiver is removed when taken: one run"
    );

    write(now.timestamp() - 3_600, "late")?;
    assert!(
        take_quiet_hours_waiver(&dir, now).is_err(),
        "a stale waiver is refused"
    );
    assert!(!dir.join(QUIET_HOURS_WAIVER_FILE).exists(), "and removed");

    write(now.timestamp(), "  ")?;
    assert!(
        take_quiet_hours_waiver(&dir, now).is_err(),
        "a waiver needs a reason"
    );
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

#[test]
fn an_operators_window_overrides_the_contracts_until_it_ends() -> anyhow::Result<()> {
    // Root ADR-0282: switches/<job>.window, the court auction collection's run window by day.
    let contract = source_contract()?;
    let night = &contract.manners.quiet_hours_kst;
    let dir = tempfile::tempdir()?;
    let noon_kst = at("2099-01-01T03:00:00Z");
    let (window, by) = run_window(night, dir.path(), noon_kst);
    assert_eq!(
        (window.start.as_str(), by),
        (night.start.as_str(), None),
        "no override: the contract's"
    );
    std::fs::write(
        dir.path().join("court_auction_collect.window"),
        "start=09:00\nend=18:00\nuntil=2099-01-01T06:00:00+00:00\nrequest=x\nactor=staff-a\nreason=day once\n",
    )?;
    let (window, by) = run_window(night, dir.path(), noon_kst);
    assert_eq!(
        (window.start.as_str(), window.end.as_str()),
        ("09:00", "18:00")
    );
    assert!(by.is_some_and(|by| by.contains("staff-a: day once")));
    assert!(
        in_quiet_hours(&window, noon_kst)?,
        "noon KST inside the operator's window"
    );
    let (window, by) = run_window(night, dir.path(), at("2099-01-01T06:00:00Z"));
    assert_eq!(
        (window.start.as_str(), by),
        (night.start.as_str(), None),
        "an ended override is not one"
    );
    std::fs::write(
        dir.path().join("court_auction_collect.window"),
        "start=9:00\nend=18:00\nuntil=2099-01-02T00:00:00+00:00\n",
    )?;
    assert_eq!(
        run_window(night, dir.path(), noon_kst).0.start,
        night.start,
        "an unreadable override is not one"
    );
    let whole_day = super::QuietHours {
        start: "00:00".to_owned(),
        end: "00:00".to_owned(),
    };
    assert!(
        in_quiet_hours(&whole_day, noon_kst)?
            && in_quiet_hours(&whole_day, at("2099-01-01T16:00:00Z"))?
    );
    Ok(())
}

#[test]
fn every_object_records_the_nights_requests_and_whether_it_was_refused() {
    // 2026-10-19: the first night's request count had to be estimated from its duration.
    let refused = night_summary(37, Some(&Stop::Refused("HTTP 429".to_owned())));
    assert_eq!(
        (
            refused["requests_sent"].as_u64(),
            refused["refused"].as_bool()
        ),
        (Some(37), Some(true))
    );
    let complete = night_summary(1_377, None);
    assert_eq!(complete["refused"].as_bool(), Some(false));
    assert!(complete["stopped"].is_null());
    let params = with_night(
        &json!({"operation": "searchControllerMain"}),
        1_125,
        &complete,
    );
    assert_eq!(params["operation"], "searchControllerMain");
    assert_eq!(params["requests_in_object"].as_u64(), Some(1_125));
    assert_eq!(params["night"]["requests_sent"].as_u64(), Some(1_377));
}
