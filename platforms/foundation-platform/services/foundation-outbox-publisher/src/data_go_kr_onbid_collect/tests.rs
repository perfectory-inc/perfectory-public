use std::{collections::BTreeSet, io::Read as _, sync::Mutex, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, SchemaProfile, SourceCatalogEntry,
};
use foundation_outbox::FileObjectStorage;
use foundation_shared_kernel::ids::SourceCatalogId;
use serde_json::{json, Value};
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    close_vanished, collect, item_key, item_pass_due, kst14, kst14_of, kst_date, listed_of,
    owed_details, persist, prune_media_memo, read_answer, settle_details, source_contract, Answer,
    Api, Coverage, Fetcher, ItemKey, ItemMark, LineOutcome, Listed, MediaRef, State, MEDIA_FIELDS,
    PREFIX,
};
use crate::provider_request_spacing::ProviderRequestSpacing;

// Synthetic only: no real item numbers, PNUs or addresses (public repository).
const KEY: &str = "synthetic-service-key-never-stored";

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_default()
}

fn rows(items: &[Value]) -> Value {
    json!({
        "header": {"resultCode": "00", "resultMsg": "NORMAL_CODE"},
        "body": {"items": {"item": items}, "totalCount": items.len(), "pageNo": 1}
    })
}

fn no_data() -> Value {
    json!({"result": {"resultCode": "03", "resultMsg": "NODATA_ERROR"}})
}

fn item(cltr: &str, condition: i64) -> Value {
    json!({"cltrMngNo": cltr, "pbctCdtnNo": condition, "ltnoPnu": "9999999999100010000", "pbctStatCd": "0001"})
}

fn key(cltr: &str, condition: i64) -> ItemKey {
    ItemKey {
        cltr_mng_no: cltr.to_owned(),
        pbct_cdtn_no: condition,
    }
}

#[test]
fn every_api_stays_within_its_share_of_the_gateway_limit_and_requests_are_spaced(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    for api in Api::ALL {
        let limit = contract.daily_limit(api)?;
        // The gateway's measured limit is 100,000 per API per day and the key has another user.
        assert!(limit > 0 && limit <= 100_000 / 5, "{}: {limit}", api.name());
    }
    assert!(contract.request_spacing_ms >= 100);
    assert!(
        contract.base_url.starts_with("https://"),
        "plain HTTP times out"
    );
    // The full item pass must fit its own daily limit at the contract's interval.
    let passes_per_day = 24 / contract.item_list.every_hours;
    let calls_per_pass =
        contract.property_codes().len() * contract.item_list.private_contract_flags.len();
    assert!(
        u64::try_from(passes_per_day)? * u64::try_from(calls_per_pass)?
            <= u64::from(contract.daily_limit(Api::ItemList)?),
        "the empty-answer floor of item passes alone exceeds the limit"
    );
    assert!(!super::CONTRACT_JSON.contains("serviceKey="));
    Ok(())
}

#[test]
fn an_answer_is_rows_empty_quota_or_refused() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let two = rows(&[item("A-1", 1), item("A-2", 2)]).to_string();
    assert_eq!(
        read_answer(&contract, 200, &two)?,
        Answer::Rows {
            items: vec![item("A-1", 1), item("A-2", 2)],
            total: 2
        }
    );
    // One item arrives as an object, not an array.
    let one = json!({"header": {"resultCode": "00"}, "body": {"items": {"item": item("A-1", 1)}, "totalCount": "1"}});
    assert_eq!(
        read_answer(&contract, 200, &one.to_string())?,
        Answer::Rows {
            items: vec![item("A-1", 1)],
            total: 1
        }
    );
    assert_eq!(
        read_answer(&contract, 200, &no_data().to_string())?,
        Answer::NoData
    );
    assert_eq!(
        read_answer(&contract, 429, "API token quota exceeded")?,
        Answer::QuotaRefused
    );
    // A missing parameter, an unregistered key (XML) and a server error must never read as "no items".
    let missing = json!({"result": {"resultCode": "11", "resultMsg": "NO_MANDATORY_REQUEST_PARAMETERS_ERROR"}});
    assert!(read_answer(&contract, 200, &missing.to_string()).is_err());
    assert!(read_answer(&contract, 200, "<OpenAPI_ServiceResponse><returnAuthMsg>SERVICE_KEY_IS_NOT_REGISTERED_ERROR</returnAuthMsg></OpenAPI_ServiceResponse>").is_err());
    assert!(read_answer(&contract, 500, "").is_err());
    Ok(())
}

#[test]
fn a_closed_item_is_owed_once_and_the_first_pass_closes_nothing() {
    let first = at("2099-01-01T00:00:00Z");
    let mut state = State::default();
    let opened = BTreeSet::from([key("A-1", 1), key("A-2", 2)]);
    assert_eq!(close_vanished(&mut state, opened, first), 0);
    let second = at("2099-01-01T03:00:00Z");
    assert_eq!(
        close_vanished(&mut state, BTreeSet::from([key("A-2", 2)]), second),
        1
    );
    assert_eq!(state.pending_results[0].key, key("A-1", 1));
    // Still absent on the next pass: already owed, not owed twice.
    assert_eq!(
        close_vanished(&mut state, BTreeSet::from([key("A-2", 2)]), second),
        0
    );
    assert_eq!(state.pending_results.len(), 1);
}

#[test]
fn the_budget_day_is_the_korean_day() {
    // 15:00 UTC is midnight in Korea.
    assert_ne!(
        kst_date(at("2099-01-01T14:59:59Z")),
        kst_date(at("2099-01-01T15:00:00Z"))
    );
    let mut state = State::default();
    state.roll_budget(at("2099-01-01T14:00:00Z"));
    state.budget.calls.insert("item_list".to_owned(), 999);
    state.roll_budget(at("2099-01-01T14:30:00Z"));
    assert_eq!(state.budget.calls.get("item_list"), Some(&999));
    state.roll_budget(at("2099-01-01T15:00:00Z"));
    assert!(state.budget.calls.is_empty());
}

#[test]
fn an_item_key_needs_both_numbers() -> anyhow::Result<()> {
    assert_eq!(
        item_key(&json!({"cltrMngNo": " A-1 ", "pbctCdtnNo": "7"}))?,
        key("A-1", 7)
    );
    assert!(item_key(&json!({"cltrMngNo": "A-1"})).is_err());
    assert!(item_key(&json!({"cltrMngNo": "", "pbctCdtnNo": 1})).is_err());
    Ok(())
}

/// A provider that answers 03 to everything except the mocks mounted before it.
async fn provider(items_first: &[Value]) -> anyhow::Result<MockServer> {
    let server = MockServer::start().await;
    let contract = source_contract()?;
    Mock::given(method("GET"))
        .and(path(contract.item_list.path.clone()))
        .and(query_param("prptDivCd", "0007"))
        .and(query_param("pvctTrgtYn", "N"))
        .and(query_param("serviceKey", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_json(rows(items_first)))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(contract.item_bid_result.path.clone()))
        .and(query_param("cltrMngNo", "A-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rows(&[
            json!({"cltrMngNo": "A-1", "pbctCdtnNo": 1, "pbctStatCd": "0010", "scfbAmt": 1000}),
        ])))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(no_data()))
        .with_priority(10)
        .mount(&server)
        .await;
    Ok(server)
}

fn spacing() -> anyhow::Result<ProviderRequestSpacing> {
    ProviderRequestSpacing::try_new(Duration::from_millis(1))
}

#[tokio::test]
async fn a_round_that_leaves_the_list_gets_its_result_asked_once() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let mut state = State::default();

    let first_run = at("2099-01-01T00:00:00Z");
    let server = provider(&[item("A-1", 1), item("A-2", 2)]).await?;
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(&contract, &mut fetcher, &mut state, first_run).await?;
    assert!(report.item_pass_complete);
    assert_eq!(report.open_items, Some(2));
    assert_eq!(report.closed_items, 0);
    // Every list API wrote lines; none of them holds the key.
    for api in [
        Api::AnnouncementList,
        Api::AnnouncementBidResultList,
        Api::ItemList,
    ] {
        let lines = fetcher.lines.get(&api).map_or(0, Vec::len);
        assert!(lines > 0, "{}", api.name());
    }
    assert!(fetcher
        .lines
        .values()
        .flatten()
        .all(|line| !line.contains(KEY)));
    let item_calls =
        contract.property_codes().len() * contract.item_list.private_contract_flags.len();
    assert_eq!(
        state.budget.calls.get("item_list").copied(),
        Some(u32::try_from(item_calls)?)
    );

    // An hour later the item list is not due; three hours later it is, and A-1 has closed.
    assert!(!item_pass_due(
        &contract,
        &state,
        at("2099-01-01T01:00:00Z")
    ));
    let later = at("2099-01-01T03:00:00Z");
    assert!(item_pass_due(&contract, &state, later));
    let server = provider(&[item("A-2", 2)]).await?;
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(&contract, &mut fetcher, &mut state, later).await?;
    assert_eq!(report.closed_items, 1);
    assert_eq!(report.results_received, 1);
    assert!(state.pending_results.is_empty());
    assert_eq!(
        fetcher.lines.get(&Api::ItemBidResult).map_or(0, Vec::len),
        1
    );
    Ok(())
}

/// A provider listing `listed` and answering every 물건상세 with a synthetic photo URL.
async fn detail_provider(listed: &[Value]) -> anyhow::Result<MockServer> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(contract.item_list.path.clone()))
        .and(query_param("prptDivCd", "0007"))
        .and(query_param("pvctTrgtYn", "N"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rows(listed)))
        .with_priority(1)
        .mount(&server)
        .await;
    // 물건상세 of the open condition, with a synthetic photo URL (no real data).
    Mock::given(method("GET"))
        .and(path(contract.item_detail.path.clone()))
        .and(query_param("cltrMngNo", "A-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rows(&[json!({
            "cltrMngNo": "A-1", "pbctCdtnNo": 1,
            "potoUrlList": [{"potoUrl": "https://example.test/synthetic.jpg"}]
        })])))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(no_data()))
        .with_priority(10)
        .mount(&server)
        .await;
    Ok(server)
}

fn listed_item(cltr: &str, condition: i64, modified: &str, bid_end: &str) -> Value {
    let mut row = item(cltr, condition);
    row["mdfcnDt"] = json!(modified);
    row["cltrBidEndDt"] = json!(bid_end);
    row
}

fn complete(photos: u32) -> LineOutcome {
    LineOutcome {
        complete: true,
        photos_complete: true,
        photos,
    }
}

#[tokio::test]
async fn an_open_item_is_detailed_until_its_media_landed_and_again_when_the_provider_changes_it(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server =
        detail_provider(&[listed_item("A-1", 1, "20990101090000", "209901101000")]).await?;

    let mut state = State::default();
    let first = at("2099-01-01T00:00:00Z");
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(&contract, &mut fetcher, &mut state, first).await?;
    assert_eq!(report.details_received, 1);
    assert_eq!(report.details_owed, 0);
    assert_eq!(report.details_errored, 0);
    assert_eq!(report.fetched.len(), 1);
    assert!(!report.fetched[0].refresh);
    // Received is not complete: nothing is marked until the media step says its media landed.
    assert!(state.items.is_empty());
    let detail_lines = fetcher
        .lines
        .get(&Api::ItemDetail)
        .cloned()
        .unwrap_or_default();
    assert_eq!(detail_lines.len(), 1);
    // The detail line kept the photo URL and never the service key.
    assert!(detail_lines[0].contains("synthetic.jpg"));
    assert!(detail_lines[0].contains("cltrMngNo"));
    assert!(!detail_lines[0].contains(KEY));

    // Its media did not all land (a cap, a timeout): an hour later it is asked again.
    assert_eq!(
        settle_details(&mut state, &report.fetched, &[LineOutcome::default()]),
        0
    );
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(
        &contract,
        &mut fetcher,
        &mut state,
        at("2099-01-01T01:00:00Z"),
    )
    .await?;
    assert_eq!(report.details_received, 1);
    assert_eq!(
        settle_details(&mut state, &report.fetched, &[complete(2)]),
        1
    );
    assert_eq!(state.items["A-1"].photos, 2);

    // Three hours later the item list is due again with the same row: detail is not re-asked.
    let later = at("2099-01-01T03:00:00Z");
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(&contract, &mut fetcher, &mut state, later).await?;
    assert!(report.item_pass_complete);
    assert_eq!(report.details_received, 0);
    assert!(!fetcher.lines.contains_key(&Api::ItemDetail));

    // A status change is not a change of the detail; the provider's modification stamp is.
    let mut status_only = listed_item("A-1", 1, "20990101090000", "209901101000");
    status_only["pbctStatCd"] = json!("0002");
    let server = detail_provider(&[status_only]).await?;
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(
        &contract,
        &mut fetcher,
        &mut state,
        at("2099-01-01T06:00:00Z"),
    )
    .await?;
    assert_eq!(report.details_received, 0);
    let server =
        detail_provider(&[listed_item("A-1", 1, "20990101150000", "209901101000")]).await?;
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(
        &contract,
        &mut fetcher,
        &mut state,
        at("2099-01-01T09:00:00Z"),
    )
    .await?;
    assert_eq!(report.details_received, 1);
    assert_eq!(report.details_refreshed, 1);
    assert!(report.fetched[0].refresh);
    Ok(())
}

fn listed(digest: &str, ends: Option<&str>) -> Listed {
    Listed {
        bid_ends_kst: ends.map(str::to_owned),
        digest: digest.to_owned(),
    }
}

fn item_mark(seen: &[&str], photos: u32) -> ItemMark {
    ItemMark {
        seen: seen.iter().map(|digest| (*digest).to_owned()).collect(),
        photos,
        media_fields: MEDIA_FIELDS,
    }
}

#[test]
fn an_item_completed_before_the_maps_were_media_is_fetched_once_more_without_a_refresh() {
    let mut state = State::default();
    let open = BTreeSet::from([key("A-1", 1)]);
    state
        .listed
        .insert(key("A-1", 1), listed("d1", Some("20990110100000")));
    state.items.insert(
        "A-1".to_owned(),
        ItemMark {
            media_fields: 0,
            ..item_mark(&["d1"], 3)
        },
    );
    let owed = owed_details(&mut state, &open, at("2099-01-01T00:00:00Z"));
    assert_eq!(owed.len(), 1);
    // Not a refresh: the photos it already stored are not downloaded again.
    assert!(!owed[0].refresh);
    state.items.insert("A-1".to_owned(), item_mark(&["d1"], 3));
    assert!(owed_details(&mut state, &open, at("2099-01-01T00:00:00Z")).is_empty());
}

#[tokio::test]
async fn bid_info_is_asked_once_per_item_and_announcement_details_once_per_announcement(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(contract.item_list.path.clone()))
        .and(query_param("prptDivCd", "0007"))
        .and(query_param("pvctTrgtYn", "N"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rows(&[
            listed_item("A-1", 1, "20990101090000", "209901101000"),
            listed_item("A-1", 2, "20990101090000", "209901201000"),
        ])))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(contract.announcement_list.path.clone()))
        .and(query_param("prptDivCd", "0007"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rows(&[
            json!({"pbancMngNo": "209901-00001-00", "onbidPbancNo": 1, "pbctNo": 1}),
        ])))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(no_data()))
        .with_priority(10)
        .mount(&server)
        .await;

    let mut state = State::default();
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(
        &contract,
        &mut fetcher,
        &mut state,
        at("2099-01-01T00:00:00Z"),
    )
    .await?;
    // One item with two rounds: one bid-info call, through its earliest-closing round.
    assert_eq!(report.bid_infos_received, 1);
    assert!(state.bid_infos.contains_key("A-1"));
    let asked = fetcher
        .lines
        .get(&Api::ItemBidInfo)
        .cloned()
        .unwrap_or_default();
    assert_eq!(asked.len(), 1);
    assert!(asked[0].contains("\"pbctCdtnNo\":\"1\""));
    // The listed announcement is asked once; it is complete only after its attachments settle.
    assert_eq!(report.announcements_received, 1);
    assert!(state.announcements.is_empty());
    assert_eq!(
        super::settle_announcements(&mut state, &report.announcements_fetched, &[complete(0)]),
        1
    );

    // An hour later nothing changed: neither is asked again.
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(
        &contract,
        &mut fetcher,
        &mut state,
        at("2099-01-01T01:00:00Z"),
    )
    .await?;
    assert_eq!(report.bid_infos_received, 0);
    assert_eq!(report.announcements_received, 0);
    assert!(!fetcher.lines.contains_key(&Api::ItemBidInfo));
    assert!(!fetcher.lines.contains_key(&Api::AnnouncementDetail));
    Ok(())
}

fn owed_keys(
    state: &mut State,
    open: &BTreeSet<ItemKey>,
    now: DateTime<Utc>,
) -> Vec<(ItemKey, bool)> {
    owed_details(state, open, now)
        .into_iter()
        .map(|owed| (owed.key, owed.refresh))
        .collect()
}

#[test]
fn one_detail_serves_every_round_of_an_item_until_a_round_shows_a_new_stamp() {
    // 2099-01-05 00:00 UTC = 09:00 KST. A has three rounds (one ended), B one.
    let now = at("2099-01-05T00:00:00Z");
    let mut state = State::default();
    let mut open = BTreeSet::from([key("A", 1), key("A", 2), key("A", 3), key("B", 1)]);
    state
        .listed
        .insert(key("A", 1), listed("m1", Some("20990301100000")));
    state
        .listed
        .insert(key("A", 2), listed("m1", Some("20990201100000")));
    state
        .listed
        .insert(key("A", 3), listed("m1", Some("20990104100000")));
    state
        .listed
        .insert(key("B", 1), listed("m1", Some("20990401100000")));

    // One detail per item, asked through its earliest-closing round whose bid has not ended.
    assert_eq!(
        owed_keys(&mut state, &open, now),
        [(key("A", 2), false), (key("B", 1), false)]
    );
    let owed = owed_details(&mut state, &open, now);
    let fetched = owed
        .iter()
        .enumerate()
        .map(|(line, owed)| super::Fetched {
            key: owed.key.clone(),
            seen: owed.seen.clone(),
            refresh: owed.refresh,
            line,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        settle_details(&mut state, &fetched, &[complete(3), LineOutcome::default()]),
        1
    );
    // A is complete for all three rounds; B's media did not land, so B alone is owed.
    assert_eq!(owed_keys(&mut state, &open, now), [(key("B", 1), false)]);
    assert_eq!(
        Coverage::of(
            &State {
                open_items: Some(open.clone()),
                ..state.clone()
            },
            now
        )
        .covered,
        2,
        "A's two rounds whose bid has not ended"
    );

    // A new round with a stamp A was complete at is not a reason to ask again.
    open.insert(key("A", 4));
    state
        .listed
        .insert(key("A", 4), listed("m1", Some("20990501100000")));
    assert_eq!(owed_keys(&mut state, &open, now), [(key("B", 1), false)]);
    // A new round with a new stamp is, and so is an existing round whose stamp changed.
    for (round, digest) in [(5, "m1"), (1, "m2")] {
        let mut changed = state.clone();
        let mut open = open.clone();
        open.insert(key("A", round));
        changed
            .listed
            .insert(key("A", round), listed(digest, Some("20990601100000")));
        let expect_a = digest != "m1";
        let owed = owed_keys(&mut changed, &open, now);
        assert_eq!(
            owed.iter()
                .any(|(owed, refresh)| owed.cltr_mng_no == "A" && *refresh),
            expect_a,
            "round {round} at {digest}"
        );
    }
    // A round that closes (leaves the list) asks nothing.
    open.remove(&key("A", 1));
    assert_eq!(owed_keys(&mut state, &open, now), [(key("B", 1), false)]);
}

#[test]
fn the_per_round_marks_of_adr_0330_fold_into_their_items() -> anyhow::Result<()> {
    let old: State = serde_json::from_value(json!({
        "details": [[{"cltr_mng_no": "A", "pbct_cdtn_no": 1}, {"digest": "m1", "photos": 2}]]
    }))?;
    let mut state = old;
    let open = BTreeSet::from([key("A", 1), key("A", 2)]);
    state.listed.insert(key("A", 1), listed("m1", None));
    state.listed.insert(key("A", 2), listed("m1", None));
    // Folded at the media fields of its time (none of root ADR-0340's maps): asked once more, and
    // not as a refresh, so the photos it stored are not downloaded again.
    let owed = owed_details(&mut state, &open, at("2099-01-01T00:00:00Z"));
    assert_eq!(owed.len(), 1);
    assert!(!owed[0].refresh);
    assert!(state.details.is_empty());
    assert_eq!(
        state.items["A"],
        ItemMark {
            media_fields: 0,
            ..item_mark(&["m1"], 2)
        }
    );
    // The folded state no longer writes the per-round field.
    assert!(!serde_json::to_string(&state)?.contains("\"details\""));
    Ok(())
}

#[test]
fn details_are_owed_open_before_ended_new_before_changed_earliest_end_first() {
    // 2099-01-05 00:00 UTC = 09:00 KST.
    let now = at("2099-01-05T00:00:00Z");
    let mut state = State::default();
    let open = BTreeSet::from([
        key("ENDED", 1),
        key("LATE", 1),
        key("SOON", 1),
        key("NO-END", 1),
        key("CHANGED", 1),
        key("SAME", 1),
    ]);
    state
        .listed
        .insert(key("ENDED", 1), listed("d", Some("20990104120000")));
    state
        .listed
        .insert(key("LATE", 1), listed("d", Some("20990301100000")));
    state
        .listed
        .insert(key("SOON", 1), listed("d", Some("20990105100000")));
    state.listed.insert(key("NO-END", 1), listed("d", None));
    state
        .listed
        .insert(key("CHANGED", 1), listed("new", Some("20990105100000")));
    state
        .listed
        .insert(key("SAME", 1), listed("d", Some("20990105100000")));
    state
        .items
        .insert("CHANGED".to_owned(), item_mark(&["old"], 1));
    state.items.insert("SAME".to_owned(), item_mark(&["d"], 1));

    let owed = owed_details(&mut state, &open, now);
    let order = owed
        .iter()
        .map(|owed| owed.key.cltr_mng_no.as_str())
        .collect::<Vec<_>>();
    assert_eq!(order, ["SOON", "LATE", "NO-END", "CHANGED", "ENDED"]);
    assert!(owed
        .iter()
        .all(|owed| owed.refresh == (owed.key.cltr_mng_no == "CHANGED")));
}

#[test]
fn an_item_owed_only_appraisals_waits_behind_every_item_still_owed_photos() {
    // 2099-01-05 00:00 UTC = 09:00 KST. SOON closes first, LATE last.
    let now = at("2099-01-05T00:00:00Z");
    let mut state = State::default();
    let open = BTreeSet::from([key("SOON", 1), key("LATE", 1)]);
    state
        .listed
        .insert(key("SOON", 1), listed("d", Some("20990105100000")));
    state
        .listed
        .insert(key("LATE", 1), listed("d", Some("20990301100000")));
    let fetched = owed_details(&mut state, &open, now)
        .into_iter()
        .enumerate()
        .map(|(line, owed)| super::Fetched {
            key: owed.key,
            seen: owed.seen,
            refresh: owed.refresh,
            line,
        })
        .collect::<Vec<_>>();
    // SOON's photos landed, its appraisal did not; LATE got nothing.
    let photos_only = LineOutcome {
        complete: false,
        photos_complete: true,
        photos: 3,
    };
    assert_eq!(
        settle_details(&mut state, &fetched, &[photos_only, LineOutcome::default()]),
        0
    );
    let order = |state: &mut State, open: &BTreeSet<ItemKey>| {
        owed_details(state, open, now)
            .into_iter()
            .map(|owed| owed.key.cltr_mng_no)
            .collect::<Vec<_>>()
    };
    assert_eq!(order(&mut state, &open), ["LATE", "SOON"]);
    // A new stamp on SOON may bring new photos: it is owed photos again.
    let mut changed = state.clone();
    changed
        .listed
        .insert(key("SOON", 1), listed("d2", Some("20990105100000")));
    assert_eq!(order(&mut changed, &open), ["SOON", "LATE"]);
    // Its appraisal lands later: complete, and no longer remembered as photos-only.
    assert_eq!(settle_details(&mut state, &fetched[..1], &[complete(3)]), 1);
    assert!(state.photos_landed.is_empty());
    assert_eq!(order(&mut state, &open), ["LATE"]);
}

#[test]
fn a_mark_made_before_the_list_was_read_adopts_its_digest_instead_of_a_refetch() {
    let mut state = State::default();
    state.items.insert("A".to_owned(), item_mark(&[], 0));
    let open = BTreeSet::from([key("A", 1)]);
    assert!(owed_details(&mut state, &open, at("2099-01-01T00:00:00Z")).is_empty());
    state.listed.insert(key("A", 1), listed("d", None));
    assert!(owed_details(&mut state, &open, at("2099-01-01T00:00:00Z")).is_empty());
    assert_eq!(state.items["A"].seen, BTreeSet::from(["d".to_owned()]));
}

#[test]
fn the_list_digest_reads_only_the_refresh_fields_and_the_bid_end_is_korean_time(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let a = listed_of(
        &contract.item_detail,
        &listed_item("A", 1, "20990101090000", "209901101000"),
    );
    let mut status = listed_item("A", 1, "20990101090000", "209901101000");
    status["pbctStatCd"] = json!("0010");
    assert_eq!(listed_of(&contract.item_detail, &status), a);
    let modified = listed_of(
        &contract.item_detail,
        &listed_item("A", 1, "20990102090000", "209901101000"),
    );
    assert_ne!(modified.digest, a.digest);
    assert_eq!(a.bid_ends_kst.as_deref(), Some("20990110100000"));
    assert_eq!(
        kst14(&json!("20990110100030")).as_deref(),
        Some("20990110100030")
    );
    assert_eq!(kst14(&json!("2099-01-10")), None);
    assert_eq!(kst14_of(at("2099-01-10T01:00:00Z")), "20990110100000");
    Ok(())
}

#[test]
fn coverage_counts_the_conditions_whose_bid_has_not_ended() {
    let now = at("2099-01-05T00:00:00Z");
    let mut state = State {
        open_items: Some(BTreeSet::from([
            key("A", 1),
            key("B", 1),
            key("C", 1),
            key("D", 1),
        ])),
        ..State::default()
    };
    state
        .listed
        .insert(key("D", 1), listed("d", Some("20990101000000")));
    state.items.insert("A".to_owned(), item_mark(&["d"], 3));
    state.items.insert("B".to_owned(), item_mark(&["d"], 0));
    state.items.insert("D".to_owned(), item_mark(&["d"], 3));
    let coverage = Coverage::of(&state, now);
    assert_eq!(
        (coverage.open, coverage.covered, coverage.with_photos),
        (3, 2, 1)
    );
    let text = coverage.prometheus(now);
    assert!(text.contains("foundation_onbid_item_detail_coverage_ratio 0.6666"));
    assert!(text.contains("foundation_onbid_item_detail_open_conditions 3\n"));
    assert!(Coverage::default()
        .prometheus(now)
        .contains("coverage_ratio 1\n"));
}

#[test]
fn the_media_memo_keeps_the_open_items_only_and_the_state_round_trips() -> anyhow::Result<()> {
    let mut state = State {
        open_items: Some(BTreeSet::from([key("A", 1)])),
        ..State::default()
    };
    let known = |cltr: &str| MediaRef {
        sha256: "a".repeat(64),
        media_format: "jpg".to_owned(),
        size_bytes: 1,
        cltr_mng_no: cltr.to_owned(),
    };
    state
        .media
        .insert("https://www.onbid.co.kr/a.jpg".to_owned(), known("A"));
    state
        .media
        .insert("https://www.onbid.co.kr/b.jpg".to_owned(), known("GONE"));
    state.items.insert("A".to_owned(), item_mark(&["d"], 1));
    state
        .listed
        .insert(key("A", 1), listed("d", Some("20990101000000")));
    prune_media_memo(&mut state);
    assert_eq!(state.media.len(), 1);

    let text = serde_json::to_string(&state)?;
    let back: State = serde_json::from_str(&text)?;
    assert_eq!(back, state);
    // A state from before root ADR-0330 (`detailed`, no details) loads with nothing complete.
    let old: State =
        serde_json::from_value(json!({"detailed": [{"cltr_mng_no": "A", "pbct_cdtn_no": 1}]}))?;
    assert!(old.details.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_spent_budget_stops_the_api_and_leaves_the_pass_incomplete() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let now = at("2099-01-01T00:00:00Z");
    let mut state = State::default();
    state.roll_budget(now);
    state
        .budget
        .calls
        .insert("item_list".to_owned(), contract.daily_limit(Api::ItemList)?);
    let server = provider(&[item("A-1", 1)]).await?;
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    let report = collect(&contract, &mut fetcher, &mut state, now).await?;
    assert!(report.item_pass_attempted && !report.item_pass_complete);
    assert!(report.stopped_by_budget.contains(&"item_list".to_owned()));
    assert!(state.open_items.is_none() && state.last_complete_item_pass_utc.is_none());
    assert!(!fetcher.lines.contains_key(&Api::ItemList));
    Ok(())
}

#[tokio::test]
async fn the_gateway_quota_answer_waits_for_tomorrow_and_a_refusal_fails_the_run(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let now = at("2099-01-01T00:00:00Z");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(contract.announcement_list.path.clone()))
        .respond_with(ResponseTemplate::new(429).set_body_string("API token quota exceeded"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(no_data()))
        .with_priority(10)
        .mount(&server)
        .await;
    let mut state = State::default();
    let mut fetcher = Fetcher::new(&contract, &server.uri(), KEY, spacing()?)?;
    collect(&contract, &mut fetcher, &mut state, now).await?;
    assert!(state.budget.quota_refused.contains("announcement_list"));
    assert_eq!(
        state.budget.calls.get("announcement_list").copied(),
        Some(1)
    );

    let refusing = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": {"resultCode": "30", "resultMsg": "SERVICE_KEY_IS_NOT_REGISTERED_ERROR"}})))
        .mount(&refusing)
        .await;
    let mut fetcher = Fetcher::new(&contract, &refusing.uri(), KEY, spacing()?)?;
    let error = collect(&contract, &mut fetcher, &mut State::default(), now)
        .await
        .err()
        .map(|error| format!("{error:#}"))
        .unwrap_or_default();
    assert!(
        error.contains("SERVICE_KEY_IS_NOT_REGISTERED_ERROR"),
        "{error}"
    );
    assert!(!error.contains(KEY));
    Ok(())
}

#[derive(Default)]
struct RecordingUow {
    runs: Mutex<Vec<IngestionRun>>,
    objects: Mutex<Vec<BronzeObject>>,
}

fn poisoned(_: impl std::fmt::Debug) -> CollectionError {
    CollectionError::Infrastructure("lock poisoned".to_owned())
}

#[async_trait]
impl BronzeIngestUnitOfWork for RecordingUow {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        // An upsert returns the source's one id, as the database does.
        let mut source = entry.clone();
        source.id = SourceCatalogId::new(Uuid::new_v5(&Uuid::NAMESPACE_OID, entry.slug.as_bytes()));
        Ok(source)
    }

    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError> {
        self.runs.lock().map_err(poisoned)?.push(run.clone());
        Ok(run.clone())
    }

    async fn complete_ingestion_run(
        &self,
        command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        self.runs
            .lock()
            .map_err(poisoned)?
            .iter()
            .find(|run| run.id == command.id)
            .cloned()
            .ok_or_else(|| CollectionError::IngestionRunNotFound(command.id.to_string()))
    }

    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(self
            .objects
            .lock()
            .map_err(poisoned)?
            .iter()
            .find(|o| {
                o.source_catalog_id == source_catalog_id && o.object_key.as_str() == object_key
            })
            .cloned())
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        self.objects.lock().map_err(poisoned)?.push(object.clone());
        Ok(object.clone())
    }

    async fn upsert_schema_profile(
        &self,
        profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError> {
        Ok(profile.clone())
    }
}

#[tokio::test]
async fn one_api_of_one_run_is_one_gzip_object_of_its_lines() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let now = at("2099-01-02T03:04:05Z");
    let root = std::env::temp_dir().join(format!("onbid-{}", Uuid::new_v4()));
    let storage = FileObjectStorage::new(root.join("bronze"))?;
    let uow = RecordingUow::default();
    let lines = vec![
        r#"{"operation":"getRlstCltrList2","body":"{}"}"#.to_owned(),
        "{}".to_owned(),
    ];
    let object = persist(
        &contract,
        &root.join("out"),
        Api::ItemList,
        &lines,
        now,
        Some((&uow, &storage)),
    )
    .await?;
    assert!(object.written);
    assert_eq!(
        object.object_key,
        "bronze/source=datagokr__onbid_real_estate_item/item-list-20990102t030405z.gz"
    );
    let mut text = String::new();
    flate2::read::GzDecoder::new(
        std::fs::read(root.join("bronze").join(&object.object_key))?.as_slice(),
    )
    .read_to_string(&mut text)?;
    assert_eq!(text, format!("{}\n{}\n", lines[0], lines[1]));
    assert_eq!(
        std::fs::read(root.join("out").join(&object.local_path))?,
        std::fs::read(root.join("bronze").join(&object.object_key))?
    );
    // A retry of the same run is the committer's idempotent success, not a second object.
    persist(
        &contract,
        &root.join("out"),
        Api::ItemList,
        &lines,
        now,
        Some((&uow, &storage)),
    )
    .await?;
    assert_eq!(
        uow.objects
            .lock()
            .map_err(|_| anyhow::anyhow!("lock"))?
            .len(),
        1
    );
    std::fs::remove_dir_all(&root)?;
    assert!(PREFIX.starts_with("FOUNDATION_PLATFORM_"));
    Ok(())
}
