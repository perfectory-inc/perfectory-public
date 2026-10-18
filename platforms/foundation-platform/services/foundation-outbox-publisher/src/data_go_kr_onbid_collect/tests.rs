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
    close_vanished, collect, item_key, item_pass_due, kst_date, persist, read_answer,
    source_contract, Answer, Api, Fetcher, ItemKey, State, PREFIX,
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
fn every_api_stays_within_the_development_limit_and_requests_are_spaced() -> anyhow::Result<()> {
    let contract = source_contract()?;
    for api in Api::ALL {
        let limit = contract.daily_limit(api)?;
        assert!(limit > 0 && limit <= 1000, "{}: {limit}", api.name());
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
