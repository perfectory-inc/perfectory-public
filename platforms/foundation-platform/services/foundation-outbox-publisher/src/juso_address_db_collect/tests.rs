use std::time::Duration;

use chrono::{NaiveDate, TimeZone, Utc};
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;
use wiremock::matchers::{body_partial_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    base_first_day, check_file, days_of_month, last_day, listed, months, newest_full, plan,
    read_state, source_contract, Collect, State, Stop, Wanted, DAILY, FULL,
};
use crate::provider_request_spacing::ProviderRequestSpacing;
use foundation_shared_kernel::ids::IngestionRunId;

// Synthetic only: the year 2099, made-up file names and zip-looking bytes with no real rows.
fn day(text: &str) -> anyhow::Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(text, "%Y-%m-%d")?)
}

fn zip(tag: &str) -> Vec<u8> {
    let mut bytes = b"PK\x03\x04".to_vec();
    bytes.extend_from_slice(tag.as_bytes());
    bytes
}

fn month_row(month: &str, exists: bool) -> JsonValue {
    json!({"crtrYm": month, "crtrYmd": null, "isExist": if exists { "Y" } else { "N" },
           "fileNm": exists.then(|| format!("{month}_합성_전체분.zip")),
           "tmprFileNm": exists.then(|| format!("{month}ALLMTCHG00.zip"))})
}

fn day_row(date: &str, exists: bool) -> JsonValue {
    json!({"crtrYm": null, "crtrYmd": date, "isExist": if exists { "Y" } else { "N" },
           "fileNm": format!("{date}_dailymatchingdata.zip"),
           "tmprFileNm": format!("{date}_dailymatchingdata.zip")})
}

fn answer(months: &[JsonValue], days: &[JsonValue]) -> JsonValue {
    json!({"status": 200, "message": "OK",
           "results": {"allMonthFileList": months, "dayFileList": days}})
}

#[test]
fn the_contract_reads_both_file_kinds() -> anyhow::Result<()> {
    let contract = source_contract()?;
    assert_eq!(contract.file(FULL)?.req_type, "ALLMTCHG");
    assert_eq!(contract.file(DAILY)?.req_type, "DCM");
    assert_eq!(contract.item.rtl_dta_dtl_sn, "8");
    Ok(())
}

#[test]
fn the_base_is_the_newest_full_that_is_there() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let full = contract.file(FULL)?;
    let rows = listed(
        &answer(
            &[
                month_row("209909", true),
                month_row("209910", true),
                month_row("209911", false),
            ],
            &[],
        ),
        &full,
    )?;
    let base = newest_full(&rows, &full).ok_or_else(|| anyhow::anyhow!("no base"))?;
    assert_eq!((base.slug, base.period.as_str()), (FULL, "209910"));
    assert_eq!(base_first_day(&base.period)?, day("2099-10-01")?);
    assert!(newest_full(&rows[2..], &full).is_none());
    Ok(())
}

#[test]
fn day_files_are_the_asked_months_from_the_cursor_to_yesterday_oldest_first() -> anyhow::Result<()>
{
    let contract = source_contract()?;
    let daily = contract.file(DAILY)?;
    let rows = listed(
        &answer(
            &[],
            &[
                day_row("20991103", true),
                day_row("20991001", true),
                day_row("20991101", true),
                day_row("20991102", false),
                day_row("20991104", true),
            ],
        ),
        &daily,
    )?;
    let picked: Vec<String> = days_of_month(
        &rows,
        &daily,
        (2099, 11),
        day("2099-11-01")?,
        day("2099-11-03")?,
    )
    .into_iter()
    .map(|wanted| wanted.period)
    .collect();
    assert_eq!(picked, ["20991101", "20991103"]);
    Ok(())
}

#[test]
fn months_run_across_a_year() -> anyhow::Result<()> {
    assert_eq!(
        months(day("2099-11-20")?, day("2100-01-02")?),
        [(2099, 11), (2099, 12), (2100, 1)]
    );
    assert!(months(day("2099-11-20")?, day("2099-10-31")?).is_empty());
    Ok(())
}

#[test]
fn yesterday_is_counted_in_kst() -> anyhow::Result<()> {
    let late_utc = Utc
        .with_ymd_and_hms(2099, 11, 10, 16, 0, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("time"))?;
    assert_eq!(last_day(late_utc), day("2099-11-10")?);
    Ok(())
}

#[test]
fn a_file_that_is_not_a_zip_is_refused_and_the_leaf_is_the_providers_name() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let daily = contract.file(DAILY)?;
    let rows = listed(&answer(&[], &[day_row("20991101", true)]), &daily)?;
    let wanted: Wanted = days_of_month(
        &rows,
        &daily,
        (2099, 11),
        day("2099-11-01")?,
        day("2099-11-30")?,
    )
    .remove(0);
    assert!(check_file(&contract, &wanted, b"{\"error\":1}").is_err());
    check_file(&contract, &wanted, &zip("a"))?;
    let at = Utc
        .with_ymd_and_hms(2099, 11, 2, 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("time"))?;
    let plan = plan(&wanted, zip("a"), IngestionRunId::new(Uuid::new_v4()), at)?;
    assert!(plan
        .object_key
        .as_str()
        .starts_with("bronze/source=jusogokr__address_db_daily_change/"));
    assert!(plan
        .object_key
        .as_str()
        .ends_with("/20991101_dailymatchingdata.zip"));
    Ok(())
}

async fn server_with(lists: &[((i32, u32), JsonValue)], files: &[&str]) -> MockServer {
    let server = MockServer::start().await;
    for ((year, month), body) in lists {
        Mock::given(method("POST"))
            .and(path("/api/jst/selectAttrbDBDwldList"))
            .and(body_partial_json(
                json!({"rtlDtaDtlSn": "8", "year": year.to_string(),
                                           "month": format!("{month:02}"), "expand": "Y"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
    }
    for period in files {
        Mock::given(method("GET"))
            .and(path("/api/jst/download"))
            .and(query_param("stdde", *period))
            .and(query_param("regYmd", &period[..4]))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(zip(period)))
            .mount(&server)
            .await;
    }
    server
}

fn collect<'a>(
    contract: &'a super::SourceContract,
    server: &MockServer,
) -> anyhow::Result<Collect<'a>> {
    Ok(Collect {
        contract,
        http: reqwest::Client::new(),
        base_uri: server.uri(),
        spacing: ProviderRequestSpacing::try_new(Duration::from_millis(1))?,
        requests: 0,
        bytes: 0,
        landed: Vec::new(),
    })
}

fn now() -> anyhow::Result<chrono::DateTime<Utc>> {
    Utc.with_ymd_and_hms(2099, 11, 4, 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("time"))
}

#[tokio::test]
async fn a_first_run_lands_the_base_then_every_day_from_the_bases_month() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let october = answer(&[], &[day_row("20991001", true), day_row("20991031", true)]);
    let november = answer(
        &[month_row("209910", true), month_row("209911", false)],
        &[
            day_row("20991031", true),
            day_row("20991101", true),
            day_row("20991102", false),
            day_row("20991103", true),
        ],
    );
    let server = server_with(
        &[((2099, 10), october), ((2099, 11), november)],
        &["209910", "20991001", "20991031", "20991101", "20991103"],
    )
    .await;
    let mut run = collect(&contract, &server)?;
    let mut state = State {
        base_month: None,
        next_day: None,
    };
    let stop = run
        .run(&mut state, day("2099-11-03")?, None, None, now()?)
        .await?;
    assert_eq!(stop, Stop::Done);
    let landed: Vec<(String, String)> = run
        .landed
        .iter()
        .map(|file| {
            (
                file["kind"].as_str().unwrap_or_default().to_owned(),
                file["period"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        landed,
        [
            (FULL.to_owned(), "209910".to_owned()),
            (DAILY.to_owned(), "20991001".to_owned()),
            (DAILY.to_owned(), "20991031".to_owned()),
            (DAILY.to_owned(), "20991101".to_owned()),
            (DAILY.to_owned(), "20991103".to_owned()),
        ],
        "the base once, each day once, a day the list does not hold passed once a later one landed"
    );
    assert_eq!(state.base_month.as_deref(), Some("209910"));
    assert_eq!(state.next_day, Some(day("2099-11-04")?));
    Ok(())
}

#[tokio::test]
async fn a_later_run_lands_only_the_days_after_its_cursor_and_stops_at_the_cap(
) -> anyhow::Result<()> {
    let mut contract = source_contract()?;
    contract.per_run_cap.max_day_files_per_run = 1;
    let november = answer(
        &[month_row("209910", true)],
        &[
            day_row("20991101", true),
            day_row("20991102", true),
            day_row("20991103", true),
        ],
    );
    let server = server_with(&[((2099, 11), november)], &["20991102", "20991103"]).await;
    let mut run = collect(&contract, &server)?;
    let mut state = State {
        base_month: Some("209910".to_owned()),
        next_day: Some(day("2099-11-02")?),
    };
    let stop = run
        .run(&mut state, day("2099-11-03")?, None, None, now()?)
        .await?;
    assert_eq!(stop, Stop::FileCap);
    assert_eq!(run.landed.len(), 1);
    assert_eq!(run.landed[0]["period"], "20991102");
    assert_eq!(
        state.next_day,
        Some(day("2099-11-03")?),
        "the cursor stops where the run stopped"
    );
    Ok(())
}

#[tokio::test]
async fn a_download_that_is_not_a_zip_fails_and_keeps_the_cursor() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = server_with(
        &[((2099, 11), answer(&[], &[day_row("20991101", true)]))],
        &[],
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/api/jst/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"error": "none"})))
        .mount(&server)
        .await;
    let mut run = collect(&contract, &server)?;
    let mut state = State {
        base_month: Some("209910".to_owned()),
        next_day: Some(day("2099-11-01")?),
    };
    assert!(run
        .run(&mut state, day("2099-11-03")?, None, None, now()?)
        .await
        .is_err());
    assert_eq!(state.next_day, Some(day("2099-11-01")?));
    assert!(run.landed.is_empty());
    Ok(())
}

#[test]
fn a_missing_state_is_a_run_with_no_base() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("juso-address-db-{}", Uuid::new_v4()));
    let state = read_state(&dir)?;
    assert_eq!(state.base_month, None);
    assert_eq!(state.next_day, None);
    Ok(())
}
