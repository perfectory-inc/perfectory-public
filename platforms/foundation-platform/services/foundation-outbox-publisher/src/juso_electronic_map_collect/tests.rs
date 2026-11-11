use std::time::Duration;

use chrono::{NaiveDate, TimeZone, Utc};
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    answer_code, ask, check_file, classify, code_deadline, last_day, next_stop, plan,
    provider_file_id, quarantine, read_cursor, read_header, source_contract, take_records, windows,
    Outcome, Received, Stop,
};
use foundation_shared_kernel::ids::IngestionRunId;

// Synthetic only: made-up file names and zip-looking bytes with no real rows (public repository).
const KEY: &str = "SYNTHETIC-KEY-0000";

fn day(text: &str) -> anyhow::Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(text, "%Y-%m-%d")?)
}

fn zip(tag: &str) -> Vec<u8> {
    let mut bytes = b"PK\x03\x04".to_vec();
    bytes.extend_from_slice(tag.as_bytes());
    bytes
}

/// One record as the guide lays it out (§3.2): 100 bytes of fixed-width header, then the file.
fn record(sequence: u8, base: &str, name: &str, code: &str, retry: char, file: &[u8]) -> Vec<u8> {
    let mut header = format!(
        "{sequence:02}{base}{name:<50}{size:010}P0000{code}{retry}{base}",
        size = file.len()
    )
    .into_bytes();
    header.resize(100, b' ');
    header.extend_from_slice(file);
    header
}

/// A record without a file, as the provider's own client reads it (ADSReceiver.makeReceiveData): the
/// file name is blank and no bytes follow. Its size and dates are what such a record may carry.
fn day_without_file(sequence: u8, base: &str, size: u64, result: &str, code: &str) -> Vec<u8> {
    let mut header = format!(
        "{sequence:02}{base}{blank:<50}{size:010}{result}{code}N{base}",
        blank = ""
    )
    .into_bytes();
    header.resize(100, b' ');
    header
}

#[test]
fn the_contract_reads_and_every_request_code_serves_a_kind() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let kinds: usize = contract
        .request_codes
        .keys()
        .map(|code| contract.kinds_of(code).map(|kinds| kinds.len()))
        .sum::<anyhow::Result<usize>>()?;
    assert_eq!(kinds, 11, "the catalog's 11 kinds");
    assert_eq!(contract.kinds_of("300003")?.len(), 3);
    assert_eq!(contract.kinds_of("300006")?.len(), 5);
    Ok(())
}

#[test]
fn a_header_is_read_at_the_guides_offsets() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let bytes = record(
        1,
        "20990101",
        "AlterD.JUSURB.20990101.ZIP",
        "300001",
        'N',
        &zip("a"),
    );
    let (received, size) = read_header(&contract.answer, &bytes[..100])?;
    assert_eq!(size, 5);
    assert_eq!(received.sequence, "01");
    assert_eq!(received.base_date, "20990101");
    assert_eq!(received.file_name, "AlterD.JUSURB.20990101.ZIP");
    assert_eq!(received.result_code, "P0000");
    assert_eq!(received.request_code, "300001");
    assert!(!received.retry);
    assert_eq!(received.created_date, "20990101");
    Ok(())
}

#[test]
fn records_split_only_where_whole_and_keep_the_rest() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let mut body = record(
        1,
        "20990101",
        "AlterD.JUSURB.20990101.ZIP",
        "300001",
        'N',
        &zip("one"),
    );
    body.extend(record(
        2,
        "20990102",
        "AlterD.JUSURB.20990102.ZIP",
        "300001",
        'Y',
        &zip("two"),
    ));
    let cut = body.len() - 3;
    let (files, used) = take_records(&contract.answer, &body[..cut])?;
    assert_eq!(files.len(), 1, "the second record is not whole yet");
    assert_eq!(files[0].bytes, zip("one"));
    let (rest, _) = take_records(&contract.answer, &body[used..])?;
    assert_eq!(rest.len(), 1);
    assert!(rest[0].retry);
    assert_eq!(rest[0].bytes, zip("two"));
    Ok(())
}

#[test]
fn a_header_that_announces_more_than_the_provider_limit_is_refused() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let mut header = format!(
        "01{d}{n:<50}{s:010}P0000300001N{d}",
        d = "20990101",
        n = "AlterD.JUSURB.20990101.ZIP",
        s = 9_999_999_999_u64
    )
    .into_bytes();
    header.resize(100, b' ');
    assert!(read_header(&contract.answer, &header).is_err());
    Ok(())
}

#[test]
fn result_codes_mean_what_the_contract_says() -> anyhow::Result<()> {
    let codes = source_contract()?.result_codes;
    assert_eq!(classify(&codes, "P0000"), Outcome::Ok);
    assert_eq!(classify(&codes, "E1001"), Outcome::NoFiles);
    for refused in ["E0001", "E0002", "E0003", "E0004", "E1008"] {
        assert_eq!(classify(&codes, refused), Outcome::Refused, "{refused}");
    }
    assert_eq!(classify(&codes, "E1999"), Outcome::Provider);
    assert_eq!(classify(&codes, "P1000"), Outcome::Ours);
    assert_eq!(
        classify(&codes, "Z9999"),
        Outcome::Ours,
        "an unknown code fails the run"
    );
    Ok(())
}

#[test]
fn windows_run_from_the_cursor_to_yesterday() -> anyhow::Result<()> {
    let all = windows(day("2099-01-01")?, day("2099-01-25")?, 10);
    assert_eq!(
        all,
        vec![
            (day("2099-01-01")?, day("2099-01-10")?),
            (day("2099-01-11")?, day("2099-01-20")?),
            (day("2099-01-21")?, day("2099-01-25")?),
        ]
    );
    assert!(windows(day("2099-01-26")?, day("2099-01-25")?, 10).is_empty());
    Ok(())
}

// W61: the first run that read its answers (2026-11-10 23:30Z) asked 30 windows per code, all of
// 2020 without a file, and stopped there; only windows that held a file count against the cap.
#[test]
fn only_windows_with_files_count_against_the_window_cap() -> anyhow::Result<()> {
    let backfill = source_contract()?.backfill;
    let now = std::time::Instant::now();
    let later = now + Duration::from_secs(60);
    assert_eq!(next_stop(&backfill, 0, 0, now, later), None);
    assert_eq!(
        next_stop(&backfill, backfill.max_windows_per_run - 1, 0, now, later),
        None
    );
    assert_eq!(
        next_stop(&backfill, backfill.max_windows_per_run, 0, now, later),
        Some(Stop::Done)
    );
    assert_eq!(
        next_stop(&backfill, 0, backfill.max_bytes_per_run, now, later),
        Some(Stop::ByteCap)
    );
    assert_eq!(
        next_stop(&backfill, 0, 0, later, later),
        Some(Stop::Deadline),
        "windows without files stop at the code's deadline"
    );
    Ok(())
}

#[test]
fn each_code_may_run_until_its_share_of_the_budget_has_passed() {
    let start = std::time::Instant::now();
    let budget = Duration::from_secs(3600);
    let deadlines: Vec<_> = (0..4)
        .map(|index| code_deadline(start, budget, index, 4))
        .collect();
    assert_eq!(
        deadlines,
        [900, 1800, 2700, 3600].map(|seconds| start + Duration::from_secs(seconds))
    );
}

// The empty history the cap no longer counts must still fit the run: every day from first_day to
// the contract's reading, one ask per window, at the measured 3.05 s per ask (366 s for 120 asks,
// 2026-11-10 23:30Z), inside the budget.
#[test]
fn the_whole_empty_history_fits_the_time_budget() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let days = (day("2026-11-10")? - contract.backfill.first_day).num_days();
    let per_code = u64::try_from(days)?.div_ceil(u64::from(contract.query.max_window_days));
    let asks = per_code * u64::try_from(contract.request_codes.len())?;
    assert!(
        asks * 305 / 100 <= contract.backfill.run_time_budget_seconds,
        "{asks} asks at 3.05 s outlast the {} s budget",
        contract.backfill.run_time_budget_seconds
    );
    Ok(())
}

#[test]
fn yesterday_is_counted_in_kst() -> anyhow::Result<()> {
    // 2099-01-01 16:00 UTC is 2099-01-02 01:00 KST: yesterday is 2099-01-01.
    let now = Utc
        .with_ymd_and_hms(2099, 1, 1, 16, 0, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("a fixed instant"))?;
    assert_eq!(last_day(now), day("2099-01-01")?);
    Ok(())
}

#[test]
fn a_missing_cursor_starts_at_the_first_day() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let first = source_contract()?.backfill.first_day;
    assert_eq!(read_cursor(dir.path(), "300001", first)?, first);
    Ok(())
}

#[test]
fn the_bronze_leaf_is_the_file_its_retry_flag_and_its_making_date() -> anyhow::Result<()> {
    let received = Received {
        sequence: "01".to_owned(),
        base_date: "20990101".to_owned(),
        file_name: "AlterD.JUSURB.20990101.ZIP".to_owned(),
        result_code: "P0000".to_owned(),
        request_code: "300001".to_owned(),
        retry: false,
        created_date: "20990102".to_owned(),
        bytes: zip("a"),
    };
    assert_eq!(
        provider_file_id(&received),
        "alterd-jusurb-20990101-daily-20990102"
    );
    let retry = Received {
        retry: true,
        ..received.clone()
    };
    assert_ne!(provider_file_id(&retry), provider_file_id(&received));
    let at = Utc::now();
    let first = plan(
        "building",
        &received,
        IngestionRunId::new(Uuid::new_v4()),
        at,
    )?;
    let again = plan(
        "building",
        &received,
        IngestionRunId::new(Uuid::new_v4()),
        at,
    )?;
    assert_eq!(
        first.object_key.as_str(),
        again.object_key.as_str(),
        "landing the same file twice is the same object"
    );
    assert!(first.object_key.as_str().contains("jusogokr__building/"));
    let entrance = plan(
        "building_entrance",
        &received,
        IngestionRunId::new(Uuid::new_v4()),
        at,
    )?;
    assert_eq!(
        entrance.checksum_sha256, first.checksum_sha256,
        "the same bytes in each kind"
    );
    assert_ne!(entrance.object_key.as_str(), first.object_key.as_str());
    Ok(())
}

#[test]
fn a_file_of_another_code_or_not_a_zip_is_refused() {
    let received = Received {
        sequence: "01".to_owned(),
        base_date: "20990101".to_owned(),
        file_name: "AlterD.JUSURB.20990101.ZIP".to_owned(),
        result_code: "P0000".to_owned(),
        request_code: "300001".to_owned(),
        retry: false,
        created_date: "20990101".to_owned(),
        bytes: b"<html>".to_vec(),
    };
    assert!(check_file(&received, "300001").is_err());
    let other = Received {
        bytes: zip("a"),
        ..received
    };
    assert!(check_file(&other, "300003").is_err());
    assert!(check_file(&other, "300001").is_ok());
}

#[tokio::test]
async fn an_ask_names_its_window_and_reads_the_files() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    let mut body = record(
        1,
        "20990101",
        "AlterD.JUSURB.20990101.ZIP",
        "300001",
        'N',
        &zip("one"),
    );
    body.extend(record(
        2,
        "20990102",
        "AlterD.JUSURB.20990102.ZIP",
        "300001",
        'N',
        &zip("two"),
    ));
    Mock::given(method("GET"))
        .and(path(contract.path.clone()))
        .and(query_param("app_key", KEY))
        .and(query_param("cntc_cd", "300001"))
        .and(query_param("date_gb", "D"))
        .and(query_param("retry_in", "Y"))
        .and(query_param("req_dt", "20990101"))
        .and(query_param("req_dt2", "20990110"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("err_code", "P0000")
                .set_body_bytes(body),
        )
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let asked = ask(
        &contract,
        &http,
        &server.uri(),
        KEY,
        "300001",
        (day("2099-01-01")?, day("2099-01-10")?),
    )
    .await?;
    assert_eq!(asked.files().count(), 2);
    assert_eq!(answer_code(&contract.result_codes, &asked)?, "P0000");
    Ok(())
}

#[tokio::test]
async fn a_refusal_comes_in_the_error_header_with_no_body() -> anyhow::Result<()> {
    // Measured 2026-11-10 without a key: HTTP 200, err_code E0001, approval_yn N, an empty body.
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(contract.path.clone()))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("err_code", "E0001")
                .insert_header("approval_yn", "N"),
        )
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let asked = ask(
        &contract,
        &http,
        &server.uri(),
        KEY,
        "300001",
        (day("2099-01-01")?, day("2099-01-10")?),
    )
    .await?;
    let code = answer_code(&contract.result_codes, &asked)?;
    assert_eq!(classify(&contract.result_codes, &code), Outcome::Refused);
    Ok(())
}

#[tokio::test]
async fn a_body_that_ends_inside_a_record_is_broken_and_kept() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    let mut body = record(
        1,
        "20990101",
        "AlterD.JUSURB.20990101.ZIP",
        "300001",
        'N',
        &zip("one"),
    );
    body.truncate(body.len() - 2);
    Mock::given(method("GET"))
        .and(path(contract.path.clone()))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let window = (day("2099-01-01")?, day("2099-01-10")?);
    let asked = ask(&contract, &http, &server.uri(), KEY, "300001", window).await?;
    assert!(asked.broken.is_some());
    assert_eq!(
        asked.raw.len() as u64,
        asked.bytes,
        "the whole answer is kept"
    );
    let dir = tempfile::tempdir()?;
    let kept = quarantine(dir.path(), "300001", window, &asked)?;
    assert_eq!(std::fs::read(&kept)?, asked.raw);
    assert!(
        quarantine(dir.path(), "300001", window, &asked).is_err(),
        "a dead letter is written once"
    );
    Ok(())
}

#[test]
fn a_record_without_a_file_takes_no_bytes() -> anyhow::Result<()> {
    let contract = source_contract()?;
    // A size on a blank record is not followed by bytes: the next header comes at once.
    let mut body = day_without_file(1, "20990101", 7, "E1001", "300001");
    body.extend(record(
        2,
        "20990102",
        "AlterD.JUSURB.20990102.ZIP",
        "300001",
        'N',
        &zip("two"),
    ));
    let (records, used) = take_records(&contract.answer, &body)?;
    assert_eq!(used, body.len());
    assert_eq!(records.len(), 2);
    assert!(!records[0].has_file());
    assert_eq!(records[0].result_code, "E1001");
    assert!(records[0].bytes.is_empty());
    assert_eq!(records[1].bytes, zip("two"));
    Ok(())
}

async fn answered(body: Vec<u8>, count: usize) -> anyhow::Result<super::Asked> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(contract.path.clone()))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("total_count", count.to_string().as_str())
                .set_body_bytes(body),
        )
        .mount(&server)
        .await;
    ask(
        &contract,
        &reqwest::Client::new(),
        &server.uri(),
        KEY,
        "300001",
        (day("2099-01-01")?, day("2099-01-10")?),
    )
    .await
}

#[tokio::test]
async fn a_window_of_days_without_files_moves_on() -> anyhow::Result<()> {
    // The first run with the key (2026-11-10) failed here on all four codes: blank-name records.
    let codes = source_contract()?.result_codes;
    let mut body = Vec::new();
    for (sequence, base) in [(1, "20990101"), (2, "20990102"), (3, "20990103")] {
        body.extend(day_without_file(sequence, base, 0, "E1001", "300001"));
    }
    let asked = answered(body, 3).await?;
    assert!(asked.broken.is_none());
    assert_eq!(asked.files().count(), 0);
    let code = answer_code(&codes, &asked)?;
    assert_eq!(classify(&codes, &code), Outcome::NoFiles);
    Ok(())
}

#[tokio::test]
async fn a_window_with_a_file_and_days_without_lands_the_file() -> anyhow::Result<()> {
    let codes = source_contract()?.result_codes;
    let mut body = day_without_file(1, "20990101", 0, "E1001", "300001");
    body.extend(record(
        2,
        "20990102",
        "AlterD.JUSURB.20990102.ZIP",
        "300001",
        'N',
        &zip("two"),
    ));
    let asked = answered(body, 2).await?;
    assert_eq!(asked.files().count(), 1);
    assert_eq!(answer_code(&codes, &asked)?, "P0000");
    Ok(())
}

#[tokio::test]
async fn a_refusal_inside_the_body_still_refuses() -> anyhow::Result<()> {
    let codes = source_contract()?.result_codes;
    let mut body = record(
        1,
        "20990101",
        "AlterD.JUSURB.20990101.ZIP",
        "300001",
        'N',
        &zip("one"),
    );
    body.extend(day_without_file(2, "20990102", 0, "E1008", "300001"));
    let asked = answered(body, 2).await?;
    assert_eq!(answer_code(&codes, &asked)?, "E1008");
    Ok(())
}

#[tokio::test]
async fn fewer_records_than_announced_is_broken() -> anyhow::Result<()> {
    let body = day_without_file(1, "20990101", 0, "E1001", "300001");
    let asked = answered(body, 2).await?;
    assert!(asked.broken.is_some());
    assert_eq!(asked.raw.len(), 100);
    Ok(())
}
