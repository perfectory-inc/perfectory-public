use super::*;

fn at(text: &str) -> DateTime<Utc> {
    text.parse().unwrap()
}

fn contract_with(datasets: &[(&str, &str)]) -> SourceContract {
    let mut contract = source_contract().unwrap();
    let all = std::mem::take(&mut contract.datasets);
    for (name, first) in datasets {
        let mut dataset = all[*name].clone();
        dataset.first_month = (*first).to_owned();
        contract.datasets.insert((*name).to_owned(), dataset);
    }
    contract
}

fn complete(state: &mut State, dataset: &str, month: &str, when: &str) {
    state.months.insert(
        month_key(dataset, month_index(month).unwrap()),
        MonthMark {
            completed_utc: Some(at(when)),
            checksum_sha256: Some("0".repeat(64)),
            provider_count: Some(1),
        },
    );
}

#[test]
fn the_contract_names_the_twelve_datasets_by_their_codes() {
    let contract = source_contract().unwrap();
    assert_eq!(contract.datasets.len(), 12);
    for name in &contract.priority {
        assert!(contract.datasets.contains_key(name), "{name}");
    }
    // ADR-0283: the User-Agent names us; it never pretends to be a browser.
    assert!(contract.user_agent.starts_with("perfectory-foundation-"));
    assert!(!contract.user_agent.contains("Mozilla"));
    // The provider allows 100 downloads a day; our bound stays under it.
    assert!(contract.manners.daily_download_limit < 100);
    assert!(contract.manners.max_downloads_per_run <= contract.manners.daily_download_limit);
}

#[test]
fn a_new_state_owes_the_recent_months_first_then_the_history_newest_first() {
    let contract = contract_with(&[
        ("real_transaction_apartment_trade", "2026-06"),
        ("real_transaction_industrial_trade", "2026-06"),
    ]);
    let order: Vec<(Stage, String, String)> =
        plan_work(&contract, &State::default(), at("2026-10-30T17:00:00Z"))
            .unwrap()
            .into_iter()
            .map(|w| (w.stage, w.dataset, yyyymm(w.month)))
            .collect();
    let expect = |stage, dataset: &str, month: &str| (stage, dataset.to_owned(), month.to_owned());
    // 17:00 UTC is 02:00 KST on 10-31: the current KST month is still 2026-10.
    assert_eq!(
        order,
        vec![
            expect(Stage::Recent, "real_transaction_industrial_trade", "202610"),
            expect(Stage::Recent, "real_transaction_industrial_trade", "202609"),
            expect(Stage::Recent, "real_transaction_industrial_trade", "202608"),
            expect(Stage::Recent, "real_transaction_apartment_trade", "202610"),
            expect(Stage::Recent, "real_transaction_apartment_trade", "202609"),
            expect(Stage::Recent, "real_transaction_apartment_trade", "202608"),
            expect(
                Stage::Backfill,
                "real_transaction_industrial_trade",
                "202607"
            ),
            expect(
                Stage::Backfill,
                "real_transaction_industrial_trade",
                "202606"
            ),
            expect(
                Stage::Backfill,
                "real_transaction_apartment_trade",
                "202607"
            ),
            expect(
                Stage::Backfill,
                "real_transaction_apartment_trade",
                "202606"
            ),
        ]
    );
}

#[test]
fn a_completed_month_is_asked_again_on_its_tiers_schedule_and_never_after_the_last_tier() {
    let name = "real_transaction_land_trade";
    let contract = contract_with(&[(name, "2024-01")]);
    let now = at("2026-10-30T17:00:00Z");
    let mut state = State::default();
    for month in month_index("2024-01").unwrap()..=month_index("2026-10").unwrap() {
        complete(&mut state, name, &yyyymm(month), "2026-10-30T16:30:00Z");
    }
    complete(&mut state, name, "2026-09", "2026-10-29T16:00:00Z"); // age 1, 25 hours: daily, due
    complete(&mut state, name, "2026-05", "2026-10-27T16:00:00Z"); // age 5, 3 days: weekly, waits
    complete(&mut state, name, "2026-04", "2026-10-22T16:00:00Z"); // age 6, 8 days: due
    complete(&mut state, name, "2025-02", "2026-09-29T16:00:00Z"); // age 20, 31 days: monthly, due
    complete(&mut state, name, "2024-01", "2025-10-01T00:00:00Z"); // age 33: never again
    let due: Vec<(Stage, String)> = plan_work(&contract, &state, now)
        .unwrap()
        .iter()
        .map(|w| (w.stage, yyyymm(w.month)))
        .collect();
    assert_eq!(
        due,
        vec![
            (Stage::Recent, "202609".to_owned()),
            (Stage::Refresh { tier: 1 }, "202604".to_owned()),
            (Stage::Refresh { tier: 2 }, "202502".to_owned()),
        ]
    );
}

#[test]
fn the_last_tier_reaches_the_latest_measured_cancellation() {
    // Root ADR-0348 §3: cancellations measured up to 442 days after the contract.
    let contract = source_contract().unwrap();
    let last = contract.refresh_tiers.last().unwrap();
    assert!(i64::from(last.max_age_months) * 30 >= 442);
    assert_eq!(
        (
            contract.refresh_tiers[0].max_age_months,
            contract.refresh_tiers[0].every_days
        ),
        (2, 1)
    );
}

#[test]
fn a_busy_answer_backs_off_doubling_up_to_the_cap() {
    let retry = Retry {
        attempts: 3,
        initial_backoff_seconds: 30,
        max_backoff_seconds: 100,
    };
    assert_eq!(backoff(&retry, 1), Duration::from_secs(30));
    assert_eq!(backoff(&retry, 2), Duration::from_secs(60));
    assert_eq!(backoff(&retry, 3), Duration::from_secs(100));
    assert_eq!(backoff(&retry, 40), Duration::from_secs(100));
}

#[test]
fn the_contract_keeps_modest_concurrency_and_retries() {
    // 사장 결정 2026-10-31: 2-4 downloads in parallel, retries with backoff on 429/5xx.
    let manners = source_contract().unwrap().manners;
    assert!((2..=4).contains(&manners.concurrency));
    assert!(manners.retry.attempts >= 1);
    assert!(manners.retry.initial_backoff_seconds > 0);
}

#[test]
fn months_span_whole_calendar_months() {
    assert_eq!(
        month_dates(month_index("2024-02").unwrap()).unwrap(),
        (
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()
        )
    );
    assert_eq!(
        month_dates(month_index("2026-12").unwrap()).unwrap().1,
        NaiveDate::from_ymd_opt(2026, 12, 31).unwrap()
    );
}

fn export(month: &str, rows: &[&str]) -> Vec<u8> {
    let mut text = format!(
        "\"□ 안내\"\r\n\"계약일자 : {month}\"\r\n\"실거래구분 : 합성(매매)\"\r\n\"NO\",\"시군구\"\r\n"
    );
    for row in rows {
        text.push_str(row);
        text.push_str("\r\n");
    }
    let (bytes, _, _) = encoding_rs::EUC_KR.encode(&text);
    bytes.into_owned()
}

fn june() -> RtMolitRealTransactionExportRequest {
    request_for(
        &contract_with(&[("real_transaction_land_trade", "2006-01")]),
        &WorkItem {
            stage: Stage::Backfill,
            priority: 0,
            newest_first: 0,
            dataset: "real_transaction_land_trade".to_owned(),
            month: month_index("2098-06").unwrap(),
        },
    )
    .unwrap()
}

#[test]
fn an_export_is_believed_only_when_it_holds_what_was_asked() {
    // root ADR-0364: the 2026-10-30 export carried another export's rows after its own.
    let request = june();
    let good = export(
        "2098-06-01 ~ 2098-06-30",
        &["\"1\",\"합성도\"", "\"2\",\"합성도\""],
    );
    assert_eq!(export_problem(&good, 2, &request), None);
    assert!(export_problem(&good, 3, &request)
        .unwrap()
        .contains("count check"));
    let mixed = export(
        "2098-06-01 ~ 2098-06-30",
        &[
            "\"1\",\"합성도\"",
            "\"2\",\"합성도\"",
            "\"1\",\"다른 내보내기\"",
        ],
    );
    assert!(export_problem(&mixed, 2, &request)
        .unwrap()
        .contains("numbered"));
    let other_month = export("2098-05-01 ~ 2098-05-31", &["\"1\",\"합성도\""]);
    assert!(export_problem(&other_month, 1, &request)
        .unwrap()
        .contains("preamble"));
    assert!(export_problem(b"<html></html>", 0, &request).is_some());
}

#[test]
fn a_refusal_stays_a_refusal_and_the_daily_limit_waits() {
    let contract = source_contract().unwrap();
    let refusal: anyhow::Error = RtMolitRefusal("HTTP 403 at the count check".to_owned()).into();
    let refused = classify(&contract, refusal.context("CSV")).unwrap_err();
    assert!(refused.downcast_ref::<RtMolitRefusal>().is_some());
    let limit = anyhow::anyhow!(
        "rt.molit.go.kr provider rejected count check: 일일 다운로드 횟수는 최대 100건 입니다."
    );
    assert!(matches!(
        classify(&contract, limit),
        Ok(Asked::DailyLimit(_))
    ));
    let ours = anyhow::anyhow!("rt.molit.go.kr provider rejected count check: 전국 자료제공 계약일자 범위는 최대 1개월입니다.");
    assert!(classify(&contract, ours).is_err());
}

#[test]
fn the_collector_source_uses_no_disguise() {
    // Root ADR-0281 §4 as the court-auction collector holds it: no proxy, rotation, browser
    // disguise, CAPTCHA handling or randomness.
    let source = include_str!("../rt_molit_real_transaction_collect.rs");
    let ingest = include_str!("../rt_molit_real_transaction_export_ingest.rs");
    for name in [
        "Proxy::",
        ".proxy(",
        "Stealth",
        "playwright",
        "rand::",
        "Sec-Fetch",
        "sec-ch-ua",
        "Mozilla/",
    ] {
        assert!(!source.contains(name), "{name}");
        assert!(!ingest.contains(name), "{name}");
    }
}
