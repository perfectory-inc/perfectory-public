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
fn runs_ask_only_inside_the_kst_quiet_hours() {
    let contract = source_contract().unwrap();
    assert!(inside_quiet_hours(&contract, at("2026-10-30T16:00:00Z")).unwrap()); // 01:00 KST
    assert!(inside_quiet_hours(&contract, at("2026-10-30T20:59:00Z")).unwrap()); // 05:59 KST
    assert!(!inside_quiet_hours(&contract, at("2026-10-30T21:00:00Z")).unwrap()); // 06:00 KST
    assert!(!inside_quiet_hours(&contract, at("2026-10-30T13:00:00Z")).unwrap());
    // 22:00 KST
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

#[test]
fn data_rows_are_the_lines_after_the_header() {
    let export = "\"□ 안내\"\r\n\"□ 검색조건\"\r\n\"NO\",\"시군구\"\r\n\"1\",\"합성도 합성시\"\r\n\"2\",\"합성도 합성시\"\r\n\r\n";
    assert_eq!(csv_data_rows(export.as_bytes()), Some(2));
    assert_eq!(csv_data_rows(b"\"NO\",\"x\"\n"), Some(0));
    assert_eq!(csv_data_rows(b"<html></html>"), None);
}

#[test]
fn a_refusal_stays_a_refusal_and_the_daily_limit_waits() {
    let contract = source_contract().unwrap();
    let refusal: anyhow::Error = RtMolitRefusal("HTTP 429 at the count check".to_owned()).into();
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
