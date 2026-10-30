use super::*;

fn at(text: &str) -> DateTime<Utc> {
    text.parse().unwrap()
}

fn lawds(codes: &[&str]) -> BTreeSet<String> {
    codes.iter().map(|code| (*code).to_owned()).collect()
}

/// The real contract with only `datasets` narrowed, so a test reads its own few months.
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

#[test]
fn the_contract_names_every_real_transaction_operation_once() {
    let contract = source_contract().unwrap();
    assert_eq!(contract.datasets.len(), 12);
    for (name, dataset) in &contract.datasets {
        assert_eq!(
            collection_domain::real_transaction_dataset_slug(&dataset.operation),
            Some(name.as_str()),
            "{name}: {}",
            dataset.operation
        );
        assert!(dataset.path.ends_with(&format!("/{}", dataset.operation)));
    }
    for name in &contract.priority {
        assert!(contract.datasets.contains_key(name), "{name}");
    }
}

#[test]
fn months_read_and_print_as_the_provider_writes_them() {
    assert_eq!(month_index("2006-01").unwrap(), 2006 * 12);
    assert_eq!(yyyymm(month_index("202610").unwrap()), "202610");
    assert_eq!(yyyymm(month_index("2026-12").unwrap() + 1), "202701");
    assert!(month_index("2026-13").is_err());
}

#[test]
fn a_new_state_owes_the_recent_months_first_then_the_history_newest_first() {
    let contract = contract_with(&[
        ("real_transaction_apartment_trade", "2026-06"),
        ("real_transaction_industrial_trade", "2026-06"),
    ]);
    let work = plan_work(
        &contract,
        &State::default(),
        &lawds(&["99999"]),
        at("2026-10-30T13:00:00Z"),
    )
    .unwrap();
    let order: Vec<(Stage, &str, String)> = work
        .iter()
        .map(|w| (w.stage, w.dataset.as_str(), yyyymm(w.month)))
        .collect();
    assert_eq!(
        order,
        vec![
            (
                Stage::Recent,
                "real_transaction_industrial_trade",
                "202610".to_owned()
            ),
            (
                Stage::Recent,
                "real_transaction_industrial_trade",
                "202609".to_owned()
            ),
            (
                Stage::Recent,
                "real_transaction_industrial_trade",
                "202608".to_owned()
            ),
            (
                Stage::Recent,
                "real_transaction_apartment_trade",
                "202610".to_owned()
            ),
            (
                Stage::Recent,
                "real_transaction_apartment_trade",
                "202609".to_owned()
            ),
            (
                Stage::Recent,
                "real_transaction_apartment_trade",
                "202608".to_owned()
            ),
            (
                Stage::Backfill,
                "real_transaction_industrial_trade",
                "202607".to_owned()
            ),
            (
                Stage::Backfill,
                "real_transaction_industrial_trade",
                "202606".to_owned()
            ),
            (
                Stage::Backfill,
                "real_transaction_apartment_trade",
                "202607".to_owned()
            ),
            (
                Stage::Backfill,
                "real_transaction_apartment_trade",
                "202606".to_owned()
            ),
        ]
    );
}

#[test]
fn a_completed_month_is_asked_again_on_its_tiers_schedule_and_never_after_the_last_tier() {
    let contract = contract_with(&[("real_transaction_land_trade", "2024-01")]);
    let now = at("2026-10-30T13:00:00Z");
    let mut state = State::default();
    let complete = |state: &mut State, month: &str, when: &str| {
        state.months.insert(
            month_key("real_transaction_land_trade", month_index(month).unwrap()),
            MonthMark {
                completed_utc: Some(at(when)),
                done: BTreeSet::new(),
            },
        );
    };
    for month in month_index("2024-01").unwrap()..=month_index("2026-10").unwrap() {
        complete(&mut state, &yyyymm(month), "2026-10-30T12:00:00Z");
    }
    // Age 1, completed 25 hours ago: the daily tier asks it again.
    complete(&mut state, "2026-09", "2026-10-29T12:00:00Z");
    // Age 5, completed 3 days ago: the weekly tier waits; 8 days ago: due.
    complete(&mut state, "2026-05", "2026-10-27T12:00:00Z");
    complete(&mut state, "2026-04", "2026-10-22T12:00:00Z");
    // Age 20, completed 31 days ago: the monthly tier asks it again.
    complete(&mut state, "2025-02", "2026-09-29T12:00:00Z");
    // Age 33, completed a year ago: older than every tier, never asked again.
    complete(&mut state, "2024-01", "2025-10-01T00:00:00Z");
    let due: Vec<(Stage, String)> = plan_work(&contract, &state, &lawds(&["99999"]), now)
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
fn a_month_completes_only_when_every_code_of_the_list_was_read() {
    let contract = contract_with(&[("real_transaction_land_trade", "2026-10")]);
    let list = lawds(&["99991", "99992"]);
    let now = at("2026-10-30T13:00:00Z");
    let mut state = State::default();
    state.absorb_lawd_set(&list);
    let work = plan_work(&contract, &state, &list, now).unwrap();
    assert_eq!(work.len(), 2);
    state.settle(&work[0], &list, now);
    assert_eq!(
        state.months["real_transaction_land_trade/202610"].completed_utc,
        None
    );
    let rest = plan_work(&contract, &state, &list, now).unwrap();
    assert_eq!(
        rest.len(),
        1,
        "the code already read is not asked again in the same pass"
    );
    state.settle(&rest[0], &list, now);
    let mark = &state.months["real_transaction_land_trade/202610"];
    assert_eq!(mark.completed_utc, Some(now));
    assert!(mark.done.is_empty());
    assert!(plan_work(&contract, &state, &list, now).unwrap().is_empty());
}

#[test]
fn a_code_that_leaves_the_list_is_asked_once_more_and_a_new_code_gets_the_history() {
    let contract = contract_with(&[("real_transaction_land_trade", "2026-09")]);
    let now = at("2026-10-30T13:00:00Z");
    let old = lawds(&["99990", "99991"]);
    let mut state = State::default();
    state.absorb_lawd_set(&old);
    for item in plan_work(&contract, &state, &old, now).unwrap() {
        state.settle(&item, &old, now);
    }
    // A 분구: 99990 leaves, 99993 comes (the provider re-files the history under the new code).
    let new = lawds(&["99991", "99993"]);
    state.absorb_lawd_set(&new);
    let later = at("2026-10-30T14:00:00Z");
    let work = plan_work(&contract, &state, &new, later).unwrap();
    let owed: Vec<(Stage, &str, String)> = work
        .iter()
        .map(|w| (w.stage, w.lawd.as_str(), yyyymm(w.month)))
        .collect();
    assert_eq!(
        owed,
        vec![
            (Stage::Retiring, "99990", "202610".to_owned()),
            (Stage::Retiring, "99990", "202609".to_owned()),
            (Stage::Entering, "99993", "202610".to_owned()),
            (Stage::Entering, "99993", "202609".to_owned()),
        ]
    );
    for item in &work {
        state.settle(item, &new, later);
    }
    assert!(state.retiring.is_empty());
    assert!(state.entering.is_empty());
    assert_eq!(state.known_lawds, new);
    assert!(plan_work(&contract, &state, &new, later)
        .unwrap()
        .is_empty());
}

fn answer(body: &str) -> anyhow::Result<Answer> {
    read_answer(&source_contract().unwrap(), 200, body)
}

#[test]
fn answers_read_as_rows_refusals_or_failures() {
    let contract = source_contract().unwrap();
    assert_eq!(read_answer(&contract, 429, "").unwrap(), Answer::Refused);
    assert_eq!(
        answer(r#"{"response":{"header":{"resultCode":"000","resultMsg":"OK"},"body":{"items":{"item":[{"a":1},{"a":2}]},"numOfRows":1000,"pageNo":1,"totalCount":2}}}"#).unwrap(),
        Answer::Rows { items: 2, total: 2 }
    );
    assert_eq!(
        answer(r#"{"response":{"header":{"resultCode":"000","resultMsg":"OK"},"body":{"items":{"item":{"a":1}},"totalCount":"1"}}}"#).unwrap(),
        Answer::Rows { items: 1, total: 1 }
    );
    assert_eq!(
        answer(r#"{"response":{"header":{"resultCode":"000","resultMsg":"OK"},"body":{"items":"","totalCount":0}}}"#).unwrap(),
        Answer::Rows { items: 0, total: 0 }
    );
    // A key or parameter problem must never read as "no transactions".
    assert!(answer(r#"{"response":{"header":{"resultCode":"30","resultMsg":"SERVICE KEY IS NOT REGISTERED ERROR"}}}"#).is_err());
    assert!(answer("<OpenAPI_ServiceResponse><cmmMsgHeader><errMsg>SERVICE ERROR</errMsg></cmmMsgHeader></OpenAPI_ServiceResponse>").is_err());
    assert!(read_answer(&contract, 500, "").is_err());
}

#[test]
fn the_lawd_list_is_refused_when_stale_and_absent_means_wait() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lawd-set.json");
    let now = at("2026-10-30T13:00:00Z");
    assert_eq!(read_lawd_set(&path, 3, now).unwrap(), None);
    let write = |written: &str, codes: &[&str]| {
        std::fs::write(
            &path,
            json!({"schema_version": LAWD_SET_SCHEMA, "written_at_utc": written, "lawd_codes": codes}).to_string(),
        )
        .unwrap();
    };
    write("2026-10-29T00:00:00Z", &["99991", "99992"]);
    assert_eq!(
        read_lawd_set(&path, 3, now).unwrap(),
        Some(lawds(&["99991", "99992"]))
    );
    write("2026-10-20T00:00:00Z", &["99991"]);
    assert!(read_lawd_set(&path, 3, now).is_err());
    write("2026-10-29T00:00:00Z", &["9999"]);
    assert!(read_lawd_set(&path, 3, now).is_err());
}

#[test]
fn object_names_end_with_the_run_and_sort_in_landing_order() {
    let now = at("2026-10-30T13:00:00Z");
    let first = file_id("real_transaction_land_trade", 1, now);
    let tenth = file_id("real_transaction_land_trade", 10, now);
    assert_eq!(first, "real-transaction-land-trade-0001-20261030t130000z");
    assert!(first < tenth);
}

#[test]
fn every_tier_boundary_matches_the_measured_cancellation_lag() {
    // root ADR-0348 §3: the last tier must reach the latest cancellation measured (442 days).
    let contract = source_contract().unwrap();
    let last = contract.refresh_tiers.last().unwrap();
    assert!(i64::from(last.max_age_months) * 30 >= 442);
    assert_eq!(contract.refresh_tiers[0].max_age_months, 2);
    assert_eq!(contract.refresh_tiers[0].every_days, 1);
}
