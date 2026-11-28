use chrono::{DateTime, Utc};
use foundation_publisher_collection::rt_molit_real_transaction_collect::{month_index, Stage};
use foundation_publisher_silver::legal_dong_code_snapshot_stage::CodeRow;

use super::*;

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

fn code(region_cd: &str, status: &str) -> CodeRow {
    CodeRow {
        region_cd: region_cd.to_owned(),
        full_name: "synthetic".to_owned(),
        status: status.to_owned(),
        parent_cd: String::new(),
        created_date: String::new(),
        abolished_date: String::new(),
        lowest_name: String::new(),
        jumin_cd: String::new(),
        jijuk_cd: String::new(),
    }
}

#[test]
fn the_contract_reads_and_has_the_twelve_kinds_of_the_export() {
    let contract = source_contract().unwrap();
    assert_eq!(contract.kinds.len(), 12);
    let export =
        foundation_publisher_collection::rt_molit_real_transaction_collect::source_contract()
            .unwrap();
    assert_eq!(
        contract.kinds.keys().collect::<Vec<_>>(),
        export.datasets.keys().collect::<Vec<_>>(),
        "the same kinds as the export it replaces"
    );
    for (name, kind) in &contract.kinds {
        assert_eq!(
            kind.first_month, export.datasets[name].first_month,
            "{name}"
        );
    }
}

#[test]
fn the_history_is_one_contract_value() {
    let mut contract = source_contract().unwrap();
    contract.history_from_month = "2024-11".to_owned();
    assert_eq!(
        contract.first_months().unwrap()["real_transaction_land_trade"],
        "202411"
    );
    // The whole history: the kinds' own first months.
    contract.history_from_month = "2000-01".to_owned();
    assert_eq!(
        contract.first_months().unwrap()["real_transaction_apartment_rent"],
        "201101"
    );
}

#[test]
fn districts_are_the_active_sigungu_and_a_si_with_gu_is_asked_by_its_gu() {
    let rows = [
        code("1100000000", EXISTS), // 시도: not a 시군구
        code("1168000000", EXISTS), // 강남구
        code("1168010100", EXISTS), // a 동: not a 시군구
        code("4111000000", EXISTS), // 수원시, has 구
        code("4111100000", EXISTS), // 장안구
        code("4111300000", EXISTS), // 권선구
        code("4115000000", EXISTS), // 의정부시, no 구
        code("4672000000", "폐지"), // abolished
        code("3611000000", EXISTS), // 세종
    ];
    assert_eq!(
        districts(&rows),
        ["11680", "36110", "41111", "41113", "41150"]
    );
}

#[test]
fn a_month_starts_only_when_the_day_s_budget_covers_what_it_took_last_time() {
    let contract = source_contract().unwrap();
    let mut state = State::default();
    state.roll_budget(at("2026-11-30T15:40:00Z"));
    let month = month_index("2026-10").unwrap();
    let kind = "real_transaction_land_trade";
    assert!(state.fits(&contract, kind, month, 250));
    state
        .budget
        .calls
        .insert(kind.to_owned(), contract.daily_call_limit_per_api - 249);
    assert!(
        !state.fits(&contract, kind, month, 250),
        "one call per 시군구 at first"
    );
    state.calls_per_month.insert(month_key(kind, month), 240);
    assert!(
        state.fits(&contract, kind, month, 250),
        "what it took last time"
    );
    state.budget.quota_refused.insert(kind.to_owned());
    assert_eq!(
        state.left(&contract, kind),
        0,
        "the provider's quota answer stops the day"
    );
    // A new KST day starts the counts again.
    state.roll_budget(at("2026-12-01T15:40:00Z"));
    assert_eq!(
        state.left(&contract, kind),
        contract.daily_call_limit_per_api
    );
}

#[test]
fn the_months_owed_are_the_export_s_plan_from_the_history_value() {
    let contract = source_contract().unwrap();
    let first_months = contract.first_months().unwrap();
    let work = plan_months(
        &contract.plan,
        first_months
            .iter()
            .map(|(name, first)| (name.as_str(), first.as_str())),
        &State::default().months,
        at("2026-11-30T15:40:00Z"),
    )
    .unwrap();
    // 15:40Z on 11-30 is 00:40 KST on 12-01: the month the provider counts is December.
    let first = &work[0];
    assert_eq!(
        (first.stage, first.month),
        (Stage::Recent, month_index("2026-12").unwrap())
    );
    // The whole history (owner 2026-11-28, D1 ii): each kind from its own first month.
    let earliest = work.iter().map(|item| item.month).min().unwrap();
    assert_eq!(earliest, month_index("2006-01").unwrap());
    assert!(work
        .iter()
        .filter(|item| item.dataset == "real_transaction_apartment_rent")
        .all(|item| item.month >= month_index("2011-01").unwrap()));
}

#[test]
fn a_month_s_object_is_named_by_its_kind_and_month() {
    assert_eq!(
        file_id(
            "real_transaction_land_trade",
            month_index("2026-10").unwrap()
        ),
        "real-transaction-land-trade-202610"
    );
}
