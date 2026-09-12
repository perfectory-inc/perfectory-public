//! A unit carries its reference-date official prices through the published contract.

use foundation_contracts::catalog::UnitResponse;
use serde_json::{json, Value};

fn unit() -> Value {
    json!({
        "id": "00000000-0000-4000-8000-000000000001",
        "parcel_id": "00000000-0000-4000-8000-000000000002",
        "building_id": null,
        "building_name": "fixture",
        "dong_name": "101동",
        "ho_name": "101호",
        "floor_label": "1",
        "usage_name": "아파트",
        "structure_name": ""
    })
}

#[test]
fn older_unit_payload_has_an_explicit_empty_history() -> Result<(), serde_json::Error> {
    let dto: UnitResponse = serde_json::from_value(unit())?;
    assert_eq!(
        serde_json::to_value(dto)?["official_price_history"],
        json!([])
    );
    Ok(())
}

#[test]
fn same_year_reference_dates_survive_a_contract_round_trip() -> Result<(), serde_json::Error> {
    let mut payload = unit();
    let history = json!([
        {"base_date": "20100601", "price_won": 35_000_000},
        {"base_date": "20100101", "price_won": 36_000_000}
    ]);
    payload["official_price_history"] = history.clone();
    let dto: UnitResponse = serde_json::from_value(payload)?;
    assert_eq!(
        serde_json::to_value(dto)?["official_price_history"],
        history
    );
    Ok(())
}
