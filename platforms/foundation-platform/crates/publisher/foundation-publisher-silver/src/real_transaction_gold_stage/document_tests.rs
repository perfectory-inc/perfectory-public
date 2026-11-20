//! The document's rules on the Spark job's own cases (`test_real_transaction_gold.py`,
//! `DocumentSparkTest` and `history_coverage`), synthetic codes and PNUs (prefix 99999) only. The
//! national verdict against the served Gold is what moves the transform; these hold the rules one
//! day's data may not exercise, and the exact text Spark's `to_json` writes.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::{json, Value};

use super::{gold_rows, history_coverage, Coverage};
use crate::real_transaction_placement_stage::place;
use crate::real_transaction_placement_stage::tests::{
    inference, inputs, pnu, Deal, INDUSTRIAL, OTHER, OTHER_NAME,
};

fn day(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap_or_default()
}

fn coverage() -> Vec<Coverage> {
    vec![Coverage {
        dataset_slug: "real_transaction_apartment_trade".to_owned(),
        from_month: Some("2095-05".to_owned()),
    }]
}

/// `pnu → (document, transaction_total, place_id, main_lot_group_place_ids, row_digest)`.
type Documents = BTreeMap<String, (Value, i64, Option<String>, Option<String>, String)>;

/// Places `deals` and builds the Gold rows for 2098-05 with every parcel's place id `P-<pnu>`.
fn documents(connection: &Connection, levels: &Value, as_of: &str) -> anyhow::Result<Documents> {
    place(connection, &inference(levels))?;
    connection.execute_batch(
        "CREATE OR REPLACE TABLE place_ids AS SELECT pnu, 'P-' || pnu AS place_id FROM parcels",
    )?;
    gold_rows(connection, day(as_of), &coverage())?;
    let rows: Vec<(String, String, i64, Option<String>, Option<String>, String)> = connection
        .prepare(
            "SELECT pnu, real_transaction_json, transaction_total, place_id, \
             main_lot_group_place_ids, row_digest FROM gold_rows",
        )?
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?
        .collect::<Result<_, _>>()?;
    rows.into_iter()
        .map(|(p, text, total, id, group, digest)| {
            Ok((p, (serde_json::from_str(&text)?, total, id, group, digest)))
        })
        .collect()
}

fn rows(deals: &[Deal]) -> Vec<String> {
    deals.iter().map(Deal::row).collect()
}

#[test]
fn a_dataset_not_whole_says_the_month_it_is_held_from() -> anyhow::Result<()> {
    let first: BTreeMap<String, String> = [
        ("real_transaction_industrial_trade", "2090-01"),
        ("real_transaction_commercial_trade", "2090-01"),
        ("real_transaction_apartment_trade", "2090-01"),
        ("real_transaction_apartment_rent", "2091-01"),
    ]
    .into_iter()
    .map(|(a, b)| (a.to_owned(), b.to_owned()))
    .collect();
    let mut months: Vec<(&str, String)> = Vec::new();
    for year in 2090..2099 {
        for month in 1..=12 {
            if (year, month) <= (2098, 5) {
                months.push((
                    "real_transaction_industrial_trade",
                    format!("{year}{month:02}"),
                ));
            }
            if (2095..2099).contains(&year) && (year, month) <= (2098, 5) || year == 2094 {
                months.push((
                    "real_transaction_commercial_trade",
                    format!("{year}{month:02}"),
                ));
            }
        }
    }
    // An island, not the run, then the run.
    months.push(("real_transaction_apartment_trade", "209103".to_owned()));
    for month in 2..=5 {
        months.push((
            "real_transaction_apartment_trade",
            format!("2098{month:02}"),
        ));
    }
    let pairs = |extra: &[(&'static str, &'static str)]| -> Vec<(String, String)> {
        months
            .iter()
            .map(|(d, m)| ((*d).to_owned(), m.clone()))
            .chain(
                extra
                    .iter()
                    .map(|(d, m)| ((*d).to_owned(), (*m).to_owned())),
            )
            .collect()
    };
    let all = pairs(&[]);
    let found = history_coverage(
        all.iter().map(|(d, m)| (d.as_str(), m.as_str())),
        &first,
        day("2098-05-20"),
        36,
    )?;
    let claim = |dataset: &str, month: Option<&str>| Coverage {
        dataset_slug: dataset.to_owned(),
        from_month: month.map(str::to_owned),
    };
    assert_eq!(
        found,
        [
            // No month at all: the screen says so, never "no deal".
            claim("real_transaction_apartment_rent", None),
            // Short of the recent history: its own run's start.
            claim("real_transaction_apartment_trade", Some("2098-02")),
            // Deeper than the recent history: the recent history's start.
            claim("real_transaction_commercial_trade", Some("2095-05")),
        ]
    );
    // The next month landing (2093-12) changes no claim.
    let more = pairs(&[("real_transaction_commercial_trade", "209312")]);
    let again = history_coverage(
        more.iter().map(|(d, m)| (d.as_str(), m.as_str())),
        &first,
        day("2098-05-02"),
        36,
    )?;
    assert_eq!(again[2].from_month.as_deref(), Some("2095-05"));
    Ok(())
}

#[test]
fn a_factory_parcel_carries_its_dongs_factory_deals_masked_ones_too() -> anyhow::Result<()> {
    let (factory, house) = (pnu(OTHER, 5, 0), pnu(OTHER, 7, 0));
    let mut own = Deal::new("o-1", OTHER_NAME, "5", "2098-02-01").land(800.0);
    own.building = Some(400.0);
    let mut cancelled = Deal::new("c-1", OTHER_NAME, "3**", "2098-04-20");
    cancelled.cancelled = true;
    let mut home = Deal::new("h-1", OTHER_NAME, "7", "2098-03-01");
    home.dataset = "real_transaction_detached_house_trade".to_owned();
    let deals = rows(&[
        Deal::new("m-1", OTHER_NAME, "3**", "2098-04-02"),
        own,
        cancelled,
        Deal::new("old", OTHER_NAME, "3**", "2094-01-05"),
        home,
    ]);
    let land = [
        (factory.clone(), "공장용지", 800.0),
        (house.clone(), "대", 300.0),
    ];
    let connection = inputs(&deals, &[], &[factory.clone(), house.clone()], &land, &[])?;
    let docs = documents(&connection, &json!([]), "2098-05-03")?;
    let (document, total, ..) = &docs[&factory];
    assert_eq!(*total, 1);
    let comparables = document["comparables"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        comparables
            .iter()
            .map(|c| c["jibun_raw"].clone())
            .collect::<Vec<_>>(),
        [json!("3**"), json!("5")]
    );
    assert_eq!(
        comparables
            .iter()
            .map(|c| c["pnu"].clone())
            .collect::<Vec<_>>(),
        [Value::Null, json!(factory)]
    );
    let region = document["region"]["datasets"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let industrial: Vec<&Value> = region
        .iter()
        .filter(|d| d["dataset_slug"] == INDUSTRIAL)
        .collect();
    assert_eq!(industrial.len(), 1);
    assert_eq!(
        (
            industrial[0]["deal_count"].clone(),
            industrial[0]["cancelled_count"].clone()
        ),
        (json!(3), json!(1))
    );
    assert_eq!(
        industrial[0]["land_price_per_m2_10k_krw"]["median"],
        json!(56.3)
    );
    // A house parcel shows its own deal and no factory comparables.
    let (house_doc, house_total, ..) = &docs[&house];
    assert_eq!(
        (*house_total, house_doc["comparables"].clone()),
        (1, json!([]))
    );
    Ok(())
}

#[test]
fn a_main_lot_group_deal_shows_on_every_parcel_of_the_group() -> anyhow::Result<()> {
    let (pick, neighbour, other) = (pnu(OTHER, 300, 0), pnu(OTHER, 300, 1), pnu(OTHER, 301, 0));
    let deals = rows(&[Deal::new("m-1", OTHER_NAME, "3**", "2098-04-02").land(800.0)]);
    let land = [
        (pick.clone(), "공장용지", 800.0),
        (neighbour.clone(), "공장용지", 500.0),
        (other.clone(), "공장용지", 900.0),
    ];
    let ownership = [(pick.clone(), "2098-05-01", "소유권이전")];
    let connection = inputs(
        &deals,
        &[],
        &[pick.clone(), neighbour.clone(), other.clone()],
        &land,
        &ownership,
    )?;
    let levels = json!([{
        "grade": "inferred_same_main_lot_group",
        "evidence": ["lot_range", "zone", "jimok", "land_area", "ownership_change"],
        "measured": {INDUSTRIAL: {"precision": 0.9963, "precision_wilson_low": 0.9953, "rows": 19483}}}]);
    let docs = documents(&connection, &levels, "2098-05-03")?;
    for parcel in [&pick, &neighbour] {
        let (document, total, id, group, _) = &docs[parcel];
        let deals = document["transactions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(deals.len(), 1);
        assert_eq!(
            (deals[0]["place_identity_grade"].clone(), *total),
            (json!("inferred_same_main_lot_group"), 1)
        );
        assert_eq!(document["main_lot_group_pnus"], json!([pick, neighbour]));
        assert_eq!(id.as_deref(), Some(format!("P-{parcel}").as_str()));
        assert_eq!(
            group.as_deref(),
            Some(format!("[\"P-{pick}\",\"P-{neighbour}\"]").as_str())
        );
    }
    let (other_doc, other_total, _, other_group, _) = &docs[&other];
    assert_eq!(
        (
            other_doc["transactions"].clone(),
            *other_total,
            other_doc["main_lot_group_pnus"].clone()
        ),
        (json!([]), 0, json!([]))
    );
    assert_eq!(*other_group, None);
    // One comparable for the deal: the group's key and size, no one PNU.
    let comparables = other_doc["comparables"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(comparables.len(), 1);
    assert_eq!(
        (
            comparables[0]["pnu"].clone(),
            comparables[0]["main_lot_group_key"].clone(),
            comparables[0]["main_lot_group_size"].clone()
        ),
        (Value::Null, json!(pick[..15]), json!(2))
    );
    Ok(())
}

#[test]
fn the_document_is_the_text_spark_writes_and_the_digest_holds_content_only() -> anyhow::Result<()> {
    let factory = pnu(OTHER, 5, 0);
    let mut big = Deal::new("o-1", OTHER_NAME, "5", "2098-02-01").land(1000.0);
    // Java writes a double past 10⁷ with an exponent.
    big.building = Some(12_345_678.5);
    let deals = rows(&[big]);
    let land = [(factory.clone(), "공장용지", 1000.0)];
    let first = documents(
        &inputs(&deals, &[], &[factory.clone()], &land, &[])?,
        &json!([]),
        "2098-05-03",
    )?;
    let again = documents(
        &inputs(&deals, &[], &[factory.clone()], &land, &[])?,
        &json!([]),
        "2098-05-28",
    )?;
    assert_eq!(first[&factory].4, again[&factory].4);
    let connection = inputs(&deals, &[], &[factory.clone()], &land, &[])?;
    documents(&connection, &json!([]), "2098-05-03")?;
    let (text, grade): (String, String) = connection.query_row(
        "SELECT g.real_transaction_json, p.place_identity_grade FROM gold_rows g JOIN placed p USING (pnu)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let deal = concat!(
        "\"deal_date\":\"2098-02-01\",\"deal_amount_10k_krw\":50000,\"land_area_m2\":1000.0,\
         \"building_area_m2\":1.23456785E7,\"land_use\":\"계획관리\",\"building_use\":\"공장\",\
         \"build_year\":2001,\"jibun_raw\":\"5\""
    );
    let expected = format!(
        "{{\"as_of_month\":\"2098-05-01\",\"transactions\":[{{\"dataset_slug\":\"{INDUSTRIAL}\",\
         \"deal_date\":\"2098-02-01\",\"deal_amount_10k_krw\":50000,\"deposit_10k_krw\":null,\
         \"monthly_rent_10k_krw\":null,\"rent_kind\":null,\"building_type\":\"일반\",\"building_use\":\"공장\",\
         \"building_name\":null,\"building_area_m2\":1.23456785E7,\"building_area_kind\":\"total_floor\",\
         \"land_area_m2\":1000.0,\"land_area_kind\":\"plottage\",\"floor\":null,\"build_year\":2001,\
         \"land_use\":\"계획관리\",\"jimok\":null,\"jibun_raw\":\"5\",\"share_dealing_type\":null,\
         \"dealing_gbn\":\"중개거래\",\"cancelled\":false,\"cancel_date\":null,\
         \"place_identity_grade\":\"{grade}\",\"attached_via\":null}}],\"transactions_omitted\":0,\
         \"main_lot_group_pnus\":[],\"comparables\":[{{{deal},\"pnu\":\"{factory}\",\
         \"place_identity_grade\":\"{grade}\",\"main_lot_group_key\":null,\"main_lot_group_size\":null}}],\
         \"comparables_from\":\"2095-05-01\",\"region\":{{\"legal_dong_code\":\"{OTHER}\",\"from\":\"2097-05-01\",\
         \"datasets\":[{{\"dataset_slug\":\"{INDUSTRIAL}\",\"deal_count\":1,\"cancelled_count\":0,\
         \"land_price_per_m2_10k_krw\":{{\"p25\":50.0,\"median\":50.0,\"p75\":50.0}},\
         \"building_price_per_m2_10k_krw\":{{\"p25\":0.0,\"median\":0.0,\"p75\":0.0}}}}]}},\
         \"coverage\":[{{\"dataset_slug\":\"real_transaction_apartment_trade\",\"from_month\":\"2095-05\"}}]}}"
    );
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn past_the_document_bound_the_oldest_deals_are_left_out() -> anyhow::Result<()> {
    let factory = pnu(OTHER, 5, 0);
    // ~600 characters a deal: 600 deals are past 255 KiB.
    let deals: Vec<Deal> = (0..600)
        .map(|n| {
            let mut deal = Deal::new(
                &format!("k-{n:04}"),
                OTHER_NAME,
                "5",
                &format!("2097-{:02}-{:02}", n % 12 + 1, n % 28 + 1),
            );
            deal.dataset = "real_transaction_detached_house_trade".to_owned();
            deal
        })
        .collect();
    let land = [(factory.clone(), "대", 1000.0)];
    let connection = inputs(&rows(&deals), &[], &[factory.clone()], &land, &[])?;
    let docs = documents(&connection, &json!([]), "2098-05-03")?;
    let (document, total, ..) = &docs[&factory];
    let kept = document["transactions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let omitted = document["transactions_omitted"]
        .as_i64()
        .unwrap_or_default();
    assert_eq!(*total, 600);
    assert!(omitted > 0, "{omitted}");
    assert_eq!(i64::try_from(kept.len())? + omitted, 600);
    // Newest first, and the kept ones are the newest.
    let days: Vec<String> = kept
        .iter()
        .map(|d| d["deal_date"].as_str().unwrap_or_default().to_owned())
        .collect();
    let mut sorted = days.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(days, sorted);
    let text: String =
        connection.query_row("SELECT real_transaction_json FROM gold_rows", [], |row| {
            row.get(0)
        })?;
    assert!(
        text.chars().count() <= 256 * 1024,
        "{}",
        text.chars().count()
    );
    Ok(())
}
