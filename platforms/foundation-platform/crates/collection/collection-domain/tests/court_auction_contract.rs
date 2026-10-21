//! The committed court auction contract deserializes into the collection rules' own types
//! (root ADR-0289/0290). This is the SSOT seam: the values live in the contract, the rules read
//! them, and this test fails if the two drift apart.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;

use collection_domain::court_auction::{BlockSignals, PaceRule, PnuAssembly, RecrawlRule};
use serde::Deserialize;
use serde_json::Value;

const CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/courtauction-go-kr.contract.json");

/// The `collection` section of the contract, each field a rule's own type.
#[derive(Debug, Deserialize)]
struct Collection {
    block_signals: BlockSignals,
    change_fields: BTreeMap<String, Value>,
    pace: BTreeMap<String, Value>,
    recrawl: RecrawlRule,
    pnu_assembly: PnuAssembly,
}

fn collection() -> Collection {
    let contract: Value = serde_json::from_str(CONTRACT).expect("contract is valid JSON");
    serde_json::from_value(contract["collection"].clone())
        .expect("the collection section deserializes into the rules' types")
}

#[test]
fn block_signals_round_trip() {
    let signals = collection().block_signals;
    assert!(signals.http_statuses.contains(&429));
    assert_eq!(signals.ipcheck_pointer, "/data/ipcheck");
    assert_eq!(signals.ok_status, 200);
}

#[test]
fn every_pace_group_deserializes_into_a_rule() {
    let pace = collection().pace;
    for group in ["item_search", "case_detail", "goods_detail"] {
        let rule: PaceRule = serde_json::from_value(pace[group].clone())
            .unwrap_or_else(|error| panic!("{group} pace is not a PaceRule: {error}"));
        assert!(rule.min >= 1 && rule.min <= rule.start && rule.start <= rule.max);
        assert!(rule.decrease_divisor >= 2);
    }
}

#[test]
fn every_change_field_group_is_a_list_of_names() {
    let change_fields = collection().change_fields;
    let fields: Vec<String> = serde_json::from_value(change_fields["item_search"].clone())
        .expect("item_search change fields are a list of names");
    assert!(fields.iter().any(|f| f == "jinstatCd"));
}

#[test]
fn the_pnu_assembly_names_four_code_fields_and_two_ledger_kinds() {
    let pnu = collection().pnu_assembly;
    assert_eq!(pnu.legal_dong_code_fields.len(), 4);
    assert!(pnu.ledger_kind_digits.contains(&pnu.flat_ledger_kind));
    assert!(pnu.ledger_kind_digits.contains(&pnu.mountain_ledger_kind));
    assert_ne!(pnu.flat_ledger_kind, pnu.mountain_ledger_kind);
}

#[test]
fn the_recrawl_backstop_is_set() {
    assert!(collection().recrawl.max_age_days > 0);
}
