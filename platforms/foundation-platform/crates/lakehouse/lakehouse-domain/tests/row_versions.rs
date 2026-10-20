//! Every contract says how its rows pile up, in terms of its own columns (root ADR-0286).

use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseRowKey, LakehouseRowVersions,
    LakehouseTableContract,
};

/// The columns a declaration names, and the reasons it gives.
fn named(contract: &LakehouseTableContract) -> (Vec<&'static str>, Vec<&'static str>) {
    match contract.row_versions {
        LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Columns(key),
        } => (key.to_vec(), Vec::new()),
        LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Keyless { reason },
        }
        | LakehouseRowVersions::Unwritten { reason } => (Vec::new(), vec![reason]),
        LakehouseRowVersions::LatestPerKey(latest) => {
            let mut columns = [latest.key, latest.newest_first].concat();
            columns.extend(latest.removal_column);
            (columns, Vec::new())
        }
        LakehouseRowVersions::Editions {
            edition_column,
            newest_first,
            per,
            ..
        } => {
            assert!(
                !newest_first.is_empty(),
                "{}: an edition order is required",
                contract.table_name
            );
            ([&[edition_column], newest_first, per].concat(), Vec::new())
        }
        LakehouseRowVersions::Events { .. } => (Vec::new(), Vec::new()),
    }
}

#[test]
fn every_declaration_names_only_its_own_columns_and_says_why_when_it_has_no_key() {
    for contract in industrial_complex_lakehouse_contracts() {
        let (columns, reasons) = named(contract);
        for column in columns {
            assert!(
                contract
                    .columns
                    .iter()
                    .any(|declared| declared.name == column),
                "{}: row_versions names {column}, which is not one of its columns",
                contract.table_name
            );
        }
        for reason in reasons {
            // A reason names the source or the planned work, not a placeholder.
            assert!(
                reason.split_whitespace().count() >= 5,
                "{}: say why in a sentence, not {reason:?}",
                contract.table_name
            );
        }
        if let LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Columns(key),
        } = contract.row_versions
        {
            assert!(
                !key.is_empty(),
                "{}: an empty key is Keyless with a reason",
                contract.table_name
            );
        }
    }
}

#[test]
fn the_newest_per_key_rule_is_the_latest_per_key_declaration() {
    for contract in industrial_complex_lakehouse_contracts() {
        assert_eq!(
            contract.current_row_latest().is_some(),
            matches!(contract.row_versions, LakehouseRowVersions::LatestPerKey(_)),
            "{}",
            contract.table_name
        );
    }
}
