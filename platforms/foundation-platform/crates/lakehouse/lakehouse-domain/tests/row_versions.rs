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

#[test]
fn the_newest_edition_is_the_one_whose_order_is_greatest_and_a_tie_is_refused() {
    use lakehouse_domain::{NewestEditionError, REFERENCE_LEGAL_DONG_CODE_SNAPSHOT as CODES};
    use serde_json::{json, Map, Value};

    let row = |date: &str, code: &str| -> Map<String, Value> {
        json!({"snapshot_date": date, "region_cd": code})
            .as_object()
            .cloned()
            .unwrap_or_default()
    };
    let rows = [
        row("2099-01-01", "9900000000"),
        row("2099-02-01", "9900000000"),
        row("2099-02-01", "9800000000"),
    ];
    let newest = CODES.newest_edition_rows(&rows);
    assert_eq!(newest.as_ref().map(Vec::len), Ok(2));
    assert!(newest
        .unwrap_or_default()
        .iter()
        .all(|row| row["snapshot_date"] == "2099-02-01"));
    assert_eq!(CODES.newest_edition_rows(&[]).map(|rows| rows.len()), Ok(0));
    // A table that keeps versions per key has no editions to pick.
    let units = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
        "silver.building_register_units",
    );
    assert!(matches!(
        units.map(|contract| contract.newest_edition_rows(&rows)),
        Some(Err(NewestEditionError::NotEditions(_)))
    ));
}

#[test]
fn two_editions_tied_on_the_order_are_refused_and_the_factory_orders_by_its_day() {
    use lakehouse_domain::{
        NewestEditionError, SILVER_ADMINISTRATIVE_BOUNDARIES as ADMIN,
        SILVER_REGISTERED_FACTORY as FACTORY,
    };
    use serde_json::{json, Map, Value};

    let row = |edition: &str, at: &str| -> Map<String, Value> {
        json!({"source_snapshot_id": edition, "ingested_at_utc": at})
            .as_object()
            .cloned()
            .unwrap_or_default()
    };
    // Two administrative rounds loaded at one instant: nothing orders them.
    let tied = [
        row("round-a", "2099-12-31T00:00:00Z"),
        row("round-b", "2099-12-31T00:00:00Z"),
    ];
    assert!(matches!(
        ADMIN.newest_edition_rows(&tied),
        Err(NewestEditionError::Tied { .. })
    ));
    // An older 기준일 loaded later does not outrank a newer one (root ADR-0328).
    let older_loaded_later = [
        row("20990630", "2099-12-31T00:00:00Z"),
        row("20991231", "2099-07-01T00:00:00Z"),
    ];
    assert_eq!(
        FACTORY
            .newest_edition_rows(&older_loaded_later)
            .map(|rows| rows
                .iter()
                .map(|row| row["source_snapshot_id"].clone())
                .collect::<Vec<_>>()),
        Ok(vec![json!("20991231")])
    );
}
