//! Which units a run rebuilds against a table's base.

use std::collections::{BTreeMap, BTreeSet};

use super::*;

const SERVED: &str = "synthetic__parcel-209906";

fn base(pins: &[(&str, i64)], code: &str, served: Option<&str>) -> Base {
    Base {
        snapshot_id: 1,
        version_seq: 1,
        pins: pins
            .iter()
            .map(|(input, snapshot)| ((*input).to_owned(), *snapshot))
            .collect(),
        code_version: code.to_owned(),
        served_edition: served.map(str::to_owned),
        current_records: 0,
        fingerprint: (0, 0),
        as_of: None,
    }
}

fn pins(registry: i64, admin: i64, changes: i64) -> BTreeMap<String, i64> {
    BTreeMap::from([
        (REGISTRY_SOURCE.to_owned(), registry),
        (ADMIN_SOURCE.to_owned(), admin),
        (CODE_CHANGE_SOURCE.to_owned(), changes),
    ])
}

fn at(registry: i64, admin: i64, changes: i64) -> Base {
    base(
        &[
            (REGISTRY_SOURCE, registry),
            (ADMIN_SOURCE, admin),
            (CODE_CHANGE_SOURCE, changes),
        ],
        "code",
        Some(SERVED),
    )
}

#[test]
fn a_first_build_or_new_code_rebuilds_both_units() {
    let every = BTreeSet::from([PARCEL_UNIT, ADMIN_UNIT]);
    assert_eq!(
        units_to_rebuild(None, &pins(1, 1, 1), "code", SERVED),
        every
    );
    assert_eq!(
        units_to_rebuild(Some(&at(1, 1, 1)), &pins(1, 1, 1), "new code", SERVED),
        every
    );
}

#[test]
fn a_unit_is_rebuilt_when_its_own_input_moved() {
    let base = at(1, 1, 1);
    assert!(units_to_rebuild(Some(&base), &pins(1, 1, 1), "code", SERVED).is_empty());
    assert_eq!(
        units_to_rebuild(Some(&base), &pins(2, 1, 1), "code", SERVED),
        BTreeSet::from([PARCEL_UNIT])
    );
    assert_eq!(
        units_to_rebuild(Some(&base), &pins(1, 2, 1), "code", SERVED),
        BTreeSet::from([ADMIN_UNIT])
    );
}

#[test]
fn a_code_change_moves_both_units_and_a_new_edition_the_parcels() {
    let base = at(1, 1, 1);
    assert_eq!(
        units_to_rebuild(Some(&base), &pins(1, 1, 2), "code", SERVED),
        BTreeSet::from([PARCEL_UNIT, ADMIN_UNIT])
    );
    assert_eq!(
        units_to_rebuild(
            Some(&base),
            &pins(1, 1, 1),
            "code",
            "synthetic__parcel-209909"
        ),
        BTreeSet::from([PARCEL_UNIT])
    );
}
