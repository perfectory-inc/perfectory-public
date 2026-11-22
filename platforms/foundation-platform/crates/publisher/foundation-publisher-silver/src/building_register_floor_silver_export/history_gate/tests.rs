//! The rules `building_register_floor_history.py` held, on a synthetic register.

use std::collections::BTreeMap;

use lakehouse_infrastructure::{IcebergRegisteredSnapshot, IcebergSnapshotRegistry};

use super::{
    identity_under_derivation, ingest_batch_token, registered_append, verify_metadata, Witness,
    INGEST_BATCH_OBJECTS_KEY, INGEST_BATCH_TOKEN_KEY,
};

const UUID: &str = "99999999-0000-4000-8000-000000000001";
const WITNESSED: &str = "building-register-floor-selection-v2-synthetic";
const NEXT: &str = "building-register-floor-content-v1-synthetic-209902";

fn snapshot(
    id: i64,
    parent: Option<i64>,
    operation: &str,
    recorded: Option<&str>,
    extra: &[(&str, &str)],
) -> IcebergRegisteredSnapshot {
    let mut summary = BTreeMap::from([("operation".to_owned(), operation.to_owned())]);
    if let Some(identity) = recorded {
        summary.insert(INGEST_BATCH_OBJECTS_KEY.to_owned(), identity.to_owned());
        summary.insert(
            INGEST_BATCH_TOKEN_KEY.to_owned(),
            ingest_batch_token(&[identity.to_owned()]),
        );
    }
    for (key, value) in extra {
        summary.insert((*key).to_owned(), (*value).to_owned());
    }
    IcebergRegisteredSnapshot {
        snapshot_id: id,
        parent_snapshot_id: parent,
        summary,
    }
}

fn witness() -> anyhow::Result<Witness> {
    Ok(Witness {
        table_uuid: UUID.parse()?,
        snapshot_id: 1,
        operation: "overwrite".to_owned(),
        source_snapshot_id: WITNESSED.to_owned(),
        row_count: 10,
    })
}

fn registry(
    snapshots: Vec<IcebergRegisteredSnapshot>,
    head: i64,
) -> anyhow::Result<IcebergSnapshotRegistry> {
    Ok(IcebergSnapshotRegistry {
        table_uuid: UUID.parse()?,
        head: Some(head),
        snapshots,
    })
}

fn history() -> Vec<IcebergRegisteredSnapshot> {
    vec![
        snapshot(
            1,
            None,
            "overwrite",
            Some(WITNESSED),
            &[("total-records", "10")],
        ),
        snapshot(2, Some(1), "append", Some(NEXT), &[("added-records", "12")]),
        snapshot(3, Some(2), "append", None, &[]),
    ]
}

#[test]
fn the_witness_and_an_append_only_descent_are_authenticated() -> anyhow::Result<()> {
    assert_eq!(verify_metadata(&witness()?, &registry(history(), 3)?)?, 3);
    Ok(())
}

#[test]
fn a_replaced_table_changed_evidence_or_a_rewrite_after_the_witness_is_refused(
) -> anyhow::Result<()> {
    let mut other = registry(history(), 3)?;
    other.table_uuid = "99999999-0000-4000-8000-000000000002".parse()?;
    assert!(
        verify_metadata(&witness()?, &other).is_err(),
        "another table"
    );

    let mut counted = history();
    counted[0]
        .summary
        .insert("total-records".to_owned(), "11".to_owned());
    assert!(
        verify_metadata(&witness()?, &registry(counted, 3)?).is_err(),
        "row count"
    );

    let mut rewritten = history();
    rewritten[1]
        .summary
        .insert("operation".to_owned(), "overwrite".to_owned());
    assert!(
        verify_metadata(&witness()?, &registry(rewritten, 3)?).is_err(),
        "not appends"
    );

    let mut twice = history();
    twice.push(snapshot(4, Some(3), "append", Some(WITNESSED), &[]));
    assert!(
        verify_metadata(&witness()?, &registry(twice, 4)?).is_err(),
        "ambiguous"
    );

    let mut duplicate = history();
    duplicate.push(snapshot(3, Some(2), "append", None, &[]));
    assert!(
        verify_metadata(&witness()?, &registry(duplicate, 3)?).is_err(),
        "duplicate ids"
    );
    Ok(())
}

#[test]
fn a_recorded_source_is_its_one_append_and_an_unrecorded_one_is_none() -> anyhow::Result<()> {
    let register = registry(history(), 3)?;
    let record = registered_append(NEXT, &register)?.map(|record| record.snapshot_id);
    assert_eq!(record, Some(2));
    assert!(registered_append("building-register-floor-content-v1-unseen", &register)?.is_none());

    let mut empty = history();
    empty[1]
        .summary
        .insert("added-records".to_owned(), "0".to_owned());
    assert!(
        registered_append(NEXT, &registry(empty, 3)?).is_err(),
        "no rows added"
    );
    Ok(())
}

#[test]
fn a_derivation_names_a_second_identity_and_refuses_separators() -> anyhow::Result<()> {
    assert_eq!(identity_under_derivation(NEXT, None)?, NEXT);
    assert_eq!(
        identity_under_derivation(NEXT, Some(" contract-2099 "))?,
        format!("{NEXT}@contract-2099")
    );
    for label in ["", "a@b", "a,b"] {
        assert!(
            identity_under_derivation(NEXT, Some(label)).is_err(),
            "{label:?}"
        );
    }
    // The token is the digest of the sorted identities (lakehouse_ingest.ingest_batch_token).
    assert_eq!(
        ingest_batch_token(&["b".to_owned(), "a".to_owned()]),
        "7e18f737311b2dc3b2f269dd78396b0351f14fb66efa879f768cb23181883c78"
    );
    Ok(())
}
