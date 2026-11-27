use std::collections::{BTreeMap, BTreeSet};

use super::rekey::fixture::pre_epoch_row;
use super::rekey::LedgerRow;
use super::{
    bronze_lock, etag_carries_over, gzip_lines, manifest_key_of, plan_moves, read_manifest,
    slug_of, Listed, Move, MANIFEST_SCHEMA,
};

const SHA: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

/// A run-stamped object of a renamed source, as the court collector landed it before the epoch.
fn court_row(file_id: &str) -> anyhow::Result<LedgerRow> {
    pre_epoch_row(
        "courtauctiongokr__auction_item",
        "searchControllerMain",
        "operation=searchControllerMain/",
        file_id,
        "gz",
        false,
        SHA,
    )
}

fn listed(size_bytes: i64) -> Listed {
    Listed {
        size_bytes,
        etag: "0123456789abcdef0123456789abcdef".to_owned(),
        last_modified: "2099-01-02T16:30:00+00:00".to_owned(),
    }
}

fn renamed() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "courtauctiongokr__auction_item".to_owned(),
        "courtauctiongokr__item".to_owned(),
    )])
}

#[test]
fn the_plan_moves_each_stamped_row_once_and_deletes_only_unregistered_unrecorded_ids(
) -> anyhow::Result<()> {
    let row = court_row("auction-item-20990102t160504z")?;
    let listing = BTreeMap::from([
        (row.object_key.clone(), listed(9)),
        (
            "bronze/source=rtmolitkr__real_transaction_land_trade/period=2099-01/export.csv"
                .to_owned(),
            listed(3),
        ),
        // A registered id the ledger lacks: never deleted by the epoch (reported elsewhere).
        (
            "bronze/source=datagokr__pension_workplace/pension-workplace--sha256-00.csv".to_owned(),
            listed(4),
        ),
    ]);
    let known = BTreeSet::from([row.slug.clone()]);
    let moves = plan_moves(std::slice::from_ref(&row), &listing, &renamed(), &known)?;
    assert_eq!(moves.len(), 2, "{moves:#?}");
    let Move::Rekey {
        new_key,
        new_slug,
        etag,
        ..
    } = &moves[0]
    else {
        anyhow::bail!("the first move is the row's");
    };
    assert_eq!(new_slug, "courtauctiongokr__item");
    assert_eq!(new_key, &format!("bronze/source=courtauctiongokr__item/operation=searchControllerMain/item--sha256-{SHA}.gz"));
    assert_eq!(etag, "0123456789abcdef0123456789abcdef");
    assert!(
        matches!(&moves[1], Move::Delete { slug, .. } if slug == "rtmolitkr__real_transaction_land_trade")
    );
    Ok(())
}

#[test]
fn the_plan_refuses_what_would_split_or_overwrite_the_ledger() -> anyhow::Result<()> {
    let row = court_row("auction-item-20990102t160504z")?;
    let listing = BTreeMap::from([(row.object_key.clone(), listed(9))]);
    // A writer created a row under the new id: renaming in place would collide (F4).
    let both = BTreeSet::from([row.slug.clone(), "courtauctiongokr__item".to_owned()]);
    assert!(plan_moves(std::slice::from_ref(&row), &listing, &renamed(), &both).is_err());
    let known = BTreeSet::from([row.slug.clone()]);
    // R2 lacks the object, or holds other bytes than the ledger says.
    assert!(plan_moves(
        std::slice::from_ref(&row),
        &BTreeMap::new(),
        &renamed(),
        &known
    )
    .is_err());
    let wrong = BTreeMap::from([(row.object_key.clone(), listed(8))]);
    assert!(plan_moves(std::slice::from_ref(&row), &wrong, &renamed(), &known).is_err());
    // Two runs' objects of the same bytes would meet at one key.
    let twin = court_row("auction-item-20990103t160504z")?;
    let listing = BTreeMap::from([
        (row.object_key.clone(), listed(9)),
        (twin.object_key.clone(), listed(9)),
    ]);
    assert!(plan_moves(&[row, twin], &listing, &renamed(), &known).is_err());
    Ok(())
}

#[test]
fn the_manifest_reads_back_what_was_written_and_refuses_a_short_one() -> anyhow::Result<()> {
    let moves = vec![Move::Delete {
        slug: "rtmolitkr__x".to_owned(),
        old_key: "bronze/source=rtmolitkr__x/a.csv".to_owned(),
        size_bytes: 1,
        etag: "e".to_owned(),
        last_modified: "2099-01-01T00:00:00Z".to_owned(),
    }];
    let header = serde_json::json!({"schema_version": MANIFEST_SCHEMA, "moves": 1});
    assert_eq!(read_manifest(&gzip_lines(&header, &moves)?)?, moves);
    let short = serde_json::json!({"schema_version": MANIFEST_SCHEMA, "moves": 2});
    assert!(read_manifest(&gzip_lines(&short, &moves)?).is_err());
    let evidence = BTreeMap::from([
        (
            "control/evidence/r2_move/2099-01-02-bronze-epoch.jsonl.gz".to_owned(),
            listed(1),
        ),
        (
            "control/evidence/r2_move/2099-01-02-bronze-epoch.report.json".to_owned(),
            listed(1),
        ),
    ]);
    assert_eq!(
        manifest_key_of(&evidence)?.as_deref(),
        Some("control/evidence/r2_move/2099-01-02-bronze-epoch.jsonl.gz")
    );
    Ok(())
}

#[test]
fn the_lock_is_read_from_its_declaration() -> anyhow::Result<()> {
    assert_eq!(bronze_lock()?.num_days(), 30);
    // A part-uploaded original's ETag is not its copy's (W108 3c); its SHA-256 readback proves it.
    assert!(etag_carries_over("0123456789abcdef0123456789abcdef"));
    assert!(!etag_carries_over("0123456789abcdef0123456789abcdef-4"));
    assert_eq!(slug_of("bronze/source=a__b/c.gz"), Some("a__b"));
    assert_eq!(slug_of("control/x"), None);
    Ok(())
}
