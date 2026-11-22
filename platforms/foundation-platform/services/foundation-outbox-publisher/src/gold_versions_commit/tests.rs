//! The plan, the diff and the fingerprint of a versions table's commit, on `DuckDB` tables.

use std::collections::BTreeMap;

use lakehouse_domain::{gold_versions_table, industrial_complex_lakehouse_contracts};
use lakehouse_infrastructure::IcebergSnapshotSummary;

use super::*;

fn snapshot(id: i64, summary: &[(&str, &str)]) -> IcebergSnapshotSummary {
    IcebergSnapshotSummary {
        snapshot_id: id,
        timestamp_ms: None,
        summary: summary
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    }
}

#[test]
fn the_base_is_the_newest_commit_that_records_the_sequence_past_compactions() -> anyhow::Result<()>
{
    let built = [
        (VERSION_SEQ_PROPERTY, "7"),
        (CODE_VERSION_PROPERTY, "c"),
        (CURRENT_RECORDS_PROPERTY, "3"),
        (FINGERPRINT_PROPERTY, "rows:3;digest_sum:42"),
        (SOURCE_SNAPSHOTS_PROPERTY, r#"{"silver.a":"11"}"#),
        ("total-records", "5"),
    ];
    let history = [
        snapshot(3, &[("operation", "replace"), ("total-records", "5")]),
        snapshot(2, &built),
        snapshot(1, &[(VERSION_SEQ_PROPERTY, "6")]),
    ];
    let found = base(&history)?.context("a base")?;
    assert_eq!((found.snapshot_id, found.version_seq), (2, 7));
    assert_eq!(found.pins, BTreeMap::from([("silver.a".to_owned(), 11)]));
    assert_eq!(found.fingerprint, (3, 42));
    // A new table, or one with no row, starts at nothing.
    assert_eq!(base(&[])?, None);
    assert_eq!(base(&[snapshot(1, &[("total-records", "0")])])?, None);
    // Rows and no sequence: the order cannot be continued.
    assert!(base(&[snapshot(1, &[("total-records", "4")])]).is_err());
    Ok(())
}

#[test]
fn a_run_is_named_by_its_inputs_code_and_edition() -> anyhow::Result<()> {
    let pins = BTreeMap::from([("silver.a".to_owned(), 11_i64), ("silver.b".to_owned(), 12)]);
    let one = run_id("gold.t_versions", &pins, "c", "e");
    assert_eq!(one, run_id("gold.t_versions", &pins, "c", "e"));
    assert_ne!(one, run_id("gold.t_versions", &pins, "d", "e"));
    assert!(one.starts_with("gold-t_versions-") && !one.contains(','));
    assert_eq!(
        pins_property(&pins)?,
        r#"{"silver.a":"11","silver.b":"12"}"#
    );
    Ok(())
}

fn digest(n: u32) -> String {
    format!("{n:015x}{}", "0".repeat(49))
}

#[test]
fn only_changed_keys_are_appended_and_a_key_that_left_gets_a_marker() -> anyhow::Result<()> {
    let replaced = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == "gold.parcel_real_transaction")
        .context("a by-PNU Gold")?;
    let versions = gold_versions_table(replaced).map_err(anyhow::Error::msg)?;
    let content: Vec<&'static str> = replaced.columns.iter().map(|column| column.name).collect();
    let connection = foundation_publisher_silver::duckdb::Connection::open_in_memory()?;
    let columns = content
        .iter()
        .map(|name| {
            let kind = if *name == "transaction_total" {
                "BIGINT"
            } else {
                "VARCHAR"
            };
            format!("{name} {kind}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!("CREATE TABLE built ({columns});"))?;
    let row = |pnu: &str, n: u32| {
        let values = content
            .iter()
            .map(|name| match *name {
                "pnu" => format!("'{pnu}'"),
                "row_digest" => format!("'{}'", digest(n)),
                "transaction_total" => "1".to_owned(),
                "source_snapshot_id" => "'s2'".to_owned(),
                "published_at_utc" => "'t2'".to_owned(),
                _ => "'x'".to_owned(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("({values})")
    };
    // The head's current rows: a (1), b (2), c (3). The build: a unchanged, b changed, d new.
    connection.execute_batch(&format!(
        "INSERT INTO built VALUES {}, {}, {};",
        row("9999910100100010000", 1),
        row("9999910100100020000", 9),
        row("9999910100100040000", 4),
    ))?;
    let mut commit = Commit {
        table: "gold.parcel_real_transaction_versions",
        versions: &versions,
        content_columns: content.clone(),
        head: None,
        base: None,
        version_seq: 1,
        source_snapshot_id: "s2".to_owned(),
        published_at_utc: "t2".to_owned(),
    };
    // A by-PNU table: its key is the PNU and its digest the row_digest it carries, as before the
    // Golds outside the lanes came to the same commit.
    assert_eq!(commit.key()?, ["pnu"]);
    assert_eq!(commit.digest_sql(), "row_digest");
    // A first commit: every key is new.
    let first = diff(&connection, &commit, "TRUE")?;
    assert_eq!((first.added, first.changed, first.removed), (3, 0, 0));
    let ((rows, _), current) = after(&connection, &commit, &first)?;
    assert_eq!((rows, current), (3, 3));

    // A later commit over a head whose current rows are a (1), b (2), c (3): `diff` reads the head
    // through the catalog, so the head's rows are put in place here and the same statement runs.
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE current_rows AS SELECT * FROM built WHERE FALSE; \
         INSERT INTO current_rows VALUES {}, {}, {};",
        row("9999910100100010000", 1),
        row("9999910100100020000", 2),
        row("9999910100100030000", 3),
    ))?;
    commit.version_seq = 2;
    commit.base = Some(Base {
        snapshot_id: 1,
        version_seq: 1,
        pins: BTreeMap::new(),
        code_version: "c".to_owned(),
        served_edition: None,
        current_records: 3,
        fingerprint: (3, 1 + 2 + 3),
    });
    connection.execute_batch(&appended_sql(&commit)?)?;
    let changes = connection
        .prepare("SELECT pnu, change, row_state FROM appended ORDER BY pnu")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        changes,
        [
            ("9999910100100020000".to_owned(), "changed".to_owned(), None),
            (
                "9999910100100030000".to_owned(),
                "removed".to_owned(),
                Some("removed".to_owned())
            ),
            ("9999910100100040000".to_owned(), "added".to_owned(), None),
        ]
    );
    let counted = Diff {
        built: 3,
        added: 1,
        changed: 1,
        removed: 1,
        current_in_range: 3,
    };
    let ((rows, sum), current) = after(&connection, &commit, &counted)?;
    // a (1) stays, b 2 -> 9, c (3) leaves, d (4) arrives: 1 + 9 + 4.
    assert_eq!((rows, sum, current), (3, 14, 3));
    assert_eq!(fingerprint_text((rows, sum)), "rows:3;digest_sum:14");
    Ok(())
}

/// A Gold outside the by-PNU lanes (the steward queue): a key of two columns and no `row_digest`,
/// so the digest is of the content columns but the stamps, and a row that differs only in its
/// stamp is not appended again.
#[test]
fn a_table_without_a_row_digest_appends_by_its_content_and_its_whole_key() -> anyhow::Result<()> {
    let versions = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == "gold.lineage_review_queue_versions")
        .context("the steward queue's versions table")?;
    let replaced = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == "gold.lineage_review_queue")
        .context("the steward queue")?;
    let content: Vec<&'static str> = replaced.columns.iter().map(|column| column.name).collect();
    let connection = foundation_publisher_silver::duckdb::Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE built (unit VARCHAR, subject_code VARCHAR, item_id VARCHAR, status VARCHAR, \
           candidates_json VARCHAR, evidence_etag VARCHAR, from_snapshot_id VARCHAR, \
           to_snapshot_id VARCHAR, published_at_utc TIMESTAMPTZ); \
         CREATE TABLE current_rows AS SELECT * FROM built WHERE FALSE; \
         INSERT INTO current_rows VALUES \
           ('parcel', '9999910100100010000', 'i1', 'pending', '[]', 'e1', NULL, NULL, TIMESTAMPTZ '2099-01-01 00:00:00+00'), \
           ('parcel', '9999910100100020000', 'i2', 'pending', '[]', 'e2', NULL, NULL, TIMESTAMPTZ '2099-01-01 00:00:00+00'), \
           ('place_reference', '9999910100100010000', 'i3', 'steward', '{}', 'e3', NULL, 's1', TIMESTAMPTZ '2099-01-01 00:00:00+00'); \
         INSERT INTO built VALUES \
           ('parcel', '9999910100100010000', 'i1', 'pending', '[]', 'e1', NULL, NULL, TIMESTAMPTZ '2099-01-02 00:00:00+00'), \
           ('parcel', '9999910100100020000', 'i2', 'needs_review', '[]', 'e9', NULL, NULL, TIMESTAMPTZ '2099-01-02 00:00:00+00'), \
           ('place_reference', '9999910100100020000', 'i4', 'steward', '{}', 'e4', NULL, 's2', TIMESTAMPTZ '2099-01-02 00:00:00+00');",
    )?;
    let commit = Commit {
        table: "gold.lineage_review_queue_versions",
        versions,
        content_columns: content,
        head: Some(1),
        base: None,
        version_seq: 2,
        source_snapshot_id: "s2".to_owned(),
        published_at_utc: "2099-01-02T00:00:00Z".to_owned(),
    };
    assert_eq!(commit.key()?, ["unit", "subject_code"]);
    assert!(commit
        .digest_sql()
        .starts_with("sha256(to_json({'unit': unit"));
    assert!(!commit.digest_sql().contains("published_at_utc"));
    connection.execute_batch(&appended_sql(&commit)?)?;
    let changes = connection
        .prepare(
            "SELECT unit, subject_code, change, CAST(published_at_utc AS VARCHAR) FROM appended \
             ORDER BY unit, subject_code",
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let (changed, removed, added) = (&changes[0], &changes[1], &changes[2]);
    // i1 differs only in its stamp: not appended. The same PNU under another unit is another key.
    assert_eq!(changes.len(), 3);
    assert_eq!(
        (changed.1.as_str(), changed.2.as_str()),
        ("9999910100100020000", "changed")
    );
    assert_eq!(
        (removed.0.as_str(), removed.1.as_str(), removed.2.as_str()),
        ("place_reference", "9999910100100010000", "removed")
    );
    assert_eq!(
        (added.0.as_str(), added.1.as_str(), added.2.as_str()),
        ("place_reference", "9999910100100020000", "added")
    );
    // The removal marker carries this commit's stamp as the column's type.
    assert!(removed.3.starts_with("2099-01-02"));
    Ok(())
}

#[test]
fn a_partition_splits_files_by_the_sido_of_the_pnu_or_by_its_identity_column() {
    assert_eq!(
        partition_expression("truncate(2, pnu)").ok().as_deref(),
        Some("substr(pnu, 1, 2)")
    );
    assert_eq!(partition_expression("unit").ok().as_deref(), Some("unit"));
    assert!(partition_expression("bucket(4, item_id)").is_err());
}

fn build_tag(snapshot_id: i64) -> lakehouse_infrastructure::IcebergSnapshotRef {
    lakehouse_infrastructure::IcebergSnapshotRef {
        snapshot_id,
        kind: "tag".to_owned(),
    }
}

#[test]
fn a_lane_serving_the_replaced_table_shadows_its_build_tags_inputs() -> anyhow::Result<()> {
    let history = [
        snapshot(9, &[("operation", "replace")]),
        snapshot(
            8,
            &[
                (
                    SOURCE_SNAPSHOTS_PROPERTY,
                    r#"{"silver.a":"11","silver.b":"12"}"#,
                ),
                (SERVED_EDITION_PROPERTY, "vworld-202611"),
            ],
        ),
    ];
    let found =
        shadow("gold.x_versions", "gold.x", Some(&build_tag(8)), &history)?.context("a shadow")?;
    assert_eq!(found.replaced, "gold.x");
    assert_eq!(found.build, 8);
    assert_eq!(
        found.pins,
        BTreeMap::from([("silver.a".to_owned(), 11), ("silver.b".to_owned(), 12)])
    );
    assert_eq!(found.served_edition.as_deref(), Some("vworld-202611"));
    Ok(())
}

#[test]
fn a_lane_serving_its_versions_table_has_no_shadow() -> anyhow::Result<()> {
    assert_eq!(
        shadow("gold.x_versions", "gold.x_versions", None, &[])?,
        None
    );
    Ok(())
}

#[test]
fn a_shadow_refuses_what_it_cannot_compare_with() {
    let pinned = [snapshot(
        8,
        &[(SOURCE_SNAPSHOTS_PROPERTY, r#"{"silver.a":"11"}"#)],
    )];
    let unpinned = [snapshot(8, &[("operation", "append")])];
    for (served, tag, history, says) in [
        ("gold.y", Some(build_tag(8)), &pinned[..], "neither"),
        ("gold.x", None, &pinned[..], "no foundation-gold-build tag"),
        (
            "gold.x",
            Some(build_tag(7)),
            &pinned[..],
            "not in its main lineage",
        ),
        ("gold.x", Some(build_tag(8)), &unpinned[..], "records no"),
    ] {
        let error = shadow("gold.x_versions", served, tag.as_ref(), history)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(error.contains(says), "{served} {tag:?}: {error}");
    }
}

fn at(id: i64, ms: i64, operation: &str) -> IcebergSnapshotSummary {
    IcebergSnapshotSummary {
        timestamp_ms: Some(ms),
        ..snapshot(id, &[("operation", operation)])
    }
}

/// The Spark served-boundary job tags no build: the shadow follows its newest commit that is not a
/// compaction, with each input as of that commit, and an input with no snapshot by then is absent.
#[test]
fn an_untagged_build_is_its_newest_commit_but_compactions_with_its_inputs_as_of_then(
) -> anyhow::Result<()> {
    let history = [
        at(30, 3_000, "replace"),
        at(20, 2_000, "overwrite"),
        at(10, 1_000, "overwrite"),
    ];
    let inputs = BTreeMap::from([
        (
            "silver.parcel_boundaries".to_owned(),
            vec![
                at(7, 2_500, "append"),
                at(6, 1_500, "replace"),
                at(5, 500, "append"),
            ],
        ),
        (
            "silver.map_edit_ledger".to_owned(),
            vec![at(9, 2_100, "append")],
        ),
    ]);
    let found = untagged_shadow(
        "gold.parcel_boundary_served_versions",
        "gold.parcel_boundary_served",
        &history,
        &inputs,
    )?;
    assert_eq!(found.replaced, "gold.parcel_boundary_served");
    assert_eq!(found.build, 20, "the compaction after it is not a build");
    assert_eq!(
        found.pins,
        BTreeMap::from([("silver.parcel_boundaries".to_owned(), 6)]),
        "the parcels as of the build; the ledger had no snapshot yet"
    );
    assert_eq!(found.served_edition, None);
    Ok(())
}

#[test]
fn an_untagged_shadow_refuses_what_it_cannot_follow() {
    let inputs = BTreeMap::from([(
        "silver.parcel_boundaries".to_owned(),
        vec![at(5, 500, "append")],
    )]);
    let compactions = [at(30, 3_000, "replace")];
    let built = [at(20, 2_000, "overwrite")];
    let early = [at(20, 100, "overwrite")];
    for (served, history, says) in [
        ("gold.y", &built[..], "neither"),
        (
            "gold.parcel_boundary_served",
            &compactions[..],
            "only compactions",
        ),
        ("gold.parcel_boundary_served", &early[..], "no input"),
    ] {
        let error = untagged_shadow(
            "gold.parcel_boundary_served_versions",
            served,
            history,
            &inputs,
        )
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
        assert!(error.contains(says), "{served}: {error}");
    }
}

fn coverage(counts: &[(&str, u64)]) -> Coverage {
    counts
        .iter()
        .map(|(region, present)| (("x_json".to_owned(), (*region).to_owned()), *present))
        .collect()
}

const BOUND: CoverageBound = CoverageBound {
    max_drop_fraction: 0.05,
    min_region_pnus: 1000,
};

#[test]
fn a_build_that_empties_one_sido_is_refused_though_the_whole_holds() {
    let served = coverage(&[("11", 50_000), ("26", 2_000)]);
    let built = coverage(&[("11", 50_000), ("26", 1_000)]);
    let found = coverage_breaches(&served, &built, &[], BOUND);
    assert_eq!(
        found,
        vec!["x_json in 시도 26: 2000 -> 1000 PNUs".to_owned()]
    );
}

#[test]
fn a_small_sido_is_judged_only_inside_the_whole() {
    let served = coverage(&[("11", 50_000), ("36", 900)]);
    let built = coverage(&[("11", 50_000), ("36", 0)]);
    assert!(coverage_breaches(&served, &built, &[], BOUND).is_empty());
}

#[test]
fn a_recorded_reorganization_moves_pnus_without_a_loss() {
    // 29 and 46 served, 12 built: the code change table records both pairs.
    let served = coverage(&[("29", 3_000), ("46", 4_000)]);
    let built = coverage(&[("12", 7_000)]);
    let pairs = [
        ("29".to_owned(), "12".to_owned()),
        ("46".to_owned(), "12".to_owned()),
    ];
    assert!(coverage_breaches(&served, &built, &pairs, BOUND).is_empty());
    // Without the record the move is a loss of both 시도.
    assert_eq!(coverage_breaches(&served, &built, &[], BOUND).len(), 2);
}

/// The same commit as `only_changed_keys_are_appended_and_a_key_that_left_gets_a_marker`, diffed in
/// two ranges and added up: the rows to append and the fingerprint after are the whole diff's.
#[test]
fn a_build_diffed_range_by_range_appends_and_counts_what_one_diff_does() -> anyhow::Result<()> {
    let replaced = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == "gold.parcel_real_transaction")
        .context("a by-PNU Gold")?;
    let versions = gold_versions_table(replaced).map_err(anyhow::Error::msg)?;
    let content: Vec<&'static str> = replaced.columns.iter().map(|column| column.name).collect();
    let connection = foundation_publisher_silver::duckdb::Connection::open_in_memory()?;
    let columns = content
        .iter()
        .map(|name| {
            let kind = if *name == "transaction_total" {
                "BIGINT"
            } else {
                "VARCHAR"
            };
            format!("{name} {kind}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let row = |pnu: &str, n: u32| {
        let values = content
            .iter()
            .map(|name| match *name {
                "pnu" => format!("'{pnu}'"),
                "row_digest" => format!("'{}'", digest(n)),
                "transaction_total" => "1".to_owned(),
                "source_snapshot_id" => "'s2'".to_owned(),
                "published_at_utc" => "'t2'".to_owned(),
                _ => "'x'".to_owned(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("({values})")
    };
    let commit = Commit {
        table: "gold.parcel_real_transaction_versions",
        versions: &versions,
        content_columns: content.clone(),
        key: "pnu",
        head: Some(1),
        base: Some(Base {
            snapshot_id: 1,
            version_seq: 1,
            pins: BTreeMap::new(),
            code_version: "c".to_owned(),
            served_edition: None,
            current_records: 3,
            fingerprint: (3, 1 + 2 + 3),
        }),
        version_seq: 2,
        source_snapshot_id: "s2".to_owned(),
        published_at_utc: "t2".to_owned(),
    };
    // Range one: a unchanged (1), b changed (2 -> 9). Range two: c left (3), d new (4).
    let ranges = [
        (
            vec![row("9999910100100010000", 1), row("9999910100100020000", 9)],
            vec![row("9999910100100010000", 1), row("9999910100100020000", 2)],
        ),
        (
            vec![row("9999910100100040000", 4)],
            vec![row("9999910100100030000", 3)],
        ),
    ];
    let mut changes = Vec::new();
    let mut summed_digests = Digests::default();
    for (built, current) in &ranges {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE built ({columns}); INSERT INTO built VALUES {};              CREATE OR REPLACE TABLE current_rows AS SELECT * FROM built WHERE FALSE;              INSERT INTO current_rows VALUES {};",
            built.join(", "),
            current.join(", "),
        ))?;
        // `diff` reads the head through the catalog; here its rows are in place and the same
        // statement runs, then the range's digests are summed and its appended rows kept.
        connection.execute_batch(&appended_sql(&commit)?)?;
        summed_digests += digests(&connection, &commit)?;
        changes.extend(
            connection
                .prepare("SELECT pnu, change FROM appended ORDER BY pnu")?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    assert_eq!(
        changes,
        [
            ("9999910100100020000".to_owned(), "changed".to_owned()),
            ("9999910100100030000".to_owned(), "removed".to_owned()),
            ("9999910100100040000".to_owned(), "added".to_owned()),
        ]
    );
    let mut summed = Diff {
        built: 2,
        added: 0,
        changed: 1,
        removed: 0,
        current_in_range: 2,
    };
    summed += Diff {
        built: 1,
        added: 1,
        changed: 0,
        removed: 1,
        current_in_range: 1,
    };
    let ((rows, sum), current) = after_digests(&commit, &summed, summed_digests)?;
    assert_eq!(
        (rows, sum, current),
        (3, 14, 3),
        "the whole diff's fingerprint"
    );
    Ok(())
}
