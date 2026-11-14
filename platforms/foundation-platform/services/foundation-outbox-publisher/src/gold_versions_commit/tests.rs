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
        key: "pnu",
        head: None,
        base: None,
        version_seq: 1,
        source_snapshot_id: "s2".to_owned(),
        published_at_utc: "t2".to_owned(),
    };
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
