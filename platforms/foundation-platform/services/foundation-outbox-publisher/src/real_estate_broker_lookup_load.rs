//! Loads the broker sign-up roster from Silver, by changed office only (root ADR-0550 §2).
//!
//! The changed offices are the keys of the Silver rows at or after the last load's watermark, read
//! at one pinned snapshot of each table; their current rows come from the contract's newest-per-key
//! rule, rendered by `LakehouseCurrentRowLatest::trino_current_rows_sql`. Each batch of offices is
//! one transaction that retires their roster rows, inserts their current rows and records the
//! batch; the last batch completes the load. A run stopped at its limit keeps its batches and the
//! next run goes on with the offices they did not hold (root ADR-0457). The first load has no
//! watermark and reads every office: the declared exception.

use std::collections::BTreeSet;
use std::io::Write as _;

use anyhow::{bail, Context};
use catalog_domain::real_estate_broker::{name_key, number_key};
use lakehouse_domain::lakehouse::LakehouseTableContract;
use lakehouse_domain::{SILVER_REAL_ESTATE_BROKER_MEMBER, SILVER_REAL_ESTATE_BROKER_OFFICE};
use serde::Deserialize;
use sqlx::{Connection, PgConnection, Row};

use crate::handoff_object_support::copy_text_escape;
use crate::public_data_control_support::{optional_env_value, required_env_value};
use crate::trino_cli::TrinoRows;

const TRINO_CONTAINER_ENV: &str = "FOUNDATION_PLATFORM_REAL_ESTATE_BROKER_LOOKUP_TRINO_CONTAINER";
const TRINO_CATALOG: &str = "r2";
/// Changed offices per Trino query, so one statement stays a few tens of kilobytes.
const KEYS_PER_QUERY: usize = 500;
/// Offices per committed batch: the first load's 106,429 offices are six batches, and a run stopped
/// at its limit keeps every batch it committed (root ADR-0457).
const OFFICES_PER_BATCH: usize = 20_000;
/// The watermark of a load that has not completed once: every Silver row is at or after it.
const UNLOADED_WATERMARK: &str = "1970-01-01 00:00:00";

const OFFICE_COLUMNS: &[&str] = &[
    "sigungu_code",
    "registration_no",
    "office_name",
    "status_code",
    "status_name",
    "data_reference_date",
];
const MEMBER_COLUMNS: &[&str] = &[
    "sigungu_code",
    "registration_no",
    "member_name",
    "member_kind_code",
    "member_kind_name",
    "license_no",
    "position_code",
    "position_name",
    "data_reference_date",
];

type OfficeKey = (String, String);

#[derive(Deserialize)]
struct Snapshot {
    snapshot_id: i64,
}

#[derive(Deserialize)]
struct ChangedKey {
    sigungu_code: String,
    registration_no: String,
    newest: String,
}

#[derive(Deserialize)]
struct OfficeRow {
    sigungu_code: String,
    registration_no: String,
    office_name: Option<String>,
    status_code: Option<String>,
    status_name: Option<String>,
    data_reference_date: Option<String>,
}

#[derive(Deserialize)]
struct MemberRow {
    sigungu_code: String,
    registration_no: String,
    member_name: Option<String>,
    member_kind_code: Option<String>,
    member_kind_name: Option<String>,
    license_no: Option<String>,
    position_code: Option<String>,
    position_name: Option<String>,
    data_reference_date: Option<String>,
}

/// One Silver table at the snapshot this load pinned.
struct Pinned {
    contract: &'static LakehouseTableContract,
    snapshot_id: i64,
}

impl Pinned {
    fn source(&self) -> String {
        format!(
            "{TRINO_CATALOG}.{} FOR VERSION AS OF {}",
            self.contract.table_name, self.snapshot_id
        )
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A watermark is a Trino timestamp as Trino wrote it; anything else is not put into SQL.
fn valid_watermark(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 19
        && bytes[..19]
            .iter()
            .enumerate()
            .all(|(index, byte)| match index {
                4 | 7 => *byte == b'-',
                10 => *byte == b' ',
                13 | 16 => *byte == b':',
                _ => byte.is_ascii_digit(),
            })
        && value[19..]
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || " ._/+:-".contains(character))
}

async fn pin(container: &str, contract: &'static LakehouseTableContract) -> anyhow::Result<Pinned> {
    let (schema, table) = contract
        .table_name
        .split_once('.')
        .context("a Silver table name is schema.table")?;
    let sql = format!(
        "SELECT snapshot_id FROM {TRINO_CATALOG}.{schema}.\"{table}$history\" \
         WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1"
    );
    let mut reader = TrinoRows::start(container, &sql)?;
    let mut head = None;
    while let Some(row) = reader.next::<Snapshot>().await? {
        head = Some(row.snapshot_id);
    }
    let snapshot_id = head
        .filter(|snapshot| *snapshot > 0)
        .with_context(|| format!("{} has no current snapshot yet", contract.table_name))?;
    Ok(Pinned {
        contract,
        snapshot_id,
    })
}

/// The offices with a row at or after `watermark` (every office when there is none), and the
/// newest `ingested_at_utc` among those rows.
async fn changed_offices(
    container: &str,
    pinned: &Pinned,
    watermark: Option<&str>,
) -> anyhow::Result<(BTreeSet<OfficeKey>, Option<String>)> {
    let filter = watermark.map_or_else(
        || "TRUE".to_owned(),
        |watermark| format!("ingested_at_utc >= TIMESTAMP {}", quote(watermark)),
    );
    let sql = format!(
        "SELECT sigungu_code, registration_no, CAST(max(ingested_at_utc) AS varchar) AS newest \
         FROM {} WHERE {filter} GROUP BY sigungu_code, registration_no",
        pinned.source()
    );
    let mut reader = TrinoRows::start(container, &sql)?;
    let mut keys = BTreeSet::new();
    let mut newest: Option<String> = None;
    while let Some(row) = reader.next::<ChangedKey>().await? {
        if !valid_watermark(&row.newest) {
            bail!(
                "{} carries an ingested_at_utc Trino did not write as a timestamp",
                pinned.contract.table_name
            );
        }
        // Trino writes one fixed-width form per column type, so the text orders as the time does.
        if newest
            .as_deref()
            .is_none_or(|current| row.newest.as_str() > current)
        {
            newest = Some(row.newest.clone());
        }
        keys.insert((row.sigungu_code, row.registration_no));
    }
    Ok((keys, newest))
}

/// The current rows of `keys`, a chunk of keys per query; every office when `all`.
async fn current_rows<T: serde::de::DeserializeOwned>(
    container: &str,
    pinned: &Pinned,
    columns: &[&str],
    keys: &BTreeSet<OfficeKey>,
    all: bool,
) -> anyhow::Result<Vec<T>> {
    let latest = pinned.contract.current_row_latest().with_context(|| {
        format!(
            "{} keeps no newest-per-key rule",
            pinned.contract.table_name
        )
    })?;
    let filters: Vec<String> = if all {
        vec!["TRUE".to_owned()]
    } else {
        keys.iter()
            .collect::<Vec<_>>()
            .chunks(KEYS_PER_QUERY)
            .map(|chunk| {
                let rows = chunk
                    .iter()
                    .map(|(sigungu, registration)| {
                        format!("ROW({}, {})", quote(sigungu), quote(registration))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("(sigungu_code, registration_no) IN ({rows})")
            })
            .collect()
    };
    let mut rows = Vec::new();
    for filter in filters {
        let sql = latest.trino_current_rows_sql(&pinned.source(), columns, &filter);
        let mut reader = TrinoRows::start(container, &sql)?;
        while let Some(row) = reader.next::<T>().await? {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn copy_field(value: Option<&str>) -> String {
    value.map_or_else(|| "\\N".to_owned(), copy_text_escape)
}

fn copy_line(fields: &[Option<&str>]) -> String {
    let mut line = fields
        .iter()
        .map(|field| copy_field(*field))
        .collect::<Vec<_>>()
        .join("\t");
    line.push('\n');
    line
}

async fn copy_rows(conn: &mut PgConnection, sql: &str, lines: &[String]) -> anyhow::Result<()> {
    let mut copy = conn.copy_in_raw(sql).await?;
    let mut buffer = String::with_capacity(1024 * 1024);
    for line in lines {
        buffer.push_str(line);
        if buffer.len() >= 1024 * 1024 {
            copy.send(buffer.as_bytes()).await?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        copy.send(buffer.as_bytes()).await?;
    }
    copy.finish().await?;
    Ok(())
}

/// Loads the roster rows of the offices that changed since the last load.
///
/// # Errors
/// Refuses missing configuration, a Silver table without a snapshot, failed reads, and a failed
/// write; a failed run writes nothing and the next one reads from the same watermark.
pub async fn run() -> anyhow::Result<()> {
    let container = optional_env_value(TRINO_CONTAINER_ENV)?
        .unwrap_or_else(|| "foundation-platform-trino".to_owned());
    let mut conn = PgConnection::connect(&required_env_value("DATABASE_URL")?).await?;
    let completed = sqlx::query(
        "SELECT id, office_watermark, member_watermark FROM catalog.real_estate_broker_lookup_load
         WHERE completes_load ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&mut conn)
    .await?
    .map(|row| -> Result<(i64, String, String), sqlx::Error> {
        Ok((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?))
    })
    .transpose()?;
    let last = completed
        .as_ref()
        .map(|(_, office, member)| (office.clone(), member.clone()));
    // The batches a stopped run committed after the last completed load: their offices are loaded
    // already, and this run goes on with the rest.
    let resumed = loaded_since(&mut conn, completed.as_ref().map(|(id, _, _)| *id)).await?;
    let office = pin(&container, &SILVER_REAL_ESTATE_BROKER_OFFICE).await?;
    let member = pin(&container, &SILVER_REAL_ESTATE_BROKER_MEMBER).await?;
    let (office_keys, office_newest) = changed_offices(
        &container,
        &office,
        last.as_ref().map(|(office, _)| office.as_str()),
    )
    .await?;
    let (member_keys, member_newest) = changed_offices(
        &container,
        &member,
        last.as_ref().map(|(_, member)| member.as_str()),
    )
    .await?;
    let changed: BTreeSet<OfficeKey> = office_keys
        .union(&member_keys)
        .filter(|key| !resumed.contains(*key))
        .cloned()
        .collect();
    if changed.is_empty() && resumed.is_empty() {
        outcome(&format!(
            "real-estate-broker-lookup-load unchanged office_snapshot={} member_snapshot={}",
            office.snapshot_id, member.snapshot_id
        ))?;
        return Ok(());
    }
    let first = last.is_none();
    let offices: Vec<OfficeRow> =
        current_rows(&container, &office, OFFICE_COLUMNS, &changed, first).await?;
    let members: Vec<MemberRow> =
        current_rows(&container, &member, MEMBER_COLUMNS, &changed, first).await?;

    let office_watermark = office_newest
        .or_else(|| last.as_ref().map(|(office, _)| office.clone()))
        .context("silver.real_estate_broker_office has no rows")?;
    let member_watermark = member_newest
        .or_else(|| last.as_ref().map(|(_, member)| member.clone()))
        .context("silver.real_estate_broker_member has no rows")?;
    // A batch before the last carries the watermarks the load started from: a run stopped after it
    // reads the same changes again and skips the offices it committed (`resumed`).
    let (start_office, start_member) = last
        .clone()
        .unwrap_or_else(|| (UNLOADED_WATERMARK.to_owned(), UNLOADED_WATERMARK.to_owned()));

    let keys: Vec<&OfficeKey> = changed.iter().collect();
    let mut batches: Vec<&[&OfficeKey]> = keys.chunks(OFFICES_PER_BATCH).collect();
    if batches.is_empty() {
        // Every changed office was committed by a stopped run: this run only completes the load.
        batches.push(&[]);
    }
    let (mut retired, mut offices_written, mut members_written, mut skipped) = (0, 0, 0, 0);
    let mut load_id = 0;
    for (index, batch) in batches.iter().enumerate() {
        let completes = index + 1 == batches.len();
        let in_batch: BTreeSet<&OfficeKey> = batch.iter().copied().collect();
        let key_of = |sigungu: &String, registration: &String| {
            in_batch.contains(&(sigungu.clone(), registration.clone()))
        };
        let batch_offices: Vec<&OfficeRow> = offices
            .iter()
            .filter(|row| key_of(&row.sigungu_code, &row.registration_no))
            .collect();
        let batch_members: Vec<&MemberRow> = members
            .iter()
            .filter(|row| key_of(&row.sigungu_code, &row.registration_no))
            .collect();
        let (office_lines, office_skipped) = office_copy_lines(&batch_offices);
        let (member_lines, member_skipped) = member_copy_lines(&batch_members);
        let load = Load {
            office: &office,
            member: &member,
            office_watermark: if completes {
                &office_watermark
            } else {
                &start_office
            },
            member_watermark: if completes {
                &member_watermark
            } else {
                &start_member
            },
            changed: batch,
            office_lines: &office_lines,
            member_lines: &member_lines,
            completes,
        };
        let (id, batch_retired) = write_load(&mut conn, &load).await?;
        load_id = id;
        retired += batch_retired;
        offices_written += office_lines.len();
        members_written += member_lines.len();
        skipped += office_skipped + member_skipped;
    }
    outcome(&format!(
        "real-estate-broker-lookup-load changed load={load_id} first={first} changed_offices={} \
         resumed_offices={} batches={} offices={offices_written} members={members_written} \
         retired={retired} skipped={skipped} office_snapshot={} member_snapshot={}",
        changed.len(),
        resumed.len(),
        batches.len(),
        office.snapshot_id,
        member.snapshot_id
    ))?;
    Ok(())
}

/// The offices whose current roster rows a load after `completed` wrote: the batches of a run that
/// stopped before its last batch (every office when no load has completed).
async fn loaded_since(
    conn: &mut PgConnection,
    completed: Option<i64>,
) -> anyhow::Result<BTreeSet<OfficeKey>> {
    let rows = sqlx::query(
        "SELECT DISTINCT sigungu_code, registration_no FROM (
             SELECT sigungu_code, registration_no, loaded_by_load_id
             FROM catalog.real_estate_broker_office WHERE retired_by_load_id IS NULL
             UNION ALL
             SELECT sigungu_code, registration_no, loaded_by_load_id
             FROM catalog.real_estate_broker_member WHERE retired_by_load_id IS NULL
         ) AS current WHERE loaded_by_load_id > $1",
    )
    .bind(completed.unwrap_or(0))
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect()
}

/// The COPY lines of the office rows, and how many had no comparable 등록번호.
fn office_copy_lines(offices: &[&OfficeRow]) -> (Vec<String>, usize) {
    let mut skipped = 0_usize;
    let lines = offices
        .iter()
        .filter_map(|row| {
            let Some(registration_key) = number_key(&row.registration_no) else {
                skipped += 1;
                return None;
            };
            Some(copy_line(&[
                Some(&row.sigungu_code),
                Some(&row.registration_no),
                Some(&registration_key),
                row.office_name.as_deref(),
                row.status_code.as_deref(),
                row.status_name.as_deref(),
                row.data_reference_date.as_deref(),
            ]))
        })
        .collect();
    (lines, skipped)
}

/// The COPY lines of the people, and how many had no name or no comparable 등록번호.
fn member_copy_lines(members: &[&MemberRow]) -> (Vec<String>, usize) {
    let mut skipped = 0_usize;
    let lines = members
        .iter()
        .filter_map(|row| {
            let (Some(registration_key), Some(name), Some(name_key)) = (
                number_key(&row.registration_no),
                row.member_name.as_deref(),
                row.member_name.as_deref().and_then(name_key),
            ) else {
                skipped += 1;
                return None;
            };
            let license_key = row.license_no.as_deref().and_then(number_key);
            Some(copy_line(&[
                Some(&row.sigungu_code),
                Some(&row.registration_no),
                Some(&registration_key),
                Some(name),
                Some(&name_key),
                row.member_kind_code.as_deref(),
                row.member_kind_name.as_deref(),
                row.license_no.as_deref(),
                license_key.as_deref(),
                row.position_code.as_deref(),
                row.position_name.as_deref(),
                row.data_reference_date.as_deref(),
            ]))
        })
        .collect();
    (lines, skipped)
}

/// One load, everything read and ready to write.
struct Load<'a> {
    office: &'a Pinned,
    member: &'a Pinned,
    office_watermark: &'a str,
    member_watermark: &'a str,
    changed: &'a [&'a OfficeKey],
    office_lines: &'a [String],
    member_lines: &'a [String],
    /// The load's last batch: its watermarks are the ones the next load reads from.
    completes: bool,
}

/// Writes one batch of a load in one transaction: retire the batch's offices' rows, insert their
/// current rows, record the batch. Returns the load id and the rows retired.
async fn write_load(conn: &mut PgConnection, load: &Load<'_>) -> anyhow::Result<(i64, u64)> {
    let mut tx = conn.begin().await?;
    let load_id: i64 = sqlx::query_scalar(
        "SELECT nextval(pg_get_serial_sequence('catalog.real_estate_broker_lookup_load', 'id'))",
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query("CREATE TEMP TABLE changed_broker_office (sigungu_code text, registration_no text) ON COMMIT DROP")
        .execute(&mut *tx)
        .await?;
    let key_lines: Vec<String> = load
        .changed
        .iter()
        .map(|(sigungu, registration)| {
            copy_line(&[Some(sigungu.as_str()), Some(registration.as_str())])
        })
        .collect();
    copy_rows(
        &mut tx,
        "COPY changed_broker_office (sigungu_code, registration_no) FROM STDIN WITH (FORMAT text)",
        &key_lines,
    )
    .await?;
    let mut retired = 0_u64;
    for table in [
        "catalog.real_estate_broker_office",
        "catalog.real_estate_broker_member",
    ] {
        retired += sqlx::query(&format!(
            "UPDATE {table} AS t SET retired_by_load_id = $1 FROM changed_broker_office c
             WHERE t.retired_by_load_id IS NULL
               AND t.sigungu_code = c.sigungu_code AND t.registration_no = c.registration_no"
        ))
        .bind(load_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    }
    // Staged, then inserted with this load's id: no lock beyond the rows written.
    for (table, columns, lines) in [
        ("real_estate_broker_office",
         "sigungu_code, registration_no, registration_key, office_name, status_code, status_name, data_reference_date",
         load.office_lines),
        ("real_estate_broker_member",
         "sigungu_code, registration_no, registration_key, member_name, name_key, member_kind_code, member_kind_name, license_no, license_key, position_code, position_name, data_reference_date",
         load.member_lines),
    ] {
        sqlx::query(&format!(
            "CREATE TEMP TABLE stage_{table} ON COMMIT DROP AS SELECT {columns} FROM catalog.{table} WITH NO DATA"
        ))
        .execute(&mut *tx)
        .await?;
        copy_rows(&mut tx, &format!("COPY stage_{table} ({columns}) FROM STDIN WITH (FORMAT text)"), lines).await?;
        sqlx::query(&format!(
            "INSERT INTO catalog.{table} ({columns}, loaded_by_load_id) SELECT {columns}, $1 FROM stage_{table}"
        ))
        .bind(load_id)
        .execute(&mut *tx)
        .await?;
    }
    let inserted = load.office_lines.len() + load.member_lines.len();
    sqlx::query(
        "INSERT INTO catalog.real_estate_broker_lookup_load
             (id, office_snapshot_id, member_snapshot_id, office_watermark, member_watermark,
              changed_offices, inserted_rows, retired_rows, completes_load)
         OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(load_id)
    .bind(load.office.snapshot_id)
    .bind(load.member.snapshot_id)
    .bind(load.office_watermark)
    .bind(load.member_watermark)
    .bind(i32::try_from(load.changed.len())?)
    .bind(i32::try_from(inserted)?)
    .bind(i32::try_from(retired)?)
    .bind(load.completes)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((load_id, retired))
}

/// The run's result line, which the job script reads (`real-estate-broker-lookup-load ...`).
fn outcome(line: &str) -> anyhow::Result<()> {
    writeln!(std::io::stdout().lock(), "{line}").context("failed to write the outcome line")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_timestamp_trino_wrote_becomes_a_watermark() {
        assert!(valid_watermark("2026-11-12 03:00:00.123456 UTC"));
        assert!(valid_watermark("2026-11-12 03:00:00.123456"));
        assert!(!valid_watermark("2026-11-12' OR TRUE --"));
        assert!(!valid_watermark("2026-11-12 03:00:00') --"));
        assert!(!valid_watermark("yesterday"));
    }

    #[test]
    fn the_current_rows_follow_the_contract_rule() {
        let latest = SILVER_REAL_ESTATE_BROKER_MEMBER.current_row_latest();
        let sql = latest.map(|latest| {
            latest.trino_current_rows_sql("r2.silver.t FOR VERSION AS OF 7", &["a"], "TRUE")
        });
        let sql = sql.unwrap_or_default();
        assert!(
            sql.contains("PARTITION BY sigungu_code, registration_no"),
            "{sql}"
        );
        assert!(
            sql.contains(
                "ORDER BY source_snapshot_id DESC NULLS LAST, ingested_at_utc DESC NULLS LAST"
            ),
            "{sql}"
        );
        assert!(sql.contains("row_state <> 'removed'"), "{sql}");
        assert_eq!(quote("a'b"), "'a''b'");
    }
}
