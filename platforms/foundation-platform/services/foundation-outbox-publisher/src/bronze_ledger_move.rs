//! A source's Bronze ledger rows move with its collector (root ADR-0616 A1b, A4; coordinator 2026-11-28).
//!
//! The ledger (`catalog.source_catalog`, `ingestion_run`, `bronze_object`, `bronze_object_version`) is one
//! place per source: the host's Postgres while its collector runs there, the AWS ledger (RDS) once its lane
//! runs on AWS. The two are not connected (ADR-0616 A1b). A load that plans from the ledger
//! (`plan-silver-load-objects`) on AWS must see the objects the host collected, so the rows move:
//!
//! - `export-bronze-ledger-rows` (the host job `bronze_ledger_export`): for each named source with no export
//!   yet, while none of its writers is running, its rows as `to_jsonb` lines, one gzip JSON Lines object,
//!   create-only, at `control/evidence/ledger_move/<date>-<source>.jsonl.gz` (a header line, then one line
//!   per row; the evidence form of root ADR-0626 §6-2). A source the host never wrote gets a header alone.
//! - `import-bronze-ledger-rows` (the rollout's one-off on AWS, `collect_lane.py import-ledger`): one export
//!   into this ledger in one transaction. A source row is matched by its id (the slug: root ADR-0626 rule 1,
//!   `ON CONFLICT (slug)` would make a second row), runs and objects keep their ids and every timestamp, and
//!   what is already there stays: a second import inserts nothing. An object whose dedupe key this ledger
//!   already holds under another row is the older collection of the two, so it is kept as a replaced row
//!   (`bronze_object_version`, change `update`), the record a re-collection leaves (root ADR-0264); a host row
//!   newer than this ledger's is refused, never guessed.
//!
//! Neither moves bytes: the objects are in R2 already, under the keys the rows name. Rows that hang off an
//! object elsewhere (members, measurements, publications, normalization proposals) are refused on export:
//! a source that has them needs its own move.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};

use anyhow::{bail, ensure, Context};
use chrono::Utc;
use foundation_outbox::object_storage::{R2InventoryRequest, R2ObjectStorageConfig};
use foundation_outbox::R2ObjectStorage;
use serde_json::{json, Value as JsonValue};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::bronze_epoch::{put_create_only, refuse_while_active};
use crate::bronze_ledger_objects::ledger_from_env;
use crate::public_data_control_support::required_env_value;

/// Where the exports live (root ADR-0626 §6-2's evidence area).
pub(crate) const EXPORT_PREFIX: &str = "control/evidence/ledger_move/";
const EXPORT_SUFFIX: &str = ".jsonl.gz";
const SCHEMA: &str = "foundation-platform.bronze_ledger_rows.v1";
const SOURCES_ENV: &str = "FOUNDATION_PLATFORM_LEDGER_EXPORT_SOURCES";
const OBJECT_ENV: &str = "FOUNDATION_PLATFORM_LEDGER_IMPORT_OBJECT";
/// The ledger's tables an export holds, in the order an import writes them.
const TABLES: [&str; 4] = [
    "source_catalog",
    "ingestion_run",
    "bronze_object",
    "bronze_object_version",
];
/// Tables whose rows hang off a Bronze object and that no export carries.
const HANGING: [&str; 4] = [
    "bronze_object_member",
    "bronze_object_measurement",
    "publication_revision",
    "normalization_proposal",
];
const LIST_PAGE: i32 = 1000;

/// The source an export key names: `<prefix><date>-<source>.jsonl.gz` → `<source>`.
pub(crate) fn exported_source(key: &str) -> Option<&str> {
    let leaf = key
        .strip_prefix(EXPORT_PREFIX)?
        .strip_suffix(EXPORT_SUFFIX)?;
    let (date, source) = leaf.split_once('-')?;
    (date.len() == 10 && !source.is_empty()).then_some(source)
}

fn gzip(lines: &[JsonValue]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for line in lines {
        serde_json::to_writer(&mut encoder, line)?;
        encoder.write_all(b"\n")?;
    }
    Ok(encoder.finish()?)
}

/// The header and the rows of an export, its counts checked.
pub(crate) fn parse_export(bytes: &[u8]) -> anyhow::Result<(JsonValue, Vec<(String, JsonValue)>)> {
    let mut text = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut text)
        .context("the export is not gzip text")?;
    let mut lines = text.lines();
    let header: JsonValue = serde_json::from_str(lines.next().context("an empty export")?)?;
    ensure!(
        header["schema_version"] == SCHEMA,
        "the export is not a {SCHEMA}"
    );
    let mut rows = Vec::new();
    let mut counts = BTreeMap::<String, u64>::new();
    for line in lines {
        let entry: JsonValue = serde_json::from_str(line)?;
        let table = entry["table"]
            .as_str()
            .context("an export line names no table")?;
        ensure!(TABLES.contains(&table), "an export line of {table}");
        ensure!(entry["row"].is_object(), "a {table} line holds no row");
        *counts.entry(table.to_owned()).or_default() += 1;
        rows.push((table.to_owned(), entry["row"].clone()));
    }
    for table in TABLES {
        ensure!(
            header["rows"][table].as_u64().unwrap_or(0) == counts.get(table).copied().unwrap_or(0),
            "the header counts {} {table} rows and the export holds {}",
            header["rows"][table],
            counts.get(table).copied().unwrap_or(0)
        );
    }
    Ok((header, rows))
}

async fn rows_of(pool: &PgPool, sql: &str, source: Uuid) -> anyhow::Result<Vec<JsonValue>> {
    sqlx::query_scalar::<_, JsonValue>(sql)
        .bind(source)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to read the ledger ({sql})"))
}

/// The lines of one source's export, after the refusals.
async fn export_lines(pool: &PgPool, slug: &str) -> anyhow::Result<Vec<(String, JsonValue)>> {
    let Some(source) =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM catalog.source_catalog WHERE slug = $1")
            .bind(slug)
            .fetch_optional(pool)
            .await?
    else {
        return Ok(Vec::new());
    };
    for table in HANGING {
        let hanging: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM catalog.{table} h JOIN catalog.bronze_object bo ON bo.id = h.bronze_object_id \
             WHERE bo.source_catalog_id = $1"
        ))
        .bind(source)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count catalog.{table}"))?;
        ensure!(
            hanging == 0,
            "{slug}: {hanging} catalog.{table} rows hang off its objects; this move carries none, so the source needs its own"
        );
    }
    let records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.bronze_object WHERE source_catalog_id = $1 AND source_record_id IS NOT NULL",
    )
    .bind(source)
    .fetch_one(pool)
    .await?;
    ensure!(
        records == 0,
        "{slug}: {records} objects name a catalog.source_record; this move carries none"
    );
    // Every source a run of these objects belongs to, so the runs' own rows resolve on import.
    let sources = rows_of(
        pool,
        "SELECT to_jsonb(sc) FROM catalog.source_catalog sc WHERE sc.id = $1 OR sc.id IN \
         (SELECT ir.source_catalog_id FROM catalog.ingestion_run ir JOIN catalog.bronze_object bo \
          ON bo.ingestion_run_id = ir.id WHERE bo.source_catalog_id = $1) ORDER BY sc.created_at, sc.id",
        source,
    )
    .await?;
    let runs = rows_of(
        pool,
        "SELECT to_jsonb(ir) FROM catalog.ingestion_run ir WHERE ir.source_catalog_id = $1 OR ir.id IN \
         (SELECT ingestion_run_id FROM catalog.bronze_object WHERE source_catalog_id = $1) \
         ORDER BY ir.created_at, ir.id",
        source,
    )
    .await?;
    let objects = rows_of(
        pool,
        "SELECT to_jsonb(bo) FROM catalog.bronze_object bo WHERE bo.source_catalog_id = $1 ORDER BY bo.created_at, bo.id",
        source,
    )
    .await?;
    // A version row's own id is this ledger's sequence; the import gives it another.
    let versions = rows_of(
        pool,
        "SELECT to_jsonb(v) - 'id' FROM catalog.bronze_object_version v WHERE v.source_catalog_id = $1 ORDER BY v.id",
        source,
    )
    .await?;
    let mut lines = Vec::new();
    for (table, rows) in TABLES.iter().zip([sources, runs, objects, versions]) {
        lines.extend(rows.into_iter().map(|row| ((*table).to_owned(), row)));
    }
    Ok(lines)
}

/// Exports every named source that has no export yet.
///
/// # Errors
/// The configuration, the ledger or R2 cannot be read, a writer of a source is running, a source carries
/// rows this move does not, or an export cannot be written.
pub async fn export() -> anyhow::Result<()> {
    let slugs: BTreeSet<String> = required_env_value(SOURCES_ENV)?
        .split(',')
        .map(str::trim)
        .filter(|slug| !slug.is_empty())
        .map(str::to_owned)
        .collect();
    let pool = ledger_from_env().await?;
    let storage = R2ObjectStorage::from_config(
        R2ObjectStorageConfig::from_env()
            .context("the ledger export writes with the lakehouse writer key")?,
    );
    let listed = storage
        .inventory_audit(R2InventoryRequest::new(
            Some(EXPORT_PREFIX),
            Some(LIST_PAGE),
        )?)
        .await
        .with_context(|| format!("failed to list {EXPORT_PREFIX}"))?;
    let exported: BTreeSet<String> = listed
        .objects()
        .iter()
        .filter_map(|object| exported_source(&object.key).map(str::to_owned))
        .collect();
    let now = Utc::now();
    let mut written = Vec::new();
    for slug in slugs.iter().filter(|slug| !exported.contains(*slug)) {
        let ids: Vec<Uuid> =
            sqlx::query_scalar("SELECT id FROM catalog.source_catalog WHERE slug = $1")
                .bind(slug)
                .fetch_all(&pool)
                .await?;
        // Its last rows must be in: no writer of it running or ending a moment ago.
        refuse_while_active(&pool, &ids).await?;
        let lines = export_lines(&pool, slug).await?;
        let mut rows = BTreeMap::new();
        for (table, _) in &lines {
            *rows.entry(table.as_str()).or_insert(0_u64) += 1;
        }
        let header = json!({
            "schema_version": SCHEMA,
            "decision": "root ADR-0616 A1b, A4 (coordinator 2026-11-28)",
            "source": slug,
            "created_at_utc": now.to_rfc3339(),
            "rows": TABLES.iter().map(|table| (*table, rows.get(table).copied().unwrap_or(0))).collect::<BTreeMap<_, _>>(),
        });
        let mut body = vec![header.clone()];
        body.extend(
            lines
                .into_iter()
                .map(|(table, row)| json!({"table": table, "row": row})),
        );
        let key = format!(
            "{EXPORT_PREFIX}{}-{slug}{EXPORT_SUFFIX}",
            now.format("%Y-%m-%d")
        );
        if put_create_only(&storage, &key, gzip(&body)?, "application/gzip").await? {
            println!(
                "bronze-ledger-export {}",
                json!({"key": key, "rows": header["rows"]})
            );
            written.push(key);
        }
    }
    pool.close().await;
    println!(
        "foundation-job-outcome {}",
        if written.is_empty() {
            "unchanged"
        } else {
            "changed"
        }
    );
    Ok(())
}

/// A row with its `source_catalog_id` replaced by this ledger's id of the same source.
fn mapped(row: &JsonValue, sources: &BTreeMap<String, Uuid>) -> anyhow::Result<JsonValue> {
    let mut row = row.clone();
    let host = row["source_catalog_id"]
        .as_str()
        .context("a row without source_catalog_id")?
        .to_owned();
    let here = sources
        .get(&host)
        .with_context(|| format!("source {host} is in no source line of the export"))?;
    row["source_catalog_id"] = json!(here.to_string());
    Ok(row)
}

#[derive(Debug, Default)]
struct Imported {
    sources_matched: u64,
    sources_inserted: u64,
    runs_inserted: u64,
    objects_inserted: u64,
    objects_already: u64,
    objects_kept_as_replaced: u64,
    versions_inserted: u64,
}

async fn insert_row(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    row: &JsonValue,
    conflict: &str,
) -> anyhow::Result<bool> {
    let done = sqlx::query(&format!(
        "INSERT INTO catalog.{table} SELECT * FROM jsonb_populate_record(NULL::catalog.{table}, $1) {conflict}"
    ))
    .bind(row)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("failed to insert a catalog.{table} row"))?;
    Ok(done.rows_affected() == 1)
}

/// An object that did not insert: already here (same id), or the older collection of a dedupe key this
/// ledger holds under another row (kept as its replaced row).
async fn place_object(
    tx: &mut Transaction<'_, Postgres>,
    row: &JsonValue,
    imported: &mut Imported,
) -> anyhow::Result<()> {
    let id: Uuid = row["id"]
        .as_str()
        .context("an object without id")?
        .parse()?;
    let present: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM catalog.bronze_object WHERE id = $1)")
            .bind(id)
            .fetch_one(&mut **tx)
            .await?;
    if present {
        imported.objects_already += 1;
        return Ok(());
    }
    let holder: Option<(Uuid, chrono::DateTime<Utc>, chrono::DateTime<Utc>)> = sqlx::query_as(
        "SELECT bo.id, bo.collected_at, bo.created_at FROM catalog.bronze_object bo \
         WHERE bo.source_catalog_id = ($1->>'source_catalog_id')::uuid \
           AND (bo.dedupe_key = $1->>'dedupe_key' OR bo.object_key = $1->>'object_key')",
    )
    .bind(row)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((holder, collected_here, replaced_at)) = holder else {
        bail!("object {id} neither inserted nor conflicts with a row here");
    };
    let collected_there: chrono::DateTime<Utc> = row["collected_at"]
        .as_str()
        .context("an object without collected_at")?
        .parse()?;
    ensure!(
        collected_there <= collected_here,
        "object {id} of the export is newer ({collected_there}) than row {holder} here ({collected_here}); \
         this move keeps the newest collection current and refuses to guess"
    );
    let inserted = sqlx::query(
        "INSERT INTO catalog.bronze_object_version (bronze_object_id, source_catalog_id, dedupe_key, change, superseded_at, prior_row) \
         SELECT $2, ($1->>'source_catalog_id')::uuid, $1->>'dedupe_key', 'update', $3, $1 \
         WHERE NOT EXISTS (SELECT 1 FROM catalog.bronze_object_version v WHERE v.bronze_object_id = $2 \
                           AND v.prior_row->>'id' = $1->>'id')",
    )
    .bind(row)
    .bind(holder)
    .bind(replaced_at)
    .execute(&mut **tx)
    .await?;
    imported.objects_kept_as_replaced += inserted.rows_affected();
    Ok(())
}

/// Imports one export into this ledger.
///
/// # Errors
/// The export cannot be read or is not well formed, or a row cannot be placed; nothing is kept then.
pub async fn import() -> anyhow::Result<()> {
    let key = required_env_value(OBJECT_ENV)?;
    ensure!(
        exported_source(&key).is_some(),
        "{key} is not a ledger export"
    );
    let storage = R2ObjectStorage::from_config(
        R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("the ledger import reads the export with the lakehouse reader key")?,
    );
    let (header, rows) = parse_export(&storage.get_object_bytes(&key).await?)?;
    let pool = ledger_from_env().await?;
    let mut tx = pool.begin().await?;
    let mut imported = Imported::default();
    let mut sources = BTreeMap::<String, Uuid>::new();
    for (table, row) in &rows {
        match table.as_str() {
            "source_catalog" => {
                let host = row["id"]
                    .as_str()
                    .context("a source without id")?
                    .to_owned();
                let here: Option<Uuid> = sqlx::query_scalar(
                    "SELECT id FROM catalog.source_catalog WHERE slug = $1->>'slug'",
                )
                .bind(row)
                .fetch_optional(&mut *tx)
                .await?;
                let id = if let Some(id) = here {
                    imported.sources_matched += 1;
                    id
                } else {
                    insert_row(&mut tx, "source_catalog", row, "").await?;
                    imported.sources_inserted += 1;
                    host.parse()?
                };
                sources.insert(host, id);
            }
            "ingestion_run" => {
                if insert_row(
                    &mut tx,
                    "ingestion_run",
                    &mapped(row, &sources)?,
                    "ON CONFLICT (id) DO NOTHING",
                )
                .await?
                {
                    imported.runs_inserted += 1;
                }
            }
            "bronze_object" => {
                let row = mapped(row, &sources)?;
                if insert_row(&mut tx, "bronze_object", &row, "ON CONFLICT DO NOTHING").await? {
                    imported.objects_inserted += 1;
                } else {
                    place_object(&mut tx, &row, &mut imported).await?;
                }
            }
            "bronze_object_version" => {
                let row = mapped(row, &sources)?;
                let inserted = sqlx::query(
                    "INSERT INTO catalog.bronze_object_version (bronze_object_id, source_catalog_id, dedupe_key, change, superseded_at, prior_row) \
                     SELECT ($1->>'bronze_object_id')::uuid, ($1->>'source_catalog_id')::uuid, $1->>'dedupe_key', $1->>'change', \
                            ($1->>'superseded_at')::timestamptz, $1->'prior_row' \
                     WHERE NOT EXISTS (SELECT 1 FROM catalog.bronze_object_version v \
                                       WHERE v.bronze_object_id = ($1->>'bronze_object_id')::uuid AND v.change = $1->>'change' \
                                         AND v.superseded_at = ($1->>'superseded_at')::timestamptz)",
                )
                .bind(&row)
                .execute(&mut *tx)
                .await?;
                imported.versions_inserted += inserted.rows_affected();
            }
            other => bail!("an export line of {other}"),
        }
    }
    tx.commit().await?;
    pool.close().await;
    println!(
        "bronze-ledger-import {}",
        json!({
            "key": key,
            "source": header["source"],
            "rows": header["rows"],
            "sources_matched": imported.sources_matched,
            "sources_inserted": imported.sources_inserted,
            "runs_inserted": imported.runs_inserted,
            "objects_inserted": imported.objects_inserted,
            "objects_already": imported.objects_already,
            "objects_kept_as_replaced": imported.objects_kept_as_replaced,
            "versions_inserted": imported.versions_inserted,
        })
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{exported_source, gzip, parse_export, SCHEMA};
    use serde_json::json;

    #[test]
    fn an_export_key_names_its_source() {
        assert_eq!(
            exported_source(
                "control/evidence/ledger_move/2099-01-01-kicoxorkr__notice_detail.jsonl.gz"
            ),
            Some("kicoxorkr__notice_detail")
        );
        assert_eq!(
            exported_source("control/evidence/r2_move/2099-01-01-x.jsonl.gz"),
            None
        );
        assert_eq!(
            exported_source("control/evidence/ledger_move/x.jsonl.gz"),
            None
        );
    }

    #[test]
    fn an_export_is_read_back_and_its_counts_are_held() -> anyhow::Result<()> {
        let header = json!({"schema_version": SCHEMA, "source": "s", "rows": {
            "source_catalog": 1, "ingestion_run": 1, "bronze_object": 1, "bronze_object_version": 0}});
        let lines = [
            header.clone(),
            json!({"table": "source_catalog", "row": {"id": "a", "slug": "s"}}),
            json!({"table": "ingestion_run", "row": {"id": "r", "source_catalog_id": "a"}}),
            json!({"table": "bronze_object", "row": {"id": "o", "source_catalog_id": "a"}}),
        ];
        let (read, rows) = parse_export(&gzip(&lines)?)?;
        assert_eq!(read, header);
        assert_eq!(rows.len(), 3);
        // A header that counts what the lines do not hold is refused: a cut export never imports.
        let short = [header, lines[1].clone()];
        assert!(parse_export(&gzip(&short)?).is_err());
        Ok(())
    }
}
