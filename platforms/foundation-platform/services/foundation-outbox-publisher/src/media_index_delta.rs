//! `media-index-delta`: the media keys the Silver photo indexes added since the media reconciler's
//! last run (scripts/ops/media_reconcile.py, root ADR-0387), read from the lakehouse itself.
//!
//! The indexes (`data-go-kr-onbid.contract.json` `media.reconcile.index_tables`) are append-only, so
//! a key once named stays named. The reconciler's state (`referenced.json`) keeps, per table, the
//! data files it has read; this command decodes only the files each table's current snapshot holds
//! beyond those (Iceberg's incremental append read, `scan_snapshot_rows_in_files`) and writes the
//! `(sha256, media_format)` pairs they name, with every file the snapshots hold, for the comparison
//! to keep once it succeeds. A state of another table set, or none, reads the tables whole once.
//!
//! It was a Trino query through `docker exec` (`ingested_at_utc` since a watermark less two hours);
//! Trino is being retired (root ADR-0559). The read is the catalog's read-only token and the
//! lakehouse reader key, as the photo ladder backlog reads the same tables (root ADR-0429). Rows are
//! dropped as they decode: what it holds is the key set, not the rows. Writes nothing to R2.
//!
//! An index table whose component has switched to the Lakekeeper catalog (root ADR-0624;
//! `config/lakekeeper-declaration.v1.json`) is not reachable from here: the catalog's endpoint is
//! private to the platform's VPC. Its load on AWS writes, before each commit of new or changed index
//! rows, the keys those rows name as one object under `control/media_index/<namespace>/<table>/`
//! ([`write_delta`], create-only, one per Bronze object); this command reads the objects it has not
//! read yet, as it reads the data files of the other tables (coordinator 2026-11-28, until the last
//! index switches and the reconciler moves to AWS whole). A delta written for a commit that then
//! failed names keys the store holds (the collector stores a file before its Bronze line), so it
//! never makes a key "missing"; its retry writes the same object.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Context as _;
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use lakehouse_domain::{
    LakehouseTableContract, SILVER_COURT_AUCTION_APPRAISAL_REPORT, SILVER_COURT_AUCTION_ITEM_PHOTO,
    SILVER_INDUSTRIAL_COMPLEX_NOTICE_ATTACHMENT, SILVER_INDUSTRIAL_LAND_SALE_NOTICE_ATTACHMENT,
    SILVER_ONBID_REAL_ESTATE_ITEM_PHOTO,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use crate::lakehouse_snapshot_scan::{scan_snapshot_rows_in_files, LakehouseObjectReader};
use crate::public_data_control_support::{required_env_value, write_json_file};
use foundation_outbox::object_storage::R2InventoryRequest;
use foundation_outbox::R2ObjectStorage;
use sha2::{Digest as _, Sha256};

const PREFIX: &str = "FOUNDATION_PLATFORM_MEDIA_INDEX_DELTA";
/// Where a load on AWS leaves the keys of the index rows it commits.
const DELTA_ROOT: &str = "control/media_index/";
const DELTA_SCHEMA: &str = "foundation-platform.media_index_delta.v1";
/// What the Lakekeeper catalog holds (generated from the pipeline graph, root ADR-0418).
const LAKEKEEPER_DECLARATION: &str = include_str!("../../../config/lakekeeper-declaration.v1.json");

/// The tables of the Lakekeeper catalog.
pub(crate) fn lakekeeper_tables() -> anyhow::Result<BTreeSet<String>> {
    let declaration: JsonValue = serde_json::from_str(LAKEKEEPER_DECLARATION)
        .context("config/lakekeeper-declaration.v1.json is not valid")?;
    serde_json::from_value(declaration["tables"].clone())
        .context("config/lakekeeper-declaration.v1.json names no tables")
}

/// `control/media_index/<namespace>/<table>/` of `table`.
pub(crate) fn delta_prefix(table: &str) -> anyhow::Result<String> {
    let (namespace, name) = table
        .split_once('.')
        .with_context(|| format!("{table} is not <namespace>.<table>"))?;
    Ok(format!("{DELTA_ROOT}{namespace}/{name}/"))
}

/// The one delta object of `table` for `bronze_object`: the same object always names the same key.
pub(crate) fn delta_key(table: &str, bronze_object: &str) -> anyhow::Result<String> {
    let digest = format!("{:x}", Sha256::digest(bronze_object.as_bytes()));
    Ok(format!("{}{}.json", delta_prefix(table)?, &digest[..32]))
}

/// Whether `table` is an index the reconciler reads (`media.reconcile.index_tables`).
pub(crate) fn is_index_table(table: &str) -> anyhow::Result<bool> {
    Ok(index_tables()?.iter().any(|indexed| indexed == table))
}

/// Writes the keys `keys` that `table`'s rows from `bronze_object` name, before they are committed;
/// an object already there (a retried commit) is kept.
///
/// # Errors
/// The key or the body cannot be made, or R2 refuses the write.
pub(crate) async fn write_delta(
    storage: &R2ObjectStorage,
    table: &str,
    bronze_object: &str,
    keys: &[(String, String)],
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(&json!({
        "schema_version": DELTA_SCHEMA,
        "table": table,
        "bronze_object": bronze_object,
        "keys": keys.iter().map(|(sha256, format)| [sha256, format]).collect::<Vec<_>>(),
    }))?;
    crate::bronze_epoch::put_create_only(
        storage,
        &delta_key(table, bronze_object)?,
        body,
        "application/json",
    )
    .await?;
    Ok(())
}

/// The `(sha256, media_format)` keys a delta object names.
fn delta_keys(table: &str, bytes: &[u8]) -> anyhow::Result<Vec<(String, String)>> {
    let delta: JsonValue = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        delta["schema_version"] == DELTA_SCHEMA && delta["table"] == table,
        "a delta of {table} that is not a {DELTA_SCHEMA} of it"
    );
    delta["keys"]
        .as_array()
        .context("a delta without keys")?
        .iter()
        .map(|pair| -> anyhow::Result<(String, String)> {
            let pair: Vec<String> = serde_json::from_value(pair.clone())?;
            match pair.as_slice() {
                [sha256, format] => Ok((sha256.clone(), format.clone())),
                _ => anyhow::bail!("a delta key that is not [sha256, media_format]"),
            }
        })
        .collect()
}

/// What this command reads of the reconciler's state; anything else in it is the comparison's.
#[derive(Debug, Default, Deserialize)]
struct State {
    index_table: Option<String>,
    #[serde(default)]
    read_files: BTreeMap<String, BTreeSet<String>>,
}

fn index_tables() -> anyhow::Result<Vec<String>> {
    let contract: JsonValue = serde_json::from_str(crate::data_go_kr_onbid_collect::CONTRACT_JSON)
        .context("data-go-kr-onbid.contract.json is not valid")?;
    serde_json::from_value(contract["media"]["reconcile"]["index_tables"].clone())
        .context("data-go-kr-onbid.contract.json media.reconcile.index_tables does not parse")
}

/// The reader of one index table: a table no contract decodes is refused, never guessed.
fn contract_of(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    [
        &SILVER_ONBID_REAL_ESTATE_ITEM_PHOTO,
        &SILVER_COURT_AUCTION_ITEM_PHOTO,
        &SILVER_INDUSTRIAL_COMPLEX_NOTICE_ATTACHMENT,
        &SILVER_COURT_AUCTION_APPRAISAL_REPORT,
        &SILVER_INDUSTRIAL_LAND_SALE_NOTICE_ATTACHMENT,
    ]
    .into_iter()
    .find(|contract| contract.table_name == table)
    .with_context(|| format!("media.reconcile.index_tables names {table}, which has no reader"))
}

fn load_state(path: &Path) -> anyhow::Result<State> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("{} is not a media reconciler state", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// The set's identity, as the comparison keys its state: the configured tables joined, or, while a
/// table has no snapshot yet, the ones that have (the next run then reads them whole again).
fn identity(configured: &[String], present: &[String]) -> String {
    if present == configured {
        configured.join("+")
    } else {
        present.join("+")
    }
}

fn media_key(row: &serde_json::Map<String, JsonValue>) -> Option<(String, String)> {
    Some((
        row.get("sha256")?.as_str()?.to_owned(),
        row.get("media_format")?.as_str()?.to_owned(),
    ))
}

/// Reads the delta and writes it.
///
/// # Errors
/// The contract does not parse, a table cannot be read or its scan does not account for every row,
/// a row names no key, or the output cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let state = load_state(&PathBuf::from(required_env_value(&format!(
        "{PREFIX}_STATE"
    ))?))?;
    let output = PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT"))?);
    let configured = index_tables()?;
    let switched = lakekeeper_tables()?;
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("the media index read uses the catalog's read-only token")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let lakehouse = LakehouseObjectReader::from_config(
        R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("the media index read reads R2 with the lakehouse reader key")?,
    );

    let objects =
        R2ObjectStorage::from_config(R2ObjectStorageConfig::lakehouse_reader_from_env().context(
            "the media index read lists the switched indexes' deltas with the reader key",
        )?);
    // A switched index: its deltas, listed (a table with none is not there yet).
    let mut deltas = BTreeMap::new();
    for table in configured.iter().filter(|table| switched.contains(*table)) {
        let listed = objects
            .inventory_audit(R2InventoryRequest::new(
                Some(&delta_prefix(table)?),
                Some(1000),
            )?)
            .await
            .with_context(|| format!("failed to list the deltas of {table}"))?;
        let keys: BTreeSet<String> = listed
            .objects()
            .iter()
            .map(|object| object.key.clone())
            .collect();
        if !keys.is_empty() {
            deltas.insert(table.clone(), keys);
        }
    }
    let mut snapshots = Vec::new();
    for table in configured.iter().filter(|table| !switched.contains(*table)) {
        let contract = contract_of(table)?;
        if let Some(snapshot) = catalog
            .load_current_snapshot_manifest_list(table)
            .await
            .with_context(|| format!("failed to resolve {table}"))?
        {
            snapshots.push((table.clone(), contract, snapshot));
        }
    }
    let present = configured
        .iter()
        .filter(|table| {
            deltas.contains_key(*table) || snapshots.iter().any(|(held, ..)| held == *table)
        })
        .cloned()
        .collect::<Vec<_>>();
    let identity = identity(&configured, &present);
    let empty = BTreeMap::new();
    let read_before = if state.index_table.as_deref() == Some(identity.as_str()) {
        &state.read_files
    } else {
        &empty
    };

    let keys = Mutex::new(BTreeSet::new());
    let unnamed = Mutex::new(0_u64);
    let mut read_files = BTreeMap::new();
    let mut tables = serde_json::Map::new();
    for (table, contract, snapshot) in &snapshots {
        let before = read_before.get(table);
        let (scanned, paths) = scan_snapshot_rows_in_files(
            contract,
            &lakehouse,
            snapshot,
            |row| {
                match media_key(row) {
                    Some(key) => {
                        if let Ok(mut keys) = keys.lock() {
                            keys.insert(key);
                        }
                    }
                    None => {
                        if let Ok(mut unnamed) = unnamed.lock() {
                            *unnamed += 1;
                        }
                    }
                }
                false
            },
            |path| before.is_none_or(|read| !read.contains(path)),
        )
        .await
        .with_context(|| format!("failed to read {table}"))?;
        scanned.ensure_complete()?;
        tables.insert(
            table.clone(),
            json!({
                "snapshot_id": snapshot.snapshot_id,
                "data_files": scanned.data_file_count,
                "read_data_files": scanned.data_file_count - scanned.skipped_data_file_count,
                "decoded_rows": scanned.decoded_row_count,
                "fetched_bytes": scanned.fetched_bytes,
            }),
        );
        read_files.insert(table.clone(), paths);
    }
    for (table, listed) in &deltas {
        let before = read_before.get(table);
        let unread: Vec<&String> = listed
            .iter()
            .filter(|key| before.is_none_or(|read| !read.contains(*key)))
            .collect();
        let mut named = 0_usize;
        for key in &unread {
            let found = delta_keys(table, &objects.get_object_bytes(key).await?)
                .with_context(|| format!("failed to read the delta {key}"))?;
            named += found.len();
            if let Ok(mut keys) = keys.lock() {
                keys.extend(found);
            }
        }
        tables.insert(
            table.clone(),
            json!({"deltas": listed.len(), "read_deltas": unread.len(), "named_keys": named}),
        );
        read_files.insert(table.clone(), listed.iter().cloned().collect());
    }
    let unnamed = unnamed
        .into_inner()
        .map_err(|_| anyhow::anyhow!("a scan of the index panicked"))?;
    anyhow::ensure!(
        unnamed == 0,
        "{unnamed} index rows name no (sha256, media_format) key"
    );
    let keys = keys
        .into_inner()
        .map_err(|_| anyhow::anyhow!("a scan of the index panicked"))?;
    write_json_file(
        &output,
        &json!({
            "index_table": identity,
            "keys": keys.iter().map(|(sha256, format)| [sha256, format]).collect::<Vec<_>>(),
            "read_files": read_files,
            "tables": tables,
        }),
    )?;
    println!(
        "media-index-delta index_table={identity} keys={} tables={}",
        keys.len(),
        JsonValue::Object(tables)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn every_index_the_contract_names_has_a_reader() {
        let tables = index_tables().unwrap();
        assert_eq!(tables.len(), 5);
        for table in &tables {
            assert_eq!(contract_of(table).unwrap().table_name, table);
            let columns = &contract_of(table).unwrap().columns;
            for needed in ["sha256", "media_format"] {
                assert!(columns.iter().any(|column| column.name == needed));
            }
        }
        assert!(contract_of("silver.not_an_index").is_err());
    }

    #[test]
    fn a_table_not_there_yet_changes_the_identity_so_the_next_run_reads_whole() {
        let configured = vec!["silver.a".to_owned(), "silver.b".to_owned()];
        assert_eq!(identity(&configured, &configured), "silver.a+silver.b");
        assert_eq!(identity(&configured, &configured[..1]), "silver.a");
    }

    /// W109e: a switched index is read from the deltas its AWS load writes; the writer and this reader
    /// name the same key for the same Bronze object, under the table's own prefix.
    #[test]
    fn a_switched_index_is_read_from_its_deltas() {
        let table = "silver.industrial_land_sale_notice_attachment";
        assert!(lakekeeper_tables().unwrap().contains(table));
        assert!(is_index_table(table).unwrap());
        let key = delta_key(table, "bronze/source=x__y/a--sha256-00.gz").unwrap();
        assert!(
            key.starts_with("control/media_index/silver/industrial_land_sale_notice_attachment/")
        );
        assert_eq!(
            key,
            delta_key(table, "bronze/source=x__y/a--sha256-00.gz").unwrap()
        );
        let body = serde_json::to_vec(&json!({"schema_version": DELTA_SCHEMA, "table": table,
            "bronze_object": "b", "keys": [["00", "pdf"]]}))
        .unwrap();
        assert_eq!(
            delta_keys(table, &body).unwrap(),
            [("00".to_owned(), "pdf".to_owned())]
        );
        assert!(delta_keys("silver.other", &body).is_err());
    }

    #[test]
    fn a_state_written_before_the_files_were_kept_reads_every_file() {
        let state: State = serde_json::from_str(
            r#"{"index_table": "silver.a", "watermark": "2026-11-15T00:00:00Z", "keys": []}"#,
        )
        .unwrap();
        assert!(state.read_files.is_empty());
    }
}
