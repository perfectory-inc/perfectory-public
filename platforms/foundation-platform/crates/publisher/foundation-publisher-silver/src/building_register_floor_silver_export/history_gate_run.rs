//! `check-building-register-floor-history`: the FLOOR history gate (root ADR-0338).
//!
//! It runs before an export. It was the Spark job `building_register_floor_history.py`; its decisions are
//! [`super::history_gate`], its reads are the catalog's snapshot register and, for the rows' lineage,
//! the publisher's `DuckDB` reading the table at a snapshot.
//!
//! The outcome file is the one the Spark job wrote (`foundation-platform.floor-history-check.v1`,
//! read by `remote_lakehouse_job::floor_history::retained_outcome`), and so are the exit codes: 0 when
//! the source is already retained, [`EXPORT_REQUIRED`] when the export must run.
//!
//! The witness tag is created through the catalog's REST commit (`IcebergRestCatalog::create_tag`,
//! `set-snapshot-ref` asserted absent): iceberg-rust 0.11 offers no public action that sets a
//! reference, its `TransactionAction` being crate-private. A tag that already names the witness is
//! left as it is (`create_tag` reads the refs first), so in steady state the gate only reads.

use std::{io::Read as _, path::Path};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_infrastructure::{IcebergRestCatalog, IcebergSnapshotRegistry};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{
    committed_inputs::{HistoricalBinding, SemanticInputs},
    history_gate::{identity_under_derivation, registered_append, verify_metadata},
    history_witness::HistoryWitness,
};
use crate::duckdb_parquet_stage::{read_catalog_config, DuckDbParquetStage, LakehouseRead};

/// The outcome's schema (unchanged from the Spark job).
pub const OUTCOME_SCHEMA: &str = "foundation-platform.floor-history-check.v1";
/// The exit code that tells the pipeline to run the export.
pub const EXPORT_REQUIRED: i32 = 10;
const TABLE: &str = "silver.building_register_floors";
/// The reader pin of the witness snapshot (`lakehouse-maintenance.contract.json` `reader_pins`).
pub const WITNESS_TAG: &str = "foundation-floor-history-witness";
const MAX_RECEIPT_BYTES: u64 = 65_536;

/// The export's source inspection receipt (`source_inspection::Receipt`), as the gate reads it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema_version: u8,
    source_snapshot_id: String,
    bronze_object_key: String,
    valid_from_utc: DateTime<Utc>,
    ingested_at_utc: DateTime<Utc>,
    inputs: SemanticInputs,
    historical_binding: Option<HistoricalBinding>,
}

/// One group of rows by their lineage columns.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
struct Lineage {
    source_snapshot_id: String,
    bronze_object_key: String,
    valid_from_us: i64,
    ingested_at_us: i64,
}

const fn micros(at: DateTime<Utc>) -> i64 {
    at.timestamp_micros()
}

/// The binding the receipt states, checked against the witness: `None` for a new source, which
/// must be later than the witness and carry a content identity.
fn bound<'a>(
    receipt: &'a Receipt,
    witness: &HistoricalBinding,
) -> anyhow::Result<Option<&'a HistoricalBinding>> {
    ensure!(
        receipt.schema_version == 2,
        "unsupported FLOOR source inspection receipt"
    );
    ensure!(
        receipt.bronze_object_key.starts_with("bronze/"),
        "missing committed Bronze object key"
    );
    let Some(binding) = receipt.historical_binding.as_ref() else {
        let (floor, title) = receipt.inputs.provider_months();
        let (witnessed_floor, witnessed_title) = witness.inputs().provider_months();
        ensure!(
            floor > witnessed_floor && title > witnessed_title,
            "historical provider month cannot fall through to a new load"
        );
        let digest = receipt
            .source_snapshot_id
            .strip_prefix("building-register-floor-content-v1-")
            .unwrap_or_default();
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "unexpected new FLOOR source identity"
        );
        return Ok(None);
    };
    binding.validate()?;
    ensure!(
        binding == witness
            && &receipt.inputs == witness.inputs()
            && receipt.source_snapshot_id == witness.source_snapshot_id()
            && receipt.valid_from_utc == witness.valid_from_utc(),
        "source inspection does not match the fixed historical witness"
    );
    Ok(Some(binding))
}

/// The lineage groups of the table at `snapshot` (of one source when given), at most three, with
/// their row counts.
fn lineage_at(
    stage: &DuckDbParquetStage,
    snapshot: i64,
    source: Option<&str>,
) -> anyhow::Result<Vec<(Lineage, i64)>> {
    let filter = match source {
        None => String::new(),
        Some(source) => {
            ensure!(!source.contains('\''), "a source identity holds a quote");
            format!("WHERE source_snapshot_id = '{source}'")
        }
    };
    let mut statement = stage.connection().prepare(&format!(
        "SELECT source_snapshot_id, bronze_object_key, epoch_us(valid_from_utc), epoch_us(ingested_at_utc), \
         count(*) FROM {pinned} {filter} \
         GROUP BY ALL ORDER BY ALL LIMIT 3",
        pinned = crate::duckdb_parquet_stage::table_at(TABLE, snapshot)
    ))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                Lineage {
                    source_snapshot_id: row.get(0)?,
                    bronze_object_key: row.get(1)?,
                    valid_from_us: row.get(2)?,
                    ingested_at_us: row.get(3)?,
                },
                row.get::<_, i64>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("DuckDB could not read {TABLE} at snapshot {snapshot}"))?;
    Ok(rows)
}

/// The lineage groups the append `snapshot` added of `source`: its groups less its parent's. Exact
/// on an append-only descent (the gate has verified one): an append only adds rows.
fn added_by(
    stage: &DuckDbParquetStage,
    registry: &IcebergSnapshotRegistry,
    snapshot: i64,
    source: &str,
) -> anyhow::Result<Vec<(Lineage, i64)>> {
    let parent = registry
        .snapshots
        .iter()
        .find(|entry| entry.snapshot_id == snapshot)
        .and_then(|entry| entry.parent_snapshot_id);
    let mut before = std::collections::BTreeMap::new();
    if let Some(parent) = parent {
        for (lineage, count) in lineage_at(stage, parent, Some(source))? {
            before.insert(lineage, count);
        }
    }
    Ok(lineage_at(stage, snapshot, Some(source))?
        .into_iter()
        .filter_map(|(lineage, count)| {
            let added = count - before.get(&lineage).copied().unwrap_or(0);
            (added > 0).then_some((lineage, added))
        })
        .collect())
}

/// The witness's rows are the one lineage it states, with its row count.
fn authenticate_witness(
    stage: &DuckDbParquetStage,
    witness: &HistoricalBinding,
) -> anyhow::Result<()> {
    let groups = lineage_at(stage, witness.snapshot_id().parse()?, None)?;
    let expected = Lineage {
        source_snapshot_id: witness.source_snapshot_id().to_owned(),
        bronze_object_key: witness.bronze_object_key().to_owned(),
        valid_from_us: micros(witness.valid_from_utc()),
        ingested_at_us: micros(witness.ingested_at_utc()),
    };
    ensure!(
        groups.len() == 1
            && groups[0].0 == expected
            && u64::try_from(groups[0].1)? == witness.row_count(),
        "historical rows do not match retained source, times and count"
    );
    Ok(())
}

/// The registry again, unchanged since it was read: the proof is of one observed head.
async fn same_head(
    catalog: &IcebergRestCatalog,
    registry: &IcebergSnapshotRegistry,
) -> anyhow::Result<()> {
    let again = catalog
        .load_snapshot_registry(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?;
    ensure!(
        again.table_uuid == registry.table_uuid && again.head == registry.head,
        "Iceberg head changed during FLOOR history authentication; retry"
    );
    Ok(())
}

/// Pins the authenticated witness snapshot (root ADR-0338): a tag on another snapshot is refused.
async fn pin_witness(
    catalog: &IcebergRestCatalog,
    witness: &HistoricalBinding,
) -> anyhow::Result<()> {
    let snapshot: i64 = witness.snapshot_id().parse()?;
    catalog.create_tag(TABLE, WITNESS_TAG, snapshot).await?;
    let refs = catalog
        .load_snapshot_refs(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?;
    let tag = refs.refs.get(WITNESS_TAG);
    ensure!(
        tag.is_some_and(|tag| tag.kind == "tag" && tag.snapshot_id == snapshot),
        "{TABLE} tag {WITNESS_TAG} does not name the witness snapshot"
    );
    Ok(())
}

fn read_receipt(path: &Path) -> anyhow::Result<(Receipt, Value)> {
    let file =
        std::fs::File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    ensure!(
        file.metadata()?.is_file(),
        "source evidence must be a regular file"
    );
    let mut bytes = Vec::new();
    file.take(MAX_RECEIPT_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        u64::try_from(bytes.len())? <= MAX_RECEIPT_BYTES,
        "source evidence exceeds its byte limit"
    );
    let raw: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        raw.get("historical_binding").is_some(),
        "unsupported FLOOR source inspection receipt"
    );
    Ok((serde_json::from_value(raw.clone())?, raw))
}

/// What the gate judges: the receipt, the witness and the identity the receipt is checked under.
struct Gate<'a> {
    receipt: &'a Receipt,
    witness: &'a HistoricalBinding,
    binding: Option<&'a HistoricalBinding>,
    identity: &'a str,
    derivation: Option<&'a str>,
}

impl Gate<'_> {
    /// The retained append of the receipt's rows, when the register records one: its fields for
    /// the outcome. A new source or a derivation is judged by its recorded append; the historical
    /// base by the witness alone.
    fn retained(
        &self,
        stage: &DuckDbParquetStage,
        registry: &IcebergSnapshotRegistry,
        head: i64,
    ) -> anyhow::Result<Option<Value>> {
        if self.binding.is_some() {
            authenticate_witness(stage, self.witness)?;
            if self.derivation.is_none() {
                return Ok(None);
            }
        }
        let Some(record) = registered_append(self.identity, registry)? else {
            self.refuse_unrecorded_rows(stage, head)?;
            return Ok(None);
        };
        let groups = added_by(
            stage,
            registry,
            record.snapshot_id,
            &self.receipt.source_snapshot_id,
        )?;
        let added: i64 = record
            .summary
            .get("added-records")
            .context("the append records no added rows")?
            .parse()?;
        let [(lineage, count)] = groups.as_slice() else {
            bail!("recorded FLOOR append has missing or mixed row lineage");
        };
        ensure!(
            lineage.source_snapshot_id == self.receipt.source_snapshot_id
                && lineage.bronze_object_key == self.receipt.bronze_object_key
                && lineage.valid_from_us == micros(self.receipt.valid_from_utc)
                && *count == added,
            "recorded FLOOR rows differ from authenticated source and count"
        );
        let ingested = DateTime::<Utc>::from_timestamp_micros(lineage.ingested_at_us)
            .context("an unreadable retained time")?;
        Ok(Some(json!({
            "table_uuid": registry.table_uuid.to_string(),
            "snapshot_id": record.snapshot_id.to_string(),
            "observed_head_snapshot_id": head.to_string(),
            "retained_row_count": count,
            "retained_ingested_at_utc": ingested.to_rfc3339(),
            "retention_kind": "registered_append",
            "derivation": self.derivation,
        })))
    }

    /// Snapshot expiry may remove the register without removing the rows: a missing record never
    /// means unprocessed.
    fn refuse_unrecorded_rows(&self, stage: &DuckDbParquetStage, head: i64) -> anyhow::Result<()> {
        let current = lineage_at(stage, head, Some(&self.receipt.source_snapshot_id))?;
        if self.derivation.is_none() {
            ensure!(
                current.is_empty(),
                "FLOOR rows exist without their recorded append; recovery required"
            );
        } else {
            ensure!(
                current.iter().all(|(lineage, _)| {
                    lineage.ingested_at_us < micros(self.receipt.ingested_at_utc)
                }),
                "changed derivation must be later than retained rows"
            );
        }
        Ok(())
    }

    /// The outcome the Spark job wrote, with the retained append's fields over it.
    fn outcome(
        &self,
        raw: &Value,
        head: i64,
        retained: Option<Value>,
    ) -> anyhow::Result<(&'static str, Value)> {
        let binding = self.binding;
        let action = if retained.is_some() || (binding.is_some() && self.derivation.is_none()) {
            "already_retained"
        } else {
            if let Some(binding) = binding {
                ensure!(
                    self.receipt.ingested_at_utc > binding.ingested_at_utc(),
                    "changed derivation must be later than retained historical rows"
                );
            }
            "export_required"
        };
        let mut outcome = json!({
            "schema_version": OUTCOME_SCHEMA,
            "action": action,
            "source_snapshot_id": self.receipt.source_snapshot_id,
            "inputs": raw["inputs"],
            "derivation": self.derivation,
            "table_uuid": binding.map(|binding| binding.table_uuid().to_string()),
            "snapshot_id": binding.map(|binding| binding.snapshot_id().to_owned()),
            "observed_head_snapshot_id": binding.map(|_| head.to_string()),
            "retained_row_count": binding.map_or(0, HistoricalBinding::row_count),
            "persisted_row_count": 0,
        });
        if let (Some(Value::Object(fields)), Some(object)) = (retained, outcome.as_object_mut()) {
            object.extend(fields);
        }
        Ok((action, outcome))
    }
}

fn write_outcome(path: &Path, outcome: &Value) -> anyhow::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("cannot create {}", path.display()))?;
    std::io::Write::write_all(&mut file, format!("{outcome}\n").as_bytes())?;
    Ok(())
}

/// Runs the gate on the receipt at `receipt_path` against `witness_file`, reading the table with
/// `DuckDB` in `work`, and writes the outcome to `summary`; returns the exit code.
///
/// Every step reads: the catalog's register and refs, and the table's rows through `DuckDB` with
/// the reader pair (`LakehouseRead::from_env`). The one write is the witness tag when it
/// is missing; once it names the witness, the gate writes nothing but its outcome file.
///
/// # Errors
/// The witness, the receipt or the table's history does not authenticate, or a read fails.
pub async fn run(
    witness_file: &HistoryWitness,
    receipt_path: &Path,
    summary: &Path,
    derivation: Option<&str>,
    work: &Path,
) -> anyhow::Result<i32> {
    let witness = witness_file.binding();
    ensure!(
        witness.table_name() == TABLE,
        "the witness names another table"
    );
    let (receipt, raw) = read_receipt(receipt_path)?;
    let identity = identity_under_derivation(&receipt.source_snapshot_id, derivation)?;
    let gate = Gate {
        receipt: &receipt,
        witness,
        binding: bound(&receipt, witness)?,
        identity: &identity,
        derivation,
    };

    let catalog = IcebergRestCatalog::new(read_catalog_config()?)?;
    let registry = catalog
        .load_snapshot_registry(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?;
    // A missing or replaced table, or a broken historical chain, is not a new empty dataset.
    let head = verify_metadata(&witness.gate_witness()?, &registry)?;
    let retained = {
        let stage = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
        gate.retained(&stage, &registry, head)?
    };
    same_head(&catalog, &registry).await?;
    pin_witness(&catalog, witness).await?;
    let (action, outcome) = gate.outcome(&raw, head, retained)?;
    write_outcome(summary, &outcome)?;
    Ok(if action == "already_retained" {
        0
    } else {
        EXPORT_REQUIRED
    })
}

#[cfg(test)]
mod tests {
    use super::{identity_under_derivation, micros};

    #[test]
    fn instants_are_compared_to_the_microsecond_iceberg_keeps() -> anyhow::Result<()> {
        let at = chrono::DateTime::parse_from_rfc3339("2099-01-02T03:04:05.123456Z")?
            .with_timezone(&chrono::Utc);
        assert_eq!(micros(at), 4_071_006_245_123_456);
        assert_eq!(identity_under_derivation("x", None)?, "x");
        Ok(())
    }
}
