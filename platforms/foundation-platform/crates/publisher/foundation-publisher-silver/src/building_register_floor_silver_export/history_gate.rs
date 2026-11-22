//! The FLOOR history gate's decisions over table metadata (root ADR-0338).
//!
//! It was `building_register_floor_history.py` (Spark). The gate authenticates the retained FLOOR
//! history before an export: the fixed witness snapshot is the table's own commit of the witnessed
//! source, `main` descends from it by appends only, and a source the table already records is its
//! one registered append. These are the rules, over the catalog's snapshot register; reading the
//! rows' lineage and pinning the witness are the caller's (`history_gate_run`).

use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context};
use lakehouse_infrastructure::{IcebergRegisteredSnapshot, IcebergSnapshotRegistry};
use sha2::{Digest, Sha256};

/// The summary key an `append_batch_once` commit records its identities under.
pub const INGEST_BATCH_OBJECTS_KEY: &str = "foundation.ingest-batch-objects";
/// The summary key of the digest of those identities.
pub const INGEST_BATCH_TOKEN_KEY: &str = "foundation.ingest-batch-token";
/// Separates identities in [`INGEST_BATCH_OBJECTS_KEY`].
const OBJECT_NAME_SEPARATOR: char = ',';
/// Separates an identity from the derivation it was appended under.
const DERIVATION_SEPARATOR: char = '@';

/// What the gate knows of the witness it authenticates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Witness {
    /// The table's UUID when the witness was taken.
    pub table_uuid: uuid::Uuid,
    /// The witnessed snapshot.
    pub snapshot_id: i64,
    /// Its operation (`overwrite`).
    pub operation: String,
    /// The source identity it committed.
    pub source_snapshot_id: String,
    /// Its `total-records`.
    pub row_count: u64,
}

/// The digest an append records of its sorted identities (`lakehouse_ingest.ingest_batch_token`).
#[must_use]
pub fn ingest_batch_token(identities: &[String]) -> String {
    let mut sorted = identities.to_vec();
    sorted.sort();
    format!("{:x}", Sha256::digest(sorted.join("\n").as_bytes()))
}

/// `identity` as it is recorded under `derivation` (`identities_under_derivation`).
///
/// # Errors
/// A derivation that is blank or holds a separator.
pub fn identity_under_derivation(
    identity: &str,
    derivation: Option<&str>,
) -> anyhow::Result<String> {
    let Some(label) = derivation else {
        return Ok(identity.to_owned());
    };
    let label = label.trim();
    ensure!(
        !label.is_empty() && !label.contains(DERIVATION_SEPARATOR) && !label.contains(OBJECT_NAME_SEPARATOR),
        "a derivation label must be non-empty and hold neither {DERIVATION_SEPARATOR:?} nor {OBJECT_NAME_SEPARATOR:?}"
    );
    Ok(format!("{identity}{DERIVATION_SEPARATOR}{label}"))
}

fn recorded(snapshot: &IcebergRegisteredSnapshot) -> Vec<&str> {
    snapshot
        .summary
        .get(INGEST_BATCH_OBJECTS_KEY)
        .map(|objects| objects.split(OBJECT_NAME_SEPARATOR).collect())
        .unwrap_or_default()
}

/// The register by snapshot id, refusing duplicate ids.
///
/// # Errors
/// Two snapshots share an id.
pub fn by_id(
    registry: &IcebergSnapshotRegistry,
) -> anyhow::Result<BTreeMap<i64, &IcebergRegisteredSnapshot>> {
    let by_id = registry
        .snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id, snapshot))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        by_id.len() == registry.snapshots.len(),
        "duplicate Iceberg snapshot IDs"
    );
    Ok(by_id)
}

/// Every snapshot from `head` back to `pin` is an append (snapshot expiry keeps them while the
/// witness tag stands).
///
/// # Errors
/// The walk leaves the register, meets a non-append or cycles before reaching `pin`.
pub fn verify_append_descendant(
    head: i64,
    pin: i64,
    by_id: &BTreeMap<i64, &IcebergRegisteredSnapshot>,
) -> anyhow::Result<()> {
    let mut visited = std::collections::BTreeSet::new();
    let mut at = head;
    while at != pin {
        let snapshot = by_id
            .get(&at)
            .filter(|snapshot| snapshot.operation() == Some("append"));
        let Some(snapshot) = snapshot.filter(|_| visited.insert(at)) else {
            bail!("current table is not an append-only descendant of retained history");
        };
        at = snapshot
            .parent_snapshot_id
            .context("current table is not an append-only descendant of retained history")?;
    }
    Ok(())
}

/// The table is the witnessed one, its witness snapshot is the commit of the witnessed source
/// (and the only commit that records it), and `main` descends from it by appends. Returns the head.
///
/// # Errors
/// Any of those does not hold.
pub fn verify_metadata(
    witness: &Witness,
    registry: &IcebergSnapshotRegistry,
) -> anyhow::Result<i64> {
    let by_id = by_id(registry)?;
    let head = registry
        .head
        .context("historical table has no current snapshot")?;
    ensure!(
        registry.table_uuid == witness.table_uuid && by_id.contains_key(&witness.snapshot_id),
        "historical Iceberg table or snapshot does not match"
    );
    let retained = by_id[&witness.snapshot_id];
    let source = &witness.source_snapshot_id;
    ensure!(
        retained.operation() == Some(witness.operation.as_str())
            && retained.summary.get(INGEST_BATCH_OBJECTS_KEY) == Some(source)
            && retained.summary.get(INGEST_BATCH_TOKEN_KEY)
                == Some(&ingest_batch_token(std::slice::from_ref(source)))
            && retained.summary.get("total-records") == Some(&witness.row_count.to_string()),
        "historical snapshot commit evidence differs"
    );
    let occurrences = registry
        .snapshots
        .iter()
        .filter(|snapshot| recorded(snapshot).contains(&source.as_str()))
        .map(|snapshot| snapshot.snapshot_id)
        .collect::<Vec<_>>();
    ensure!(
        occurrences == [witness.snapshot_id],
        "historical source has ambiguous snapshot registry entries"
    );
    verify_append_descendant(head, witness.snapshot_id, &by_id)?;
    Ok(head)
}

/// The one append that records `identity`, checked as an append of exactly it with a positive
/// row count that `main` descends from; `None` when no commit records it.
///
/// # Errors
/// Several commits record it, its evidence differs, or `main` does not descend from it by appends.
pub fn registered_append<'a>(
    identity: &str,
    registry: &'a IcebergSnapshotRegistry,
) -> anyhow::Result<Option<&'a IcebergRegisteredSnapshot>> {
    let matches = registry
        .snapshots
        .iter()
        .filter(|snapshot| recorded(snapshot).contains(&identity))
        .collect::<Vec<_>>();
    let record = match matches.as_slice() {
        [] => return Ok(None),
        [one] => *one,
        _ => bail!("FLOOR source has ambiguous append registry entries"),
    };
    let added = record
        .summary
        .get("added-records")
        .map(String::as_str)
        .unwrap_or_default();
    ensure!(
        record.operation() == Some("append")
            && record
                .summary
                .get(INGEST_BATCH_OBJECTS_KEY)
                .map(String::as_str)
                == Some(identity)
            && record.summary.get(INGEST_BATCH_TOKEN_KEY)
                == Some(&ingest_batch_token(&[identity.to_owned()]))
            && !added.is_empty()
            && !added.starts_with('0')
            && added.bytes().all(|byte| byte.is_ascii_digit()),
        "FLOOR append commit evidence differs"
    );
    let by_id = by_id(registry)?;
    let head = registry
        .head
        .context("historical table has no current snapshot")?;
    verify_append_descendant(head, record.snapshot_id, &by_id)?;
    Ok(Some(record))
}

#[cfg(test)]
mod tests;
