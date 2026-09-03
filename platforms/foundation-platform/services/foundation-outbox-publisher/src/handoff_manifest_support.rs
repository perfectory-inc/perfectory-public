//! What every manifest-driven projection load does the same way, held once.
//!
//! Two commands now start from an export-written manifest and merge per-object into a catalog
//! table: the building-unit load (ADR-0072) and the building load (ADR-0073). The manifest's
//! shape, its refusals, and the final verdict are properties of the pipeline, not of either
//! dataset — and the Spark boundary jobs already showed what a copy per caller costs (thirty
//! same-named functions, five identical, 2026-09-02).

use anyhow::bail;
use serde::Deserialize;

/// The handoff contract: every name the Spark writer and a reader must agree on, once.
#[derive(Debug, Deserialize)]
pub(crate) struct HandoffContract {
    pub(crate) schema_version: u32,
    pub(crate) manifest_object: String,
    pub(crate) columns: Vec<String>,
}

impl HandoffContract {
    /// Reads and validates the contract file this command was pointed at.
    pub(crate) fn load(path: &str) -> anyhow::Result<Self> {
        let contract: Self =
            serde_json::from_str(&std::fs::read_to_string(path).map_err(|error| {
                anyhow::anyhow!("failed to read the handoff contract {path}: {error}")
            })?)
            .map_err(|error| {
                anyhow::anyhow!("failed to parse the handoff contract {path}: {error}")
            })?;
        if contract.schema_version != 1 {
            bail!(
                "handoff contract schema_version {} is not the 1 this command reads",
                contract.schema_version
            );
        }
        Ok(contract)
    }
}

/// What an export wrote, read back as a load's work list.
#[derive(Debug, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) schema_version: String,
    pub(crate) columns: Vec<String>,
    pub(crate) objects: Vec<ManifestObject>,
    pub(crate) exported_row_count: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ManifestObject {
    pub(crate) key: String,
    pub(crate) rows: u64,
}

/// Refuses a manifest a load cannot vouch for, before any object is read.
///
/// The column list is compared against the contract because the writer and the reader both read
/// it: a column added on one side is refused here until both moved. The row-count sum is compared
/// against the manifest's own total because the manifest is written last — a sum that disagrees
/// means the export died between writing objects and counting them, and the load must not guess
/// which is true.
pub(crate) fn validate_manifest(
    manifest: &Manifest,
    contract: &HandoffContract,
    expected_schema_version: &str,
) -> anyhow::Result<()> {
    if manifest.schema_version != expected_schema_version {
        bail!(
            "manifest schema_version {:?} is not the {expected_schema_version:?} this command reads",
            manifest.schema_version
        );
    }
    if manifest.columns != contract.columns {
        bail!(
            "manifest columns {:?} do not match the contract's {:?}; the export and this loader \
             are reading different contracts",
            manifest.columns,
            contract.columns
        );
    }
    if manifest.objects.is_empty() {
        bail!("the manifest names no objects, which is not a state this dataset has");
    }
    let listed: u64 = manifest.objects.iter().map(|object| object.rows).sum();
    if listed != manifest.exported_row_count {
        bail!(
            "the manifest's objects sum to {listed} rows but it claims {} were exported; \
             the export did not finish writing what it counted",
            manifest.exported_row_count
        );
    }
    Ok(())
}

/// Counters a finished pass reports, whatever its table.
pub(crate) struct PassTotals {
    pub(crate) object_count: usize,
    pub(crate) staged: u64,
    pub(crate) attached: u64,
    pub(crate) orphaned: u64,
    pub(crate) manifest_rows: u64,
    pub(crate) table_rows: i64,
}

/// Decides what a finished pass amounts to, and refuses to call a partial one complete.
///
/// A function over values rather than lines inside each `run`, so a test can ask it directly:
/// the property is not "an error is printed" but "an unread object, a manifest shortfall, or an
/// unaccounted row makes this return `Err`".
pub(crate) fn verdict(
    label_prefix: &str,
    unread: &[String],
    totals: &PassTotals,
) -> anyhow::Result<()> {
    let label = if unread.is_empty() {
        format!("{label_prefix}-ok")
    } else {
        format!("{label_prefix}-incomplete")
    };
    println!(
        "{label} objects={} unread={} staged={} attached={} orphaned={} manifest_rows={} table_rows={}",
        totals.object_count,
        unread.len(),
        totals.staged,
        totals.attached,
        totals.orphaned,
        totals.manifest_rows,
        totals.table_rows
    );
    for key in unread {
        println!("{label_prefix}-incomplete-key {key}");
    }
    if !unread.is_empty() {
        bail!(
            "{} of {} objects could not be read; the table holds {} rows and is not the whole \
             dataset. Re-running loads only what is missing.",
            unread.len(),
            totals.object_count,
            totals.table_rows
        );
    }
    if totals.staged != totals.manifest_rows {
        bail!(
            "the pass staged {} rows but the manifest promised {}; an object changed between \
             export and load",
            totals.staged,
            totals.manifest_rows
        );
    }
    if totals.attached + totals.orphaned != totals.staged {
        bail!(
            "attached {} + orphaned {} does not account for the {} staged rows; the merge lost \
             track of some",
            totals.attached,
            totals.orphaned,
            totals.staged
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract() -> HandoffContract {
        HandoffContract {
            schema_version: 1,
            manifest_object: "silver-handoff/x/manifest.json".to_owned(),
            columns: vec!["register_pk".to_owned(), "pnu".to_owned()],
        }
    }

    fn manifest() -> Manifest {
        Manifest {
            schema_version: "v1".to_owned(),
            columns: vec!["register_pk".to_owned(), "pnu".to_owned()],
            objects: vec![
                ManifestObject {
                    key: "silver-handoff/x/99999.jsonl.gz".to_owned(),
                    rows: 3,
                },
                ManifestObject {
                    key: "silver-handoff/x/99998.jsonl.gz".to_owned(),
                    rows: 2,
                },
            ],
            exported_row_count: 5,
        }
    }

    fn totals() -> PassTotals {
        PassTotals {
            object_count: 2,
            staged: 5,
            attached: 4,
            orphaned: 1,
            manifest_rows: 5,
            table_rows: 4,
        }
    }

    #[test]
    fn a_manifest_that_adds_up_passes() {
        validate_manifest(&manifest(), &contract(), "v1").expect("a consistent manifest");
    }

    #[test]
    fn a_manifest_whose_objects_do_not_sum_to_its_total_is_refused() {
        let mut broken = manifest();
        broken.exported_row_count = 99;

        let error = validate_manifest(&broken, &contract(), "v1")
            .expect_err("a manifest that does not add up must be refused");

        assert!(format!("{error:#}").contains("did not finish"));
    }

    #[test]
    fn a_manifest_with_different_columns_is_refused() {
        let mut drifted = manifest();
        drifted.columns = vec!["register_pk".to_owned()];

        let error = validate_manifest(&drifted, &contract(), "v1")
            .expect_err("a column drift must be refused");

        assert!(format!("{error:#}").contains("different contracts"));
    }

    #[test]
    fn a_manifest_from_a_newer_export_is_refused() {
        let error = validate_manifest(&manifest(), &contract(), "v2")
            .expect_err("a newer manifest must not be guessed at");

        assert!(format!("{error:#}").contains("schema_version"));
    }

    #[test]
    fn an_empty_manifest_is_refused() {
        let mut empty = manifest();
        empty.objects.clear();
        empty.exported_row_count = 0;

        let error = validate_manifest(&empty, &contract(), "v1")
            .expect_err("no objects is not a state this dataset has");

        assert!(format!("{error:#}").contains("no objects"));
    }

    #[test]
    fn a_pass_with_nothing_unread_and_matching_counts_is_complete() {
        verdict("x-load", &[], &totals()).expect("a complete pass");
    }

    #[test]
    fn one_unread_object_makes_the_pass_fail() {
        let unread = vec!["silver-handoff/x/99999.jsonl.gz".to_owned()];

        let error = verdict("x-load", &unread, &totals())
            .expect_err("a pass that could not read an object is not a finished pass");

        assert!(format!("{error:#}").contains("1 of 2"));
    }

    #[test]
    fn a_pass_that_staged_fewer_rows_than_the_manifest_promised_fails() {
        let mut short = totals();
        short.staged = 4;
        short.attached = 4;
        short.orphaned = 0;

        let error = verdict("x-load", &[], &short)
            .expect_err("a shortfall against the manifest is not a finished pass");

        assert!(format!("{error:#}").contains("manifest promised"));
    }

    #[test]
    fn rows_the_merge_lost_track_of_fail_the_pass() {
        let mut lost = totals();
        lost.attached = 3;

        let error = verdict("x-load", &[], &lost)
            .expect_err("attached plus orphaned must account for every staged row");

        assert!(format!("{error:#}").contains("lost track"));
    }
}
