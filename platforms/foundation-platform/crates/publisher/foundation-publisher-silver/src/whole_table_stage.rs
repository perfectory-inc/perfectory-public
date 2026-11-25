//! What a whole-table lane gives the shared versions build (root ADR-0615, revision 2026-11-26).
//!
//! A Gold that no PNU keys (the industrial-complex catalog, keyed by its complex) is one partition:
//! the shared build plans it as it plans every lane (the inputs' pins, a shadow's replaced build, the
//! base commit), lets the stage read its inputs and build the whole table in one pass, diffs it by the
//! versions table's current-row key and appends the keys that changed in one commit. The stage knows
//! nothing of the plan, the diff or the commit; the build knows nothing of the stage's SQL.

use std::collections::BTreeMap;

use duckdb::Connection;

use crate::kind_versions_stage::ResultCheck;

/// What one whole build left.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WholeBuilt {
    /// The rows of `built`.
    pub rows: u64,
    /// What every row records as the snapshot it was built from (`source_snapshot_id`).
    pub source_snapshot_id: String,
}

/// A whole-table lane's stage.
pub trait WholeTableStage {
    /// Every input, each read at its pin.
    fn inputs(&self) -> Vec<&'static str>;
    /// The build's code version: a change of it explains a change of any size (root ADR-0346).
    fn code_version(&self) -> String;
    /// Reads every input at `pins` and leaves `built`: the whole table in the versions table's content
    /// columns, every row stamped `published_at_utc`.
    ///
    /// # Errors
    /// Returns an error when an input is not pinned or cannot be read, or a row breaks the stage's
    /// own gate.
    fn build(
        &self,
        connection: &Connection,
        pins: &BTreeMap<String, i64>,
        published_at_utc: &str,
    ) -> anyhow::Result<WholeBuilt>;
    /// The lane's own checks of the result, run before the commit (root ADR-0615 §7): the same shape
    /// as a 시도 lane's, over the whole of `built`, `appended` and `current_rows`.
    fn result_checks(&self) -> Vec<ResultCheck> {
        Vec::new()
    }
}
