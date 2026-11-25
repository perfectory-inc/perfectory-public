//! The Dagster Pipes messages the publisher writes: one module, so every command that reports a
//! check writes the protocol's one shape (root ADR-0615 §7 "Pipes 줄", ADR-0616 §3).
//!
//! A message is one JSON line (`{"__dagster_pipes_version", "method", "params"}`, the protocol's
//! envelope). The asset is its Iceberg table (`iceberg/<namespace>/<table>`, the code location's key
//! of a table node). A check's severity is its declaration's: the line says only whether it
//! passed. Each metadata entry is `{"raw_value", "type": "__infer__"}`, as the Python
//! `dagster-pipes` writes a plain value.
//!
//! The publisher does not open a Pipes context (ADR-0616 §3): it prints its lines on stdout, and
//! whoever runs it reads them there (a unit's journal on ai-server, `systemd_pipes.py`; the
//! subprocess of `court_auction_lane.py`).

use serde_json::{json, Map, Value};

/// The protocol version the lines say.
pub(crate) const PROTOCOL_VERSION: &str = "0.1";

/// The code location's asset key of the Iceberg table `table` (`gold.x` -> `iceberg/gold/x`).
pub(crate) fn asset_key(table: &str) -> String {
    format!("iceberg/{}", table.replace('.', "/"))
}

/// The `report_asset_check` message of `check` on the asset `asset_key`, with `metadata`.
pub(crate) fn asset_check(
    asset_key: &str,
    check: &str,
    passed: bool,
    metadata: impl IntoIterator<Item = (String, Value)>,
) -> Value {
    let metadata: Map<String, Value> = metadata
        .into_iter()
        .map(|(name, value)| (name, json!({"raw_value": value, "type": "__infer__"})))
        .collect();
    json!({
        "__dagster_pipes_version": PROTOCOL_VERSION,
        "method": "report_asset_check",
        "params": {
            "asset_key": asset_key,
            "check_name": check,
            "passed": passed,
            "metadata": metadata,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_check_is_the_protocols_envelope_on_the_tables_asset_with_plain_metadata() {
        let line = asset_check(
            &asset_key("gold.parcel_example_versions"),
            "rows_and_keys",
            true,
            [("sido".to_owned(), json!("99"))],
        );
        assert_eq!(
            line,
            json!({
                "__dagster_pipes_version": "0.1",
                "method": "report_asset_check",
                "params": {
                    "asset_key": "iceberg/gold/parcel_example_versions",
                    "check_name": "rows_and_keys",
                    "passed": true,
                    "metadata": {"sido": {"raw_value": "99", "type": "__infer__"}},
                },
            })
        );
    }
}
