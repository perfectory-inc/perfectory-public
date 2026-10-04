//! The crosswalk the export tests compose through: a projection that reproduces the baseline
//! fixture's pairs, loaded by the same code path and checks as a real one (root ADR-0143 §5,
//! ADR-0145). The exports themselves refuse to run without a real projection.

use foundation_outbox_publisher::sigungu_crosswalk::{
    baseline_fixture_json, hub_sigungu_crosswalk_from,
};
use foundation_shared_kernel::pnu::SigunguCrosswalk;
use serde_json::{json, Value};

const RECORD: &str = "bronze/source=codegokr__legal_dong_code_table/regcode-test.html";

pub(crate) fn seed() -> anyhow::Result<SigunguCrosswalk> {
    let baseline: Value = serde_json::from_str(baseline_fixture_json())?;
    let entries = |value: &Value| value.as_array().cloned().unwrap_or_default();
    let sido = entries(&baseline["sido"])
        .iter()
        .map(|sido| json!({"new_code": sido["current_code"], "old_codes": sido["supersedes"]}))
        .collect::<Vec<_>>();
    let sigungu = entries(&baseline["sigungu"])
        .iter()
        .map(|pair| json!({"old_code": pair["superseded_code"], "new_code": pair["current_code"]}))
        .collect::<Vec<_>>();
    let projection = json!({
        "schema_version": "foundation-platform.sigungu_crosswalk_projection.v2",
        "legal_dong_snapshot_date": "2099-01-01",
        "legal_dong_snapshot_record": RECORD,
        "change_table": "reference.legal_dong_code_change",
        "change_table_snapshot_id": "test",
        "sido": sido,
        "sigungu": sigungu,
    });
    let marker = json!({"snapshot_date": "2099-01-01", "source_record_id": RECORD});
    hub_sigungu_crosswalk_from(
        projection.to_string().as_bytes(),
        marker.to_string().as_bytes(),
        "test projection",
    )
}
