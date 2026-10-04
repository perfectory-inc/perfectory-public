//! The crosswalk the export tests compose through: a projection that reproduces the seed's own
//! pairs, loaded by the same code path and checks as a real one (root ADR-0143 §5). The exports
//! themselves refuse to run without a real projection.

use foundation_outbox_publisher::sigungu_crosswalk::{
    hub_sigungu_crosswalk_from, seed_contract_json,
};
use foundation_shared_kernel::pnu::SigunguCrosswalk;
use serde_json::{json, Value};

const RECORD: &str = "bronze/source=codegokr__legal_dong_code_table/regcode-test.html";

pub(crate) fn seed() -> anyhow::Result<SigunguCrosswalk> {
    let seed: Value = serde_json::from_str(seed_contract_json())?;
    let projection = json!({
        "schema_version": "foundation-platform.sigungu_crosswalk_projection.v1",
        "legal_dong_snapshot_date": "2099-01-01",
        "legal_dong_snapshot_record": RECORD,
        "crosswalk_table_snapshot_id": "test",
        "sido": seed["sido"],
        "sigungu": seed["sigungu"],
    });
    let marker = json!({"snapshot_date": "2099-01-01", "source_record_id": RECORD});
    hub_sigungu_crosswalk_from(
        projection.to_string().as_bytes(),
        marker.to_string().as_bytes(),
        "test projection",
    )
}
