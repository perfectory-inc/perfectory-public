//! The FLOOR inputs a cycle selected and its execution time; Bronze authentication runs in the export.
use super::shell_quote;
use crate::building_register_floor_silver_export::HistoryWitness;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FloorSource {
    pub floor: String,
    pub title: String,
    pub ingested_at: DateTime<Utc>,
    pub derivation: Option<String>,
    pub history: HistoryWitness,
}

impl FloorSource {
    pub fn root(&self) -> String {
        // One source/derivation owns one ready handoff across retry dates. A retry recovers
        // its original time from that summary; clock time must not hide a completed handoff.
        // This names local artifacts only. Bronze-derived source identity stays in the exporter.
        let identity = format!("{}\n{}\n{:?}", self.floor, self.title, self.derivation);
        format!(
            "target/lakehouse/floor-runs/{:x}",
            Sha256::digest(identity.as_bytes())
        )
    }

    pub fn exporter_env(&self) -> String {
        format!(
            "  -e DATABASE_URL \\\n  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_REUSE_COMPLETED_HANDOFF=1 \\\n  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_VERIFY_BRONZE_LEDGER=1 \\\n  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_OBJECT={} \\\n  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_TITLE_SOURCE_OBJECT={} \\\n  -e FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC={} \\\n",
            shell_quote(&self.floor), shell_quote(&self.title), shell_quote(&self.ingested_at.to_rfc3339())
        )
    }
}
