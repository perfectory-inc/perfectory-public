//! Explicit FLOOR inputs and retained execution time; Bronze authentication runs on the server.
use super::{required_lookup, shell_quote};
use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use foundation_outbox_publisher::building_register_floor_silver_export::HistoryWitness;
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
    pub fn from_lookup(lookup: &mut impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
        let floor = required_lookup(
            lookup,
            "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_FLOOR_SOURCE_OBJECT",
        )?;
        let title = required_lookup(
            lookup,
            "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_TITLE_SOURCE_OBJECT",
        )?;
        let date = foundation_outbox_publisher::building_register_snapshot::object_date(&floor)?;
        foundation_outbox_publisher::building_register_snapshot::validate_object_name(
            &title, date,
        )?;
        let time = required_lookup(
            lookup,
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC",
        )?;
        let ingested_at = DateTime::parse_from_rfc3339(&time)
            .context("invalid retained FLOOR ingestion time")?
            .with_timezone(&Utc);
        let derivation = lookup("FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_DERIVATION");
        ensure!(
            lookup("FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_INPUT_FILE_BATCH_SIZE").is_none(),
            "FLOOR source is one logical load; input file batching cannot split its identity"
        );
        ensure!(
            lookup("FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_INPUT_PATH").is_none(),
            "FLOOR pipeline input must be its exact exported handoff"
        );
        Ok(Self {
            floor,
            title,
            ingested_at,
            derivation,
            history: HistoryWitness::from_lookup(lookup, false)?,
        })
    }

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
