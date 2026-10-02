//! Latest complete monthly pair, with ambiguity refused before any downstream work.
use std::io::Write as _;

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use collection_application::ports::{BronzeIngestRepository, BronzeMonthCandidate};
use collection_infrastructure::PgBronzeIngestRepository;
use serde::Serialize;
use sqlx::PgPool;

use super::{CommittedInputs, HistoryWitness};
use crate::{bounded_bytes::BoundedBytes, building_register_source_role::SourceRole};

const MAX_SELECTION_BYTES: usize = 16 * 1024;

/// A metadata-authenticated pair; staging still authenticates the actual retained ZIP bytes.
#[derive(Clone, Debug, Serialize)]
pub struct FloorInputSelection {
    /// JSON output contract identifier.
    pub schema_version: &'static str,
    /// Exact FLOOR filename, without any prefix selection.
    pub floor_source_object: String,
    /// Exact title filename from the same provider export day.
    pub title_source_object: String,
    /// Stable source identity derived through the existing committed-input contract.
    pub source_snapshot_id: String,
    /// Provider validity timestamp, preserving an authenticated historical override.
    pub valid_from_utc: DateTime<Utc>,
    /// Historical persisted time only; new sources require the caller's retry/time policy.
    pub retained_ingested_at_utc: Option<DateTime<Utc>>,
    /// Exact IDs, keys, checksums, sizes and provider evidence selected in one query snapshot.
    pub committed_inputs: CommittedInputs,
}

/// Selects the latest committed complete FLOOR/title month without writing or staging objects.
/// # Errors
/// Returns an error for unavailable storage metadata, missing pairs, ambiguity or invalid evidence.
pub async fn select_inputs() -> anyhow::Result<FloorInputSelection> {
    let history = HistoryWitness::from_lookup(&mut |key| std::env::var(key).ok(), false)?;
    let pool = PgPool::connect(&super::required_env("DATABASE_URL")?).await?;
    let result = PgBronzeIngestRepository::new(pool.clone())
        .latest_complete_bronze_month_candidates(SourceRole::Floor.slug(), SourceRole::Title.slug())
        .await;
    pool.close().await;
    from_candidates(result?, history)
}

pub(super) fn from_candidates(
    candidates: Vec<BronzeMonthCandidate>,
    history: HistoryWitness,
) -> anyhow::Result<FloorInputSelection> {
    ensure!(
        candidates.len() == 2,
        "latest complete FLOOR/title month is missing or ambiguous"
    );
    let mut floor = None;
    let mut title = None;
    for candidate in candidates {
        let slot = match candidate.source_slug.as_str() {
            slug if slug == SourceRole::Floor.slug() => &mut floor,
            slug if slug == SourceRole::Title.slug() => &mut title,
            _ => anyhow::bail!("selected month contains an unexpected source role"),
        };
        ensure!(
            slot.is_none(),
            "selected month contains duplicate source roles"
        );
        *slot = Some(candidate.object);
    }
    let committed = CommittedInputs::from_objects(
        floor.as_ref().context("selected month is missing FLOOR")?,
        title.as_ref().context("selected month is missing title")?,
        history,
    )?;
    let (floor_source_object, title_source_object) = committed.source_names();
    Ok(FloorInputSelection {
        schema_version: "foundation-platform.floor-input-selection.v1",
        floor_source_object,
        title_source_object,
        source_snapshot_id: committed.source_snapshot_id()?,
        valid_from_utc: committed.valid_from_utc()?,
        retained_ingested_at_utc: committed.retained_ingested_at_utc()?,
        committed_inputs: committed,
    })
}

pub(super) fn payload(selection: &FloorInputSelection) -> anyhow::Result<Vec<u8>> {
    let mut bytes =
        BoundedBytes::with_error(MAX_SELECTION_BYTES, "FLOOR selection exceeds byte bound");
    serde_json::to_writer(&mut bytes, selection)?;
    bytes.write_all(b"\n")?;
    Ok(bytes.into_inner())
}

pub(super) async fn run() -> anyhow::Result<()> {
    let bytes = payload(&select_inputs().await?)?;
    std::io::stdout().lock().write_all(&bytes)?;
    Ok(())
}
