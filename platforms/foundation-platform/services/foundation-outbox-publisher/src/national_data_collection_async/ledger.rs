use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::{bail, Context};
use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct LedgerEntry {
    pub(super) job_id: String,
    pub(super) provider: String,
    #[serde(default)]
    pub(super) endpoint_slug: String,
    pub(super) endpoint: String,
    pub(super) operation: String,
    pub(super) sigungu_cd: String,
    pub(super) bjdong_cd: String,
    #[serde(default)]
    pub(super) lawd_cd: String,
    #[serde(default)]
    pub(super) deal_ymd: String,
    pub(super) scope_unit_id: String,
    pub(super) shard_id: String,
    pub(super) idempotency_key: String,
    pub(super) source_slug: String,
    pub(super) request_fingerprint_sha256: String,
    pub(super) request_fingerprint_schema_version: String,
    pub(super) collection_snapshot_id: String,
    pub(super) status: String,
    pub(super) page_start: Option<u32>,
    pub(super) page_end: Option<u32>,
    pub(super) max_pages: u32,
    pub(super) num_of_rows: u32,
    pub(super) request_count_estimate: u32,
}

pub(super) fn read_ledger_entries(path: &Path) -> anyhow::Result<Vec<LedgerEntry>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open execution ledger {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read ledger line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let line = line.trim_start_matches('\u{feff}');
        entries.push(
            serde_json::from_str::<LedgerEntry>(line)
                .with_context(|| format!("failed to parse ledger line {}", index + 1))?,
        );
    }
    Ok(entries)
}

pub(super) fn read_succeeded_job_ids(
    evidence_scan_dir: &Path,
    compiler_input_hash: &str,
) -> anyhow::Result<BTreeSet<String>> {
    let mut succeeded = BTreeSet::new();
    if !evidence_scan_dir.exists() {
        return Ok(succeeded);
    }
    for entry in fs::read_dir(evidence_scan_dir)
        .with_context(|| format!("failed to read {}", evidence_scan_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with("national-data-collection-ledger-events-") || !name.ends_with(".jsonl")
        {
            continue;
        }
        read_succeeded_job_ids_from_event_log(&path, compiler_input_hash, &mut succeeded)?;
    }
    Ok(succeeded)
}

fn read_succeeded_job_ids_from_event_log(
    path: &Path,
    compiler_input_hash: &str,
    succeeded: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    let file =
        File::open(path).with_context(|| format!("failed to open event log {}", path.display()))?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<JsonValue>(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if value
            .pointer("/compiler_input_hash_sha256")
            .and_then(JsonValue::as_str)
            != Some(compiler_input_hash)
        {
            continue;
        }
        if value.pointer("/event_type").and_then(JsonValue::as_str) == Some("job_succeeded") {
            if let Some(job_id) = value.pointer("/job_id").and_then(JsonValue::as_str) {
                succeeded.insert(job_id.to_owned());
            }
        }
    }
    Ok(())
}

pub(super) fn select_pending_jobs(
    entries: &[LedgerEntry],
    succeeded_job_ids: &BTreeSet<String>,
    max_jobs: usize,
    request_cap: u64,
) -> anyhow::Result<Vec<LedgerEntry>> {
    let mut selected = Vec::new();
    let mut selected_request_count = 0_u64;
    for entry in entries {
        if entry.status != "planned" || succeeded_job_ids.contains(&entry.job_id) {
            continue;
        }
        let estimate = u64::from(entry.request_count_estimate);
        if selected.is_empty() && estimate > request_cap {
            bail!(
                "first pending job request estimate exceeds request cap: job_id={} estimate={} cap={}",
                entry.job_id,
                estimate,
                request_cap
            );
        }
        if selected_request_count + estimate > request_cap {
            break;
        }
        selected.push(entry.clone());
        selected_request_count += estimate;
        if selected.len() >= max_jobs {
            break;
        }
    }
    if selected.is_empty() {
        bail!("national async ledger has no pending jobs to execute");
    }
    Ok(selected)
}
