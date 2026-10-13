use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use serde::Serialize;

use super::{ledger::LedgerEntry, redact_sensitive_error, utc_now, JobSuccessReport};

const EVENT_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_ledger_event.v1";

#[derive(Clone)]
pub(super) struct EventWriter {
    writer: Arc<Mutex<File>>,
    entry_count: Arc<Mutex<u64>>,
}

impl EventWriter {
    pub(super) fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create event log directory {}", parent.display())
            })?;
        }
        let writer = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("failed to create event log {}", path.display()))?;
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            entry_count: Arc::new(Mutex::new(0)),
        })
    }

    pub(super) fn write_event(&self, event: &LedgerEvent) -> anyhow::Result<()> {
        let line = serde_json::to_string(event).context("failed to serialize ledger event")?;
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| anyhow::anyhow!("event writer lock poisoned"))?;
        writer
            .write_all(line.as_bytes())
            .context("failed to write ledger event")?;
        writer
            .write_all(b"\n")
            .context("failed to write ledger event newline")?;
        let mut entry_count = self
            .entry_count
            .lock()
            .map_err(|_| anyhow::anyhow!("event count lock poisoned"))?;
        *entry_count += 1;
        Ok(())
    }

    pub(super) fn flush(&self) -> anyhow::Result<()> {
        self.writer
            .lock()
            .map_err(|_| anyhow::anyhow!("event writer lock poisoned"))?
            .flush()
            .context("failed to flush event log")
    }

    pub(super) fn entry_count(&self) -> anyhow::Result<u64> {
        Ok(*self
            .entry_count
            .lock()
            .map_err(|_| anyhow::anyhow!("event count lock poisoned"))?)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct LedgerEvent {
    schema_version: &'static str,
    generated_at_utc: String,
    compiler_input_hash_sha256: String,
    request_fingerprint_schema_version: String,
    request_fingerprint_sha256: String,
    collection_snapshot_id: String,
    job_id: String,
    idempotency_key: String,
    scope_unit_id: String,
    shard_id: String,
    provider: String,
    endpoint: String,
    event_type: &'static str,
    status: &'static str,
    request_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_request_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_driver: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_record_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bronze_object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bronze_checksum_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bronze_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reused_bronze_object: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
}

pub(super) fn started_event(entry: &LedgerEntry, compiler_input_hash: &str) -> LedgerEvent {
    base_event(entry, compiler_input_hash, "job_started", "running", 0)
}

pub(super) fn succeeded_event(
    entry: &LedgerEntry,
    compiler_input_hash: &str,
    report: &JobSuccessReport,
) -> LedgerEvent {
    let mut event = base_event(
        entry,
        compiler_input_hash,
        "job_succeeded",
        "succeeded",
        report.provider_request_count,
    );
    event.provider_request_count = Some(report.provider_request_count);
    event.storage_driver = Some("r2");
    event.source_record_count = Some(report.source_record_count);
    event.bronze_object_key = Some(report.last_object_key.clone());
    event.bronze_checksum_sha256 = Some(report.last_checksum_sha256.clone());
    event.bronze_size_bytes = Some(report.bronze_size_bytes);
    event.reused_bronze_object = Some(false);
    event
}

pub(super) fn failed_event(
    entry: &LedgerEntry,
    compiler_input_hash: &str,
    error_message: String,
) -> LedgerEvent {
    let mut event = base_event(entry, compiler_input_hash, "job_failed", "failed", 0);
    event.provider_request_count = Some(0);
    event.error_message = Some(redact_sensitive_error(&error_message));
    event
}

fn base_event(
    entry: &LedgerEntry,
    compiler_input_hash: &str,
    event_type: &'static str,
    status: &'static str,
    request_count: u64,
) -> LedgerEvent {
    LedgerEvent {
        schema_version: EVENT_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        compiler_input_hash_sha256: compiler_input_hash.to_owned(),
        request_fingerprint_schema_version: entry.request_fingerprint_schema_version.clone(),
        request_fingerprint_sha256: entry.request_fingerprint_sha256.clone(),
        collection_snapshot_id: entry.collection_snapshot_id.clone(),
        job_id: entry.job_id.clone(),
        idempotency_key: entry.idempotency_key.clone(),
        scope_unit_id: entry.scope_unit_id.clone(),
        shard_id: entry.shard_id.clone(),
        provider: entry.provider.clone(),
        endpoint: entry.endpoint.clone(),
        event_type,
        status,
        request_count,
        provider_request_count: None,
        storage_driver: None,
        source_record_count: None,
        bronze_object_key: None,
        bronze_checksum_sha256: None,
        bronze_size_bytes: None,
        reused_bronze_object: None,
        error_message: None,
    }
}
