use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::{bail, Context};
use serde_json::Value as JsonValue;

use super::{is_sha256, ledger_jsonl::read_jsonl, string_prop, u64_prop};

#[derive(Default)]
pub(in crate::national_data_collection_ledger_execute) struct ReuseIndex {
    pub(in crate::national_data_collection_ledger_execute) by_fingerprint:
        BTreeMap<String, ReuseEntry>,
}

impl ReuseIndex {
    pub(in crate::national_data_collection_ledger_execute) fn read(
        path: &Path,
    ) -> anyhow::Result<Self> {
        if !path.is_file() {
            bail!("reuse Bronze object manifest missing: {}", path.display());
        }
        let rows = read_jsonl(path, "reuse Bronze object manifest")?;
        let mut groups: BTreeMap<String, Vec<JsonValue>> = BTreeMap::new();
        for row in rows {
            let fingerprint = string_prop(&row, "request_fingerprint_sha256");
            if !is_sha256(&fingerprint) {
                bail!("reuse Bronze object manifest entry must include request_fingerprint_sha256");
            }
            groups.entry(fingerprint).or_default().push(row);
        }
        let mut by_fingerprint = BTreeMap::new();
        for (fingerprint, group) in groups {
            by_fingerprint.insert(
                fingerprint.clone(),
                ReuseEntry::from_group(&fingerprint, &group)?,
            );
        }
        Ok(Self { by_fingerprint })
    }

    pub(in crate::national_data_collection_ledger_execute) fn contains(
        &self,
        job: &JsonValue,
    ) -> bool {
        self.by_fingerprint
            .contains_key(&string_prop(job, "request_fingerprint_sha256"))
    }
}

pub(in crate::national_data_collection_ledger_execute) struct ReuseEntry {
    pub(in crate::national_data_collection_ledger_execute) request_fingerprint_schema_version:
        String,
    pub(in crate::national_data_collection_ledger_execute) request_fingerprint_sha256: String,
    pub(in crate::national_data_collection_ledger_execute) collection_snapshot_id: String,
    pub(in crate::national_data_collection_ledger_execute) job_id: String,
    pub(in crate::national_data_collection_ledger_execute) scope_unit_id: String,
    pub(in crate::national_data_collection_ledger_execute) provider: String,
    pub(in crate::national_data_collection_ledger_execute) endpoint: String,
    pub(in crate::national_data_collection_ledger_execute) storage_driver: String,
    pub(in crate::national_data_collection_ledger_execute) page_count: u64,
    pub(in crate::national_data_collection_ledger_execute) source_record_count: u64,
    pub(in crate::national_data_collection_ledger_execute) last_object_key: String,
}

impl ReuseEntry {
    fn from_group(fingerprint: &str, rows: &[JsonValue]) -> anyhow::Result<Self> {
        let first = rows.first().context("reuse group must not be empty")?;
        let entry = Self {
            request_fingerprint_schema_version: string_prop(
                first,
                "request_fingerprint_schema_version",
            ),
            request_fingerprint_sha256: fingerprint.to_owned(),
            collection_snapshot_id: string_prop(first, "collection_snapshot_id"),
            job_id: string_prop(first, "job_id"),
            scope_unit_id: string_prop(first, "scope_unit_id"),
            provider: string_prop(first, "provider"),
            endpoint: string_prop(first, "endpoint"),
            storage_driver: string_prop(first, "storage_driver"),
            page_count: u64_prop(first, "page_count", 0),
            source_record_count: u64_prop(first, "job_source_record_count", u64::MAX),
            last_object_key: string_prop(first, "job_last_bronze_object_key"),
        };
        entry.validate_identity()?;
        let mut seen_pages = BTreeSet::new();
        for row in rows {
            validate_reuse_row(&entry, row)?;
            let page_number = u64_prop(row, "page_number", 0);
            if !seen_pages.insert(page_number) {
                bail!(
                    "reuse Bronze object manifest duplicate page: {} page={}",
                    entry.job_id,
                    page_number
                );
            }
        }
        if seen_pages.len() != usize::try_from(entry.page_count).unwrap_or(usize::MAX) {
            bail!(
                "reuse Bronze object manifest must include every page for job: {}",
                entry.job_id
            );
        }
        Ok(entry)
    }

    fn validate_identity(&self) -> anyhow::Result<()> {
        if self.job_id.is_empty()
            || self.scope_unit_id.is_empty()
            || self.provider.is_empty()
            || self.endpoint.is_empty()
        {
            bail!(
                "reuse Bronze object manifest entry identity fields are required: {}",
                self.request_fingerprint_sha256
            );
        }
        if self.request_fingerprint_schema_version.is_empty()
            || self.collection_snapshot_id.is_empty()
        {
            bail!(
                "reuse Bronze object manifest entry fingerprint metadata is required: {}",
                self.job_id
            );
        }
        if !matches!(self.storage_driver.as_str(), "local" | "r2") {
            bail!(
                "reuse Bronze object manifest storage_driver is invalid: {}",
                self.job_id
            );
        }
        if self.page_count < 1 {
            bail!(
                "reuse Bronze object manifest page_count must be positive: {}",
                self.job_id
            );
        }
        if self.source_record_count == u64::MAX {
            bail!(
                "reuse Bronze object manifest job_source_record_count must be non-negative: {}",
                self.job_id
            );
        }
        if self.last_object_key.is_empty() {
            bail!(
                "reuse Bronze object manifest job_last_bronze_object_key is required: {}",
                self.job_id
            );
        }
        Ok(())
    }
}

fn validate_reuse_row(entry: &ReuseEntry, row: &JsonValue) -> anyhow::Result<()> {
    if string_prop(row, "job_id") != entry.job_id
        || string_prop(row, "scope_unit_id") != entry.scope_unit_id
        || string_prop(row, "provider") != entry.provider
        || string_prop(row, "endpoint") != entry.endpoint
    {
        bail!(
            "reuse Bronze object manifest request fingerprint maps to multiple jobs: {}",
            entry.request_fingerprint_sha256
        );
    }
    if string_prop(row, "storage_driver") != entry.storage_driver
        || string_prop(row, "request_fingerprint_schema_version")
            != entry.request_fingerprint_schema_version
        || string_prop(row, "collection_snapshot_id") != entry.collection_snapshot_id
        || u64_prop(row, "page_count", 0) != entry.page_count
    {
        bail!(
            "reuse Bronze object manifest metadata must be stable within fingerprint: {}",
            entry.job_id
        );
    }
    let page_number = u64_prop(row, "page_number", 0);
    if page_number < 1 || page_number > entry.page_count {
        bail!(
            "reuse Bronze object manifest page_number is out of range: {}",
            entry.job_id
        );
    }
    let object_key = string_prop(row, "object_key");
    if object_key.is_empty() || !object_key.starts_with("bronze/source=") {
        bail!(
            "reuse Bronze object manifest object_key must be provider-relative Bronze key: {}",
            entry.job_id
        );
    }
    Ok(())
}

pub(in crate::national_data_collection_ledger_execute) fn validate_reuse_identity(
    job: &JsonValue,
    reuse: &ReuseEntry,
) -> anyhow::Result<()> {
    if reuse.job_id != string_prop(job, "job_id")
        || reuse.scope_unit_id != string_prop(job, "scope_unit_id")
        || reuse.provider != string_prop(job, "provider")
        || reuse.endpoint != string_prop(job, "endpoint")
        || reuse.request_fingerprint_schema_version
            != string_prop(job, "request_fingerprint_schema_version")
        || reuse.collection_snapshot_id != string_prop(job, "collection_snapshot_id")
    {
        bail!(
            "reuse Bronze object manifest identity mismatch for job: {}",
            string_prop(job, "job_id")
        );
    }
    Ok(())
}
