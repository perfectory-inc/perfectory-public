use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use anyhow::bail;
use serde_json::Value as JsonValue;

use super::{ledger_jsonl::read_ledger_jsonl, string_prop, strip_utf8_bom};

pub(in crate::national_data_collection_ledger_execute) struct SelectedJobs {
    pub(in crate::national_data_collection_ledger_execute) jobs: Vec<JsonValue>,
    pub(in crate::national_data_collection_ledger_execute) read_mode: &'static str,
    pub(in crate::national_data_collection_ledger_execute) scanned_row_count: usize,
    pub(in crate::national_data_collection_ledger_execute) loaded_row_count: usize,
    pub(in crate::national_data_collection_ledger_execute) skipped_job_count: usize,
}

pub(in crate::national_data_collection_ledger_execute) struct RequestedJobIds {
    pub(in crate::national_data_collection_ledger_execute) ordered: Vec<String>,
    pub(in crate::national_data_collection_ledger_execute) set: BTreeSet<String>,
}

pub(in crate::national_data_collection_ledger_execute) fn read_requested_job_ids(
    path: &Path,
) -> anyhow::Result<RequestedJobIds> {
    let bytes = fs::read(path)?;
    let raw = String::from_utf8_lossy(strip_utf8_bom(&bytes));
    let mut ordered = Vec::new();
    let mut set = BTreeSet::new();
    for line in raw.lines() {
        let job_id = line.trim();
        if job_id.is_empty() {
            continue;
        }
        if !set.insert(job_id.to_owned()) {
            bail!("JobIdsPath contains duplicate job id: {job_id}");
        }
        ordered.push(job_id.to_owned());
    }
    if ordered.is_empty() {
        bail!("JobIdsPath must contain at least one job id");
    }
    Ok(RequestedJobIds { ordered, set })
}

pub(in crate::national_data_collection_ledger_execute) fn read_planned_ledger_rows_for_job_ids(
    path: &Path,
    requested: &[String],
    requested_set: &BTreeSet<String>,
) -> anyhow::Result<SelectedJobs> {
    let rows = read_ledger_jsonl(path)?;
    let mut scanned = 0;
    let mut found = BTreeMap::new();
    for row in rows {
        scanned += 1;
        let job_id = string_prop(&row, "job_id");
        if !requested_set.contains(&job_id) || string_prop(&row, "status") != "planned" {
            continue;
        }
        if found.insert(job_id.clone(), row).is_some() {
            bail!("execution ledger contains duplicate planned job id: {job_id}");
        }
        if found.len() == requested_set.len() {
            break;
        }
    }
    let mut jobs = Vec::new();
    for job_id in requested {
        let Some(job) = found.remove(job_id) else {
            bail!("JobIdsPath contains job id not present in plan: {job_id}");
        };
        jobs.push(job);
    }
    if jobs.is_empty() {
        bail!("ledger has no planned jobs to execute");
    }
    Ok(SelectedJobs {
        loaded_row_count: jobs.len(),
        jobs,
        read_mode: "job_ids_stream_filter",
        scanned_row_count: scanned,
        skipped_job_count: 0,
    })
}
