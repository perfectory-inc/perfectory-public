//! `list-lakehouse-objects`: every object under one prefix of the lakehouse bucket, read-only, as
//! JSON for the media reconciler (scripts/ops/media_reconcile.py, root ADR-0387).
//!
//! It lists with the lakehouse reader key (`FOUNDATION_PLATFORM_LAKEHOUSE_READER_*`, the key the Silver
//! load plan lists Bronze with) and writes `{prefix, list_request_count, objects: [{key, size_bytes,
//! last_modified}]}` to `FOUNDATION_PLATFORM_LAKEHOUSE_OBJECT_LISTING_OUTPUT`. It writes nothing to R2.

use std::path::PathBuf;

use anyhow::{ensure, Context};
use foundation_outbox::object_storage::{R2InventoryRequest, R2ObjectStorageConfig};
use foundation_outbox::R2ObjectStorage;
use serde_json::json;

use crate::public_data_control_support::{required_env_value, write_json_file};

const PREFIX: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_OBJECT_LISTING";
const LIST_PAGE: i32 = 1000;

/// Lists the prefix and writes the listing.
///
/// # Errors
/// Returns an error when the prefix is empty or not a directory (`/`-terminated), R2 cannot be
/// listed, or the output cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let prefix = required_env_value(&format!("{PREFIX}_PREFIX"))?;
    ensure!(
        prefix.ends_with('/') && prefix.len() > 1,
        "{PREFIX}_PREFIX must name a directory ending in '/', not the bucket root: {prefix}"
    );
    let output = PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT"))?);
    let storage = R2ObjectStorage::from_config(
        R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("the lakehouse object listing reads R2 with the lakehouse reader key")?,
    );
    let report = storage
        .inventory_audit(R2InventoryRequest::new(Some(&prefix), Some(LIST_PAGE))?)
        .await
        .with_context(|| format!("failed to list {prefix}"))?;
    let objects: Vec<_> = report
        .objects()
        .iter()
        .map(|object| {
            json!({
                "key": object.key,
                "size_bytes": object.size_bytes,
                "last_modified": object.last_modified,
            })
        })
        .collect();
    write_json_file(
        &output,
        &json!({
            "prefix": prefix,
            "list_request_count": report.list_request_count(),
            "objects": objects,
        }),
    )?;
    println!(
        "lakehouse-object-listing prefix={prefix} objects={} list_requests={}",
        objects.len(),
        report.list_request_count()
    );
    Ok(())
}
