//! `list-lakehouse-objects`: every object under one prefix of the lakehouse bucket, read-only, as
//! JSON for the media reconciler (scripts/ops/media_reconcile.py, root ADR-0387).
//!
//! It lists with the lakehouse reader key (`FOUNDATION_PLATFORM_LAKEHOUSE_READER_*`, the key the Silver
//! load plan lists Bronze with) and writes JSON Lines to `FOUNDATION_PLATFORM_LAKEHOUSE_OBJECT_LISTING_OUTPUT`:
//! a first line `{prefix, list_request_count, object_count}`, then one `{key, size_bytes, last_modified}`
//! per object. Lines, not one document, so the reconciler compares as it reads and holds only its own
//! key set: one JSON document of 188,000 objects held as Python dicts took the run to its 512 MB cap
//! on 2026-11-16 and every run since timed out (root ADR-0387). It writes nothing to R2.

use std::io::{BufWriter, Write as _};
use std::path::PathBuf;

use anyhow::{ensure, Context};
use foundation_outbox::object_storage::{R2InventoryRequest, R2ObjectStorageConfig};
use foundation_outbox::R2ObjectStorage;
use serde_json::json;

use crate::public_data_control_support::required_env_value;

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
    let objects = report.objects();
    let mut lines = BufWriter::new(
        std::fs::File::create(&output)
            .with_context(|| format!("failed to create {}", output.display()))?,
    );
    serde_json::to_writer(
        &mut lines,
        &json!({
            "prefix": prefix,
            "list_request_count": report.list_request_count(),
            "object_count": objects.len(),
        }),
    )?;
    lines.write_all(b"\n")?;
    for object in objects {
        serde_json::to_writer(
            &mut lines,
            &json!({
                "key": object.key,
                "size_bytes": object.size_bytes,
                "last_modified": object.last_modified,
            }),
        )?;
        lines.write_all(b"\n")?;
    }
    lines
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("failed to write {}", output.display()))?;
    println!(
        "lakehouse-object-listing prefix={prefix} objects={} list_requests={}",
        objects.len(),
        report.list_request_count()
    );
    Ok(())
}
