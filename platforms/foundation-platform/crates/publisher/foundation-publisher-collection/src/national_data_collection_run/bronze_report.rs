use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::bail;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};

#[derive(Serialize)]
pub(super) struct BronzeRunReport {
    /// Ingestion run id. After ADR 0019 the run id is no longer encoded in the Bronze object
    /// key, so this local-mirror proof cannot recover it from the path; run lineage lives in the
    /// `bronze_object` row + run manifest (control plane). `None` here means "not recoverable from
    /// the local object mirror".
    pub(super) run_id: Option<String>,
    pub(super) object_count: i64,
    pub(super) total_size_bytes: i64,
    pub(super) logical_record_count: i64,
    pub(super) objects: Vec<JsonValue>,
}

pub(super) fn bronze_run_report(
    local_root: &Path,
    started_at: SystemTime,
    source_slug: &str,
    provider_label: &str,
) -> anyhow::Result<BronzeRunReport> {
    let bronze_root = local_root.join("bronze");
    if !bronze_root.is_dir() {
        bail!("local Bronze root does not contain a bronze/ directory");
    }
    let source_root = bronze_root.join(format!("source={source_slug}"));
    if !source_root.is_dir() {
        bail!("local Bronze run directory was not found for source={source_slug}");
    }

    // The readable Bronze key (ADR 0019) no longer encodes run_id in the path, so the
    // run is identified by the write time window, not a `run_id=` directory. Report every object
    // under `source={slug}/` written at/after the child run started.
    let mut candidates = Vec::new();
    collect_files(&source_root, &mut candidates)?;
    let mut files: Vec<PathBuf> = candidates
        .into_iter()
        .filter(|path| {
            fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .map(|modified| modified >= started_at - Duration::from_secs(1))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    if files.is_empty() {
        bail!("local Bronze run directory contains no objects written by this run for source={source_slug}");
    }
    let mut objects = Vec::new();
    let mut total_size_bytes = 0_i64;
    let mut total_logical_record_count = 0_i64;
    for path in files {
        let bytes = fs::read(&path)?;
        if bytes.is_empty() {
            bail!(
                "{provider_label} Bronze object was empty: {}",
                bronze_object_key(local_root, &path)?
            );
        }
        let record_count = count_logical_records(&bytes)?;
        if record_count < 1 {
            bail!(
                "{provider_label} Bronze object contains no logical records: {}",
                bronze_object_key(local_root, &path)?
            );
        }
        total_size_bytes += i64::try_from(bytes.len()).unwrap_or(i64::MAX);
        total_logical_record_count += record_count;
        objects.push(json!({
            "object_key": bronze_object_key(local_root, &path)?,
            "checksum_sha256": sha256_bytes(&bytes),
            "size_bytes": bytes.len(),
            "logical_record_count": record_count
        }));
    }
    Ok(BronzeRunReport {
        run_id: None,
        object_count: i64::try_from(objects.len()).unwrap_or(i64::MAX),
        total_size_bytes,
        logical_record_count: total_logical_record_count,
        objects,
    })
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files)?;
        } else if path.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn bronze_object_key(local_root: &Path, path: &Path) -> anyhow::Result<String> {
    Ok(path
        .strip_prefix(local_root)?
        .to_string_lossy()
        .replace('\\', "/"))
}

fn count_logical_records(bytes: &[u8]) -> anyhow::Result<i64> {
    let json: JsonValue = serde_json::from_slice(strip_utf8_bom(bytes))?;
    let items = json
        .pointer("/response/body/items/item")
        .map(array_or_one_len)
        .unwrap_or(0);
    if items > 0 {
        return Ok(items);
    }
    Ok(json
        .pointer("/response/result/featureCollection/features")
        .and_then(JsonValue::as_array)
        .map(|items| i64::try_from(items.len()).unwrap_or(i64::MAX))
        .unwrap_or(0))
}

fn array_or_one_len(value: &JsonValue) -> i64 {
    value
        .as_array()
        .map(|items| i64::try_from(items.len()).unwrap_or(i64::MAX))
        .unwrap_or(1)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

#[cfg(test)]
mod tests {
    use super::count_logical_records;

    #[test]
    fn counts_data_go_item_arrays() {
        let bytes = br#"{"response":{"body":{"items":{"item":[{"a":1},{"a":2}]}}}}"#;

        let count = count_logical_records(bytes).expect("logical records should be counted");

        assert_eq!(count, 2);
    }

    #[test]
    fn counts_vworld_feature_arrays() {
        let bytes = br#"{"response":{"result":{"featureCollection":{"features":[{}, {}, {}]}}}}"#;

        let count = count_logical_records(bytes).expect("logical records should be counted");

        assert_eq!(count, 3);
    }
}
