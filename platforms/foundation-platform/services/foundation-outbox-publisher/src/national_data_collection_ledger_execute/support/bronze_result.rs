use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::bail;
use serde_json::Value as JsonValue;

use super::{
    safe_runner_error_message, sha256_hex, string_prop, strip_utf8_bom, u64_prop, value_at,
};

pub(in crate::national_data_collection_ledger_execute) struct BronzeResult {
    pub(in crate::national_data_collection_ledger_execute) object_key: String,
    pub(in crate::national_data_collection_ledger_execute) record_count: u64,
    pub(in crate::national_data_collection_ledger_execute) request_count: u64,
    pub(in crate::national_data_collection_ledger_execute) size_bytes: u64,
    pub(in crate::national_data_collection_ledger_execute) checksum_sha256: String,
}

pub(in crate::national_data_collection_ledger_execute) fn read_local_bronze_result(
    local_root: &Path,
    started_at: SystemTime,
    source_slug: &str,
    request_count: u64,
) -> anyhow::Result<BronzeResult> {
    let object = find_latest_bronze_object(local_root, started_at, source_slug)?;
    let bytes = fs::read(&object.path)?;
    Ok(BronzeResult {
        object_key: object.object_key,
        record_count: logical_record_count(&bytes)?,
        request_count,
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        checksum_sha256: sha256_hex(&bytes),
    })
}

struct LocalBronzeObject {
    path: PathBuf,
    object_key: String,
}

fn find_latest_bronze_object(
    local_root: &Path,
    started_at: SystemTime,
    source_slug: &str,
) -> anyhow::Result<LocalBronzeObject> {
    let bronze_root = local_root.join("bronze");
    if !bronze_root.is_dir() {
        bail!("local Bronze root does not contain a bronze/ directory");
    }
    let threshold = started_at
        .checked_sub(Duration::from_secs(1))
        .unwrap_or(started_at);
    let mut candidates = Vec::new();
    collect_bronze_candidates(&bronze_root, source_slug, threshold, &mut candidates)?;
    candidates.sort_by_key(|(modified, _)| *modified);
    let Some((_, path)) = candidates.pop() else {
        bail!("local Bronze object file written by this job was not found");
    };
    let object_key = path
        .strip_prefix(local_root)
        .unwrap_or(&path)
        .to_string_lossy()
        .replace('\\', "/");
    Ok(LocalBronzeObject { path, object_key })
}

fn collect_bronze_candidates(
    dir: &Path,
    source_slug: &str,
    threshold: SystemTime,
    candidates: &mut Vec<(SystemTime, PathBuf)>,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_bronze_candidates(&path, source_slug, threshold, candidates)?;
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        if modified >= threshold
            && path
                .to_string_lossy()
                .replace('\\', "/")
                .contains(&format!("source={source_slug}"))
        {
            candidates.push((modified, path));
        }
    }
    Ok(())
}

pub(in crate::national_data_collection_ledger_execute) fn parse_r2_run_summary(
    output: &[String],
) -> anyhow::Result<BronzeResult> {
    for line in output.iter().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() || !trimmed.starts_with('{') {
            continue;
        }
        let Ok(json) = serde_json::from_str::<JsonValue>(trimmed) else {
            continue;
        };
        let fields = json.get("fields").unwrap_or(&json);
        let object_key = parse_optional_object_key(&string_prop(fields, "last_object_key"));
        let record_count = u64_prop(fields, "logical_record_count", u64::MAX).min(u64_prop(
            fields,
            "logical_records_seen",
            u64::MAX,
        ));
        let objects_written = u64_prop(fields, "objects_written", 0);
        if !object_key.is_empty() && record_count != u64::MAX && objects_written > 0 {
            return Ok(BronzeResult {
                object_key,
                record_count,
                request_count: objects_written,
                size_bytes: 0,
                // The child ingest echoes the last object's producer-computed sha256 in its run
                // summary (Slice 2d); empty only if an older child omitted it (then re-derivable
                // from bronze_object, which always has it).
                checksum_sha256: string_prop(fields, "last_object_checksum_sha256"),
            });
        }
    }
    bail!(
        "runner output did not include parseable Bronze write summary; output_count={}; sample={}",
        output.len(),
        safe_runner_error_message(output)
    )
}

fn parse_optional_object_key(value: &str) -> String {
    if value.starts_with("bronze/") {
        return value.to_owned();
    }
    value
        .strip_prefix("Some(\"")
        .and_then(|rest| rest.strip_suffix("\")"))
        .unwrap_or_default()
        .to_owned()
}

fn logical_record_count(bytes: &[u8]) -> anyhow::Result<u64> {
    let json: JsonValue = serde_json::from_slice(strip_utf8_bom(bytes))?;
    for path in [
        &["response", "body", "items", "item"][..],
        &["response", "result", "featureCollection", "features"][..],
        &["ladfrlVOList", "ladfrlVOList"][..],
    ] {
        if let Some(value) = value_at(&json, path) {
            return Ok(count_json_records(value));
        }
    }
    Ok(0)
}

fn count_json_records(value: &JsonValue) -> u64 {
    match value {
        JsonValue::Array(items) => u64::try_from(items.len()).unwrap_or(u64::MAX),
        JsonValue::Object(_) => 1,
        JsonValue::Null => 0,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_r2_run_summary;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn parse_r2_run_summary_reads_last_object_checksum() -> TestResult {
        // Slice 2d: the child echoes the last object's sha256 in its run summary; the parent reads it.
        let checksum = "a".repeat(64);
        let line = format!(
            "{{\"last_object_key\":\"bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json\",\
             \"logical_record_count\":5,\"objects_written\":1,\
             \"last_object_checksum_sha256\":\"{checksum}\"}}"
        );
        let result = parse_r2_run_summary(&[line])?;
        assert_eq!(result.checksum_sha256, checksum);
        assert_eq!(
            result.object_key,
            "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json"
        );
        assert_eq!(result.request_count, 1);
        assert_eq!(result.record_count, 5);
        Ok(())
    }

    #[test]
    fn parse_r2_run_summary_tolerates_missing_checksum() -> TestResult {
        // Backward-compatible: an older child without the field still parses (empty, re-derivable).
        let line =
            "{\"last_object_key\":\"bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json\",\
                     \"logical_record_count\":5,\"objects_written\":1}"
                .to_owned();
        let result = parse_r2_run_summary(&[line])?;
        assert!(result.checksum_sha256.is_empty());
        Ok(())
    }
}
