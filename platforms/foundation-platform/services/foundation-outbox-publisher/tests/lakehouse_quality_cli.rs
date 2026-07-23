//! Process-level compatibility tests for the Lakehouse quality CLI adapter.

use std::{fs, path::PathBuf, process::Command};

use serde_json::{json, Value};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BINARY: &str = env!("CARGO_BIN_EXE_foundation-outbox-publisher");

#[test]
fn passing_quality_evaluation_preserves_stdout_and_exit_code() -> TestResult {
    let fixture = CliFixture::new(&summary_json())?;

    let output = fixture.run()?;

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        String::from_utf8(output.stdout)?.trim(),
        "lakehouse-quality-evaluation-ok table=silver.industrial_complexes rules=6"
    );
    assert!(String::from_utf8(output.stderr)?.trim().is_empty());
    Ok(())
}

#[test]
fn blocking_quality_evaluation_preserves_stderr_and_nonzero_exit() -> TestResult {
    let mut summary = summary_json();
    summary["quality_metrics"]["complex_name__empty_count"] = json!(2);
    let fixture = CliFixture::new(&summary)?;

    let output = fixture.run()?;

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains(
        "lakehouse quality rule failed: rule=silver-industrial-complexes-complex-name-empty metric=complex_name__empty_count actual=2 threshold={\"kind\":\"equals\",\"value\":0}"
    ));
    assert!(stderr.contains("lakehouse quality evaluation blocked"));
    Ok(())
}

struct CliFixture {
    root: PathBuf,
    summary_path: PathBuf,
    rules_path: PathBuf,
}

impl CliFixture {
    fn new(summary: &Value) -> TestResult<Self> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .and_then(|path| path.parent())
            .ok_or_else(|| std::io::Error::other("service must be inside workspace services"))?;
        let root = repo_root
            .join("target")
            .join(format!("lakehouse-quality-cli-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let summary_path = root.join("summary.json");
        fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)?;
        let rules_path = repo_root
            .join("docs")
            .join("data-quality")
            .join("lakehouse-quality-rules.v1.example.json");
        Ok(Self {
            root,
            summary_path,
            rules_path,
        })
    }

    fn run(&self) -> TestResult<std::process::Output> {
        Command::new(BINARY)
            .arg("evaluate-lakehouse-quality-rules")
            .env("FOUNDATION_PLATFORM_REPO_ROOT", &self.root)
            .env(
                "FOUNDATION_PLATFORM_LAKEHOUSE_QUALITY_EVALUATION_SUMMARY_PATH",
                &self.summary_path,
            )
            .env(
                "FOUNDATION_PLATFORM_LAKEHOUSE_QUALITY_EVALUATION_RULES_PATH",
                &self.rules_path,
            )
            .output()
            .map_err(Into::into)
    }
}

impl Drop for CliFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn summary_json() -> Value {
    json!({
        "schema_version": "foundation-platform.spark_run_summary.v1",
        "job_name": "industrial_complex_bronze_to_silver",
        "contract": "silver.industrial_complexes",
        "created_at_utc": "2026-05-18T12:00:00Z",
        "input": {"kind": "silver_handoff_jsonl", "path": "target/input.jsonl"},
        "target": {"kind": "parquet", "path": "target/output.parquet"},
        "write_mode": "parquet",
        "write_disposition": "parquet_overwrite",
        "row_count": 2,
        "persisted_row_count": 2,
        "quality_metrics": {
            "row_count": 2,
            "complex_id__null_count": 0,
            "complex_name__empty_count": 0,
            "invalid_official_area_count": 0,
            "invalid_checksum_count": 0
        },
        "column_count": 1,
        "columns": ["complex_id"],
        "required_columns": ["complex_id"],
        "source_snapshot_count": 1,
        "source_snapshot_ids": ["snapshot-1"],
        "source_snapshot_truncated": false
    })
}
