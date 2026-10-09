use std::path::PathBuf;

use serde_json::json;
use uuid::Uuid;

use super::{
    compile_provider_acquisition_plan_report, parse_max_files, parse_new_bytes_budget,
    read_blocked_file_rows, ProviderAcquisitionPlanLimits, ProviderBlockedFileRow,
};

fn row(file_no: &str, listed_bytes: u64) -> ProviderBlockedFileRow {
    ProviderBlockedFileRow {
        source_slug: "vworldkr__parcel".to_owned(),
        download_ds_id: "20991231DS99991".to_owned(),
        file_no: file_no.to_owned(),
        provider_file_name: format!("{file_no}.zip"),
        operation: "parcel".to_owned(),
        source_name: "V-World synthetic parcel dataset file".to_owned(),
        dataset_name: "parcel".to_owned(),
        base_ym: Some("2099-12".to_owned()),
        updated_at: Some("2099-12-31".to_owned()),
        listed_bytes,
    }
}

#[test]
fn report_converts_blocked_rows_to_jobs_the_batch_can_run() {
    let report = compile_provider_acquisition_plan_report(
        &[row("9001", 600)],
        ProviderAcquisitionPlanLimits::default(),
    )
    .expect("report");

    assert_eq!(
        report.schema_version,
        "foundation-platform.provider_acquisition_plan.v2"
    );
    assert_eq!(report.status, "ready");
    assert_eq!(report.job_count, 1);
    let job = &report.jobs[0];
    assert_eq!(job.source_slug, "vworldkr__parcel");
    assert_eq!(job.acquisition_method, "raon_kupload_browser");
    assert_eq!(
        job.provider_resource_id,
        "vworld_dataset_file:20991231DS99991:9001"
    );
    assert_eq!(job.provider_file_id, "20991231DS99991-9001");
    assert_eq!(
        job.source_identity_key,
        "provider_file_id=20991231DS99991-9001"
    );
    // What raon_batch.load_selection requires of every job, and the importer's identity.
    assert_eq!(job.operation, "parcel");
    assert_eq!(job.dataset_name, "parcel");
    assert_eq!(job.source_name, "V-World synthetic parcel dataset file");
    assert_eq!(job.provider_file_name, "9001.zip");
    assert_eq!(job.base_ym.as_deref(), Some("2099-12"));
    assert_eq!(job.updated_at.as_deref(), Some("2099-12-31"));
    assert_eq!(job.listed_bytes, 600);
    assert_eq!(report.listed_bytes_total, 600);
}

/// The plan is the RAON batch's selection as written (root ADR-0170): the worker's test reads this
/// same file through `raon_batch.load_selection`, so a field renamed on either side fails a test.
#[test]
fn the_plan_is_the_shape_the_raon_batch_reads() {
    let report = compile_provider_acquisition_plan_report(
        &[row("9001", 600)],
        ProviderAcquisitionPlanLimits::default(),
    )
    .expect("report");
    let handoff: serde_json::Value = serde_json::from_str(include_str!(
        "../../../foundation-provider-acquisition-worker/tests/fixtures/provider-acquisition-plan.v2.json"
    ))
    .expect("handoff fixture");
    assert_eq!(serde_json::to_value(&report).expect("report json"), handoff);
}

#[test]
fn a_run_takes_its_first_files_and_is_refused_whole_above_its_budget() {
    let rows = [row("9001", 600), row("9002", 700), row("9003", 800)];
    let limited = compile_provider_acquisition_plan_report(
        &rows,
        ProviderAcquisitionPlanLimits {
            max_files: Some(1),
            new_bytes_budget: Some(600),
        },
    )
    .expect("limited report");
    assert_eq!(
        limited.status, "ready",
        "the budget prices what this run takes"
    );
    assert_eq!(limited.candidate_count, 3);
    assert_eq!(
        limited
            .jobs
            .iter()
            .map(|job| job.file_no.as_str())
            .collect::<Vec<_>>(),
        ["9001"]
    );

    let at = ProviderAcquisitionPlanLimits {
        max_files: None,
        new_bytes_budget: Some(2100),
    };
    assert_eq!(
        compile_provider_acquisition_plan_report(&rows, at)
            .expect("at budget")
            .status,
        "ready"
    );
    let over = compile_provider_acquisition_plan_report(
        &rows,
        ProviderAcquisitionPlanLimits {
            new_bytes_budget: Some(2099),
            ..at
        },
    )
    .expect("over budget");
    assert_eq!(over.status, "blocked_new_bytes_budget");
    assert_eq!(over.listed_bytes_total, 2100);
    assert_eq!(
        over.job_count, 3,
        "the refused plan still says what it would take"
    );
}

#[test]
fn limits_are_whole_numbers() {
    assert_eq!(parse_new_bytes_budget("0").expect("zero budget"), 0);
    assert!(parse_new_bytes_budget("20GiB").is_err());
    assert_eq!(parse_max_files("1").expect("one file"), 1);
    assert!(parse_max_files("0").is_err());
    assert!(parse_max_files("all").is_err());
}

struct Handoff {
    dir: PathBuf,
}

impl Handoff {
    fn new(selection_archives: serde_json::Value, inventory_files: serde_json::Value) -> Self {
        let dir =
            std::env::temp_dir().join(format!("provider-acquisition-plan-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create handoff dir");
        let inventory = dir.join("vworld-inventory.json");
        std::fs::write(
            &inventory,
            serde_json::to_vec(&json!({"jobs": [{
                "endpoint_slug": "vworld-dataset-parcel",
                "source_slug": "vworldkr__parcel",
                "source_name": "V-World synthetic parcel dataset file",
                "dataset_name": "parcel",
                "operation": "parcel",
                "files": inventory_files,
            }]}))
            .expect("inventory json"),
        )
        .expect("write inventory");
        std::fs::write(
            dir.join("vworld-evidence.json"),
            serde_json::to_vec(&json!({
                "file_inventory_path": inventory.to_string_lossy(),
                "files": [
                    evidence_row("9001", "succeeded"),
                    evidence_row("9002", "provider_acquisition_blocked"),
                ],
                "selection_archives": selection_archives,
            }))
            .expect("evidence json"),
        )
        .expect("write evidence");
        Self { dir }
    }

    fn evidence(&self) -> PathBuf {
        self.dir.join("vworld-evidence.json")
    }
}

impl Drop for Handoff {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn evidence_row(file_no: &str, status: &str) -> serde_json::Value {
    json!({"endpoint_slug": "vworld-dataset-parcel", "source_slug": "vworldkr__parcel",
           "download_ds_id": "20991231DS99991", "file_no": file_no,
           "provider_file_name": format!("{file_no}.zip"), "status": status})
}

fn listed(file_no: &str, size_kib: u64) -> serde_json::Value {
    json!({"download_ds_id": "20991231DS99991", "file_no": file_no, "base_ym": "2099-12",
           "updated_at": "-", "size_kib": size_kib, "download_kind": "selection_archive"})
}

/// Root ADR-0170: the sweep's evidence is the handoff. Blocked files and deferred selection
/// archives are planned with what the inventory the ingest read says of them; a held archive and
/// a downloaded file are not.
#[test]
fn the_sweep_evidence_hands_over_the_deferred_archives_with_their_listing() {
    let handoff = Handoff::new(
        json!([
            evidence_row("9003", "skipped_existing"),
            evidence_row("9004", "deferred_selection_archive")
        ]),
        json!([
            listed("9001", 1),
            listed("9002", 2),
            listed("9003", 600_000),
            listed("9004", 700_000)
        ]),
    );
    let rows = read_blocked_file_rows(&handoff.evidence()).expect("rows");
    assert_eq!(
        rows.iter()
            .map(|row| row.file_no.as_str())
            .collect::<Vec<_>>(),
        ["9002", "9004"]
    );
    let archive = &rows[1];
    assert_eq!(archive.operation, "parcel");
    assert_eq!(archive.listed_bytes, 700_000 * 1024);
    assert_eq!(archive.base_ym.as_deref(), Some("2099-12"));
    assert_eq!(archive.updated_at, None, "the listing's '-' is no date");
}

#[test]
fn a_candidate_missing_from_the_inventory_is_refused() {
    let handoff = Handoff::new(
        json!([evidence_row("9004", "deferred_selection_archive")]),
        json!([listed("9002", 2)]),
    );
    let error = read_blocked_file_rows(&handoff.evidence()).expect_err("unknown file");
    assert!(
        error.to_string().contains("not in the inventory"),
        "{error}"
    );
}
