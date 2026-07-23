use collection_infrastructure::{VWorldDatasetFileInventoryItem, VWorldDatasetFileKind};

use super::compile_vworld_dataset_file_inventory_report;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn compile_report_preserves_file_level_inventory_and_aggregate_counts() -> TestResult {
    let plan_json = r#"
    {
      "schema_version": "foundation-platform.vworld_dataset_collection_plan.v1",
      "status": "ready",
      "job_count": 1,
      "jobs": [
        {
          "endpoint_slug": "vworld-dataset-parcel",
          "source_slug": "vworldkr__parcel",
          "source_name": "VWorld parcel",
          "dataset_name": "parcel",
          "base_uri": "https://www.vworld.kr",
          "terms_url": "https://www.vworld.kr/dtmk/dtmk_ntads_s001.do",
          "operation": "parcel",
          "provider_module": "parcel",
          "svc_cde": "MK",
          "ds_id": "30563",
          "file_pages": 1,
          "file_count": 2,
          "large_file_count": 1,
          "listed_gib": "1.50"
        }
      ]
    }
    "#;
    let files_by_job = vec![(
        "vworld-dataset-parcel".to_owned(),
        vec![
            test_file(
                "MK",
                "30563",
                "132",
                VWorldDatasetFileKind::SingleResourceFile,
            ),
            test_file(
                "MK",
                "30563",
                "999",
                VWorldDatasetFileKind::SelectionArchive,
            ),
        ],
    )];

    let selected_endpoint_slugs = vec!["vworld-dataset-parcel".to_owned()];
    let report = compile_vworld_dataset_file_inventory_report(
        plan_json,
        "target/plan.json",
        &selected_endpoint_slugs,
        files_by_job,
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.plan_job_count, 1);
    assert_eq!(report.inventory_job_count, 1);
    assert_eq!(report.expected_file_count, 2);
    assert_eq!(report.discovered_file_count, 2);
    assert_eq!(report.single_resource_file_count, 1);
    assert_eq!(report.selection_archive_file_count, 1);
    assert_eq!(report.blockers, Vec::<String>::new());
    assert_eq!(report.jobs[0].endpoint_slug, "vworld-dataset-parcel");
    assert_eq!(report.jobs[0].base_uri, "https://www.vworld.kr");
    assert_eq!(
        report.jobs[0].terms_url.as_deref(),
        Some("https://www.vworld.kr/dtmk/dtmk_ntads_s001.do")
    );
    assert_eq!(report.jobs[0].files.len(), 2);
    assert_eq!(report.jobs[0].files[0].file_no, "132");
    Ok(())
}

#[test]
fn compile_report_records_provider_file_count_drift_without_blocking() -> TestResult {
    let plan_json = r#"
    {
      "schema_version": "foundation-platform.vworld_dataset_collection_plan.v1",
      "status": "ready",
      "job_count": 1,
      "jobs": [
        {
          "endpoint_slug": "vworld-dataset-parcel",
          "source_slug": "vworldkr__parcel",
          "source_name": "VWorld parcel",
          "dataset_name": "parcel",
          "base_uri": "https://www.vworld.kr",
          "terms_url": null,
          "operation": "parcel",
          "provider_module": "parcel",
          "svc_cde": "MK",
          "ds_id": "30563",
          "file_pages": 1,
          "file_count": 2,
          "large_file_count": 1,
          "listed_gib": "1.50"
        }
      ]
    }
    "#;
    let files_by_job = vec![(
        "vworld-dataset-parcel".to_owned(),
        vec![test_file(
            "MK",
            "30563",
            "132",
            VWorldDatasetFileKind::SingleResourceFile,
        )],
    )];

    let selected_endpoint_slugs = vec!["vworld-dataset-parcel".to_owned()];
    let report = compile_vworld_dataset_file_inventory_report(
        plan_json,
        "target/plan.json",
        &selected_endpoint_slugs,
        files_by_job,
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.discovered_file_count, 1);
    assert!(report.blockers.is_empty());
    assert_eq!(report.count_drift.len(), 1);
    assert_eq!(report.count_drift[0].endpoint_slug, "vworld-dataset-parcel");
    assert_eq!(report.count_drift[0].expected_file_count, 2);
    assert_eq!(report.count_drift[0].discovered_file_count, 1);
    assert_eq!(report.count_drift[0].expected_selection_archive_count, 1);
    assert_eq!(report.count_drift[0].selection_archive_count, 0);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("expected 2 files, discovered 1")),
        "unexpected warnings: {:?}",
        report.warnings
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("expected 1 large files, discovered 0")),
        "unexpected warnings: {:?}",
        report.warnings
    );
    Ok(())
}

#[test]
fn compile_report_blocks_when_provider_file_selector_does_not_match_plan() -> TestResult {
    let plan_json = r#"
    {
      "schema_version": "foundation-platform.vworld_dataset_collection_plan.v1",
      "status": "ready",
      "job_count": 1,
      "jobs": [
        {
          "endpoint_slug": "vworld-dataset-parcel",
          "source_slug": "vworldkr__parcel",
          "source_name": "VWorld parcel",
          "dataset_name": "parcel",
          "base_uri": "https://www.vworld.kr",
          "terms_url": null,
          "operation": "parcel",
          "provider_module": "parcel",
          "svc_cde": "MK",
          "ds_id": "30563",
          "file_pages": 1,
          "file_count": 1,
          "large_file_count": 0,
          "listed_gib": "1.50"
        }
      ]
    }
    "#;
    let files_by_job = vec![(
        "vworld-dataset-parcel".to_owned(),
        vec![test_file(
            "NA",
            "99999",
            "132",
            VWorldDatasetFileKind::SingleResourceFile,
        )],
    )];

    let selected_endpoint_slugs = vec!["vworld-dataset-parcel".to_owned()];
    let report = compile_vworld_dataset_file_inventory_report(
        plan_json,
        "target/plan.json",
        &selected_endpoint_slugs,
        files_by_job,
    )?;

    assert_eq!(report.status, "blocked");
    assert_eq!(report.blockers.len(), 1);
    assert!(
        report.blockers[0].contains("selector mismatch"),
        "unexpected blockers: {:?}",
        report.blockers
    );
    Ok(())
}

#[test]
fn compile_report_validates_only_selected_jobs_for_partial_inventory_runs() -> TestResult {
    let plan_json = r#"
    {
      "schema_version": "foundation-platform.vworld_dataset_collection_plan.v1",
      "status": "ready",
      "job_count": 2,
      "jobs": [
        {
          "endpoint_slug": "vworld-dataset-boundary_sido",
          "source_slug": "vworldkr__boundary_sido",
          "source_name": "VWorld boundary sido",
          "dataset_name": "boundary_sido",
          "base_uri": "https://www.vworld.kr",
          "terms_url": null,
          "operation": "boundary_sido",
          "provider_module": "boundary_sido",
          "svc_cde": "MK",
          "ds_id": "30253",
          "file_pages": 1,
          "file_count": 1,
          "large_file_count": 0,
          "listed_gib": "0.01"
        },
        {
          "endpoint_slug": "vworld-dataset-parcel",
          "source_slug": "vworldkr__parcel",
          "source_name": "VWorld parcel",
          "dataset_name": "parcel",
          "base_uri": "https://www.vworld.kr",
          "terms_url": null,
          "operation": "parcel",
          "provider_module": "parcel",
          "svc_cde": "MK",
          "ds_id": "30563",
          "file_pages": 1,
          "file_count": 1,
          "large_file_count": 0,
          "listed_gib": "1.50"
        }
      ]
    }
    "#;
    let selected_endpoint_slugs = vec!["vworld-dataset-boundary_sido".to_owned()];
    let files_by_job = vec![(
        "vworld-dataset-boundary_sido".to_owned(),
        vec![test_file(
            "MK",
            "30253",
            "1",
            VWorldDatasetFileKind::SingleResourceFile,
        )],
    )];

    let report = compile_vworld_dataset_file_inventory_report(
        plan_json,
        "target/plan.json",
        &selected_endpoint_slugs,
        files_by_job,
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.plan_job_count, 2);
    assert_eq!(report.inventory_job_count, 1);
    assert_eq!(report.expected_file_count, 1);
    assert_eq!(report.discovered_file_count, 1);
    assert_eq!(report.jobs[0].endpoint_slug, "vworld-dataset-boundary_sido");
    Ok(())
}

fn test_file(
    svc_cde: &str,
    ds_id: &str,
    file_no: &str,
    download_kind: VWorldDatasetFileKind,
) -> VWorldDatasetFileInventoryItem {
    VWorldDatasetFileInventoryItem {
        svc_cde: svc_cde.to_owned(),
        ds_id: ds_id.to_owned(),
        download_ds_id: ds_id.to_owned(),
        file_no: file_no.to_owned(),
        provider_file_name: format!("{file_no}.zip"),
        file_format: "SHP".to_owned(),
        size_mb_label: "1".to_owned(),
        size_kib: 1_024,
        provider_file_kind: "data".to_owned(),
        base_ym: "2026-05".to_owned(),
        updated_at: "2026-05-13".to_owned(),
        download_kind,
    }
}
