use super::compile_vworld_dataset_collection_plan;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn compile_plan_reports_matching_dataset_jobs_and_missing_selector_blockers() -> TestResult {
    let catalog = r#"
    {
      "endpoints": [
        {
          "endpoint_slug": "vworld-dataset-parcel",
          "provider": "VWorld",
          "group": "vworld_dataset",
          "display_name_ko": "VWorld parcel",
          "operation": "parcel",
          "source_acquisition_lane": "provider_dataset_file",
          "national_collection_allowed": true,
          "provider_dataset_selector": {
            "svc_cde": "MK",
            "ds_id": "30563"
          },
          "bronze": {
            "source_slug": "vworldkr__parcel"
          }
        },
        {
          "endpoint_slug": "vworld-dataset-land_register",
          "provider": "VWorld",
          "group": "vworld_dataset",
          "display_name_ko": "VWorld land register",
          "operation": "land_register",
          "source_acquisition_lane": "provider_dataset_file",
          "national_collection_allowed": true,
          "bronze": {
            "source_slug": "vworldkr__land_register"
          }
        }
      ]
    }
    "#;
    let inventory_csv = r#""module","svc_cde","ds_id","file_pages","file_count","large_file_count","listed_gib"
"parcel","MK","30563","3","4","1","0.04"
"#;

    let report = compile_vworld_dataset_collection_plan(
        catalog,
        inventory_csv,
        "https://www.vworld.kr",
        Some("https://www.vworld.kr/dtmk/dtmk_ntads_s001.do"),
    )?;

    assert_eq!(report.status, "blocked");
    assert_eq!(report.endpoint_count, 2);
    assert_eq!(report.inventory_dataset_count, 1);
    assert_eq!(report.job_count, 1);
    assert_eq!(report.blockers.len(), 1);
    assert!(
        report.blockers[0].contains("provider_dataset_selector is required"),
        "unexpected blocker: {:?}",
        report.blockers
    );
    assert_eq!(report.jobs[0].endpoint_slug, "vworld-dataset-parcel");
    assert_eq!(report.jobs[0].file_count, 4);
    Ok(())
}

#[test]
fn compile_plan_reports_missing_provider_inventory_blockers() -> TestResult {
    let catalog = r#"
    {
      "endpoints": [
        {
          "endpoint_slug": "vworld-dataset-parcel",
          "provider": "VWorld",
          "group": "vworld_dataset",
          "display_name_ko": "VWorld parcel",
          "operation": "parcel",
          "source_acquisition_lane": "provider_dataset_file",
          "national_collection_allowed": true,
          "provider_dataset_selector": {
            "svc_cde": "MK",
            "ds_id": "30563"
          },
          "bronze": {
            "source_slug": "vworldkr__parcel"
          }
        }
      ]
    }
    "#;
    let inventory_csv = r#""module","svc_cde","ds_id","file_pages","file_count","large_file_count","listed_gib"
"boundary_sido","MK","30253","1","2","0","0.08"
"#;

    let report = compile_vworld_dataset_collection_plan(
        catalog,
        inventory_csv,
        "https://www.vworld.kr",
        None,
    )?;

    assert_eq!(report.status, "blocked");
    assert_eq!(report.job_count, 0);
    assert_eq!(report.blockers.len(), 1);
    assert!(
        report.blockers[0].contains("no VWorld dataset inventory match"),
        "unexpected blocker: {:?}",
        report.blockers
    );
    Ok(())
}

#[test]
fn compile_plan_accepts_utf8_bom_inventory_summary() -> TestResult {
    let catalog = r#"
    {
      "endpoints": [
        {
          "endpoint_slug": "vworld-dataset-boundary_sido",
          "provider": "VWorld",
          "group": "vworld_dataset",
          "display_name_ko": "VWorld boundary sido",
          "operation": "boundary_sido",
          "source_acquisition_lane": "provider_dataset_file",
          "national_collection_allowed": true,
          "provider_dataset_selector": {
            "svc_cde": "MK",
            "ds_id": "30253"
          },
          "bronze": {
            "source_slug": "vworldkr__boundary_sido"
          }
        }
      ]
    }
    "#;
    let inventory_csv = "\u{feff}\"module\",\"svc_cde\",\"ds_id\",\"file_pages\",\"file_count\",\"large_file_count\",\"listed_gib\"\n\"boundary_sido\",\"MK\",\"30253\",\"1\",\"2\",\"0\",\"0.08\"\n";

    let report = compile_vworld_dataset_collection_plan(
        catalog,
        inventory_csv,
        "https://www.vworld.kr",
        None,
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.job_count, 1);
    Ok(())
}

#[test]
fn compile_plan_accepts_utf8_bom_endpoint_catalog() -> TestResult {
    let catalog = "\u{feff}{
      \"endpoints\": [
        {
          \"endpoint_slug\": \"vworld-dataset-parcel\",
          \"provider\": \"VWorld\",
          \"group\": \"vworld_dataset\",
          \"display_name_ko\": \"VWorld parcel\",
          \"operation\": \"parcel\",
          \"source_acquisition_lane\": \"provider_dataset_file\",
          \"national_collection_allowed\": true,
          \"provider_dataset_selector\": {
            \"svc_cde\": \"MK\",
            \"ds_id\": \"30563\"
          },
          \"bronze\": {
            \"source_slug\": \"vworldkr__parcel\"
          }
        }
      ]
    }";
    let inventory_csv = r#""module","svc_cde","ds_id","file_pages","file_count","large_file_count","listed_gib"
"parcel","MK","30563","3","4","1","0.04"
"#;

    let report = compile_vworld_dataset_collection_plan(
        catalog,
        inventory_csv,
        "https://www.vworld.kr",
        None,
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.job_count, 1);
    Ok(())
}
