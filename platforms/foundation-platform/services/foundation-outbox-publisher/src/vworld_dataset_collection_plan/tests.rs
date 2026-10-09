use super::{compile_vworld_dataset_collection_plan, inventory_summary_path};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn compile_plan_reports_matching_dataset_jobs_and_missing_selector_blockers() -> TestResult {
    let catalog = r#"
    {
      "endpoints": [
        {
          "endpoint_slug": "vworld-dataset-parcel",
          "provider": "vworld.kr",
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
          "provider": "vworld.kr",
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
        Some(inventory_csv),
        None,
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
          "provider": "vworld.kr",
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
        Some(inventory_csv),
        None,
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
          "provider": "vworld.kr",
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
        Some(inventory_csv),
        None,
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
          \"provider\": \"vworld.kr\",
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
        Some(inventory_csv),
        None,
        "https://www.vworld.kr",
        None,
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.job_count, 1);
    Ok(())
}

/// Three synthetic endpoints: two swept daily, one not, and the declared collection.
const DAILY_CATALOG: &str = r#"
{
  "daily_collections": {
    "source_sweep": { "new_bytes_budget": 1024 }
  },
  "endpoints": [
    {
      "endpoint_slug": "vworld-dataset-synthetic_a",
      "group": "vworld_dataset",
      "display_name_ko": "synthetic a",
      "operation": "synthetic_a",
      "source_acquisition_lane": "provider_dataset_file",
      "national_collection_allowed": true,
      "provider_dataset_selector": { "svc_cde": "NA", "ds_id": "9991" },
      "daily_collection": "source_sweep",
      "bronze": { "source_slug": "vworldkr__synthetic_a" }
    },
    {
      "endpoint_slug": "vworld-dataset-synthetic_b",
      "group": "vworld_dataset",
      "display_name_ko": "synthetic b",
      "operation": "synthetic_b",
      "source_acquisition_lane": "provider_dataset_file",
      "national_collection_allowed": true,
      "provider_dataset_selector": { "svc_cde": "MK", "ds_id": "99992" },
      "daily_collection": "source_sweep",
      "bronze": { "source_slug": "vworldkr__synthetic_b" }
    },
    {
      "endpoint_slug": "vworld-dataset-synthetic_c",
      "group": "vworld_dataset",
      "display_name_ko": "synthetic c",
      "operation": "synthetic_c",
      "source_acquisition_lane": "provider_dataset_file",
      "national_collection_allowed": true,
      "provider_dataset_selector": { "svc_cde": "MK", "ds_id": "99993" },
      "bronze": { "source_slug": "vworldkr__synthetic_c" }
    }
  ]
}
"#;

/// The sweep plans exactly the endpoints the catalog marks, with no summary file, and carries the
/// collection's budget; the jobs say their counts are unknown rather than zero.
#[test]
fn a_daily_collection_plans_only_its_marked_endpoints_without_a_summary() -> TestResult {
    let report = compile_vworld_dataset_collection_plan(
        DAILY_CATALOG,
        None,
        Some("source_sweep"),
        "https://www.vworld.kr",
        None,
    )?;

    assert_eq!(report.status, "ready", "blockers: {:?}", report.blockers);
    let slugs = report
        .jobs
        .iter()
        .map(|job| job.endpoint_slug.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        slugs,
        ["vworld-dataset-synthetic_a", "vworld-dataset-synthetic_b"]
    );
    assert_eq!(report.endpoint_count, 2);
    assert_eq!(report.inventory_dataset_count, 0);
    assert_eq!(report.new_bytes_budget, Some(1024));
    assert_eq!(report.daily_collection.as_deref(), Some("source_sweep"));
    assert!(report.jobs.iter().all(|job| !job.expected_counts_known));
    assert_eq!(report.jobs[1].svc_cde, "MK");
    assert_eq!(report.jobs[1].ds_id, "99992");
    Ok(())
}

/// Without a selector every endpoint is planned, as before.
#[test]
fn without_a_daily_collection_every_endpoint_is_planned() -> TestResult {
    let report = compile_vworld_dataset_collection_plan(
        DAILY_CATALOG,
        None,
        None,
        "https://www.vworld.kr",
        None,
    )?;
    assert_eq!(report.job_count, 3);
    assert_eq!(report.new_bytes_budget, None);
    Ok(())
}

/// A collection nobody declared is an operator typo, not an empty sweep.
#[test]
fn an_undeclared_daily_collection_is_refused() {
    let result = compile_vworld_dataset_collection_plan(
        DAILY_CATALOG,
        None,
        Some("source_swep"),
        "https://www.vworld.kr",
        None,
    );
    assert!(result.is_err(), "an undeclared collection must be refused");
}

/// An endpoint that names a collection the catalog does not declare blocks the plan: otherwise a
/// misspelt field would silently drop the dataset from the sweep.
#[test]
fn an_endpoint_naming_an_undeclared_collection_blocks_the_plan() -> TestResult {
    let catalog = DAILY_CATALOG.replacen(
        r#""daily_collection": "source_sweep",
      "bronze": { "source_slug": "vworldkr__synthetic_b" }"#,
        r#""daily_collection": "source_swep",
      "bronze": { "source_slug": "vworldkr__synthetic_b" }"#,
        1,
    );
    assert_ne!(catalog, DAILY_CATALOG, "the fixture edit must apply");
    let report = compile_vworld_dataset_collection_plan(
        &catalog,
        None,
        Some("source_sweep"),
        "https://www.vworld.kr",
        None,
    )?;
    assert_eq!(report.status, "blocked");
    assert!(
        report.blockers[0].contains("source_swep"),
        "unexpected blockers: {:?}",
        report.blockers
    );
    Ok(())
}

/// A summary given explicitly is still read and its counts become expectations.
#[test]
fn a_given_summary_still_sets_expected_counts_for_a_daily_collection() -> TestResult {
    let summary = r#""module","svc_cde","ds_id","file_pages","file_count","large_file_count","listed_gib"
"synthetic_a","NA","9991","1","3","0","0.01"
"synthetic_b","MK","99992","1","2","0","0.01"
"#;
    let report = compile_vworld_dataset_collection_plan(
        DAILY_CATALOG,
        Some(summary),
        Some("source_sweep"),
        "https://www.vworld.kr",
        None,
    )?;
    assert_eq!(report.status, "ready");
    assert!(report.jobs.iter().all(|job| job.expected_counts_known));
    assert_eq!(report.jobs[0].file_count, 3);
    Ok(())
}

#[test]
fn the_summary_is_optional_only_for_a_daily_collection() {
    assert_eq!(inventory_summary_path(None, Some("source_sweep")), None);
    assert!(inventory_summary_path(None, None).is_some());
    assert_eq!(
        inventory_summary_path(Some("x.csv".to_owned()), Some("source_sweep")),
        Some("x.csv".into())
    );
}
