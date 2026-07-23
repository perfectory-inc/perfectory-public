use collection_infrastructure::BuildingHubBulkInventoryItem;

use super::compile_building_hub_bulk_collection_plan;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn compile_plan_expands_one_catalog_endpoint_to_all_matching_provider_files() -> TestResult {
    let catalog = r#"
    {
      "endpoints": [
        {
          "endpoint_slug": "hub-building-building_register_main",
          "provider": "hub.go.kr",
          "group": "building_hub_bulk",
          "display_name_ko": "building register main",
          "operation": "building_register_main",
          "source_acquisition_lane": "bulk_file",
          "national_collection_allowed": true,
          "provider_inventory_selector": {
            "task_group_code": "03",
            "task_code": "0303"
          },
          "bronze": {
            "source_slug": "hubgokr__building_register_main"
          }
        }
      ]
    }
    "#;
    let inventory = vec![
        inventory_item("2026-04", "OPN209912310000000001"),
        inventory_item("2026-05", "OPN209912310000000004"),
    ];

    let report = compile_building_hub_bulk_collection_plan(
        catalog,
        &inventory,
        "https://www.hub.go.kr",
        Some("https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"),
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.endpoint_count, 1);
    assert_eq!(report.inventory_file_count, 2);
    assert_eq!(report.job_count, 2);
    assert_eq!(report.cataloged_job_count, 2);
    assert_eq!(report.provider_inventory_only_job_count, 0);
    assert_eq!(report.jobs[0].provider_file_period, "2026-04");
    assert_eq!(report.jobs[1].provider_file_period, "2026-05");
    assert_eq!(
        report.jobs[0].source_slug,
        "hubgokr__building_register_main"
    );
    assert_eq!(report.jobs[0].catalog_binding_status, "cataloged_endpoint");
    Ok(())
}

#[test]
fn compile_plan_fails_closed_on_uncataloged_provider_inventory() {
    // ADR 0014 §6/§7 (owner-confirmed): an inventory task that no cataloged endpoint covers has no
    // canonical dataset_slug, so the planner must fail closed rather than mint the old opaque
    // `hub-go-kr-public-bulk-task-*` slug. Register the task in the catalog before collecting it.
    let catalog = r#"
    {
      "endpoints": [
        {
          "endpoint_slug": "hub-building-building_register_main",
          "provider": "hub.go.kr",
          "group": "building_hub_bulk",
          "display_name_ko": "building register main",
          "operation": "building_register_main",
          "source_acquisition_lane": "bulk_file",
          "national_collection_allowed": true,
          "provider_inventory_selector": {
            "task_group_code": "03",
            "task_code": "0303"
          },
          "bronze": {
            "source_slug": "hubgokr__building_register_main"
          }
        }
      ]
    }
    "#;
    let mut permit = inventory_item("2026-05", "OPN209912310000000005");
    permit.category_name = "building-permit".to_owned();
    permit.service_name = "permit basis".to_owned();
    permit.task_group_code = "01".to_owned();
    permit.task_code = "0101".to_owned();
    let inventory = vec![inventory_item("2026-05", "OPN209912310000000004"), permit];

    let error = compile_building_hub_bulk_collection_plan(
        catalog,
        &inventory,
        "https://www.hub.go.kr",
        None,
    )
    .err()
    .expect("expected uncataloged inventory task to fail closed");

    assert!(
        format!("{error:#}").contains("no registered dataset_slug"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn compile_plan_rejects_hub_endpoint_without_provider_inventory_selector() {
    let catalog = r#"
    {
      "endpoints": [
        {
          "endpoint_slug": "hub-building-building_register_main",
          "provider": "hub.go.kr",
          "group": "building_hub_bulk",
          "display_name_ko": "building register main",
          "operation": "building_register_main",
          "source_acquisition_lane": "bulk_file",
          "national_collection_allowed": true,
          "bronze": {
            "source_slug": "hubgokr__building_register_main"
          }
        }
      ]
    }
    "#;

    let error = compile_building_hub_bulk_collection_plan(
        catalog,
        &[inventory_item("2026-05", "OPN209912310000000004")],
        "https://www.hub.go.kr",
        None,
    )
    .err()
    .expect("expected missing selector to fail");

    assert!(
        format!("{error:#}").contains("provider_inventory_selector"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn compile_plan_accepts_utf8_bom_endpoint_catalog() -> TestResult {
    let catalog = "\u{feff}{
      \"endpoints\": [
        {
          \"endpoint_slug\": \"hub-building-building-register-main\",
          \"provider\": \"hub.go.kr\",
          \"group\": \"building_hub_bulk\",
          \"display_name_ko\": \"building register main\",
          \"operation\": \"building_register_main\",
          \"source_acquisition_lane\": \"bulk_file\",
          \"national_collection_allowed\": true,
          \"provider_inventory_selector\": {
            \"task_group_code\": \"03\",
            \"task_code\": \"0303\"
          },
          \"bronze\": {
            \"source_slug\": \"hubgokr__building_register_main\"
          }
        }
      ]
    }";
    let inventory = vec![inventory_item("2026-05", "OPN209912310000000004")];

    let report = compile_building_hub_bulk_collection_plan(
        catalog,
        &inventory,
        "https://www.hub.go.kr",
        None,
    )?;

    assert_eq!(report.status, "ready");
    assert_eq!(report.job_count, 1);
    Ok(())
}

fn inventory_item(period: &str, file_id: &str) -> BuildingHubBulkInventoryItem {
    BuildingHubBulkInventoryItem {
        category_name: "building-register".to_owned(),
        service_name: "main title".to_owned(),
        service_period_label: "2026-04".to_owned(),
        provider_file_period: period.to_owned(),
        task_group_code: "03".to_owned(),
        task_code: "0303".to_owned(),
        file_id: file_id.to_owned(),
    }
}
