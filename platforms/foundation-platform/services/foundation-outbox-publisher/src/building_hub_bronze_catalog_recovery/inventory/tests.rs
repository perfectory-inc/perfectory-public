use collection_infrastructure::BuildingHubBulkInventoryItem;

use super::compile_recovery_inventory;

#[test]
fn missing_current_endpoint_file_blocks_only_that_job_and_preserves_other_evidence() {
    let report = compile_recovery_inventory(
        endpoint_catalog(),
        &[
            "hubgokr__building_register_main".to_owned(),
            "hubgokr__building_energy_yearly_electricity".to_owned(),
        ],
        &[inventory_item()],
        "https://www.hub.go.kr",
        Some("https://www.hub.go.kr/terms"),
        "2026-07-14T00:00:00Z",
    )
    .expect("missing inventory evidence must be represented, not abort compilation");

    assert_eq!(report.status, "blocked");
    assert_eq!(report.jobs.len(), 2);
    assert_eq!(report.jobs[0].files.len(), 1);
    assert!(report.jobs[1].files.is_empty());
    assert_eq!(report.blockers.len(), 1);
    assert_eq!(
        report.blockers[0].source_slug,
        "hubgokr__building_energy_yearly_electricity"
    );
    assert_eq!(
        report.blockers[0].reason,
        "missing_provider_inventory_match"
    );
}

#[test]
fn duplicate_requested_source_is_rejected() {
    let error = compile_recovery_inventory(
        endpoint_catalog(),
        &[
            "hubgokr__building_register_main".to_owned(),
            "hubgokr__building_register_main".to_owned(),
        ],
        &[inventory_item()],
        "https://www.hub.go.kr",
        None,
        "2026-07-14T00:00:00Z",
    )
    .expect_err("recovery scope must be unambiguous");

    assert!(error.to_string().contains("duplicate"));
}

fn endpoint_catalog() -> &'static str {
    r#"{
      "schema_version": "foundation-platform.public_source_endpoint_catalog.v1",
      "status": "ready",
      "endpoints": [
        {
          "endpoint_slug": "hub-building-building_register_main",
          "provider": "hub.go.kr",
          "group": "building_hub_bulk",
          "display_name_ko": "Building register main",
          "dataset_slug": "building_register_main",
          "operation": "building_register_main",
          "source_acquisition_lane": "bulk_file",
          "national_collection_allowed": true,
          "provider_inventory_selector": {"task_group_code": "03", "task_code": "0303"},
          "auth_kind": "provider_managed_credential",
          "bronze": {"source_slug": "hubgokr__building_register_main"}
        },
        {
          "endpoint_slug": "hub-building-building_energy_yearly_electricity",
          "provider": "hub.go.kr",
          "group": "building_hub_bulk",
          "display_name_ko": "Building energy yearly electricity",
          "dataset_slug": "building_energy_yearly_electricity",
          "operation": "building_energy_yearly_electricity",
          "source_acquisition_lane": "bulk_file",
          "national_collection_allowed": true,
          "provider_inventory_selector": {"task_group_code": "05", "task_code": "0501"},
          "auth_kind": "provider_managed_credential",
          "bronze": {"source_slug": "hubgokr__building_energy_yearly_electricity"}
        },
        {
          "endpoint_slug": "unrelated-provider-without-dataset-slug",
          "provider": "other.example",
          "group": "other_group",
          "display_name_ko": "Unrelated",
          "operation": "unrelated",
          "source_acquisition_lane": "api",
          "national_collection_allowed": false,
          "provider_inventory_selector": null,
          "auth_kind": "none",
          "bronze": {"source_slug": "other__unrelated"}
        }
      ]
    }"#
}

fn inventory_item() -> BuildingHubBulkInventoryItem {
    BuildingHubBulkInventoryItem {
        category_name: "Building".to_owned(),
        service_name: "Building register main".to_owned(),
        service_period_label: "2026-06".to_owned(),
        provider_file_period: "2026-06".to_owned(),
        task_group_code: "03".to_owned(),
        task_code: "0303".to_owned(),
        file_id: "OPN209912310000000012".to_owned(),
    }
}
