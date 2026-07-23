//! Contract tests for catalog-driven `hub.go.kr` bulk collection planning.

use collection_application::{
    plan_building_hub_bulk_collection, BuildingHubBulkEndpoint, BuildingHubBulkInventoryFile,
    BuildingHubBulkInventorySelector,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn hub_bulk_collection_plan_matches_catalog_selectors_to_provider_inventory() -> TestResult {
    let endpoints = vec![
        BuildingHubBulkEndpoint {
            endpoint_slug: "hub-building-building_register_main".to_owned(),
            source_slug: "hubgokr__building_register_main".to_owned(),
            source_name: "Building register main bulk".to_owned(),
            dataset_name: "building-register-main".to_owned(),
            base_uri: "https://www.hub.go.kr".to_owned(),
            terms_url: Some(
                "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do".to_owned(),
            ),
            operation: "building_register_main".to_owned(),
            source_acquisition_lane: "bulk_file".to_owned(),
            national_collection_allowed: true,
            selector: BuildingHubBulkInventorySelector {
                task_group_code: "04".to_owned(),
                task_code: "0403".to_owned(),
            },
        },
        BuildingHubBulkEndpoint {
            endpoint_slug: "hub-building-building_register_floor_overview".to_owned(),
            source_slug: "hubgokr__building_register_floor_overview".to_owned(),
            source_name: "Building register floor overview bulk".to_owned(),
            dataset_name: "building-register-floor-overview".to_owned(),
            base_uri: "https://www.hub.go.kr".to_owned(),
            terms_url: Some(
                "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do".to_owned(),
            ),
            operation: "building_register_floor_overview".to_owned(),
            source_acquisition_lane: "bulk_file".to_owned(),
            national_collection_allowed: true,
            selector: BuildingHubBulkInventorySelector {
                task_group_code: "03".to_owned(),
                task_code: "0304".to_owned(),
            },
        },
    ];
    let inventory = vec![
        BuildingHubBulkInventoryFile {
            category_name: "closed_register".to_owned(),
            service_name: "main_title".to_owned(),
            service_period_label: "2026-04".to_owned(),
            provider_file_period: "2026-05".to_owned(),
            task_group_code: "04".to_owned(),
            task_code: "0403".to_owned(),
            provider_file_id: "OPN209912310000000008".to_owned(),
        },
        BuildingHubBulkInventoryFile {
            category_name: "building_register".to_owned(),
            service_name: "floor_overview".to_owned(),
            service_period_label: "2026-04".to_owned(),
            provider_file_period: "2026-05".to_owned(),
            task_group_code: "03".to_owned(),
            task_code: "0304".to_owned(),
            provider_file_id: "OPN209912310000000010".to_owned(),
        },
    ];

    let plan = plan_building_hub_bulk_collection(&endpoints, &inventory)?;

    assert_eq!(plan.jobs.len(), 2);
    assert_eq!(
        plan.jobs[0].endpoint_slug,
        "hub-building-building_register_floor_overview"
    );
    assert_eq!(plan.jobs[0].operation, "building_register_floor_overview");
    assert_eq!(plan.jobs[0].provider_file_id, "OPN209912310000000010");
    assert_eq!(
        plan.jobs[1].endpoint_slug,
        "hub-building-building_register_main"
    );
    assert_eq!(plan.jobs[1].operation, "building_register_main");
    assert_eq!(plan.jobs[1].provider_file_period, "2026-05");
    assert_eq!(plan.jobs[1].provider_file_id, "OPN209912310000000008");
    Ok(())
}

#[test]
fn hub_bulk_collection_plan_rejects_disabled_or_wrong_lane_endpoints() -> TestResult {
    let endpoints = vec![BuildingHubBulkEndpoint {
        endpoint_slug: "hub-building-building_register_main".to_owned(),
        source_slug: "hubgokr__building_register_main".to_owned(),
        source_name: "Building register main bulk".to_owned(),
        dataset_name: "building-register-main".to_owned(),
        base_uri: "https://www.hub.go.kr".to_owned(),
        terms_url: None,
        operation: "building_register_main".to_owned(),
        source_acquisition_lane: "open_api_only".to_owned(),
        national_collection_allowed: true,
        selector: BuildingHubBulkInventorySelector {
            task_group_code: "04".to_owned(),
            task_code: "0403".to_owned(),
        },
    }];
    let inventory = vec![BuildingHubBulkInventoryFile {
        category_name: "closed_register".to_owned(),
        service_name: "main_title".to_owned(),
        service_period_label: "2026-04".to_owned(),
        provider_file_period: "2026-05".to_owned(),
        task_group_code: "04".to_owned(),
        task_code: "0403".to_owned(),
        provider_file_id: "OPN209912310000000008".to_owned(),
    }];

    let error = plan_building_hub_bulk_collection(&endpoints, &inventory)
        .err()
        .ok_or("expected lane validation failure")?;

    assert!(
        error.to_string().contains("source_acquisition_lane"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn hub_bulk_collection_plan_rejects_missing_inventory_match() -> TestResult {
    let endpoints = vec![BuildingHubBulkEndpoint {
        endpoint_slug: "hub-building-building_register_main".to_owned(),
        source_slug: "hubgokr__building_register_main".to_owned(),
        source_name: "Building register main bulk".to_owned(),
        dataset_name: "building-register-main".to_owned(),
        base_uri: "https://www.hub.go.kr".to_owned(),
        terms_url: None,
        operation: "building_register_main".to_owned(),
        source_acquisition_lane: "bulk_file".to_owned(),
        national_collection_allowed: true,
        selector: BuildingHubBulkInventorySelector {
            task_group_code: "04".to_owned(),
            task_code: "0403".to_owned(),
        },
    }];

    let error = plan_building_hub_bulk_collection(&endpoints, &[])
        .err()
        .ok_or("expected missing inventory failure")?;

    assert!(
        error.to_string().contains("no hub.go.kr inventory match"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn hub_bulk_collection_plan_creates_one_job_per_matching_provider_file() -> TestResult {
    let endpoints = vec![BuildingHubBulkEndpoint {
        endpoint_slug: "hub-building-building_register_main".to_owned(),
        source_slug: "hubgokr__building_register_main".to_owned(),
        source_name: "Building register main bulk".to_owned(),
        dataset_name: "building-register-main".to_owned(),
        base_uri: "https://www.hub.go.kr".to_owned(),
        terms_url: None,
        operation: "building_register_main".to_owned(),
        source_acquisition_lane: "bulk_file".to_owned(),
        national_collection_allowed: true,
        selector: BuildingHubBulkInventorySelector {
            task_group_code: "04".to_owned(),
            task_code: "0403".to_owned(),
        },
    }];
    let first = BuildingHubBulkInventoryFile {
        category_name: "closed_register".to_owned(),
        service_name: "main_title".to_owned(),
        service_period_label: "2026-04".to_owned(),
        provider_file_period: "2026-05".to_owned(),
        task_group_code: "04".to_owned(),
        task_code: "0403".to_owned(),
        provider_file_id: "OPN209912310000000008".to_owned(),
    };
    let second = BuildingHubBulkInventoryFile {
        provider_file_id: "OPN209912310000000009".to_owned(),
        provider_file_period: "2026-06".to_owned(),
        ..first.clone()
    };

    let plan = plan_building_hub_bulk_collection(&endpoints, &[first, second])?;

    assert_eq!(plan.jobs.len(), 2);
    assert_eq!(plan.jobs[0].provider_file_period, "2026-05");
    assert_eq!(plan.jobs[1].provider_file_period, "2026-06");
    Ok(())
}
