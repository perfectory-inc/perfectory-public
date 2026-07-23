//! Contract tests for `VWorld` provider dataset-file collection planning.

use collection_application::{
    plan_vworld_dataset_collection, VWorldDatasetCollectionEndpoint, VWorldDatasetInventoryDataset,
    VWorldDatasetInventorySelector,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn planner_matches_catalog_selectors_to_provider_inventory_summary() -> TestResult {
    let endpoints = vec![VWorldDatasetCollectionEndpoint {
        endpoint_slug: "vworld-dataset-parcel".to_owned(),
        source_slug: "vworldkr__parcel".to_owned(),
        source_name: "VWorld 필지".to_owned(),
        dataset_name: "parcel".to_owned(),
        base_uri: "https://www.vworld.kr".to_owned(),
        terms_url: Some("https://www.vworld.kr/dtmk/dtmk_ntads_s001.do".to_owned()),
        operation: "parcel".to_owned(),
        source_acquisition_lane: "provider_dataset_file".to_owned(),
        national_collection_allowed: true,
        selector: VWorldDatasetInventorySelector {
            svc_cde: "MK".to_owned(),
            ds_id: "30563".to_owned(),
        },
    }];
    let inventory = vec![VWorldDatasetInventoryDataset {
        module: "parcel".to_owned(),
        svc_cde: "MK".to_owned(),
        ds_id: "30563".to_owned(),
        file_pages: 3,
        file_count: 4,
        large_file_count: 1,
        listed_gib: "0.04".to_owned(),
    }];

    let plan = plan_vworld_dataset_collection(&endpoints, &inventory)?;

    assert_eq!(plan.jobs.len(), 1);
    assert_eq!(plan.jobs[0].endpoint_slug, "vworld-dataset-parcel");
    assert_eq!(plan.jobs[0].source_slug, "vworldkr__parcel");
    assert_eq!(plan.jobs[0].svc_cde, "MK");
    assert_eq!(plan.jobs[0].ds_id, "30563");
    assert_eq!(plan.jobs[0].file_count, 4);
    assert_eq!(plan.jobs[0].large_file_count, 1);
    Ok(())
}

#[test]
fn planner_rejects_provider_dataset_endpoint_without_matching_inventory() -> TestResult {
    let endpoints = vec![VWorldDatasetCollectionEndpoint {
        endpoint_slug: "vworld-dataset-land_register".to_owned(),
        source_slug: "vworldkr__land_register".to_owned(),
        source_name: "VWorld 토지대장".to_owned(),
        dataset_name: "land_register".to_owned(),
        base_uri: "https://www.vworld.kr".to_owned(),
        terms_url: None,
        operation: "land_register".to_owned(),
        source_acquisition_lane: "provider_dataset_file".to_owned(),
        national_collection_allowed: true,
        selector: VWorldDatasetInventorySelector {
            svc_cde: "NA".to_owned(),
            ds_id: "99999".to_owned(),
        },
    }];

    let result = plan_vworld_dataset_collection(&endpoints, &[]);
    let Err(error) = result else {
        return Err(std::io::Error::other("missing provider inventory must fail").into());
    };

    assert!(
        error
            .to_string()
            .contains("no VWorld dataset inventory match"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn planner_rejects_wrong_collection_lane() -> TestResult {
    let endpoints = vec![VWorldDatasetCollectionEndpoint {
        endpoint_slug: "vworld-dataset-parcel".to_owned(),
        source_slug: "vworldkr__parcel".to_owned(),
        source_name: "VWorld 필지".to_owned(),
        dataset_name: "parcel".to_owned(),
        base_uri: "https://www.vworld.kr".to_owned(),
        terms_url: None,
        operation: "parcel".to_owned(),
        source_acquisition_lane: "open_api_only".to_owned(),
        national_collection_allowed: true,
        selector: VWorldDatasetInventorySelector {
            svc_cde: "MK".to_owned(),
            ds_id: "30563".to_owned(),
        },
    }];
    let inventory = vec![VWorldDatasetInventoryDataset {
        module: "parcel".to_owned(),
        svc_cde: "MK".to_owned(),
        ds_id: "30563".to_owned(),
        file_pages: 3,
        file_count: 4,
        large_file_count: 1,
        listed_gib: "0.04".to_owned(),
    }];

    let result = plan_vworld_dataset_collection(&endpoints, &inventory);
    let Err(error) = result else {
        return Err(std::io::Error::other("wrong lane must fail").into());
    };

    assert!(
        error
            .to_string()
            .contains("source_acquisition_lane must be provider_dataset_file"),
        "unexpected error: {error}"
    );
    Ok(())
}
