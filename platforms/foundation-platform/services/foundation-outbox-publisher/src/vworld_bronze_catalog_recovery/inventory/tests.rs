use super::{parse_requested_source_slugs, select_endpoints};

#[test]
fn selects_exact_endpoint_catalog_scope_without_collection_plan_csv() {
    let catalog = serde_json::from_str(
        r#"{
          "schema_version":"foundation-platform.public_source_endpoint_catalog.v1",
          "status":"ready",
          "endpoints":[
            {
              "endpoint_slug":"vworld-dataset-land_right_registration",
              "provider":"VWorld",
              "dataset_slug":"land_right_registration",
              "operation":"land_right_registration",
              "source_acquisition_lane":"provider_dataset_file",
              "provider_dataset_selector":{"svc_cde":"NA","ds_id":"20"},
              "auth_kind":"provider_managed_credential",
              "bronze":{"source_slug":"vworldkr__land_right_registration"}
            },
            {
              "endpoint_slug":"vworld-dataset-land_transfer_history",
              "provider":"VWorld",
              "dataset_slug":"land_transfer_history",
              "operation":"land_transfer_history",
              "source_acquisition_lane":"provider_dataset_file",
              "provider_dataset_selector":{"svc_cde":"NA","ds_id":"13"},
              "auth_kind":"provider_managed_credential",
              "bronze":{"source_slug":"vworldkr__land_transfer_history"}
            },
            {
              "endpoint_slug":"public-bulk-without-dataset-slug",
              "provider":"mixed_public_source",
              "operation":"unrelated",
              "source_acquisition_lane":"manual_approval_bulk",
              "auth_kind":"provider_managed_credential",
              "bronze":{"source_slug":"public-bulk-unrelated"}
            }
          ]
        }"#,
    )
    .expect("endpoint catalog fixture should parse");
    let requested = parse_requested_source_slugs("vworldkr__land_right_registration")
        .expect("one source slug should parse");

    let selected = select_endpoints(catalog, &requested).expect("source should resolve");

    assert_eq!(selected.len(), 1);
    assert_eq!(
        selected[0].endpoint_slug,
        "vworld-dataset-land_right_registration"
    );
    assert_eq!(
        selected[0]
            .provider_dataset_selector
            .as_ref()
            .expect("selector")
            .ds_id,
        "20"
    );
}

#[test]
fn duplicate_or_unknown_recovery_source_scope_is_rejected() {
    assert!(parse_requested_source_slugs(
        "vworldkr__land_right_registration,vworldkr__land_right_registration"
    )
    .is_err());

    let catalog = serde_json::from_str(
        r#"{
          "schema_version":"foundation-platform.public_source_endpoint_catalog.v1",
          "status":"ready",
          "endpoints":[]
        }"#,
    )
    .expect("endpoint catalog fixture should parse");
    let requested =
        parse_requested_source_slugs("vworldkr__missing").expect("source slug syntax should parse");

    assert!(select_endpoints(catalog, &requested).is_err());
}
