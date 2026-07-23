use super::{compile_provider_acquisition_plan_report, ProviderBlockedFileRow};

#[test]
fn report_converts_blocked_csv_rows_to_jobs() {
    let rows = vec![ProviderBlockedFileRow {
        source_slug: "vworldkr__parcel".to_owned(),
        download_ds_id: "20991231DS99991".to_owned(),
        file_no: "9001".to_owned(),
        provider_file_name: "parcel.zip".to_owned(),
    }];

    let report = compile_provider_acquisition_plan_report(&rows).expect("report");

    assert_eq!(
        report.schema_version,
        "foundation-platform.provider_acquisition_plan.v1"
    );
    assert_eq!(report.job_count, 1);
    assert_eq!(report.jobs[0].source_slug, "vworldkr__parcel");
    assert_eq!(report.jobs[0].acquisition_method, "raon_kupload_browser");
    assert_eq!(
        report.jobs[0].provider_resource_id,
        "vworld_dataset_file:20991231DS99991:9001"
    );
    assert_eq!(report.jobs[0].download_ds_id, "20991231DS99991");
    assert_eq!(report.jobs[0].file_no, "9001");
    assert_eq!(report.jobs[0].provider_file_name, "parcel.zip");
    assert_eq!(report.jobs[0].provider_file_id, "20991231DS99991-9001");
    assert_eq!(
        report.jobs[0].source_identity_key,
        "provider_file_id=20991231DS99991-9001"
    );
}
