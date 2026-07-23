//! Provider acquisition landing contract tests.

use collection_application::provider_landing_key;
use collection_domain::ProviderAcquisitionJob;

#[test]
fn vworld_raon_landing_key_is_not_a_bronze_key() -> Result<(), Box<dyn std::error::Error>> {
    let job = ProviderAcquisitionJob::new_vworld_raon(
        "vworldkr__parcel",
        "20991231DS99991",
        "9001",
        "parcel.zip",
    )?;

    let key = provider_landing_key("job-001", &job)?;

    assert_eq!(
        key,
        "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/download_ds_id=20991231DS99991/file_no=9001/parcel.zip"
    );
    assert!(!key.starts_with("bronze/"));
    Ok(())
}

#[test]
fn landing_key_rejects_path_traversal_in_file_name() -> Result<(), Box<dyn std::error::Error>> {
    let job = ProviderAcquisitionJob::new_vworld_raon(
        "vworldkr__parcel",
        "20991231DS99991",
        "9001",
        "../parcel.zip",
    )?;

    let Err(error) = provider_landing_key("job-001", &job) else {
        return Err("unsafe file name must fail".into());
    };

    assert!(error.to_string().contains("provider file name"));
    Ok(())
}
