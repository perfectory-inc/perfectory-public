//! Provider acquisition job domain tests.

use collection_domain::{
    ProviderAcquisitionJob, ProviderAcquisitionMethod, ProviderAcquisitionResource,
};

#[test]
fn vworld_raon_job_keeps_provider_identity_but_is_not_bronze_identity(
) -> Result<(), Box<dyn std::error::Error>> {
    let job = ProviderAcquisitionJob::new_vworld_raon(
        "vworldkr__parcel",
        "20991231DS99991",
        "9001",
        "parcel.zip",
    )?;

    assert_eq!(job.provider(), "vworldkr");
    assert_eq!(job.source_slug(), "vworldkr__parcel");
    assert_eq!(job.method(), ProviderAcquisitionMethod::RaonKuploadBrowser);
    assert_eq!(
        job.resource(),
        &ProviderAcquisitionResource::VWorldDatasetFile {
            download_ds_id: "20991231DS99991".to_owned(),
            file_no: "9001".to_owned(),
        }
    );
    assert!(!job.is_bronze_identity());
    Ok(())
}

#[test]
fn vworld_raon_job_rejects_empty_identity_parts() -> Result<(), Box<dyn std::error::Error>> {
    let Err(error) =
        ProviderAcquisitionJob::new_vworld_raon("vworldkr__parcel", "", "9001", "parcel.zip")
    else {
        return Err("empty provider id must fail".into());
    };

    assert!(error.to_string().contains("download_ds_id"));
    Ok(())
}
