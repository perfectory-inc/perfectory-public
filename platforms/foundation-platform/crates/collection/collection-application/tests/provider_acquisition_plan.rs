//! Provider acquisition plan tests.

use collection_application::{plan_vworld_raon_acquisition, ProviderBlockedVWorldFile};
use collection_domain::{ProviderAcquisitionMethod, ProviderAcquisitionResource};

#[test]
fn plans_one_acquisition_job_per_blocked_vworld_file() -> Result<(), Box<dyn std::error::Error>> {
    let blocked = vec![ProviderBlockedVWorldFile {
        source_slug: "vworldkr__parcel".to_owned(),
        download_ds_id: "20991231DS99991".to_owned(),
        file_no: "9001".to_owned(),
        provider_file_name: "parcel.zip".to_owned(),
    }];

    let plan = plan_vworld_raon_acquisition(&blocked)?;

    assert_eq!(plan.jobs.len(), 1);
    let job = &plan.jobs[0];
    assert_eq!(job.source_slug(), "vworldkr__parcel");
    assert_eq!(job.method(), ProviderAcquisitionMethod::RaonKuploadBrowser);
    assert_eq!(
        job.resource(),
        &ProviderAcquisitionResource::VWorldDatasetFile {
            download_ds_id: "20991231DS99991".to_owned(),
            file_no: "9001".to_owned(),
        }
    );
    Ok(())
}

#[test]
fn rejects_empty_blocked_file_identity() -> Result<(), Box<dyn std::error::Error>> {
    let blocked = vec![ProviderBlockedVWorldFile {
        source_slug: "vworldkr__parcel".to_owned(),
        download_ds_id: String::new(),
        file_no: "9001".to_owned(),
        provider_file_name: "parcel.zip".to_owned(),
    }];

    let Err(error) = plan_vworld_raon_acquisition(&blocked) else {
        return Err("invalid blocked file identity must fail".into());
    };

    assert!(error.to_string().contains("download_ds_id"));
    Ok(())
}
