use collection_domain::{ProviderAcquisitionError, ProviderAcquisitionJob};

/// Provider-blocked V-World file that needs acquisition outside plain HTTP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderBlockedVWorldFile {
    /// Canonical Foundation Platform source slug.
    pub source_slug: String,
    /// V-World download dataset id.
    pub download_ds_id: String,
    /// V-World provider file number.
    pub file_no: String,
    /// Provider file name expected after acquisition.
    pub provider_file_name: String,
}

/// Provider acquisition jobs compiled from blocked source inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAcquisitionPlan {
    /// Acquisition jobs to dispatch.
    pub jobs: Vec<ProviderAcquisitionJob>,
}

/// Compiles V-World RAON/KUpload acquisition jobs from blocked file inventory.
///
/// # Errors
///
/// Returns an error when any blocked file row has an invalid provider identity.
pub fn plan_vworld_raon_acquisition(
    blocked_files: &[ProviderBlockedVWorldFile],
) -> Result<ProviderAcquisitionPlan, ProviderAcquisitionError> {
    let jobs = blocked_files
        .iter()
        .map(|file| {
            ProviderAcquisitionJob::new_vworld_raon(
                file.source_slug.clone(),
                file.download_ds_id.clone(),
                file.file_no.clone(),
                file.provider_file_name.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ProviderAcquisitionPlan { jobs })
}
