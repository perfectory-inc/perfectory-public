use std::fmt;

use collection_domain::{
    ProviderAcquisitionJob, ProviderAcquisitionMethod, ProviderAcquisitionResource,
};

/// Temporary object written by an acquisition worker before Bronze validation/import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderLandingObject {
    /// Temporary R2 landing key.
    pub object_key: String,
    /// Landing object size in bytes.
    pub size_bytes: u64,
    /// Optional checksum if already known at acquisition time.
    pub checksum_sha256: Option<String>,
}

/// Error returned when a provider landing key would be unsafe or unsupported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderLandingError {
    message: String,
}

/// Builds a temporary landing key for a provider acquisition job.
///
/// # Errors
///
/// Returns an error when the job cannot be represented as a safe landing object key.
pub fn provider_landing_key(
    job_id: &str,
    job: &ProviderAcquisitionJob,
) -> Result<String, ProviderLandingError> {
    let job_id = safe_segment("job_id", job_id)?;
    let file_name = safe_segment("provider file name", job.expected_file_name())?;

    match (job.method(), job.resource()) {
        (
            ProviderAcquisitionMethod::RaonKuploadBrowser,
            ProviderAcquisitionResource::VWorldDatasetFile {
                download_ds_id,
                file_no,
            },
        ) => Ok(format!(
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id={job_id}/download_ds_id={}/file_no={}/{}",
            safe_segment("download_ds_id", download_ds_id)?,
            safe_segment("file_no", file_no)?,
            file_name
        )),
        _ => Err(ProviderLandingError {
            message: "unsupported provider acquisition method".to_owned(),
        }),
    }
}

impl fmt::Display for ProviderLandingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProviderLandingError {}

fn safe_segment(name: &str, value: &str) -> Result<String, ProviderLandingError> {
    let value = value.trim();
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".."
        || value.contains("..")
    {
        return Err(ProviderLandingError {
            message: format!("{name} must be a safe object-key segment"),
        });
    }
    Ok(value.to_owned())
}
