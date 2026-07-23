use std::fmt;

/// Provider-side acquisition mechanism used before raw bytes enter Bronze.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderAcquisitionMethod {
    /// Plain server-side HTTP request.
    Http,
    /// Browser-mediated V-World RAON/KUpload flow.
    RaonKuploadBrowser,
    /// Browser-mediated dynamic fetch driven by Scrapling.
    ScraplingDynamic,
    /// Operator-controlled fallback when automation cannot acquire the source.
    ManualFallback,
}

/// Provider-side resource identity for an acquisition job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderAcquisitionResource {
    /// V-World dataset file identified by provider dataset id and file number.
    VWorldDatasetFile {
        /// V-World `ds_id` / download dataset id.
        download_ds_id: String,
        /// V-World provider file number.
        file_no: String,
    },
}

/// Job asking an acquisition worker to obtain provider bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAcquisitionJob {
    provider: String,
    source_slug: String,
    method: ProviderAcquisitionMethod,
    resource: ProviderAcquisitionResource,
    expected_file_name: String,
}

/// Evidence emitted by an acquisition worker after writing a landing object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAcquisitionEvidence {
    /// Internal acquisition job id.
    pub job_id: String,
    /// Temporary landing object key, never a final Bronze object key.
    pub landing_object_key: String,
    /// Landing object size in bytes.
    pub size_bytes: u64,
    /// Optional checksum if the acquisition worker computed one.
    pub checksum_sha256: Option<String>,
}

/// Validation error for provider acquisition domain values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAcquisitionError {
    message: String,
}

impl ProviderAcquisitionJob {
    /// Builds a V-World RAON/KUpload browser acquisition job.
    ///
    /// # Errors
    ///
    /// Returns an error when any required provider or source identity field is empty.
    pub fn new_vworld_raon(
        source_slug: impl Into<String>,
        download_ds_id: impl Into<String>,
        file_no: impl Into<String>,
        expected_file_name: impl Into<String>,
    ) -> Result<Self, ProviderAcquisitionError> {
        let source_slug = non_empty("source_slug", source_slug.into())?;
        let download_ds_id = non_empty("download_ds_id", download_ds_id.into())?;
        let file_no = non_empty("file_no", file_no.into())?;
        let expected_file_name = non_empty("expected_file_name", expected_file_name.into())?;

        Ok(Self {
            provider: "vworldkr".to_owned(),
            source_slug,
            method: ProviderAcquisitionMethod::RaonKuploadBrowser,
            resource: ProviderAcquisitionResource::VWorldDatasetFile {
                download_ds_id,
                file_no,
            },
            expected_file_name,
        })
    }

    /// Provider slug such as `vworldkr`.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Expected Foundation Platform source slug.
    #[must_use]
    pub fn source_slug(&self) -> &str {
        &self.source_slug
    }

    /// Acquisition mechanism.
    #[must_use]
    pub const fn method(&self) -> ProviderAcquisitionMethod {
        self.method
    }

    /// Provider-side resource identity.
    #[must_use]
    pub const fn resource(&self) -> &ProviderAcquisitionResource {
        &self.resource
    }

    /// Provider file name expected from the acquisition flow.
    #[must_use]
    pub fn expected_file_name(&self) -> &str {
        &self.expected_file_name
    }

    /// Acquisition jobs are instructions, not final Bronze source identities.
    #[must_use]
    pub const fn is_bronze_identity(&self) -> bool {
        false
    }
}

impl fmt::Display for ProviderAcquisitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProviderAcquisitionError {}

fn non_empty(name: &str, value: String) -> Result<String, ProviderAcquisitionError> {
    if value.trim().is_empty() {
        return Err(ProviderAcquisitionError {
            message: format!("{name} must not be empty"),
        });
    }
    Ok(value)
}
