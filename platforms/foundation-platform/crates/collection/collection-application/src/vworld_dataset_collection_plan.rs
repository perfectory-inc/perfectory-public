//! Catalog-driven planning for `VWorld` provider dataset-file collection.

use std::collections::BTreeSet;

use thiserror::Error;

const REQUIRED_SOURCE_ACQUISITION_LANE: &str = "provider_dataset_file";

/// Selector that binds one catalog endpoint to one official `VWorld` dataset.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct VWorldDatasetInventorySelector {
    /// `VWorld` service code, for example `MK` or `NA`.
    pub svc_cde: String,
    /// `VWorld` dataset id.
    pub ds_id: String,
}

/// Catalog endpoint eligible for `VWorld` dataset-file collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetCollectionEndpoint {
    /// Endpoint catalog slug.
    pub endpoint_slug: String,
    /// Bronze source slug.
    pub source_slug: String,
    /// Source catalog display name.
    pub source_name: String,
    /// Foundation Platform dataset name.
    pub dataset_name: String,
    /// Provider base URI.
    pub base_uri: String,
    /// Provider terms or listing URI.
    pub terms_url: Option<String>,
    /// Foundation Platform operation name.
    pub operation: String,
    /// Endpoint acquisition lane. Must be `provider_dataset_file`.
    pub source_acquisition_lane: String,
    /// Whether this endpoint may be used for national collection.
    pub national_collection_allowed: bool,
    /// Provider dataset selector.
    pub selector: VWorldDatasetInventorySelector,
}

/// One official dataset listed by the `VWorld` data catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetInventoryDataset {
    /// Foundation Platform module name associated with the provider dataset.
    pub module: String,
    /// `VWorld` service code.
    pub svc_cde: String,
    /// `VWorld` dataset id.
    pub ds_id: String,
    /// Number of provider file listing pages observed for this dataset.
    pub file_pages: u64,
    /// Number of provider files observed for this dataset.
    pub file_count: u64,
    /// Number of large-file download entries observed for this dataset.
    pub large_file_count: u64,
    /// Listed provider size in GiB as displayed or summarized by the inventory adapter.
    pub listed_gib: String,
}

/// One executable dataset collection job derived from catalog and provider inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetCollectionJob {
    /// Endpoint catalog slug.
    pub endpoint_slug: String,
    /// Bronze source slug.
    pub source_slug: String,
    /// Source catalog display name.
    pub source_name: String,
    /// Foundation Platform dataset name.
    pub dataset_name: String,
    /// Provider base URI.
    pub base_uri: String,
    /// Provider terms or listing URI.
    pub terms_url: Option<String>,
    /// Foundation Platform operation name.
    pub operation: String,
    /// Provider inventory module.
    pub provider_module: String,
    /// `VWorld` service code.
    pub svc_cde: String,
    /// `VWorld` dataset id.
    pub ds_id: String,
    /// Number of provider file listing pages to fetch.
    pub file_pages: u64,
    /// Number of provider files expected.
    pub file_count: u64,
    /// Number of large-file entries expected.
    pub large_file_count: u64,
    /// Listed provider size in GiB.
    pub listed_gib: String,
}

/// Complete executable `VWorld` dataset collection plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDatasetCollectionPlan {
    /// Jobs in deterministic endpoint slug order.
    pub jobs: Vec<VWorldDatasetCollectionJob>,
}

/// Error returned while compiling `VWorld` dataset collection plans.
#[derive(Debug, Error)]
pub enum VWorldDatasetCollectionPlanError {
    /// Endpoint failed catalog-level validation.
    #[error("invalid VWorld dataset endpoint {endpoint_slug}: {reason}")]
    InvalidEndpoint {
        /// Endpoint catalog slug.
        endpoint_slug: String,
        /// Validation failure.
        reason: String,
    },
    /// Provider inventory did not contain an expected dataset.
    #[error("no VWorld dataset inventory match for endpoint {endpoint_slug} selector svc_cde={svc_cde} ds_id={ds_id}")]
    MissingInventoryMatch {
        /// Endpoint catalog slug.
        endpoint_slug: String,
        /// Expected service code.
        svc_cde: String,
        /// Expected dataset id.
        ds_id: String,
    },
}

/// Compiles deterministic `VWorld` provider dataset-file collection jobs from catalog endpoints
/// and official provider inventory.
///
/// # Errors
///
/// Returns `VWorldDatasetCollectionPlanError` when an endpoint is disabled, ambiguous, or cannot
/// be matched to one official provider dataset.
pub fn plan_vworld_dataset_collection(
    endpoints: &[VWorldDatasetCollectionEndpoint],
    inventory: &[VWorldDatasetInventoryDataset],
) -> Result<VWorldDatasetCollectionPlan, VWorldDatasetCollectionPlanError> {
    let mut endpoints = endpoints.to_vec();
    endpoints.sort_by(|left, right| left.endpoint_slug.cmp(&right.endpoint_slug));
    let mut seen_endpoint_slugs = BTreeSet::new();
    let mut jobs = Vec::with_capacity(endpoints.len());

    for endpoint in &endpoints {
        validate_endpoint(endpoint, &mut seen_endpoint_slugs)?;
        let dataset = inventory
            .iter()
            .find(|dataset| {
                dataset.svc_cde == endpoint.selector.svc_cde
                    && dataset.ds_id == endpoint.selector.ds_id
            })
            .ok_or_else(|| VWorldDatasetCollectionPlanError::MissingInventoryMatch {
                endpoint_slug: endpoint.endpoint_slug.clone(),
                svc_cde: endpoint.selector.svc_cde.clone(),
                ds_id: endpoint.selector.ds_id.clone(),
            })?;

        jobs.push(VWorldDatasetCollectionJob {
            endpoint_slug: endpoint.endpoint_slug.clone(),
            source_slug: endpoint.source_slug.clone(),
            source_name: endpoint.source_name.clone(),
            dataset_name: endpoint.dataset_name.clone(),
            base_uri: endpoint.base_uri.clone(),
            terms_url: endpoint.terms_url.clone(),
            operation: endpoint.operation.clone(),
            provider_module: dataset.module.clone(),
            svc_cde: dataset.svc_cde.clone(),
            ds_id: dataset.ds_id.clone(),
            file_pages: dataset.file_pages,
            file_count: dataset.file_count,
            large_file_count: dataset.large_file_count,
            listed_gib: dataset.listed_gib.clone(),
        });
    }

    Ok(VWorldDatasetCollectionPlan { jobs })
}

fn validate_endpoint(
    endpoint: &VWorldDatasetCollectionEndpoint,
    seen_endpoint_slugs: &mut BTreeSet<String>,
) -> Result<(), VWorldDatasetCollectionPlanError> {
    validate_required("endpoint_slug", &endpoint.endpoint_slug, endpoint)?;
    if !seen_endpoint_slugs.insert(endpoint.endpoint_slug.clone()) {
        return Err(invalid_endpoint(endpoint, "endpoint_slug must be unique"));
    }
    validate_required("source_slug", &endpoint.source_slug, endpoint)?;
    validate_required("source_name", &endpoint.source_name, endpoint)?;
    validate_required("dataset_name", &endpoint.dataset_name, endpoint)?;
    validate_required("base_uri", &endpoint.base_uri, endpoint)?;
    validate_required("operation", &endpoint.operation, endpoint)?;
    validate_required(
        "provider_dataset_selector.svc_cde",
        &endpoint.selector.svc_cde,
        endpoint,
    )?;
    validate_required(
        "provider_dataset_selector.ds_id",
        &endpoint.selector.ds_id,
        endpoint,
    )?;
    if endpoint.source_acquisition_lane != REQUIRED_SOURCE_ACQUISITION_LANE {
        return Err(invalid_endpoint(
            endpoint,
            "source_acquisition_lane must be provider_dataset_file",
        ));
    }
    if !endpoint.national_collection_allowed {
        return Err(invalid_endpoint(
            endpoint,
            "national_collection_allowed must be true",
        ));
    }
    Ok(())
}

fn validate_required(
    field: &'static str,
    value: &str,
    endpoint: &VWorldDatasetCollectionEndpoint,
) -> Result<(), VWorldDatasetCollectionPlanError> {
    if value.trim().is_empty() {
        return Err(invalid_endpoint(endpoint, &format!("{field} is required")));
    }
    Ok(())
}

fn invalid_endpoint(
    endpoint: &VWorldDatasetCollectionEndpoint,
    reason: &str,
) -> VWorldDatasetCollectionPlanError {
    VWorldDatasetCollectionPlanError::InvalidEndpoint {
        endpoint_slug: endpoint.endpoint_slug.clone(),
        reason: reason.to_owned(),
    }
}
