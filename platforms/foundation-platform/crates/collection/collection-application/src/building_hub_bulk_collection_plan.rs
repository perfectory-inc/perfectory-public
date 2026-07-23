//! Catalog-driven planning for `hub.go.kr` bulk-file collection.

use std::collections::BTreeSet;

use thiserror::Error;

const REQUIRED_SOURCE_ACQUISITION_LANE: &str = "bulk_file";

/// Selector that binds one catalog endpoint to one official `hub.go.kr` inventory row.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BuildingHubBulkInventorySelector {
    /// First official `fnDownloadPop` argument.
    pub task_group_code: String,
    /// Second official `fnDownloadPop` argument.
    pub task_code: String,
}

/// Catalog endpoint that is eligible for `hub.go.kr` bulk collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkEndpoint {
    /// Endpoint catalog slug.
    pub endpoint_slug: String,
    /// Bronze source slug.
    pub source_slug: String,
    /// Source catalog display name.
    pub source_name: String,
    /// Source catalog dataset name.
    pub dataset_name: String,
    /// Provider base URI.
    pub base_uri: String,
    /// Provider terms or listing URI.
    pub terms_url: Option<String>,
    /// Foundation Platform operation name.
    pub operation: String,
    /// Endpoint acquisition lane. Must be `bulk_file`.
    pub source_acquisition_lane: String,
    /// Whether this endpoint may be used for national collection.
    pub national_collection_allowed: bool,
    /// Provider inventory selector.
    pub selector: BuildingHubBulkInventorySelector,
}

/// One official file listed by `hub.go.kr`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkInventoryFile {
    /// Provider category name normalized by the inventory adapter.
    pub category_name: String,
    /// Provider service name normalized by the inventory adapter.
    pub service_name: String,
    /// Provider service period label.
    pub service_period_label: String,
    /// Provider file period such as `2026-05`.
    pub provider_file_period: String,
    /// First official `fnDownloadPop` argument.
    pub task_group_code: String,
    /// Second official `fnDownloadPop` argument.
    pub task_code: String,
    /// Stable `hub.go.kr` server file id.
    pub provider_file_id: String,
}

/// One executable collection job derived from catalog and official provider inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkCollectionJob {
    /// Endpoint catalog slug.
    pub endpoint_slug: String,
    /// Bronze source slug.
    pub source_slug: String,
    /// Source catalog display name.
    pub source_name: String,
    /// Source catalog dataset name.
    pub dataset_name: String,
    /// Provider base URI.
    pub base_uri: String,
    /// Provider terms or listing URI.
    pub terms_url: Option<String>,
    /// Foundation Platform operation name.
    pub operation: String,
    /// Provider file period such as `2026-05`.
    pub provider_file_period: String,
    /// Stable `hub.go.kr` server file id.
    pub provider_file_id: String,
    /// Provider category matched by the selector.
    pub category_name: String,
    /// Provider service name matched by the selector.
    pub service_name: String,
    /// Provider service period label.
    pub service_period_label: String,
    /// First official `fnDownloadPop` argument.
    pub task_group_code: String,
    /// Second official `fnDownloadPop` argument.
    pub task_code: String,
}

/// Complete executable `hub.go.kr` bulk collection plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingHubBulkCollectionPlan {
    /// Jobs in deterministic endpoint slug order.
    pub jobs: Vec<BuildingHubBulkCollectionJob>,
}

/// Error returned while compiling `hub.go.kr` bulk collection plans.
#[derive(Debug, Error)]
pub enum BuildingHubBulkCollectionPlanError {
    /// Endpoint failed catalog-level validation.
    #[error("invalid hub.go.kr bulk endpoint {endpoint_slug}: {reason}")]
    InvalidEndpoint {
        /// Endpoint catalog slug.
        endpoint_slug: String,
        /// Validation failure.
        reason: String,
    },
    /// Official provider inventory did not contain an expected file.
    #[error("no hub.go.kr inventory match for endpoint {endpoint_slug} selector task_group_code={task_group_code} task_code={task_code}")]
    MissingInventoryMatch {
        /// Endpoint catalog slug.
        endpoint_slug: String,
        /// Expected task group code.
        task_group_code: String,
        /// Expected task code.
        task_code: String,
    },
}

/// Compiles deterministic `hub.go.kr` bulk-file collection jobs from catalog endpoints and
/// official provider inventory.
///
/// # Errors
///
/// Returns `BuildingHubBulkCollectionPlanError` when an endpoint is disabled, ambiguous, or
/// cannot be matched to exactly one official inventory row.
pub fn plan_building_hub_bulk_collection(
    endpoints: &[BuildingHubBulkEndpoint],
    inventory: &[BuildingHubBulkInventoryFile],
) -> Result<BuildingHubBulkCollectionPlan, BuildingHubBulkCollectionPlanError> {
    let mut endpoints = endpoints.to_vec();
    endpoints.sort_by(|left, right| left.endpoint_slug.cmp(&right.endpoint_slug));
    let mut seen_endpoint_slugs = BTreeSet::new();
    let mut jobs = Vec::with_capacity(endpoints.len());

    for endpoint in &endpoints {
        validate_endpoint(endpoint, &mut seen_endpoint_slugs)?;
        let mut matches = inventory
            .iter()
            .filter(|file| {
                file.task_group_code == endpoint.selector.task_group_code
                    && file.task_code == endpoint.selector.task_code
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(BuildingHubBulkCollectionPlanError::MissingInventoryMatch {
                endpoint_slug: endpoint.endpoint_slug.clone(),
                task_group_code: endpoint.selector.task_group_code.clone(),
                task_code: endpoint.selector.task_code.clone(),
            });
        }
        matches.sort_by(|left, right| {
            left.provider_file_period
                .cmp(&right.provider_file_period)
                .then_with(|| left.provider_file_id.cmp(&right.provider_file_id))
        });

        for file in matches {
            jobs.push(BuildingHubBulkCollectionJob {
                endpoint_slug: endpoint.endpoint_slug.clone(),
                source_slug: endpoint.source_slug.clone(),
                source_name: endpoint.source_name.clone(),
                dataset_name: endpoint.dataset_name.clone(),
                base_uri: endpoint.base_uri.clone(),
                terms_url: endpoint.terms_url.clone(),
                operation: endpoint.operation.clone(),
                provider_file_period: file.provider_file_period.clone(),
                provider_file_id: file.provider_file_id.clone(),
                category_name: file.category_name.clone(),
                service_name: file.service_name.clone(),
                service_period_label: file.service_period_label.clone(),
                task_group_code: file.task_group_code.clone(),
                task_code: file.task_code.clone(),
            });
        }
    }

    Ok(BuildingHubBulkCollectionPlan { jobs })
}

fn validate_endpoint(
    endpoint: &BuildingHubBulkEndpoint,
    seen_endpoint_slugs: &mut BTreeSet<String>,
) -> Result<(), BuildingHubBulkCollectionPlanError> {
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
        "provider_inventory_selector.task_group_code",
        &endpoint.selector.task_group_code,
        endpoint,
    )?;
    validate_required(
        "provider_inventory_selector.task_code",
        &endpoint.selector.task_code,
        endpoint,
    )?;
    if endpoint.source_acquisition_lane != REQUIRED_SOURCE_ACQUISITION_LANE {
        return Err(invalid_endpoint(
            endpoint,
            "source_acquisition_lane must be bulk_file",
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
    endpoint: &BuildingHubBulkEndpoint,
) -> Result<(), BuildingHubBulkCollectionPlanError> {
    if value.trim().is_empty() {
        return Err(invalid_endpoint(endpoint, &format!("{field} is required")));
    }
    Ok(())
}

fn invalid_endpoint(
    endpoint: &BuildingHubBulkEndpoint,
    reason: &str,
) -> BuildingHubBulkCollectionPlanError {
    BuildingHubBulkCollectionPlanError::InvalidEndpoint {
        endpoint_slug: endpoint.endpoint_slug.clone(),
        reason: reason.to_owned(),
    }
}
