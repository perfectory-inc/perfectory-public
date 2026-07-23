//! Versioned workload policy and environment subject-binding contracts.

use authorization_domain::Permission;
use identity_contracts::PrincipalId;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// Version accepted by the compiled workload policy parser.
pub const POLICY_SCHEMA_VERSION: &str = "identity.workload-principal-policy.v1";
/// Version accepted by the environment subject-binding parser.
pub const BINDINGS_SCHEMA_VERSION: &str = "identity.workload-principal-bindings.v1";

const COMPILED_POLICY: &str = include_str!("../../../config/workload-principal-policy.v1.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    schema_version: String,
    principals: Vec<RawPolicyPrincipal>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicyPrincipal {
    service_slug: String,
    principal_id: PrincipalId,
    display_name: String,
    capabilities: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBindings {
    schema_version: String,
    bindings: Vec<RawBinding>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBinding {
    service_slug: String,
    zitadel_subject: String,
}

/// Fully validated, source-controlled workload policy.
#[derive(Debug)]
pub struct ValidatedPolicy {
    /// Policy contract version.
    pub schema_version: String,
    /// Stable workload identities and exact capabilities.
    pub principals: Vec<WorkloadPrincipalPolicy>,
}

/// One source-controlled workload identity policy.
#[derive(Debug)]
pub struct WorkloadPrincipalPolicy {
    /// Stable lowercase service identifier.
    pub service_slug: String,
    /// Stable Identity-owned principal identifier.
    pub principal_id: PrincipalId,
    /// Operator-facing principal name.
    pub display_name: String,
    /// Exact capability set owned by Identity Platform policy.
    pub capabilities: Vec<Permission>,
}

/// Fully validated environment-specific subject bindings.
#[derive(Debug)]
pub struct ValidatedBindings {
    bindings: Vec<ServiceSubjectBinding>,
}

#[derive(Debug)]
struct ServiceSubjectBinding {
    service_slug: String,
    zitadel_subject: String,
}

/// Fully resolved service-principal manifest ready for one transaction.
#[derive(Debug)]
pub struct ValidatedManifest {
    /// Policy contract version used to build this manifest.
    pub schema_version: String,
    /// Service principals with environment subjects and policy-owned capabilities.
    pub principals: Vec<ServicePrincipalDefinition>,
}

/// Resolved service principal definition.
#[derive(Debug)]
pub struct ServicePrincipalDefinition {
    /// Stable service identifier used to join policy and environment binding.
    pub service_slug: String,
    /// Stable Identity principal identifier.
    pub principal_id: PrincipalId,
    /// Exact signed Zitadel subject supplied by the deployment environment.
    pub zitadel_subject: String,
    /// Operator-facing principal name.
    pub display_name: String,
    /// Exact policy-owned capability set to synchronize.
    pub capabilities: Vec<Permission>,
}

/// Workload policy or subject-binding validation failure.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManifestError {
    /// Input was not valid JSON or did not match the strict contract shape.
    #[error("workload identity document is not valid versioned JSON")]
    InvalidJson,
    /// The document uses an unsupported contract version.
    #[error("workload identity document schema version is unsupported")]
    UnsupportedSchemaVersion,
    /// The policy or bindings document contains no entries.
    #[error("workload identity document contains no entries")]
    EmptyDocument,
    /// A service slug is malformed.
    #[error("workload service slug is invalid")]
    InvalidServiceSlug,
    /// A principal identifier is nil.
    #[error("workload principal identifier is invalid")]
    InvalidPrincipalId,
    /// A signed subject is blank, malformed, or still a placeholder.
    #[error("workload principal subject is invalid")]
    InvalidSubject,
    /// A display name is blank or malformed.
    #[error("workload principal display name is invalid")]
    InvalidDisplayName,
    /// A capability does not use canonical resource/action syntax.
    #[error("workload principal capability is invalid")]
    InvalidCapability,
    /// A service slug is repeated in one document.
    #[error("workload identity document contains a duplicate service slug")]
    DuplicateServiceSlug,
    /// Two policies use the same principal identifier.
    #[error("workload policy contains a duplicate principal identifier")]
    DuplicatePrincipalId,
    /// Two bindings use the same signed subject.
    #[error("workload bindings contain a duplicate principal subject")]
    DuplicateSubject,
    /// One policy repeats a capability.
    #[error("workload policy contains a duplicate capability")]
    DuplicateCapability,
    /// A policy service has no environment subject binding.
    #[error("workload policy is missing an environment subject binding")]
    MissingBinding,
    /// A binding references a service absent from policy.
    #[error("workload subject binding references an unknown service")]
    UnknownBinding,
}

/// Parses and validates a source-controlled workload policy.
///
/// # Errors
/// Returns [`ManifestError`] when shape, version, identities, or capabilities are invalid.
pub fn parse_policy(raw: &str) -> Result<ValidatedPolicy, ManifestError> {
    let raw: RawPolicy = serde_json::from_str(raw).map_err(|_| ManifestError::InvalidJson)?;
    if raw.schema_version != POLICY_SCHEMA_VERSION {
        return Err(ManifestError::UnsupportedSchemaVersion);
    }
    if raw.principals.is_empty() {
        return Err(ManifestError::EmptyDocument);
    }

    let mut service_slugs = HashSet::new();
    let mut principal_ids = HashSet::new();
    let mut principals = Vec::with_capacity(raw.principals.len());
    for principal in raw.principals {
        validate_service_slug(&principal.service_slug)?;
        if !service_slugs.insert(principal.service_slug.clone()) {
            return Err(ManifestError::DuplicateServiceSlug);
        }
        if principal.principal_id.as_uuid().is_nil() {
            return Err(ManifestError::InvalidPrincipalId);
        }
        if !principal_ids.insert(principal.principal_id) {
            return Err(ManifestError::DuplicatePrincipalId);
        }
        validate_display_name(&principal.display_name)?;

        let mut capability_values = HashSet::new();
        let mut capabilities = Vec::with_capacity(principal.capabilities.len());
        for capability in principal.capabilities {
            validate_capability(&capability)?;
            if !capability_values.insert(capability.clone()) {
                return Err(ManifestError::DuplicateCapability);
            }
            capabilities
                .push(Permission::parse(capability).map_err(|_| ManifestError::InvalidCapability)?);
        }
        capabilities.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        principals.push(WorkloadPrincipalPolicy {
            service_slug: principal.service_slug,
            principal_id: principal.principal_id,
            display_name: principal.display_name,
            capabilities,
        });
    }

    Ok(ValidatedPolicy {
        schema_version: raw.schema_version,
        principals,
    })
}

/// Parses and validates environment-specific Zitadel subject bindings.
///
/// # Errors
/// Returns [`ManifestError`] when shape, version, service slugs, or subjects are invalid.
pub fn parse_bindings(raw: &str) -> Result<ValidatedBindings, ManifestError> {
    let raw: RawBindings = serde_json::from_str(raw).map_err(|_| ManifestError::InvalidJson)?;
    if raw.schema_version != BINDINGS_SCHEMA_VERSION {
        return Err(ManifestError::UnsupportedSchemaVersion);
    }
    if raw.bindings.is_empty() {
        return Err(ManifestError::EmptyDocument);
    }

    let mut service_slugs = HashSet::new();
    let mut subjects = HashSet::new();
    let mut bindings = Vec::with_capacity(raw.bindings.len());
    for binding in raw.bindings {
        validate_service_slug(&binding.service_slug)?;
        if !service_slugs.insert(binding.service_slug.clone()) {
            return Err(ManifestError::DuplicateServiceSlug);
        }
        validate_subject(&binding.zitadel_subject)?;
        if !subjects.insert(binding.zitadel_subject.clone()) {
            return Err(ManifestError::DuplicateSubject);
        }
        bindings.push(ServiceSubjectBinding {
            service_slug: binding.service_slug,
            zitadel_subject: binding.zitadel_subject,
        });
    }
    Ok(ValidatedBindings { bindings })
}

/// Resolves an exact one-to-one policy and environment binding set.
///
/// # Errors
/// Returns [`ManifestError::UnknownBinding`] or [`ManifestError::MissingBinding`] on drift.
pub fn resolve_manifest(
    policy: ValidatedPolicy,
    bindings: ValidatedBindings,
) -> Result<ValidatedManifest, ManifestError> {
    let policy_slugs = policy
        .principals
        .iter()
        .map(|principal| principal.service_slug.as_str())
        .collect::<HashSet<_>>();
    if bindings
        .bindings
        .iter()
        .any(|binding| !policy_slugs.contains(binding.service_slug.as_str()))
    {
        return Err(ManifestError::UnknownBinding);
    }

    let mut subjects = bindings
        .bindings
        .into_iter()
        .map(|binding| (binding.service_slug, binding.zitadel_subject))
        .collect::<HashMap<_, _>>();
    let mut principals = Vec::with_capacity(policy.principals.len());
    for principal in policy.principals {
        let Some(zitadel_subject) = subjects.remove(&principal.service_slug) else {
            return Err(ManifestError::MissingBinding);
        };
        principals.push(ServicePrincipalDefinition {
            service_slug: principal.service_slug,
            principal_id: principal.principal_id,
            zitadel_subject,
            display_name: principal.display_name,
            capabilities: principal.capabilities,
        });
    }

    Ok(ValidatedManifest {
        schema_version: policy.schema_version,
        principals,
    })
}

/// Parses the policy artifact compiled into the provisioner binary.
///
/// # Errors
/// Returns [`ManifestError`] if the committed policy and binary ever drift into invalid state.
pub fn compiled_policy() -> Result<ValidatedPolicy, ManifestError> {
    parse_policy(COMPILED_POLICY)
}

fn validate_service_slug(service_slug: &str) -> Result<(), ManifestError> {
    let valid = !service_slug.is_empty()
        && service_slug.len() <= 100
        && service_slug.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        });
    if valid {
        Ok(())
    } else {
        Err(ManifestError::InvalidServiceSlug)
    }
}

fn validate_subject(subject: &str) -> Result<(), ManifestError> {
    let lowercase = subject.to_ascii_lowercase();
    let placeholder = lowercase.contains("placeholder")
        || lowercase.contains("replace_with")
        || subject.contains('<')
        || subject.contains("${");
    if subject.is_empty()
        || subject.len() > 512
        || subject.trim() != subject
        || subject.chars().any(char::is_whitespace)
        || subject.chars().any(char::is_control)
        || placeholder
    {
        return Err(ManifestError::InvalidSubject);
    }
    Ok(())
}

fn validate_display_name(display_name: &str) -> Result<(), ManifestError> {
    if display_name.is_empty()
        || display_name.len() > 200
        || display_name.trim() != display_name
        || display_name.chars().any(char::is_control)
    {
        return Err(ManifestError::InvalidDisplayName);
    }
    Ok(())
}

fn validate_capability(capability: &str) -> Result<(), ManifestError> {
    let Some((resource, action)) = capability.split_once(':') else {
        return Err(ManifestError::InvalidCapability);
    };
    if action.contains(':') || !valid_capability_part(resource) || !valid_capability_part(action) {
        return Err(ManifestError::InvalidCapability);
    }
    Ok(())
}

fn valid_capability_part(part: &str) -> bool {
    !part.is_empty()
        && part.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
}
