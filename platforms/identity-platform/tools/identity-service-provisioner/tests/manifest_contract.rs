//! Pure contracts for workload policy and environment subject bindings.

use authorization_domain::Permission;
use identity_service_provisioner::{
    parse_bindings, parse_policy, resolve_manifest, ManifestError, BINDINGS_SCHEMA_VERSION,
    POLICY_SCHEMA_VERSION,
};
use serde_json::{json, Value};

const POLICY: &str = include_str!("../../../config/workload-principal-policy.v1.json");
const BINDINGS_EXAMPLE: &str =
    include_str!("../../../config/workload-principal-bindings.example.v1.json");

#[test]
fn committed_policy_is_the_least_privilege_ssot() -> Result<(), Box<dyn std::error::Error>> {
    let policy = parse_policy(POLICY)?;
    assert_eq!(policy.schema_version, POLICY_SCHEMA_VERSION);

    let actual = policy
        .principals
        .iter()
        .map(|principal| {
            (
                principal.service_slug.as_str(),
                principal
                    .capabilities
                    .iter()
                    .map(Permission::as_str)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            ("gongzzang-api", vec!["foundation.catalog:read"]),
            (
                "gongzzang-outbox-publisher",
                vec!["foundation.lakehouse:write"]
            ),
            ("dawneer-web", vec!["foundation.catalog:read"]),
            (
                "intelligence-normalization",
                vec!["foundation.normalization:propose"]
            ),
        ]
    );
    Ok(())
}

#[test]
fn exact_environment_bindings_resolve_without_repeating_capabilities(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = parse_policy(POLICY)?;
    let bindings = parse_bindings(&bindings_json(&[
        binding("gongzzang-api", "zitadel-gongzzang-api"),
        binding("gongzzang-outbox-publisher", "zitadel-gongzzang-outbox"),
        binding("dawneer-web", "zitadel-dawneer-web"),
        binding(
            "intelligence-normalization",
            "zitadel-intelligence-normalization",
        ),
    ]))?;

    let resolved = resolve_manifest(policy, bindings)?;

    assert_eq!(resolved.principals.len(), 4);
    assert_eq!(
        resolved.principals[0].zitadel_subject,
        "zitadel-gongzzang-api"
    );
    assert_eq!(
        resolved.principals[0].capabilities[0].as_str(),
        "foundation.catalog:read"
    );
    Ok(())
}

#[test]
fn bindings_must_cover_policy_exactly() -> Result<(), Box<dyn std::error::Error>> {
    let policy = parse_policy(POLICY)?;
    let missing = parse_bindings(&bindings_json(&[
        binding("gongzzang-api", "zitadel-gongzzang-api"),
        binding("gongzzang-outbox-publisher", "zitadel-gongzzang-outbox"),
        binding("dawneer-web", "zitadel-dawneer-web"),
    ]))?;
    assert!(matches!(
        resolve_manifest(policy, missing),
        Err(ManifestError::MissingBinding)
    ));

    let policy = parse_policy(POLICY)?;
    let unknown = parse_bindings(&bindings_json(&[
        binding("gongzzang-api", "zitadel-gongzzang-api"),
        binding("gongzzang-outbox-publisher", "zitadel-gongzzang-outbox"),
        binding("dawneer-web", "zitadel-dawneer-web"),
        binding(
            "intelligence-normalization",
            "zitadel-intelligence-normalization",
        ),
        binding("unknown-service", "zitadel-unknown-service"),
    ]))?;
    assert!(matches!(
        resolve_manifest(policy, unknown),
        Err(ManifestError::UnknownBinding)
    ));
    Ok(())
}

#[test]
fn policy_rejects_invalid_or_duplicated_identity_and_capability_values() {
    for policy in [
        policy_json(&[
            principal("same", "foundation.catalog:read"),
            principal("same", "foundation.lakehouse:write"),
        ]),
        policy_json(&[principal("Invalid Slug", "foundation.catalog:read")]),
        policy_json(&[principal("service", "foundation.catalog")]),
        policy_json(&[principal("service", "Foundation.catalog:read")]),
    ] {
        assert!(parse_policy(&policy).is_err());
    }
}

#[test]
fn bindings_reject_placeholders_duplicate_services_and_duplicate_subjects() {
    for bindings in [
        BINDINGS_EXAMPLE.to_owned(),
        bindings_json(&[
            binding("service-a", "subject-a"),
            binding("service-a", "subject-b"),
        ]),
        bindings_json(&[
            binding("service-a", "same-subject"),
            binding("service-b", "same-subject"),
        ]),
    ] {
        assert!(parse_bindings(&bindings).is_err());
    }
}

#[test]
fn rejects_unknown_contract_versions() {
    assert!(matches!(
        parse_policy(
            &policy_json(&[principal("service", "foundation.catalog:read")]).replace(
                POLICY_SCHEMA_VERSION,
                "identity.workload-principal-policy.v2"
            )
        ),
        Err(ManifestError::UnsupportedSchemaVersion)
    ));
    assert!(matches!(
        parse_bindings(&bindings_json(&[binding("service", "subject")]).replace(
            BINDINGS_SCHEMA_VERSION,
            "identity.workload-principal-bindings.v2"
        )),
        Err(ManifestError::UnsupportedSchemaVersion)
    ));
}

fn policy_json(principals: &[Value]) -> String {
    json!({
        "schema_version": POLICY_SCHEMA_VERSION,
        "principals": principals,
    })
    .to_string()
}

fn bindings_json(bindings: &[Value]) -> String {
    json!({
        "schema_version": BINDINGS_SCHEMA_VERSION,
        "bindings": bindings,
    })
    .to_string()
}

fn principal(service_slug: &str, capability: &str) -> Value {
    json!({
        "service_slug": service_slug,
        "principal_id": "018f7c6a-0000-7000-8000-000000000101",
        "display_name": "Service",
        "capabilities": [capability],
    })
}

fn binding(service_slug: &str, subject: &str) -> Value {
    json!({
        "service_slug": service_slug,
        "zitadel_subject": subject,
    })
}
