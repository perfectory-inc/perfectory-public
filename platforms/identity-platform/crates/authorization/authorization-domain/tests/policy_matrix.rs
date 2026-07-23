//! Policy behavior at the authorization-domain boundary.

use authorization_domain::{evaluate_policy, Permission, PolicyDecision, PolicyInput, RoleCode};
use identity_contracts::ResourceAction;

#[test]
fn role_code_rejects_empty_and_non_screaming_snake_case_values() {
    assert!(RoleCode::parse("").is_err());
    assert!(RoleCode::parse("catalog_admin").is_err());
}

#[test]
fn permission_requires_a_resource_and_action_separator() -> Result<(), Box<dyn std::error::Error>> {
    let permission = Permission::parse("foundation.catalog:write")?;

    assert_eq!(permission.as_str(), "foundation.catalog:write");
    assert!(Permission::parse("foundation.catalog").is_err());
    assert!(Permission::parse("foundation:catalog:write").is_err());
    Ok(())
}

#[test]
fn only_master_admin_can_grant_staff_roles() -> Result<(), Box<dyn std::error::Error>> {
    let allow = PolicyInput::staff_role_grant(vec![RoleCode::parse("MASTER_ADMIN")?]);
    let deny = PolicyInput::staff_role_grant(vec![RoleCode::parse("CATALOG_ADMIN")?]);

    assert_eq!(evaluate_policy(&allow), PolicyDecision::allow("role_grant"));
    assert_eq!(
        evaluate_policy(&deny),
        PolicyDecision::deny("missing_master_admin")
    );
    Ok(())
}

#[test]
fn foundation_capability_mapping_is_owned_by_authorization_domain(
) -> Result<(), Box<dyn std::error::Error>> {
    let input = PolicyInput::resource_action(
        vec![RoleCode::parse("LAKEHOUSE_ADMIN")?],
        "foundation.lakehouse",
        "batch_audit",
    );

    assert!(evaluate_policy(&input).is_allowed());
    Ok(())
}

#[test]
fn catalog_admin_retains_lakehouse_batch_audit_access() -> Result<(), Box<dyn std::error::Error>> {
    let input = PolicyInput::resource_action(
        vec![RoleCode::parse("CATALOG_ADMIN")?],
        "foundation.lakehouse",
        "batch_audit",
    );

    assert!(evaluate_policy(&input).is_allowed());
    Ok(())
}

#[test]
fn catalog_admin_can_write_catalog() -> Result<(), Box<dyn std::error::Error>> {
    let input = PolicyInput::resource_action(
        vec![RoleCode::parse("CATALOG_ADMIN")?],
        "foundation.catalog",
        "write",
    );

    assert!(evaluate_policy(&input).is_allowed());
    Ok(())
}

#[test]
fn vector_tile_admin_can_administer_spatial_resources() -> Result<(), Box<dyn std::error::Error>> {
    for action in ["manifest_admin", "anchor_rebuild"] {
        let input = PolicyInput::resource_action(
            vec![RoleCode::parse("VECTOR_TILE_ADMIN")?],
            "foundation.spatial",
            action,
        );

        assert!(evaluate_policy(&input).is_allowed());
    }
    Ok(())
}

#[test]
fn unrelated_role_is_denied_capability_access() -> Result<(), Box<dyn std::error::Error>> {
    let input = PolicyInput::resource_action(
        vec![RoleCode::parse("COMPLEX_EDITOR")?],
        "foundation.catalog",
        "write",
    );

    assert_eq!(
        evaluate_policy(&input),
        PolicyDecision::deny("missing_capability")
    );
    Ok(())
}

#[test]
fn role_capability_boundaries_expose_decision_and_reason_code(
) -> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (
            "CATALOG_ADMIN",
            "foundation.spatial",
            "manifest_admin",
            ResourceAction::Deny,
            "missing_capability",
        ),
        (
            "CATALOG_ADMIN",
            "foundation.spatial",
            "anchor_rebuild",
            ResourceAction::Deny,
            "missing_capability",
        ),
        (
            "LAKEHOUSE_ADMIN",
            "foundation.catalog",
            "write",
            ResourceAction::Deny,
            "missing_capability",
        ),
        (
            "VECTOR_TILE_ADMIN",
            "foundation.catalog",
            "write",
            ResourceAction::Deny,
            "missing_capability",
        ),
        (
            "VECTOR_TILE_ADMIN",
            "foundation.lakehouse",
            "batch_audit",
            ResourceAction::Deny,
            "missing_capability",
        ),
        (
            "MASTER_ADMIN",
            "arbitrary.resource",
            "arbitrary_action",
            ResourceAction::Allow,
            "role_grant",
        ),
    ];

    for (role, resource, action, expected_decision, expected_reason_code) in cases {
        let input = PolicyInput::resource_action(vec![RoleCode::parse(role)?], resource, action);
        let decision = evaluate_policy(&input);

        assert_eq!(decision.decision(), &expected_decision);
        assert_eq!(decision.reason_code(), expected_reason_code);
    }

    Ok(())
}
