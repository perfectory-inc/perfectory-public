// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_application::{
    trace_context_from_principal, PermissionAction, PermissionCheck, PermissionDecision,
    PrincipalKind, PrincipalScope, VerifiedPrincipal,
};

fn valid_scope() -> PrincipalScope {
    PrincipalScope {
        tenant_id: "tenant-1".to_string(),
        product_id: "foundation-platform".to_string(),
        actions: vec![PermissionAction::ChatCompletions],
    }
}

// --- existing tests (adjusted) ---

#[test]
fn verified_principal_rejects_empty_tenant_scope() {
    let error = VerifiedPrincipal::service(
        "service:intelligence-api",
        vec![PrincipalScope {
            tenant_id: "".to_string(),
            product_id: "foundation-platform".to_string(),
            actions: vec![PermissionAction::ChatCompletions],
        }],
    )
    .unwrap_err();

    assert_eq!(error.safe_message(), "principal tenant scope is invalid");
}

#[test]
fn trace_context_is_derived_from_verified_principal() {
    let principal = VerifiedPrincipal::service(
        "service:foundation-platform",
        vec![PrincipalScope {
            tenant_id: "tenant-1".to_string(),
            product_id: "foundation-platform".to_string(),
            actions: vec![PermissionAction::SubmitNormalizationProposal],
        }],
    )
    .unwrap();

    let trace_context = trace_context_from_principal(
        "trace-runtime-1".to_string(),
        &principal,
        "tenant-1",
        "foundation-platform".to_string(),
    )
    .unwrap();

    assert_eq!(trace_context.trace_id, "trace-runtime-1");
    assert_eq!(trace_context.tenant_id, "tenant-1");
    assert_eq!(trace_context.human_user_id, "service:foundation-platform");
    assert_eq!(trace_context.product_id, "foundation-platform");
}

#[test]
fn permission_decision_matches_requested_scope_and_action() {
    let principal = VerifiedPrincipal::user(
        "user-1",
        vec![PrincipalScope {
            tenant_id: "tenant-1".to_string(),
            product_id: "gongzzang".to_string(),
            actions: vec![PermissionAction::ChatCompletions],
        }],
    )
    .unwrap();

    let allowed = PermissionDecision::from_principal(
        &principal,
        &PermissionCheck {
            tenant_id: "tenant-1".to_string(),
            product_id: "gongzzang".to_string(),
            action: PermissionAction::ChatCompletions,
        },
    );
    let denied = PermissionDecision::from_principal(
        &principal,
        &PermissionCheck {
            tenant_id: "tenant-2".to_string(),
            product_id: "gongzzang".to_string(),
            action: PermissionAction::ChatCompletions,
        },
    );

    assert!(allowed.allowed);
    assert!(!denied.allowed);
    assert_eq!(principal.kind, PrincipalKind::User);
}

// --- new tests ---

#[test]
fn rejects_empty_subject_id() {
    let error = VerifiedPrincipal::service("  ", vec![valid_scope()]).unwrap_err();
    assert_eq!(error.safe_message(), "principal tenant scope is invalid");
}

#[test]
fn rejects_empty_scopes() {
    let error = VerifiedPrincipal::service("service:foo", vec![]).unwrap_err();
    assert_eq!(error.safe_message(), "principal tenant scope is invalid");
}

#[test]
fn rejects_scope_without_actions() {
    let error = VerifiedPrincipal::service(
        "service:foo",
        vec![PrincipalScope {
            tenant_id: "tenant-1".to_string(),
            product_id: "foundation-platform".to_string(),
            actions: vec![],
        }],
    )
    .unwrap_err();
    assert_eq!(error.safe_message(), "principal tenant scope is invalid");
}

#[test]
fn normalizes_padded_scope_identifiers() {
    let principal = VerifiedPrincipal::service(
        "service:foo",
        vec![PrincipalScope {
            tenant_id: " tenant-1 ".to_string(),
            product_id: " foundation-platform ".to_string(),
            actions: vec![PermissionAction::ChatCompletions],
        }],
    )
    .unwrap();

    assert_eq!(principal.scopes[0].tenant_id, "tenant-1");
    assert_eq!(principal.scopes[0].product_id, "foundation-platform");
}

#[test]
fn permission_decision_matches_second_scope() {
    let principal = VerifiedPrincipal::user(
        "user-multi",
        vec![
            PrincipalScope {
                tenant_id: "tenant-1".to_string(),
                product_id: "foundation-platform".to_string(),
                actions: vec![PermissionAction::ChatCompletions],
            },
            PrincipalScope {
                tenant_id: "tenant-2".to_string(),
                product_id: "foundation-platform".to_string(),
                actions: vec![PermissionAction::RetrieveKnowledge],
            },
        ],
    )
    .unwrap();

    let decision = PermissionDecision::from_principal(
        &principal,
        &PermissionCheck {
            tenant_id: "tenant-2".to_string(),
            product_id: "foundation-platform".to_string(),
            action: PermissionAction::RetrieveKnowledge,
        },
    );
    assert!(decision.allowed);
}

#[test]
fn trace_context_rejects_unscoped_tenant() {
    let principal = VerifiedPrincipal::service("service:foo", vec![valid_scope()]).unwrap();

    let err = trace_context_from_principal(
        "trace-x".to_string(),
        &principal,
        "tenant-9",
        "foundation-platform".to_string(),
    )
    .unwrap_err();

    assert_eq!(err.safe_message(), "authorization failed");
}

#[test]
fn new_for_kind_builds_user_principal() {
    let principal =
        VerifiedPrincipal::new_for_kind("user-42", PrincipalKind::User, vec![valid_scope()])
            .unwrap();

    assert_eq!(principal.kind, PrincipalKind::User);
}
