//! HTTP composition and published-contract tests using explicit fake ports.

use std::error::Error;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use authorization_application::ports::{RoleGrantPersistenceError, RoleGrantUnitOfWork};
use authorization_application::{AssignStaffRole, EvaluateAccess};
use authorization_domain::{Permission, RoleCode, RoleGrant};
use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{Duration, TimeZone, Utc};
use identity_api::state::{AppState, ReadinessProbe};
use identity_api::{openapi_document, router};
use identity_contracts::{
    PolicyDecisionResponse, PrincipalId, ResourceAction, StaffRoleResponse,
    VerifyStaffSessionResponse,
};
use identity_shared_kernel::StaffId;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::{json, Value};
use service_identity_application::ports::{
    IdentityAuditSink, ServiceAuthorizationAudit, ServiceCredentialVerifier,
};
use service_identity_application::AuthorizeServiceCall;
use service_identity_domain::{ServiceIdentityError, ValidatedServicePrincipal};
use service_identity_infrastructure::{ServicePrincipalReader, ZitadelMachineTokenVerifier};
use staff_identity_application::ports::{
    EffectiveRoleReader, OidcVerifier, StaffRepository, StaffSessionUnitOfWork, VerifiedOidcClaims,
};
use staff_identity_application::VerifyStaffSession;
use staff_identity_domain::{Staff, StaffIdentityError, StaffSession};
use staff_identity_infrastructure::ZitadelOidcVerifier;
use tower::ServiceExt;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ACTOR_ID: Uuid = Uuid::from_u128(1);
const TARGET_ID: Uuid = Uuid::from_u128(2);
const SERVICE_ID: Uuid = Uuid::from_u128(3);
const USER_ID: Uuid = Uuid::from_u128(4);
const EXPIRES_AT: i64 = 4_102_444_800;
const COLLISION_SUBJECT: &str = "shared-zitadel-subject";
const TEST_KID: &str = "rsa-test-key";
const TEST_RSA_MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const TEST_RSA_PRIVATE_KEY_BODY: &str = r"MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJETqse41HRBsc
7cfcq3ak4oZWFCoZlcic525A3FfO4qW9BMtRO/iXiyCCHn8JhiL9y8j5JdVP2Q9Z
IpfElcFd3/guS9w+5RqQGgCR+H56IVUyHZWtTJbKPcwWXQdNUX0rBFcsBzCRESJL
eelOEdHIjG7LRkx5l/FUvlqsyHDVJEQsHwegZ8b8C0fz0EgT2MMEdn10t6Ur1rXz
jMB/wvCg8vG8lvciXmedyo9xJ8oMOh0wUEgxziVDMMovmC+aJctcHUAYubwoGN8T
yzcvnGqL7JSh36Pwy28iPzXZ2RLhAyJFU39vLaHdljwthUaupldlNyCfa6Ofy4qN
ctlUPlN1AgMBAAECggEAdESTQjQ70O8QIp1ZSkCYXeZjuhj081CK7jhhp/4ChK7J
GlFQZMwiBze7d6K84TwAtfQGZhQ7km25E1kOm+3hIDCoKdVSKch/oL54f/BK6sKl
qlIzQEAenho4DuKCm3I4yAw9gEc0DV70DuMTR0LEpYyXcNJY3KNBOTjN5EYQAR9s
2MeurpgK2MdJlIuZaIbzSGd+diiz2E6vkmcufJLtmYUT/k/ddWvEtz+1DnO6bRHh
xuuDMeJA/lGB/EYloSLtdyCF6sII6C6slJJtgfb0bPy7l8VtL5iDyz46IKyzdyzW
tKAn394dm7MYR1RlUBEfqFUyNK7C+pVMVoTwCC2V4QKBgQD64syfiQ2oeUlLYDm4
CcKSP3RnES02bcTyEDFSuGyyS1jldI4A8GXHJ/lG5EYgiYa1RUivge4lJrlNfjyf
dV230xgKms7+JiXqag1FI+3mqjAgg4mYiNjaao8N8O3/PD59wMPeWYImsWXNyeHS
55rUKiHERtCcvdzKl4u35ZtTqQKBgQDNKnX2bVqOJ4WSqCgHRhOm386ugPHfy+8j
m6cicmUR46ND6ggBB03bCnEG9OtGisxTo/TuYVRu3WP4KjoJs2LD5fwdwJqpgtHl
yVsk45Y1Hfo+7M6lAuR8rzCi6kHHNb0HyBmZjysHWZsn79ZM+sQnLpgaYgQGRbKV
DZWlbw7g7QKBgQCl1u+98UGXAP1jFutwbPsx40IVszP4y5ypCe0gqgon3UiY/G+1
zTLp79GGe/SjI2VpQ7AlW7TI2A0bXXvDSDi3/5Dfya9ULnFXv9yfvH1QwWToySpW
Kvd1gYSoiX84/WCtjZOr0e0HmLIb0vw0hqZA4szJSqoxQgvF22EfIWaIaQKBgQCf
34+OmMYw8fEvSCPxDxVvOwW2i7pvV14hFEDYIeZKW2W1HWBhVMzBfFB5SE8yaCQy
pRfOzj9aKOCm2FjjiErVNpkQoi6jGtLvScnhZAt/lr2TXTrl8OwVkPrIaN0bG/AS
aUYxmBPCpXu3UjhfQiWqFq/mFyzlqlgvuCc9g95HPQKBgAscKP8mLxdKwOgX8yFW
GcZ0izY/30012ajdHY+/QK5lsMoxTnn0skdS+spLxaS5ZEO4qvPVb8RAoCkWMMal
2pOhmquJQVDPDLuZHdrIiKiDM20dy9sMfHygWcZjQ4WSxf/J7T9canLZIXFhHAZT
3wc9h4G8BBCtWN2TN/LsGZdB";

#[tokio::test]
async fn protected_routes_require_a_well_formed_bearer_header() -> Result<(), Box<dyn Error>> {
    for (method, uri, body) in [
        (Method::POST, "/identity/v1/staff/sessions/verify", None),
        (Method::POST, "/identity/v1/staff/sessions/revoke", None),
        (
            Method::POST,
            "/identity/v1/staff/00000000-0000-0000-0000-000000000002/roles",
            Some(json!({"role_code": "CATALOG_ADMIN"})),
        ),
        (
            Method::POST,
            "/identity/v1/policy/decisions",
            Some(policy_body()),
        ),
    ] {
        for authorization in [
            None,
            Some("Basic abc".to_owned()),
            Some("Bearer".to_owned()),
            Some("Bearer a b".to_owned()),
        ] {
            let response = send(
                &test_harness(true).app,
                method.clone(),
                uri,
                authorization,
                body.clone(),
                &[],
            )
            .await?;
            assert_eq!(response.status, StatusCode::UNAUTHORIZED);
        }
    }
    Ok(())
}

#[tokio::test]
async fn verify_session_invokes_the_use_case_once_and_maps_the_full_response(
) -> Result<(), Box<dyn Error>> {
    let harness = test_harness(true);

    let response = send(
        &harness.app,
        Method::POST,
        "/identity/v1/staff/sessions/verify",
        Some(bearer(Some("staff"), "staff-admin")),
        None,
        &[],
    )
    .await?;

    assert_eq!(response.status, StatusCode::OK);
    let body: VerifyStaffSessionResponse = serde_json::from_slice(&response.body)?;
    assert_eq!(body.principal_id, PrincipalId::new(ACTOR_ID));
    assert_eq!(body.email, "admin@example.test");
    assert_eq!(body.display_name, "Admin");
    assert_eq!(body.roles, vec!["CATALOG_ADMIN", "MASTER_ADMIN"]);
    assert_eq!(body.expires_at.timestamp(), EXPIRES_AT);
    assert_eq!(count_event(&harness.events, "staff.verify"), 1);
    assert!(!String::from_utf8(response.body)?.contains("staff-admin"));
    Ok(())
}

#[tokio::test]
async fn revoke_route_verifies_the_current_session_before_recording_logout(
) -> Result<(), Box<dyn Error>> {
    let harness = test_harness(true);
    let response = send(
        &harness.app,
        Method::POST,
        "/identity/v1/staff/sessions/revoke",
        Some(bearer(Some("staff"), "staff-admin")),
        None,
        &[],
    )
    .await?;

    assert_eq!(response.status, StatusCode::NO_CONTENT);
    assert_eq!(
        event_snapshot(&harness.events),
        vec!["staff.verify", "session.persist", "session.revoke"]
    );
    Ok(())
}

#[tokio::test]
async fn credential_failures_are_unauthorized_and_never_echo_credentials(
) -> Result<(), Box<dyn Error>> {
    for token in ["invalid-secret", "expired-secret", "revoked-secret"] {
        let response = send(
            &test_harness(true).app,
            Method::POST,
            "/identity/v1/staff/sessions/verify",
            Some(bearer(Some("staff"), token)),
            None,
            &[],
        )
        .await?;
        assert_eq!(response.status, StatusCode::UNAUTHORIZED);
        assert!(!String::from_utf8(response.body)?.contains(token));
    }
    Ok(())
}

#[tokio::test]
async fn role_assignment_verifies_first_and_uses_only_trusted_actor_context(
) -> Result<(), Box<dyn Error>> {
    let harness = test_harness(true);
    let response = send(
        &harness.app,
        Method::POST,
        "/identity/v1/staff/00000000-0000-0000-0000-000000000002/roles",
        Some(bearer(Some("staff"), "staff-admin")),
        Some(json!({"role_code": "CATALOG_ADMIN"})),
        &[("x-actor-id", "00000000-0000-0000-0000-000000000099")],
    )
    .await?;

    assert_eq!(response.status, StatusCode::OK);
    let body: StaffRoleResponse = serde_json::from_slice(&response.body)?;
    assert_eq!(body.principal_id, PrincipalId::new(TARGET_ID));
    assert_eq!(body.granted_by, PrincipalId::new(ACTOR_ID));
    assert_eq!(
        event_snapshot(&harness.events),
        vec!["staff.verify", "session.persist", "role.assign"]
    );
    Ok(())
}

#[tokio::test]
async fn role_assignment_maps_denied_duplicate_and_infrastructure_failures(
) -> Result<(), Box<dyn Error>> {
    let denied = send(
        &test_harness(true).app,
        Method::POST,
        "/identity/v1/staff/00000000-0000-0000-0000-000000000002/roles",
        Some(bearer(Some("staff"), "staff-user")),
        Some(json!({"role_code": "CATALOG_ADMIN"})),
        &[],
    )
    .await?;
    assert_eq!(denied.status, StatusCode::FORBIDDEN);

    let duplicate = send(
        &test_harness(true).app,
        Method::POST,
        "/identity/v1/staff/00000000-0000-0000-0000-000000000002/roles",
        Some(bearer(Some("staff"), "staff-admin")),
        Some(json!({"role_code": "DUPLICATE"})),
        &[],
    )
    .await?;
    assert_eq!(duplicate.status, StatusCode::CONFLICT);

    let infrastructure = send(
        &test_harness(true).app,
        Method::POST,
        "/identity/v1/staff/00000000-0000-0000-0000-000000000002/roles",
        Some(bearer(Some("staff"), "staff-admin")),
        Some(json!({"role_code": "INFRA"})),
        &[],
    )
    .await?;
    assert_opaque_internal_error(&infrastructure, &["database-detail", "staff-admin"])?;
    Ok(())
}

#[tokio::test]
async fn policy_uses_server_owned_staff_or_service_path_and_returns_typed_decisions(
) -> Result<(), Box<dyn Error>> {
    let staff = test_harness(true);
    let staff_response = send(
        &staff.app,
        Method::POST,
        "/identity/v1/policy/decisions",
        Some(bearer(Some("staff"), "staff-admin")),
        Some(policy_body()),
        &[("x-principal-type", "service")],
    )
    .await?;
    assert_eq!(staff_response.status, StatusCode::OK);
    let staff_body: PolicyDecisionResponse = serde_json::from_slice(&staff_response.body)?;
    assert_eq!(staff_body.principal_id, PrincipalId::new(ACTOR_ID));
    assert_eq!(staff_body.decision, ResourceAction::Allow);
    assert_eq!(count_event(&staff.events, "service.verify"), 0);

    let service = test_harness(true);
    let service_response = send(
        &service.app,
        Method::POST,
        "/identity/v1/policy/decisions",
        Some(bearer(Some("service"), "service-deny")),
        Some(policy_body()),
        &[("x-principal-type", "staff")],
    )
    .await?;
    assert_eq!(service_response.status, StatusCode::OK);
    let service_body: PolicyDecisionResponse = serde_json::from_slice(&service_response.body)?;
    assert_eq!(service_body.principal_id, PrincipalId::new(SERVICE_ID));
    assert_eq!(service_body.decision, ResourceAction::Deny);
    assert_eq!(service_body.reason_code, "missing_service_capability");
    assert_eq!(
        event_snapshot(&service.events),
        vec!["service.verify", "service.audit"]
    );
    Ok(())
}

#[tokio::test]
async fn policy_fails_closed_on_staff_infrastructure_failure_without_service_fallback(
) -> Result<(), Box<dyn Error>> {
    let harness = test_harness(true);
    let response = send(
        &harness.app,
        Method::POST,
        "/identity/v1/policy/decisions",
        Some(bearer(Some("staff"), "staff-infra")),
        Some(policy_body()),
        &[],
    )
    .await?;

    assert_opaque_internal_error(&response, &["database-detail", "staff-infra"])?;
    assert_eq!(count_event(&harness.events, "service.verify"), 0);
    Ok(())
}

#[tokio::test]
async fn policy_rejects_missing_and_unknown_principal_kind_without_invoking_a_verifier(
) -> Result<(), Box<dyn Error>> {
    for kind in [None, Some("unknown")] {
        let harness = test_harness(true);
        let response = send(
            &harness.app,
            Method::POST,
            "/identity/v1/policy/decisions",
            Some(bearer(kind, "collision-subject")),
            Some(policy_body()),
            &[],
        )
        .await?;

        assert_eq!(response.status, StatusCode::UNAUTHORIZED);
        assert!(event_snapshot(&harness.events).is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn signed_kind_routing_keeps_a_colliding_subject_on_its_declared_principal_path(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jwks_uri": format!("{}/keys", server.uri()),
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/keys"))
        .respond_with(ResponseTemplate::new(200).set_body_json(test_jwks_json()))
        .mount(&server)
        .await;
    let harness = real_verifier_harness(&server.uri());

    let staff_token = signed_token(&server.uri(), "staff", COLLISION_SUBJECT)?;
    let staff_response = send(
        &harness.app,
        Method::POST,
        "/identity/v1/policy/decisions",
        Some(format!("Bearer {staff_token}")),
        Some(policy_body()),
        &[],
    )
    .await?;
    assert_eq!(staff_response.status, StatusCode::OK);
    let staff_body: PolicyDecisionResponse = serde_json::from_slice(&staff_response.body)?;
    assert_eq!(staff_body.principal_id, PrincipalId::new(ACTOR_ID));
    assert_eq!(staff_body.decision, ResourceAction::Allow);

    let service_token = signed_token(&server.uri(), "service", COLLISION_SUBJECT)?;
    let service_response = send(
        &harness.app,
        Method::POST,
        "/identity/v1/policy/decisions",
        Some(format!("Bearer {service_token}")),
        Some(policy_body()),
        &[],
    )
    .await?;
    assert_eq!(service_response.status, StatusCode::OK);
    let service_body: PolicyDecisionResponse = serde_json::from_slice(&service_response.body)?;
    assert_eq!(service_body.principal_id, PrincipalId::new(SERVICE_ID));
    assert_eq!(service_body.decision, ResourceAction::Deny);

    let tampered = replace_principal_kind_without_resigning(&staff_token, "service")?;
    let tampered_response = send(
        &harness.app,
        Method::POST,
        "/identity/v1/policy/decisions",
        Some(format!("Bearer {tampered}")),
        Some(policy_body()),
        &[],
    )
    .await?;
    assert_eq!(tampered_response.status, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn selected_verifier_infrastructure_and_timeout_failures_are_opaque_500s(
) -> Result<(), Box<dyn Error>> {
    for (kind, subject, expected_event, forbidden) in [
        ("staff", "staff-infra", "staff.verify", "database-detail"),
        (
            "service",
            "service-timeout",
            "service.verify",
            "zitadel-timeout-detail",
        ),
    ] {
        let harness = test_harness(true);
        let response = send(
            &harness.app,
            Method::POST,
            "/identity/v1/policy/decisions",
            Some(bearer(Some(kind), subject)),
            Some(policy_body()),
            &[],
        )
        .await?;

        assert_opaque_internal_error(&response, &[forbidden, subject])?;
        assert_eq!(event_snapshot(&harness.events), vec![expected_event]);
    }
    Ok(())
}

#[tokio::test]
async fn body_credentials_are_ignored_without_authorization_header() -> Result<(), Box<dyn Error>> {
    let response = send(
        &test_harness(true).app,
        Method::POST,
        "/identity/v1/staff/sessions/verify",
        None,
        Some(json!({"token": "body-secret"})),
        &[],
    )
    .await?;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert!(!String::from_utf8(response.body)?.contains("body-secret"));
    Ok(())
}

#[tokio::test]
async fn malformed_and_wrong_content_type_json_use_the_documented_error_envelope(
) -> Result<(), Box<dyn Error>> {
    for (uri, content_type, body) in [
        (
            "/identity/v1/policy/decisions",
            "application/json",
            "{not-json",
        ),
        (
            "/identity/v1/staff/00000000-0000-0000-0000-000000000002/roles",
            "text/plain",
            r#"{"role_code":"CATALOG_ADMIN"}"#,
        ),
    ] {
        let response = send_raw(
            &test_harness(true).app,
            uri,
            bearer(Some("staff"), "staff-admin"),
            content_type,
            body,
        )
        .await?;

        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert_eq!(response.content_type.as_deref(), Some("application/json"));
        let envelope: Value = serde_json::from_slice(&response.body)?;
        assert_eq!(envelope["code"], "invalid_request");
        assert!(envelope["correlation_id"].is_null());
    }
    Ok(())
}

#[tokio::test]
async fn liveness_and_readiness_report_only_safe_wiring_state() -> Result<(), Box<dyn Error>> {
    let live = send(
        &test_harness(true).app,
        Method::GET,
        "/healthz",
        None,
        None,
        &[],
    )
    .await?;
    assert_eq!(live.status, StatusCode::OK);

    let ready = send(
        &test_harness(true).app,
        Method::GET,
        "/readyz",
        None,
        None,
        &[],
    )
    .await?;
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&ready.body)?["status"],
        "ready"
    );
    let ready_body: Value = serde_json::from_slice(&ready.body)?;
    assert_eq!(ready_body["verifier_configuration"], "valid");
    assert!(ready_body["verifier_wiring"].is_null());

    let unavailable = send(
        &test_harness(false).app,
        Method::GET,
        "/readyz",
        None,
        None,
        &[],
    )
    .await?;
    assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
    let text = String::from_utf8(unavailable.body)?;
    assert!(!text.contains("DATABASE_URL"));
    assert!(!text.contains("secret"));

    Ok(())
}

#[tokio::test]
async fn router_exposes_exact_routes_and_no_http_bootstrap_route() -> Result<(), Box<dyn Error>> {
    for uri in [
        "/bootstrap",
        "/identity/v1/bootstrap",
        "/health",
        "/ready",
        "/health/live",
        "/health/ready",
    ] {
        let response = send(&test_harness(true).app, Method::POST, uri, None, None, &[]).await?;
        assert_eq!(response.status, StatusCode::NOT_FOUND, "{uri}");
    }
    Ok(())
}

#[test]
fn openapi_declares_header_bearer_security_without_credential_request_fields(
) -> Result<(), Box<dyn Error>> {
    let document = serde_json::to_value(openapi_document())?;
    let paths = document["paths"].as_object().ok_or("paths")?;
    assert_eq!(
        paths.keys().cloned().collect::<Vec<_>>(),
        vec![
            "/healthz",
            "/identity/v1/policy/decisions",
            "/identity/v1/staff/sessions/revoke",
            "/identity/v1/staff/sessions/verify",
            "/identity/v1/staff/{staff_id}/roles",
            "/readyz",
        ]
    );
    assert!(document["components"]["securitySchemes"]["bearerAuth"].is_object());
    let schemas = &document["components"]["schemas"];
    let schemas_text = serde_json::to_string(schemas)?;
    for forbidden in ["bearer_token", "id_token", "access_token", "token"] {
        assert!(
            !schemas_text.contains(forbidden),
            "{forbidden} leaked into schemas"
        );
    }
    assert!(
        document["paths"]["/identity/v1/staff/sessions/verify"]["post"]["requestBody"].is_null()
    );
    assert!(document["paths"]["/identity/v1/bootstrap"].is_null());
    for path in [
        "/identity/v1/policy/decisions",
        "/identity/v1/staff/{staff_id}/roles",
    ] {
        assert!(document["paths"][path]["post"]["responses"]["400"].is_object());
    }
    Ok(())
}

struct Harness {
    app: axum::Router,
    events: Arc<Mutex<Vec<&'static str>>>,
}

fn test_harness(database_ready: bool) -> Harness {
    let events = Arc::new(Mutex::new(Vec::new()));
    let staff_repository = Arc::new(FakeStaffRepository);
    let session_uow = Arc::new(FakeSessionUnitOfWork {
        events: events.clone(),
    });
    let verify = Arc::new(VerifyStaffSession::new(
        staff_repository,
        session_uow.clone(),
        Arc::new(FakeRoleReader),
        Arc::new(FakeOidcVerifier {
            events: events.clone(),
        }),
    ));
    let revoke = Arc::new(staff_identity_application::RevokeStaffSession::new(
        session_uow,
    ));
    let assign = Arc::new(AssignStaffRole::new(Arc::new(FakeRoleGrantUnitOfWork {
        events: events.clone(),
    })));
    let authorize = Arc::new(AuthorizeServiceCall::new(Arc::new(FakeAuditSink {
        events: events.clone(),
    })));
    let state = AppState::new(
        verify,
        revoke,
        assign,
        EvaluateAccess::new(),
        Arc::new(FakeServiceVerifier {
            events: events.clone(),
        }),
        authorize,
        Arc::new(FakeReadinessProbe(database_ready)),
    );
    Harness {
        app: router(Arc::new(state)),
        events,
    }
}

fn real_verifier_harness(issuer: &str) -> Harness {
    let events = Arc::new(Mutex::new(Vec::new()));
    let session_uow = Arc::new(FakeSessionUnitOfWork {
        events: events.clone(),
    });
    let verify = Arc::new(VerifyStaffSession::new(
        Arc::new(FakeStaffRepository),
        session_uow.clone(),
        Arc::new(FakeRoleReader),
        Arc::new(ZitadelOidcVerifier::new(issuer, "identity-api")),
    ));
    let revoke = Arc::new(staff_identity_application::RevokeStaffSession::new(
        session_uow,
    ));
    let assign = Arc::new(AssignStaffRole::new(Arc::new(FakeRoleGrantUnitOfWork {
        events: events.clone(),
    })));
    let authorize = Arc::new(AuthorizeServiceCall::new(Arc::new(FakeAuditSink {
        events: events.clone(),
    })));
    let service_verifier = Arc::new(ZitadelMachineTokenVerifier::new(
        issuer,
        "identity-api",
        Arc::new(CollisionServiceReader),
    ));
    let state = AppState::new(
        verify,
        revoke,
        assign,
        EvaluateAccess::new(),
        service_verifier,
        authorize,
        Arc::new(FakeReadinessProbe(true)),
    );
    Harness {
        app: router(Arc::new(state)),
        events,
    }
}

struct FakeOidcVerifier {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl OidcVerifier for FakeOidcVerifier {
    async fn verify_bearer(&self, token: &str) -> Result<VerifiedOidcClaims, StaffIdentityError> {
        record(&self.events, "staff.verify");
        let (kind, subject) = test_token_claims(token).ok_or_else(|| {
            StaffIdentityError::InvalidClaims("malformed test credential".to_owned())
        })?;
        if kind.as_deref() != Some("staff") {
            return Err(StaffIdentityError::InvalidClaims(
                "principal_kind must be staff".to_owned(),
            ));
        }
        match subject.as_str() {
            "invalid-secret" => Err(StaffIdentityError::InvalidClaims(
                "bad-signature".to_owned(),
            )),
            "expired-secret" => Err(StaffIdentityError::SessionExpired),
            "staff-infra" => Err(StaffIdentityError::Infrastructure(
                "database-detail".to_owned(),
            )),
            value => Ok(VerifiedOidcClaims {
                subject: value.to_owned(),
                jti: if value == "revoked-secret" {
                    "revoked"
                } else {
                    "active"
                }
                .to_owned(),
                issued_at: Utc::now() - Duration::minutes(1),
                expires_at: Utc
                    .timestamp_opt(EXPIRES_AT, 0)
                    .single()
                    .unwrap_or_else(Utc::now),
            }),
        }
    }
}

struct FakeStaffRepository;

#[async_trait]
impl StaffRepository for FakeStaffRepository {
    async fn find_by_zitadel_subject(
        &self,
        subject: &str,
    ) -> Result<Option<Staff>, StaffIdentityError> {
        let (id, role) = match subject {
            "staff-admin" | COLLISION_SUBJECT => (ACTOR_ID, "MASTER_ADMIN"),
            "staff-user" => (USER_ID, "CATALOG_VIEWER"),
            _ => return Ok(None),
        };
        Ok(Some(Staff {
            id: StaffId::new(id),
            zitadel_subject: subject.to_owned(),
            email: "admin@example.test".to_owned(),
            display_name: "Admin".to_owned(),
            primary_role_code: role.to_owned(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 1,
        }))
    }

    async fn is_jti_revoked(&self, jti: &str) -> Result<bool, StaffIdentityError> {
        Ok(jti == "revoked")
    }
}

struct CollisionServiceReader;

#[async_trait]
impl ServicePrincipalReader for CollisionServiceReader {
    async fn read_by_zitadel_subject(
        &self,
        subject: &str,
    ) -> Result<Option<ValidatedServicePrincipal>, ServiceIdentityError> {
        Ok(
            (subject == COLLISION_SUBJECT).then(|| ValidatedServicePrincipal {
                principal_id: PrincipalId::new(SERVICE_ID),
                capabilities: Vec::new(),
            }),
        )
    }
}

struct FakeSessionUnitOfWork {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl StaffSessionUnitOfWork for FakeSessionUnitOfWork {
    async fn persist_verified_session(
        &self,
        _session: &StaffSession,
    ) -> Result<(), StaffIdentityError> {
        record(&self.events, "session.persist");
        Ok(())
    }

    async fn revoke_jti(
        &self,
        _jti: &str,
        _reason: &str,
        _revoked_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StaffIdentityError> {
        record(&self.events, "session.revoke");
        Ok(())
    }
}

struct FakeRoleReader;

#[async_trait]
impl EffectiveRoleReader for FakeRoleReader {
    async fn read_effective_roles(
        &self,
        staff_id: StaffId,
    ) -> Result<Vec<RoleCode>, StaffIdentityError> {
        let names = if staff_id == StaffId::new(ACTOR_ID) {
            vec!["MASTER_ADMIN", "CATALOG_ADMIN"]
        } else {
            vec!["CATALOG_VIEWER"]
        };
        names
            .into_iter()
            .map(|name| {
                RoleCode::parse(name)
                    .map_err(|error| StaffIdentityError::Infrastructure(error.to_string()))
            })
            .collect()
    }
}

struct FakeRoleGrantUnitOfWork {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl RoleGrantUnitOfWork for FakeRoleGrantUnitOfWork {
    async fn assign_role(
        &self,
        staff_id: StaffId,
        role_code: &RoleCode,
        granted_by: StaffId,
    ) -> Result<RoleGrant, RoleGrantPersistenceError> {
        record(&self.events, "role.assign");
        match role_code.as_str() {
            "DUPLICATE" => Err(RoleGrantPersistenceError::DuplicateRole),
            "INFRA" => Err(RoleGrantPersistenceError::Infrastructure(
                "database-detail".to_owned(),
            )),
            _ => Ok(RoleGrant {
                staff_id,
                role_code: role_code.clone(),
                granted_at: Utc::now(),
                granted_by,
            }),
        }
    }
}

struct FakeServiceVerifier {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl ServiceCredentialVerifier for FakeServiceVerifier {
    async fn verify_credential(
        &self,
        token: &str,
    ) -> Result<ValidatedServicePrincipal, ServiceIdentityError> {
        record(&self.events, "service.verify");
        let (kind, subject) =
            test_token_claims(token).ok_or(ServiceIdentityError::InvalidCredential)?;
        if kind.as_deref() != Some("service") || !subject.starts_with("service-") {
            return Err(ServiceIdentityError::InvalidCredential);
        }
        if subject == "service-timeout" {
            return Err(ServiceIdentityError::Infrastructure(
                "zitadel-timeout-detail".to_owned(),
            ));
        }
        let capabilities = if subject == "service-allow" {
            vec![Permission::parse("catalog:read")
                .map_err(|error| ServiceIdentityError::Infrastructure(error.to_string()))?]
        } else {
            Vec::new()
        };
        Ok(ValidatedServicePrincipal {
            principal_id: PrincipalId::new(SERVICE_ID),
            capabilities,
        })
    }
}

struct FakeAuditSink {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl IdentityAuditSink for FakeAuditSink {
    async fn record_service_authorization(
        &self,
        _audit: &ServiceAuthorizationAudit,
    ) -> Result<(), ServiceIdentityError> {
        record(&self.events, "service.audit");
        Ok(())
    }
}

struct FakeReadinessProbe(bool);

#[async_trait]
impl ReadinessProbe for FakeReadinessProbe {
    async fn database_ready(&self) -> bool {
        self.0
    }
}

struct TestResponse {
    status: StatusCode,
    content_type: Option<String>,
    body: Vec<u8>,
}

async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    authorization: Option<String>,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> Result<TestResponse, Box<dyn Error>> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(value) = authorization {
        builder = builder.header(header::AUTHORIZATION, value);
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request_body = if let Some(value) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        Body::from(serde_json::to_vec(&value)?)
    } else {
        Body::empty()
    };
    collect_response(app.clone().oneshot(builder.body(request_body)?).await?).await
}

async fn send_raw(
    app: &axum::Router,
    uri: &str,
    authorization: String,
    content_type: &str,
    body: &str,
) -> Result<TestResponse, Box<dyn Error>> {
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::AUTHORIZATION, authorization)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body.to_owned()))?;
    collect_response(app.clone().oneshot(request).await?).await
}

async fn collect_response(
    response: axum::response::Response,
) -> Result<TestResponse, Box<dyn Error>> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = to_bytes(response.into_body(), usize::MAX).await?.to_vec();
    Ok(TestResponse {
        status,
        content_type,
        body,
    })
}

fn bearer(principal_kind: Option<&str>, subject: &str) -> String {
    format!("Bearer {}", test_token(principal_kind, subject))
}

fn test_token(principal_kind: Option<&str>, subject: &str) -> String {
    let payload = json!({
        "principal_kind": principal_kind,
        "sub": subject,
    });
    format!(
        "e30.{}.test-signature",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap_or_default())
    )
}

fn test_token_claims(token: &str) -> Option<(Option<String>, String)> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: Value = serde_json::from_slice(&decoded).ok()?;
    Some((
        claims["principal_kind"].as_str().map(str::to_owned),
        claims["sub"].as_str()?.to_owned(),
    ))
}

#[derive(Serialize)]
struct SignedTestClaims<'a> {
    sub: &'a str,
    jti: &'a str,
    iat: i64,
    exp: i64,
    iss: &'a str,
    aud: &'a str,
    principal_kind: &'a str,
}

fn signed_token(
    issuer: &str,
    principal_kind: &str,
    subject: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KID.to_owned());
    let private_key = format!(
        "-----BEGIN {label}-----\n{TEST_RSA_PRIVATE_KEY_BODY}\n-----END {label}-----",
        label = "PRIVATE KEY"
    );
    let now = Utc::now().timestamp();
    encode(
        &header,
        &SignedTestClaims {
            sub: subject,
            jti: "integration-test-jti",
            iat: now,
            exp: now + 300,
            iss: issuer,
            aud: "identity-api",
            principal_kind,
        },
        &EncodingKey::from_rsa_pem(private_key.as_bytes())?,
    )
}

fn replace_principal_kind_without_resigning(
    token: &str,
    principal_kind: &str,
) -> Result<String, Box<dyn Error>> {
    let mut segments = token.split('.');
    let header = segments.next().ok_or("header")?;
    let payload = segments.next().ok_or("payload")?;
    let signature = segments.next().ok_or("signature")?;
    let mut claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
    claims["principal_kind"] = Value::String(principal_kind.to_owned());
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
    Ok(format!("{header}.{payload}.{signature}"))
}

fn test_jwks_json() -> Value {
    json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "kid": TEST_KID,
            "alg": "RS256",
            "n": TEST_RSA_MODULUS,
            "e": "AQAB"
        }]
    })
}

fn policy_body() -> Value {
    json!({
        "resource": "catalog",
        "action": "read",
        "resource_id": "parcel-1",
        "trace_id": "trace-1"
    })
}

fn assert_opaque_internal_error(
    response: &TestResponse,
    forbidden: &[&str],
) -> Result<(), Box<dyn Error>> {
    assert_eq!(response.status, StatusCode::INTERNAL_SERVER_ERROR);
    let body: Value = serde_json::from_slice(&response.body)?;
    assert_eq!(body["code"], "internal_error");
    assert!(Uuid::parse_str(body["correlation_id"].as_str().ok_or("correlation_id")?).is_ok());
    let text = String::from_utf8(response.body.clone())?;
    for value in forbidden {
        assert!(!text.contains(value), "opaque response leaked {value}");
    }
    Ok(())
}

fn record(events: &Arc<Mutex<Vec<&'static str>>>, event: &'static str) {
    if let Ok(mut guard) = events.lock() {
        guard.push(event);
    }
}

fn count_event(events: &Arc<Mutex<Vec<&'static str>>>, expected: &str) -> usize {
    event_snapshot(events)
        .into_iter()
        .filter(|event| *event == expected)
        .count()
}

fn event_snapshot(events: &Arc<Mutex<Vec<&'static str>>>) -> Vec<&'static str> {
    events.lock().map(|guard| guard.clone()).unwrap_or_default()
}
