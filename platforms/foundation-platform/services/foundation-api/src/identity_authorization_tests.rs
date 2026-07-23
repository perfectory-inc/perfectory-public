use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::json;
use tokio::sync::Notify;
use tower::ServiceExt;
use uuid::Uuid;
use wiremock::matchers::{body_json, header as header_matcher, method, path};
use wiremock::{Mock, MockServer, Request as WiremockRequest, Respond, ResponseTemplate};

use crate::identity_authorization::{
    AuthorizedPrincipal, HttpIdentityAuthorization, IdentityAuthorization,
    IdentityAuthorizationError, RequiredPrincipalKind,
};
use crate::identity_http_client::HttpIdentityClient;
use crate::identity_token_verifier::IdentityTokenVerifier;
use crate::routes::{router, router_with_traffic};
use crate::state::AppState;
use crate::traffic::TrafficConfig;

const TEST_KID: &str = "foundation-rsa-test-key";
const ROTATED_TEST_KID: &str = "foundation-rsa-rotated-key";
const TEST_AUDIENCE: &str = "foundation-api";
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorizationCall {
    required_principal_kind: RequiredPrincipalKind,
    resource: String,
    action: String,
    resource_id: Option<String>,
    trace_id: String,
}

struct RecordingAuthorization {
    calls: Arc<Mutex<Vec<AuthorizationCall>>>,
    error: IdentityAuthorizationError,
}

struct BlockingAuthorization {
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl IdentityAuthorization for RecordingAuthorization {
    async fn authorize(
        &self,
        _bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        self.calls
            .lock()
            .map_err(|_| IdentityAuthorizationError::Unavailable)?
            .push(AuthorizationCall {
                required_principal_kind,
                resource: resource.to_owned(),
                action: action.to_owned(),
                resource_id: resource_id.map(str::to_owned),
                trace_id: trace_id.to_owned(),
            });
        Err(self.error)
    }
}

#[async_trait]
impl IdentityAuthorization for BlockingAuthorization {
    async fn authorize(
        &self,
        _bearer: &str,
        _required_principal_kind: RequiredPrincipalKind,
        _resource: &str,
        _action: &str,
        _resource_id: Option<&str>,
        _trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        Err(IdentityAuthorizationError::Unavailable)
    }
}

#[derive(Clone)]
struct ServiceRouteCase {
    name: &'static str,
    method: Method,
    uri: &'static str,
    resource: &'static str,
    action: &'static str,
    resource_id: Option<&'static str>,
}

#[tokio::test]
async fn service_route_matrix_derives_policy_and_fails_closed() -> Result<(), Box<dyn Error>> {
    for case in service_route_cases() {
        for (error, expected_status) in [
            (IdentityAuthorizationError::Forbidden, StatusCode::FORBIDDEN),
            (
                IdentityAuthorizationError::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
                Arc::new(RecordingAuthorization {
                    calls: calls.clone(),
                    error,
                }),
            )?);
            let trace_id = format!("trace-{}-{}", case.name, expected_status.as_u16());

            let response = router(state)
                .oneshot(service_route_request(
                    &case,
                    Some("signed-service-token"),
                    &trace_id,
                )?)
                .await?;

            assert_eq!(response.status(), expected_status, "route: {}", case.name);
            assert_eq!(
                *calls.lock().map_err(|_| "authorization calls poisoned")?,
                vec![AuthorizationCall {
                    required_principal_kind: RequiredPrincipalKind::Service,
                    resource: case.resource.to_owned(),
                    action: case.action.to_owned(),
                    resource_id: case.resource_id.map(str::to_owned),
                    trace_id,
                }],
                "route: {}",
                case.name
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn service_route_matrix_rejects_missing_bearer_without_policy_call(
) -> Result<(), Box<dyn Error>> {
    for case in service_route_cases() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            Arc::new(RecordingAuthorization {
                calls: calls.clone(),
                error: IdentityAuthorizationError::Forbidden,
            }),
        )?);

        let response = router(state)
            .oneshot(service_route_request(&case, None, "trace-missing")?)
            .await?;

        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "route: {}",
            case.name
        );
        assert!(
            calls
                .lock()
                .map_err(|_| "authorization calls poisoned")?
                .is_empty(),
            "route: {}",
            case.name
        );
    }
    Ok(())
}

#[tokio::test]
async fn protected_get_head_requests_cannot_bypass_authorization() -> Result<(), Box<dyn Error>> {
    for case in service_route_cases()
        .into_iter()
        .filter(|case| case.method == Method::GET)
    {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            Arc::new(RecordingAuthorization {
                calls: calls.clone(),
                error: IdentityAuthorizationError::Forbidden,
            }),
        )?);
        let head_case = ServiceRouteCase {
            method: Method::HEAD,
            ..case.clone()
        };

        let response = router(state)
            .oneshot(service_route_request(&head_case, None, "trace-head")?)
            .await?;

        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "route: {}",
            case.name
        );
        assert!(calls
            .lock()
            .map_err(|_| "authorization calls poisoned")?
            .is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn staff_path_routes_forward_exact_resource_id() -> Result<(), Box<dyn Error>> {
    let cases = [
        (
            Method::PATCH,
            "/catalog/v1/complexes/018f7c6a-0000-7000-8000-000000000011",
            "018f7c6a-0000-7000-8000-000000000011",
        ),
        (
            Method::PATCH,
            "/catalog/v1/parcels/018f7c6a-0000-7000-8000-000000000012/kind",
            "018f7c6a-0000-7000-8000-000000000012",
        ),
        (
            Method::POST,
            "/catalog/v1/normalization/proposals/018f7c6a-0000-7000-8000-000000000013/approve",
            "018f7c6a-0000-7000-8000-000000000013",
        ),
        (
            Method::POST,
            "/catalog/v1/normalization/applications/018f7c6a-0000-7000-8000-000000000014/rollback",
            "018f7c6a-0000-7000-8000-000000000014",
        ),
    ];

    for (method, uri, resource_id) in cases {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
            Arc::new(RecordingAuthorization {
                calls: calls.clone(),
                error: IdentityAuthorizationError::Forbidden,
            }),
        )?);
        let response = router(state)
            .oneshot(protected_request(
                method,
                uri,
                "staff-token",
                "trace-staff-id",
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::FORBIDDEN, "route: {uri}");
        assert_eq!(
            *calls.lock().map_err(|_| "authorization calls poisoned")?,
            vec![AuthorizationCall {
                required_principal_kind: RequiredPrincipalKind::Staff,
                resource: "foundation.catalog".to_owned(),
                action: "write".to_owned(),
                resource_id: Some(resource_id.to_owned()),
                trace_id: "trace-staff-id".to_owned(),
            }]
        );
    }
    Ok(())
}

#[tokio::test]
async fn public_health_route_bypasses_identity_policy() -> Result<(), Box<dyn Error>> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        Arc::new(RecordingAuthorization {
            calls: calls.clone(),
            error: IdentityAuthorizationError::Forbidden,
        }),
    )?);

    let response = router(state)
        .oneshot(Request::builder().uri("/healthz").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(calls
        .lock()
        .map_err(|_| "authorization calls poisoned")?
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn forged_service_token_is_unauthorized_before_identity_policy_call(
) -> Result<(), Box<dyn Error>> {
    let zitadel = MockServer::start().await;
    mount_zitadel_metadata(&zitadel).await;
    let identity = MockServer::start().await;
    let verifier = IdentityTokenVerifier::new(
        zitadel.uri(),
        TEST_AUDIENCE,
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let client = HttpIdentityClient::new(
        identity.uri(),
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        Arc::new(HttpIdentityAuthorization::new(
            verifier,
            client,
            Duration::from_millis(100),
        )),
    )?);
    let token = corrupt_signature(&signed_token(&zitadel.uri(), "service")?);
    let normalization = service_route_cases()
        .into_iter()
        .find(|case| case.name == "normalization_proposal")
        .ok_or("normalization route missing")?;

    let response = router(state)
        .oneshot(service_route_request(
            &normalization,
            Some(&token),
            "trace-forged-route",
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(identity
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn principal_kind_mismatch_is_forbidden_before_identity_policy_call(
) -> Result<(), Box<dyn Error>> {
    let zitadel = MockServer::start().await;
    mount_zitadel_metadata(&zitadel).await;
    let identity = MockServer::start().await;
    let verifier = IdentityTokenVerifier::new(
        zitadel.uri(),
        TEST_AUDIENCE,
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let client = HttpIdentityClient::new(
        identity.uri(),
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        Arc::new(HttpIdentityAuthorization::new(
            verifier,
            client,
            Duration::from_secs(1),
        )),
    )?);
    let token = signed_token(&zitadel.uri(), "staff")?;
    let route = service_route_cases()[0].clone();

    let response = router(state)
        .oneshot(service_route_request(
            &route,
            Some(&token),
            "trace-kind-mismatch",
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(identity
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn identity_policy_timeout_returns_service_unavailable_through_router(
) -> Result<(), Box<dyn Error>> {
    let zitadel = MockServer::start().await;
    mount_zitadel_metadata(&zitadel).await;
    let identity = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/identity/v1/policy/decisions"))
        .and(body_json(json!({
            "resource": "foundation.normalization",
            "action": "propose",
            "resource_id": null,
            "trace_id": "trace-policy-timeout"
        })))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)))
        .expect(2)
        .mount(&identity)
        .await;
    let verifier = IdentityTokenVerifier::new(
        zitadel.uri(),
        TEST_AUDIENCE,
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let client = HttpIdentityClient::new(
        identity.uri(),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )?;
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        Arc::new(HttpIdentityAuthorization::new(
            verifier,
            client,
            Duration::from_secs(1),
        )),
    )?);
    let token = signed_token(&zitadel.uri(), "service")?;
    let normalization = service_route_cases()
        .into_iter()
        .find(|case| case.name == "normalization_proposal")
        .ok_or("normalization route missing")?;

    let response = router(state)
        .oneshot(service_route_request(
            &normalization,
            Some(&token),
            "trace-policy-timeout",
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

#[tokio::test]
async fn catalog_write_derives_identity_policy_and_maps_deny_to_forbidden(
) -> Result<(), Box<dyn Error>> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let authorization = Arc::new(RecordingAuthorization {
        calls: calls.clone(),
        error: IdentityAuthorizationError::Forbidden,
    });
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        authorization,
    )?);
    let response = router(state)
        .oneshot(catalog_write_request("staff-token", "trace-route-deny")?)
        .await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        *calls.lock().map_err(|_| "authorization calls poisoned")?,
        vec![AuthorizationCall {
            required_principal_kind: RequiredPrincipalKind::Staff,
            resource: "foundation.catalog".to_owned(),
            action: "write".to_owned(),
            resource_id: None,
            trace_id: "trace-route-deny".to_owned(),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn identity_infrastructure_failure_never_allows_catalog_write() -> Result<(), Box<dyn Error>>
{
    let authorization = Arc::new(RecordingAuthorization {
        calls: Arc::new(Mutex::new(Vec::new())),
        error: IdentityAuthorizationError::Unavailable,
    });
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        authorization,
    )?);
    let response = router(state)
        .oneshot(catalog_write_request("staff-token", "trace-route-timeout")?)
        .await?;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

#[tokio::test]
async fn identity_dependency_timeout_returns_503_before_outer_timeout() -> Result<(), Box<dyn Error>>
{
    let zitadel = MockServer::start().await;
    mount_zitadel_metadata(&zitadel).await;
    let identity = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/identity/v1/policy/decisions"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
        .mount(&identity)
        .await;
    let verifier = IdentityTokenVerifier::new(
        zitadel.uri(),
        TEST_AUDIENCE,
        Duration::from_millis(50),
        Duration::from_millis(100),
    )?;
    let client = HttpIdentityClient::new(
        identity.uri(),
        Duration::from_millis(50),
        Duration::from_millis(200),
    )?;
    let authorization = HttpIdentityAuthorization::new(verifier, client, Duration::from_millis(40));
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        Arc::new(authorization),
    )?);
    let traffic = TrafficConfig {
        request_timeout_ms: 80,
        ..TrafficConfig::default()
    };
    let token = signed_token(&zitadel.uri(), "staff")?;
    let started = tokio::time::Instant::now();
    let response = router_with_traffic(state, traffic)
        .oneshot(catalog_write_request(&token, "trace-route-timeout")?)
        .await?;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(started.elapsed() < Duration::from_millis(80));
    Ok(())
}

#[tokio::test]
async fn concurrency_budget_includes_identity_authorization() -> Result<(), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let authorization = Arc::new(BlockingAuthorization {
        calls: calls.clone(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        authorization,
    )?);
    let traffic = TrafficConfig {
        request_timeout_ms: 500,
        max_concurrency: 1,
        ..TrafficConfig::default()
    };
    let app = router_with_traffic(state, traffic);
    let first_route = service_route_cases()[0].clone();
    let first = tokio::spawn(app.clone().oneshot(service_route_request(
        &first_route,
        Some("service-token"),
        "trace-concurrency-1",
    )?));
    tokio::time::timeout(Duration::from_millis(100), entered.notified()).await?;

    let second = tokio::time::timeout(
        Duration::from_millis(100),
        app.oneshot(service_route_request(
            &first_route,
            Some("service-token"),
            "trace-concurrency-2",
        )?),
    )
    .await??;

    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();
    assert_eq!(first.await??.status(), StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

#[test]
fn identity_endpoints_require_https_except_explicit_loopback() {
    let timeout = Duration::from_millis(100);

    assert!(HttpIdentityClient::new("http://identity.example.com", timeout, timeout).is_err());
    assert!(IdentityTokenVerifier::new(
        "http://issuer.example.com",
        TEST_AUDIENCE,
        timeout,
        timeout,
    )
    .is_err());
    assert!(HttpIdentityClient::new("http://localhost:18080", timeout, timeout).is_ok());
    assert!(
        IdentityTokenVerifier::new("http://127.0.0.1:18081", TEST_AUDIENCE, timeout, timeout,)
            .is_ok()
    );
}

#[tokio::test]
async fn discovery_rejects_cross_origin_jwks_uri_before_fetch() -> Result<(), Box<dyn Error>> {
    let issuer = MockServer::start().await;
    let foreign_jwks = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jwks_uri": format!("{}/keys", foreign_jwks.uri())
        })))
        .mount(&issuer)
        .await;
    let verifier = IdentityTokenVerifier::new(
        issuer.uri(),
        TEST_AUDIENCE,
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let token = signed_token(&issuer.uri(), "service")?;

    let result = verifier.verify(&token).await;

    assert_eq!(
        result,
        Err(crate::identity_token_verifier::IdentityTokenVerificationError::Infrastructure)
    );
    assert!(foreign_jwks
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn unknown_kid_refresh_is_single_flight_and_rate_limited() -> Result<(), Box<dyn Error>> {
    let zitadel = MockServer::start().await;
    let jwks_fetches = Arc::new(AtomicUsize::new(0));
    mount_rotating_zitadel_metadata(&zitadel, jwks_fetches.clone()).await;
    let cooldown = Duration::from_millis(60);
    let verifier = IdentityTokenVerifier::new_with_unknown_kid_cooldown(
        zitadel.uri(),
        TEST_AUDIENCE,
        Duration::from_millis(100),
        Duration::from_millis(200),
        cooldown,
    )?;
    let cached_token = signed_token(&zitadel.uri(), "service")?;
    assert!(verifier.verify(&cached_token).await.is_ok());
    assert_eq!(jwks_fetches.load(Ordering::SeqCst), 1);

    let random_tokens = (0..16)
        .map(|index| signed_token_with_kid(&zitadel.uri(), "service", &format!("random-{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    let mut attempts = Vec::new();
    for token in random_tokens {
        let verifier = verifier.clone();
        attempts.push(tokio::spawn(async move { verifier.verify(&token).await }));
    }
    for attempt in attempts {
        assert_eq!(
            attempt.await?,
            Err(crate::identity_token_verifier::IdentityTokenVerificationError::Unauthorized)
        );
    }

    assert_eq!(jwks_fetches.load(Ordering::SeqCst), 2);
    assert!(verifier.verify(&cached_token).await.is_ok());
    let another_unknown = signed_token_with_kid(&zitadel.uri(), "service", "another-random")?;
    assert_eq!(
        verifier.verify(&another_unknown).await,
        Err(crate::identity_token_verifier::IdentityTokenVerificationError::Unauthorized)
    );
    assert_eq!(jwks_fetches.load(Ordering::SeqCst), 2);

    tokio::time::sleep(cooldown + Duration::from_millis(10)).await;
    let rotated_token = signed_token_with_kid(&zitadel.uri(), "service", ROTATED_TEST_KID)?;
    assert!(verifier.verify(&rotated_token).await.is_ok());
    assert_eq!(jwks_fetches.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn http_identity_client_forwards_only_bearer_and_published_decision_body(
) -> Result<(), Box<dyn Error>> {
    let identity = MockServer::start().await;
    let principal_id = Uuid::now_v7();
    Mock::given(method("POST"))
        .and(path("/identity/v1/policy/decisions"))
        .and(header_matcher("authorization", "Bearer signed-token"))
        .and(body_json(json!({
            "resource": "foundation.catalog",
            "action": "read",
            "resource_id": "parcel-1",
            "trace_id": "trace-client-1"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "principal_id": principal_id,
            "decision": "deny",
            "reason_code": "missing_service_capability",
            "evaluated_at": Utc::now(),
        })))
        .expect(1)
        .mount(&identity)
        .await;
    let client = HttpIdentityClient::new(
        identity.uri(),
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;

    let result = client
        .authorize(
            "signed-token",
            "foundation.catalog",
            "read",
            Some("parcel-1"),
            "trace-client-1",
        )
        .await;

    assert_eq!(result, Err(IdentityAuthorizationError::Forbidden));
    Ok(())
}

#[tokio::test]
async fn invalid_signature_is_unauthorized_before_identity_policy_call(
) -> Result<(), Box<dyn Error>> {
    let zitadel = MockServer::start().await;
    mount_zitadel_metadata(&zitadel).await;
    let identity = MockServer::start().await;
    let verifier = IdentityTokenVerifier::new(
        zitadel.uri(),
        TEST_AUDIENCE,
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let client = HttpIdentityClient::new(
        identity.uri(),
        Duration::from_millis(100),
        Duration::from_millis(200),
    )?;
    let authorization =
        HttpIdentityAuthorization::new(verifier, client, Duration::from_millis(100));
    let token = corrupt_signature(&signed_token(&zitadel.uri(), "service")?);

    let result = authorization
        .authorize(
            &token,
            RequiredPrincipalKind::Service,
            "foundation.catalog",
            "read",
            None,
            "trace-forged",
        )
        .await;

    assert_eq!(result, Err(IdentityAuthorizationError::Unauthorized));
    assert!(identity
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn identity_request_timeout_fails_closed() -> Result<(), Box<dyn Error>> {
    let identity = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/identity/v1/policy/decisions"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
        .expect(2)
        .mount(&identity)
        .await;
    let client = HttpIdentityClient::new(
        identity.uri(),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )?;

    let result = client
        .authorize(
            "signed-token",
            "foundation.catalog",
            "write",
            None,
            "trace-timeout",
        )
        .await;

    assert_eq!(result, Err(IdentityAuthorizationError::Unavailable));
    Ok(())
}

fn catalog_write_request(token: &str, trace_id: &str) -> Result<Request<Body>, axum::http::Error> {
    let body = json!({
        "official_complex_code": "1234567",
        "name": "test complex",
        "kind": "national",
        "primary_bjdong_code": "9999900101",
        "area_m2": 1000
    });
    Request::builder()
        .method(Method::POST)
        .uri("/catalog/v1/complexes")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header("x-request-id", trace_id)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
}

fn protected_request(
    method: Method,
    uri: &str,
    token: &str,
    trace_id: &str,
) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header("x-request-id", trace_id)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
}

fn service_route_cases() -> [ServiceRouteCase; 6] {
    [
        ServiceRouteCase {
            name: "parcel_by_pnu",
            method: Method::GET,
            uri: "/catalog/v1/parcels/by-pnu/9999900101100090000",
            resource: "foundation.catalog",
            action: "read",
            resource_id: Some("9999900101100090000"),
        },
        ServiceRouteCase {
            name: "parcel_buildings",
            method: Method::GET,
            uri: "/catalog/v1/parcels/by-pnu/9999900101100090000/buildings",
            resource: "foundation.catalog",
            action: "read",
            resource_id: Some("9999900101100090000"),
        },
        ServiceRouteCase {
            name: "complex_parcels",
            method: Method::GET,
            uri: "/catalog/v1/complexes/018f7c6a-0000-7000-8000-000000000001/parcels",
            resource: "foundation.catalog",
            action: "read",
            resource_id: Some("018f7c6a-0000-7000-8000-000000000001"),
        },
        ServiceRouteCase {
            name: "parcel_by_id",
            method: Method::GET,
            uri: "/catalog/v1/parcels/018f7c6a-0000-7000-8000-000000000002",
            resource: "foundation.catalog",
            action: "read",
            resource_id: Some("018f7c6a-0000-7000-8000-000000000002"),
        },
        ServiceRouteCase {
            name: "lakehouse_artifact",
            method: Method::POST,
            uri: "/internal/lakehouse/artifacts",
            resource: "foundation.lakehouse",
            action: "write",
            resource_id: None,
        },
        ServiceRouteCase {
            name: "normalization_proposal",
            method: Method::POST,
            uri: "/internal/normalization/proposals",
            resource: "foundation.normalization",
            action: "propose",
            resource_id: None,
        },
    ]
}

fn service_route_request(
    case: &ServiceRouteCase,
    token: Option<&str>,
    trace_id: &str,
) -> Result<Request<Body>, axum::http::Error> {
    let mut request = Request::builder()
        .method(case.method.clone())
        .uri(case.uri)
        .header("x-request-id", trace_id)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    request.body(Body::from("{}"))
}

async fn mount_zitadel_metadata(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jwks_uri": format!("{}/keys", server.uri())
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/keys"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "kid": TEST_KID,
                "alg": "RS256",
                "n": TEST_RSA_MODULUS,
                "e": "AQAB"
            }]
        })))
        .mount(server)
        .await;
}

#[derive(Clone)]
struct RotatingJwksResponse {
    fetches: Arc<AtomicUsize>,
}

impl Respond for RotatingJwksResponse {
    fn respond(&self, _request: &WiremockRequest) -> ResponseTemplate {
        let fetch = self.fetches.fetch_add(1, Ordering::SeqCst);
        let mut keys = vec![json!({
            "kty": "RSA",
            "use": "sig",
            "kid": TEST_KID,
            "alg": "RS256",
            "n": TEST_RSA_MODULUS,
            "e": "AQAB"
        })];
        if fetch >= 2 {
            keys.push(json!({
                "kty": "RSA",
                "use": "sig",
                "kid": ROTATED_TEST_KID,
                "alg": "RS256",
                "n": TEST_RSA_MODULUS,
                "e": "AQAB"
            }));
        }
        ResponseTemplate::new(200).set_body_json(json!({ "keys": keys }))
    }
}

async fn mount_rotating_zitadel_metadata(server: &MockServer, fetches: Arc<AtomicUsize>) {
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jwks_uri": format!("{}/keys", server.uri())
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/keys"))
        .respond_with(RotatingJwksResponse { fetches })
        .mount(server)
        .await;
}

#[derive(Serialize)]
struct SignedClaims<'a> {
    sub: &'a str,
    iat: i64,
    exp: i64,
    iss: &'a str,
    aud: &'a str,
    principal_kind: &'a str,
}

fn signed_token(issuer: &str, principal_kind: &str) -> Result<String, Box<dyn Error>> {
    signed_token_with_kid(issuer, principal_kind, TEST_KID)
}

fn signed_token_with_kid(
    issuer: &str,
    principal_kind: &str,
    kid: &str,
) -> Result<String, Box<dyn Error>> {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_owned());
    let private_key = format!(
        "-----BEGIN {label}-----\n{TEST_RSA_PRIVATE_KEY_BODY}\n-----END {label}-----",
        label = "PRIVATE KEY"
    );
    let now = Utc::now().timestamp();
    Ok(encode(
        &header,
        &SignedClaims {
            sub: "service-subject",
            iat: now,
            exp: now + 300,
            iss: issuer,
            aud: TEST_AUDIENCE,
            principal_kind,
        },
        &EncodingKey::from_rsa_pem(private_key.as_bytes())?,
    )?)
}

fn corrupt_signature(token: &str) -> String {
    let Some((signing_input, signature)) = token.rsplit_once('.') else {
        return token.to_owned();
    };
    format!("{signing_input}.{}", "A".repeat(signature.len()))
}
