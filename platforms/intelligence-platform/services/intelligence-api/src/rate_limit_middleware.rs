use axum::{
    body::Body,
    extract::{MatchedPath, State},
    http::{
        header::{HeaderValue, RETRY_AFTER},
        Request, StatusCode,
    },
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use intelligence_normalization_application::{
    RateLimitDecision, RateLimitRequest, RateLimitRouteClass, RateLimitSubject,
};
use serde::Serialize;

use crate::auth::AuthenticatedRequestPrincipal;
use crate::state::AppState;

#[derive(Debug, Serialize)]
struct RateLimitErrorBody {
    code: &'static str,
    message: &'static str,
}

pub async fn rate_limit_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(route_class) = route_class_for_request(&request) else {
        return next.run(request).await;
    };

    let Some(limiter) = state.rate_limiter.as_ref().cloned() else {
        return next.run(request).await;
    };

    let Some(principal) = request
        .extensions()
        .get::<AuthenticatedRequestPrincipal>()
        .map(|principal| principal.0.clone())
    else {
        return retry_after_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "rate_limiter_identity_missing",
            "rate limiter identity is missing",
            1,
        );
    };

    let Some(scope) = principal.scopes.first() else {
        return retry_after_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "rate_limiter_identity_missing",
            "rate limiter identity is missing",
            1,
        );
    };

    let route_policy = state.rate_limit_policy.for_route(route_class);
    let decision = limiter
        .check(RateLimitRequest {
            subject: RateLimitSubject {
                tenant_id: scope.tenant_id.clone(),
                subject_id: principal.subject_id,
            },
            route_class,
            quota: route_policy.quota,
            cost: route_policy.cost,
        })
        .await;

    match decision {
        Ok(RateLimitDecision::Allowed { .. }) => next.run(request).await,
        Ok(RateLimitDecision::Denied {
            retry_after_seconds,
        }) => {
            let labels = [("route_class", route_class.as_key_segment().to_string())];
            metrics::counter!("http_rate_limited_total", &labels).increment(1);
            retry_after_response(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "rate limit exceeded",
                retry_after_seconds,
            )
        }
        Err(error) => {
            tracing::warn!(%error, "rate limiter unavailable; failing closed");
            retry_after_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "rate_limiter_unavailable",
                error.safe_message(),
                1,
            )
        }
    }
}

fn route_class_for_request(request: &Request<Body>) -> Option<RateLimitRouteClass> {
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str())?;

    // Both route namespaces per root ADR-0001 §6: OpenAI-compatible chat stays
    // at `/v1/...` (recorded exception); platform-native normalization routes
    // live under `/intelligence/v1/...`.
    match path {
        "/v1/chat/completions" => Some(RateLimitRouteClass::Chat),
        "/intelligence/v1/normalization/validate-proposal"
        | "/intelligence/v1/normalization/generate-and-validate"
        | "/intelligence/v1/normalization/generate-validate-submit"
        | "/intelligence/v1/normalization/submit-proposal" => {
            Some(RateLimitRouteClass::NormalizationSubmit)
        }
        _ => None,
    }
}

fn retry_after_response(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    retry_after_seconds: u64,
) -> Response {
    let mut response = (status, Json(RateLimitErrorBody { code, message })).into_response();
    let retry_after = HeaderValue::from_str(&retry_after_seconds.max(1).to_string())
        .unwrap_or_else(|_| HeaderValue::from_static("1"));
    response.headers_mut().insert(RETRY_AFTER, retry_after);
    response
}
