//! Routes of the Dawneer server (root ADR-0116).
//!
//! `/auth/*` signs a staff member in and out, `/api/session` tells the screens who is signed in,
//! `/api/foundation/*` relays the allow-listed platform routes with the session's token, and
//! everything else is the built screens.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::config::Config;
use crate::oidc::{self, Discovery};
use crate::relay;
use crate::session::{Session, SessionStore};

/// The session cookie. Its value is a random id; the tokens stay in [`SessionStore`].
pub const SESSION_COOKIE: &str = "dawneer_session";
/// The header every state-changing request echoes the session's CSRF value in.
pub const CSRF_HEADER: &str = "x-dawneer-csrf";
/// Refresh the access token when it has less than this left.
const REFRESH_MARGIN: Duration = Duration::from_mins(1);
/// Largest request body relayed (a decision is a few hundred bytes).
const BODY_LIMIT: usize = 64 * 1024;

/// Everything a request handler needs.
pub struct AppState {
    config: Config,
    discovery: Discovery,
    http: reqwest::Client,
    sessions: SessionStore,
}

impl AppState {
    /// Builds the state from settings and the issuer's discovery document.
    #[must_use]
    pub fn new(config: Config, discovery: Discovery, http: reqwest::Client) -> Self {
        Self {
            config,
            discovery,
            http,
            sessions: SessionStore::default(),
        }
    }
}

/// The server's router.
pub fn router(state: Arc<AppState>) -> Router {
    let web = ServeDir::new(&state.config.web_dir)
        .fallback(ServeFile::new(state.config.web_dir.join("index.html")));
    Router::new()
        .route("/auth/login", get(login))
        .route("/auth/callback", get(callback))
        .route("/auth/logout", post(logout))
        .route("/api/session", get(session))
        .route("/api/foundation/{*path}", any(relay_route))
        .fallback_service(web)
        .layer(SetResponseHeaderLayer::overriding(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            ),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        ))
        .with_state(state)
}

fn plain(status: StatusCode, message: &'static str) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], message).into_response()
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|pair| {
            let (key, value) = pair.trim().split_once('=')?;
            (key == name).then(|| value.to_owned())
        })
}

fn session_cookie(state: &AppState, value: &str, max_age: u64) -> String {
    let secure = if state.config.secure_cookies() {
        "; Secure"
    } else {
        ""
    };
    format!("{SESSION_COOKIE}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{secure}")
}

async fn login(State(state): State<Arc<AppState>>) -> Response {
    let (Ok(login_state), Ok(verifier)) = (oidc::random_token(32), oidc::random_token(48)) else {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "no randomness available");
    };
    let Ok(url) = oidc::authorize_url(&state.discovery, &state.config, &login_state, &verifier)
    else {
        return plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            "redirect URI is misconfigured",
        );
    };
    state.sessions.begin(login_state, verifier).await;
    Redirect::to(url.as_str()).into_response()
}

#[derive(Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn callback(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CallbackQuery>,
) -> Response {
    if let Some(error) = query.error {
        tracing::warn!(%error, "issuer returned an error to the callback");
        return plain(StatusCode::BAD_REQUEST, "sign-in was not completed");
    }
    let (Some(code), Some(login_state)) = (query.code, query.state) else {
        return plain(StatusCode::BAD_REQUEST, "callback is missing code or state");
    };
    let Some(pending) = state.sessions.finish(&login_state).await else {
        return plain(
            StatusCode::BAD_REQUEST,
            "unknown or expired sign-in; start again",
        );
    };
    let tokens = match oidc::exchange_code(
        &state.http,
        &state.discovery,
        &state.config,
        &code,
        &pending.verifier,
    )
    .await
    {
        Ok(tokens) => tokens,
        Err(error) => {
            tracing::warn!(%error, "code exchange failed");
            return plain(
                StatusCode::BAD_GATEWAY,
                "the sign-in could not be completed",
            );
        }
    };
    let identity = match oidc::userinfo(&state.http, &state.discovery, &tokens.access_token).await {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(%error, "userinfo failed");
            return plain(
                StatusCode::BAD_GATEWAY,
                "the sign-in could not be completed",
            );
        }
    };
    let (Ok(id), Ok(csrf)) = (oidc::random_token(32), oidc::random_token(32)) else {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "no randomness available");
    };
    tracing::info!(sub = %identity.sub, "staff signed in");
    let session = Session::new(
        identity,
        tokens.access_token,
        tokens.refresh_token,
        Duration::from_secs(tokens.expires_in.unwrap_or(300)),
        tokens.id_token,
        csrf,
    );
    state.sessions.insert(id.clone(), session).await;
    let mut response = Redirect::to("/").into_response();
    if let Ok(value) = HeaderValue::from_str(&session_cookie(&state, &id, 12 * 3600)) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

/// A state-changing request must come from this origin and echo the session's CSRF value.
fn csrf_ok(state: &AppState, headers: &HeaderMap, session: &Session) -> bool {
    let origin_ok = headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .is_none_or(|origin| {
            origin.trim_end_matches('/') == state.config.public_base.as_str().trim_end_matches('/')
        });
    let echoed = headers.get(CSRF_HEADER).and_then(|v| v.to_str().ok());
    origin_ok && echoed == Some(session.csrf.as_str())
}

async fn current(state: &AppState, headers: &HeaderMap) -> Option<(String, Session)> {
    let id = cookie_value(headers, SESSION_COOKIE)?;
    let session = state.sessions.get(&id).await?;
    Some((id, session))
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some((id, session)) = current(&state, &headers).await else {
        return plain(StatusCode::UNAUTHORIZED, "not signed in");
    };
    if !csrf_ok(&state, &headers, &session) {
        return plain(StatusCode::FORBIDDEN, "missing or wrong CSRF token");
    }
    state.sessions.remove(&id).await;
    let end_session = state.discovery.end_session_endpoint.clone().map(|mut url| {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("client_id", &state.config.client_id);
        pairs.append_pair(
            "post_logout_redirect_uri",
            state.config.public_base.as_str(),
        );
        if let Some(hint) = &session.id_token {
            pairs.append_pair("id_token_hint", hint);
        }
        drop(pairs);
        url.to_string()
    });
    let mut response = (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "end_session_url": end_session })),
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&session_cookie(&state, "", 0)) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

async fn session(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some((_, session)) = current(&state, &headers).await else {
        return plain(StatusCode::UNAUTHORIZED, "not signed in");
    };
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "sub": session.identity.sub,
            "name": session.identity.name,
            "email": session.identity.email,
            "csrf": session.csrf,
        })),
    )
        .into_response()
}

/// The session's access token, refreshed first if it is about to expire.
async fn access_token(state: &AppState, id: &str, session: &Session) -> Option<String> {
    if session
        .access_expires
        .saturating_duration_since(std::time::Instant::now())
        > REFRESH_MARGIN
    {
        return Some(session.access_token.clone());
    }
    let refresh_token = session.refresh_token.as_deref()?;
    match oidc::refresh(&state.http, &state.discovery, &state.config, refresh_token).await {
        Ok(tokens) => {
            let token = tokens.access_token.clone();
            state
                .sessions
                .update_tokens(
                    id,
                    tokens.access_token,
                    tokens.refresh_token,
                    Duration::from_secs(tokens.expires_in.unwrap_or(300)),
                )
                .await;
            Some(token)
        }
        Err(error) => {
            tracing::warn!(%error, "refresh failed; the session ends");
            state.sessions.remove(id).await;
            None
        }
    }
}

async fn relay_route(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some((id, session)) = current(&state, &headers).await else {
        return plain(StatusCode::UNAUTHORIZED, "not signed in");
    };
    if method != Method::GET && !csrf_ok(&state, &headers, &session) {
        return plain(StatusCode::FORBIDDEN, "missing or wrong CSRF token");
    }
    if !relay::allowed(&method, &path) {
        return plain(StatusCode::NOT_FOUND, "not a console route");
    }
    if body.len() > BODY_LIMIT {
        return plain(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
    }
    let Some(token) = access_token(&state, &id, &session).await else {
        return plain(StatusCode::UNAUTHORIZED, "session expired; sign in again");
    };
    let Ok(mut target) = state
        .config
        .foundation_base
        .join(&format!("catalog/v1/{path}"))
    else {
        return plain(StatusCode::BAD_REQUEST, "malformed path");
    };
    target.set_query(uri.query());
    let mut request = state
        .http
        .request(method, target)
        .bearer_auth(token)
        .body(body.to_vec());
    for name in [header::CONTENT_TYPE.as_str(), "idempotency-key"] {
        if let Some(value) = headers.get(name) {
            request = request.header(name, value.clone());
        }
    }
    let upstream = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "foundation relay failed");
            return plain(StatusCode::BAD_GATEWAY, "Foundation is unavailable");
        }
    };
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .cloned();
    let bytes = upstream.bytes().await.unwrap_or_default();
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(value) = content_type.and_then(|v| HeaderValue::from_bytes(v.as_bytes()).ok()) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}
