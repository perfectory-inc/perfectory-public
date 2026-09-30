//! The whole sign-in → relay → sign-out path against a fake issuer and a fake Foundation.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::extract::{Form, Path, State};
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use dawneer_server::app::{router, AppState, CSRF_HEADER, SESSION_COOKIE};
use dawneer_server::config::Config;
use dawneer_server::oidc::{self, pkce_challenge};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tower::ServiceExt;
use url::Url;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const CLIENT_ID: &str = "dawneer-test-client";
const CLIENT_SECRET: &str = "dawneer-test-secret";
const PROJECT: &str = "999990000000000001";
const ITEM: &str = "00000000-0000-5000-8000-000000000001";

/// What the fake issuer expects the verifier to hash to.
#[derive(Default)]
struct Issuer {
    base: Mutex<String>,
    challenge: Mutex<String>,
}

async fn discovery(State(issuer): State<Arc<Issuer>>) -> Json<Value> {
    let base = issuer.base.lock().await.clone();
    Json(json!({
        "issuer": base,
        "authorization_endpoint": format!("{base}/oauth/v2/authorize"),
        "token_endpoint": format!("{base}/oauth/v2/token"),
        "userinfo_endpoint": format!("{base}/oidc/v1/userinfo"),
        "end_session_endpoint": format!("{base}/oidc/v1/end_session"),
    }))
}

async fn token(
    State(issuer): State<Arc<Issuer>>,
    headers: HeaderMap,
    Form(form): Form<std::collections::HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    use base64::Engine as _;
    let expected = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{CLIENT_ID}:{CLIENT_SECRET}"))
    );
    if headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        != Some(expected.as_str())
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid_client"})),
        );
    }
    let verifier = form.get("code_verifier").cloned().unwrap_or_default();
    if form.get("code").map(String::as_str) != Some("code-ok")
        || pkce_challenge(&verifier) != *issuer.challenge.lock().await
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_grant"})),
        );
    }
    (
        StatusCode::OK,
        Json(
            json!({"access_token": "access-1", "refresh_token": "refresh-1", "expires_in": 3600, "id_token": "id-1"}),
        ),
    )
}

async fn userinfo(headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        == Some("Bearer access-1")
    {
        (
            StatusCode::OK,
            Json(
                json!({"sub": "999990000000000042", "name": "Synthetic Steward", "email": "steward@example.test"}),
            ),
        )
    } else {
        (StatusCode::UNAUTHORIZED, Json(json!({})))
    }
}

/// Echoes what reached Foundation.
async fn foundation(method: Method, Path(path): Path<String>, headers: HeaderMap) -> Json<Value> {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned)
    };
    Json(json!({
        "method": method.as_str(),
        "path": path,
        "authorization": header("authorization"),
        "idempotency_key": header("idempotency-key"),
    }))
}

async fn serve(app: Router) -> Result<String, std::io::Error> {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let base = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(base)
}

struct Harness {
    app: Router,
    issuer: Arc<Issuer>,
}

async fn harness() -> Result<Harness, Box<dyn std::error::Error + Send + Sync>> {
    let issuer = Arc::new(Issuer::default());
    let issuer_base = serve(
        Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/oauth/v2/token", post(token))
            .route("/oidc/v1/userinfo", get(userinfo))
            .with_state(issuer.clone()),
    )
    .await?;
    *issuer.base.lock().await = issuer_base.clone();
    let foundation_base =
        serve(Router::new().route("/catalog/v1/{*path}", any(foundation))).await?;
    let web_dir = std::env::temp_dir().join(format!("dawneer-web-{}", std::process::id()));
    std::fs::create_dir_all(&web_dir)?;
    std::fs::write(
        web_dir.join("index.html"),
        "<!doctype html><title>Dawneer</title>",
    )?;
    let config = Config {
        bind: SocketAddr::from(([127, 0, 0, 1], 3120)),
        public_base: Url::parse("http://127.0.0.1:3120")?,
        issuer: Url::parse(&issuer_base)?,
        project_id: PROJECT.to_owned(),
        client_id: CLIENT_ID.to_owned(),
        client_secret: CLIENT_SECRET.to_owned(),
        foundation_base: Url::parse(&foundation_base)?,
        web_dir,
    };
    let http = reqwest::Client::new();
    let discovery = oidc::discover(&http, &config).await?;
    Ok(Harness {
        app: router(Arc::new(AppState::new(config, discovery, http))),
        issuer,
    })
}

async fn body_json(
    response: axum::response::Response,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX).await?,
    )?)
}

/// Signs in and returns the session cookie and CSRF value.
async fn sign_in(
    h: &Harness,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    let login = h
        .app
        .clone()
        .oneshot(Request::get("/auth/login").body(Body::empty())?)
        .await?;
    assert_eq!(login.status(), StatusCode::SEE_OTHER);
    let location = Url::parse(
        login
            .headers()
            .get(header::LOCATION)
            .ok_or("no redirect")?
            .to_str()?,
    )?;
    let query: std::collections::HashMap<_, _> = location.query_pairs().into_owned().collect();
    assert_eq!(
        query.get("code_challenge_method").map(String::as_str),
        Some("S256")
    );
    assert!(
        query
            .get("scope")
            .is_some_and(|s| s.contains(&format!("urn:zitadel:iam:org:project:id:{PROJECT}:aud"))),
        "the project audience scope is what makes Foundation accept the token"
    );
    *h.issuer.challenge.lock().await = query.get("code_challenge").cloned().unwrap_or_default();
    let state = query.get("state").ok_or("no state")?;
    let callback = h
        .app
        .clone()
        .oneshot(
            Request::get(format!("/auth/callback?code=code-ok&state={state}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    let cookie = callback
        .headers()
        .get(header::SET_COOKIE)
        .ok_or("no cookie")?
        .to_str()?
        .to_owned();
    assert!(
        cookie.contains("HttpOnly") && cookie.contains("SameSite=Lax"),
        "{cookie}"
    );
    let pair = cookie.split(';').next().ok_or("empty cookie")?.to_owned();
    let session = h
        .app
        .clone()
        .oneshot(
            Request::get("/api/session")
                .header(header::COOKIE, &pair)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(session.status(), StatusCode::OK);
    let body = body_json(session).await?;
    assert_eq!(body["name"], "Synthetic Steward");
    assert!(
        body.get("access_token").is_none(),
        "the browser never sees a token"
    );
    let csrf = body["csrf"].as_str().ok_or("no csrf")?.to_owned();
    Ok((pair, csrf))
}

#[tokio::test]
async fn nothing_is_open_before_signing_in() -> TestResult {
    let h = harness().await?;
    for uri in ["/api/session", "/api/foundation/lineage-review/items"] {
        let response = h
            .app
            .clone()
            .oneshot(Request::get(uri).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
    }
    Ok(())
}

#[tokio::test]
async fn a_callback_with_an_unknown_state_is_refused() -> TestResult {
    let h = harness().await?;
    let forged = h
        .app
        .clone()
        .oneshot(Request::get("/auth/callback?code=code-ok&state=forged").body(Body::empty())?)
        .await?;
    assert_eq!(forged.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn a_signed_in_steward_reaches_only_the_listed_routes_with_their_token() -> TestResult {
    let h = harness().await?;
    let (cookie, csrf) = sign_in(&h).await?;

    let list = h
        .app
        .clone()
        .oneshot(
            Request::get("/api/foundation/lineage-review/items?limit=5")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(list.status(), StatusCode::OK);
    let echoed = body_json(list).await?;
    assert_eq!(
        echoed["authorization"], "Bearer access-1",
        "the server attaches the session's token"
    );

    let other = h
        .app
        .clone()
        .oneshot(
            // Complexes are listed; their attachments are not.
            Request::get(
                "/api/foundation/complexes/00000000-0000-5000-8000-000000000001/attachments",
            )
            .header(header::COOKIE, &cookie)
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(
        other.status(),
        StatusCode::NOT_FOUND,
        "an unlisted route is not relayed"
    );

    let decide = |csrf: Option<&str>| -> Result<Request<Body>, axum::http::Error> {
        let mut request = Request::post(format!(
            "/api/foundation/lineage-review/items/{ITEM}/decisions"
        ))
        .header(header::COOKIE, &cookie)
        .header(header::CONTENT_TYPE, "application/json")
        .header("idempotency-key", "decide-key-0001");
        if let Some(csrf) = csrf {
            request = request.header(CSRF_HEADER, csrf);
        }
        request.body(Body::from("{}"))
    };
    let forged = h.app.clone().oneshot(decide(None)?).await?;
    assert_eq!(
        forged.status(),
        StatusCode::FORBIDDEN,
        "a write without the CSRF value is refused"
    );
    let wrong = h.app.clone().oneshot(decide(Some("wrong"))?).await?;
    assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
    let decided = h.app.clone().oneshot(decide(Some(&csrf))?).await?;
    assert_eq!(decided.status(), StatusCode::OK);
    let echoed = body_json(decided).await?;
    assert_eq!(
        echoed["idempotency_key"], "decide-key-0001",
        "business headers pass through"
    );
    assert_eq!(echoed["method"], "POST");
    Ok(())
}

#[tokio::test]
async fn signing_out_ends_the_session() -> TestResult {
    let h = harness().await?;
    let (cookie, csrf) = sign_in(&h).await?;
    let out = h
        .app
        .clone()
        .oneshot(
            Request::post("/auth/logout")
                .header(header::COOKIE, &cookie)
                .header(CSRF_HEADER, &csrf)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(out.status(), StatusCode::OK);
    assert!(out
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with(&format!("{SESSION_COOKIE}=;")) && v.contains("Max-Age=0")));
    let end = body_json(out).await?;
    assert!(end["end_session_url"]
        .as_str()
        .is_some_and(|u| u.contains("/oidc/v1/end_session")));
    let after = h
        .app
        .clone()
        .oneshot(
            Request::get("/api/session")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(after.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn every_response_carries_the_security_headers() -> TestResult {
    let h = harness().await?;
    let page = h
        .app
        .clone()
        .oneshot(Request::get("/").body(Body::empty())?)
        .await?;
    let headers = page.headers();
    assert!(headers
        .get(header::CONTENT_SECURITY_POLICY)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("frame-ancestors 'none'")));
    assert_eq!(
        headers
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("nosniff")
    );
    Ok(())
}
