#![allow(clippy::disallowed_types, clippy::expect_used)]

use super::*;

#[test]
fn foundation_endpoint_accepts_https_and_explicit_loopback_http() {
    for (input, expected) in [
        (
            " https://foundation.example/api ",
            "https://foundation.example/api/",
        ),
        ("http://localhost:8080", "http://localhost:8080/"),
        ("http://127.0.0.1:8080", "http://127.0.0.1:8080/"),
        ("http://[::1]:8080", "http://[::1]:8080/"),
    ] {
        let parsed = parse_foundation_endpoint_url(input).expect("valid Foundation endpoint");
        assert_eq!(parsed.as_str(), expected);
    }
}

#[test]
fn foundation_endpoint_rejects_insecure_or_ambiguous_urls() {
    for input in [
        "http://foundation.example",
        "https://user:password@foundation.example",
        "https://foundation.example?tenant=other",
        "https://foundation.example#fragment",
    ] {
        assert!(
            parse_foundation_endpoint_url(input).is_err(),
            "endpoint should be rejected: {input}"
        );
    }
}

#[test]
fn applies_only_zitadel_workload_bearer() {
    let auth = FoundationServiceAuth::from_bearer_token("zitadel-workload-token-32-valid")
        .expect("foundation auth");
    let request = auth
        .apply(reqwest::Client::new().get("http://127.0.0.1/catalog/v1/parcels/by-pnu/1"))
        .expect("apply auth")
        .build()
        .expect("request");

    assert_eq!(
        request.headers().get(reqwest::header::AUTHORIZATION),
        Some(
            &"Bearer zitadel-workload-token-32-valid"
                .parse()
                .expect("authorization header")
        )
    );
    assert!(request.headers().keys().all(|name| !name
        .as_str()
        .starts_with("x-gongzzang-service-auth-")
        && !name.as_str().starts_with("x-foundation-platform-")));
}

#[test]
fn workload_identity_token_file_is_read_before_each_request() {
    let token_file = std::env::temp_dir().join(format!(
        "gongzzang-foundation-token-{}-{}.txt",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::write(&token_file, "zitadel-workload-token-32-first").expect("write token file");
    let auth = FoundationServiceAuth::from_workload_identity_token_file(&token_file)
        .expect("workload identity auth");

    let first = auth
        .apply(reqwest::Client::new().get("http://127.0.0.1/health"))
        .expect("apply first token")
        .build()
        .expect("first request");
    assert_eq!(
        first.headers().get(reqwest::header::AUTHORIZATION),
        Some(
            &"Bearer zitadel-workload-token-32-first"
                .parse()
                .expect("header")
        )
    );

    std::fs::write(&token_file, "zitadel-workload-token-32-second").expect("rotate token file");
    let second = auth
        .apply(reqwest::Client::new().get("http://127.0.0.1/health"))
        .expect("apply rotated token")
        .build()
        .expect("second request");
    assert_eq!(
        second.headers().get(reqwest::header::AUTHORIZATION),
        Some(
            &"Bearer zitadel-workload-token-32-second"
                .parse()
                .expect("header")
        )
    );

    let _ = std::fs::remove_file(token_file);
}

#[test]
fn rejects_blank_or_short_bearers() {
    assert!(matches!(
        FoundationServiceAuth::from_bearer_token("   "),
        Err(FoundationServiceAuthError::EmptyToken)
    ));
    assert!(matches!(
        FoundationServiceAuth::from_bearer_token("short"),
        Err(FoundationServiceAuthError::TokenTooShort)
    ));
}

#[test]
fn debug_output_never_contains_bearer() {
    let auth = FoundationServiceAuth::from_bearer_token("zitadel-workload-token-32-secret")
        .expect("foundation auth");

    let debug = format!("{auth:?}");

    assert!(!debug.contains("zitadel-workload-token-32-secret"));
    assert!(debug.contains("<redacted>"));
}
