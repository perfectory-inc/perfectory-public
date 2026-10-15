// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Intelligence reads Identity's published files, never a copy (root ADR-0216): the wire shape
//! of the policy decision it calls, and the capabilities Identity's workload policy grants.

use std::collections::BTreeSet;
use std::path::PathBuf;

use intelligence_normalization_application::{PermissionAction, METRICS_IDENTITY_CAPABILITY};
use intelligence_normalization_infrastructure::identity_policy::{
    PolicyDecisionRequest, DECISION_VALUES, POLICY_DECISION_PATH, READ_RESPONSE_FIELDS,
};
use serde_json::Value;

fn repo_file(relative: &str) -> Value {
    // Runtime, not `env!`: cargo sets it for test runs too, and no compile-time read is added.
    // The crate sits five levels below the repository root; quoted paths stay full repo paths
    // so the change-scope check sees exactly which files are read (root ADR-0202 §6).
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let path = manifest.ancestors().nth(5).unwrap().join(relative);
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn identity_openapi() -> Value {
    repo_file("platforms/identity-platform/docs/openapi/identity.v1.json")
}

fn names(value: &Value) -> BTreeSet<String> {
    match value {
        Value::Object(map) => map.keys().cloned().collect(),
        Value::Array(items) => items
            .iter()
            .map(|item| item.as_str().unwrap().to_owned())
            .collect(),
        _ => panic!("expected an object or array"),
    }
}

#[test]
fn the_request_sent_is_identitys_published_request() {
    let openapi = identity_openapi();
    assert!(
        openapi["paths"][POLICY_DECISION_PATH]["post"].is_object(),
        "Identity no longer publishes POST {POLICY_DECISION_PATH}"
    );
    let schema = &openapi["components"]["schemas"]["PolicyDecisionRequest"];
    let sent = serde_json::to_value(PolicyDecisionRequest {
        resource: "r",
        action: "a",
        resource_id: None,
        trace_id: "t",
    })
    .unwrap();
    assert_eq!(names(&sent), names(&schema["properties"]));
    assert!(names(&schema["required"]).is_subset(&names(&sent)));
}

#[test]
fn the_response_fields_read_are_ones_identity_always_sends() {
    let openapi = identity_openapi();
    let schemas = &openapi["components"]["schemas"];
    let required = names(&schemas["PolicyDecisionResponse"]["required"]);
    for field in READ_RESPONSE_FIELDS {
        assert!(
            required.contains(field),
            "{field} is not a required response field"
        );
    }
    assert_eq!(
        names(&schemas["ResourceAction"]["enum"]),
        DECISION_VALUES
            .iter()
            .map(|value| (*value).to_owned())
            .collect()
    );
}

#[test]
fn every_intelligence_capability_identity_grants_names_a_real_action() {
    let known: BTreeSet<String> = PermissionAction::ALL
        .iter()
        .map(PermissionAction::identity_capability)
        .chain([METRICS_IDENTITY_CAPABILITY])
        .map(|(resource, action)| format!("{resource}:{action}"))
        .collect();
    assert_eq!(
        known.len(),
        PermissionAction::ALL.len() + 1,
        "two actions share one Identity capability"
    );

    let policy = repo_file("platforms/identity-platform/config/workload-principal-policy.v1.json");
    for principal in policy["principals"].as_array().unwrap() {
        for capability in principal["capabilities"].as_array().unwrap() {
            let capability = capability.as_str().unwrap();
            if capability.starts_with("intelligence.") {
                assert!(
                    known.contains(capability),
                    "Identity grants {capability}, which no intelligence route asks for"
                );
            }
        }
    }
}
