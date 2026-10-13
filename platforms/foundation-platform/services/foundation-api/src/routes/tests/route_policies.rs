//! The route policy contract is exactly the policies the routes ask Identity for (root ADR-0214).

use super::super::{IdentityRoutePolicy, ROUTE_POLICIES};
use crate::identity_authorization::RequiredPrincipalKind;
use std::collections::BTreeSet;
use std::error::Error;
use std::path::Path;

const CONTRACT: &str = "../../config/identity-route-policies.contract.json";
const SCHEMA_VERSION: &str = "foundation.identity-route-policies.v1";
const ROUTES_SOURCE: &str = "src/routes/mod.rs";

type Policy = (String, String, String);

fn kind(policy: &IdentityRoutePolicy) -> &'static str {
    match policy.required_principal_kind {
        RequiredPrincipalKind::Staff => "staff",
        RequiredPrincipalKind::Service => "service",
    }
}

fn from_routes() -> BTreeSet<Policy> {
    ROUTE_POLICIES
        .iter()
        .map(|policy| {
            (
                kind(policy).to_owned(),
                policy.resource.to_owned(),
                policy.action.to_owned(),
            )
        })
        .collect()
}

fn read_crate_file(relative: &str) -> Result<String, std::io::Error> {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(relative))
}

fn from_contract() -> Result<BTreeSet<Policy>, Box<dyn Error>> {
    let raw = read_crate_file(CONTRACT)?;
    let document: serde_json::Value = serde_json::from_str(&raw)?;
    assert_eq!(document["schema_version"], SCHEMA_VERSION);
    let policies = document["policies"]
        .as_array()
        .ok_or("contract has no policies array")?;
    let mut set = BTreeSet::new();
    for policy in policies {
        let field = |name: &str| -> Result<String, Box<dyn Error>> {
            Ok(policy[name]
                .as_str()
                .ok_or_else(|| format!("policy without {name}"))?
                .to_owned())
        };
        assert!(
            set.insert((
                field("principal_kind")?,
                field("resource")?,
                field("action")?
            )),
            "contract lists a policy twice"
        );
    }
    Ok(set)
}

#[test]
fn the_contract_lists_exactly_the_policies_the_routes_ask_for() -> Result<(), Box<dyn Error>> {
    let routes = from_routes();
    let contract = from_contract()?;
    assert_eq!(
        routes.len(),
        ROUTE_POLICIES.len(),
        "two route policies are the same"
    );
    assert_eq!(
        contract, routes,
        "{CONTRACT} must list exactly ROUTE_POLICIES; routes ask for {routes:#?}"
    );
    Ok(())
}

#[test]
fn every_route_policy_constant_is_in_the_list() -> Result<(), Box<dyn Error>> {
    let source = read_crate_file(ROUTES_SOURCE)?;
    let declared = source
        .matches(": IdentityRoutePolicy = IdentityRoutePolicy {")
        .count();
    assert_eq!(
        declared,
        ROUTE_POLICIES.len(),
        "a route policy constant is missing from ROUTE_POLICIES"
    );
    Ok(())
}
