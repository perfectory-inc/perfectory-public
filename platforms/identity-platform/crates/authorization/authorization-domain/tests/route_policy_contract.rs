//! Identity grants exactly the policies Foundation's routes ask for (root ADR-0214).
//!
//! Foundation owns the list of policy decisions its routes request
//! (`config/identity-route-policies.contract.json`, checked there against the routes). Identity owns
//! who may get them: the staff role matrix and the workload principal policy. A route no role can
//! reach, or a grant for a route that no longer exists, fails here.

use authorization_domain::{evaluate_policy, PolicyInput, RoleCode, STAFF_ROLE_GRANTS};
use std::collections::BTreeSet;
use std::error::Error;
use std::path::Path;

const FOUNDATION_ROUTE_POLICIES: &str =
    "../../../../foundation-platform/config/identity-route-policies.contract.json";
const WORKLOAD_POLICY: &str = "../../../config/workload-principal-policy.v1.json";

fn read_json(relative: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    let raw = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(relative))?;
    Ok(serde_json::from_str(&raw)?)
}

fn route_policies(kind: &str) -> Result<BTreeSet<(String, String)>, Box<dyn Error>> {
    let document = read_json(FOUNDATION_ROUTE_POLICIES)?;
    assert_eq!(
        document["schema_version"],
        "foundation.identity-route-policies.v1"
    );
    let mut set = BTreeSet::new();
    for policy in document["policies"].as_array().ok_or("no policies")? {
        if policy["principal_kind"] == kind {
            set.insert((
                policy["resource"].as_str().ok_or("resource")?.to_owned(),
                policy["action"].as_str().ok_or("action")?.to_owned(),
            ));
        }
    }
    assert!(
        !set.is_empty(),
        "no {kind} policies in the Foundation contract"
    );
    Ok(set)
}

#[test]
fn the_staff_role_matrix_grants_exactly_the_foundation_staff_routes() -> Result<(), Box<dyn Error>>
{
    let routes = route_policies("staff")?;
    let granted: BTreeSet<(String, String)> = STAFF_ROLE_GRANTS
        .iter()
        .filter(|grant| grant.resource.starts_with("foundation."))
        .map(|grant| (grant.resource.to_owned(), grant.action.to_owned()))
        .collect();
    assert_eq!(
        granted, routes,
        "a Foundation staff route no role but MASTER_ADMIN can reach, or a grant for no route"
    );

    // The matrix is what evaluation reads: each grant allows its role and only its role.
    for grant in STAFF_ROLE_GRANTS {
        let input = PolicyInput::resource_action(
            vec![RoleCode::parse(grant.role)?],
            grant.resource,
            grant.action,
        );
        assert!(evaluate_policy(&input).is_allowed(), "{grant:?}");
    }
    let nobody = PolicyInput::resource_action(
        vec![RoleCode::parse("DATA_CATALOG_READER")?],
        "foundation.catalog",
        "write",
    );
    assert!(!evaluate_policy(&nobody).is_allowed());
    Ok(())
}

#[test]
fn the_workload_policy_grants_exactly_the_foundation_service_routes() -> Result<(), Box<dyn Error>>
{
    let routes = route_policies("service")?;
    let policy = read_json(WORKLOAD_POLICY)?;
    let mut granted = BTreeSet::new();
    for principal in policy["principals"].as_array().ok_or("no principals")? {
        for capability in principal["capabilities"]
            .as_array()
            .ok_or("no capabilities")?
        {
            let capability = capability.as_str().ok_or("capability")?;
            let (resource, action) = capability.split_once(':').ok_or("capability form")?;
            if resource.starts_with("foundation.") {
                granted.insert((resource.to_owned(), action.to_owned()));
            }
        }
    }
    assert_eq!(
        granted, routes,
        "a Foundation service route no workload may call, or a capability for no route"
    );
    Ok(())
}
