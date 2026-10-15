//! Identity v1 wire contract compatibility tests.

use chrono::Utc;
use identity_contracts::{
    AssignStaffRoleRequest, IdentityEventDeliveryV1, IdentityEventV1, PolicyDecisionRequest,
    PolicyDecisionResponse, PrincipalId, ResourceAction, ServiceCapabilityChangedV1,
    StaffInvitedV1, StaffRoleAssignedV1, StaffRoleResponse, StaffSessionRevokedV1,
    VerifyStaffSessionResponse, ASSIGN_STAFF_ROLE_ROUTE, POLICY_DECISION_ROUTE,
    VERIFY_STAFF_SESSION_ROUTE,
};
use uuid::Uuid;

#[test]
fn policy_decision_contract_round_trips_without_bearer_token(
) -> Result<(), Box<dyn std::error::Error>> {
    let request = PolicyDecisionRequest {
        resource: "foundation.catalog".to_owned(),
        action: "write".to_owned(),
        resource_id: None,
        trace_id: "trace-1".to_owned(),
    };
    let json = serde_json::to_value(&request)?;
    assert!(json.get("bearer_token").is_none());
    assert_eq!(json["resource"], "foundation.catalog");

    let response = PolicyDecisionResponse {
        principal_id: PrincipalId::new(Uuid::nil()),
        decision: ResourceAction::Allow,
        reason_code: "role_grant".to_owned(),
        evaluated_at: Utc::now(),
    };
    assert_eq!(
        serde_json::from_value::<PolicyDecisionResponse>(serde_json::to_value(&response)?)?,
        response
    );
    Ok(())
}

#[test]
fn staff_contracts_publish_v1_routes_without_credentials() -> Result<(), Box<dyn std::error::Error>>
{
    assert_eq!(
        VERIFY_STAFF_SESSION_ROUTE,
        "/identity/v1/staff/sessions/verify"
    );
    assert_eq!(
        ASSIGN_STAFF_ROLE_ROUTE,
        "/identity/v1/staff/{staff_id}/roles"
    );
    assert_eq!(POLICY_DECISION_ROUTE, "/identity/v1/policy/decisions");

    let principal_id = PrincipalId::new(Uuid::nil());
    let session = VerifyStaffSessionResponse {
        principal_id,
        email: "staff@example.test".to_owned(),
        display_name: "Staff".to_owned(),
        roles: vec!["MASTER_ADMIN".to_owned()],
        expires_at: Utc::now(),
    };
    let role_request = AssignStaffRoleRequest {
        role_code: "COMPLEX_EDITOR".to_owned(),
    };
    let role_response = StaffRoleResponse {
        principal_id,
        role_code: role_request.role_code.clone(),
        granted_at: Utc::now(),
        granted_by: principal_id,
    };

    for value in [
        serde_json::to_value(session)?,
        serde_json::to_value(role_request)?,
        serde_json::to_value(role_response)?,
    ] {
        assert!(value.get("bearer_token").is_none());
        assert!(value.get("id_token").is_none());
    }
    Ok(())
}

#[test]
fn identity_events_use_identity_v1_names_and_preserve_payload_fields(
) -> Result<(), Box<dyn std::error::Error>> {
    let principal_id = PrincipalId::new(Uuid::nil());
    let timestamp = Utc::now();
    let events = [
        IdentityEventV1::StaffInvited(StaffInvitedV1 {
            schema_version: 1,
            staff_id: principal_id,
            email: "staff@example.test".to_owned(),
            invited_at: timestamp,
            invited_by: principal_id,
        }),
        IdentityEventV1::StaffRoleAssigned(StaffRoleAssignedV1 {
            schema_version: 1,
            staff_id: principal_id,
            role_code: "MASTER_ADMIN".to_owned(),
            assigned_at: timestamp,
            assigned_by: principal_id,
        }),
        IdentityEventV1::StaffSessionRevoked(StaffSessionRevokedV1 {
            schema_version: 1,
            staff_id: principal_id,
            jti: "jti-1".to_owned(),
            revoked_at: timestamp,
            reason: "logout".to_owned(),
        }),
        IdentityEventV1::ServiceCapabilityGranted(capability_change(principal_id, timestamp)),
        IdentityEventV1::ServiceCapabilityRevoked(capability_change(principal_id, timestamp)),
    ];

    let event_types = events
        .into_iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|event| event["type"].as_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>();

    assert_eq!(
        event_types,
        Some(vec![
            "identity.staff.invited.v1".to_owned(),
            "identity.staff.role_assigned.v1".to_owned(),
            "identity.staff.session_revoked.v1".to_owned(),
            "identity.service_principal.capability_granted.v1".to_owned(),
            "identity.service_principal.capability_revoked.v1".to_owned(),
        ])
    );
    Ok(())
}

fn capability_change(
    service_principal_id: PrincipalId,
    changed_at: chrono::DateTime<Utc>,
) -> ServiceCapabilityChangedV1 {
    ServiceCapabilityChangedV1 {
        schema_version: 1,
        service_principal_id,
        capability: "foundation.catalog:read".to_owned(),
        changed_at,
        policy_sha256: "0".repeat(64),
    }
}

#[test]
fn identity_event_delivery_v1_has_exact_wire_shape() -> Result<(), Box<dyn std::error::Error>> {
    let event_id = Uuid::from_u128(1);
    let staff_id = PrincipalId::new(Uuid::from_u128(2));
    let occurred_at = "2026-07-12T10:00:00Z".parse()?;
    let delivery = IdentityEventDeliveryV1 {
        event_id,
        event_type: "identity.staff.session_revoked.v1".to_owned(),
        occurred_at,
        payload: IdentityEventV1::StaffSessionRevoked(StaffSessionRevokedV1 {
            schema_version: 1,
            staff_id,
            jti: "test-jti".to_owned(),
            revoked_at: occurred_at,
            reason: "logout".to_owned(),
        }),
    };

    assert_eq!(
        serde_json::to_value(delivery)?,
        serde_json::json!({
            "event_id": event_id,
            "event_type": "identity.staff.session_revoked.v1",
            "occurred_at": "2026-07-12T10:00:00Z",
            "payload": {
                "type": "identity.staff.session_revoked.v1",
                "schema_version": 1,
                "staff_id": Uuid::from_u128(2),
                "jti": "test-jti",
                "revoked_at": "2026-07-12T10:00:00Z",
                "reason": "logout"
            }
        })
    );
    Ok(())
}
