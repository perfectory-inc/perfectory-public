//! Authorization use cases execute policy outside HTTP routes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use authorization_application::ports::{
    IdentityBootstrapUnitOfWork, RoleGrantPersistenceError, RoleGrantUnitOfWork,
};
use authorization_application::{
    AssignStaffRole, AssignStaffRoleError, AssignStaffRoleInput, BootstrapMasterAdmin,
    BootstrapMasterAdminInput, BootstrapMasterAdminOutcome, EvaluateAccess, EvaluateAccessInput,
};
use authorization_domain::{RoleCode, RoleGrant};
use identity_contracts::PrincipalId;
use identity_shared_kernel::StaffId;
use staff_identity_domain::{Staff, StaffIdentityError};
use uuid::Uuid;

#[tokio::test]
async fn evaluate_access_returns_principal_and_allows_master_admin_unknown_action(
) -> Result<(), Box<dyn std::error::Error>> {
    let principal_id = PrincipalId::new(Uuid::nil());
    let use_case = EvaluateAccess::new();
    let output = use_case
        .execute(EvaluateAccessInput {
            principal_id,
            roles: vec![RoleCode::parse("MASTER_ADMIN")?],
            resource: "foundation.catalog".to_owned(),
            action: "delete_everything".to_owned(),
            resource_id: None,
            trace_id: "trace-1".to_owned(),
        })
        .await?;

    assert!(output.decision.is_allowed());
    assert_eq!(output.principal_id, principal_id);
    Ok(())
}

#[tokio::test]
async fn assign_role_uses_verified_actor_staff_id_as_grantor(
) -> Result<(), Box<dyn std::error::Error>> {
    let uow = Arc::new(RecordingRoleGrantUnitOfWork::default());
    let use_case = AssignStaffRole::new(uow.clone());
    let actor_staff_id = StaffId::new(Uuid::from_u128(1));
    let target_staff_id = StaffId::new(Uuid::from_u128(2));

    let output = use_case
        .execute(AssignStaffRoleInput {
            actor_principal_id: PrincipalId::new(Uuid::from_u128(1)),
            actor_staff_id,
            actor_roles: vec![RoleCode::parse("MASTER_ADMIN")?],
            target_staff_id,
            role_code: RoleCode::parse("CATALOG_ADMIN")?,
            trace_id: "trace-2".to_owned(),
        })
        .await?;

    assert_eq!(output.grant.granted_by, actor_staff_id);
    assert_eq!(output.grant.staff_id, target_staff_id);
    assert_eq!(uow.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn assign_role_denies_unprivileged_actor_without_persistence(
) -> Result<(), Box<dyn std::error::Error>> {
    let uow = Arc::new(RecordingRoleGrantUnitOfWork::default());
    let use_case = AssignStaffRole::new(uow.clone());

    let result = use_case
        .execute(AssignStaffRoleInput {
            actor_principal_id: PrincipalId::new(Uuid::from_u128(1)),
            actor_staff_id: StaffId::new(Uuid::from_u128(1)),
            actor_roles: vec![RoleCode::parse("CATALOG_ADMIN")?],
            target_staff_id: StaffId::new(Uuid::from_u128(2)),
            role_code: RoleCode::parse("LAKEHOUSE_ADMIN")?,
            trace_id: "trace-3".to_owned(),
        })
        .await;

    assert!(matches!(
        result,
        Err(AssignStaffRoleError::PermissionDenied(
            "missing_master_admin"
        ))
    ));
    assert_eq!(uow.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn assign_role_exposes_duplicate_role_without_string_parsing(
) -> Result<(), Box<dyn std::error::Error>> {
    let use_case = AssignStaffRole::new(Arc::new(DuplicateRoleGrantUnitOfWork));

    let result = use_case
        .execute(AssignStaffRoleInput {
            actor_principal_id: PrincipalId::new(Uuid::from_u128(1)),
            actor_staff_id: StaffId::new(Uuid::from_u128(1)),
            actor_roles: vec![RoleCode::parse("MASTER_ADMIN")?],
            target_staff_id: StaffId::new(Uuid::from_u128(2)),
            role_code: RoleCode::parse("CATALOG_ADMIN")?,
            trace_id: "trace-duplicate-role".to_owned(),
        })
        .await;

    assert!(matches!(result, Err(AssignStaffRoleError::DuplicateRole)));
    Ok(())
}

#[tokio::test]
async fn bootstrap_master_admin_is_idempotent_when_one_exists(
) -> Result<(), Box<dyn std::error::Error>> {
    let uow = Arc::new(RecordingBootstrapUnitOfWork::new(true));
    let use_case = BootstrapMasterAdmin::new(uow.clone());

    let outcome = use_case.execute(bootstrap_input()).await?;

    assert!(matches!(
        outcome,
        BootstrapMasterAdminOutcome::AlreadyPresent
    ));
    assert_eq!(uow.create_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn bootstrap_master_admin_builds_self_granted_master_admin_transaction(
) -> Result<(), Box<dyn std::error::Error>> {
    let uow = Arc::new(RecordingBootstrapUnitOfWork::new(false));
    let use_case = BootstrapMasterAdmin::new(uow.clone());

    let outcome = use_case.execute(bootstrap_input()).await?;

    let BootstrapMasterAdminOutcome::Created { staff, role_grant } = outcome else {
        return Err("expected created bootstrap outcome".into());
    };
    assert_eq!(staff.id, role_grant.staff_id);
    assert_eq!(staff.id, role_grant.granted_by);
    assert_eq!(role_grant.role_code.as_str(), "MASTER_ADMIN");
    assert_eq!(uow.create_calls.load(Ordering::SeqCst), 1);

    let (recorded_staff_id, recorded_granted_by, recorded_role_code) = {
        let recorded_guard = uow
            .recorded
            .lock()
            .map_err(|_| "bootstrap recording mutex poisoned")?;
        let Some(recorded) = recorded_guard.as_ref() else {
            return Err("bootstrap transaction was not recorded".into());
        };
        let snapshot = (
            recorded.staff_id,
            recorded.granted_by,
            recorded.role_code.clone(),
        );
        drop(recorded_guard);
        snapshot
    };
    assert_eq!(recorded_staff_id, recorded_granted_by);
    assert_eq!(recorded_role_code.as_str(), "MASTER_ADMIN");
    Ok(())
}

fn bootstrap_input() -> BootstrapMasterAdminInput {
    BootstrapMasterAdminInput {
        zitadel_subject: "bootstrap-subject".to_owned(),
        email: "admin@example.test".to_owned(),
        display_name: "Bootstrap Admin".to_owned(),
    }
}

#[derive(Default)]
struct RecordingRoleGrantUnitOfWork {
    calls: AtomicUsize,
}

#[async_trait]
impl RoleGrantUnitOfWork for RecordingRoleGrantUnitOfWork {
    async fn assign_role(
        &self,
        staff_id: StaffId,
        role_code: &RoleCode,
        granted_by: StaffId,
    ) -> Result<RoleGrant, RoleGrantPersistenceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(RoleGrant {
            staff_id,
            role_code: role_code.clone(),
            granted_at: chrono::Utc::now(),
            granted_by,
        })
    }
}

struct DuplicateRoleGrantUnitOfWork;

#[async_trait]
impl RoleGrantUnitOfWork for DuplicateRoleGrantUnitOfWork {
    async fn assign_role(
        &self,
        _staff_id: StaffId,
        _role_code: &RoleCode,
        _granted_by: StaffId,
    ) -> Result<RoleGrant, RoleGrantPersistenceError> {
        Err(RoleGrantPersistenceError::DuplicateRole)
    }
}

struct RecordedBootstrap {
    staff_id: StaffId,
    role_code: RoleCode,
    granted_by: StaffId,
}

struct RecordingBootstrapUnitOfWork {
    master_admin_exists: bool,
    create_calls: AtomicUsize,
    recorded: Mutex<Option<RecordedBootstrap>>,
}

impl RecordingBootstrapUnitOfWork {
    const fn new(master_admin_exists: bool) -> Self {
        Self {
            master_admin_exists,
            create_calls: AtomicUsize::new(0),
            recorded: Mutex::new(None),
        }
    }
}

#[async_trait]
impl IdentityBootstrapUnitOfWork for RecordingBootstrapUnitOfWork {
    async fn master_admin_exists(&self) -> Result<bool, StaffIdentityError> {
        Ok(self.master_admin_exists)
    }

    async fn create_first_master_admin(
        &self,
        staff: &Staff,
        role_grant: &RoleGrant,
    ) -> Result<(), StaffIdentityError> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        let mut recorded = self.recorded.lock().map_err(|_| {
            StaffIdentityError::Infrastructure("recording mutex poisoned".to_owned())
        })?;
        *recorded = Some(RecordedBootstrap {
            staff_id: staff.id,
            role_code: role_grant.role_code.clone(),
            granted_by: role_grant.granted_by,
        });
        drop(recorded);
        Ok(())
    }
}
