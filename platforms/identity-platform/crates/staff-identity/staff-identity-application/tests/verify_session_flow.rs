//! Staff session verification owns the complete bearer-to-context flow.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use authorization_domain::RoleCode;
use chrono::{Duration, Utc};
use identity_contracts::VerifyStaffSessionResponse;
use identity_shared_kernel::StaffId;
use staff_identity_application::ports::{
    EffectiveRoleReader, OidcVerifier, StaffRepository, StaffSessionUnitOfWork, VerifiedOidcClaims,
};
use staff_identity_application::{RevokeStaffSession, VerifyStaffSession, VerifyStaffSessionInput};
use staff_identity_domain::{Staff, StaffIdentityError, StaffSession};
use uuid::Uuid;

#[tokio::test]
async fn verify_session_returns_trusted_context_and_persists_session(
) -> Result<(), Box<dyn std::error::Error>> {
    let staff = sample_staff();
    let repository = Arc::new(FakeStaffRepository::new(staff.clone(), HashSet::new()));
    let sessions = Arc::new(RecordingSessionUnitOfWork::default());
    let roles = Arc::new(FakeEffectiveRoleReader {
        roles: vec![
            RoleCode::parse("MASTER_ADMIN")?,
            RoleCode::parse("CATALOG_ADMIN")?,
        ],
        calls: AtomicUsize::new(0),
    });
    let verifier = Arc::new(FakeOidcVerifier::valid_for(&staff));
    let expected_expires_at = verifier.claims.expires_at;
    let use_case = VerifyStaffSession::new(
        repository.clone(),
        sessions.clone(),
        roles.clone(),
        verifier.clone(),
    );

    let output = use_case
        .execute(VerifyStaffSessionInput {
            bearer_token: "verified-token".to_owned(),
        })
        .await?;

    assert_eq!(output.context.staff_id, staff.id);
    assert_eq!(output.context.principal_id.as_uuid(), staff.id.as_uuid());
    assert_eq!(output.context.roles.len(), 2);
    let response = VerifyStaffSessionResponse {
        principal_id: output.context.principal_id,
        email: output.email.clone(),
        display_name: output.display_name.clone(),
        roles: output
            .context
            .roles
            .iter()
            .map(|role| role.as_str().to_owned())
            .collect(),
        expires_at: output.expires_at,
    };
    assert_eq!(response.principal_id.as_uuid(), staff.id.as_uuid());
    assert_eq!(response.email, staff.email);
    assert_eq!(response.display_name, staff.display_name);
    assert_eq!(response.roles, vec!["CATALOG_ADMIN", "MASTER_ADMIN"]);
    assert_eq!(response.expires_at, expected_expires_at);
    assert_eq!(repository.subject_reads.load(Ordering::SeqCst), 1);
    assert_eq!(repository.revoke_reads.load(Ordering::SeqCst), 1);
    assert_eq!(roles.calls.load(Ordering::SeqCst), 1);
    assert_eq!(verifier.calls.load(Ordering::SeqCst), 1);

    let (persisted_staff_id, persisted_jti) = {
        let persisted_guard = sessions
            .session
            .lock()
            .map_err(|_| "session recording mutex poisoned")?;
        let Some(persisted) = persisted_guard.as_ref() else {
            return Err("verified session was not persisted".into());
        };
        let snapshot = (persisted.staff_id, persisted.jti.clone());
        drop(persisted_guard);
        snapshot
    };
    assert_eq!(persisted_staff_id, staff.id);
    assert_eq!(persisted_jti, "jti-1");
    Ok(())
}

#[tokio::test]
async fn verify_session_rejects_revoked_jti_before_staff_or_role_reads(
) -> Result<(), Box<dyn std::error::Error>> {
    let staff = sample_staff();
    let repository = Arc::new(FakeStaffRepository::new(
        staff.clone(),
        HashSet::from(["jti-1".to_owned()]),
    ));
    let sessions = Arc::new(RecordingSessionUnitOfWork::default());
    let roles = Arc::new(FakeEffectiveRoleReader {
        roles: Vec::new(),
        calls: AtomicUsize::new(0),
    });
    let use_case = VerifyStaffSession::new(
        repository.clone(),
        sessions.clone(),
        roles.clone(),
        Arc::new(FakeOidcVerifier::valid_for(&staff)),
    );

    let result = use_case
        .execute(VerifyStaffSessionInput {
            bearer_token: "revoked-token".to_owned(),
        })
        .await;

    assert!(matches!(result, Err(StaffIdentityError::JtiRevoked(_))));
    assert_eq!(repository.subject_reads.load(Ordering::SeqCst), 0);
    assert_eq!(roles.calls.load(Ordering::SeqCst), 0);
    assert!(sessions
        .session
        .lock()
        .map_err(|_| "session recording mutex poisoned")?
        .is_none());
    Ok(())
}

#[tokio::test]
async fn verify_session_rejects_expired_verified_claims_before_persistence(
) -> Result<(), Box<dyn std::error::Error>> {
    let staff = sample_staff();
    let repository = Arc::new(FakeStaffRepository::new(staff.clone(), HashSet::new()));
    let sessions = Arc::new(RecordingSessionUnitOfWork::default());
    let roles = Arc::new(FakeEffectiveRoleReader {
        roles: Vec::new(),
        calls: AtomicUsize::new(0),
    });
    let mut claims = FakeOidcVerifier::valid_for(&staff).claims;
    claims.expires_at = Utc::now() - Duration::seconds(1);
    let use_case = VerifyStaffSession::new(
        repository.clone(),
        sessions,
        roles,
        Arc::new(FakeOidcVerifier {
            claims,
            calls: Arc::new(AtomicUsize::new(0)),
        }),
    );

    let result = use_case
        .execute(VerifyStaffSessionInput {
            bearer_token: "expired-token".to_owned(),
        })
        .await;

    assert!(matches!(result, Err(StaffIdentityError::SessionExpired)));
    assert_eq!(repository.revoke_reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn revoke_session_delegates_verified_jti_and_reason_to_session_store(
) -> Result<(), Box<dyn std::error::Error>> {
    let sessions = Arc::new(RecordingSessionUnitOfWork::default());
    let use_case = RevokeStaffSession::new(sessions.clone());

    use_case.execute("jti-1", "logout").await?;

    let revoked = sessions
        .revoked
        .lock()
        .map_err(|_| "revocation recording mutex poisoned")?
        .clone();
    assert_eq!(revoked, Some(("jti-1".to_owned(), "logout".to_owned())));
    Ok(())
}

fn sample_staff() -> Staff {
    let now = Utc::now();
    Staff {
        id: StaffId::new(Uuid::from_u128(42)),
        zitadel_subject: "zitadel-subject".to_owned(),
        email: "staff@example.test".to_owned(),
        display_name: "Staff".to_owned(),
        primary_role_code: "MASTER_ADMIN".to_owned(),
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

struct FakeStaffRepository {
    staff: Staff,
    revoked_jtis: HashSet<String>,
    subject_reads: AtomicUsize,
    revoke_reads: AtomicUsize,
}

impl FakeStaffRepository {
    const fn new(staff: Staff, revoked_jtis: HashSet<String>) -> Self {
        Self {
            staff,
            revoked_jtis,
            subject_reads: AtomicUsize::new(0),
            revoke_reads: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StaffRepository for FakeStaffRepository {
    async fn find_by_zitadel_subject(
        &self,
        subject: &str,
    ) -> Result<Option<Staff>, StaffIdentityError> {
        self.subject_reads.fetch_add(1, Ordering::SeqCst);
        Ok((subject == self.staff.zitadel_subject).then(|| self.staff.clone()))
    }

    async fn is_jti_revoked(&self, jti: &str) -> Result<bool, StaffIdentityError> {
        self.revoke_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.revoked_jtis.contains(jti))
    }
}

#[derive(Default)]
struct RecordingSessionUnitOfWork {
    session: Mutex<Option<StaffSession>>,
    revoked: Mutex<Option<(String, String)>>,
}

#[async_trait]
impl StaffSessionUnitOfWork for RecordingSessionUnitOfWork {
    async fn persist_verified_session(
        &self,
        session: &StaffSession,
    ) -> Result<(), StaffIdentityError> {
        let mut recorded = self.session.lock().map_err(|_| {
            StaffIdentityError::Infrastructure("recording mutex poisoned".to_owned())
        })?;
        *recorded = Some(session.clone());
        drop(recorded);
        Ok(())
    }

    async fn revoke_jti(
        &self,
        jti: &str,
        reason: &str,
        _revoked_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StaffIdentityError> {
        let mut revoked = self.revoked.lock().map_err(|_| {
            StaffIdentityError::Infrastructure("revocation recording mutex poisoned".to_owned())
        })?;
        *revoked = Some((jti.to_owned(), reason.to_owned()));
        drop(revoked);
        Ok(())
    }
}

struct FakeEffectiveRoleReader {
    roles: Vec<RoleCode>,
    calls: AtomicUsize,
}

#[async_trait]
impl EffectiveRoleReader for FakeEffectiveRoleReader {
    async fn read_effective_roles(
        &self,
        _staff_id: StaffId,
    ) -> Result<Vec<RoleCode>, StaffIdentityError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.roles.clone())
    }
}

struct FakeOidcVerifier {
    claims: VerifiedOidcClaims,
    calls: Arc<AtomicUsize>,
}

impl FakeOidcVerifier {
    fn valid_for(staff: &Staff) -> Self {
        Self {
            claims: VerifiedOidcClaims {
                subject: staff.zitadel_subject.clone(),
                jti: "jti-1".to_owned(),
                issued_at: Utc::now() - Duration::seconds(1),
                expires_at: Utc::now() + Duration::minutes(5),
            },
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl OidcVerifier for FakeOidcVerifier {
    async fn verify_bearer(
        &self,
        _bearer_token: &str,
    ) -> Result<VerifiedOidcClaims, StaffIdentityError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.claims.clone())
    }
}
