//! 도메인 mutation 메서드 검증 — `verify_*`, `revoke_*`, `add_role`, `remove_role`,
//! `record_*`, `soft_delete`, `is_*` 등.

use chrono::Utc;

use super::super::UserRole;
use super::fixtures::{sample_broker_license, sample_user};

// ── Domain mutation methods (T9) ──────────────────────────────────────────

#[test]
fn verify_broker_sets_license_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let lic = sample_broker_license();
    let later = now + chrono::Duration::seconds(60);

    user.verify_broker(lic.clone(), later);

    assert_eq!(user.broker_license_number, Some(lic));
    assert_eq!(user.broker_verified_at, Some(later));
    assert_eq!(user.version, 2);
    assert_eq!(user.updated_at, later);
}

#[test]
fn revoke_broker_verification_clears_timestamp_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    user.verify_broker(sample_broker_license(), now + chrono::Duration::seconds(30));

    let revoke_at = now + chrono::Duration::seconds(60);
    user.revoke_broker_verification(revoke_at);

    assert_eq!(user.broker_verified_at, None);
    assert!(user.broker_license_number.is_some());
    assert_eq!(user.version, 3);
}

#[test]
fn record_nice_verification_sets_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let later = now + chrono::Duration::seconds(60);

    user.record_nice_verification(later);

    assert_eq!(user.nice_verified_at, Some(later));
    assert_eq!(user.version, 2);
    assert_eq!(user.updated_at, later);
}

#[test]
fn record_marketing_consent_sets_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let later = now + chrono::Duration::seconds(60);

    user.record_marketing_consent(later);

    assert_eq!(user.marketing_consent_at, Some(later));
    assert_eq!(user.version, 2);
}

#[test]
fn revoke_marketing_consent_clears_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    user.record_marketing_consent(now + chrono::Duration::seconds(30));

    let revoke_at = now + chrono::Duration::seconds(60);
    user.revoke_marketing_consent(revoke_at);

    assert_eq!(user.marketing_consent_at, None);
    assert_eq!(user.version, 3);
}

#[test]
fn record_login_does_not_bump_version() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let initial_version = user.version;
    let later = now + chrono::Duration::seconds(60);

    user.record_login(later);

    assert_eq!(user.last_login_at, Some(later));
    assert_eq!(user.updated_at, later);
    // version은 의도적으로 bump 안 함 (로그인은 동시성 충돌 검사 대상 아님).
    assert_eq!(user.version, initial_version);
}

#[test]
fn add_role_appends_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let later = now + chrono::Duration::seconds(60);

    user.add_role(UserRole::Buyer, later);

    assert_eq!(user.roles, vec![UserRole::Buyer]);
    assert_eq!(user.version, 2);
}

#[test]
fn add_role_duplicate_is_noop() {
    let now = Utc::now();
    let mut user = sample_user(now);
    user.add_role(UserRole::Buyer, now + chrono::Duration::seconds(30));
    let version_after_first = user.version;
    let updated_after_first = user.updated_at;

    user.add_role(UserRole::Buyer, now + chrono::Duration::seconds(60));

    assert_eq!(user.roles, vec![UserRole::Buyer]);
    assert_eq!(user.version, version_after_first);
    assert_eq!(user.updated_at, updated_after_first);
}

#[test]
fn remove_role_removes_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    user.add_role(UserRole::Buyer, now + chrono::Duration::seconds(30));
    user.add_role(UserRole::Seller, now + chrono::Duration::seconds(45));

    let later = now + chrono::Duration::seconds(60);
    user.remove_role(UserRole::Buyer, later);

    assert_eq!(user.roles, vec![UserRole::Seller]);
    assert_eq!(user.version, 4);
    assert_eq!(user.updated_at, later);
}

#[test]
fn remove_role_missing_is_noop() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let initial_version = user.version;
    let initial_updated = user.updated_at;

    user.remove_role(UserRole::Admin, now + chrono::Duration::seconds(60));

    assert!(user.roles.is_empty());
    assert_eq!(user.version, initial_version);
    assert_eq!(user.updated_at, initial_updated);
}

#[test]
fn has_role_reports_membership() {
    let now = Utc::now();
    let mut user = sample_user(now);
    assert!(!user.has_role(UserRole::Buyer));

    user.add_role(UserRole::Buyer, now + chrono::Duration::seconds(30));

    assert!(user.has_role(UserRole::Buyer));
    assert!(!user.has_role(UserRole::Admin));
}

#[test]
fn soft_delete_sets_deleted_at_and_bumps() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let later = now + chrono::Duration::seconds(60);

    user.soft_delete(later);

    assert_eq!(user.deleted_at, Some(later));
    assert_eq!(user.version, 2);
    assert_eq!(user.updated_at, later);
    assert!(!user.is_active());
}

#[test]
fn soft_delete_idempotent() {
    let now = Utc::now();
    let mut user = sample_user(now);
    let t1 = now + chrono::Duration::seconds(60);
    let t2 = now + chrono::Duration::seconds(120);

    user.soft_delete(t1);
    let version_after_first = user.version;
    let deleted_at_after_first = user.deleted_at;

    user.soft_delete(t2);

    assert_eq!(user.deleted_at, deleted_at_after_first);
    assert_eq!(user.version, version_after_first);
}

#[test]
fn is_active_reflects_deleted_at() {
    let now = Utc::now();
    let mut user = sample_user(now);
    assert!(user.is_active());

    user.soft_delete(now + chrono::Duration::seconds(60));

    assert!(!user.is_active());
}

#[test]
fn is_broker_reflects_timestamp() {
    let now = Utc::now();
    let mut user = sample_user(now);
    assert!(!user.is_broker());

    user.verify_broker(sample_broker_license(), now + chrono::Duration::seconds(60));

    assert!(user.is_broker());
}
