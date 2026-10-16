//! Ignored `PostgreSQL` contract for transactional, exact service-principal provisioning.

use identity_service_provisioner::{
    parse_bindings, parse_policy, provision, resolve_manifest, ManifestError, ProvisionError,
    ValidatedManifest, BINDINGS_SCHEMA_VERSION, POLICY_SCHEMA_VERSION,
};
use sqlx::postgres::PgPoolOptions;
use std::env;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a migrated disposable PostgreSQL database in IDENTITY_PROVISIONER_TEST_DATABASE_URL"]
async fn provisioning_is_idempotent_exact_and_transactional(
) -> Result<(), Box<dyn std::error::Error>> {
    // Fail loud, like `role_grant_postgres` beside it. This used to return
    // `Ok(())` when the URL was absent, so the identity Postgres lane could run
    // it against nothing and report a pass — the "resource absent = verified"
    // confusion ADR-0010 exists to remove, surviving inside a lane target.
    let database_url = env::var("IDENTITY_PROVISIONER_TEST_DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    let principal_id = Uuid::new_v4();
    let subject = format!("contract-{principal_id}");

    let initial = principal_manifest(
        principal_id,
        &subject,
        &["foundation.catalog:read", "foundation.lakehouse:write"],
    )?;
    provision(&pool, &initial).await?;
    let first_state = grant_state(&pool, principal_id).await?;
    assert_eq!(ledger(&pool, principal_id).await?, (2, 0, 2, 0));
    provision(&pool, &initial).await?;
    assert_eq!(grant_state(&pool, principal_id).await?, first_state);
    // An unchanged policy appends nothing: no record, no event.
    assert_eq!(ledger(&pool, principal_id).await?, (2, 0, 2, 0));

    let reduced = principal_manifest(principal_id, &subject, &["foundation.catalog:read"])?;
    provision(&pool, &reduced).await?;
    let reduced_state = grant_state(&pool, principal_id).await?;
    assert_eq!(reduced_state.len(), 1);
    assert_eq!(reduced_state[0].0, "foundation.catalog:read");
    // The dropped capability is a revoke record and a revoke event; nothing was deleted.
    assert_eq!(ledger(&pool, principal_id).await?, (2, 1, 2, 1));

    let revoked = principal_manifest(principal_id, &subject, &[])?;
    provision(&pool, &revoked).await?;
    assert!(grant_state(&pool, principal_id).await?.is_empty());
    provision(&pool, &revoked).await?;
    assert!(grant_state(&pool, principal_id).await?.is_empty());
    assert_eq!(ledger(&pool, principal_id).await?, (2, 2, 2, 2));

    assert_ledger_refuses_rewrites(&pool, principal_id).await;

    assert_rebinding_is_recorded(&pool, principal_id, &subject).await?;

    let occupied_id = Uuid::new_v4();
    let occupied_subject = format!("occupied-{occupied_id}");
    sqlx::query(
        "INSERT INTO identity.service_principal (id, zitadel_subject, display_name) \
         VALUES ($1, $2, 'Occupied Contract Principal')",
    )
    .bind(occupied_id)
    .bind(&occupied_subject)
    .execute(&pool)
    .await?;
    let rolled_back_id = Uuid::new_v4();
    let collision_id = Uuid::new_v4();
    let rollback_manifest = resolve_manifest(
        parse_policy(
            &serde_json::json!({
                "schema_version": POLICY_SCHEMA_VERSION,
                "principals": [
                    {
                        "service_slug": "rollback-first",
                        "principal_id": rolled_back_id,
                        "display_name": "Must Roll Back",
                        "capabilities": ["foundation.catalog:read"]
                    },
                    {
                        "service_slug": "rollback-collision",
                        "principal_id": collision_id,
                        "display_name": "Subject Collision",
                        "capabilities": ["foundation.catalog:read"]
                    }
                ]
            })
            .to_string(),
        )?,
        parse_bindings(
            &serde_json::json!({
                "schema_version": BINDINGS_SCHEMA_VERSION,
                "bindings": [
                    {
                        "service_slug": "rollback-first",
                        "zitadel_subject": format!("rollback-{rolled_back_id}")
                    },
                    {
                        "service_slug": "rollback-collision",
                        "zitadel_subject": occupied_subject
                    }
                ]
            })
            .to_string(),
        )?,
    )?;
    assert!(matches!(
        provision(&pool, &rollback_manifest).await,
        Err(ProvisionError::Database(_))
    ));
    let rolled_back: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (SELECT 1 FROM identity.service_principal WHERE id = $1)",
    )
    .bind(rolled_back_id)
    .fetch_one(&pool)
    .await?;
    assert!(rolled_back);

    // The contract principal keeps its grant history (the ledger refuses deletes); only the
    // occupied principal, which never received a grant, is removed.
    sqlx::query("DELETE FROM identity.service_principal WHERE id = $1")
        .bind(occupied_id)
        .execute(&pool)
        .await?;
    Ok(())
}

async fn grant_state(
    pool: &sqlx::PgPool,
    principal_id: Uuid,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT capability, granted_at::text \
         FROM identity.service_capability_grant \
         WHERE service_principal_id = $1 \
         ORDER BY capability",
    )
    .bind(principal_id)
    .fetch_all(pool)
    .await
}

/// A rebinding and a rename overwrite the principal row; the values it held stay recorded
/// (root ADR-0264).
async fn assert_rebinding_is_recorded(
    pool: &sqlx::PgPool,
    principal_id: Uuid,
    subject: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // An unchanged principal appended nothing.
    assert_eq!(principal_records(pool, principal_id).await?.len(), 1);
    let rebound_subject = format!("{subject}-rebound");
    let renamed =
        named_principal_manifest(principal_id, &rebound_subject, "Renamed Contract", &[])?;
    provision(pool, &renamed).await?;
    provision(pool, &renamed).await?;
    assert_eq!(
        principal_records(pool, principal_id).await?,
        vec![
            (subject.to_owned(), "Provisioning Contract".to_owned()),
            (rebound_subject, "Renamed Contract".to_owned()),
        ]
    );
    Ok(())
}

/// (subject, display name) of every recorded value of one principal, oldest first.
async fn principal_records(
    pool: &sqlx::PgPool,
    principal_id: Uuid,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT zitadel_subject, display_name FROM identity.service_principal_record          WHERE service_principal_id = $1 ORDER BY record_seq",
    )
    .bind(principal_id)
    .fetch_all(pool)
    .await
}

/// The ledgers refuse rewriting history, whoever asks.
async fn assert_ledger_refuses_rewrites(pool: &sqlx::PgPool, principal_id: Uuid) {
    for statement in [
        "UPDATE identity.service_capability_grant_record SET change = 'granted'          WHERE service_principal_id = $1",
        "DELETE FROM identity.service_capability_grant_record WHERE service_principal_id = $1",
        "UPDATE identity.service_principal_record SET display_name = 'x' WHERE service_principal_id = $1",
        "DELETE FROM identity.service_principal_record WHERE service_principal_id = $1",
    ] {
        assert!(sqlx::query(statement)
            .bind(principal_id)
            .execute(pool)
            .await
            .is_err());
    }
}

/// (granted records, revoked records, granted events, revoked events) for one principal.
async fn ledger(
    pool: &sqlx::PgPool,
    principal_id: Uuid,
) -> Result<(i64, i64, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "SELECT \
           (SELECT count(*) FROM identity.service_capability_grant_record \
            WHERE service_principal_id = $1 AND change = 'granted'), \
           (SELECT count(*) FROM identity.service_capability_grant_record \
            WHERE service_principal_id = $1 AND change = 'revoked'), \
           (SELECT count(*) FROM identity.outbox_event \
            WHERE type = 'identity.service_principal.capability_granted.v1' \
              AND payload->>'service_principal_id' = $1::text), \
           (SELECT count(*) FROM identity.outbox_event \
            WHERE type = 'identity.service_principal.capability_revoked.v1' \
              AND payload->>'service_principal_id' = $1::text)",
    )
    .bind(principal_id)
    .fetch_one(pool)
    .await
}

fn principal_manifest(
    principal_id: Uuid,
    subject: &str,
    capabilities: &[&str],
) -> Result<ValidatedManifest, ManifestError> {
    named_principal_manifest(principal_id, subject, "Provisioning Contract", capabilities)
}

fn named_principal_manifest(
    principal_id: Uuid,
    subject: &str,
    display_name: &str,
    capabilities: &[&str],
) -> Result<ValidatedManifest, ManifestError> {
    resolve_manifest(
        parse_policy(
            &serde_json::json!({
                "schema_version": POLICY_SCHEMA_VERSION,
                "principals": [{
                    "service_slug": "provisioning-contract",
                    "principal_id": principal_id,
                    "display_name": display_name,
                    "capabilities": capabilities
                }]
            })
            .to_string(),
        )?,
        parse_bindings(
            &serde_json::json!({
                "schema_version": BINDINGS_SCHEMA_VERSION,
                "bindings": [{
                    "service_slug": "provisioning-contract",
                    "zitadel_subject": subject
                }]
            })
            .to_string(),
        )?,
    )
}
