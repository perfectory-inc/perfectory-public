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
    let Ok(database_url) = env::var("IDENTITY_PROVISIONER_TEST_DATABASE_URL") else {
        return Ok(());
    };
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
    provision(&pool, &initial).await?;
    assert_eq!(grant_state(&pool, principal_id).await?, first_state);

    let reduced = principal_manifest(principal_id, &subject, &["foundation.catalog:read"])?;
    provision(&pool, &reduced).await?;
    let reduced_state = grant_state(&pool, principal_id).await?;
    assert_eq!(reduced_state.len(), 1);
    assert_eq!(reduced_state[0].0, "foundation.catalog:read");

    let revoked = principal_manifest(principal_id, &subject, &[])?;
    provision(&pool, &revoked).await?;
    assert!(grant_state(&pool, principal_id).await?.is_empty());
    provision(&pool, &revoked).await?;
    assert!(grant_state(&pool, principal_id).await?.is_empty());

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

    sqlx::query("DELETE FROM identity.service_principal WHERE id = ANY($1::uuid[])")
        .bind(vec![principal_id, occupied_id])
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

fn principal_manifest(
    principal_id: Uuid,
    subject: &str,
    capabilities: &[&str],
) -> Result<ValidatedManifest, ManifestError> {
    resolve_manifest(
        parse_policy(
            &serde_json::json!({
                "schema_version": POLICY_SCHEMA_VERSION,
                "principals": [{
                    "service_slug": "provisioning-contract",
                    "principal_id": principal_id,
                    "display_name": "Provisioning Contract",
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
