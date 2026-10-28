//! Idempotency keys: a true retry, a reused key, another fingerprint version and a contended key.
//! Moved out of `spatial_tile_publication.rs` unchanged.

use super::*;

/// A true retry: the identical body under the identical key is answered from the ledger.
///
/// This is the case Step 5 asks for and the one nothing covered before — the earlier "replay" test
/// sent a different body. Identical means identical, `expected_active_release_id: None` included, even
/// though that observation is now stale: a retry does not get to re-observe, and the ledger answers
/// before the compare-and-swap is ever evaluated.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_identical_retry_is_answered_from_the_ledger_and_writes_nothing() -> TestResult {
    run_in_disposable_database("tile_true_replay", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision =
            seed_data_revision(&pool, "parcels", PARCELS_SNAPSHOT, source_record_id).await?;
        let command = || {
            activation_command_with_key(
                "retry-me",
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )
        };

        let first = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(command()?)
            .await?;
        let manifests_before = manifest_count(&pool).await?;
        let releases_before = release_count(&pool).await?;
        let assets_before = file_asset_count(&pool).await?;

        let retried = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(command()?)
            .await?;

        assert!(
            retried.was_replayed(),
            "an identical retry must be answered from the ledger, not re-executed"
        );
        let replayed = retried.into_manifest();
        assert_eq!(
            replayed.current_version, first.current_version,
            "the retry must return the manifest the first call published"
        );
        assert_eq!(
            replayed.manifest_generation.value(),
            first.manifest_generation.value()
        );
        // Byte-identical, not merely equal-looking: the reply is re-derived from immutable rows, so a
        // drift in how it is rebuilt would show up here rather than in production on someone's retry.
        assert_eq!(
            serde_json::to_string(&replayed)?,
            serde_json::to_string(&first)?
        );

        assert_eq!(manifest_count(&pool).await?, manifests_before);
        assert_eq!(release_count(&pool).await?, releases_before);
        assert_eq!(file_asset_count(&pool).await?, assets_before);
        assert_eq!(
            outbox_types(&pool).await?,
            vec![RUNTIME_MANIFEST_PUBLISHED_V2],
            "a replay must not announce a second time"
        );
        assert_eq!(ledger_row_count(&pool).await?, 1);
        Ok(())
    })
    .await
}

/// The same key carrying a different request is a key reuse, not a retry.
///
/// Answering it with the earlier outcome would report a publication the caller never asked for. The
/// refusal has to be its own error: the caller's fix is to mint a new key, which is a different action
/// from "re-read state and retry" and from "resend the identical body".
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_idempotency_key_reused_for_a_different_request_is_refused() -> TestResult {
    run_in_disposable_database("tile_key_reuse", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision =
            seed_data_revision(&pool, "parcels", PARCELS_SNAPSHOT, source_record_id).await?;
        let next_revision =
            seed_data_revision(&pool, "parcels", NEXT_SNAPSHOT, source_record_id).await?;

        let published = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command_with_key(
                "reused-key",
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let manifests_before = manifest_count(&pool).await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(activation_command_with_key(
                "reused-key",
                "parcels",
                next_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                Some((
                    published_unit(&published, "parcels")?.active_release_id,
                    ServingGeneration::new(1)?,
                )),
            )?)
            .await
            .expect_err("a key already spent on another request must not answer this one");
        assert!(
            matches!(
                &error,
                CatalogError::MutationIdempotencyKeyReused { idempotency_key, command_kind }
                    if idempotency_key == "reused-key" && command_kind == "mark_tile_layer_dynamic"
            ),
            "got: {error:?}"
        );

        assert_eq!(
            active_pointer(&pool).await?,
            published.current_version.as_uuid()
        );
        assert_eq!(manifest_count(&pool).await?, manifests_before);
        assert_eq!(ledger_row_count(&pool).await?, 1);
        Ok(())
    })
    .await
}

/// A key recorded under another fingerprint encoding says so, instead of blaming the caller.
///
/// The version column is read before the digest is compared, because comparing digests from two
/// encodings is meaningless. Without this the operator gets "you reused a key" for a request they
/// never changed, and goes looking for a caller bug that does not exist. Note what it does *not* buy:
/// the key still has to be re-minted — making it replay would mean storing the old encoding, not its
/// name.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_key_recorded_under_another_fingerprint_version_is_named_as_such() -> TestResult {
    run_in_disposable_database("tile_fingerprint_version", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, "parcels", PARCELS_SNAPSHOT, source_record_id).await?;

        // A key left by a deployment that computed fingerprints differently. The digest is
        // irrelevant — it is never reached.
        sqlx::query(
            "INSERT INTO catalog.catalog_mutation_idempotency
             (idempotency_key, command_kind, request_fingerprint_sha256,
              request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
             VALUES ('older-encoding', 'mark_tile_layer_dynamic', repeat('a', 64),
                     'foundation-platform.catalog_mutation_fingerprint.v0', $1, $2)",
        )
        .bind(seed_bare_manifest(&pool).await?)
        .bind(OPERATOR_STAFF_ID)
        .execute(&pool)
        .await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(activation_command_with_key(
                "older-encoding",
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await
            .expect_err("a fingerprint from another encoding cannot be compared");
        assert!(
            matches!(
                &error,
                CatalogError::MutationFingerprintVersionChanged { idempotency_key, recorded, current }
                    if idempotency_key == "older-encoding"
                        && recorded == "foundation-platform.catalog_mutation_fingerprint.v0"
                        && current == CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION
            ),
            "the refusal must name the encoding change rather than blame the caller: {error:?}"
        );
        Ok(())
    })
    .await
}

/// A key held by an uncommitted transaction answers "contended", not an opaque server error.
///
/// This exists because the reasoning behind it is not self-evident and was flagged as needing proof
/// rather than argument: that `lock_timeout` bounds the wait an `ON CONFLICT DO NOTHING` insert
/// performs on a conflicting row's transaction. Without that bound the wait runs into the pool's
/// 2500ms `statement_timeout`, arrives as an infrastructure failure, and is served as a 500 with the
/// reason redacted — hiding the one action that resolves it.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_key_held_by_an_uncommitted_transaction_answers_contended() -> TestResult {
    run_in_disposable_database("tile_key_contended", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, "parcels", PARCELS_SNAPSHOT, source_record_id).await?;

        // Hold the key from a transaction that never commits. `outcome_manifest_id` may name a
        // manifest that does not exist because the foreign key is deferred to commit time, and this
        // transaction is rolled back — which is also a small proof that the deferral works.
        let mut holder = pool.begin().await?;
        sqlx::query(
            "INSERT INTO catalog.catalog_mutation_idempotency
             (idempotency_key, command_kind, request_fingerprint_sha256,
              request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
             VALUES ('contended-key', 'mark_tile_layer_dynamic', repeat('a', 64), 'test.v1', $1, $2)",
        )
        .bind(Uuid::new_v4())
        .bind(OPERATOR_STAFF_ID)
        .execute(&mut *holder)
        .await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(activation_command_with_key(
                "contended-key",
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await
            .expect_err("a key held by an uncommitted transaction cannot be claimed");
        assert!(
            matches!(
                &error,
                CatalogError::MutationContended { idempotency_key } if idempotency_key == "contended-key"
            ),
            "the wait must be bounded and named, not reported as an infrastructure failure: {error:?}"
        );

        holder.rollback().await?;
        assert_eq!(
            ledger_row_count(&pool).await?,
            0,
            "the holder rolled back, so its claim and its dangling manifest reference are gone"
        );
        Ok(())
    })
    .await
}
