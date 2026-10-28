//! SQL and Rust spell the same vocabulary: unit keys, database enums and command answers. Moved out
//! of `spatial_tile_publication.rs` unchanged.

use super::*;

/// The column and the domain admit exactly the same unit keys.
///
/// A unit key becomes a Martin source id and a `PMTiles` object-key stem, so the two statements of
/// the rule — `vector_tile_publication_unit_key_check` in SQL and
/// [`catalog_domain::is_publication_unit_key`] in Rust — decide the same thing about storage. They
/// cannot share a definition across languages, so this compares them instead of trusting that they
/// were written to agree. They did not: the column admitted upper case and refused `.`, the
/// publisher refused upper case, admitted `.`, and let a key start with anything, and ADR-0013
/// 남은 부채 1 recorded that as the publisher being "narrower".
///
/// The candidates are chosen one per axis of that disagreement, plus the length boundary on both
/// sides of the limit.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_publication_unit_key_is_spelled_the_same_way_in_both_languages() -> TestResult {
    run_in_disposable_database("unit_key_spelling", |pool| async move {
        MIGRATOR.run(&pool).await?;

        let long = "a".repeat(128);
        let too_long = "a".repeat(129);
        let candidates = [
            "parcels",
            "complex-2",
            "legal_dong",
            "Parcels",
            "par.cels",
            "1parcels",
            "_parcels",
            "parcels ",
            "",
            long.as_str(),
            too_long.as_str(),
        ];

        for candidate in candidates {
            let domain_accepts = catalog_domain::is_publication_unit_key(candidate);
            let database_accepts = sqlx::query(
                "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key) VALUES ($1, $2)",
            )
            .bind(Uuid::new_v4())
            .bind(candidate)
            .execute(&pool)
            .await
            .is_ok();
            assert_eq!(
                domain_accepts, database_accepts,
                "the column and the domain disagree about {candidate:?}: \
                 domain={domain_accepts}, database={database_accepts}"
            );
        }
        Ok(())
    })
    .await
}

/// The values a single-list `IN` constraint admits, read out of its installed definition.
///
/// `pg_get_constraintdef` renders such a constraint as `= ANY (ARRAY['a'::text, 'b'::text])`, so
/// the admitted values are the odd-indexed pieces of a split on the quote character. That is exact
/// only while every value is `[a-z_]+` — no value carrying a quote of its own to escape — and while
/// the constraint really is one list. Both are asserted rather than assumed, because a reader that
/// quietly returns a short list would turn this comparison into one that always agrees.
fn admitted_values(constraint: &str, definition: &str) -> Vec<String> {
    // Two fragments rather than one: a `varchar` column renders the same list as
    // `= ANY ((ARRAY[...])::text[])`, and the question here is whether the constraint is one list,
    // not what the column's type happens to be.
    assert!(
        definition.contains("= ANY (") && definition.contains("ARRAY["),
        "{constraint} is no longer a single admitted-value list, so reading it as one would be \
         guesswork: {definition}"
    );
    let values: Vec<String> = definition
        .split('\'')
        .skip(1)
        .step_by(2)
        .map(ToOwned::to_owned)
        .collect();
    assert!(
        !values.is_empty()
            && values.iter().all(|value| {
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            }),
        "{constraint} admits values this reader cannot account for: {values:?}"
    );
    values
}

/// Every enum that spells a database vocabulary admits exactly what its constraint admits.
///
/// Several enums in `catalog_domain` write out values the database also constrains, and the first
/// three of them said so in a doc comment — "mirrors ... exactly" — with nothing checking it. That
/// is the same standing assumption `a_publication_unit_key_is_spelled_the_same_way_in_both_languages`
/// found already broken in three directions, so it is checked here for the same reason: the two
/// statements cannot share a definition across languages, and a claim that they agree is worth what
/// proves it.
///
/// The Rust side comes from each enum's own `ALL`, and the SQL side is read out of the installed
/// constraint. Neither list is restated here — only the binding between them, which is the one
/// thing that is genuinely this test's to know. That is what keeps registering a vocabulary down to
/// one row, and what lets ADR-0019 require it of every new one.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_database_vocabulary_is_spelled_the_same_way_in_both_languages() -> TestResult {
    run_in_disposable_database("database_vocabulary", |pool| async move {
        MIGRATOR.run(&pool).await?;

        let vocabularies: [(&str, Vec<&str>); 5] = [
            (
                "catalog_mutation_idempotency_command_kind_check",
                CatalogMutationKind::ALL
                    .into_iter()
                    .map(CatalogMutationKind::as_str)
                    .collect(),
            ),
            (
                "parcel_complex_membership_asserted_by_check",
                MembershipAssertedBy::ALL
                    .into_iter()
                    .map(MembershipAssertedBy::as_str)
                    .collect(),
            ),
            (
                "vector_tile_build_job_status_check",
                VectorTileBuildStatus::ALL
                    .into_iter()
                    .map(VectorTileBuildStatus::as_str)
                    .collect(),
            ),
            (
                "vector_tile_build_job_kind_check",
                VectorTileBuildKind::ALL
                    .into_iter()
                    .map(VectorTileBuildKind::as_str)
                    .collect(),
            ),
            (
                "vector_tile_release_source_kind_check",
                ServingSourceKind::ALL
                    .into_iter()
                    .map(ServingSourceKind::as_str)
                    .collect(),
            ),
        ];

        for (constraint, rust_spellings) in vocabularies {
            let definition: Option<String> = sqlx::query_scalar(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = $1",
            )
            .bind(constraint)
            .fetch_optional(&pool)
            .await?;
            // A missing constraint is a failure, not an empty comparison: `fetch_optional` is here
            // so this test says which name it could not find rather than comparing nothing and
            // agreeing.
            let definition = definition
                .ok_or_else(|| format!("{constraint} is not installed in the migrated schema"))?;

            let mut admitted = admitted_values(constraint, &definition);
            admitted.sort_unstable();
            let mut expected: Vec<String> = rust_spellings
                .into_iter()
                .map(ToOwned::to_owned)
                .collect::<Vec<String>>();
            expected.sort_unstable();

            assert_eq!(
                admitted, expected,
                "{constraint} and the Rust enum that spells it disagree"
            );
        }
        Ok(())
    })
    .await
}

/// A command carries an outcome manifest in the ledger exactly when the domain says it answers with
/// one.
///
/// `catalog_mutation_idempotency_manifest_outcome_check` names its kinds across two branches rather
/// than as one list, so it cannot be read the way the vocabularies above are. It is compared by
/// offering the database every kind both with and without an outcome and asking which pairs it
/// takes — which tests the constraint as installed rather than as parsed.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_command_answers_with_a_manifest_in_both_languages_or_in_neither() -> TestResult {
    run_in_disposable_database("manifest_outcome", |pool| async move {
        MIGRATOR.run(&pool).await?;

        let manifest_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest
                (id, manifest_generation, published_at)
             VALUES ($1, 1, now())",
        )
        .bind(manifest_id)
        .execute(&pool)
        .await?;

        // The key is `[A-Za-z0-9._:-]`, which has no underscore, so the command kind cannot be
        // spelled into it. An index keeps every probe distinct without smuggling the vocabulary in.
        let mut probe = 0;
        for kind in CatalogMutationKind::ALL {
            for outcome in [Some(manifest_id), None] {
                probe += 1;
                let accepted = sqlx::query(
                    "INSERT INTO catalog.catalog_mutation_idempotency
                        (idempotency_key, command_kind, request_fingerprint_sha256,
                         request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                )
                .bind(format!("probe-{probe}"))
                .bind(kind.as_str())
                .bind("0".repeat(64))
                .bind(CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION)
                .bind(outcome)
                .bind(Uuid::new_v4())
                .execute(&pool)
                .await
                .is_ok();

                let domain_expects = outcome.is_some() == kind.answers_with_manifest();
                assert_eq!(
                    accepted,
                    domain_expects,
                    "{} with{} an outcome manifest: database={accepted}, domain={domain_expects}",
                    kind.as_str(),
                    if outcome.is_some() { "" } else { "out" }
                );
            }
        }
        Ok(())
    })
    .await
}
