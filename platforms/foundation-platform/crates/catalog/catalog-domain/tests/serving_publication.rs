//! Contract tests for the single-source v2 spatial publication manifest.

use catalog_domain::{
    validate_build_promotion, validate_build_result_report, validate_build_snapshot_binding,
    validate_serving_transition, BuildEvidenceDigest, CanonicalIcebergSnapshotId,
    ServingGeneration, ServingSelection, ServingSourceKind, VectorTileBuildOutcome,
    VectorTileBuildPromotionInput, VectorTileBuildPromotionVerdict, VectorTileBuildStatus,
    VectorTileRuntimeManifest,
};
use foundation_shared_kernel::ids::{VectorTileDataRevisionId, VectorTileReleaseId};
use serde_json::json;
use uuid::Uuid;

fn valid_manifest() -> serde_json::Value {
    json!({
        "schema_version": 2,
        "current_version": "0196e7e0-3c20-7000-8000-000000000052",
        "manifest_generation": 108,
        "refresh_after_seconds": 4,
        "published_at": "2026-07-24T00:00:00Z",
        "publication_units": {
            "parcels": {
                "data_revision": "0196e7e0-3c20-7000-8000-000000000061",
                "serving_generation": 42,
                "active_release_id": "0196e7e0-3c20-7000-8000-000000000062",
                "canonical_iceberg_snapshot_id": "70000000000000001",
                "source": {
                    "kind": "static_pmtiles",
                    "martin_source_id": "parcels-0196e7e0-3c20-7000-8000-000000000062",
                    "tiles_url_template": "https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}",
                    "pmtiles_object_key": "gold/vector-tiles/releases/0196e7e0-3c20-7000-8000-000000000062/parcels-0196e7e0-3c20-7000-8000-000000000062.pmtiles",
                    "pmtiles_file_asset_id": "0196e7e0-3c20-7000-8000-000000000063",
                    "pmtiles_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "pmtiles_bytes": 987_654_321
                },
                "layers": {
                    "parcels": {
                        "source_layer": "parcels",
                        "feature_id_property": "pnu",
                        "tile_min_zoom": 8,
                        "tile_max_zoom": 16,
                        "render_min_zoom": 10,
                        "render_max_zoom": 22,
                        "feature_filter_properties": {"pnu": "pnu"}
                    }
                },
                "lineage": {
                    "source_record_id": "0196e7e0-3c20-7000-8000-000000000064",
                    "source_file_asset_ids": ["0196e7e0-3c20-7000-8000-000000000065"]
                }
            }
        }
    })
}

#[test]
fn v2_manifest_accepts_one_complete_static_source() -> Result<(), String> {
    let manifest: VectorTileRuntimeManifest =
        serde_json::from_value(valid_manifest()).map_err(|error| error.to_string())?;
    assert_eq!(manifest.schema_version, 2);
    manifest.validate()?;
    Ok(())
}

#[test]
fn v2_manifest_rejects_unknown_source_and_dynamic_cache_busting_query() {
    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["kind"] = json!("overlay");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["kind"] = json!("dynamic_postgis");
    value["publication_units"]["parcels"]["source"] = json!({
        "kind": "dynamic_postgis",
        "martin_source_id": "parcels",
        "tiles_url_template": "http://127.0.0.1:3000/parcels/{z}/{x}/{y}",
        "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000063",
        "cache_policy": "no_store"
    });
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_ok());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"] = json!({
        "kind": "dynamic_postgis",
        "martin_source_id": "parcels",
        "tiles_url_template": "http://127.0.0.1:3000/parcels/{z}/{x}/{y}?generation=42",
        "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000063",
        "cache_policy": "no_store"
    });
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["tiles_url_template"] =
        serde_json::json!("http://127.0.0.1:3000/not-parcels/{z}/{x}/{y}");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());
}

#[test]
fn v2_manifest_rejects_javascript_unsafe_generation_and_bad_snapshot() {
    let mut value = valid_manifest();
    value["manifest_generation"] = json!(9_007_199_254_740_992_u64);
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["canonical_iceberg_snapshot_id"] = json!("0");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());
}

#[test]
fn v2_manifest_rejects_duplicate_source_ids_and_unsafe_unit_names() {
    // Both units are dynamic and internally consistent, so manifest-wide Martin source uniqueness
    // is the only invariant left to fail. A static fixture would fail its release-addressed
    // filename check first and the assertion could not tell the two reasons apart.
    let mut duplicate = valid_manifest();
    duplicate["publication_units"]["parcels"]["source"] = shared_dynamic_source();
    duplicate["publication_units"]["anchors"] = duplicate["publication_units"]["parcels"].clone();
    let message = serde_json::from_value::<VectorTileRuntimeManifest>(duplicate)
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        message.contains("already used by another publication unit"),
        "expected a manifest-wide source id conflict, got: {message}"
    );

    let mut unsafe_name = valid_manifest();
    let parcels_unit = unsafe_name["publication_units"]["parcels"].clone();
    unsafe_name["publication_units"] = serde_json::json!({"bad/name": parcels_unit});
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(unsafe_name).is_err());
}

fn shared_dynamic_source() -> serde_json::Value {
    json!({
        "kind": "dynamic_postgis",
        "martin_source_id": "shared",
        "tiles_url_template": "https://tiles.example.com/shared/{z}/{x}/{y}",
        "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000063",
        "cache_policy": "no_store"
    })
}

#[test]
fn v2_manifest_rejects_static_source_identity_that_does_not_match_release_filename() {
    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["martin_source_id"] =
        serde_json::json!("parcels");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["pmtiles_object_key"] = serde_json::json!(
        "gold/vector-tiles/releases/0196e7e0-3c20-7000-8000-000000000062/parcels.pmtiles"
    );
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());
}

fn selection(
    source_kind: ServingSourceKind,
    revision: u128,
    generation: u64,
) -> Result<ServingSelection, String> {
    Ok(ServingSelection {
        source_kind,
        data_revision: VectorTileDataRevisionId::new(Uuid::from_u128(revision)),
        serving_generation: ServingGeneration::new(generation)?,
    })
}

#[test]
fn serving_state_machine_allows_only_complete_source_transitions() -> Result<(), String> {
    let revision_a = 1;
    let revision_b = 2;

    assert!(validate_serving_transition(
        None,
        selection(ServingSourceKind::DynamicPostgis, revision_a, 1)?
    )
    .is_ok());

    let dynamic_a = selection(ServingSourceKind::DynamicPostgis, revision_a, 1)?;
    assert!(validate_serving_transition(
        Some(dynamic_a),
        selection(ServingSourceKind::StaticPmtiles, revision_a, 2)?
    )
    .is_ok());

    let static_a = selection(ServingSourceKind::StaticPmtiles, revision_a, 2)?;
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::DynamicPostgis, revision_b, 3)?
    )
    .is_ok());
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::DynamicPostgis, revision_a, 3)?
    )
    .is_ok());
    Ok(())
}

#[test]
fn serving_state_machine_rejects_stale_static_and_generation_gaps() -> Result<(), String> {
    let static_a = selection(ServingSourceKind::StaticPmtiles, 1, 7)?;
    let stale_static = selection(ServingSourceKind::StaticPmtiles, 2, 8)?;
    assert!(validate_serving_transition(Some(static_a), stale_static).is_err());

    assert!(
        validate_serving_transition(None, selection(ServingSourceKind::DynamicPostgis, 1, 2)?)
            .is_err()
    );
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::DynamicPostgis, 1, 9)?
    )
    .is_err());
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::StaticPmtiles, 1, 8)?
    )
    .is_ok());
    Ok(())
}

// --- static build lifecycle (guide Task 6 steps 2-3) ---------------------------
//
// A static release is only meaningful for the revision it was built from. These tests fix the two
// ways a build can become invalid while it runs: the unit moved on, or the build reported a
// snapshot that its input release was never pinned to.

const fn release_id(seed: u128) -> VectorTileReleaseId {
    VectorTileReleaseId::new(Uuid::from_u128(seed))
}

const fn promotion_input(
    status: VectorTileBuildStatus,
    input_release: u128,
    active_release: u128,
) -> VectorTileBuildPromotionInput {
    VectorTileBuildPromotionInput {
        status,
        input_release_id: release_id(input_release),
        active_release_id: release_id(active_release),
    }
}

#[test]
fn a_validated_build_promotes_only_while_its_input_release_is_still_active() {
    assert_eq!(
        validate_build_promotion(promotion_input(VectorTileBuildStatus::Validated, 10, 10)),
        Ok(VectorTileBuildPromotionVerdict::Promotable)
    );

    // R10 active, B10 starts from R10, an edit activates R11, B10 validates. Promoting now would
    // publish tiles for a revision the unit no longer serves, so the build is superseded — a
    // conflict would invite a retry that can never succeed.
    assert_eq!(
        validate_build_promotion(promotion_input(VectorTileBuildStatus::Validated, 10, 11)),
        Ok(VectorTileBuildPromotionVerdict::Superseded)
    );
}

#[test]
fn promotion_before_evidence_exists_is_refused_rather_than_superseded() {
    for status in [
        VectorTileBuildStatus::Planned,
        VectorTileBuildStatus::Running,
        VectorTileBuildStatus::Failed,
    ] {
        assert!(
            validate_build_promotion(promotion_input(status, 10, 10)).is_err(),
            "{} must not promote",
            status.as_str()
        );
    }
}

#[test]
fn build_status_spelling_round_trips_through_the_database_form() {
    for status in [
        VectorTileBuildStatus::Planned,
        VectorTileBuildStatus::Running,
        VectorTileBuildStatus::Validated,
        VectorTileBuildStatus::Promoted,
        VectorTileBuildStatus::Superseded,
        VectorTileBuildStatus::Failed,
    ] {
        assert_eq!(VectorTileBuildStatus::try_from(status.as_str()), Ok(status));
    }
    assert!(VectorTileBuildStatus::try_from("cancelled").is_err());
}

#[test]
fn only_promoted_superseded_and_failed_are_terminal() {
    assert!(VectorTileBuildStatus::Promoted.is_terminal());
    assert!(VectorTileBuildStatus::Superseded.is_terminal());
    assert!(VectorTileBuildStatus::Failed.is_terminal());
    assert!(!VectorTileBuildStatus::Planned.is_terminal());
    assert!(!VectorTileBuildStatus::Running.is_terminal());
    assert!(!VectorTileBuildStatus::Validated.is_terminal());
}

#[test]
fn a_build_must_report_the_snapshot_its_input_release_is_pinned_to() -> Result<(), String> {
    let pinned = CanonicalIcebergSnapshotId::new("7412".to_owned())?;
    let other = CanonicalIcebergSnapshotId::new("7413".to_owned())?;

    validate_build_snapshot_binding(&pinned, &pinned)?;
    assert!(validate_build_snapshot_binding(&other, &pinned).is_err());
    Ok(())
}

#[test]
fn evidence_digest_accepts_only_lowercase_hex_of_exactly_sha256_length() -> Result<(), String> {
    let digest = BuildEvidenceDigest::new("0".repeat(64))?;
    assert_eq!(digest.as_str().len(), 64);

    assert!(BuildEvidenceDigest::new("a".repeat(63)).is_err());
    assert!(BuildEvidenceDigest::new("a".repeat(65)).is_err());
    // Uppercase and non-hex both fail: the column is `character(64)` matching `^[0-9a-f]{64}$`,
    // so accepting either here would only move the failure to the insert.
    assert!(BuildEvidenceDigest::new("A".repeat(64)).is_err());
    assert!(BuildEvidenceDigest::new("g".repeat(64)).is_err());
    Ok(())
}

#[test]
fn a_build_can_only_report_the_two_outcomes_it_owns() -> Result<(), String> {
    let validated = VectorTileBuildOutcome::Validated(BuildEvidenceDigest::new("a".repeat(64))?);
    assert_eq!(validated.status(), VectorTileBuildStatus::Validated);

    let failed = VectorTileBuildOutcome::Failed("tippecanoe exited 1".to_owned());
    assert_eq!(failed.status(), VectorTileBuildStatus::Failed);

    // Neither outcome maps to a status the promotion decision owns. `promoted` and `superseded`
    // are unreachable from a build's own report by construction, which is the point of the type.
    for owned in [validated.status(), failed.status()] {
        assert!(!matches!(
            owned,
            VectorTileBuildStatus::Promoted | VectorTileBuildStatus::Superseded
        ));
    }
    Ok(())
}

#[test]
fn a_terminal_build_cannot_be_reported_again() -> Result<(), String> {
    let outcome = VectorTileBuildOutcome::Validated(BuildEvidenceDigest::new("b".repeat(64))?);

    // A running attempt may report; it has not been acted on yet.
    validate_build_result_report(VectorTileBuildStatus::Planned, &outcome)?;
    validate_build_result_report(VectorTileBuildStatus::Running, &outcome)?;

    // Each terminal status was already acted on: `promoted` moved the pointer, `superseded`
    // recorded that the unit moved past this revision, `failed` closed the attempt.
    for terminal in [
        VectorTileBuildStatus::Promoted,
        VectorTileBuildStatus::Superseded,
        VectorTileBuildStatus::Failed,
    ] {
        let message = validate_build_result_report(terminal, &outcome)
            .err()
            .unwrap_or_default();
        assert!(
            message.contains(terminal.as_str()),
            "refusal must name the terminal status, got: {message}"
        );
    }

    // `validated` is not terminal: a validated build is still awaiting a promotion decision, and
    // re-reporting it with fresh evidence is a retry of the same attempt, not a rewrite of a
    // decision. Refusing it here would strand builds that revalidate.
    validate_build_result_report(VectorTileBuildStatus::Validated, &outcome)?;
    Ok(())
}
