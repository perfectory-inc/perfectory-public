use std::fs;

use chrono::{TimeZone as _, Utc};
use serde_json::json;
use uuid::Uuid;

use super::{
    BronzeCatalogRecoveryManifest, BronzeCatalogRecoveryManifestCandidate,
    BronzeCatalogRecoveryManifestStatus, BronzeCatalogRecoverySourceManifest,
    BronzeCatalogRecoveryUnresolvedObject, RecoveryEvidenceArtifact, RecoverySourceSnapshot,
    BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION,
};
use collection_application::bronze_catalog_recovery::{
    BronzeCatalogRecoveryMode, RecoveryEvidenceKind,
};
use collection_domain::{SnapshotBasis, SnapshotGranularity};

#[test]
fn ready_manifest_converts_to_source_scoped_recovery_input_without_losing_lineage() {
    let manifest = ready_manifest();
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap();

    let inputs = manifest
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect("ready manifest should convert");

    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    assert_eq!(input.mode, BronzeCatalogRecoveryMode::DryRun);
    assert_eq!(input.source.slug, "vworldkr__land_characteristic");
    assert_eq!(input.source.auth_kind.wire_name(), "manual");
    assert_eq!(input.source.payload_format.wire_name(), "unknown");
    assert_eq!(input.evidence_manifest_uri, "target/audit/recovery.json");
    assert_eq!(input.evidence_manifest_sha256, "a".repeat(64));
    assert_eq!(input.started_at, started_at);
    assert_eq!(input.excluded_unresolved_object_count, 0);
    assert_eq!(input.candidates.len(), 1);
    let candidate = &input.candidates[0];
    assert_eq!(
        candidate.object_key.as_str(),
        "bronze/source=vworldkr__land_characteristic/20991231DS99992-9003.zip"
    );
    assert_eq!(candidate.snapshot_granularity, SnapshotGranularity::Day);
    assert_eq!(
        candidate.snapshot_basis,
        SnapshotBasis::ProviderSnapshotDate
    );
    assert_eq!(
        candidate.provider_file_id.as_deref(),
        Some("20991231DS99992-9003")
    );
    assert_eq!(
        candidate.evidence_kind,
        RecoveryEvidenceKind::ProviderInventory
    );
}

#[test]
fn blocked_or_unresolved_manifest_is_rejected_before_execution() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap();
    let mut blocked = ready_manifest();
    blocked.status = BronzeCatalogRecoveryManifestStatus::Blocked;

    let error = blocked
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect_err("blocked manifest must not execute");
    assert!(error.to_string().contains("blocked"));

    let mut unresolved = ready_manifest();
    unresolved
        .unresolved
        .push(BronzeCatalogRecoveryUnresolvedObject {
            source_slug: "vworldkr__land_characteristic".to_owned(),
            object_key: "bronze/source=vworldkr__land_characteristic/unresolved.zip".to_owned(),
            reason: "ambiguous_provider_inventory_match".to_owned(),
            matching_evidence_count: 2,
        });

    let error = unresolved
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect_err("unresolved manifest must not execute");
    assert!(error.to_string().contains("cannot hide unresolved objects"));
}

#[test]
fn executable_source_projections_keep_proven_candidates_and_explicit_quarantine() {
    let mut aggregate = ready_manifest();
    let mut blocked_source = aggregate.sources[0].clone();
    blocked_source.source.endpoint_slug = "vworld-dataset-parcel".to_owned();
    blocked_source.source.slug = "vworldkr__parcel".to_owned();
    blocked_source.candidates[0].object_key =
        "bronze/source=vworldkr__parcel/20991231DS99995-9008.zip".to_owned();
    aggregate.sources.push(blocked_source);
    aggregate.status = BronzeCatalogRecoveryManifestStatus::Blocked;
    aggregate
        .unresolved
        .push(BronzeCatalogRecoveryUnresolvedObject {
            source_slug: "vworldkr__parcel".to_owned(),
            object_key: "bronze/source=vworldkr__parcel/20991231DS99996-9009.zip".to_owned(),
            reason: "missing_provider_inventory_match".to_owned(),
            matching_evidence_count: 0,
        });

    let projections = aggregate.executable_source_projections();

    assert_eq!(projections.len(), 2);
    assert_eq!(
        projections[0].status,
        BronzeCatalogRecoveryManifestStatus::Ready
    );
    assert_eq!(
        projections[0].sources[0].source.slug,
        "vworldkr__land_characteristic"
    );
    assert!(projections[0].unresolved.is_empty());
    assert_eq!(
        projections[1].status,
        BronzeCatalogRecoveryManifestStatus::ReadyWithQuarantine
    );
    assert_eq!(projections[1].sources[0].source.slug, "vworldkr__parcel");
    assert_eq!(projections[1].sources[0].candidates.len(), 1);
    assert_eq!(projections[1].unresolved.len(), 1);

    let inputs = projections[1]
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap(),
        )
        .expect("proven candidates with explicit quarantine should execute");
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].candidates.len(), 1);
    assert_eq!(inputs[0].excluded_unresolved_object_count, 1);
}

#[test]
fn writing_executable_source_projections_removes_only_stale_generated_manifests() {
    let output_directory = std::env::temp_dir().join(format!(
        "foundation-bronze-recovery-projections-{}",
        Uuid::new_v4()
    ));
    fs::create_dir_all(&output_directory).expect("projection test directory");
    fs::write(output_directory.join("source=stale.json"), b"stale")
        .expect("stale projection fixture");
    fs::write(output_directory.join("ready-sources.json"), b"legacy")
        .expect("legacy aggregate fixture");
    fs::write(output_directory.join("keep.txt"), b"keep").expect("unrelated fixture");

    let written = ready_manifest()
        .write_executable_source_projections(&output_directory)
        .expect("executable projections should be written as one clean snapshot");

    assert_eq!(written.len(), 1);
    assert!(!output_directory.join("source=stale.json").exists());
    assert!(output_directory.join("keep.txt").exists());
    assert!(output_directory
        .join("source=vworldkr__land_characteristic.json")
        .exists());
    let aggregate: BronzeCatalogRecoveryManifest = serde_json::from_slice(
        &fs::read(output_directory.join("executable-sources.json"))
            .expect("executable aggregate projection"),
    )
    .expect("executable aggregate manifest");
    assert_eq!(aggregate.sources.len(), 1);
    assert_eq!(aggregate.status, BronzeCatalogRecoveryManifestStatus::Ready);
    assert!(!output_directory.join("ready-sources.json").exists());
    fs::remove_dir_all(output_directory).expect("projection test cleanup");
}

#[test]
fn ready_with_quarantine_rejects_hidden_or_overlapping_unresolved_objects() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap();
    let mut manifest = ready_manifest();
    manifest.status = BronzeCatalogRecoveryManifestStatus::ReadyWithQuarantine;
    manifest
        .unresolved
        .push(BronzeCatalogRecoveryUnresolvedObject {
            source_slug: "vworldkr__other".to_owned(),
            object_key: "bronze/source=vworldkr__other/unknown.zip".to_owned(),
            reason: "missing_provider_inventory_match".to_owned(),
            matching_evidence_count: 0,
        });

    let error = manifest
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect_err("unresolved objects outside the source scope must not be hidden");
    assert!(error.to_string().contains("unresolved source"));

    manifest.unresolved[0].source_slug = "vworldkr__land_characteristic".to_owned();
    manifest.unresolved[0].object_key = manifest.sources[0].candidates[0].object_key.clone();
    let error = manifest
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect_err("candidate and quarantine scopes must be disjoint");
    assert!(error.to_string().contains("both candidate and unresolved"));
}

#[test]
fn malformed_candidate_wire_values_are_rejected_before_execution() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap();

    for mutate in [
        |candidate: &mut BronzeCatalogRecoveryManifestCandidate| {
            candidate.snapshot_date = "2026-06-31".to_owned();
        },
        |candidate: &mut BronzeCatalogRecoveryManifestCandidate| {
            candidate.snapshot_granularity = "quarter".to_owned();
        },
        |candidate: &mut BronzeCatalogRecoveryManifestCandidate| {
            candidate.evidence_kind = "object_name_guess".to_owned();
        },
    ] {
        let mut manifest = ready_manifest();
        mutate(&mut manifest.sources[0].candidates[0]);

        assert!(manifest
            .to_recovery_inputs(
                BronzeCatalogRecoveryMode::DryRun,
                "target/audit/recovery.json",
                &"a".repeat(64),
                started_at,
            )
            .is_err());
    }
}

#[test]
fn source_without_candidates_is_rejected_before_execution() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap();
    let mut manifest = ready_manifest();
    manifest.sources[0].candidates.clear();

    let error = manifest
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect_err("empty source scope must not execute");

    assert!(error.to_string().contains("contains no candidates"));
}

#[test]
fn malformed_embedded_evidence_identity_is_rejected() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap();
    let mut manifest = ready_manifest();
    manifest.provider_inventory.sha256 = "not-a-sha256".to_owned();

    let error = manifest
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect_err("embedded evidence identity must be canonical");

    assert!(error.to_string().contains("provider_inventory"));
}

#[test]
fn duplicate_object_key_across_sources_is_rejected() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 3, 4, 5).unwrap();
    let mut manifest = ready_manifest();
    let duplicate_source = manifest.sources[0].clone();
    manifest.sources.push(duplicate_source);

    let error = manifest
        .to_recovery_inputs(
            BronzeCatalogRecoveryMode::DryRun,
            "target/audit/recovery.json",
            &"a".repeat(64),
            started_at,
        )
        .expect_err("duplicate recovery scope must not execute");

    assert!(error.to_string().contains("duplicate"));
}

fn ready_manifest() -> BronzeCatalogRecoveryManifest {
    BronzeCatalogRecoveryManifest {
        schema_version: BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION.to_owned(),
        generated_at_utc: "2026-07-14T01:02:03Z".to_owned(),
        status: BronzeCatalogRecoveryManifestStatus::Ready,
        endpoint_catalog: artifact("docs/catalog/endpoints.json"),
        provider_inventory: artifact("target/audit/inventory.json"),
        r2_inventory: artifact("target/audit/r2.json"),
        sources: vec![BronzeCatalogRecoverySourceManifest {
            source: RecoverySourceSnapshot {
                endpoint_slug: "vworld-dataset-land_characteristic".to_owned(),
                slug: "vworldkr__land_characteristic".to_owned(),
                name: "VWorld land characteristic".to_owned(),
                provider: "VWorld".to_owned(),
                dataset_name: "Land characteristic".to_owned(),
                base_url: Some("https://www.vworld.kr".to_owned()),
                auth_kind: "manual".to_owned(),
                payload_format: "unknown".to_owned(),
                terms_url: Some("https://www.vworld.kr/terms.do".to_owned()),
            },
            candidates: vec![BronzeCatalogRecoveryManifestCandidate {
                object_key: "bronze/source=vworldkr__land_characteristic/20991231DS99992-9003.zip"
                    .to_owned(),
                expected_size_bytes: 1024,
                expected_checksum_sha256: None,
                source_partition_key: Some(
                    "operation=land_characteristic/provider_file_id=20991231DS99992-9003"
                        .to_owned(),
                ),
                source_identity_key: "provider_file_id=20991231DS99992-9003".to_owned(),
                request_params: json!({"endpointSlug": "vworld-dataset-land_characteristic"}),
                content_type: "application/zip".to_owned(),
                logical_record_count: None,
                observed_r2_etag: Some("inventory-etag".to_owned()),
                observed_r2_last_modified: "2026-07-02T12:00:55Z".to_owned(),
                snapshot_period: Some("2026-06".to_owned()),
                snapshot_date: "2026-06-30".to_owned(),
                snapshot_granularity: "day".to_owned(),
                snapshot_basis: "provider_snapshot_date".to_owned(),
                provider_file_id: Some("20991231DS99992-9003".to_owned()),
                provider_file_name: Some("land-characteristic.zip".to_owned()),
                provider_updated_at: Some("2026-07-01".to_owned()),
                effective_date: None,
                evidence_kind: "provider_inventory".to_owned(),
            }],
        }],
        unresolved: Vec::new(),
    }
}

fn artifact(uri: &str) -> RecoveryEvidenceArtifact {
    RecoveryEvidenceArtifact {
        uri: uri.to_owned(),
        sha256: "b".repeat(64),
    }
}
