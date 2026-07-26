//! Contract guard for temporal parcel/admin identity through legal boundary changes.

use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("foundation-api must be nested under the repository root")
        .to_path_buf()
}

fn read_repo_text(relative: &str) -> String {
    let path = repo_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

#[test]
fn administrative_boundary_migration_keeps_external_codes_temporal_and_lineaged() {
    let migration = read_repo_text(
        "platforms/foundation-platform/migrations/20260727000001_administrative_boundary_identity.sql",
    );

    for table in [
        "catalog.administrative_unit",
        "catalog.administrative_unit_identifier",
        "catalog.administrative_unit_transition",
        "catalog.parcel_identifier",
        "catalog.parcel_administrative_unit",
    ] {
        assert!(
            migration.contains(&format!("CREATE TABLE {table}")),
            "migration must create {table}"
        );
    }

    assert!(migration.contains("PRIMARY KEY (id)"));
    assert!(migration.contains("stable_key"));
    assert!(migration.contains("effective_period daterange NOT NULL"));
    assert!(migration.contains("source_snapshot_id text NOT NULL"));
    assert!(migration.contains("source_record_id uuid"));
    assert!(migration.contains("EXCLUDE USING gist"));
    assert!(migration.contains("merged_into"));
    assert!(migration.contains("split_from"));
    assert!(migration.contains("replaced_by"));
    assert!(migration.contains("renamed"));

    assert!(
        migration.contains("FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id) ON DELETE RESTRICT")
    );
    assert!(!migration.contains("ON DELETE CASCADE"));
    assert!(!migration.contains("PRIMARY KEY (pnu)"));
}

#[test]
fn administrative_boundary_docs_define_revisioned_merge_and_static_promotion() {
    let architecture = read_repo_text("docs/architecture/administrative-boundary-versioning.md");
    let spatial_publication = read_repo_text("docs/architecture/single-source-spatial-publication.md");

    for phrase in [
        "stable internal UUID",
        "PNU",
        "effective-dated",
        "data_revision",
        "전남광주통합특별시",
        "dynamic",
        "PMTiles",
        "old identifier",
        "new identifier",
    ] {
        assert!(
            architecture.contains(phrase),
            "administrative boundary architecture must define {phrase:?}"
        );
    }

    assert!(spatial_publication.contains("one complete active tile source"));
    assert!(spatial_publication.contains("stale static build cannot replace a newer dynamic revision"));
}

