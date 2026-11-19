//! Cross-area recurrence guard for the Martin sources the boundary tile proof serves.
//!
//! The parcel slice (`parcels`, `parcel_anchor*`, its fixture, local manifests and static Martin
//! config) retired with the PostGIS parcel copy (root ADR-0571): parcel tiles are baked from the
//! lakehouse and served from R2.

#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("foundation-api must be nested under the repository root")
        .to_path_buf()
}

fn read_text(relative: &str) -> String {
    let path = repo_root().join(relative);
    let message = format!("read {}", path.display());
    fs::read_to_string(&path).expect(&message)
}

fn indentation(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

fn yaml_block<'a>(yaml: &'a str, header: &str, indent: usize) -> Vec<&'a str> {
    let marker = format!("{}{header}", " ".repeat(indent));
    let mut found = false;
    let mut result = Vec::new();
    for line in yaml.lines() {
        if !found {
            found = line == marker;
            continue;
        }
        if !line.trim().is_empty() && indentation(line) <= indent {
            break;
        }
        result.push(line);
    }
    assert!(found, "YAML block {header:?} must exist");
    result
}

fn yaml_mapping_keys<'a>(lines: &'a [&str], indent: usize) -> BTreeSet<&'a str> {
    let keys: Vec<_> = lines
        .iter()
        .filter(|line| indentation(line) == indent)
        .filter(|line| !line.trim_start().starts_with('#'))
        .filter_map(|line| line.trim().split_once(':').map(|(key, _)| key))
        .collect();
    let unique: BTreeSet<_> = keys.iter().copied().collect();
    assert_eq!(
        keys.len(),
        unique.len(),
        "YAML mapping at indentation {indent} must not contain duplicate keys"
    );
    unique
}

fn yaml_scalar<'a>(lines: &'a [&str], key: &str, indent: usize) -> &'a str {
    let prefix = format!("{}{key}: ", " ".repeat(indent));
    let message = format!("YAML scalar {key:?} at indentation {indent} must exist");
    lines
        .iter()
        .find_map(|line| line.strip_prefix(&prefix))
        .expect(&message)
}

#[test]
fn martin_sources_cannot_drift() {
    let dynamic = read_text("scripts/tiles/martin-dynamic.yaml");
    let production_static_config = read_text("scripts/tiles/martin-static-production.yaml");
    let publication_migration = read_text(
        "platforms/foundation-platform/migrations/20260724000001_spatial_tile_publication.sql",
    );

    let dynamic_lines: Vec<_> = dynamic.lines().collect();
    assert!(
        dynamic_lines
            .iter()
            .all(|line| !line.contains('\t') && indentation(line).is_multiple_of(2)),
        "dynamic Martin config must use two-space indentation and no tabs"
    );
    assert_eq!(
        yaml_mapping_keys(&dynamic_lines, 0),
        BTreeSet::from(["cache", "listen_addresses", "on_invalid", "postgres"])
    );
    assert_eq!(
        yaml_scalar(&dynamic_lines, "listen_addresses", 0),
        "0.0.0.0:3000"
    );
    assert_eq!(yaml_scalar(&dynamic_lines, "on_invalid", 0), "abort");
    assert_eq!(yaml_scalar(&dynamic_lines, "cache", 0), "disable");
    assert!(!dynamic.contains("\ncache_size_mb:"));
    assert!(production_static_config.contains("pmtiles:\n  aws_access_key_id:"));
    assert!(production_static_config.contains("\n  paths:"));
    assert!(production_static_config.contains("reload_interval:"));
    assert!(!production_static_config.contains("allow_http"));
    assert!(!production_static_config.contains("sources:"));
    assert!(publication_migration.contains("vector_tile_runtime_manifest_pointer"));
    assert!(publication_migration.contains("release.source_kind = 'dynamic_postgis'"));
    assert_eq!(
        yaml_scalar(&dynamic_lines, "connection_string", 2),
        "${DATABASE_URL}"
    );
    assert_eq!(yaml_scalar(&dynamic_lines, "pool_size", 2), "4");
    assert_eq!(yaml_scalar(&dynamic_lines, "auto_bounds", 2), "calc");
    assert_eq!(yaml_scalar(&dynamic_lines, "auto_publish", 2), "false");

    let postgres_lines = yaml_block(&dynamic, "postgres:", 0);
    assert_eq!(
        yaml_mapping_keys(&postgres_lines, 2),
        BTreeSet::from([
            "auto_bounds",
            "auto_publish",
            "connection_string",
            "pool_size",
            "tables",
        ])
    );
    let table_lines = yaml_block(&dynamic, "tables:", 2);
    assert_eq!(
        yaml_mapping_keys(&table_lines, 4),
        BTreeSet::from(["admin", "complex"])
    );

    let expected_tables = [
        (
            "admin",
            "administrative_unit_boundary_current",
            4326,
            "MULTIPOLYGON",
            5,
            16,
        ),
        (
            "complex",
            "industrial_complex_boundary_current",
            4326,
            "MULTIPOLYGON",
            6,
            16,
        ),
    ];
    for (source, table, srid, geometry_type, minzoom, maxzoom) in expected_tables {
        let source_lines = yaml_block(&dynamic, &format!("{source}:"), 4);
        assert_eq!(
            yaml_mapping_keys(&source_lines, 6),
            BTreeSet::from([
                "geometry_column",
                "geometry_type",
                "layer_id",
                "maxzoom",
                "minzoom",
                "properties",
                "schema",
                "srid",
                "table",
            ])
        );
        assert_eq!(yaml_scalar(&source_lines, "schema", 6), "serving_postgis");
        assert_eq!(yaml_scalar(&source_lines, "table", 6), table);
        assert_eq!(yaml_scalar(&source_lines, "srid", 6), srid.to_string());
        assert_eq!(yaml_scalar(&source_lines, "geometry_column", 6), "geom");
        assert_eq!(
            yaml_scalar(&source_lines, "geometry_type", 6),
            geometry_type
        );
        assert_eq!(yaml_scalar(&source_lines, "layer_id", 6), source);
        assert_eq!(
            yaml_scalar(&source_lines, "minzoom", 6),
            minzoom.to_string()
        );
        assert_eq!(
            yaml_scalar(&source_lines, "maxzoom", 6),
            maxzoom.to_string()
        );

        let source_yaml = source_lines.join("\n");
        let properties = yaml_block(&source_yaml, "properties:", 6);
        let expected_properties = match source {
            "admin" => BTreeSet::from([
                "administrative_unit_id",
                "canonical_code",
                "display_name",
                "scope_kind",
            ]),
            // The feature IS a complex, so the code is its own identity, and these are the two
            // columns `serving_postgis.industrial_complex_boundary_current` exposes. Both are
            // identities. Designation status, development progress and occupancy are deliberately
            // not here: a tile is cached, and a value that moves goes stale in it without the
            // geometry doing so.
            "complex" => BTreeSet::from(["complex_id", "official_complex_code"]),
            _ => unreachable!("expected_tables contains only guarded source IDs"),
        };
        assert_eq!(yaml_mapping_keys(&properties, 8), expected_properties);
    }
}
