//! Cross-area recurrence guard for the Martin object-storage-first tile proof.

#![allow(clippy::expect_used)]

use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const LAYERS: [&str; 3] = ["parcel_anchor", "parcel_anchor_aggregate", "parcels"];
const FIXTURE_PNUS: [&str; 3] = [
    "9999900000000000001",
    "9999900000000000002",
    "9999900000000000003",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("foundation-api must be nested under the repository root")
        .to_path_buf()
}

fn read_json(relative: &str) -> Value {
    let path = repo_root().join(relative);
    let read_message = format!("read {}", path.display());
    let bytes = fs::read(&path).expect(&read_message);
    let parse_message = format!("parse {} as JSON", path.display());
    serde_json::from_slice(&bytes).expect(&parse_message)
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

fn object_keys(value: &Value) -> BTreeSet<&str> {
    value
        .as_object()
        .expect("contract value must be an object")
        .keys()
        .map(String::as_str)
        .collect()
}

fn assert_uuid(value: &Value, context: &str) {
    let string_message = format!("{context} must be a string UUID");
    let raw = value.as_str().expect(&string_message);
    let uuid_message = format!("{context} must be a UUID");
    Uuid::parse_str(raw).expect(&uuid_message);
}

fn insert_values<'a>(sql: &'a str, table: &str) -> &'a str {
    let marker = format!("INSERT INTO {table}");
    let insert_message = format!("fixture must insert into {table}");
    let insert = sql.split_once(&marker).expect(&insert_message).1;
    let values_message = format!("{table} insert must have a VALUES block");
    let conflict_message = format!("{table} fixture insert must be replay-safe");
    insert
        .split_once("\nVALUES\n")
        .expect(&values_message)
        .1
        .split_once("\nON CONFLICT")
        .expect(&conflict_message)
        .0
}

fn values_row<'a>(values: &'a str, id: &str, context: &str) -> &'a str {
    let id_literal = format!("'{id}'");
    let mut matches = values
        .split("\n    ),\n    (")
        .filter(|row| row.contains(&id_literal));
    let message = format!("{context} row {id} must exist");
    let row = matches.next().expect(&message);
    assert!(
        matches.next().is_none(),
        "{context} row {id} must be unique"
    );
    row
}

#[test]
#[allow(clippy::too_many_lines)]
fn local_manifest_and_martin_sources_cannot_drift() {
    let manifest = read_json("scripts/tiles/vector-tile-manifest.local.json");
    let dynamic = read_text("scripts/tiles/martin-dynamic.yaml");
    let static_config = read_text("scripts/tiles/martin-static.yaml");
    let flat_seed =
        read_text("platforms/foundation-platform/infra/db/seeds/local_vector_tile_manifest.sql");

    assert_eq!(manifest["schema_version"], 1);
    assert_uuid(&manifest["current_version"], "current_version");
    assert_uuid(&manifest["previous_version"], "previous_version");
    let current_version = manifest["current_version"]
        .as_str()
        .expect("current_version must be a string");
    let previous_version = manifest["previous_version"]
        .as_str()
        .expect("previous_version must be a string");
    assert_ne!(current_version, previous_version);
    assert!(
        !flat_seed.contains(current_version) && !flat_seed.contains(previous_version),
        "the PMTiles proof and the unrelated flat-tile seed must not reuse immutable version UUIDs"
    );
    assert_eq!(
        manifest["x_tiles_slice_proof"]["contract_status"],
        "proof-adapter-not-adr-0036-production"
    );

    let artifacts = &manifest["artifacts"];
    assert_eq!(object_keys(artifacts), BTreeSet::from(LAYERS));

    let dynamic_lines: Vec<_> = dynamic.lines().collect();
    let static_lines: Vec<_> = static_config.lines().collect();
    for (name, lines) in [("dynamic", &dynamic_lines), ("static", &static_lines)] {
        assert!(
            lines
                .iter()
                .all(|line| !line.contains('\t') && indentation(line).is_multiple_of(2)),
            "{name} Martin config must use two-space indentation and no tabs"
        );
    }
    assert_eq!(
        yaml_mapping_keys(&dynamic_lines, 0),
        BTreeSet::from(["listen_addresses", "on_invalid", "postgres"])
    );
    assert_eq!(
        yaml_mapping_keys(&static_lines, 0),
        BTreeSet::from(["listen_addresses", "on_invalid", "pmtiles"])
    );
    assert_eq!(
        yaml_scalar(&dynamic_lines, "listen_addresses", 0),
        "0.0.0.0:3000"
    );
    assert_eq!(
        yaml_scalar(&static_lines, "listen_addresses", 0),
        "0.0.0.0:3000"
    );
    assert_eq!(yaml_scalar(&dynamic_lines, "on_invalid", 0), "abort");
    assert_eq!(yaml_scalar(&static_lines, "on_invalid", 0), "abort");
    assert_eq!(
        yaml_scalar(&dynamic_lines, "connection_string", 2),
        "${DATABASE_URL}"
    );
    assert_eq!(yaml_scalar(&dynamic_lines, "pool_size", 2), "4");
    assert_eq!(yaml_scalar(&dynamic_lines, "auto_bounds", 2), "calc");
    assert_eq!(yaml_scalar(&dynamic_lines, "auto_publish", 2), "false");
    assert_eq!(yaml_scalar(&static_lines, "allow_http", 2), "true");

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
    let pmtiles_lines = yaml_block(&static_config, "pmtiles:", 0);
    assert_eq!(
        yaml_mapping_keys(&pmtiles_lines, 2),
        BTreeSet::from(["allow_http", "sources"])
    );
    let table_lines = yaml_block(&dynamic, "tables:", 2);
    assert_eq!(yaml_mapping_keys(&table_lines, 4), BTreeSet::from(LAYERS));
    let static_source_lines = yaml_block(&static_config, "sources:", 2);
    assert_eq!(
        yaml_mapping_keys(&static_source_lines, 4),
        BTreeSet::from(["foundation_static"])
    );
    assert_eq!(
        yaml_scalar(&static_source_lines, "foundation_static", 4),
        "${TILES_SLICE_PMTILES_URL}"
    );

    let expected_tables = [
        (
            "parcels",
            "tiles_slice_parcels",
            5179,
            "MULTIPOLYGON",
            14,
            16,
        ),
        (
            "parcel_anchor_aggregate",
            "tiles_slice_parcel_anchor_aggregate",
            4326,
            "POINT",
            0,
            11,
        ),
        (
            "parcel_anchor",
            "tiles_slice_parcel_anchor",
            4326,
            "POINT",
            12,
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
            "parcel_anchor_aggregate" => BTreeSet::from(["count", "official_complex_code", "pnu"]),
            "parcels" => BTreeSet::from(["PNU", "official_complex_code", "pnu"]),
            "parcel_anchor" => BTreeSet::from(["official_complex_code", "pnu"]),
            _ => unreachable!("expected_tables contains only guarded source IDs"),
        };
        assert_eq!(yaml_mapping_keys(&properties, 8), expected_properties);
    }

    let template = manifest["tiles_url_template"]
        .as_str()
        .expect("tiles_url_template must be a string");
    for layer in LAYERS {
        let artifact = &artifacts[layer];
        let (tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom) = match layer {
            "parcel_anchor_aggregate" => (0, 11, 0, 12),
            "parcel_anchor" => (12, 16, 12, 22),
            "parcels" => (14, 16, 14, 22),
            _ => unreachable!("LAYERS contains only the guarded source IDs"),
        };
        assert_eq!(artifact["source_layer"], layer);
        assert_eq!(artifact["object_key_prefix"], "foundation_static");
        assert_eq!(
            artifact["tilejson_object_key"],
            "tiles-slice-proof/local/foundation-static.tilejson.json"
        );
        assert_eq!(artifact["tile_min_zoom"], tile_min_zoom);
        assert_eq!(artifact["tile_max_zoom"], tile_max_zoom);
        assert_eq!(artifact["render_min_zoom"], render_min_zoom);
        assert_eq!(artifact["render_max_zoom"], render_max_zoom);
        assert_uuid(
            &artifact["lineage"]["source_record_id"],
            &format!("{layer}.lineage.source_record_id"),
        );
        assert_uuid(
            &artifact["lineage"]["manifest_file_asset_id"],
            &format!("{layer}.lineage.manifest_file_asset_id"),
        );
        assert_uuid(
            &artifact["lineage"]["tilejson_file_asset_id"],
            &format!("{layer}.lineage.tilejson_file_asset_id"),
        );
        for source_id in artifact["lineage"]["source_file_asset_ids"]
            .as_array()
            .expect("source_file_asset_ids must be an array")
        {
            assert_uuid(
                source_id,
                &format!("{layer}.lineage.source_file_asset_ids[]"),
            );
        }

        let url = template.replace("{object_key_prefix}", "foundation_static");
        assert_eq!(url, "http://127.0.0.1:3101/foundation_static/{z}/{x}/{y}");
        assert!(!url.trim_start_matches("http://").contains("//"));
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn tiles_slice_fixture_preserves_geometry_and_manifest_lineage_contract() {
    let fixture = read_text("scripts/tiles/fixture.sql");
    let manifest = read_json("scripts/tiles/vector-tile-manifest.local.json");

    assert!(fixture.contains("BEGIN;"));
    assert!(fixture.trim_end().ends_with("COMMIT;"));

    let fixture_pnus: Vec<_> = fixture
        .split('\'')
        .enumerate()
        .filter_map(|(index, value)| {
            (!index.is_multiple_of(2)
                && !value.is_empty()
                && value.bytes().all(|byte| byte.is_ascii_digit()))
            .then_some(value)
        })
        .filter(|value| value.starts_with("99999000000"))
        .collect();
    assert_eq!(fixture_pnus.len(), FIXTURE_PNUS.len());
    assert!(fixture_pnus.iter().all(|pnu| pnu.len() == 19));
    assert_eq!(
        fixture_pnus.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from(FIXTURE_PNUS)
    );

    let canonical_lineage = &manifest["artifacts"]["parcels"]["lineage"];
    let source_record_id = canonical_lineage["source_record_id"]
        .as_str()
        .expect("parcels source_record_id must be a string");
    let manifest_file_asset_id = canonical_lineage["manifest_file_asset_id"]
        .as_str()
        .expect("parcels manifest_file_asset_id must be a string");
    let tilejson_file_asset_id = canonical_lineage["tilejson_file_asset_id"]
        .as_str()
        .expect("parcels tilejson_file_asset_id must be a string");
    let source_file_asset_id = canonical_lineage["source_file_asset_ids"]
        .as_array()
        .expect("parcels source_file_asset_ids must be an array")
        .as_slice();
    assert_eq!(source_file_asset_id.len(), 1);
    let source_file_asset_id = source_file_asset_id[0]
        .as_str()
        .expect("parcels source_file_asset_ids[0] must be a string");

    for layer in LAYERS {
        let lineage = &manifest["artifacts"][layer]["lineage"];
        assert_eq!(lineage["source_record_id"], source_record_id, "{layer}");
        assert_eq!(
            lineage["manifest_file_asset_id"], manifest_file_asset_id,
            "{layer}"
        );
        assert_eq!(
            lineage["tilejson_file_asset_id"], tilejson_file_asset_id,
            "{layer}"
        );
        assert_eq!(
            lineage["source_file_asset_ids"],
            serde_json::json!([source_file_asset_id]),
            "{layer}"
        );
    }

    let source_values = insert_values(&fixture, "catalog.source_record");
    let source_row = values_row(source_values, source_record_id, "source record");
    assert!(source_row.contains("'tiles-slice-proof-fixture'"));
    assert!(source_row.contains("'tiles-slice-proof/fixture/source.json'"));

    let file_asset_values = insert_values(&fixture, "catalog.file_asset");
    for (role, id, object_key) in [
        (
            "manifest file asset",
            manifest_file_asset_id,
            "tiles-slice-proof/local/manifest.json",
        ),
        (
            "TileJSON file asset",
            tilejson_file_asset_id,
            "tiles-slice-proof/local/foundation-static.tilejson.json",
        ),
        (
            "geometry source file asset",
            source_file_asset_id,
            "tiles-slice-proof/fixture/parcel-boundaries.geojson",
        ),
    ] {
        let row = values_row(file_asset_values, id, role);
        assert!(row.contains(&format!("'{object_key}'")), "{role}");
        assert!(row.contains(&format!("'{source_record_id}'")), "{role}");
    }

    assert!(fixture.contains("public.ST_Transform("));
    assert!(fixture.contains("public.ST_SetSRID("));
    assert!(fixture.contains("public.geometry(MultiPolygon, 5179)"));
    assert!(fixture.contains("public.geometry(Point, 4326)"));
    assert!(fixture.contains("boundary.pnu::text AS \"PNU\""));
    assert!(fixture.contains("'silver.parcel_boundaries'"));
    assert!(fixture.contains("'succeeded'"));

    let expected_views = [
        "serving_postgis.tiles_slice_parcels",
        "serving_postgis.tiles_slice_parcel_anchor_aggregate",
        "serving_postgis.tiles_slice_parcel_anchor",
    ];
    assert_eq!(
        fixture
            .matches("CREATE OR REPLACE VIEW serving_postgis.tiles_slice_")
            .count(),
        expected_views.len()
    );
    for view in expected_views {
        assert_eq!(
            fixture
                .matches(&format!("CREATE OR REPLACE VIEW {view} AS"))
                .count(),
            1,
            "fixture must define exactly one {view} view"
        );
    }
}
