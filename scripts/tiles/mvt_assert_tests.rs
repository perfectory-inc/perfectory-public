//! The decoder's unit tests, moved out of `mvt_assert.rs` unchanged. Foundation's `tiles_mvt_decoder`
//! test target compiles them with `cfg(test)` through `mvt_assert.rs`.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

fn varint(mut value: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            encoded.push(byte);
            return encoded;
        }
        encoded.push(byte | 0x80);
    }
}

fn field_varint(number: u32, value: u64) -> Vec<u8> {
    let mut encoded = varint(u64::from(number) << 3);
    encoded.extend(varint(value));
    encoded
}

fn field_bytes(number: u32, value: &[u8]) -> Vec<u8> {
    let mut encoded = varint((u64::from(number) << 3) | 2);
    encoded.extend(varint(value.len() as u64));
    encoded.extend(value);
    encoded
}

fn string_value(value: &str) -> Vec<u8> {
    field_bytes(1, value.as_bytes())
}

fn signed_value(value: i64) -> Vec<u8> {
    field_varint(4, value.cast_unsigned())
}

fn packed_u32_field(number: u32, values: &[u32]) -> Vec<u8> {
    let packed = values
        .iter()
        .flat_map(|value| varint(u64::from(*value)))
        .collect::<Vec<_>>();
    field_bytes(number, &packed)
}

fn tag_only_feature(tags: &[u32]) -> Vec<u8> {
    packed_u32_field(2, tags)
}

fn feature(tags: &[u32], geometry_type: u64, geometry: &[u32]) -> Vec<u8> {
    let mut encoded = packed_u32_field(2, tags);
    encoded.extend(field_varint(3, geometry_type));
    encoded.extend(packed_u32_field(4, geometry));
    encoded
}

fn point_geometry() -> Vec<u32> {
    vec![9, 20, 40]
}

fn polygon_geometry() -> Vec<u32> {
    // MoveTo(1), LineTo(3), ClosePath(1): a closed square ring.
    vec![9, 0, 0, 26, 20, 0, 0, 20, 19, 0, 15]
}

fn layer(name: &str, features: &[Vec<u8>], keys: &[&str], values: &[Vec<u8>]) -> Vec<u8> {
    let mut encoded = field_bytes(1, name.as_bytes());
    for feature in features {
        encoded.extend(field_bytes(2, feature));
    }
    for key in keys {
        encoded.extend(field_bytes(3, key.as_bytes()));
    }
    for value in values {
        encoded.extend(field_bytes(4, value));
    }
    encoded.extend(field_varint(5, 4096));
    encoded.extend(field_varint(15, 2));
    encoded
}

fn pnu_identity(layer: &str, pnu: &str) -> Identity {
    Identity::new(layer, vec![("pnu".to_owned(), pnu.to_owned())])
}

fn tile(layers: &[Vec<u8>]) -> Vec<u8> {
    layers
        .iter()
        .flat_map(|layer| field_bytes(3, layer))
        .collect()
}

fn sample_tile() -> Vec<u8> {
    let keys = ["pnu", "official_complex_code", "count"];
    let values = [
        string_value("9999900000000000001"),
        string_value("IC-SYNTHETIC-001"),
        signed_value(3),
        string_value("9999900000000000002"),
    ];
    let parcels = layer(
        "parcels",
        &[
            feature(&[0, 0, 1, 1], 3, &polygon_geometry()),
            feature(&[0, 3, 1, 1], 3, &polygon_geometry()),
        ],
        &keys,
        &values,
    );
    let aggregate = layer(
        "parcel_anchor_aggregate",
        &[feature(&[0, 0, 1, 1, 2, 2], 1, &point_geometry())],
        &keys,
        &values,
    );
    tile(&[aggregate, parcels])
}

#[test]
fn decodes_varints_and_rejects_overflow_or_truncation() {
    let mut cursor = Cursor::new(&[0xac, 0x02]);
    assert_eq!(cursor.read_varint().unwrap(), 300);
    assert!(Cursor::new(&[0x80]).read_varint().is_err());
    assert!(Cursor::new(&[0xff; 10]).read_varint().is_err());
}

#[test]
fn decodes_mvt_layers_features_and_string_or_integer_properties() {
    let decoded = parse_tile(&sample_tile()).unwrap();
    assert_eq!(decoded.layers.len(), 2);
    assert_eq!(decoded.layers["parcels"].features.len(), 2);
    assert_eq!(
        decoded.layers["parcels"].features[0].geometry_type,
        GeometryType::Polygon
    );
    assert_eq!(
        decoded.layers["parcel_anchor_aggregate"].features[0].properties["count"],
        PropertyValue::Signed(3)
    );
    assert_eq!(
        decoded.layers["parcels"].features[1].properties["pnu"],
        PropertyValue::String("9999900000000000002".to_owned())
    );
}

#[test]
fn canonical_identity_output_is_sorted_and_preserves_duplicates() {
    let decoded = parse_tile(&sample_tile()).unwrap();
    assert_eq!(
        canonical_identity_lines(&decoded, &["pnu".to_owned()]).unwrap(),
        vec![
            "layer=\"parcel_anchor_aggregate\"\tpnu=\"9999900000000000001\"",
            "layer=\"parcels\"\tpnu=\"9999900000000000001\"",
            "layer=\"parcels\"\tpnu=\"9999900000000000002\"",
        ]
    );
}

/// A layer whose features carry no `pnu` is dumpable, and its dump names the keys it used.
///
/// This is the case the tool could not express at all: `complex` features carry `complex_id`
/// and `official_complex_code`, so a `pnu`-keyed dump failed on the first feature. The second
/// half is why the keys live inside the identity — a dump keyed on the code alone must not
/// compare equal to one keyed on both.
#[test]
fn identity_keys_are_named_by_the_caller_and_travel_with_the_dump() {
    let decoded = parse_tile(&tile(&[layer(
        "complex",
        &[feature(&[0, 0, 1, 1], 3, &polygon_geometry())],
        &["complex_id", "official_complex_code"],
        &[
            string_value("00000000-0000-5000-8000-000000000001"),
            string_value("999ZZ0"),
        ],
    )]))
    .unwrap();

    assert!(canonical_identity_lines(&decoded, &["pnu".to_owned()])
        .unwrap_err()
        .contains("missing property \"pnu\""));
    assert_eq!(
        canonical_identity_lines(
            &decoded,
            &["complex_id".to_owned(), "official_complex_code".to_owned()],
        )
        .unwrap(),
        vec![concat!(
            "layer=\"complex\"",
            "\tcomplex_id=\"00000000-0000-5000-8000-000000000001\"",
            "\tofficial_complex_code=\"999ZZ0\"",
        )],
    );
    assert_ne!(
        canonical_identity_lines(&decoded, &["official_complex_code".to_owned()]).unwrap(),
        canonical_identity_lines(
            &decoded,
            &["complex_id".to_owned(), "official_complex_code".to_owned()],
        )
        .unwrap(),
    );
}

/// One invocation decodes many tiles, in the order they were named.
///
/// Whole-archive decoding is the check that catches an empty zoom or a missing layer, and it is
/// the check a proof drops first when each tile costs a process. Order is preserved rather than
/// globally sorted so the output equals what appending one dump per tile produced.
#[test]
fn dump_decodes_every_named_tile_in_order() {
    let directory = std::env::temp_dir().join(format!("mvt-assert-dump-{}", std::process::id()));
    fs::create_dir_all(&directory).unwrap();
    let first = directory.join("first.pbf");
    let second = directory.join("second.pbf");
    fs::write(
        &first,
        tile(&[layer(
            "complex",
            &[feature(&[0, 0], 3, &polygon_geometry())],
            &["official_complex_code"],
            &[string_value("999ZZ1")],
        )]),
    )
    .unwrap();
    fs::write(
        &second,
        tile(&[layer(
            "complex",
            &[feature(&[0, 0], 3, &polygon_geometry())],
            &["official_complex_code"],
            &[string_value("999ZZ0")],
        )]),
    )
    .unwrap();

    let output = execute(&[
        "dump".to_owned(),
        first.to_string_lossy().into_owned(),
        second.to_string_lossy().into_owned(),
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--identity-property".to_owned(),
        "official_complex_code".to_owned(),
    ])
    .unwrap();
    assert_eq!(
        output,
        concat!(
            "layer=\"complex\"\tofficial_complex_code=\"999ZZ1\"\n",
            "layer=\"complex\"\tofficial_complex_code=\"999ZZ0\"",
        )
    );

    fs::remove_dir_all(&directory).unwrap();
}

#[test]
fn dump_requires_the_identity_properties_it_is_keyed_on() {
    assert!(
        parse_dump_options(&["--content-encoding".to_owned(), "identity".to_owned(),])
            .unwrap_err()
            .contains("at least one --identity-property")
    );
    assert_eq!(
        parse_dump_options(&[
            "--content-encoding".to_owned(),
            "identity".to_owned(),
            "--identity-property".to_owned(),
            "complex_id".to_owned(),
            "--identity-property".to_owned(),
            "official_complex_code".to_owned(),
        ])
        .unwrap(),
        vec!["complex_id".to_owned(), "official_complex_code".to_owned()],
    );
    assert!(parse_dump_options(&[
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--identity-property".to_owned(),
        "pnu".to_owned(),
        "--identity-property".to_owned(),
        "pnu".to_owned(),
    ])
    .unwrap_err()
    .contains("duplicate --identity-property"));
}

#[test]
fn parse_accepts_nonempty_layer_expectation() {
    let arguments = vec![
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--expect-nonempty-layer".to_owned(),
        "admin".to_owned(),
    ];

    assert!(parse_expectations(&arguments).is_ok());
}

#[test]
fn nonempty_layer_assertion_checks_the_layer_without_identity_properties() {
    let decoded = parse_tile(&sample_tile()).unwrap();
    let arguments = vec![
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--expect-nonempty-layer".to_owned(),
        "parcels".to_owned(),
        "--expect-nonempty-layer".to_owned(),
        "parcel_anchor_aggregate".to_owned(),
    ];
    let expectations = parse_expectations(&arguments).unwrap();

    assert_tile(&decoded, &expectations).unwrap();
}

/// An empty tile is the only shape a "this layer serves nothing" assertion can have.
///
/// Martin answers a source with no rows with `204 No Content` and a zero-byte body, so the
/// proof's before/after contrast has to be expressible against zero bytes. The negative half is
/// worthless unless the same expectation *fails* once the layer is there, which is the second
/// half of this test.
#[test]
fn a_zero_count_asserts_the_layer_is_absent() {
    let empty = parse_tile(&[]).unwrap();
    let arguments = vec![
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--expect-layer".to_owned(),
        "complex=0".to_owned(),
    ];
    let expectations = parse_expectations(&arguments).unwrap();
    assert_tile(&empty, &expectations).unwrap();

    let populated = parse_tile(&tile(&[layer(
        "complex",
        &[feature(&[0, 0], 3, &polygon_geometry())],
        &["official_complex_code"],
        &[string_value("999ZZ0")],
    )]))
    .unwrap();
    assert!(assert_tile(&populated, &expectations)
        .unwrap_err()
        .contains("layer set mismatch"));

    // And the same tile satisfies the count the promoted half of the proof asserts, so the two
    // expectations differ only in the number.
    let promoted = parse_expectations(&[
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--expect-layer".to_owned(),
        "complex=1".to_owned(),
        "--expect-property".to_owned(),
        "official_complex_code=999ZZ0".to_owned(),
    ])
    .unwrap();
    assert_tile(&populated, &promoted).unwrap();
    assert!(assert_tile(&empty, &promoted)
        .unwrap_err()
        .contains("layer set mismatch"));
}

#[test]
fn assertions_require_exact_layers_counts_and_identity_sets() {
    let decoded = parse_tile(&sample_tile()).unwrap();
    let expectations = Expectations {
        layers: BTreeMap::from([
            ("parcel_anchor_aggregate".to_owned(), 1),
            ("parcels".to_owned(), 2),
        ]),
        nonempty_layers: BTreeSet::new(),
        pnus: BTreeSet::from([
            "9999900000000000001".to_owned(),
            "9999900000000000002".to_owned(),
        ]),
        complex_codes: BTreeSet::from(["IC-SYNTHETIC-001".to_owned()]),
        properties: vec![("count".to_owned(), "3".to_owned())],
        identity_properties: vec!["pnu".to_owned()],
        identities: vec![
            pnu_identity("parcel_anchor_aggregate", "9999900000000000001"),
            pnu_identity("parcels", "9999900000000000001"),
            pnu_identity("parcels", "9999900000000000002"),
        ],
    };
    assert_tile(&decoded, &expectations).unwrap();

    let mut wrong_count = expectations.clone();
    wrong_count.layers.insert("parcels".to_owned(), 3);
    assert!(assert_tile(&decoded, &wrong_count)
        .unwrap_err()
        .contains("feature count"));

    let mut missing_pnu = expectations;
    missing_pnu.pnus.insert("9999900000000000009".to_owned());
    assert!(assert_tile(&decoded, &missing_pnu)
        .unwrap_err()
        .contains("PNU set"));
}

#[test]
fn exact_identity_multiset_rejects_cross_layer_swaps_and_duplicates() {
    let decoded = parse_tile(&sample_tile()).unwrap();
    let base = Expectations {
        layers: BTreeMap::from([
            ("parcel_anchor_aggregate".to_owned(), 1),
            ("parcels".to_owned(), 2),
        ]),
        identity_properties: vec!["pnu".to_owned()],
        identities: vec![
            pnu_identity("parcel_anchor_aggregate", "9999900000000000001"),
            pnu_identity("parcels", "9999900000000000001"),
            pnu_identity("parcels", "9999900000000000002"),
        ],
        ..Expectations::default()
    };
    assert_tile(&decoded, &base).unwrap();

    let mut swapped = base.clone();
    swapped.identities = vec![
        pnu_identity("parcel_anchor_aggregate", "9999900000000000002"),
        pnu_identity("parcels", "9999900000000000001"),
        pnu_identity("parcels", "9999900000000000001"),
    ];
    assert!(assert_tile(&decoded, &swapped)
        .unwrap_err()
        .contains("identity multiset"));

    let mut duplicated = base;
    duplicated.identities[2] = duplicated.identities[1].clone();
    assert!(assert_tile(&decoded, &duplicated)
        .unwrap_err()
        .contains("identity multiset"));
}

#[test]
fn validates_renderable_point_and_polygon_geometry() {
    let decoded = parse_tile(&sample_tile()).unwrap();
    assert_eq!(
        decoded.layers["parcel_anchor_aggregate"].features[0].geometry_type,
        GeometryType::Point
    );

    let tag_only = layer(
        "parcel_anchor",
        &[tag_only_feature(&[0, 0, 1, 1])],
        &["pnu", "official_complex_code"],
        &[
            string_value("9999900000000000001"),
            string_value("IC-SYNTHETIC-001"),
        ],
    );
    assert!(parse_tile(&tile(&[tag_only]))
        .unwrap_err()
        .contains("geometry type"));

    let mut typed_but_empty_feature = tag_only_feature(&[0, 0, 1, 1]);
    typed_but_empty_feature.extend(field_varint(3, 1));
    let typed_but_empty = layer(
        "parcel_anchor",
        &[typed_but_empty_feature],
        &["pnu", "official_complex_code"],
        &[
            string_value("9999900000000000001"),
            string_value("IC-SYNTHETIC-001"),
        ],
    );
    assert!(parse_tile(&tile(&[typed_but_empty]))
        .unwrap_err()
        .contains("empty geometry command stream"));

    let truncated_point = layer(
        "parcel_anchor",
        &[feature(&[0, 0, 1, 1], 1, &[17, 20, 40])],
        &["pnu", "official_complex_code"],
        &[
            string_value("9999900000000000001"),
            string_value("IC-SYNTHETIC-001"),
        ],
    );
    assert!(parse_tile(&tile(&[truncated_point]))
        .unwrap_err()
        .contains("parameters"));

    let repeated_point_commands = layer(
        "parcel_anchor",
        &[feature(&[0, 0, 1, 1], 1, &[9, 20, 40, 9, 2, 2])],
        &["pnu", "official_complex_code"],
        &[
            string_value("9999900000000000001"),
            string_value("IC-SYNTHETIC-001"),
        ],
    );
    assert!(parse_tile(&tile(&[repeated_point_commands]))
        .unwrap_err()
        .contains("single MoveTo"));

    let open_polygon = layer(
        "parcels",
        &[feature(
            &[0, 0, 1, 1],
            3,
            &[9, 0, 0, 26, 20, 0, 0, 20, 19, 0],
        )],
        &["pnu", "official_complex_code"],
        &[
            string_value("9999900000000000001"),
            string_value("IC-SYNTHETIC-001"),
        ],
    );
    assert!(parse_tile(&tile(&[open_polygon]))
        .unwrap_err()
        .contains("ClosePath"));

    let zero_length_polygon_segment = layer(
        "parcels",
        &[feature(
            &[0, 0, 1, 1],
            3,
            &[9, 0, 0, 26, 0, 0, 20, 0, 19, 0, 15],
        )],
        &["pnu", "official_complex_code"],
        &[
            string_value("9999900000000000001"),
            string_value("IC-SYNTHETIC-001"),
        ],
    );
    assert!(parse_tile(&tile(&[zero_length_polygon_segment]))
        .unwrap_err()
        .contains("zero-length LineTo"));
}

#[test]
fn rejects_wrong_geometry_type_and_integer_identity_values() {
    let wrong_type = layer(
        "parcels",
        &[feature(&[0, 0, 1, 1], 1, &point_geometry())],
        &["pnu", "official_complex_code"],
        &[
            string_value("9999900000000000001"),
            string_value("IC-SYNTHETIC-001"),
        ],
    );
    assert!(parse_tile(&tile(&[wrong_type]))
        .unwrap_err()
        .contains("must use POLYGON"));

    let integer_pnu = layer(
        "parcel_anchor",
        &[feature(&[0, 0, 1, 1], 1, &point_geometry())],
        &["pnu", "official_complex_code"],
        &[signed_value(123), string_value("IC-SYNTHETIC-001")],
    );
    assert!(parse_tile(&tile(&[integer_pnu]))
        .unwrap_err()
        .contains("pnu must be an MVT string"));

    let integer_complex_code = layer(
        "parcel_anchor",
        &[feature(&[0, 0, 1, 1], 1, &point_geometry())],
        &["pnu", "official_complex_code"],
        &[string_value("9999900000000000001"), signed_value(123)],
    );
    assert!(parse_tile(&tile(&[integer_complex_code]))
        .unwrap_err()
        .contains("official_complex_code must be an MVT string"));
}

#[test]
fn rejects_malformed_tiles_and_non_identity_content_encoding() {
    assert!(parse_tile(&[0x1a, 0x05, 0x0a]).is_err());
    assert!(parse_tile(&[0x00]).unwrap_err().contains("field number 0"));
    assert!(execute(&[
        "dump".to_owned(),
        "missing.pbf".to_owned(),
        "--content-encoding".to_owned(),
        "br".to_owned(),
    ])
    .unwrap_err()
    .contains("content encoding must be identity"));
    assert!(execute(&["dump".to_owned(), "missing.pbf".to_owned()])
        .unwrap_err()
        .contains("--content-encoding identity is required"));
}

#[test]
fn parses_repeatable_exact_identity_cli_expectations() {
    let parsed = parse_expectations(&[
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--expect-layer".to_owned(),
        "parcels=2".to_owned(),
        // Deliberately before the key that names its field: the resolution happens after the
        // whole argument list is read, so flag order is not a hidden contract.
        "--expect-identity".to_owned(),
        "parcels|9999900000000000001".to_owned(),
        "--identity-property".to_owned(),
        "pnu".to_owned(),
        "--expect-identity".to_owned(),
        "parcels|9999900000000000002".to_owned(),
    ])
    .unwrap();
    assert_eq!(parsed.layers["parcels"], 2);
    assert_eq!(
        parsed.identities,
        vec![
            pnu_identity("parcels", "9999900000000000001"),
            pnu_identity("parcels", "9999900000000000002"),
        ]
    );
}

/// An identity expectation is only meaningful against the keys it was written for.
#[test]
fn identity_expectations_need_their_keys_and_must_match_them_in_arity() {
    assert!(parse_expectations(&[
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--expect-layer".to_owned(),
        "complex=1".to_owned(),
        "--expect-identity".to_owned(),
        "complex|999ZZ0".to_owned(),
    ])
    .unwrap_err()
    .contains("requires at least one --identity-property"));

    assert!(parse_expectations(&[
        "--content-encoding".to_owned(),
        "identity".to_owned(),
        "--expect-layer".to_owned(),
        "complex=1".to_owned(),
        "--identity-property".to_owned(),
        "complex_id".to_owned(),
        "--identity-property".to_owned(),
        "official_complex_code".to_owned(),
        "--expect-identity".to_owned(),
        "complex|999ZZ0".to_owned(),
    ])
    .unwrap_err()
    .contains("layer|complex_id|official_complex_code"));
}

#[test]
fn rejects_invalid_tag_indexes_and_duplicate_layer_names() {
    let bad_tags = layer("parcels", &[tag_only_feature(&[0, 9])], &["pnu"], &[]);
    assert!(parse_tile(&tile(&[bad_tags]))
        .unwrap_err()
        .contains("value index"));

    let first = layer("parcels", &[], &[], &[]);
    let second = layer("parcels", &[], &[], &[]);
    assert!(parse_tile(&tile(&[first, second]))
        .unwrap_err()
        .contains("duplicate layer"));
}
