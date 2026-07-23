//! Minimal, dependency-free Mapbox Vector Tile assertion tool for the tile proof.
//!
//! This intentionally decodes raw (identity-encoded) protobuf bytes. It is not a
//! general-purpose MVT renderer: it validates the point/polygon command grammar,
//! layer IDs, and scalar feature properties needed by this proof.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::process;

const USAGE: &str = "Usage:\n\
  mvt_assert dump <tile.pbf> --content-encoding identity\n\
  mvt_assert assert <tile.pbf> --content-encoding identity\n\
      --expect-layer <name>=<count> [--expect-layer ...]\n\
      [--expect-pnu <pnu>]... [--expect-complex-code <code>]...\n\
      [--expect-identity <layer>|<pnu>|<complex-code>]...\n\
      [--expect-property <key>=<value>]...";

#[derive(Clone, Debug, PartialEq)]
enum PropertyValue {
    String(String),
    Signed(i64),
    Unsigned(u64),
    Bool(bool),
    Float32(f32),
    Float64(f64),
}

impl PropertyValue {
    fn assertion_text(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Signed(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::Bool(value) => value.to_string(),
            Self::Float32(value) => value.to_string(),
            Self::Float64(value) => value.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeometryType {
    Point,
    LineString,
    Polygon,
}

impl GeometryType {
    fn decode(value: u64) -> Result<Self, String> {
        match value {
            1 => Ok(Self::Point),
            2 => Ok(Self::LineString),
            3 => Ok(Self::Polygon),
            _ => Err(format!("invalid MVT geometry type {value}")),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Point => "POINT",
            Self::LineString => "LINESTRING",
            Self::Polygon => "POLYGON",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Feature {
    properties: BTreeMap<String, PropertyValue>,
    geometry_type: GeometryType,
}

#[derive(Clone, Debug, PartialEq)]
struct Layer {
    features: Vec<Feature>,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Tile {
    layers: BTreeMap<String, Layer>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Identity {
    layer: String,
    pnu: String,
    complex_code: String,
}

impl Identity {
    fn new(
        layer: impl Into<String>,
        pnu: impl Into<String>,
        complex_code: impl Into<String>,
    ) -> Self {
        Self {
            layer: layer.into(),
            pnu: pnu.into(),
            complex_code: complex_code.into(),
        }
    }

    fn canonical_line(&self) -> String {
        format!(
            "layer={}\tpnu={}\tofficial_complex_code={}",
            quote(&self.layer),
            quote(&self.pnu),
            quote(&self.complex_code)
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Expectations {
    layers: BTreeMap<String, usize>,
    pnus: BTreeSet<String>,
    complex_codes: BTreeSet<String>,
    properties: Vec<(String, String)>,
    identities: Vec<Identity>,
}

enum Operation {
    Dump,
    Assert(Expectations),
}

#[derive(Clone, Copy)]
struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    const fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    const fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn read_varint(&mut self) -> Result<u64, String> {
        let mut value = 0_u64;
        for index in 0..10 {
            let byte = *self
                .bytes
                .get(self.position)
                .ok_or_else(|| "truncated protobuf varint".to_owned())?;
            self.position += 1;

            if index == 9 && byte > 1 {
                return Err("protobuf varint overflows u64".to_owned());
            }
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err("protobuf varint is longer than 10 bytes".to_owned())
    }

    fn read_key(&mut self) -> Result<(u32, u8), String> {
        let key = self.read_varint()?;
        let field = key >> 3;
        if field == 0 {
            return Err("invalid protobuf field number 0".to_owned());
        }
        if field > 0x1fff_ffff {
            return Err(format!("protobuf field number {field} exceeds the limit"));
        }
        let field = u32::try_from(field)
            .map_err(|_| "validated protobuf field does not fit u32".to_owned())?;
        let wire_type = u8::try_from(key & 0x07)
            .map_err(|_| "validated protobuf wire type does not fit u8".to_owned())?;
        Ok((field, wire_type))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], String> {
        let length = usize::try_from(self.read_varint()?)
            .map_err(|_| "protobuf length does not fit this platform".to_owned())?;
        if length > self.remaining() {
            return Err(format!(
                "truncated length-delimited protobuf field: need {length} bytes, have {}",
                self.remaining()
            ));
        }
        let start = self.position;
        self.position += length;
        Ok(&self.bytes[start..start + length])
    }

    fn skip_exact(&mut self, length: usize) -> Result<(), String> {
        if length > self.remaining() {
            return Err(format!(
                "truncated protobuf field: need {length} bytes, have {}",
                self.remaining()
            ));
        }
        self.position += length;
        Ok(())
    }

    fn skip_field(&mut self, wire_type: u8) -> Result<(), String> {
        match wire_type {
            0 => {
                self.read_varint()?;
                Ok(())
            }
            1 => self.skip_exact(8),
            2 => {
                self.read_bytes()?;
                Ok(())
            }
            5 => self.skip_exact(4),
            3 | 4 => Err("protobuf groups are not supported in MVT input".to_owned()),
            other => Err(format!("invalid protobuf wire type {other}")),
        }
    }
}

fn expect_wire(actual: u8, expected: u8, context: &str) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{context} uses wire type {actual}, expected {expected}"
        ))
    }
}

fn utf8(bytes: &[u8], context: &str) -> Result<String, String> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|error| format!("{context} is not valid UTF-8: {error}"))
}

fn parse_tile(bytes: &[u8]) -> Result<Tile, String> {
    let mut cursor = Cursor::new(bytes);
    let mut layers = BTreeMap::new();

    while !cursor.is_empty() {
        let (field, wire_type) = cursor.read_key()?;
        if field == 3 {
            expect_wire(wire_type, 2, "Tile.layers")?;
            let (name, layer) = parse_layer(cursor.read_bytes()?)?;
            if layers.insert(name.clone(), layer).is_some() {
                return Err(format!("duplicate layer name {name:?}"));
            }
        } else {
            cursor.skip_field(wire_type)?;
        }
    }

    Ok(Tile { layers })
}

fn parse_layer(bytes: &[u8]) -> Result<(String, Layer), String> {
    let mut cursor = Cursor::new(bytes);
    let mut name = None;
    let mut version = None;
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut encoded_features = Vec::new();

    while !cursor.is_empty() {
        let (field, wire_type) = cursor.read_key()?;
        match field {
            1 => {
                expect_wire(wire_type, 2, "Layer.name")?;
                if name.is_some() {
                    return Err("layer contains more than one name".to_owned());
                }
                name = Some(utf8(cursor.read_bytes()?, "layer name")?);
            }
            2 => {
                expect_wire(wire_type, 2, "Layer.features")?;
                encoded_features.push(cursor.read_bytes()?);
            }
            3 => {
                expect_wire(wire_type, 2, "Layer.keys")?;
                keys.push(utf8(cursor.read_bytes()?, "property key")?);
            }
            4 => {
                expect_wire(wire_type, 2, "Layer.values")?;
                values.push(parse_value(cursor.read_bytes()?)?);
            }
            15 => {
                expect_wire(wire_type, 0, "Layer.version")?;
                if version.is_some() {
                    return Err("layer contains more than one version".to_owned());
                }
                version = Some(cursor.read_varint()?);
            }
            _ => cursor.skip_field(wire_type)?,
        }
    }

    let name = name.ok_or_else(|| "MVT layer is missing its required name".to_owned())?;
    let version = version.ok_or_else(|| format!("MVT layer {name:?} is missing its version"))?;
    if !matches!(version, 1 | 2) {
        return Err(format!(
            "MVT layer {name:?} has unsupported version {version}"
        ));
    }

    let features = encoded_features
        .into_iter()
        .enumerate()
        .map(|(index, encoded)| {
            parse_feature(encoded, &keys, &values)
                .and_then(|feature| validate_layer_geometry_type(&name, feature))
                .map_err(|error| format!("layer {name:?} feature {index}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((name, Layer { features }))
}

fn validate_layer_geometry_type(layer_name: &str, feature: Feature) -> Result<Feature, String> {
    let expected = match layer_name {
        "parcels" => Some(GeometryType::Polygon),
        "parcel_anchor" | "parcel_anchor_aggregate" => Some(GeometryType::Point),
        _ => None,
    };
    if let Some(expected) = expected {
        if feature.geometry_type != expected {
            return Err(format!(
                "layer {layer_name:?} must use {} geometry, got {}",
                expected.label(),
                feature.geometry_type.label()
            ));
        }
    }
    Ok(feature)
}

fn parse_value(bytes: &[u8]) -> Result<PropertyValue, String> {
    let mut cursor = Cursor::new(bytes);
    let mut value = None;

    while !cursor.is_empty() {
        let (field, wire_type) = cursor.read_key()?;
        let decoded = match field {
            1 => {
                expect_wire(wire_type, 2, "Value.string_value")?;
                Some(PropertyValue::String(utf8(
                    cursor.read_bytes()?,
                    "string property value",
                )?))
            }
            2 => {
                expect_wire(wire_type, 5, "Value.float_value")?;
                let start = cursor.position;
                cursor.skip_exact(4)?;
                let raw = cursor.bytes[start..start + 4]
                    .try_into()
                    .expect("four bytes were checked");
                Some(PropertyValue::Float32(f32::from_le_bytes(raw)))
            }
            3 => {
                expect_wire(wire_type, 1, "Value.double_value")?;
                let start = cursor.position;
                cursor.skip_exact(8)?;
                let raw = cursor.bytes[start..start + 8]
                    .try_into()
                    .expect("eight bytes were checked");
                Some(PropertyValue::Float64(f64::from_le_bytes(raw)))
            }
            4 => {
                expect_wire(wire_type, 0, "Value.int_value")?;
                Some(PropertyValue::Signed(cursor.read_varint()?.cast_signed()))
            }
            5 => {
                expect_wire(wire_type, 0, "Value.uint_value")?;
                Some(PropertyValue::Unsigned(cursor.read_varint()?))
            }
            6 => {
                expect_wire(wire_type, 0, "Value.sint_value")?;
                let encoded = cursor.read_varint()?;
                let decoded = (encoded >> 1).cast_signed() ^ -(encoded & 1).cast_signed();
                Some(PropertyValue::Signed(decoded))
            }
            7 => {
                expect_wire(wire_type, 0, "Value.bool_value")?;
                let encoded = cursor.read_varint()?;
                if encoded > 1 {
                    return Err(format!("boolean property has invalid value {encoded}"));
                }
                Some(PropertyValue::Bool(encoded == 1))
            }
            _ => {
                cursor.skip_field(wire_type)?;
                None
            }
        };

        if let Some(decoded) = decoded {
            if value.replace(decoded).is_some() {
                return Err("MVT Value contains multiple scalar values".to_owned());
            }
        }
    }

    value.ok_or_else(|| "MVT Value contains no supported scalar value".to_owned())
}

fn parse_feature(
    bytes: &[u8],
    keys: &[String],
    values: &[PropertyValue],
) -> Result<Feature, String> {
    let mut cursor = Cursor::new(bytes);
    let mut tags = Vec::new();
    let mut geometry_type = None;
    let mut geometry = Vec::new();

    while !cursor.is_empty() {
        let (field, wire_type) = cursor.read_key()?;
        match field {
            2 => append_repeated_u32(&mut cursor, wire_type, &mut tags, "Feature.tags")?,
            3 => {
                expect_wire(wire_type, 0, "Feature.type")?;
                if geometry_type.is_some() {
                    return Err("feature contains more than one geometry type".to_owned());
                }
                geometry_type = Some(GeometryType::decode(cursor.read_varint()?)?);
            }
            4 => append_repeated_u32(&mut cursor, wire_type, &mut geometry, "Feature.geometry")?,
            _ => cursor.skip_field(wire_type)?,
        }
    }

    if tags.len() % 2 != 0 {
        return Err(format!(
            "feature has an odd number of tag indexes ({})",
            tags.len()
        ));
    }

    let mut properties = BTreeMap::new();
    for indexes in tags.chunks_exact(2) {
        let key_index = indexes[0] as usize;
        let value_index = indexes[1] as usize;
        let key = keys.get(key_index).ok_or_else(|| {
            format!(
                "property key index {key_index} is out of range for {} keys",
                keys.len()
            )
        })?;
        let value = values.get(value_index).ok_or_else(|| {
            format!(
                "property value index {value_index} is out of range for {} values",
                values.len()
            )
        })?;
        if matches!(key.as_str(), "pnu" | "official_complex_code")
            && !matches!(value, PropertyValue::String(_))
        {
            return Err(format!("{key} must be an MVT string, got {value:?}"));
        }
        if properties.insert(key.clone(), value.clone()).is_some() {
            return Err(format!("feature repeats property key {key:?}"));
        }
    }

    let geometry_type =
        geometry_type.ok_or_else(|| "feature is missing its geometry type".to_owned())?;
    if geometry.is_empty() {
        return Err("feature has an empty geometry command stream".to_owned());
    }
    validate_geometry(geometry_type, &geometry)?;

    Ok(Feature {
        properties,
        geometry_type,
    })
}

fn append_repeated_u32(
    cursor: &mut Cursor<'_>,
    wire_type: u8,
    output: &mut Vec<u32>,
    context: &str,
) -> Result<(), String> {
    match wire_type {
        0 => output.push(to_u32(cursor.read_varint()?, context)?),
        2 => {
            let mut packed = Cursor::new(cursor.read_bytes()?);
            while !packed.is_empty() {
                output.push(to_u32(packed.read_varint()?, context)?);
            }
        }
        _ => return Err(format!("{context} uses invalid wire type {wire_type}")),
    }
    Ok(())
}

fn validate_geometry(geometry_type: GeometryType, geometry: &[u32]) -> Result<(), String> {
    let mut position = 0;
    match geometry_type {
        GeometryType::Point => {
            let count = expect_geometry_command(geometry, &mut position, 1, "MoveTo")?;
            consume_coordinate_parameters(geometry, &mut position, count, "MoveTo", false)?;
            if position != geometry.len() {
                return Err("POINT geometry must consist of a single MoveTo command".to_owned());
            }
        }
        GeometryType::LineString => {
            while position < geometry.len() {
                let move_count = expect_geometry_command(geometry, &mut position, 1, "MoveTo")?;
                if move_count != 1 {
                    return Err(format!(
                        "LINESTRING MoveTo count must be 1, got {move_count}"
                    ));
                }
                consume_coordinate_parameters(
                    geometry,
                    &mut position,
                    move_count,
                    "MoveTo",
                    false,
                )?;
                let line_count = expect_geometry_command(geometry, &mut position, 2, "LineTo")?;
                consume_coordinate_parameters(geometry, &mut position, line_count, "LineTo", true)?;
            }
        }
        GeometryType::Polygon => {
            while position < geometry.len() {
                let move_count = expect_geometry_command(geometry, &mut position, 1, "MoveTo")?;
                if move_count != 1 {
                    return Err(format!("POLYGON MoveTo count must be 1, got {move_count}"));
                }
                consume_coordinate_parameters(
                    geometry,
                    &mut position,
                    move_count,
                    "MoveTo",
                    false,
                )?;

                let line_count = expect_geometry_command(geometry, &mut position, 2, "LineTo")?;
                if line_count < 2 {
                    return Err(format!(
                        "POLYGON LineTo count must be at least 2, got {line_count}"
                    ));
                }
                consume_coordinate_parameters(geometry, &mut position, line_count, "LineTo", true)?;

                let close_count = expect_geometry_command(geometry, &mut position, 7, "ClosePath")?;
                if close_count != 1 {
                    return Err(format!(
                        "POLYGON ClosePath count must be 1, got {close_count}"
                    ));
                }
            }
        }
    }
    Ok(())
}

fn expect_geometry_command(
    geometry: &[u32],
    position: &mut usize,
    expected_id: u32,
    expected_name: &str,
) -> Result<u32, String> {
    let encoded = geometry
        .get(*position)
        .ok_or_else(|| format!("geometry is missing required {expected_name} command"))?;
    *position += 1;
    let command_id = encoded & 0x07;
    let count = encoded >> 3;
    if count == 0 {
        return Err(format!(
            "geometry command {} has a zero repeat count",
            geometry_command_name(command_id)
        ));
    }
    if command_id != expected_id {
        return Err(format!(
            "expected {expected_name} command, got {}",
            geometry_command_name(command_id)
        ));
    }
    Ok(count)
}

fn consume_coordinate_parameters(
    geometry: &[u32],
    position: &mut usize,
    command_count: u32,
    command_name: &str,
    reject_zero_delta: bool,
) -> Result<(), String> {
    let parameter_count = usize::try_from(command_count)
        .ok()
        .and_then(|count| count.checked_mul(2))
        .ok_or_else(|| format!("{command_name} parameter count overflows usize"))?;
    let remaining = geometry.len() - *position;
    if remaining < parameter_count {
        return Err(format!(
            "{command_name} declares {parameter_count} coordinate parameters, but only {remaining} remain"
        ));
    }
    if reject_zero_delta
        && geometry[*position..*position + parameter_count]
            .chunks_exact(2)
            .any(|delta| delta == [0, 0])
    {
        return Err(format!(
            "{command_name} contains a zero-length LineTo segment"
        ));
    }
    *position += parameter_count;
    Ok(())
}

const fn geometry_command_name(command_id: u32) -> &'static str {
    match command_id {
        1 => "MoveTo",
        2 => "LineTo",
        7 => "ClosePath",
        _ => "unknown command",
    }
}

fn to_u32(value: u64, context: &str) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("{context} {value} exceeds u32"))
}

fn canonical_identity_lines(tile: &Tile) -> Result<Vec<String>, String> {
    Ok(tile_identities(tile)?
        .into_iter()
        .map(|identity| identity.canonical_line())
        .collect())
}

fn tile_identities(tile: &Tile) -> Result<Vec<Identity>, String> {
    let mut identities = Vec::new();
    for (layer_name, layer) in &tile.layers {
        for (index, feature) in layer.features.iter().enumerate() {
            let pnu = required_string_property(feature, "pnu", layer_name, index)?;
            let complex_code =
                required_string_property(feature, "official_complex_code", layer_name, index)?;
            identities.push(Identity::new(layer_name, pnu, complex_code));
        }
    }
    identities.sort_unstable();
    Ok(identities)
}

fn required_property<'a>(
    feature: &'a Feature,
    key: &str,
    layer_name: &str,
    feature_index: usize,
) -> Result<&'a PropertyValue, String> {
    feature.properties.get(key).ok_or_else(|| {
        format!("layer {layer_name:?} feature {feature_index} is missing property {key:?}")
    })
}

fn required_string_property<'a>(
    feature: &'a Feature,
    key: &str,
    layer_name: &str,
    feature_index: usize,
) -> Result<&'a str, String> {
    match required_property(feature, key, layer_name, feature_index)? {
        PropertyValue::String(value) => Ok(value),
        other => Err(format!(
            "layer {layer_name:?} feature {feature_index} property {key:?} must be an MVT string, got {other:?}"
        )),
    }
}

fn quote(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            control if control.is_control() => {
                use std::fmt::Write;
                write!(escaped, "\\u{:04x}", control as u32)
                    .expect("writing to a String cannot fail");
            }
            other => escaped.push(other),
        }
    }
    escaped.push('"');
    escaped
}

fn assert_tile(tile: &Tile, expectations: &Expectations) -> Result<(), String> {
    let actual_layers = tile.layers.keys().cloned().collect::<BTreeSet<_>>();
    let expected_layers = expectations.layers.keys().cloned().collect::<BTreeSet<_>>();
    if actual_layers != expected_layers {
        return Err(format!(
            "layer set mismatch: expected {expected_layers:?}, got {actual_layers:?}"
        ));
    }

    for (name, expected_count) in &expectations.layers {
        let actual_count = tile.layers[name].features.len();
        if actual_count != *expected_count {
            return Err(format!(
                "layer {name:?} feature count mismatch: expected {expected_count}, got {actual_count}"
            ));
        }
    }

    if !expectations.pnus.is_empty() {
        let actual = identity_set(tile, "pnu")?;
        if actual != expectations.pnus {
            return Err(format!(
                "PNU set mismatch: expected {:?}, got {actual:?}",
                expectations.pnus
            ));
        }
    }
    if !expectations.complex_codes.is_empty() {
        let actual = identity_set(tile, "official_complex_code")?;
        if actual != expectations.complex_codes {
            return Err(format!(
                "complex-code set mismatch: expected {:?}, got {actual:?}",
                expectations.complex_codes
            ));
        }
    }
    if !expectations.identities.is_empty() {
        let actual = tile_identities(tile)?;
        let mut expected = expectations.identities.clone();
        expected.sort_unstable();
        if actual != expected {
            return Err(format!(
                "identity multiset mismatch: expected {expected:?}, got {actual:?}"
            ));
        }
    }

    for (key, expected_value) in &expectations.properties {
        let found = tile.layers.values().any(|layer| {
            layer.features.iter().any(|feature| {
                feature
                    .properties
                    .get(key)
                    .is_some_and(|value| value.assertion_text() == *expected_value)
            })
        });
        if !found {
            return Err(format!(
                "no feature has property {key:?} with value {expected_value:?}"
            ));
        }
    }

    Ok(())
}

fn identity_set(tile: &Tile, key: &str) -> Result<BTreeSet<String>, String> {
    let mut values = BTreeSet::new();
    for (layer_name, layer) in &tile.layers {
        for (index, feature) in layer.features.iter().enumerate() {
            let value = required_property(feature, key, layer_name, index)?;
            match value {
                PropertyValue::String(value) => {
                    values.insert(value.clone());
                }
                other => {
                    return Err(format!(
                        "layer {layer_name:?} feature {index} property {key:?} must be an MVT string, got {other:?}"
                    ));
                }
            }
        }
    }
    Ok(values)
}

fn parse_expectations(arguments: &[String]) -> Result<Expectations, String> {
    let mut expectations = Expectations::default();
    let mut has_content_encoding = false;
    let mut index = 0;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| format!("{option} requires a value\n{USAGE}"))?;
        match option.as_str() {
            "--content-encoding" => {
                if has_content_encoding {
                    return Err("duplicate --content-encoding option".to_owned());
                }
                require_identity_content_encoding(value)?;
                has_content_encoding = true;
            }
            "--expect-layer" => {
                let (name, count) = split_assignment(value, "layer expectation")?;
                let count = count.parse::<usize>().map_err(|error| {
                    format!("invalid feature count {count:?} for layer {name:?}: {error}")
                })?;
                if expectations.layers.insert(name.to_owned(), count).is_some() {
                    return Err(format!("duplicate --expect-layer for {name:?}"));
                }
            }
            "--expect-pnu" => {
                require_nonempty(value, "PNU")?;
                if !expectations.pnus.insert(value.clone()) {
                    return Err(format!("duplicate --expect-pnu {value:?}"));
                }
            }
            "--expect-complex-code" => {
                require_nonempty(value, "complex code")?;
                if !expectations.complex_codes.insert(value.clone()) {
                    return Err(format!("duplicate --expect-complex-code {value:?}"));
                }
            }
            "--expect-identity" => {
                expectations.identities.push(parse_identity(value)?);
            }
            "--expect-property" => {
                let (key, expected) = split_assignment(value, "property expectation")?;
                expectations
                    .properties
                    .push((key.to_owned(), expected.to_owned()));
            }
            _ => return Err(format!("unknown option {option:?}\n{USAGE}")),
        }
        index += 2;
    }
    if !has_content_encoding {
        return Err(format!("--content-encoding identity is required\n{USAGE}"));
    }
    if expectations.layers.is_empty() {
        return Err(format!(
            "assert requires at least one --expect-layer\n{USAGE}"
        ));
    }
    Ok(expectations)
}

fn parse_identity(value: &str) -> Result<Identity, String> {
    let fields = value.split('|').collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err(format!(
            "identity expectation must have the form layer|pnu|complex-code, got {value:?}"
        ));
    }
    require_nonempty(fields[0], "identity layer")?;
    require_nonempty(fields[1], "identity PNU")?;
    require_nonempty(fields[2], "identity complex code")?;
    Ok(Identity::new(fields[0], fields[1], fields[2]))
}

fn require_identity_content_encoding(value: &str) -> Result<(), String> {
    if value.trim().eq_ignore_ascii_case("identity") {
        Ok(())
    } else {
        Err(format!(
            "content encoding must be identity, got {value:?}; compressed MVT bytes are not accepted"
        ))
    }
}

fn parse_dump_options(arguments: &[String]) -> Result<(), String> {
    match arguments {
        [option, value] if option == "--content-encoding" => {
            require_identity_content_encoding(value)
        }
        [] => Err(format!("--content-encoding identity is required\n{USAGE}")),
        _ => Err(format!(
            "dump requires exactly --content-encoding identity\n{USAGE}"
        )),
    }
}

fn split_assignment<'a>(value: &'a str, context: &str) -> Result<(&'a str, &'a str), String> {
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| format!("{context} must have the form name=value"))?;
    require_nonempty(key, &format!("{context} name"))?;
    require_nonempty(value, &format!("{context} value"))?;
    Ok((key, value))
}

fn require_nonempty(value: &str, context: &str) -> Result<(), String> {
    if value.is_empty() {
        Err(format!("{context} must not be empty"))
    } else {
        Ok(())
    }
}

fn execute(arguments: &[String]) -> Result<String, String> {
    let command = arguments.first().ok_or_else(|| USAGE.to_owned())?;
    let path = arguments.get(1).ok_or_else(|| USAGE.to_owned())?;
    let operation = match command.as_str() {
        "dump" => {
            parse_dump_options(&arguments[2..])?;
            Operation::Dump
        }
        "assert" => Operation::Assert(parse_expectations(&arguments[2..])?),
        _ => return Err(format!("unknown command {command:?}\n{USAGE}")),
    };

    let bytes = fs::read(path).map_err(|error| format!("cannot read {path:?}: {error}"))?;
    let tile = parse_tile(&bytes).map_err(|error| format!("cannot decode {path:?}: {error}"))?;

    match operation {
        Operation::Dump => Ok(canonical_identity_lines(&tile)?.join("\n")),
        Operation::Assert(expectations) => {
            assert_tile(&tile, &expectations)?;
            let feature_count = tile
                .layers
                .values()
                .map(|layer| layer.features.len())
                .sum::<usize>();
            Ok(format!(
                "OK layers={} features={feature_count}",
                tile.layers.len()
            ))
        }
    }
}

fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match execute(&arguments) {
        Ok(output) => {
            if !output.is_empty() {
                println!("{output}");
            }
        }
        Err(error) => {
            eprintln!("mvt_assert: {error}");
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
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
            canonical_identity_lines(&decoded).unwrap(),
            vec![
                "layer=\"parcel_anchor_aggregate\"\tpnu=\"9999900000000000001\"\tofficial_complex_code=\"IC-SYNTHETIC-001\"",
                "layer=\"parcels\"\tpnu=\"9999900000000000001\"\tofficial_complex_code=\"IC-SYNTHETIC-001\"",
                "layer=\"parcels\"\tpnu=\"9999900000000000002\"\tofficial_complex_code=\"IC-SYNTHETIC-001\"",
            ]
        );
    }

    #[test]
    fn assertions_require_exact_layers_counts_and_identity_sets() {
        let decoded = parse_tile(&sample_tile()).unwrap();
        let expectations = Expectations {
            layers: BTreeMap::from([
                ("parcel_anchor_aggregate".to_owned(), 1),
                ("parcels".to_owned(), 2),
            ]),
            pnus: BTreeSet::from([
                "9999900000000000001".to_owned(),
                "9999900000000000002".to_owned(),
            ]),
            complex_codes: BTreeSet::from(["IC-SYNTHETIC-001".to_owned()]),
            properties: vec![("count".to_owned(), "3".to_owned())],
            identities: vec![
                Identity::new(
                    "parcel_anchor_aggregate",
                    "9999900000000000001",
                    "IC-SYNTHETIC-001",
                ),
                Identity::new("parcels", "9999900000000000001", "IC-SYNTHETIC-001"),
                Identity::new("parcels", "9999900000000000002", "IC-SYNTHETIC-001"),
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
            identities: vec![
                Identity::new(
                    "parcel_anchor_aggregate",
                    "9999900000000000001",
                    "IC-SYNTHETIC-001",
                ),
                Identity::new("parcels", "9999900000000000001", "IC-SYNTHETIC-001"),
                Identity::new("parcels", "9999900000000000002", "IC-SYNTHETIC-001"),
            ],
            ..Expectations::default()
        };
        assert_tile(&decoded, &base).unwrap();

        let mut swapped = base.clone();
        swapped.identities = vec![
            Identity::new(
                "parcel_anchor_aggregate",
                "9999900000000000002",
                "IC-SYNTHETIC-001",
            ),
            Identity::new("parcels", "9999900000000000001", "IC-SYNTHETIC-001"),
            Identity::new("parcels", "9999900000000000001", "IC-SYNTHETIC-001"),
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
            "--expect-identity".to_owned(),
            "parcels|9999900000000000001|IC-SYNTHETIC-001".to_owned(),
            "--expect-identity".to_owned(),
            "parcels|9999900000000000002|IC-SYNTHETIC-001".to_owned(),
        ])
        .unwrap();
        assert_eq!(parsed.layers["parcels"], 2);
        assert_eq!(
            parsed.identities,
            vec![
                Identity::new("parcels", "9999900000000000001", "IC-SYNTHETIC-001"),
                Identity::new("parcels", "9999900000000000002", "IC-SYNTHETIC-001"),
            ]
        );
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
}
