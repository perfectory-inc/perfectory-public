//! A polygon edit an administrator saves before the next bake folds it into the tiles (ADR-0112).
//!
//! The edit store checks structure (ring closure, coordinate bounds, vertex count, the unit's id
//! grammar and properties) against its contract. What only geometry code can decide — whether the
//! rings cross themselves, whether a hole lies inside its shell, whether two parts of a
//! multipolygon overlap — is decided here, once, before the edit is sent. A geometry that fails
//! these rules would draw wrongly on every customer's map until the next bake.

use geo::{Coord, LineString, MultiPolygon, Polygon, Validation as _};
use serde_json::Value;

/// What an edit does to the feature it names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MapEditOperation {
    /// Replace the feature's polygon, or add a feature the tiles do not have yet.
    Upsert,
    /// Remove the feature from what customers see.
    Delete,
}

impl MapEditOperation {
    /// The wire name the edit store uses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }

    /// Parses the wire name.
    ///
    /// # Errors
    /// Returns [`MapEditError::UnknownOperation`] for anything but `upsert` or `delete`.
    pub fn parse(value: &str) -> Result<Self, MapEditError> {
        match value {
            "upsert" => Ok(Self::Upsert),
            "delete" => Ok(Self::Delete),
            _ => Err(MapEditError::UnknownOperation),
        }
    }
}

/// A `GeoJSON` `Polygon` or `MultiPolygon` (EPSG:4326) that is topologically valid.
///
/// The original JSON is kept as given so the store receives exactly what was checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MapEditGeometry(Value);

impl MapEditGeometry {
    /// Checks a `GeoJSON` geometry object.
    ///
    /// # Errors
    /// Returns [`MapEditError`] when the value is not a polygonal `GeoJSON` geometry with 2D
    /// numeric positions, or when it breaks OGC validity (self-intersection, a hole outside its
    /// shell, overlapping parts, a ring with no area).
    pub fn parse(value: Value) -> Result<Self, MapEditError> {
        let kind = value.get("type").and_then(Value::as_str);
        let coordinates = value.get("coordinates").ok_or(MapEditError::NotPolygonal)?;
        let multipolygon = match kind {
            Some("Polygon") => MultiPolygon::new(vec![polygon(coordinates)?]),
            Some("MultiPolygon") => MultiPolygon::new(
                array(coordinates)?
                    .iter()
                    .map(polygon)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            _ => return Err(MapEditError::NotPolygonal),
        };
        if multipolygon.0.is_empty() {
            return Err(MapEditError::NotPolygonal);
        }
        multipolygon
            .check_validation()
            .map_err(|error| MapEditError::Invalid(error.to_string()))?;
        Ok(Self(value))
    }

    /// The checked `GeoJSON` geometry object.
    #[must_use]
    pub const fn as_json(&self) -> &Value {
        &self.0
    }
}

/// Why an edit was refused before it reached the store.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MapEditError {
    /// The operation is neither `upsert` nor `delete`.
    #[error("map edit operation must be upsert or delete")]
    UnknownOperation,
    /// The geometry is not a `GeoJSON` `Polygon` or `MultiPolygon`.
    #[error("map edit geometry must be a GeoJSON Polygon or MultiPolygon")]
    NotPolygonal,
    /// A position is not two finite numbers.
    #[error("map edit geometry positions must be two finite numbers")]
    InvalidPosition,
    /// The geometry breaks OGC validity; the text names the first problem found.
    #[error("map edit geometry is not valid: {0}")]
    Invalid(String),
    /// An upsert carries no geometry, or a delete carries one.
    #[error("an upsert needs a geometry and a delete must not carry one")]
    GeometryDoesNotMatchOperation,
}

fn array(value: &Value) -> Result<&Vec<Value>, MapEditError> {
    value.as_array().ok_or(MapEditError::NotPolygonal)
}

fn polygon(value: &Value) -> Result<Polygon<f64>, MapEditError> {
    let mut rings = array(value)?.iter().map(ring);
    let exterior = rings.next().ok_or(MapEditError::NotPolygonal)??;
    Ok(Polygon::new(
        exterior,
        rings.collect::<Result<Vec<_>, _>>()?,
    ))
}

fn ring(value: &Value) -> Result<LineString<f64>, MapEditError> {
    array(value)?
        .iter()
        .map(|position| match position.as_array().map(Vec::as_slice) {
            Some([x, y]) => match (x.as_f64(), y.as_f64()) {
                (Some(x), Some(y)) if x.is_finite() && y.is_finite() => Ok(Coord { x, y }),
                _ => Err(MapEditError::InvalidPosition),
            },
            _ => Err(MapEditError::InvalidPosition),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(LineString::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py.
    // The range is half-open, so every corner stays strictly inside it.
    const LO_X: f64 = 127.1231;
    const HI_X: f64 = 127.1234;
    const NEAR_X: f64 = 127.1236;
    const FAR_X: f64 = 127.1238;
    const LO_Y: f64 = 36.1231;
    const HI_Y: f64 = 36.1234;
    const FAR_Y: f64 = 36.1238;
    const MID_X: f64 = 127.1232;
    const MID_Y: f64 = 36.1232;

    fn assert_refused_for(result: &Result<MapEditGeometry, MapEditError>, reason: &str) {
        assert!(
            matches!(result, Err(MapEditError::Invalid(text)) if text.contains(reason)),
            "expected a refusal for {reason}: {result:?}"
        );
    }

    fn square() -> Value {
        json!([
            [LO_X, LO_Y],
            [HI_X, LO_Y],
            [HI_X, HI_Y],
            [LO_X, HI_Y],
            [LO_X, LO_Y]
        ])
    }

    #[test]
    fn a_simple_polygon_and_multipolygon_are_accepted_as_given() -> Result<(), MapEditError> {
        let polygon = json!({"type": "Polygon", "coordinates": [square()]});
        assert_eq!(MapEditGeometry::parse(polygon.clone())?.as_json(), &polygon);
        MapEditGeometry::parse(json!({"type": "MultiPolygon", "coordinates": [[square()]]}))?;
        Ok(())
    }

    #[test]
    fn a_self_intersecting_ring_is_refused() {
        // A bow tie: the second and fourth edges cross in the middle.
        let bow_tie = json!([
            [LO_X, LO_Y],
            [HI_X, HI_Y],
            [HI_X, LO_Y],
            [LO_X, HI_Y],
            [LO_X, LO_Y]
        ]);
        let result = MapEditGeometry::parse(json!({"type": "Polygon", "coordinates": [bow_tie]}));
        assert_refused_for(&result, "self-intersection");
    }

    #[test]
    fn a_hole_outside_its_shell_is_refused() {
        let outside = json!([
            [NEAR_X, LO_Y],
            [FAR_X, LO_Y],
            [FAR_X, MID_Y],
            [NEAR_X, LO_Y]
        ]);
        let result =
            MapEditGeometry::parse(json!({"type": "Polygon", "coordinates": [square(), outside]}));
        assert_refused_for(&result, "not contained");
    }

    #[test]
    fn overlapping_parts_of_a_multipolygon_are_refused() {
        let shifted = json!([
            [MID_X, MID_Y],
            [FAR_X, MID_Y],
            [FAR_X, FAR_Y],
            [MID_X, FAR_Y],
            [MID_X, MID_Y]
        ]);
        let result = MapEditGeometry::parse(
            json!({"type": "MultiPolygon", "coordinates": [[square()], [shifted]]}),
        );
        assert_refused_for(&result, "overlap");
    }

    #[test]
    fn a_ring_with_no_area_is_refused() {
        let flat = json!([[LO_X, LO_Y], [HI_X, LO_Y], [LO_X, LO_Y], [LO_X, LO_Y]]);
        let result = MapEditGeometry::parse(json!({"type": "Polygon", "coordinates": [flat]}));
        assert_refused_for(&result, "at least 3 distinct points");
    }

    #[test]
    fn non_polygonal_and_malformed_geometry_is_refused() {
        for (value, expected) in [
            (
                json!({"type": "LineString", "coordinates": [[LO_X, LO_Y], [HI_X, HI_Y]]}),
                MapEditError::NotPolygonal,
            ),
            (json!({"type": "Polygon"}), MapEditError::NotPolygonal),
            (
                json!({"type": "Polygon", "coordinates": []}),
                MapEditError::NotPolygonal,
            ),
            (
                json!({"type": "MultiPolygon", "coordinates": []}),
                MapEditError::NotPolygonal,
            ),
            (
                json!({"type": "Polygon", "coordinates": [[[LO_X, LO_Y, 0.0]]]}),
                MapEditError::InvalidPosition,
            ),
            (
                json!({"type": "Polygon", "coordinates": [[["x", LO_Y]]]}),
                MapEditError::InvalidPosition,
            ),
        ] {
            assert_eq!(
                MapEditGeometry::parse(value.clone()),
                Err(expected),
                "{value}"
            );
        }
    }

    #[test]
    fn operations_parse_only_their_wire_names() {
        assert_eq!(
            MapEditOperation::parse("upsert"),
            Ok(MapEditOperation::Upsert)
        );
        assert_eq!(
            MapEditOperation::parse("delete").map(MapEditOperation::as_str),
            Ok("delete")
        );
        assert_eq!(
            MapEditOperation::parse("update"),
            Err(MapEditError::UnknownOperation)
        );
    }
}
