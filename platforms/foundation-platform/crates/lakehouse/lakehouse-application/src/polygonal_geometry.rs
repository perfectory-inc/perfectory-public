//! Polygonal geometry primitives shared by the Silver boundary normalizers.
//!
//! Parcel boundaries and industrial-complex boundaries arrive in different container formats
//! (`GeoJSON` and ESRI shapefile) but land in the same place: a ring structure, a bounding box, and
//! standard little-endian WKB. Only the reader that produces [`ParsedPolygonalGeometry`] is
//! source-specific; everything after it is the same arithmetic, so it lives here once.
//!
//! Ring order follows the `GeoJSON` convention inside [`PolygonRings`]: the first ring is the
//! exterior ring and every ring after it is an interior ring (a hole). A reader whose source states
//! ring roles some other way — a shapefile states them by winding direction — is responsible for
//! translating into that convention before handing a value to this module.

use thiserror::Error;

/// One coordinate pair in the geometry's own spatial reference system.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    /// X coordinate (easting or longitude, depending on the geometry's SRID).
    pub x: f64,
    /// Y coordinate (northing or latitude, depending on the geometry's SRID).
    pub y: f64,
}

/// A closed ring of coordinates whose first and last positions are equal.
pub type LinearRing = Vec<GeoPoint>;

/// One polygon: an exterior ring followed by its interior rings.
pub type PolygonRings = Vec<LinearRing>;

/// Polygonal geometry decoded from a provider source, before WKB encoding.
#[derive(Clone, Debug, PartialEq)]
pub enum ParsedPolygonalGeometry {
    /// A single polygon.
    Polygon(PolygonRings),
    /// Several polygons that together form one feature.
    MultiPolygon(Vec<PolygonRings>),
}

/// Bounding box derived from a full polygonal geometry, in the geometry's own SRID.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeometryBoundingBox {
    /// Minimum X coordinate.
    pub min_x: f64,
    /// Minimum Y coordinate.
    pub min_y: f64,
    /// Maximum X coordinate.
    pub max_x: f64,
    /// Maximum Y coordinate.
    pub max_y: f64,
}

/// Error returned while deriving values from polygonal geometry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PolygonalGeometryError {
    /// The geometry held no coordinate at all, so no bounding box exists.
    #[error("geometry must contain at least one coordinate")]
    NoCoordinates,
    /// A ring, polygon, or part count does not fit the `u32` field WKB reserves for it.
    #[error("{label} exceeds WKB u32 length capacity")]
    LengthOverflow {
        /// Which count overflowed, in the words the caller uses for it.
        label: &'static str,
    },
}

/// Accumulates the bounding box of a polygonal geometry point by point.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BoundingBoxAccumulator {
    bbox: Option<GeometryBoundingBox>,
}

impl BoundingBoxAccumulator {
    /// Records every point of one polygon, exterior ring and holes alike.
    pub fn record_polygon(&mut self, polygon: &[LinearRing]) {
        for ring in polygon {
            for point in ring {
                self.record_point(*point);
            }
        }
    }

    /// Records one point.
    pub const fn record_point(&mut self, point: GeoPoint) {
        self.bbox = Some(match self.bbox {
            Some(bbox) => GeometryBoundingBox {
                min_x: bbox.min_x.min(point.x),
                min_y: bbox.min_y.min(point.y),
                max_x: bbox.max_x.max(point.x),
                max_y: bbox.max_y.max(point.y),
            },
            None => GeometryBoundingBox {
                min_x: point.x,
                min_y: point.y,
                max_x: point.x,
                max_y: point.y,
            },
        });
    }

    /// Returns the accumulated bounding box.
    ///
    /// # Errors
    /// Returns [`PolygonalGeometryError::NoCoordinates`] when no point was ever recorded.
    pub fn finish(self) -> Result<GeometryBoundingBox, PolygonalGeometryError> {
        self.bbox.ok_or(PolygonalGeometryError::NoCoordinates)
    }
}

/// Returns the bounding box of a polygonal geometry.
///
/// # Errors
/// Returns [`PolygonalGeometryError::NoCoordinates`] when the geometry holds no coordinate.
pub fn geometry_bounding_box(
    geometry: &ParsedPolygonalGeometry,
) -> Result<GeometryBoundingBox, PolygonalGeometryError> {
    let mut accumulator = BoundingBoxAccumulator::default();
    match geometry {
        ParsedPolygonalGeometry::Polygon(polygon) => accumulator.record_polygon(polygon),
        ParsedPolygonalGeometry::MultiPolygon(polygons) => {
            for polygon in polygons {
                accumulator.record_polygon(polygon);
            }
        }
    }
    accumulator.finish()
}

/// Encodes polygonal geometry as standard little-endian WKB for `GeoParquet` writers.
///
/// # Errors
/// Returns [`PolygonalGeometryError::LengthOverflow`] when a ring, polygon, or part count does not
/// fit the `u32` field WKB reserves for it.
pub fn geometry_to_wkb(
    geometry: &ParsedPolygonalGeometry,
) -> Result<Vec<u8>, PolygonalGeometryError> {
    let mut bytes = Vec::new();
    match geometry {
        ParsedPolygonalGeometry::Polygon(polygon) => write_polygon_wkb(&mut bytes, polygon)?,
        ParsedPolygonalGeometry::MultiPolygon(polygons) => {
            write_u8(&mut bytes, 1);
            write_u32_le(&mut bytes, 6);
            write_len_u32(&mut bytes, polygons.len(), "MultiPolygon polygon count")?;
            for polygon in polygons {
                write_polygon_wkb(&mut bytes, polygon)?;
            }
        }
    }
    Ok(bytes)
}

fn write_polygon_wkb(
    bytes: &mut Vec<u8>,
    polygon: &[LinearRing],
) -> Result<(), PolygonalGeometryError> {
    write_u8(bytes, 1);
    write_u32_le(bytes, 3);
    write_len_u32(bytes, polygon.len(), "Polygon ring count")?;
    for ring in polygon {
        write_len_u32(bytes, ring.len(), "linear ring point count")?;
        for point in ring {
            write_f64_le(bytes, point.x);
            write_f64_le(bytes, point.y);
        }
    }
    Ok(())
}

fn write_len_u32(
    bytes: &mut Vec<u8>,
    len: usize,
    label: &'static str,
) -> Result<(), PolygonalGeometryError> {
    let value = u32::try_from(len).map_err(|_| PolygonalGeometryError::LengthOverflow { label })?;
    write_u32_le(bytes, value);
    Ok(())
}

fn write_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn write_u32_le(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_f64_le(bytes: &mut Vec<u8>, value: f64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}
