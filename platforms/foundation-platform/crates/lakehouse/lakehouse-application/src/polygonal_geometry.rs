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

/// Returns the area of a polygonal geometry in the squared unit of its own SRID.
///
/// Holes are subtracted. The result is meaningful only when that unit is a length: a geometry in a
/// geographic CRS returns square degrees, which is not an area anyone wants. The caller knows its
/// SRID and is responsible for that judgement.
///
/// Ring winding does not matter here — each ring contributes the absolute value of its shoelace
/// area and its role comes from its position, exterior first — so a reader that has already sorted
/// exterior rings from holes cannot make the total come out negative by winding them either way.
#[must_use]
pub fn geometry_area(geometry: &ParsedPolygonalGeometry) -> f64 {
    match geometry {
        ParsedPolygonalGeometry::Polygon(polygon) => polygon_area(polygon),
        ParsedPolygonalGeometry::MultiPolygon(polygons) => {
            polygons.iter().map(|polygon| polygon_area(polygon)).sum()
        }
    }
}

/// Returns the area-weighted centroid of a polygonal geometry, in its own SRID.
///
/// Returns `None` when the geometry encloses no area, which is the only case where the centroid is
/// undefined. A degenerate ring — every point collinear, or a ring whose holes cancel it out — is
/// the shape that produces it.
#[must_use]
pub fn geometry_centroid(geometry: &ParsedPolygonalGeometry) -> Option<GeoPoint> {
    let polygons: &[PolygonRings] = match geometry {
        ParsedPolygonalGeometry::Polygon(polygon) => std::slice::from_ref(polygon),
        ParsedPolygonalGeometry::MultiPolygon(polygons) => polygons,
    };
    let mut area_total = 0.0_f64;
    let mut moment_x = 0.0_f64;
    let mut moment_y = 0.0_f64;
    for polygon in polygons {
        let Some((centroid, area)) = polygon_centroid_and_area(polygon) else {
            continue;
        };
        area_total += area;
        moment_x = centroid.x.mul_add(area, moment_x);
        moment_y = centroid.y.mul_add(area, moment_y);
    }
    if area_total <= 0.0 {
        return None;
    }
    Some(GeoPoint {
        x: moment_x / area_total,
        y: moment_y / area_total,
    })
}

fn polygon_area(polygon: &[LinearRing]) -> f64 {
    let mut rings = polygon.iter();
    let Some(exterior) = rings.next() else {
        return 0.0;
    };
    let holes: f64 = rings.map(|ring| ring_signed_area(ring).abs()).sum();
    ring_signed_area(exterior).abs() - holes
}

/// Returns the polygon's centroid and its area, or `None` when it encloses no area.
fn polygon_centroid_and_area(polygon: &[LinearRing]) -> Option<(GeoPoint, f64)> {
    let mut rings = polygon.iter();
    let exterior = rings.next()?;
    let (exterior_centroid, exterior_area) = ring_centroid_and_area(exterior)?;
    let mut area = exterior_area;
    let mut moment_x = exterior_centroid.x * exterior_area;
    let mut moment_y = exterior_centroid.y * exterior_area;
    for hole in rings {
        let Some((hole_centroid, hole_area)) = ring_centroid_and_area(hole) else {
            continue;
        };
        area -= hole_area;
        moment_x = hole_centroid.x.mul_add(-hole_area, moment_x);
        moment_y = hole_centroid.y.mul_add(-hole_area, moment_y);
    }
    if area <= 0.0 {
        return None;
    }
    Some((
        GeoPoint {
            x: moment_x / area,
            y: moment_y / area,
        },
        area,
    ))
}

/// Returns one ring's centroid and its unsigned area, or `None` for a ring that encloses none.
///
/// The centroid itself does not depend on winding: reversing a ring flips the sign of both the
/// moment sums and the signed area, and they divide out.
fn ring_centroid_and_area(ring: &[GeoPoint]) -> Option<(GeoPoint, f64)> {
    let mut signed_area = 0.0_f64;
    let mut moment_x = 0.0_f64;
    let mut moment_y = 0.0_f64;
    for pair in ring.windows(2) {
        let (current, next) = (pair[0], pair[1]);
        let cross = current.x.mul_add(next.y, -(next.x * current.y));
        signed_area += cross;
        moment_x = (current.x + next.x).mul_add(cross, moment_x);
        moment_y = (current.y + next.y).mul_add(cross, moment_y);
    }
    if signed_area == 0.0 {
        return None;
    }
    Some((
        GeoPoint {
            x: moment_x / (3.0 * signed_area),
            y: moment_y / (3.0 * signed_area),
        },
        (signed_area / 2.0).abs(),
    ))
}

/// Which way a ring is wound, for the sources that state a ring's role that way.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RingWinding {
    /// Vertices run clockwise; the shoelace sum is negative.
    Clockwise,
    /// Vertices run counter-clockwise; the shoelace sum is positive.
    CounterClockwise,
}

/// Returns which way a ring winds, or `None` when its own coordinates do not determine that.
///
/// A shapefile states ring roles by winding, so this sign decides whether a ring is an exterior
/// ring or a hole — and a ring can be too small for its coordinates to answer. In a projected CRS
/// the coordinates run to six figures, so each shoelace term is around 1e11 and carries roughly
/// 3e-5 of rounding error; a ring whose segments have collapsed onto each other produces a sum
/// smaller than that, and its sign is then rounding noise rather than a fact about the ring. The
/// comparison is against the error the ring's own terms carry, not a fixed tolerance, so a ring
/// that really does enclose a square metre still answers.
#[must_use]
pub fn ring_winding(ring: &[GeoPoint]) -> Option<RingWinding> {
    let mut sum = 0.0_f64;
    let mut magnitude = 0.0_f64;
    for pair in ring.windows(2) {
        let (current, next) = (pair[0], pair[1]);
        sum += current.x.mul_add(next.y, -(next.x * current.y));
        magnitude += (current.x * next.y).abs() + (next.x * current.y).abs();
    }
    if sum.abs() <= 3.0 * f64::EPSILON * magnitude {
        return None;
    }
    if sum < 0.0 {
        return Some(RingWinding::Clockwise);
    }
    Some(RingWinding::CounterClockwise)
}

/// Returns twice the shoelace area of a ring: positive counter-clockwise, negative clockwise.
fn ring_signed_double_area(ring: &[GeoPoint]) -> f64 {
    ring.windows(2)
        .map(|pair| pair[0].x.mul_add(pair[1].y, -(pair[1].x * pair[0].y)))
        .sum()
}

fn ring_signed_area(ring: &[GeoPoint]) -> f64 {
    ring_signed_double_area(ring) / 2.0
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

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::{
        geometry_area, geometry_bounding_box, geometry_centroid, ring_winding, GeoPoint,
        ParsedPolygonalGeometry, RingWinding,
    };

    fn ring(points: &[(f64, f64)]) -> Vec<GeoPoint> {
        points
            .iter()
            .map(|(x, y)| GeoPoint { x: *x, y: *y })
            .collect()
    }

    /// Closed counter-clockwise square of the given side, anchored at `(x0, y0)`.
    fn square(x0: f64, y0: f64, side: f64) -> Vec<GeoPoint> {
        ring(&[
            (x0, y0),
            (x0 + side, y0),
            (x0 + side, y0 + side),
            (x0, y0 + side),
            (x0, y0),
        ])
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn a_square_has_its_side_squared_as_area() {
        let geometry = ParsedPolygonalGeometry::Polygon(vec![square(0.0, 0.0, 10.0)]);

        assert_close(geometry_area(&geometry), 100.0);
    }

    /// The hole is what makes this worth testing: a reader that mistook it for a second exterior
    /// ring would report 100 + 4 instead of 100 - 4, and nothing downstream could tell.
    #[test]
    fn a_hole_is_subtracted_not_added() {
        let geometry =
            ParsedPolygonalGeometry::Polygon(vec![square(0.0, 0.0, 10.0), square(4.0, 4.0, 2.0)]);

        assert_close(geometry_area(&geometry), 96.0);
    }

    /// Shapefiles wind exterior rings clockwise, `GeoJSON` counter-clockwise. Area must not depend
    /// on which convention the source used, only on which ring the reader called exterior.
    #[test]
    fn area_does_not_depend_on_winding() {
        let mut reversed = square(0.0, 0.0, 10.0);
        reversed.reverse();

        let clockwise = ParsedPolygonalGeometry::Polygon(vec![reversed]);
        let counter_clockwise = ParsedPolygonalGeometry::Polygon(vec![square(0.0, 0.0, 10.0)]);

        assert_close(geometry_area(&clockwise), geometry_area(&counter_clockwise));
    }

    #[test]
    fn a_square_centroid_is_its_middle() -> anyhow::Result<()> {
        let geometry = ParsedPolygonalGeometry::Polygon(vec![square(0.0, 0.0, 10.0)]);

        let centroid = geometry_centroid(&geometry).context("a square encloses area")?;

        assert_close(centroid.x, 5.0);
        assert_close(centroid.y, 5.0);
        Ok(())
    }

    /// An off-centre hole pulls the centroid away from it — the check that the hole enters the
    /// moment sum with a negative weight rather than being ignored.
    #[test]
    fn an_off_centre_hole_moves_the_centroid_away_from_itself() -> anyhow::Result<()> {
        let geometry =
            ParsedPolygonalGeometry::Polygon(vec![square(0.0, 0.0, 10.0), square(1.0, 1.0, 2.0)]);

        let centroid = geometry_centroid(&geometry).context("the ring still encloses area")?;

        assert!(centroid.x > 5.0, "{}", centroid.x);
        assert!(centroid.y > 5.0, "{}", centroid.y);
        Ok(())
    }

    /// Two equal parts put the centroid halfway between them, weighted by area.
    #[test]
    fn a_multipolygon_centroid_is_area_weighted() -> anyhow::Result<()> {
        let geometry = ParsedPolygonalGeometry::MultiPolygon(vec![
            vec![square(0.0, 0.0, 2.0)],
            vec![square(10.0, 0.0, 2.0)],
        ]);

        let centroid = geometry_centroid(&geometry).context("both parts enclose area")?;

        assert_close(geometry_area(&geometry), 8.0);
        assert_close(centroid.x, 6.0);
        assert_close(centroid.y, 1.0);
        Ok(())
    }

    /// The contract gate says the centroid sits inside the bbox. For an area-weighted centroid of
    /// disjoint parts that is a consequence, not a coincidence — it is a convex combination.
    #[test]
    fn a_multipolygon_centroid_stays_inside_the_bounding_box() -> anyhow::Result<()> {
        let geometry = ParsedPolygonalGeometry::MultiPolygon(vec![
            vec![square(0.0, 0.0, 1.0)],
            vec![square(100.0, 50.0, 3.0)],
        ]);

        let centroid = geometry_centroid(&geometry).context("both parts enclose area")?;
        let bbox = geometry_bounding_box(&geometry)?;

        assert!(
            centroid.x >= bbox.min_x && centroid.x <= bbox.max_x,
            "{centroid:?}"
        );
        assert!(
            centroid.y >= bbox.min_y && centroid.y <= bbox.max_y,
            "{centroid:?}"
        );
        Ok(())
    }

    #[test]
    fn a_collinear_ring_has_no_centroid() {
        let geometry = ParsedPolygonalGeometry::Polygon(vec![ring(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (2.0, 0.0),
            (0.0, 0.0),
        ])]);

        assert!(geometry_centroid(&geometry).is_none());
    }

    /// The sign is the whole point: it is what a shapefile reader uses to tell an exterior ring
    /// from a hole.
    #[test]
    fn the_shoelace_sign_says_which_way_a_ring_winds() {
        let counter_clockwise = square(0.0, 0.0, 1.0);
        let mut clockwise = counter_clockwise.clone();
        clockwise.reverse();

        assert_eq!(
            ring_winding(&counter_clockwise),
            Some(RingWinding::CounterClockwise)
        );
        assert_eq!(ring_winding(&clockwise), Some(RingWinding::Clockwise));
    }

    /// The provider's own boundary file carries one of these: three vertices where two are 4.5e-9
    /// metres apart, at coordinates around 2.5e5. Its shoelace sum is 1.5e-5, which is smaller than
    /// the rounding error its own terms carry, so its sign says nothing and calling it a hole would
    /// reject the 50,000 m² ring beside it.
    #[test]
    fn a_ring_collapsed_to_rounding_error_has_no_winding() {
        let collapsed = ring(&[
            (248_592.392_248_501_16, 513_262.824_836_841_4),
            (248_592.392_248_505_67, 513_262.824_836_841_4),
            (248_592.346_986_101_24, 513_262.405_135_691_6),
            (248_592.392_248_501_16, 513_262.824_836_841_4),
        ]);

        assert_eq!(ring_winding(&collapsed), None);
    }

    /// One square metre at the same six-figure coordinates still answers, so the bound rejects
    /// noise rather than small rings.
    #[test]
    fn a_one_square_metre_ring_at_projected_coordinates_still_winds() {
        let tiny = square(248_592.0, 513_262.0, 1.0);

        assert_eq!(ring_winding(&tiny), Some(RingWinding::CounterClockwise));
    }
}
