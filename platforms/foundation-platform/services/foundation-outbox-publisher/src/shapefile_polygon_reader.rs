//! Reads polygon geometry out of the `.shp` half of an ESRI shapefile.
//!
//! The format is small enough to state in full. A 100-byte main header carries a file code, the
//! file length in 16-bit words, and the shape type. Then records follow back to back, each with an
//! 8-byte header — record number and content length in 16-bit words, both big-endian — and a
//! content block that for a polygon holds: shape type, bounding box, part count, point count, the
//! index in the point array where each part starts, and the points themselves.
//!
//! **Ring winding is the only thing that says which ring is a hole.** The shapefile specification
//! winds an exterior ring clockwise and an interior ring counter-clockwise, and the file says
//! nothing else about the difference: a record with an exterior ring and a hole looks exactly like
//! a record with two islands until the winding is read. Getting it backwards turns every hole into
//! an island — the area comes out as a sum instead of a difference, and every downstream check
//! still passes.
//!
//! The dBase table beside the file is read separately ([`crate::dbase_table`]); the two are joined
//! by position, record `i` here against record `i` there.

use anyhow::{bail, ensure, Context as _};
use lakehouse_application::{
    ring_winding, GeoPoint, LinearRing, ParsedPolygonalGeometry, PolygonRings, RingWinding,
};

/// Length of the shapefile main header.
const MAIN_HEADER_LEN: usize = 100;

/// File code every shapefile main header starts with.
const SHAPEFILE_FILE_CODE: i32 = 9994;

/// Shape type of a record that carries no geometry.
const SHAPE_TYPE_NULL: i32 = 0;

/// Shape type this reader accepts.
const SHAPE_TYPE_POLYGON: i32 = 5;

/// Length of one record header: record number and content length, both big-endian.
const RECORD_HEADER_LEN: usize = 8;

/// Smallest number of positions a closed ring can have.
const MIN_RING_POSITIONS: usize = 4;

/// One record of a polygon shapefile, in file order.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ShapefilePolygonRecord {
    /// Record number the file declares, counted from one.
    pub(crate) record_number: u32,
    /// Decoded geometry, or `None` when the record declares the null shape.
    pub(crate) geometry: Option<ParsedPolygonalGeometry>,
    /// Rings dropped because their coordinates did not determine a winding.
    pub(crate) collapsed_ring_count: usize,
}

/// Reads every record of a polygon shapefile in file order.
///
/// # Errors
/// Returns an error when the main header is not a shapefile, when the declared shape type is not
/// polygon, when a record is truncated, or when a record's rings cannot be sorted into exterior
/// rings and holes.
pub(crate) fn read_shapefile_polygons(bytes: &[u8]) -> anyhow::Result<Vec<ShapefilePolygonRecord>> {
    ensure!(
        bytes.len() >= MAIN_HEADER_LEN,
        "not a shapefile: {} bytes, shorter than the 100-byte main header",
        bytes.len()
    );
    let file_code = read_i32_be(bytes, 0)?;
    ensure!(
        file_code == SHAPEFILE_FILE_CODE,
        "not a shapefile: file code {file_code}, expected {SHAPEFILE_FILE_CODE}"
    );
    let shape_type = read_i32_le(bytes, 32)?;
    ensure!(
        shape_type == SHAPE_TYPE_POLYGON,
        "shapefile declares shape type {shape_type}; this reader only accepts polygon \
         ({SHAPE_TYPE_POLYGON})"
    );

    let mut records = Vec::new();
    let mut offset = MAIN_HEADER_LEN;
    while offset + RECORD_HEADER_LEN <= bytes.len() {
        let record_number = u32::try_from(read_i32_be(bytes, offset)?)
            .context("shapefile record number is negative")?;
        let content_words = read_i32_be(bytes, offset + 4)?;
        let content_len = usize::try_from(content_words)
            .context("shapefile record content length is negative")?
            * 2;
        let content_start = offset + RECORD_HEADER_LEN;
        let content = bytes
            .get(content_start..content_start + content_len)
            .with_context(|| {
                format!(
                    "shapefile record {record_number} declares {content_len} content bytes but \
                     the file ends first"
                )
            })?;
        let (geometry, collapsed_ring_count) = decode_polygon_record(content)
            .with_context(|| format!("shapefile record {record_number} is unreadable"))?;
        records.push(ShapefilePolygonRecord {
            record_number,
            geometry,
            collapsed_ring_count,
        });
        offset = content_start + content_len;
    }
    Ok(records)
}

fn decode_polygon_record(
    content: &[u8],
) -> anyhow::Result<(Option<ParsedPolygonalGeometry>, usize)> {
    let shape_type = read_i32_le(content, 0)?;
    if shape_type == SHAPE_TYPE_NULL {
        return Ok((None, 0));
    }
    ensure!(
        shape_type == SHAPE_TYPE_POLYGON,
        "record shape type {shape_type} is neither null ({SHAPE_TYPE_NULL}) nor polygon \
         ({SHAPE_TYPE_POLYGON})"
    );

    // 4 shape type + 32 bounding box, then the two counts.
    let part_count = usize::try_from(read_i32_le(content, 36)?)
        .context("shapefile record declares a negative part count")?;
    let point_count = usize::try_from(read_i32_le(content, 40)?)
        .context("shapefile record declares a negative point count")?;
    ensure!(part_count > 0, "a polygon record must declare a part");
    ensure!(point_count > 0, "a polygon record must declare a point");

    let parts_start = 44;
    let points_start = parts_start + part_count * 4;
    let mut part_offsets = Vec::with_capacity(part_count);
    for index in 0..part_count {
        let start = usize::try_from(read_i32_le(content, parts_start + index * 4)?)
            .context("shapefile part index is negative")?;
        ensure!(
            start < point_count,
            "part {index} starts at point {start} but the record declares {point_count} points"
        );
        if let Some(previous) = part_offsets.last() {
            ensure!(
                start > *previous,
                "part offsets must increase; part {index} starts at {start} after {previous}"
            );
        }
        part_offsets.push(start);
    }

    let mut rings = Vec::with_capacity(part_count);
    for (index, start) in part_offsets.iter().copied().enumerate() {
        let end = part_offsets.get(index + 1).copied().unwrap_or(point_count);
        let mut ring: LinearRing = Vec::with_capacity(end - start);
        for point_index in start..end {
            let point_offset = points_start + point_index * 16;
            ring.push(GeoPoint {
                x: read_f64_le(content, point_offset)?,
                y: read_f64_le(content, point_offset + 8)?,
            });
        }
        ensure!(
            ring.len() >= MIN_RING_POSITIONS,
            "ring {index} has {} positions; a closed ring needs at least {MIN_RING_POSITIONS}",
            ring.len()
        );
        ensure!(
            ring.first() == ring.last(),
            "ring {index} is not closed; its first and last positions differ"
        );
        rings.push(ring);
    }

    let (geometry, collapsed_ring_count) = rings_to_geometry(rings)?;
    Ok((Some(geometry), collapsed_ring_count))
}

/// Sorts a record's rings into polygons using the winding direction the specification assigns.
///
/// Clockwise opens a new polygon as its exterior ring; counter-clockwise is a hole in the polygon
/// opened most recently. A ring whose coordinates do not determine a winding is dropped and
/// counted: the provider's own file carries one, three vertices where two sit 4.5 nanometres apart,
/// and calling its rounding-error winding a hole would reject the 50,000 m² ring beside it.
fn rings_to_geometry(rings: Vec<LinearRing>) -> anyhow::Result<(ParsedPolygonalGeometry, usize)> {
    let mut polygons: Vec<PolygonRings> = Vec::new();
    let mut collapsed_ring_count = 0_usize;
    for (index, ring) in rings.into_iter().enumerate() {
        match ring_winding(&ring) {
            Some(RingWinding::Clockwise) => polygons.push(vec![ring]),
            Some(RingWinding::CounterClockwise) => {
                let Some(polygon) = polygons.last_mut() else {
                    bail!(
                        "ring {index} winds counter-clockwise, which makes it a hole, but no \
                         exterior ring has opened a polygon for it to be a hole in"
                    );
                };
                polygon.push(ring);
            }
            None => collapsed_ring_count += 1,
        }
    }

    let mut polygons = polygons.into_iter();
    let first = polygons
        .next()
        .context("the record holds no clockwise ring, so it has no exterior ring at all")?;
    let rest = polygons.collect::<Vec<_>>();
    if rest.is_empty() {
        return Ok((
            ParsedPolygonalGeometry::Polygon(first),
            collapsed_ring_count,
        ));
    }
    let mut all = Vec::with_capacity(rest.len() + 1);
    all.push(first);
    all.extend(rest);
    Ok((
        ParsedPolygonalGeometry::MultiPolygon(all),
        collapsed_ring_count,
    ))
}

fn read_i32_be(bytes: &[u8], offset: usize) -> anyhow::Result<i32> {
    Ok(i32::from_be_bytes(slice4(bytes, offset)?))
}

fn read_i32_le(bytes: &[u8], offset: usize) -> anyhow::Result<i32> {
    Ok(i32::from_le_bytes(slice4(bytes, offset)?))
}

fn read_f64_le(bytes: &[u8], offset: usize) -> anyhow::Result<f64> {
    let slice = bytes
        .get(offset..offset + 8)
        .context("shapefile ends inside a coordinate")?;
    Ok(f64::from_le_bytes(slice.try_into()?))
}

fn slice4(bytes: &[u8], offset: usize) -> anyhow::Result<[u8; 4]> {
    let slice = bytes
        .get(offset..offset + 4)
        .context("shapefile ends inside a 4-byte field")?;
    Ok(slice.try_into()?)
}

#[cfg(test)]
pub(crate) mod test_support {
    use lakehouse_application::GeoPoint;

    /// Builds the bytes of a polygon shapefile holding one record per ring group.
    ///
    /// Each element of `records` is that record's rings, in file order, wound however the caller
    /// wants — which is the point: a test needs to be able to hand this reader a hole wound the
    /// wrong way.
    pub(crate) fn shapefile_bytes(records: &[Vec<Vec<GeoPoint>>]) -> Vec<u8> {
        let mut bytes = vec![0_u8; 100];
        bytes[0..4].copy_from_slice(&9994_i32.to_be_bytes());
        bytes[28..32].copy_from_slice(&1000_i32.to_le_bytes());
        bytes[32..36].copy_from_slice(&5_i32.to_le_bytes());

        for (index, rings) in records.iter().enumerate() {
            let mut content = Vec::new();
            content.extend_from_slice(&5_i32.to_le_bytes());
            content.extend_from_slice(&[0_u8; 32]);
            let point_count: usize = rings.iter().map(Vec::len).sum();
            content.extend_from_slice(&le(rings.len()));
            content.extend_from_slice(&le(point_count));
            let mut start = 0_usize;
            for ring in rings {
                content.extend_from_slice(&le(start));
                start += ring.len();
            }
            for ring in rings {
                for point in ring {
                    content.extend_from_slice(&point.x.to_le_bytes());
                    content.extend_from_slice(&point.y.to_le_bytes());
                }
            }
            bytes.extend_from_slice(&be(index + 1));
            bytes.extend_from_slice(&be(content.len() / 2));
            bytes.extend_from_slice(&content);
        }

        let words = bytes.len() / 2;
        bytes[24..28].copy_from_slice(&be(words));
        bytes
    }

    fn le(value: usize) -> [u8; 4] {
        i32::try_from(value).unwrap_or(i32::MAX).to_le_bytes()
    }

    fn be(value: usize) -> [u8; 4] {
        i32::try_from(value).unwrap_or(i32::MAX).to_be_bytes()
    }

    /// A closed square wound clockwise, which is how a shapefile writes an exterior ring.
    pub(crate) fn clockwise_square(x0: f64, y0: f64, side: f64) -> Vec<GeoPoint> {
        [
            (x0, y0),
            (x0, y0 + side),
            (x0 + side, y0 + side),
            (x0 + side, y0),
            (x0, y0),
        ]
        .into_iter()
        .map(|(x, y)| GeoPoint { x, y })
        .collect()
    }

    /// The same square wound the other way, which is how a shapefile writes a hole.
    pub(crate) fn counter_clockwise_square(x0: f64, y0: f64, side: f64) -> Vec<GeoPoint> {
        let mut ring = clockwise_square(x0, y0, side);
        ring.reverse();
        ring
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;
    use lakehouse_application::{geometry_area, GeoPoint, ParsedPolygonalGeometry};

    use super::{
        read_shapefile_polygons,
        test_support::{clockwise_square, counter_clockwise_square, shapefile_bytes},
    };

    #[test]
    fn a_single_clockwise_ring_is_one_polygon() -> anyhow::Result<()> {
        let bytes = shapefile_bytes(&[vec![clockwise_square(0.0, 0.0, 10.0)]]);

        let records = read_shapefile_polygons(&bytes)?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_number, 1);
        let geometry = records[0]
            .geometry
            .as_ref()
            .context("the record carries geometry")?;
        assert!(matches!(geometry, ParsedPolygonalGeometry::Polygon(_)));
        assert!((geometry_area(geometry) - 100.0).abs() < 1e-9);
        Ok(())
    }

    /// The load-bearing case. Clockwise outer, counter-clockwise inner: one polygon with a hole,
    /// so the area is the difference.
    #[test]
    fn a_counter_clockwise_ring_becomes_a_hole_in_the_ring_before_it() -> anyhow::Result<()> {
        let bytes = shapefile_bytes(&[vec![
            clockwise_square(0.0, 0.0, 10.0),
            counter_clockwise_square(4.0, 4.0, 2.0),
        ]]);

        let records = read_shapefile_polygons(&bytes)?;

        let geometry = records[0]
            .geometry
            .as_ref()
            .context("the record carries geometry")?;
        assert!(
            matches!(geometry, ParsedPolygonalGeometry::Polygon(rings) if rings.len() == 2),
            "a hole must stay inside one polygon as its second ring, not open a second \
             polygon: {geometry:?}"
        );
        assert!((geometry_area(geometry) - 96.0).abs() < 1e-9, "100 - 4");
        Ok(())
    }

    /// The same two squares, at the same coordinates, with only the inner ring's winding flipped.
    /// That single flip changes the feature from "a 10×10 square with a 2×2 hole" (96) into "a
    /// 10×10 island and a 2×2 island on top of it" (104). Nothing but the winding says which one
    /// the source meant, which is why reading it backwards is undetectable downstream: both answers
    /// are valid polygons, both pass every bbox and checksum gate, and only the area differs.
    #[test]
    fn winding_alone_decides_between_a_hole_and_an_island() -> anyhow::Result<()> {
        let as_hole = shapefile_bytes(&[vec![
            clockwise_square(0.0, 0.0, 10.0),
            counter_clockwise_square(4.0, 4.0, 2.0),
        ]]);
        let as_island = shapefile_bytes(&[vec![
            clockwise_square(0.0, 0.0, 10.0),
            clockwise_square(4.0, 4.0, 2.0),
        ]]);

        let hole = read_shapefile_polygons(&as_hole)?;
        let island = read_shapefile_polygons(&as_island)?;

        let hole = hole[0].geometry.as_ref().context("geometry")?;
        let island = island[0].geometry.as_ref().context("geometry")?;
        assert!((geometry_area(hole) - 96.0).abs() < 1e-9, "100 - 4");
        assert!((geometry_area(island) - 104.0).abs() < 1e-9, "100 + 4");
        Ok(())
    }

    #[test]
    fn two_clockwise_rings_are_two_polygons() -> anyhow::Result<()> {
        let bytes = shapefile_bytes(&[vec![
            clockwise_square(0.0, 0.0, 2.0),
            clockwise_square(10.0, 0.0, 2.0),
        ]]);

        let records = read_shapefile_polygons(&bytes)?;

        let geometry = records[0]
            .geometry
            .as_ref()
            .context("the record carries geometry")?;
        assert!(
            matches!(geometry, ParsedPolygonalGeometry::MultiPolygon(parts) if parts.len() == 2),
            "{geometry:?}"
        );
        assert!((geometry_area(geometry) - 8.0).abs() < 1e-9);
        Ok(())
    }

    /// A hole with nothing to be a hole in is a malformed record, not a polygon. Silently promoting
    /// it to an exterior ring would invent an island the source never drew.
    #[test]
    fn a_leading_counter_clockwise_ring_is_rejected() {
        let bytes = shapefile_bytes(&[vec![counter_clockwise_square(0.0, 0.0, 2.0)]]);

        let error = read_shapefile_polygons(&bytes)
            .map(|_| ())
            .expect_err("a record whose first ring is a hole must be rejected");

        assert!(
            format!("{error:#}").contains("no exterior ring"),
            "{error:#}"
        );
    }

    /// The provider's own file carries this: a first "ring" of three vertices, two of them 4.5
    /// nanometres apart, in front of the real 50,000 m² ring. Its shoelace sum is 1.5e-5 at
    /// coordinates around 2.5e5, which is rounding noise rather than a counter-clockwise winding.
    /// Reading it as a hole rejected the whole record, and with it the other 1,362.
    #[test]
    fn a_ring_collapsed_to_rounding_error_is_dropped_and_counted() -> anyhow::Result<()> {
        let collapsed = [
            (248_592.392_248_501_16, 513_262.824_836_841_4),
            (248_592.392_248_505_67, 513_262.824_836_841_4),
            (248_592.346_986_101_24, 513_262.405_135_691_6),
            (248_592.392_248_501_16, 513_262.824_836_841_4),
        ]
        .into_iter()
        .map(|(x, y)| GeoPoint { x, y })
        .collect::<Vec<_>>();
        let bytes = shapefile_bytes(&[vec![
            collapsed,
            clockwise_square(248_474.0, 513_262.0, 200.0),
        ]]);

        let records = read_shapefile_polygons(&bytes)?;

        assert_eq!(records[0].collapsed_ring_count, 1);
        let geometry = records[0]
            .geometry
            .as_ref()
            .context("the record still carries its real ring")?;
        assert!(matches!(geometry, ParsedPolygonalGeometry::Polygon(rings) if rings.len() == 1));
        assert!((geometry_area(geometry) - 40_000.0).abs() < 1e-6);
        Ok(())
    }

    #[test]
    fn record_order_is_file_order() -> anyhow::Result<()> {
        let bytes = shapefile_bytes(&[
            vec![clockwise_square(0.0, 0.0, 1.0)],
            vec![clockwise_square(0.0, 0.0, 3.0)],
        ]);

        let records = read_shapefile_polygons(&bytes)?;

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record_number, 1);
        assert_eq!(records[1].record_number, 2);
        Ok(())
    }

    #[test]
    fn a_file_that_is_not_a_shapefile_is_rejected() {
        let error = read_shapefile_polygons(&[0_u8; 120])
            .map(|_| ())
            .expect_err("a file without the shapefile file code must be rejected");

        assert!(format!("{error:#}").contains("file code"), "{error:#}");
    }

    #[test]
    fn a_truncated_record_is_rejected() {
        let mut bytes = shapefile_bytes(&[vec![clockwise_square(0.0, 0.0, 1.0)]]);
        bytes.truncate(bytes.len() - 8);

        let error = read_shapefile_polygons(&bytes)
            .map(|_| ())
            .expect_err("a record cut short must be rejected");

        assert!(
            format!("{error:#}").contains("the file ends first"),
            "{error:#}"
        );
    }
}
