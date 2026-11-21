//! Polygons read from standard WKB, and whether two of them are one shape within a tolerance.
//!
//! One reader for the publishers: the tile gate measures a served polygon with it (root ADR-0262),
//! and the Silver release compare asks [`within_tolerance`] of a geometry a provider re-projected
//! by a few centimetres (the lane contract's `compare.geometry_tolerance`, root ADR-0599 §7).

use anyhow::{bail, ensure, Context as _};

/// Polygons, each a list of rings (exterior first), each a list of (longitude, latitude).
pub type Polygons = Vec<Vec<Vec<(f64, f64)>>>;

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> anyhow::Result<[u8; N]> {
        let end = self.at.checked_add(N).context("WKB offset overflow")?;
        let slice = self.bytes.get(self.at..end).context("WKB ends early")?;
        self.at = end;
        let mut out = [0_u8; N];
        out.copy_from_slice(slice);
        Ok(out)
    }

    fn u32(&mut self, little: bool) -> anyhow::Result<u32> {
        let raw = self.take::<4>()?;
        Ok(if little {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        })
    }

    fn f64(&mut self, little: bool) -> anyhow::Result<f64> {
        let raw = self.take::<8>()?;
        Ok(if little {
            f64::from_le_bytes(raw)
        } else {
            f64::from_be_bytes(raw)
        })
    }

    fn header(&mut self) -> anyhow::Result<(bool, u32)> {
        let [order] = self.take::<1>()?;
        ensure!(order <= 1, "WKB byte order {order} is neither 0 nor 1");
        let little = order == 1;
        Ok((little, self.u32(little)?))
    }

    fn polygon(&mut self, little: bool) -> anyhow::Result<Vec<Vec<(f64, f64)>>> {
        let rings = self.u32(little)?;
        let mut out = Vec::new();
        for _ in 0..rings {
            let points = self.u32(little)?;
            ensure!(
                usize::try_from(points)? <= self.bytes.len() / 16,
                "WKB ring claims more points than bytes"
            );
            let mut ring = Vec::with_capacity(usize::try_from(points)?);
            for _ in 0..points {
                ring.push((self.f64(little)?, self.f64(little)?));
            }
            out.push(ring);
        }
        Ok(out)
    }
}

/// Reads a 2D Polygon or MultiPolygon.
///
/// # Errors
/// The bytes are not one whole 2D Polygon or MultiPolygon in standard WKB.
pub fn polygons(wkb: &[u8]) -> anyhow::Result<Polygons> {
    let mut reader = Reader { bytes: wkb, at: 0 };
    let (little, kind) = reader.header()?;
    let polygons = match kind {
        3 => vec![reader.polygon(little)?],
        6 => {
            let count = reader.u32(little)?;
            let mut out = Vec::new();
            for _ in 0..count {
                let (part_little, part_kind) = reader.header()?;
                ensure!(
                    part_kind == 3,
                    "a MultiPolygon part is WKB type {part_kind}"
                );
                out.push(reader.polygon(part_little)?);
            }
            out
        }
        other => bail!("WKB type {other} is not a 2D Polygon or MultiPolygon"),
    };
    ensure!(reader.at == wkb.len(), "WKB has trailing bytes");
    Ok(polygons)
}

/// Metres per degree of latitude, and of longitude at the equator.
const METRES_PER_DEGREE: f64 = 111_320.0;

/// The WKB geometry type of `wkb` (3 Polygon, 6 MultiPolygon).
fn geometry_type(wkb: &[u8]) -> anyhow::Result<u32> {
    Ok(Reader { bytes: wkb, at: 0 }.header()?.1)
}

/// Whether `new` is `old` with every vertex moved by at most `metres`.
///
/// The rule is JTS `Geometry.equalsExact(other, tolerance)` on longitude/latitude polygons: the
/// same type and the same parts, rings and vertices in the same order, each vertex within the
/// tolerance of its partner, measured by local scaling (latitude x 111,320 m, longitude x
/// 111,320 m x cos latitude). False for any other difference (type, part, ring or vertex count)
/// and for a geometry that is not a readable polygon: those stay changes, as an exact compare
/// calls them. Rounding coordinates before a checksum cannot do this: a vertex near a grid line
/// rounds either way (measured 2026-11-05: 1e-6 degree rounding made about 1% of shifted zones
/// equal). It was `geometry_tolerance.within_tolerance` of the Spark compare.
#[must_use]
pub fn within_tolerance(old: &[u8], new: &[u8], metres: f64) -> bool {
    let (Ok(old_type), Ok(new_type), Ok(old_parts), Ok(new_parts)) = (
        geometry_type(old),
        geometry_type(new),
        polygons(old),
        polygons(new),
    ) else {
        return false;
    };
    if old_type != new_type || old_parts.len() != new_parts.len() {
        return false;
    }
    let limit = metres * metres;
    old_parts
        .iter()
        .zip(&new_parts)
        .all(|(old_rings, new_rings)| {
            old_rings.len() == new_rings.len()
                && old_rings.iter().zip(new_rings).all(|(old_ring, new_ring)| {
                    old_ring.len() == new_ring.len()
                        && old_ring.iter().zip(new_ring).all(|((x1, y1), (x2, y2))| {
                            let dy = (y2 - y1) * METRES_PER_DEGREE;
                            let dx = (x2 - x1) * METRES_PER_DEGREE * y1.to_radians().cos();
                            dx.mul_add(dx, dy * dy) <= limit
                        })
                })
        })
}

#[cfg(test)]
mod tests {
    use super::within_tolerance;

    /// One degree of latitude is 111,320 m: `metres` north of a synthetic place.
    const fn north(metres: f64) -> f64 {
        metres / 111_320.0
    }

    fn ring_bytes(ring: &[(f64, f64)]) -> Vec<u8> {
        let mut out = u32::try_from(ring.len())
            .unwrap_or(0)
            .to_le_bytes()
            .to_vec();
        for (x, y) in ring {
            out.extend(x.to_le_bytes());
            out.extend(y.to_le_bytes());
        }
        out
    }

    /// A little-endian WKB Polygon of `rings`.
    fn polygon(rings: &[Vec<(f64, f64)>]) -> Vec<u8> {
        let mut out = vec![1_u8];
        out.extend(3_u32.to_le_bytes());
        out.extend(u32::try_from(rings.len()).unwrap_or(0).to_le_bytes());
        for ring in rings {
            out.extend(ring_bytes(ring));
        }
        out
    }

    fn multipolygon(parts: &[Vec<u8>]) -> Vec<u8> {
        let mut out = vec![1_u8];
        out.extend(6_u32.to_le_bytes());
        out.extend(u32::try_from(parts.len()).unwrap_or(0).to_le_bytes());
        for part in parts {
            out.extend(part);
        }
        out
    }

    /// A square of about 100 m at a synthetic place, moved `metres` north.
    fn square(metres: f64) -> Vec<(f64, f64)> {
        let (x, y, side) = (99.0, 9.0 + north(metres), north(100.0));
        vec![
            (x, y),
            (x + side, y),
            (x + side, y + side),
            (x, y + side),
            (x, y),
        ]
    }

    #[test]
    fn a_shift_within_the_tolerance_is_the_same_polygon_and_a_wider_one_a_change() {
        let old = polygon(&[square(0.0)]);
        assert!(within_tolerance(&old, &polygon(&[square(0.05)]), 0.1));
        assert!(!within_tolerance(&old, &polygon(&[square(0.2)]), 0.1));
        // One vertex alone beyond the tolerance is a change.
        let mut moved = square(0.0);
        moved[2].1 += north(0.5);
        assert!(!within_tolerance(&old, &polygon(&[moved]), 0.1));
    }

    #[test]
    fn another_structure_is_a_change_whatever_the_distance() {
        let old = polygon(&[square(0.0)]);
        // A vertex more.
        let mut longer = square(0.0);
        longer.insert(1, (99.0 + north(50.0), 9.0));
        assert!(!within_tolerance(&old, &polygon(&[longer]), 0.1));
        // A hole more; with the hole on both sides, ring by ring.
        let hole: Vec<(f64, f64)> = square(0.0)
            .iter()
            .map(|(x, y)| (x + north(10.0), y + north(10.0)))
            .collect();
        let holed = polygon(&[square(0.0), hole.clone()]);
        assert!(!within_tolerance(&old, &holed, 0.1));
        assert!(within_tolerance(
            &holed,
            &polygon(&[square(0.05), hole]),
            0.1
        ));
        // A Polygon is not a MultiPolygon of the same part; parts are compared one by one.
        let multi = multipolygon(&[old.clone(), old.clone()]);
        assert!(!within_tolerance(
            &old,
            &multipolygon(std::slice::from_ref(&old)),
            0.1
        ));
        assert!(within_tolerance(
            &multi,
            &multipolygon(&[polygon(&[square(0.05)]), old.clone()]),
            0.1
        ));
        assert!(!within_tolerance(
            &multi,
            &multipolygon(std::slice::from_ref(&old)),
            0.1
        ));
    }

    #[test]
    fn an_unreadable_geometry_is_a_change() {
        let old = polygon(&[square(0.0)]);
        assert!(!within_tolerance(&old, &[], 0.1));
        assert!(!within_tolerance(&old[..old.len() - 3], &old, 0.1));
        // A point is not a polygon.
        let mut point = vec![1_u8];
        point.extend(1_u32.to_le_bytes());
        point.extend(99.0_f64.to_le_bytes());
        point.extend(9.0_f64.to_le_bytes());
        assert!(!within_tolerance(&point, &point, 0.1));
    }
}
