//! Whether a served polygon is too small for the map to draw (root ADR-0255).
//!
//! The maxzoom id gate requires every served id in the archive's maxzoom tiles. tippecanoe drops a
//! polygon its integer grid collapses, and no grid keeps every parcel: the national 202609 bake at
//! detail 14 still lost slivers a few centimetres wide. Such a polygon is allowed to be missing only
//! when it is sub-pixel at the layer's `render_max_zoom`, measured on its own geometry in Web
//! Mercator pixels of that zoom (256 per tile): its area is under one square pixel, or its mean width
//! (twice the area over the perimeter) is under one pixel. Nothing the map could draw meets that.
//!
//! The geometry is the handoff's WKB, longitude/latitude (EPSG:4326). Only Polygon and MultiPolygon
//! are read; anything else is an error, which the gate treats as an unexplained miss.

use anyhow::{bail, ensure, Context as _};

/// Polygons, each a list of rings (exterior first), each a list of (longitude, latitude).
type Polygons = Vec<Vec<Vec<(f64, f64)>>>;

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
pub(crate) fn polygons(wkb: &[u8]) -> anyhow::Result<Polygons> {
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

/// Web Mercator pixel coordinates at `zoom`, 256 pixels per tile.
fn pixel((lon, lat): (f64, f64), zoom: u8) -> (f64, f64) {
    let scale = 256.0 * f64::from(1_u32 << zoom);
    let phi = lat.to_radians();
    (
        (lon + 180.0) / 360.0 * scale,
        (1.0 - (phi.tan() + 1.0 / phi.cos()).ln() / std::f64::consts::PI) / 2.0 * scale,
    )
}

/// (area, perimeter) of `polygons` in pixels at `zoom`; holes subtract from the area.
fn measure(polygons: &Polygons, zoom: u8) -> (f64, f64) {
    let (mut area, mut perimeter) = (0.0, 0.0);
    for polygon in polygons {
        for (index, ring) in polygon.iter().enumerate() {
            // Relative to the ring's first point: absolute pixels near 2^30 would lose a sub-pixel
            // area to cancellation in the cross products.
            let origin = ring.first().map_or((0.0, 0.0), |point| pixel(*point, zoom));
            let points: Vec<(f64, f64)> = ring
                .iter()
                .map(|point| {
                    let (x, y) = pixel(*point, zoom);
                    (x - origin.0, y - origin.1)
                })
                .collect();
            let (mut twice, mut length) = (0.0, 0.0);
            for pair in points.windows(2) {
                let ((x0, y0), (x1, y1)) = (pair[0], pair[1]);
                twice += x0 * y1 - x1 * y0;
                length += (x1 - x0).hypot(y1 - y0);
            }
            let ring_area = (twice / 2.0).abs();
            area += if index == 0 { ring_area } else { -ring_area };
            perimeter += length;
        }
    }
    (area.max(0.0), perimeter)
}

/// ADR-0255: whether the polygon in `wkb` covers less than one pixel at `render_max_zoom`, by area
/// or by mean width.
pub(crate) fn is_subpixel(wkb: &[u8], render_max_zoom: u8) -> anyhow::Result<bool> {
    ensure!(
        render_max_zoom <= 30,
        "render_max_zoom {render_max_zoom} is above 30"
    );
    let (area, perimeter) = measure(&polygons(wkb)?, render_max_zoom);
    Ok(area < 1.0 || perimeter == 0.0 || 2.0 * area / perimeter < 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A little-endian WKB Polygon with one ring.
    fn polygon_wkb(ring: &[(f64, f64)]) -> Vec<u8> {
        let mut out = vec![1_u8];
        out.extend(3_u32.to_le_bytes());
        out.extend(1_u32.to_le_bytes());
        out.extend(u32::try_from(ring.len()).unwrap_or(0).to_le_bytes());
        for (x, y) in ring {
            out.extend(x.to_le_bytes());
            out.extend(y.to_le_bytes());
        }
        out
    }

    /// A rectangle at a synthetic place, `width` x `height` pixels at zoom 22.
    fn rectangle(width: f64, height: f64) -> Vec<u8> {
        // At zoom 22 one pixel is 360 / (256 * 2^22) degrees of longitude; latitude is taken near
        // the equator, where Mercator pixels are almost square.
        let degree = 360.0 / (256.0 * f64::from(1_u32 << 22));
        let (x, y) = (10.0, 0.001);
        polygon_wkb(&[
            (x, y),
            (x + width * degree, y),
            (x + width * degree, y + height * degree),
            (x, y + height * degree),
            (x, y),
        ])
    }

    #[test]
    fn a_polygon_under_one_pixel_by_area_or_by_width_is_subpixel() -> anyhow::Result<()> {
        assert!(is_subpixel(&rectangle(0.5, 0.5), 22)?, "a quarter pixel");
        assert!(
            is_subpixel(&rectangle(3_000.0, 0.4), 22)?,
            "a long sliver 0.4 px wide"
        );
        assert!(
            !is_subpixel(&rectangle(3_000.0, 2.0), 22)?,
            "2 px wide is drawn"
        );
        assert!(!is_subpixel(&rectangle(3.0, 3.0), 22)?);
        // The same sliver is drawn at a higher zoom: the test is the layer's render_max_zoom.
        assert!(!is_subpixel(&rectangle(3_000.0, 0.4), 24)?);
        Ok(())
    }

    #[test]
    fn a_hole_counts_against_the_area() -> anyhow::Result<()> {
        let degree = 360.0 / (256.0 * f64::from(1_u32 << 22));
        let square = |x: f64, y: f64, side: f64| {
            vec![
                (x, y),
                (x + side, y),
                (x + side, y + side),
                (x, y + side),
                (x, y),
            ]
        };
        let mut wkb = vec![1_u8];
        wkb.extend(3_u32.to_le_bytes());
        wkb.extend(2_u32.to_le_bytes());
        for ring in [
            square(10.0, 0.001, 4.0 * degree),
            square(10.0 + 0.2 * degree, 0.001 + 0.2 * degree, 3.6 * degree),
        ] {
            wkb.extend(u32::try_from(ring.len())?.to_le_bytes());
            for (x, y) in ring {
                wkb.extend(x.to_le_bytes());
                wkb.extend(y.to_le_bytes());
            }
        }
        // 4x4 px with a 3.6x3.6 px hole: a frame 0.2 px wide.
        assert!(is_subpixel(&wkb, 22)?);
        Ok(())
    }

    #[test]
    fn a_multipolygon_and_big_endian_are_read_and_other_types_refused() -> anyhow::Result<()> {
        let part = rectangle(10.0, 10.0);
        let mut multi = vec![1_u8];
        multi.extend(6_u32.to_le_bytes());
        multi.extend(2_u32.to_le_bytes());
        multi.extend(&part);
        multi.extend(&part);
        assert_eq!(polygons(&multi)?.len(), 2);

        let mut big = vec![0_u8];
        big.extend(3_u32.to_be_bytes());
        big.extend(1_u32.to_be_bytes());
        big.extend(4_u32.to_be_bytes());
        for (x, y) in [(0.0_f64, 0.0_f64), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)] {
            big.extend(x.to_be_bytes());
            big.extend(y.to_be_bytes());
        }
        assert_eq!(polygons(&big)?[0][0][1], (1.0, 0.0));

        let mut point = vec![1_u8];
        point.extend(1_u32.to_le_bytes());
        point.extend(0.0_f64.to_le_bytes());
        point.extend(0.0_f64.to_le_bytes());
        assert!(is_subpixel(&point, 22).is_err());
        assert!(
            polygons(&part[..part.len() - 3]).is_err(),
            "a truncated ring"
        );
        let mut trailing = part.clone();
        trailing.push(0);
        assert!(polygons(&trailing).is_err());
        Ok(())
    }
}
