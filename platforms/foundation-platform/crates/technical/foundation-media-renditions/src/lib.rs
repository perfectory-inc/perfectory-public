//! A photo's rendition ladder, made from its stored original (root ADR-0426, ADR-0297 §2·§3).
//!
//! [`render`] decodes the source bytes (JPEG, PNG, GIF, WebP), turns the pixels upright by the
//! EXIF orientation, and for each rung of the [`Spec`] scales the longest side down to the rung
//! (never up) and encodes every format of the spec. The output is built from decoded pixels only,
//! so no EXIF, GPS or other metadata of the source reaches a rendition. The codecs are the `image`
//! crate's (decoding, AVIF through `ravif`/rav1e) and libwebp's (WebP, through the `webp` crate); this
//! crate writes no codec of its own.
//!
//! The spec is data, not code: its single source is `config/r2-connections.contract.json`
//! `parcel_media.renditions`, read by the caller. Where the renditions are stored and how they are
//! named is the caller's too.

use std::io::Cursor;

use image::codecs::avif::AvifEncoder;
use image::imageops::FilterType;
use image::{DynamicImage, ExtendedColorType, ImageDecoder, ImageEncoder, ImageReader, Limits};
use serde::Deserialize;

/// A rendition ladder: which longest sides, in which formats (root ADR-0426 §2).
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Spec {
    /// Names the ladder in rendition keys; a change to anything below takes a new version.
    pub version: String,
    /// The longest side, in pixels, of each rung (a source smaller than a rung is kept at its size).
    pub widths: Vec<u32>,
    /// Every rung is encoded in each of these, in this order.
    pub formats: Vec<FormatSpec>,
    /// A source of more pixels than this is refused before it is decoded (a decompression bomb).
    pub max_source_pixels: u64,
}

/// One output format of the ladder and its encoder settings.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum FormatSpec {
    /// AV1 still image through rav1e; `speed` 1 (slowest) to 10.
    Avif {
        /// 1 to 100.
        quality: u8,
        /// 1 (best) to 10 (fastest).
        speed: u8,
    },
    /// Lossy WebP through libwebp.
    Webp {
        /// 0 to 100.
        quality: f32,
    },
}

impl FormatSpec {
    /// The file extension a rendition of this format is stored under.
    #[must_use]
    pub const fn extension(&self) -> &'static str {
        match self {
            Self::Avif { .. } => "avif",
            Self::Webp { .. } => "webp",
        }
    }

    /// The `Content-Type` a rendition of this format is served with.
    #[must_use]
    pub const fn content_type(&self) -> &'static str {
        match self {
            Self::Avif { .. } => "image/avif",
            Self::Webp { .. } => "image/webp",
        }
    }
}

/// One encoded rendition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rendition {
    /// The spec rung it was made for (its key), which may be larger than its pixels.
    pub rung: u32,
    /// Its pixel size after the scale-down.
    pub width: u32,
    /// Its pixel size after the scale-down.
    pub height: u32,
    /// Its extension (`avif`, `webp`).
    pub extension: &'static str,
    /// Its `Content-Type`.
    pub content_type: &'static str,
    /// The encoded file.
    pub bytes: Vec<u8>,
}

/// A source's ladder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rendered {
    /// The upright source size.
    pub source_width: u32,
    /// The upright source size.
    pub source_height: u32,
    /// Every rung × format of the spec, rung by rung.
    pub renditions: Vec<Rendition>,
}

/// Why a source gave no ladder.
#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    /// The bytes are not an image this crate decodes, or are broken.
    #[error("the source does not decode: {0}")]
    Decode(#[from] image::ImageError),
    /// The source is larger than `Spec::max_source_pixels`.
    #[error("the source is {width}x{height}, more pixels than the spec allows")]
    TooLarge {
        /// Its declared width.
        width: u32,
        /// Its declared height.
        height: u32,
    },
    /// An encoder refused the pixels.
    #[error("the {0} encoder failed: {1}")]
    Encode(&'static str, String),
}

/// The pixel size an image file declares in its header, without decoding it; `None` when the bytes
/// are not an image this crate reads. The provider-rule monitor reads it (root ADR-0426 §4).
#[must_use]
pub fn dimensions(source: &[u8]) -> Option<(u32, u32)> {
    ImageReader::new(Cursor::new(source))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()
}

/// Renders the ladder of `spec` from `source`.
///
/// # Errors
/// [`RenderError`] when the source does not decode, is over the pixel cap, or an encoder fails.
pub fn render(source: &[u8], spec: &Spec) -> Result<Rendered, RenderError> {
    let image = decode_upright(source, spec.max_source_pixels)?;
    let (source_width, source_height) = (image.width(), image.height());
    let mut renditions: Vec<Rendition> = Vec::new();
    for &rung in &spec.widths {
        let (width, height) = fit(source_width, source_height, rung);
        // A rung the source is too small for comes out the same size as the one below it: reuse
        // those files rather than encode the same pixels again.
        let same = renditions
            .iter()
            .filter(|done| done.width == width && done.height == height)
            .cloned()
            .collect::<Vec<_>>();
        if same.len() == spec.formats.len() {
            renditions.extend(same.into_iter().map(|done| Rendition { rung, ..done }));
            continue;
        }
        let scaled = if (width, height) == (source_width, source_height) {
            image.clone()
        } else {
            image.resize_exact(width, height, FilterType::Lanczos3)
        };
        for format in &spec.formats {
            renditions.push(Rendition {
                rung,
                width,
                height,
                extension: format.extension(),
                content_type: format.content_type(),
                bytes: encode(&scaled, *format)?,
            });
        }
    }
    Ok(Rendered {
        source_width,
        source_height,
        renditions,
    })
}

fn decode_upright(source: &[u8], max_pixels: u64) -> Result<DynamicImage, RenderError> {
    let mut reader = ImageReader::new(Cursor::new(source))
        .with_guessed_format()
        .map_err(image::ImageError::IoError)?;
    let mut limits = Limits::default();
    // The decoder's own allocation cap: the pixel cap at 4 bytes a pixel, plus headroom.
    limits.max_alloc = Some(max_pixels.saturating_mul(4).saturating_add(64 << 20));
    reader.limits(limits);
    let mut decoder = reader.into_decoder()?;
    let (width, height) = decoder.dimensions();
    if u64::from(width) * u64::from(height) > max_pixels {
        return Err(RenderError::TooLarge { width, height });
    }
    let orientation = decoder.orientation()?;
    let mut image = DynamicImage::from_decoder(decoder)?;
    image.apply_orientation(orientation);
    Ok(image)
}

/// The size whose longest side is `min(rung, longest)`, the other side in proportion (at least 1).
fn fit(width: u32, height: u32, rung: u32) -> (u32, u32) {
    let longest = width.max(height);
    if longest <= rung || longest == 0 {
        return (width, height);
    }
    let scale = |side: u32| {
        let scaled =
            (u64::from(side) * u64::from(rung) + u64::from(longest) / 2) / u64::from(longest);
        u32::try_from(scaled).unwrap_or(rung).max(1)
    };
    (scale(width), scale(height))
}

fn encode(image: &DynamicImage, format: FormatSpec) -> Result<Vec<u8>, RenderError> {
    let alpha = image.color().has_alpha();
    let mut out = Vec::new();
    match format {
        FormatSpec::Avif { quality, speed } => {
            let encoder = AvifEncoder::new_with_speed_quality(&mut out, speed, quality);
            let result = if alpha {
                let rgba = image.to_rgba8();
                encoder.write_image(
                    rgba.as_raw(),
                    rgba.width(),
                    rgba.height(),
                    ExtendedColorType::Rgba8,
                )
            } else {
                let rgb = image.to_rgb8();
                encoder.write_image(
                    rgb.as_raw(),
                    rgb.width(),
                    rgb.height(),
                    ExtendedColorType::Rgb8,
                )
            };
            result.map_err(|error| RenderError::Encode("avif", error.to_string()))?;
        }
        FormatSpec::Webp { quality } => {
            let encoded = if alpha {
                let rgba = image.to_rgba8();
                webp::Encoder::from_rgba(rgba.as_raw(), rgba.width(), rgba.height())
                    .encode_simple(false, quality)
            } else {
                let rgb = image.to_rgb8();
                webp::Encoder::from_rgb(rgb.as_raw(), rgb.width(), rgb.height())
                    .encode_simple(false, quality)
            };
            out = encoded
                .map_err(|error| RenderError::Encode("webp", format!("{error:?}")))?
                .to_vec();
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
