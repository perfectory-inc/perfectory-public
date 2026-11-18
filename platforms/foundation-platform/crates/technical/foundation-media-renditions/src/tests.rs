use super::*;
use image::codecs::jpeg::JpegEncoder;

fn spec(widths: &[u32]) -> Spec {
    Spec {
        version: "test".to_owned(),
        widths: widths.to_vec(),
        formats: vec![
            FormatSpec::Avif {
                quality: 50,
                speed: 10,
            },
            FormatSpec::Webp { quality: 75.0 },
        ],
        max_source_pixels: 10_000,
    }
}

fn jpeg(width: u32, height: u32) -> Vec<u8> {
    let image = image::RgbImage::from_fn(width, height, |x, y| {
        image::Rgb([
            u8::try_from(x % 256).unwrap_or(0),
            u8::try_from(y % 256).unwrap_or(0),
            90,
        ])
    });
    let mut out = Vec::new();
    JpegEncoder::new_with_quality(&mut out, 90)
        .write_image(image.as_raw(), width, height, ExtendedColorType::Rgb8)
        .unwrap_or_default();
    out
}

/// The JPEG with an EXIF APP1 segment right after SOI: orientation 6 (rotate 90° clockwise) and
/// a maker string, so a rendition that kept metadata would carry it.
fn with_exif_orientation_6(jpeg: &[u8]) -> Vec<u8> {
    let mut tiff = b"MM\0\x2a\0\0\0\x08".to_vec();
    tiff.extend_from_slice(&[0, 1]); // one IFD entry
    tiff.extend_from_slice(&[0x01, 0x12, 0, 3, 0, 0, 0, 1, 0, 6, 0, 0]); // Orientation SHORT 6
    tiff.extend_from_slice(&[0, 0, 0, 0]); // no next IFD
    tiff.extend_from_slice(b"SYNTHETIC-GPS-MAKER");
    let mut payload = b"Exif\0\0".to_vec();
    payload.extend_from_slice(&tiff);
    let length = u16::try_from(payload.len() + 2).unwrap_or(u16::MAX);
    let mut out = jpeg[..2].to_vec();
    out.extend_from_slice(&[0xFF, 0xE1]);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&payload);
    out.extend_from_slice(&jpeg[2..]);
    out
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[test]
fn turns_the_source_upright_and_keeps_no_metadata() -> Result<(), RenderError> {
    let source = with_exif_orientation_6(&jpeg(40, 20));
    assert!(contains(&source, b"SYNTHETIC-GPS-MAKER"));
    let rendered = render(&source, &spec(&[16, 32, 64]))?;
    assert_eq!((rendered.source_width, rendered.source_height), (20, 40));
    for rendition in &rendered.renditions {
        assert!(
            rendition.height >= rendition.width,
            "{rendition:?} is not upright"
        );
        assert!(
            !contains(&rendition.bytes, b"Exif"),
            "{} kept EXIF",
            rendition.format.extension()
        );
        assert!(!contains(&rendition.bytes, b"SYNTHETIC-GPS-MAKER"));
        // This build decodes WebP but not AVIF (no dav1d): the WebP rendition's own header is
        // read back; the AVIF one is checked by its `ftyp` brand.
        if rendition.format == MediaFormat::Webp {
            assert_eq!(
                dimensions(&rendition.bytes),
                Some((rendition.width, rendition.height))
            );
        } else {
            assert_eq!(&rendition.bytes[4..12], b"ftypavif");
        }
    }
    Ok(())
}

#[test]
fn scales_down_to_each_rung_and_never_up() -> Result<(), RenderError> {
    let rendered = render(&jpeg(40, 20), &spec(&[16, 32, 64]))?;
    let sizes = rendered
        .renditions
        .iter()
        .map(|rendition| {
            (
                rendition.rung,
                rendition.format.extension(),
                rendition.width,
                rendition.height,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sizes,
        vec![
            (16, "avif", 16, 8),
            (16, "webp", 16, 8),
            (32, "avif", 32, 16),
            (32, "webp", 32, 16),
            (64, "avif", 40, 20),
            (64, "webp", 40, 20),
        ]
    );
    assert_eq!(&rendered.renditions[4].bytes[4..8], b"ftyp");
    assert_eq!(&rendered.renditions[5].bytes[..4], b"RIFF");
    Ok(())
}

#[test]
fn a_rung_the_source_is_too_small_for_reuses_the_files_below_it() -> Result<(), RenderError> {
    let rendered = render(&jpeg(30, 30), &spec(&[64, 128]))?;
    assert_eq!(rendered.renditions.len(), 4);
    for (small, large) in rendered.renditions[..2]
        .iter()
        .zip(&rendered.renditions[2..])
    {
        assert_eq!((large.rung, large.width), (128, 30));
        assert_eq!(small.bytes, large.bytes);
    }
    Ok(())
}

#[test]
fn a_transparent_png_keeps_its_alpha() -> Result<(), RenderError> {
    let image = image::RgbaImage::from_pixel(8, 8, image::Rgba([0, 0, 0, 0]));
    let mut png = Vec::new();
    image::codecs::png::PngEncoder::new(&mut png).write_image(
        image.as_raw(),
        8,
        8,
        ExtendedColorType::Rgba8,
    )?;
    let rendered = render(&png, &spec(&[8]))?;
    let webp = image::load_from_memory(&rendered.renditions[1].bytes)?;
    assert!(webp.color().has_alpha());
    Ok(())
}

#[test]
fn refuses_a_source_over_the_pixel_cap_and_bytes_that_are_not_an_image() {
    assert!(matches!(
        render(&jpeg(200, 100), &spec(&[16])),
        Err(RenderError::TooLarge {
            width: 200,
            height: 100
        })
    ));
    assert!(matches!(
        render(b"<html>gone</html>", &spec(&[16])),
        Err(RenderError::Decode(_))
    ));
    assert_eq!(dimensions(b"<html>gone</html>"), None);
}

fn encoded(format: image::ImageFormat, width: u32, height: u32) -> Vec<u8> {
    let image = image::RgbImage::from_fn(width, height, |x, y| {
        image::Rgb([
            u8::try_from(x % 256).unwrap_or(0),
            u8::try_from(y % 256).unwrap_or(0),
            40,
        ])
    });
    let mut out = Vec::new();
    DynamicImage::ImageRgb8(image)
        .write_to(&mut Cursor::new(&mut out), format)
        .unwrap_or_default();
    out
}

#[test]
fn a_bitmap_and_a_tiff_give_a_ladder_and_a_lossless_png_master() -> Result<(), RenderError> {
    for format in [image::ImageFormat::Bmp, image::ImageFormat::Tiff] {
        let source = encoded(format, 90, 60);
        assert_eq!(dimensions(&source), Some((90, 60)), "{format:?}");
        let rendered = render(&source, &spec(&[40]))?;
        assert_eq!((rendered.source_width, rendered.source_height), (90, 60));
        assert_eq!(rendered.renditions.len(), 2);
        let master = display_master(&source, 10_000)?;
        assert_eq!(MediaFormat::sniff(&master), Some(MediaFormat::Png));
        // Lossless: the master's pixels are the source's.
        let pixels = |bytes: &[u8]| image::load_from_memory(bytes).map(|image| image.to_rgb8());
        assert_eq!(pixels(&master)?, pixels(&source)?, "{format:?}");
        // The same pixels give the same bytes, so a re-seen source is a dedup.
        assert_eq!(display_master(&source, 10_000)?, master);
    }
    Ok(())
}

#[test]
fn a_master_over_the_pixel_cap_is_refused() {
    let source = encoded(image::ImageFormat::Bmp, 200, 200);
    assert!(matches!(
        display_master(&source, 10_000),
        Err(RenderError::TooLarge { .. })
    ));
}
