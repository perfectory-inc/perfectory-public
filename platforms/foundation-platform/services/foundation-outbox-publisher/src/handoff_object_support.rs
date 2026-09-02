//! What every handoff-object loader does the same way, held once.
//!
//! Two commands stream gzip JSONL objects out of R2 into a Postgres stage: the parcel projection
//! load and the building-unit projection load. The decompression rule and the retry pacing are
//! not properties of either dataset — they are properties of the transport — and a copy per
//! loader is how the two Spark boundary jobs came to hold thirty same-named functions with five
//! identical (2026-09-02). This module is where the loaders agree.

use std::io::Read;

use anyhow::Context;
use flate2::read::GzDecoder;

/// Decompresses one gzip handoff object into text, refusing plain bytes.
///
/// The handoff is gzip: the national parcel conversion took 46.82 GiB to 9.03 GiB, and a reader
/// that assumed plain text would parse the compressed bytes as zero rows and report an empty
/// object as success.
pub(crate) fn gunzip_text(object_bytes: &[u8], object_key: &str) -> anyhow::Result<String> {
    let mut text = String::new();
    GzDecoder::new(object_bytes)
        .read_to_string(&mut text)
        .with_context(|| format!("failed to decompress handoff object {object_key}"))?;
    Ok(text)
}

/// Waits longer after each failed whole-object attempt.
///
/// Immediate retries lose to the failure they are retrying: five range reads inside one second
/// all meet the same second. Measured 2026-09-01 — object 222 of 255 died with all its low-level
/// retries spent inside one bad minute, and read cleanly on the next run.
pub(crate) fn object_retry_delay(base_seconds: u64, attempt: usize) -> std::time::Duration {
    std::time::Duration::from_secs(base_seconds * attempt as u64)
}

/// Escapes one value for `COPY ... FROM STDIN WITH (FORMAT text)`.
///
/// The parcel loader never needed this — a PNU is nineteen digits — but a building or unit name
/// is free text from a national register, and an unescaped tab or newline in one name would
/// shift every column after it by one for that row, silently. Backslash first, so the escapes
/// this function writes are not themselves re-escaped.
pub(crate) fn copy_text_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn gzipped(body: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(body.as_bytes()).expect("fixture write");
        encoder.finish().expect("fixture finish")
    }

    #[test]
    fn compressed_bytes_come_back_as_text() {
        let text = gunzip_text(&gzipped("a\nb\n"), "k").expect("text");

        assert_eq!(text, "a\nb\n");
    }

    #[test]
    fn plain_text_where_gzip_was_promised_is_an_error() {
        let error = gunzip_text(b"not gzip\n", "k").expect_err("plain bytes must be refused");

        assert!(format!("{error:#}").contains("decompress"));
    }

    #[test]
    fn the_retry_waits_longer_each_time() {
        let first = object_retry_delay(5, 1);
        let second = object_retry_delay(5, 2);

        assert!(second > first);
        assert!(
            first.as_secs() > 0,
            "a retry that waits no time is not a retry"
        );
    }

    #[test]
    fn a_tab_in_a_name_does_not_become_a_column_boundary() {
        assert_eq!(copy_text_escape("A\t동"), "A\\t동");
        assert_eq!(copy_text_escape("줄\n바꿈"), "줄\\n바꿈");
        assert_eq!(copy_text_escape("역\\슬래시"), "역\\\\슬래시");
        assert_eq!(copy_text_escape("멀쩡한 이름 101동"), "멀쩡한 이름 101동");
    }

    #[test]
    fn escaping_is_not_applied_twice() {
        // Backslash is replaced first; the sequences the later replacements write must survive.
        assert_eq!(copy_text_escape("\\t"), "\\\\t");
    }
}
