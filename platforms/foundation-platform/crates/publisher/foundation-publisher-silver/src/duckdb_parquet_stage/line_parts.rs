//! A JSON Lines file a Silver load reads, streamed into the stage whole, and the measures a lane
//! sizes its reads by (the stage's memory limit, a file's text).
//!
//! The court auction lane reads its objects with `DuckDB`'s own `read_json` (root ADR-0616 §3-1)
//! and uses only the measures; the other lanes load their lines here ([`DuckDbParquetStage::load_lines`]).

use std::{
    io::{BufRead, BufReader, Read},
    path::Path,
};

use anyhow::{ensure, Context};

use super::DuckDbParquetStage;

impl DuckDbParquetStage {
    /// A gzip JSON Lines object as `table(line_number BIGINT, line VARCHAR)`, whole: the non-blank
    /// lines, numbered from 1, as Python's text reader split them (a `\r\n` ends a line like `\n`).
    /// The file is streamed a line at a time, never held whole as text.
    ///
    /// # Errors
    /// Returns an error when the file is neither gzip nor plain UTF-8 text, `table` is not a plain
    /// identifier, or `DuckDB` refuses a row.
    pub fn load_lines(&self, path: &Path, table: &str) -> anyhow::Result<u64> {
        ensure!(
            !table.is_empty()
                && table
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "{table:?} is not a plain table name"
        );
        self.connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {table} (line_number BIGINT NOT NULL, line VARCHAR NOT NULL)"
        ))?;
        let mut lines = 0;
        {
            let mut appender = self.connection.appender(table)?;
            for_each_line(path, |number, line| {
                lines = number;
                appender.append_row(duckdb::params![i64::try_from(number)?, line])?;
                Ok(())
            })?;
            appender.flush()?;
        }
        Ok(u64::try_from(lines)?)
    }

    /// The memory `DuckDB` may hold before it spills, as it reports it (`'2.0 GiB'`).
    ///
    /// # Errors
    /// Returns an error when `DuckDB` reports it in a form this does not read.
    pub fn memory_limit_bytes(&self) -> anyhow::Result<u64> {
        let text: String =
            self.connection
                .query_row("SELECT current_setting('memory_limit')", [], |row| {
                    row.get(0)
                })?;
        human_bytes(&text).with_context(|| format!("DuckDB reports its memory limit as {text:?}"))
    }
}

/// The bytes of text in the gzip or plain file at `path`, uncompressed (counted as it streams).
///
/// # Errors
/// Returns an error when the file cannot be read or its gzip is broken.
pub fn text_bytes(path: &Path) -> anyhow::Result<u64> {
    let (mut reader, _) = open_text(path)?;
    std::io::copy(&mut reader, &mut std::io::sink())
        .with_context(|| format!("cannot read the text of {}", path.display()))
}

/// `DuckDB`'s human form of a byte count (`StringUtil::BytesToHumanReadableString`): a number and
/// a unit of 1000 or 1024.
fn human_bytes(text: &str) -> Option<u64> {
    let (number, unit) = text.trim().split_once(' ')?;
    let number: f64 = number.parse().ok()?;
    let scale: u64 = match unit {
        "bytes" | "byte" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return None,
    };
    // A memory limit is far below 2^53 bytes, where an f64 holds every integer.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let bytes = (number * scale as f64) as u64;
    (number >= 0.0).then_some(bytes)
}

/// The text of a gzip (by its magic bytes) or plain file, and whether it is gzip.
fn open_text(path: &Path) -> anyhow::Result<(Box<dyn BufRead>, bool)> {
    let mut opened =
        std::fs::File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let mut magic = [0_u8; 2];
    // gzip by its magic bytes, plain text otherwise (a run file an image wrote).
    let gzip = opened.read(&mut magic)? == 2 && magic == [0x1f, 0x8b];
    let opened =
        std::fs::File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let reader: Box<dyn BufRead> = if gzip {
        Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(opened)))
    } else {
        Box::new(BufReader::new(opened))
    };
    Ok((reader, gzip))
}

/// Calls `line` with each non-blank line of the gzip or plain text file and its number from 1.
fn for_each_line(
    path: &Path,
    mut line: impl FnMut(usize, &str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let (mut reader, gzip) = open_text(path)?;
    let what = if gzip { "gzip UTF-8" } else { "UTF-8" };
    let (mut text, mut number) = (String::new(), 0);
    loop {
        text.clear();
        if reader
            .read_line(&mut text)
            .with_context(|| format!("{} is not {what} text", path.display()))?
            == 0
        {
            return Ok(());
        }
        let read = text.strip_suffix('\n').unwrap_or(&text);
        let read = read.strip_suffix('\r').unwrap_or(read);
        if read.trim().is_empty() {
            continue;
        }
        number += 1;
        line(number, read)?;
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::{human_bytes, text_bytes};

    #[test]
    fn duckdb_s_human_limit_is_read_as_bytes() {
        assert_eq!(human_bytes("2.0 GiB"), Some(2 << 30));
        assert_eq!(human_bytes("512.0 MiB"), Some(512 << 20));
        assert_eq!(human_bytes("1.5 GB"), Some(1_500_000_000));
        assert_eq!(human_bytes("900 bytes"), Some(900));
        assert_eq!(human_bytes("lots"), None);
    }

    #[test]
    fn a_file_s_text_is_counted_uncompressed() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let plain = directory.path().join("lines.jsonl");
        std::fs::write(&plain, "{\"a\": 1}\n\n{\"a\": 2}\n")?;
        assert_eq!(text_bytes(&plain)?, 19);
        let packed = directory.path().join("lines.jsonl.gz");
        let mut gzip = flate2::write::GzEncoder::new(
            std::fs::File::create(&packed)?,
            flate2::Compression::default(),
        );
        gzip.write_all(&[b'x'; 10_000])?;
        gzip.finish()?;
        assert_eq!(text_bytes(&packed)?, 10_000);
        Ok(())
    }
}
