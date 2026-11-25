//! The one reader of a file a Silver load reads: a Bronze object a plan fetched, or a run file an
//! image wrote.
//!
//! 2026-11-24 21:35Z the court auction Silver load ran out of `DuckDB`'s 2 GiB on one 물건 검색
//! object (394 MiB of text in 2,435 lines): the stage held the whole object at once, and a
//! statement that parses JSON holds many times the bytes of the rows it reads at once. Memory grew
//! with the object, which no limit bounds.
//!
//! So a load holds a [`FetchedFile`], which nothing but this module opens: its lines are streamed
//! into the stage a part at a time ([`DuckDbParquetStage::read_line_parts`]) or handed one by one
//! to Rust ([`for_each_line`]), and its bytes are read whole only with the lane's declared reason
//! ([`FetchedFile::read_whole`]). A lane says only which lines must share a part and what its
//! statements took in memory per byte of lines, measured ([`LineParts`]); how large a part is
//! comes from that and the stage's memory limit, packed by the one part planner
//! ([`foundation_publisher_core::lakehouse_snapshot_scan::shard_plan::pack_in_order`]).

use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context};
use foundation_publisher_core::lakehouse_snapshot_scan::shard_plan::pack_in_order;

use super::DuckDbParquetStage;

/// A file a Silver load reads. Only this module opens it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchedFile(PathBuf);

impl FetchedFile {
    /// The file at `path`, as the plan fetched it or the image wrote it.
    #[must_use]
    pub const fn at(path: PathBuf) -> Self {
        Self(path)
    }

    /// Whether the file is there (an image that wrote no row writes no file).
    #[must_use]
    pub fn exists(&self) -> bool {
        self.0.exists()
    }

    /// The file's bytes, whole: for a format the lane decodes whole (a CSV in its encoding). `why`
    /// is the lane's declared reason with its largest file measured.
    ///
    /// # Errors
    /// Returns an error when the file cannot be read.
    pub fn read_whole(&self, why: &'static str) -> anyhow::Result<Vec<u8>> {
        ensure!(!why.is_empty(), "a whole read says why");
        std::fs::read(&self.0).with_context(|| format!("cannot read {}", self.0.display()))
    }

    /// The file, opened for a reader that seeks in it (a zip's directory); as [`Self::read_whole`].
    ///
    /// # Errors
    /// Returns an error when the file cannot be opened.
    pub fn open_whole(&self, why: &'static str) -> anyhow::Result<std::fs::File> {
        ensure!(!why.is_empty(), "a whole read says why");
        std::fs::File::open(&self.0).with_context(|| format!("cannot open {}", self.0.display()))
    }
}

/// Which lines of a file a lane's statements compare, so must be read in one part: the lane's one
/// declaration of how it reads.
#[derive(Clone, Copy, Debug)]
pub enum LineParts<'a> {
    /// All of them, the file in one part: why, with the largest file measured.
    Whole(&'static str),
    /// None: parts are runs of lines in file order. A caller that compares lines merges the rows
    /// its parts kept (the latest line winning, as within a part).
    InOrder {
        /// What the lane's statements took per byte of lines, measured.
        memory_per_byte: u64,
    },
    /// The lines with the same values at these JSON pointers. A line that is not JSON, or has none
    /// of them, shares a part with every other such line.
    Grouped {
        /// The pointers.
        pointers: &'a [String],
        /// What the lane's statements took per byte of lines, measured.
        memory_per_byte: u64,
    },
}

impl DuckDbParquetStage {
    /// Reads the JSON Lines `file` into `table(line_number BIGINT, line VARCHAR)` one part at a time
    /// and calls `each` after each part is loaded. The lines are the non-blank ones, numbered from 1
    /// through the whole file, as Python's text reader split them (a `\r\n` ends a line like `\n`);
    /// a part holds its lines with their numbers in the file, and at most the stage's memory limit
    /// divided by the lane's memory per byte (a group of lines larger than that is a part alone).
    ///
    /// After every part, each table named in `kept` (built by `each`) is appended to the rows the
    /// earlier parts kept; after the last part, each holds the rows of all of them, in part order.
    /// The file is streamed, never held whole: one pass to group its lines (none for
    /// [`LineParts::Whole`]), then one per part. Returns the number of lines.
    ///
    /// # Errors
    /// Returns an error when the file is neither gzip nor plain UTF-8 text, a name is not a plain
    /// identifier, a declaration is empty, `each` fails, or `DuckDB` refuses a row.
    pub fn read_line_parts(
        &self,
        file: &FetchedFile,
        table: &str,
        parts: LineParts<'_>,
        kept: &[String],
        mut each: impl FnMut() -> anyhow::Result<()>,
    ) -> anyhow::Result<u64> {
        for name in std::iter::once(table).chain(kept.iter().map(String::as_str)) {
            ensure!(
                !name.is_empty()
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                "{name:?} is not a plain table name"
            );
        }
        let part_of_line = match parts {
            LineParts::Whole(why) => {
                ensure!(!why.is_empty(), "a whole read says why");
                None
            }
            LineParts::InOrder { memory_per_byte }
            | LineParts::Grouped {
                memory_per_byte, ..
            } => {
                ensure!(memory_per_byte > 0, "a lane's memory per byte is measured");
                let grouping = Grouping::of(file, parts)?;
                let bound = self.memory_limit_bytes()? / memory_per_byte;
                let part_of_group = pack_in_order(&grouping.group_bytes, bound);
                Some(
                    grouping
                        .group_of_line
                        .iter()
                        .map(|&group| part_of_group[group])
                        .collect::<Vec<_>>(),
                )
            }
        };
        let count = part_of_line
            .as_ref()
            .and_then(|parts| parts.iter().max())
            .map_or(1, |last| last + 1);
        let mut lines = 0;
        for part in 0..count {
            self.connection.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {table} (line_number BIGINT NOT NULL, line VARCHAR NOT NULL)"
            ))?;
            {
                let mut appender = self.connection.appender(table)?;
                for_each_line(file, |number, line| {
                    lines = number;
                    if part_of_line
                        .as_ref()
                        .is_none_or(|assigned| assigned[number - 1] == part)
                    {
                        appender.append_row(duckdb::params![i64::try_from(number)?, line])?;
                    }
                    Ok(())
                })?;
                appender.flush()?;
            }
            each()?;
            if count == 1 {
                break;
            }
            for name in kept {
                let sql = if part == 0 {
                    format!("CREATE OR REPLACE TEMP TABLE {name}__parts AS SELECT * FROM {name}")
                } else {
                    format!("INSERT INTO {name}__parts BY NAME SELECT * FROM {name}")
                };
                self.connection
                    .execute_batch(&format!("{sql}; DROP TABLE {name}"))
                    .with_context(|| format!("DuckDB failed to keep part {part} of {name}"))?;
            }
        }
        if count > 1 {
            for name in kept {
                self.connection
                    .execute_batch(&format!("ALTER TABLE {name}__parts RENAME TO {name}"))?;
            }
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

/// Each line's group, and each group's bytes, groups numbered in first-seen order.
struct Grouping {
    group_of_line: Vec<usize>,
    group_bytes: Vec<u64>,
}

impl Grouping {
    fn of(file: &FetchedFile, parts: LineParts<'_>) -> anyhow::Result<Self> {
        let mut index = HashMap::<Option<String>, usize>::new();
        let mut grouping = Self {
            group_of_line: Vec::new(),
            group_bytes: Vec::new(),
        };
        for_each_line(file, |_, line| {
            let group = match parts {
                LineParts::Whole(_) => 0,
                LineParts::InOrder { .. } => grouping.group_bytes.len(),
                LineParts::Grouped { pointers, .. } => {
                    let next = index.len();
                    *index.entry(group_key(line, pointers)).or_insert(next)
                }
            };
            if group == grouping.group_bytes.len() {
                grouping.group_bytes.push(0);
            }
            grouping.group_bytes[group] += u64::try_from(line.len())?;
            grouping.group_of_line.push(group);
            Ok(())
        })?;
        Ok(grouping)
    }
}

/// The values at `pointers` of a JSON line, spaces dropped from strings (a coarser key keeps more
/// lines together, never fewer); `None` for a line that is not JSON or has none of them.
fn group_key(line: &str, pointers: &[String]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    let found = pointers
        .iter()
        .map(|pointer| match value.pointer(pointer) {
            Some(serde_json::Value::String(text)) => {
                Some(text.chars().filter(|c| !c.is_whitespace()).collect())
            }
            Some(other) => Some(other.to_string()),
            None => None,
        })
        .collect::<Vec<Option<String>>>();
    if found.iter().all(Option::is_none) {
        return None;
    }
    Some(
        found
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect::<Vec<_>>()
            .join("\u{1f}"),
    )
}

/// Calls `line` with each non-blank line of the gzip or plain text `file` and its number from 1,
/// one line held at a time.
///
/// # Errors
/// Returns an error when the file cannot be read or is not UTF-8 text, or `line` fails.
pub fn for_each_line(
    file: &FetchedFile,
    mut line: impl FnMut(usize, &str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let path: &Path = &file.0;
    let mut opened =
        std::fs::File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let mut magic = [0_u8; 2];
    // gzip by its magic bytes, plain text otherwise (a run file an image wrote).
    let gzip = opened.read(&mut magic)? == 2 && magic == [0x1f, 0x8b];
    let opened =
        std::fs::File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let mut reader: Box<dyn BufRead> = if gzip {
        Box::new(BufReader::new(flate2::read::GzDecoder::new(opened)))
    } else {
        Box::new(BufReader::new(opened))
    };
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
    use foundation_publisher_core::lakehouse_snapshot_scan::shard_plan::pack_in_order;

    use super::{human_bytes, FetchedFile, Grouping, LineParts};

    #[test]
    fn duckdb_s_human_limit_is_read_as_bytes() {
        assert_eq!(human_bytes("2.0 GiB"), Some(2 << 30));
        assert_eq!(human_bytes("512.0 MiB"), Some(512 << 20));
        assert_eq!(human_bytes("1.5 GB"), Some(1_500_000_000));
        assert_eq!(human_bytes("900 bytes"), Some(900));
        assert_eq!(human_bytes("lots"), None);
    }

    #[test]
    fn lines_of_one_key_share_a_group_and_groups_pack_under_the_bound() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("object.jsonl");
        let line = |case: &str| {
            format!("{{\"request\": {{\"r\": {{\"csNo\": \"{case}\"}}}}, \"pad\": \"xxxxxxxx\"}}")
        };
        let lines = [
            line("A"),
            line("B"),
            line(" A"),
            "not json".to_owned(),
            line("C"),
            String::new(),
        ];
        std::fs::write(&path, lines.join("\n"))?;
        let file = FetchedFile::at(path);
        let pointers = ["/request/r/csNo".to_owned()];
        let grouped = Grouping::of(
            &file,
            LineParts::Grouped {
                pointers: &pointers,
                memory_per_byte: 1,
            },
        )?;
        // A and " A" are one case; the line that is not JSON is a group of its own; the blank line
        // is not a line.
        assert_eq!(grouped.group_of_line, vec![0, 1, 0, 2, 3]);
        assert_eq!(pack_in_order(&grouped.group_bytes, 1), vec![0, 1, 2, 3]);
        let in_order = Grouping::of(&file, LineParts::InOrder { memory_per_byte: 1 })?;
        assert_eq!(in_order.group_of_line, vec![0, 1, 2, 3, 4]);
        Ok(())
    }
}
