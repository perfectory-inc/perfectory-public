//! How a building-register Silver row is named.
//!
//! The three register tables used to build their row id out of `source_record_id`, which at
//! the time carried `<object>#line-000001`. That made one column do two jobs: name where a row
//! came from, and make the row unique. It worked, and it cost the lineage column its meaning —
//! every other table in this repository uses `source_record_id` for the source object alone, so
//! the batch a load carries could be read off five tables and not off these three.
//!
//! Splitting them costs nothing, because the line number was already stored beside it in
//! `source_line_number`. The id is assembled here rather than at each of the three call sites:
//! three spellings of one identity rule is how two of them end up different.
//!
//! The floor and unit-area tables no longer use the location id for new batches: they name a
//! row by its natural key ([`natural_row_identity`], root ADR-0331). Older batches keep their
//! location ids; nothing is rewritten.

/// Names one Silver row: its kind, the object it came from, and the line within that object.
///
/// A row with no line number falls back to the object alone. That is not a silent collision
/// risk being waved through — the register readers set the line for every row they decode, and
/// a source that cannot say which line a row came from cannot distinguish two identical rows
/// by any other means either.
#[must_use]
pub fn row_identity(kind: &str, source_record_id: &str, source_line_number: Option<u64>) -> String {
    source_line_number.map_or_else(
        || format!("{kind}:{source_record_id}"),
        |line| format!("{kind}:{source_record_id}#line-{line:06}"),
    )
}

/// One row's place in the natural-key ranking (root ADR-0331).
///
/// 층별개요 and 전유공용면적 rows carry no row key: the only provider key is the register's
/// `mgm_bldrgst_pk`, and one floor or one unit's floor holds several rows. A row is named by its identifying columns and its occurrence among the rows that share them,
/// never by where it sat in a file: a new monthly file moves every line, and a location id made
/// every row of every release look changed (21,189,570 floor rows against 29,754 that changed,
/// ADR-0326). The occurrence is ranked by a fingerprint of the row's other columns, then by input
/// order, so a file that lists the same rows in another order names them the same.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NaturalKeyRank {
    group: [u8; 8],
    content: [u8; 8],
    index: u32,
}

impl NaturalKeyRank {
    /// The rank entry of the `index`-th input row.
    #[must_use]
    pub fn new(identity: &[&str], content: &[&str], index: u32) -> Self {
        Self {
            group: prefix(&digest(identity)),
            content: prefix(&digest(content)),
            index,
        }
    }
}

/// The 1-based occurrence of each row among the rows of its identity group, by input index.
///
/// Takes the entries by value and sorts them in place: the 전유공용면적 export ranks 114 million
/// rows, 20 bytes each, and a second index vector would double that.
#[must_use]
pub fn natural_key_occurrences(mut ranks: Vec<NaturalKeyRank>) -> Vec<u32> {
    ranks.sort_unstable();
    let mut occurrences = vec![0_u32; ranks.len()];
    let mut previous: Option<[u8; 8]> = None;
    let mut occurrence = 0_u32;
    for rank in &ranks {
        occurrence = if previous == Some(rank.group) {
            occurrence + 1
        } else {
            1
        };
        previous = Some(rank.group);
        if let Some(slot) = occurrences.get_mut(rank.index as usize) {
            *slot = occurrence;
        }
    }
    occurrences
}

/// Names one row by its identifying columns and occurrence: `<kind>:nk1:<32 hex>`.
///
/// The hex is the first 128 bits of SHA-256 over the identity and the occurrence joined by the
/// unit separator (U+001F), so a reader can recompute it in SQL (ADR-0331 §crosswalk). `nk1`
/// tells these ids from the location ids (`<kind>:bronze/…#line-N`) older batches keep.
#[must_use]
pub fn natural_row_identity(kind: &str, identity: &[&str], occurrence: u32) -> String {
    use std::fmt::Write as _;
    let occurrence = occurrence.to_string();
    let mut parts = identity.to_vec();
    parts.push(&occurrence);
    let mut id = format!("{kind}:nk1:");
    for byte in &digest(&parts)[..16] {
        let _ = write!(id, "{byte:02x}");
    }
    id
}

fn digest(parts: &[&str]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(parts.join("\u{1f}").as_bytes()).into()
}

fn prefix(digest: &[u8; 32]) -> [u8; 8] {
    let mut out = [0_u8; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The id is the SQL crosswalk's: Python `hashlib.sha256("PK\x1f20\x1f1\x1f1".encode())`.
    #[test]
    fn a_natural_id_is_the_first_128_bits_of_the_key_digest() {
        assert_eq!(
            natural_row_identity("building-register-floor", &["99999-1", "20", "1"], 1),
            "building-register-floor:nk1:16b1784f0b652ef9aeedeea4c2fd3b96"
        );
    }

    /// Two rows of one floor are told apart by occurrence, and a file that lists them in the
    /// other order names each the same: the rank follows their content, not their line.
    #[test]
    fn occurrences_follow_content_not_file_order() {
        let first = [
            NaturalKeyRank::new(&["pk", "20", "1"], &["지상", "1층"], 0),
            NaturalKeyRank::new(&["pk", "20", "1"], &["지상", "1층 일부"], 1),
            NaturalKeyRank::new(&["pk", "20", "2"], &["지상", "2층"], 2),
        ];
        let reordered = [
            NaturalKeyRank::new(&["pk", "20", "2"], &["지상", "2층"], 0),
            NaturalKeyRank::new(&["pk", "20", "1"], &["지상", "1층 일부"], 1),
            NaturalKeyRank::new(&["pk", "20", "1"], &["지상", "1층"], 2),
        ];
        let a = natural_key_occurrences(first.to_vec());
        let b = natural_key_occurrences(reordered.to_vec());

        assert_eq!(a[2], 1);
        assert_eq!((a[0], a[1]), (b[2], b[1]));
        assert_ne!(a[0], a[1]);
    }

    /// Identical rows still get distinct occurrences, so their ids differ.
    #[test]
    fn identical_rows_get_distinct_occurrences() {
        let rows = vec![
            NaturalKeyRank::new(&["pk", "20", "1"], &["지상", "1층"], 0),
            NaturalKeyRank::new(&["pk", "20", "1"], &["지상", "1층"], 1),
        ];
        let mut occurrences = natural_key_occurrences(rows);
        occurrences.sort_unstable();

        assert_eq!(occurrences, vec![1, 2]);
    }

    /// Two rows of one object must not share an id. They did the moment the line number came
    /// out of `source_record_id`, and nothing else in the row would have said so.
    #[test]
    fn two_lines_of_one_object_get_different_ids() {
        let first = row_identity("building-register-unit", "bronze/source=x/a.zip", Some(1));
        let second = row_identity("building-register-unit", "bronze/source=x/a.zip", Some(2));

        assert_ne!(first, second);
        assert!(first.ends_with("#line-000001"));
        assert!(second.ends_with("#line-000002"));
    }

    /// The id keeps the shape the tables already hold, so rows written before this split and
    /// rows written after it are the same row.
    #[test]
    fn the_id_matches_what_the_tables_already_hold() {
        assert_eq!(
            row_identity(
                "building-register-unit-area",
                "bronze/source=x/a.zip",
                Some(1)
            ),
            "building-register-unit-area:bronze/source=x/a.zip#line-000001"
        );
    }

    /// Six digits, not the raw number: an id sorts as text, and `#line-2` would sort before
    /// `#line-10` in every listing that shows these.
    #[test]
    fn the_line_number_is_padded_so_ids_sort_in_line_order() {
        let mut ids = [
            row_identity("k", "obj", Some(10)),
            row_identity("k", "obj", Some(2)),
        ];
        ids.sort();

        assert_eq!(ids[0], row_identity("k", "obj", Some(2)));
    }

    /// A source that states no line still yields an id rather than a panic or an empty string.
    #[test]
    fn a_row_without_a_line_number_still_gets_an_id() {
        assert_eq!(
            row_identity("building-register-floor", "bronze/source=x/a.zip", None),
            "building-register-floor:bronze/source=x/a.zip"
        );
    }
}
