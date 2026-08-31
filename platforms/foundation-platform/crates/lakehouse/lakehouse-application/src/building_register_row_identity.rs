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

#[cfg(test)]
mod tests {
    use super::*;

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
