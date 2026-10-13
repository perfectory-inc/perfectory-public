//! The shards of a by-PNU bake, planned from one snapshot's manifests before any shard runs.
//!
//! A bake used to start from the nine one-digit PNU prefixes and split a shard into its ten
//! children only after the export refused it for its row cap. On 2026-10-13 the parcel kind lanes'
//! first generations ran 243 shards for one lane, 134 of them over prefixes that hold no row; each
//! still opened the catalog and every manifest (about 42 s), 37% of the bake's shard time, and the
//! refused shards were read twice. The manifests already say which rows can be where: a data file
//! whose PNU bounds exclude a prefix holds none of it ([`ScannedDataFile::excludes_prefix`], root
//! ADR-0164, the table written in PNU order).
//!
//! The plan counts, for a prefix, the records of every data file the bounds cannot exclude — an
//! upper bound on its rows, so a planned shard under the cap is under it when baked. A prefix with
//! no such file is not a shard at all. A prefix over the cap is replaced by its ten children while
//! that separates its files; when no child holds fewer files (a file with no bounds, or one larger
//! than the cap), the prefix stays whole and the bake's refusal-and-split remains the fallback.

use super::iceberg_scan::ScannedDataFile;

/// Digits of a full PNU: no shard is longer.
const PNU_DIGITS: usize = 10;

/// One planned shard: a PNU prefix and the most rows the manifests allow it to hold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedShard {
    pub prefix: String,
    pub estimated_rows: u64,
}

/// The shards that cover every row of `files` keyed by `column`, each estimated at most
/// `max_rows` where the manifests can show it, in prefix order.
pub fn plan_prefix_shards(
    files: &[ScannedDataFile],
    column: &str,
    max_rows: u64,
) -> Vec<PlannedShard> {
    let all = files.iter().collect::<Vec<_>>();
    let mut planned = Vec::new();
    for digit in '1'..='9' {
        visit(&digit.to_string(), &all, column, max_rows, &mut planned);
    }
    planned
}

fn visit(
    prefix: &str,
    candidates: &[&ScannedDataFile],
    column: &str,
    max_rows: u64,
    planned: &mut Vec<PlannedShard>,
) {
    let here = holding(prefix, candidates, column);
    if here.is_empty() {
        return;
    }
    let rows = records(&here);
    if rows <= max_rows || prefix.len() >= PNU_DIGITS {
        planned.push(PlannedShard {
            prefix: prefix.to_owned(),
            estimated_rows: rows,
        });
        return;
    }
    let children = ('0'..='9')
        .map(|digit| format!("{prefix}{digit}"))
        .collect::<Vec<_>>();
    // Splitting helps only when some child holds fewer of the files.
    let separates = children
        .iter()
        .any(|child| holding(child, &here, column).len() < here.len());
    if !separates {
        planned.push(PlannedShard {
            prefix: prefix.to_owned(),
            estimated_rows: rows,
        });
        return;
    }
    for child in &children {
        visit(child, &here, column, max_rows, planned);
    }
}

fn holding<'a>(
    prefix: &str,
    candidates: &[&'a ScannedDataFile],
    column: &str,
) -> Vec<&'a ScannedDataFile> {
    candidates
        .iter()
        .copied()
        .filter(|file| !file.excludes_prefix(column, prefix))
        .collect()
}

fn records(files: &[&ScannedDataFile]) -> u64 {
    files
        .iter()
        .map(|file| file.record_count)
        .fold(0, u64::saturating_add)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::iceberg_scan::ColumnStatistics;
    use super::*;

    /// A data file of `records` rows whose PNUs lie between `lower` and `upper` (synthetic, in the
    /// reserved `99999` region where it matters).
    fn file(lower: &str, upper: &str, records: u64) -> ScannedDataFile {
        ScannedDataFile {
            file_path: format!("s3://fixture/{lower}-{upper}.parquet"),
            record_count: records,
            file_size_in_bytes: 1,
            column_statistics: BTreeMap::from([(
                "pnu".to_owned(),
                ColumnStatistics {
                    lower_bound: Some(lower.as_bytes().to_vec()),
                    upper_bound: Some(upper.as_bytes().to_vec()),
                    null_value_count: Some(0),
                },
            )]),
        }
    }

    fn prefixes(plan: &[PlannedShard]) -> Vec<&str> {
        plan.iter().map(|shard| shard.prefix.as_str()).collect()
    }

    #[test]
    fn a_prefix_no_file_can_hold_is_not_a_shard() {
        // Rows under 1 and 9 only: 2..8 hold nothing and are never baked.
        let files = [
            file("1000000000000000000", "1999999999999999999", 10),
            file("9999900000000000000", "9999999999999999999", 5),
        ];
        let plan = plan_prefix_shards(&files, "pnu", 100);
        assert_eq!(prefixes(&plan), ["1", "9"]);
        assert_eq!(plan[0].estimated_rows, 10);
    }

    #[test]
    fn a_prefix_over_the_cap_is_split_only_where_its_files_lie() {
        // Prefix 99 holds 300 rows in three files under 9991, 9993 and 9995: three shards, not
        // ten children of every level down.
        let files = [
            file("9991000000000000000", "9991999999999999999", 100),
            file("9993000000000000000", "9993999999999999999", 100),
            file("9995000000000000000", "9995999999999999999", 100),
        ];
        let plan = plan_prefix_shards(&files, "pnu", 150);
        assert_eq!(prefixes(&plan), ["9991", "9993", "9995"]);
        assert!(plan.iter().all(|shard| shard.estimated_rows <= 150));
        // Every row is in exactly one shard.
        assert_eq!(
            plan.iter().map(|shard| shard.estimated_rows).sum::<u64>(),
            300
        );
    }

    #[test]
    fn a_file_the_bounds_cannot_place_keeps_its_prefix_whole() {
        // No bounds: every child would still hold it, so splitting cannot help and the bake's
        // refusal-and-split stays the fallback instead of ten-to-the-tenth empty shards.
        let mut unbounded = file("1", "9", 500);
        unbounded.column_statistics.clear();
        let plan = plan_prefix_shards(&[unbounded], "pnu", 100);
        assert_eq!(
            prefixes(&plan),
            ["1", "2", "3", "4", "5", "6", "7", "8", "9"]
        );
    }
}
