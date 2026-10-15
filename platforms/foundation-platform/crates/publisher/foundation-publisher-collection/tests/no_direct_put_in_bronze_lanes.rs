//! Structural guard (ADR 0016): Bronze collection lanes must route every Bronze
//! raw write through `BronzeCommitter`, never by calling the object-storage put
//! methods directly.
//!
//! The ONLY production code allowed to call `ObjectStorageService::put_object` /
//! `put_streaming_object` for Bronze is the committer's two write adapters:
//!   - `services/foundation-outbox-publisher/src/bronze_object_storage.rs`
//!     (`BronzeObjectStorageWriter`, page adapter)
//!   - `services/foundation-outbox-publisher/src/bulk_streaming_bronze.rs`
//!     (`BronzeStreamingObjectStorageWriter`, streaming adapter)
//!
//! Those two adapter files are NOT in the guarded lane set below — they ARE the
//! single allowed Bronze write point. Mutable non-Bronze writers (vector-tile
//! manifest pointer, `parcel_marker_anchor_*` artifacts, smoke, cadastral silver
//! shard export) legitimately call `put_object(OverwriteAllowed)` and are also
//! out of scope.
//!
//! This test FAILS if any Bronze collection lane module reintroduces a direct
//! `.put_object(` / `.put_streaming_object(` in PRODUCTION (non-`#[cfg(test)]`)
//! code, making the committer routing structurally enforced rather than mere
//! convention.
//!
//! Test code is excluded with a brace-depth scanner (see `production_lines`):
//! every `.put_object(` inside a `#[cfg(test)]`-attributed item (e.g. the trailing
//! `#[cfg(test)] mod tests { … }`, or a `#[tokio::test]` fn within it) is ignored.
//! A naive "split on the first `#[cfg(test)]`" is deliberately NOT used: several
//! lanes (notably `building_register_ingest.rs`) place `#[cfg(test)] use …;`
//! test-only imports near the top of the file, so splitting there would discard
//! real production code and could hide a reintroduced production put.

/// A guarded Bronze-lane source file: its `label` (for failure messages) and its
/// full text, embedded at compile time so the test is hermetic (no runtime cwd
/// dependency). Paths are relative to THIS file (`services/foundation-outbox-publisher/tests/`).
struct GuardedFile {
    label: &'static str,
    source: &'static str,
}

/// The forbidden direct-write method calls (Bronze must go through `BronzeCommitter`).
const FORBIDDEN_PUTS: [&str; 2] = [".put_object(", ".put_streaming_object("];

/// Path-token reads that would promote the human-readable object key back into
/// catalog truth. Bronze collection code must get source/date/identity facts from
/// `BronzeObject` / plan metadata, not by parsing `object_key`.
const FORBIDDEN_PATH_TRUTH_READS: [&str; 16] = [
    "object_key.strip_prefix(",
    "object_key.strip_suffix(",
    "object_key.split(",
    "object_key.split_once(",
    "object_key.rsplit(",
    "object_key.rsplit_once(",
    "object_key.find(",
    "object_key.rfind(",
    "object_key.starts_with(\"bronze/source=\")",
    "object_key.starts_with(\"source=\")",
    "object_key.contains(\"source=\")",
    "object_key.contains(\"period=\")",
    "object_key.contains(\"run_id=\")",
    "object_key.contains(\"partition=\")",
    "object_key.contains(\"provider_file_period=\")",
    "object_key.contains(\"ingest_date=\")",
];

fn guarded_files() -> Vec<GuardedFile> {
    macro_rules! guarded {
        ($rel:literal) => {
            GuardedFile {
                label: $rel,
                source: include_str!(concat!("../src/", $rel)),
            }
        };
    }

    vec![
        // building-register lane (top module + its put-bearing submodules)
        guarded!("building_register_ingest.rs"),
        guarded!("building_register_ingest/plan.rs"),
        guarded!("building_register_ingest/persist.rs"),
        // single-file lanes
        guarded!("real_transaction_ingest.rs"),
        guarded!("vworld_cadastral_ingest.rs"),
        guarded!("vworld_ned_attribute_ingest.rs"),
        guarded!("vworld_land_register_ingest.rs"),
        // building-hub bulk lane (top module; submodule dir holds only tests.rs)
        guarded!("building_hub_bulk_ingest.rs"),
        // vworld dataset-file lane (top module; submodule dir holds only tests.rs)
        guarded!("vworld_dataset_file_ingest.rs"),
        // national async lane (every .rs)
        guarded!("national_data_collection_async/bronze_ingest.rs"),
        guarded!("national_data_collection_async/building_register.rs"),
        guarded!("national_data_collection_async/config.rs"),
        guarded!("national_data_collection_async/env.rs"),
        guarded!("national_data_collection_async/events.rs"),
        guarded!("national_data_collection_async/evidence.rs"),
        guarded!("national_data_collection_async/ledger.rs"),
        guarded!("national_data_collection_async/ledger_job_bus.rs"),
        guarded!("national_data_collection_async/page_queue.rs"),
        guarded!("national_data_collection_async/plan.rs"),
        guarded!("national_data_collection_async/real_transaction.rs"),
    ]
}

/// Returns the 1-based line numbers that are PRODUCTION code (i.e. NOT inside any
/// `#[cfg(test)]`-attributed item). A `#[cfg(test)]` attribute at the file's top
/// brace level marks the item that follows it as test code; if that item opens a
/// block (`{`), the test region lasts until brace depth returns to the level where
/// it started, otherwise (a single statement such as `#[cfg(test)] use …;`) it
/// covers only that statement.
///
/// This is a pragmatic Rust-source scanner: it counts net `{`/`}` per line and does
/// not parse strings/comments. That is sound for this guard because (a) the lanes
/// are real, well-formed Rust whose `#[cfg(test)]` items are ordinary `mod`/`use`/
/// `fn` items, and (b) any miscount would only ever cause the guard to be MORE
/// conservative about what counts as test code — it can never silently reclassify a
/// production put as test (the failure direction we care about is never masked,
/// because a stray brace in a string would, if anything, end a test region early and
/// expose more lines as production).
fn production_lines(source: &str) -> Vec<usize> {
    // Net `{` minus `}` for a line, as a signed delta (no lossy casts).
    fn brace_delta(line: &str) -> isize {
        line.chars().fold(0isize, |acc, ch| match ch {
            '{' => acc + 1,
            '}' => acc - 1,
            _ => acc,
        })
    }
    // Whether a line opens at least one `{` (head of a block item).
    fn opens_block(line: &str) -> bool {
        line.contains('{')
    }

    let mut out = Vec::new();
    let mut depth: isize = 0;
    // When `Some(start_depth)`, we are inside a `#[cfg(test)]` block item that began
    // at brace depth `start_depth`; it ends when depth returns to `start_depth`.
    let mut test_block_until: Option<isize> = None;
    // True after seeing a `#[cfg(test)]` attribute, until the attributed item begins.
    let mut pending_cfg_test = false;

    for (idx, raw) in source.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();

        // A line still inside an open #[cfg(test)] block is test code regardless of
        // its content; account for its braces, then check whether the block closes.
        if let Some(start_depth) = test_block_until {
            depth += brace_delta(raw);
            if depth <= start_depth {
                test_block_until = None;
            }
            continue; // line classified as test
        }

        if pending_cfg_test {
            // This line begins the #[cfg(test)]-attributed item. Allow further
            // attributes (e.g. `#[tokio::test]`, doc lines) to stack ahead of it.
            if line.starts_with("#[") || line.starts_with("//") || line.is_empty() {
                // still scanning attributes/blank/comment lines before the item head
            } else {
                if opens_block(raw) {
                    // Block item (mod/fn/impl …): test region until depth unwinds.
                    let start_depth = depth;
                    depth += brace_delta(raw);
                    if depth > start_depth {
                        test_block_until = Some(start_depth);
                    }
                    // else opened and closed on one line -> only this line is test.
                } else {
                    // Single-statement item (e.g. `#[cfg(test)] use …;`): just this line.
                    depth += brace_delta(raw);
                }
                pending_cfg_test = false;
                continue; // line classified as test
            }
            // (attribute/blank/comment line ahead of the item head) -> treat as test
            continue;
        }

        // Plain production line. Detect a fresh `#[cfg(test)]` attribute.
        if line.contains("#[cfg(test)]") {
            pending_cfg_test = true;
            // The attribute line itself is non-executable; classify as test, no braces.
            continue;
        }

        // Genuine production line.
        out.push(line_no);
        depth += brace_delta(raw);
    }

    out
}

fn path_truth_violations_in_source(label: &str, source: &str) -> Vec<String> {
    let production: std::collections::BTreeSet<usize> =
        production_lines(source).into_iter().collect();
    let mut violations = Vec::new();

    for (idx, raw) in source.lines().enumerate() {
        let line_no = idx + 1;
        if !production.contains(&line_no) {
            continue;
        }

        let compact: String = raw.chars().filter(|ch| !ch.is_whitespace()).collect();
        for needle in FORBIDDEN_PATH_TRUTH_READS {
            if compact.contains(needle) {
                violations.push(format!(
                    "  {label}:{line_no}: forbidden object_key-derived catalog truth `{needle}`\n    -> {}",
                    raw.trim(),
                ));
            }
        }
    }

    violations
}

#[test]
fn bronze_lanes_have_no_direct_put_in_production_code() {
    let mut violations: Vec<String> = Vec::new();

    for file in guarded_files() {
        let production: std::collections::BTreeSet<usize> =
            production_lines(file.source).into_iter().collect();

        for (idx, raw) in file.source.lines().enumerate() {
            let line_no = idx + 1;
            if !production.contains(&line_no) {
                continue; // test code -> ignored
            }
            for needle in FORBIDDEN_PUTS {
                if raw.contains(needle) {
                    violations.push(format!(
                        "  {}:{}: forbidden direct Bronze write `{}` in production code\n    -> {}",
                        file.label,
                        line_no,
                        needle.trim_start_matches('.').trim_end_matches('('),
                        raw.trim(),
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "\n\nBronze collection lane(s) call object-storage put directly in production code, \
bypassing the committer.\n\
Every Bronze raw write MUST route through `BronzeCommitter` (ADR 0016); the only allowed \
write point is the committer's adapters in `bronze_object_storage.rs` / `bulk_streaming_bronze.rs`.\n\
Offending site(s):\n{}\n\n\
Fix: emit the Bronze object through the committer / its write adapters instead of calling \
`put_object` / `put_streaming_object` from the lane module.\n",
        violations.join("\n"),
    );
}

#[test]
fn bronze_lanes_do_not_derive_catalog_truth_from_object_key() {
    let mut violations: Vec<String> = Vec::new();

    for file in guarded_files() {
        violations.extend(path_truth_violations_in_source(file.label, file.source));
    }

    assert!(
        violations.is_empty(),
        "\n\nBronze collection lane(s) derive catalog truth from the human-readable object key.\n\
R2 object_key is a physical location label only (ADR 0019); source identity, snapshot period/date, \
checksum, and lineage MUST come from the Bronze plan / Postgres catalog metadata.\n\
Offending site(s):\n{}\n\n\
Fix: carry the required field through the plan/committer/catalog record instead of parsing \
`object_key` path tokens.\n",
        violations.join("\n"),
    );
}

#[test]
fn path_truth_guard_detects_catalog_metadata_inference_from_object_key() {
    let source = r#"
fn leak_catalog_truth(object_key: &str) -> Option<&str> {
    object_key.strip_prefix("bronze/source=")
}
"#;

    let violations = path_truth_violations_in_source("fixture.rs", source);

    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("fixture.rs:3"));
    assert!(violations[0].contains("object_key.strip_prefix"));
}
