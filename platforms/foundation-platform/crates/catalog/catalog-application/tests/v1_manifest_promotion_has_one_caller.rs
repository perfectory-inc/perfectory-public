//! The schema-v1 tile manifest is being retired (root ADR-0225): no new code may write one.
//!
//! The only place left that builds the v1 promotion use case is the foundation-api state behind
//! the `manifest:promote` route, which ADR-0225 step 3 deletes together with v1. A command that
//! promotes markers (or anything else) into v1 would keep a version alive that production no
//! longer serves, so this test names every file that constructs the use case and fails on any
//! other.

use std::fs;
use std::path::{Path, PathBuf};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Constructing the use case is what writing a v1 manifest requires; a mention in prose is not.
const CONSTRUCTOR: &str = "PromoteVectorTileManifest::new(";
const ALLOWED: &[&str] = &["services/foundation-api/src/state.rs"];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            rust_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

#[test]
fn only_the_v1_http_route_constructs_the_v1_manifest_promotion() -> TestResult {
    let area = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let mut files = Vec::new();
    for root in ["crates", "services"] {
        rust_files(&area.join(root), &mut files)?;
    }
    let this_file = Path::new(file!()).file_name();
    let mut callers = Vec::new();
    for file in files {
        if file.file_name() == this_file {
            continue;
        }
        if fs::read_to_string(&file)?.contains(CONSTRUCTOR) {
            let relative = file
                .strip_prefix(&area)?
                .to_string_lossy()
                .replace('\\', "/");
            callers.push(relative);
        }
    }
    callers.sort();
    assert!(
        callers
            .iter()
            .all(|caller| ALLOWED.contains(&caller.as_str())),
        "these files build the schema-v1 manifest promotion; v1 is retiring (root ADR-0225) and \
         a new layer is published as a v2 publication unit instead: {callers:?}"
    );
    assert!(
        !callers.is_empty(),
        "the scan found no caller at all, so it is not looking at the source tree"
    );
    Ok(())
}
