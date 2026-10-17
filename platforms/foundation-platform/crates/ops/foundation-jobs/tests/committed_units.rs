//! The committed job units say what the job list says (root ADR-0270 §1). This is the CI check:
//! a hand edit to a unit's `TimeoutStartSec=`, or a limit changed in the list without
//! `cargo run -p foundation-jobs -- generate`, fails here.

use std::path::PathBuf;

use foundation_jobs::{check, JobList, UNIT_DIR};

#[test]
fn every_job_unit_says_its_time_limit() -> Result<(), String> {
    // Read at run time: cargo sets it for the test process (no compile-time file dependency).
    let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|error| error.to_string())?;
    let root = PathBuf::from(manifest).join("../../..");
    let list = JobList::read(&root)?;
    let units = root.join(UNIT_DIR);
    let read =
        |file: &str| std::fs::read_to_string(units.join(file)).map_err(|error| error.to_string());
    let problems = check(&list, &read);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    assert!(!list.jobs.is_empty(), "the job list names no job");
    Ok(())
}
