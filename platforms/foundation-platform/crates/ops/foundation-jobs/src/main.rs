//! `foundation-jobs check | generate [<platform root>]` (root ADR-0270 §1).
//!
//! `check` lists every way the committed job units and the job state `tmpfiles.d` file differ from
//! the job list and fails if any does; `generate` rewrites what differs. The platform root defaults to the current directory
//! (run it from `platforms/foundation-platform`).

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use foundation_jobs::{
    check, check_tmpfiles, render, render_tmpfiles, JobList, TMPFILES, UNIT_DIR,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = args
        .get(1)
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let mut err = std::io::stderr().lock();
    let list = match JobList::read(&root) {
        Ok(list) => list,
        Err(error) => {
            let _ = writeln!(err, "{error}");
            return ExitCode::FAILURE;
        }
    };
    let units = root.join(UNIT_DIR);
    let read =
        |file: &str| std::fs::read_to_string(units.join(file)).map_err(|error| error.to_string());
    match args.first().map(String::as_str) {
        Some("check") => {
            let mut problems = check(&list, &read);
            problems.extend(check_tmpfiles(
                &list,
                std::fs::read_to_string(root.join(TMPFILES)).map_err(|error| error.to_string()),
            ));
            for problem in &problems {
                let _ = writeln!(err, "{problem}");
            }
            if problems.is_empty() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Some("generate") => match render(&list, &read) {
            Err(problems) => {
                for problem in &problems {
                    let _ = writeln!(err, "{problem}");
                }
                ExitCode::FAILURE
            }
            Ok(rendered) => {
                for unit in rendered.iter().filter(|unit| unit.current != unit.rendered) {
                    if let Err(error) = std::fs::write(units.join(&unit.file), &unit.rendered) {
                        let _ = writeln!(err, "{}: {error}", unit.file);
                        return ExitCode::FAILURE;
                    }
                    let _ = writeln!(err, "rendered {}", unit.file);
                }
                match render_tmpfiles(&list) {
                    Ok(rendered) => {
                        if let Err(error) = std::fs::write(root.join(TMPFILES), rendered) {
                            let _ = writeln!(err, "{TMPFILES}: {error}");
                            return ExitCode::FAILURE;
                        }
                        ExitCode::SUCCESS
                    }
                    Err(problems) => {
                        for problem in &problems {
                            let _ = writeln!(err, "{problem}");
                        }
                        ExitCode::FAILURE
                    }
                }
            }
        },
        _ => {
            let _ = writeln!(
                err,
                "usage: foundation-jobs check | generate [<platform root>]"
            );
            ExitCode::from(64)
        }
    }
}
