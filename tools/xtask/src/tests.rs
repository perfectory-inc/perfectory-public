
use super::*;

/// The area list has one home, `tools/areas.contract.json`; this table attaches verification
/// to it. An area added on one side only, or a dir that differs, fails here. `tooling` is the
/// verifier verifying itself, not an area.
#[test]
fn areas_match_the_registry() {
    let text = std::fs::read_to_string(repo_root().join("tools/areas.contract.json")).unwrap();
    let registry: Vec<(String, String)> = text
        .lines()
        .filter(|line| line.contains("\"slug\":"))
        .map(|line| {
            let field = |key: &str| {
                let fields: Vec<&str> = line.split('"').collect();
                let at = fields.iter().position(|f| *f == key).unwrap();
                fields[at + 2].to_owned()
            };
            (field("slug"), field("dir"))
        })
        .collect();
    let table: Vec<(String, String)> = AREAS
        .iter()
        .filter(|area| area.slug != "tooling")
        .map(|area| (area.slug.to_owned(), area.dir.to_owned()))
        .collect();
    assert!(!registry.is_empty(), "the registry lists no areas");
    assert_eq!(table, registry);
}

#[test]
fn ci_job_without_the_shared_rust_setup_is_refused() {
    assert!(ci_rust_setup_missing(Some("true"), None));
    assert!(ci_rust_setup_missing(Some("true"), Some("")));
    assert!(!ci_rust_setup_missing(Some("true"), Some("1.96.0")));
    assert!(!ci_rust_setup_missing(None, None));
    assert!(!ci_rust_setup_missing(Some("false"), None));
}

#[test]
fn nested_cargo_removes_only_parent_crate_metadata() {
    let mut command = Command::new("cargo");
    let metadata = [
        "CARGO_MANIFEST_DIR",
        "CARGO_MANIFEST_PATH",
        "CARGO_PKG_NAME",
        "CARGO_PKG_VERSION_MAJOR",
        "CARGO_PKG_NEW_METADATA",
        "CARGO_CRATE_NAME",
        "CARGO_BIN_NAME",
        "CARGO_PRIMARY_PACKAGE",
        "CARGO_TARGET_TMPDIR",
        "CARGO_BIN_EXE_example",
    ];
    let settings = [
        "CARGO_TARGET_DIR",
        "CARGO_PROFILE_DEV_DEBUG",
        "CARGO_PROFILE_TEST_DEBUG",
        "CARGO_NET_OFFLINE",
        "CARGO_BUILD_JOBS",
        "CARGO_INCREMENTAL",
        "CARGO_HOME",
        "RUSTFLAGS",
        "CARGO",
        "RUSTC_WRAPPER",
        "CARGO_ENCODED_RUSTFLAGS",
    ];
    for key in metadata {
        command.env(key, "parent-crate");
    }
    for key in settings {
        command.env(key, "explicit-setting");
    }
    remove_parent_crate_metadata(&mut command);
    let overrides: std::collections::BTreeMap<_, _> = command.get_envs().collect();
    for key in metadata {
        assert_eq!(
            overrides.get(std::ffi::OsStr::new(key)),
            Some(&None),
            "{key}"
        );
    }
    for key in settings {
        assert_eq!(
            overrides.get(std::ffi::OsStr::new(key)),
            Some(&Some(std::ffi::OsStr::new("explicit-setting"))),
            "{key}"
        );
    }
}

#[test]
fn shared_cargo_command_keeps_arguments_directory_and_user_settings() {
    let directory = Path::new("area-workspace");
    let command = cargo_command(directory, &["test", "--locked", "--workspace"]);
    assert_eq!(command.get_current_dir(), Some(directory));
    assert_eq!(command.get_program(), "cargo");
    assert_eq!(
        command.get_args().collect::<Vec<_>>(),
        ["test", "--locked", "--workspace"]
    );
    let overrides: std::collections::BTreeMap<_, _> = command.get_envs().collect();
    assert_eq!(
        overrides.get(std::ffi::OsStr::new("SQLX_OFFLINE")),
        Some(&Some(std::ffi::OsStr::new("true")))
    );
    for key in [
        "CARGO_TARGET_DIR",
        "CARGO_PROFILE_DEV_DEBUG",
        "CARGO_NET_OFFLINE",
        "CARGO_BUILD_JOBS",
    ] {
        assert!(
            !overrides.contains_key(std::ffi::OsStr::new(key)),
            "user setting {key} must stay inherited"
        );
    }
}

/// Every `cargo test` selection verify makes runs twice, under nextest and as doctests, with
/// the same packages, features and exclusions (root ADR-0205). Dropping the doctest half, or
/// letting the two halves select differently, fails here.
#[test]
fn every_test_selection_runs_under_nextest_and_as_doctests() {
    let mut checked = 0;
    for area in AREAS {
        for selection in verify_test_commands(area) {
            let [nextest, doc] = nextest_commands(&selection);
            assert_eq!(nextest[..3], ["nextest", "run", "--no-fail-fast"]);
            assert_eq!(doc[..2], ["test", "--doc"]);
            assert_eq!(nextest[3..], doc[2..], "both halves select the same tests");
            assert_eq!(
                doc[2..],
                selection[1..],
                "the selection is carried unchanged"
            );
            checked += 1;
        }
    }
    assert!(checked >= AREAS.len());
}

/// CI runs every workspace's tests under the nextest `ci` profile (`NEXTEST_PROFILE=ci` from
/// scripts/ci/rust-setup.sh, root ADR-0269); nextest refuses to start when the profile is
/// missing, so a workspace without it would fail every CI run.
#[test]
fn every_verified_workspace_defines_the_nextest_ci_profile() {
    for area in AREAS {
        let config = repo_root().join(area.dir).join(".config/nextest.toml");
        let text = std::fs::read_to_string(&config)
            .unwrap_or_else(|error| panic!("{}: {error}", config.display()));
        assert!(
            text.lines().any(|line| line.trim() == "[profile.ci]"),
            "{} defines no [profile.ci]",
            config.display()
        );
    }
}

/// The suite answer is read exactly (root ADR-0269): a skip needs the selector to say so.
#[test]
fn the_suite_selection_is_read_exactly() {
    assert_eq!(
        parse_suite_selection("all\ta run-all input changed\n"),
        Ok((SuiteSelection::All, "a run-all input changed".to_owned()))
    );
    let (some, reason) = parse_suite_selection(
        "some\t2 test module(s)\ntest\tp/a/tests/test_x.py\nnode\tp/a/services/gw\n",
    )
    .unwrap();
    assert_eq!(reason, "2 test module(s)");
    assert!(some.runs_test("p/a/tests/test_x.py") && !some.runs_test("p/a/tests/test_y.py"));
    assert!(some.runs_any_under("p/a/tests") && !some.runs_any_under("p/a/other"));
    assert!(some.runs_node("p/a/services/gw") && !some.runs_node("p/a/services/other"));
    assert!(parse_suite_selection("some\tonly Rust\n")
        .unwrap()
        .0
        .is_empty());
    for broken in [
        "",
        "some\n",
        "maybe\twhy\n",
        "all\twhy\ntest\tx\n",
        "some\twhy\nfile\tx\n",
    ] {
        assert!(
            parse_suite_selection(broken).is_err(),
            "{broken:?} must be refused"
        );
    }
}

/// A selection keeps exactly the discovered modules it names, by the repository path the
/// selector prints (root ADR-0269): a module the change reaches must run.
#[test]
fn a_selected_module_is_found_by_its_repository_path() {
    let foundation = AREAS.iter().find(|area| area.slug == "foundation").unwrap();
    assert_eq!(
        repo_path(
            foundation,
            &[".", "infra/lakehouse/spark/tests", "test_gold_rebuild.py"]
        ),
        "platforms/foundation-platform/infra/lakehouse/spark/tests/test_gold_rebuild.py"
    );
    assert_eq!(
        repo_path(
            foundation,
            &["services/foundation-provider-acquisition-worker/tests"]
        ),
        "platforms/foundation-platform/services/foundation-provider-acquisition-worker/tests"
    );
}

/// Every workspace with more than one member keeps a hakari workspace-hack crate (root
/// ADR-0269), so `-p` subsets and `--workspace` build dependencies with one feature set; one
/// member has nothing to unify. A workspace that grows a second member without one fails here.
#[test]
fn every_multi_member_workspace_keeps_a_workspace_hack() {
    for area in AREAS {
        let dir = repo_root().join(area.dir);
        let manifest = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
        let members = manifest
            .split_once("members = [")
            .and_then(|(_, rest)| rest.split_once(']'))
            .map_or(0, |(list, _)| list.matches('"').count() / 2);
        let config = std::fs::read_to_string(dir.join(".config/hakari.toml")).ok();
        if members <= 1 {
            assert!(
                config.is_none(),
                "{}: one member has nothing to unify",
                area.dir
            );
            continue;
        }
        let config = config.unwrap_or_else(|| panic!("{} has no .config/hakari.toml", area.dir));
        let package = format!("{}-workspace-hack", area.slug);
        assert!(
            config.contains(&format!("hakari-package = \"{package}\"")),
            "{}: hakari-package must be {package}",
            area.dir
        );
        assert!(
            manifest.contains(&format!("\"crates/{package}\"")),
            "{}: crates/{package} must be a member",
            area.dir
        );
    }
}

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

/// The selector's answer is read exactly; anything else is a failure, never a full or empty run
/// (root ADR-0269).
#[test]
fn the_crate_selection_is_read_exactly() {
    assert_eq!(
        parse_selection("all\tmerge_group verifies every crate\n"),
        Ok(Selection::All)
    );
    assert_eq!(
        parse_selection(
            "some\twhy\nchanged\ta\nreader\tr\naffected\ta\naffected\tb\naffected\tr\n"
        ),
        Ok(Selection::Some {
            changed: names(&["a"]),
            readers: names(&["r"]),
            affected: names(&["a", "b", "r"]),
        })
    );
    assert_eq!(
        parse_selection("some\tnothing reaches a crate\n"),
        Ok(Selection::Some {
            changed: vec![],
            readers: vec![],
            affected: vec![],
        })
    );
    for broken in [
        "",
        "maybe\twhy\n",
        "some\twhy\nadded\ta\n",
        "all\twhy\naffected\ta\n",
    ] {
        assert!(
            parse_selection(broken).is_err(),
            "{broken:?} must be refused"
        );
    }
}

/// A pull request lints, builds and tests the affected packages only, and nextest narrows to
/// the changed crates' rdeps() and the readers' own package(); no selection keeps the workspace.
#[test]
fn an_affected_selection_narrows_clippy_and_the_tests() {
    let foundation = AREAS.iter().find(|area| area.slug == "foundation").unwrap();
    let some = Selection::Some {
        changed: names(&["foundation-outbox"]),
        readers: names(&["catalog-application"]),
        affected: names(&["catalog-application", "foundation-api", "foundation-outbox"]),
    };
    let clippy = clippy_command(&some).unwrap();
    assert!(!clippy.contains(&"--workspace".to_owned()));
    assert_eq!(clippy[..3], names(&["clippy", "--locked", "--timings"])[..]);
    assert_eq!(
        clippy[3..9],
        names(&[
            "-p",
            "catalog-application",
            "-p",
            "foundation-api",
            "-p",
            "foundation-outbox"
        ])[..]
    );
    let tests = test_selections(foundation, &some);
    assert_eq!(tests.len(), 1);
    let (command, filterset) = &tests[0];
    assert!(!command.contains(&"--workspace".to_owned()));
    assert_eq!(command.iter().filter(|arg| *arg == "-p").count(), 3);
    assert_eq!(
        filterset.as_deref(),
        Some("rdeps(=foundation-outbox) | package(=catalog-application)")
    );

    let nothing = Selection::Some {
        changed: vec![],
        readers: vec![],
        affected: vec![],
    };
    assert_eq!(clippy_command(&nothing), None);
    assert!(test_selections(foundation, &nothing).is_empty());

    assert!(clippy_command(&Selection::All)
        .unwrap()
        .contains(&"--workspace".to_owned()));
    let full: Vec<Vec<String>> = test_selections(foundation, &Selection::All)
        .into_iter()
        .map(|(command, filterset)| {
            assert_eq!(filterset, None);
            command
        })
        .collect();
    let expected: Vec<Vec<String>> = verify_test_commands(foundation)
        .into_iter()
        .map(|command| names(&command))
        .collect();
    assert_eq!(full, expected);
}

/// A feature-gated package keeps its own default-features stage under a selection, so its
/// live suite is never compiled into the offline run (as in `verify_test_commands`).
#[test]
fn an_affected_feature_gated_package_keeps_its_own_stage() {
    let gongzzang = AREAS.iter().find(|area| area.slug == "gongzzang").unwrap();
    let some = Selection::Some {
        changed: names(&["gongzzang-persistence"]),
        readers: vec![],
        affected: names(&["gongzzang-api", "gongzzang-persistence"]),
    };
    let commands: Vec<Vec<String>> = test_selections(gongzzang, &some)
        .into_iter()
        .map(|(command, _)| command)
        .collect();
    assert_eq!(
        commands,
        vec![
            names(&["test", "--locked", "--all-features", "-p", "gongzzang-api"]),
            names(&["test", "--locked", "-p", "gongzzang-persistence"]),
        ]
    );
    assert!(some.verifies("gongzzang-persistence") && !some.verifies("gongzzang-web"));
    assert!(Selection::All.verifies("anything"));
}

/// Only cargo's "no library" answer excuses a failed doctest run; a failing doctest, or that
/// answer mixed with any other error, still fails (root ADR-0205).
#[test]
fn only_a_missing_library_excuses_the_doctest_run() {
    assert!(no_doctest_targets(
        "error: no library targets found in package `xtask`\n"
    ));
    assert!(!no_doctest_targets(""));
    assert!(!no_doctest_targets(
        "error: doctest failed, to rerun pass `--doc`\n"
    ));
    assert!(!no_doctest_targets(
        "error: no library targets found in package `a`\nerror: could not compile `b`\n"
    ));
    // What CI prints under CARGO_TERM_COLOR=always (the required/repository failure).
    assert!(no_doctest_targets(
            "\u{1b}[1m\u{1b}[91merror\u{1b}[0m\u{1b}[1m:\u{1b}[0m no library targets found in package `xtask`\n"
        ));
    assert!(!no_doctest_targets(
        "\u{1b}[1m\u{1b}[91merror\u{1b}[0m\u{1b}[1m:\u{1b}[0m doctest failed\n"
    ));
}

/// `--part` selects one half and nothing else parses (root ADR-0199).
#[test]
fn verify_parts_parse_strictly() {
    assert_eq!(verify_part(None, None), Some(VerifyPart::Whole));
    assert_eq!(
        verify_part(Some("--part"), Some("cargo")),
        Some(VerifyPart::Cargo)
    );
    assert_eq!(
        verify_part(Some("--part"), Some("suites")),
        Some(VerifyPart::Suites)
    );
    assert_eq!(verify_part(Some("--part"), None), None);
    assert_eq!(verify_part(Some("--part"), Some("python")), None);
    assert_eq!(verify_part(Some("--fast"), None), None);
}

/// A workflow that runs one half of an area's verify runs the other half too, or the half it
/// left out would stop running in CI while every check stayed green (root ADR-0199).
#[test]
fn every_workflow_that_splits_verify_runs_both_halves() {
    let workflows = repo_root().join(".github/workflows");
    let mut split = 0;
    for entry in std::fs::read_dir(&workflows).expect("workflow directory") {
        let path = entry.expect("workflow entry").path();
        let text = std::fs::read_to_string(&path).expect("workflow text");
        assert_eq!(
            unpaired_verify_parts(&text),
            Vec::<String>::new(),
            "{}",
            path.display()
        );
        split += text.matches("--part ").count();
    }
    assert!(
        split > 0,
        "no workflow splits verify; the pairing check would see nothing"
    );
    assert_eq!(
        unpaired_verify_parts("run: cargo xtask verify foundation --part cargo\n"),
        ["foundation --part suites"]
    );
}

fn unpaired_verify_parts(workflow: &str) -> Vec<String> {
    let mut missing = Vec::new();
    for line in workflow.lines() {
        let Some(rest) = line.split("cargo xtask verify ").nth(1) else {
            continue;
        };
        let mut words = rest.split_whitespace();
        let (Some(area), Some("--part"), Some(part)) = (words.next(), words.next(), words.next())
        else {
            continue;
        };
        let other = if part == "cargo" { "suites" } else { "cargo" };
        let wanted = format!("{area} --part {other}");
        if !workflow.contains(&format!("cargo xtask verify {wanted}")) {
            missing.push(wanted);
        }
    }
    missing
}

/// A discover suite splits into exactly the modules discover itself would load (root
/// ADR-0199): matching files at the root only, not helpers, not a plain subdirectory; a root
/// with a package, or any other command shape, stays one process.
#[test]
fn a_discover_suite_splits_into_the_modules_discover_would_load() {
    let root = std::env::temp_dir().join(format!("xtask-split-{}", std::process::id()));
    let tests = root.join("tests");
    std::fs::create_dir_all(tests.join("fixtures")).expect("fixture dirs");
    for name in [
        "test_b.py",
        "test_a.py",
        "helper.py",
        "fixtures/test_inner.py",
    ] {
        std::fs::write(tests.join(name), "").expect("fixture file");
    }
    let plan = |args: &'static [&'static str]| PythonCommandPlan {
        current_dir: root.clone(),
        python_path: None,
        args,
    };
    let discover = plan(&[
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
        "-p",
        "test_*.py",
    ]);
    let modules = python_discover_modules(&discover).expect("splittable");
    let patterns: Vec<&str> = modules.iter().map(|args| args[6].as_str()).collect();
    assert_eq!(patterns, ["test_a.py", "test_b.py"]);
    assert!(modules.iter().all(|args| args[..6] == discover.args[..6]));

    assert!(python_discover_modules(&plan(&["-m", "pytest", "tests", "-q"])).is_none());
    std::fs::write(tests.join("fixtures/__init__.py"), "").expect("package marker");
    assert!(
        python_discover_modules(&discover).is_none(),
        "a package keeps one process"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The Python/Node side runs while cargo runs, not after it (root ADR-0199). Each side waits
/// for a signal only the other sends, so running them one after the other times out instead
/// of passing; both sides' failures, and a panicking side, all land in the one log.
#[test]
fn the_side_suites_run_beside_cargo_and_report_into_one_log() {
    use std::sync::mpsc;
    use std::time::Duration;
    let (to_main, from_side) = mpsc::channel();
    let (to_side, from_main) = mpsc::channel();
    let mut log = FailureLog::default();
    run_beside(
        &mut log,
        "side",
        |log| {
            to_side.send(()).ok();
            if from_side.recv_timeout(Duration::from_secs(10)).is_err() {
                log.failures
                    .push("main never heard from the side".to_owned());
            }
            log.failures.push("main failure".to_owned());
        },
        move |log| {
            to_main.send(()).ok();
            if from_main.recv_timeout(Duration::from_secs(10)).is_err() {
                log.failures.push("side never heard from main".to_owned());
            }
            log.failures.push("side failure".to_owned());
        },
    );
    assert_eq!(log.failures, ["main failure", "side failure"]);

    let mut log = FailureLog::default();
    run_beside(
        &mut log,
        "python/node",
        |_| {},
        |_| panic!("suite runner crashed"),
    );
    assert_eq!(log.failures.len(), 1);
    assert!(log.failures[0].contains("python/node thread panicked"));
}

/// An authoritative gate reports every failure, not the first one (ADR-0012 rule 1).
///
/// `verify` and the lanes used to exit at the first failing stage, so an auditor shown one
/// failure could not tell whether it was the only one — and a twenty-minute lane had to be run
/// again from the start after every fix. The log is asserted directly rather than through a
/// spawned build: `finish` ends the process, which a unit test cannot observe, but everything
/// that decides *what* it prints lives here.
#[test]
fn a_failure_log_keeps_every_failure_not_only_the_first() {
    let mut log = FailureLog::default();

    assert_eq!(
        log.absorb(Ok::<_, String>("clippy output")),
        Some("clippy output")
    );
    assert!(log.failures.is_empty(), "a success records nothing");

    assert_eq!(
        log.absorb(Err::<&str, _>("(exit 1) cargo fmt".to_owned())),
        None
    );
    log.record("foundation python suite 'tests' executed 0 tests".to_owned());

    assert_eq!(
        log.failures,
        vec![
            "(exit 1) cargo fmt",
            "foundation python suite 'tests' executed 0 tests"
        ],
        "both failures survive, in the order they happened"
    );
}

/// A lane must run exactly the targets it declares — never a workspace sweep.
///
/// `--workspace ... -- --ignored` is what currently drags Kafka/R2/lakehouse
/// tests into the Postgres job, where they find no broker, take their
/// "resource absent" branch, and are reported as passed. Enumerating targets
/// per lane is what makes an unrun test impossible to mistake for a verified
/// one.
#[test]
fn live_lane_runs_only_its_declared_targets_and_never_sweeps_the_workspace() {
    let area = AREAS.iter().find(|area| area.slug == "foundation").unwrap();
    let lane = area
        .live_lanes
        .iter()
        .find(|lane| lane.name == "kafka")
        .expect("foundation must declare a kafka live lane");

    // The env contract itself is asserted by
    // `foundation_lanes_require_every_variable_their_targets_read`; this
    // test owns only the shape of the command.
    assert!(!lane.required_env.is_empty());

    let commands = lane_commands(lane);
    assert_eq!(commands.len(), lane.targets.len());
    for command in &commands {
        assert!(command.iter().any(|arg| arg == "--locked"));
        assert!(command.iter().any(|arg| arg == "--ignored"));
        assert!(
            !command.iter().any(|arg| arg == "--workspace"),
            "a lane must never sweep the workspace: {command:?}"
        );
    }
}

/// Live lanes stay on `cargo test`, one process at a time per target, while `verify` moved to
/// nextest (root ADR-0205). Their suites share one database and serialize on an in-process
/// `TEST_LOCK`; nextest runs each test in its own process, where that lock serializes nothing.
/// `lane_target_verdict` also reads libtest's summary, which nextest does not print.
#[test]
fn every_live_lane_runs_as_one_serial_cargo_test_process() {
    let mut checked = 0;
    for area in AREAS {
        for lane in area.live_lanes {
            for command in lane_commands(lane) {
                assert_eq!(
                    command[0], "test",
                    "{} lane {}: {command:?}",
                    area.slug, lane.name
                );
                assert!(
                    !command.iter().any(|arg| arg == "nextest"),
                    "{} lane {}: {command:?}",
                    area.slug,
                    lane.name
                );
                assert_eq!(
                    command.last().map(String::as_str),
                    Some("--test-threads=1"),
                    "{} lane {}: {command:?}",
                    area.slug,
                    lane.name
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 0, "no lane command was checked");
}

/// A feature-gated lane needs the OPPOSITE flags of an `#[ignore]` lane, and
/// getting it wrong is silent.
///
#[test]
fn postgres_lane_selects_the_recovery_sql_binary_test_exactly() {
    let area = AREAS.iter().find(|area| area.slug == "foundation").unwrap();
    let lane = area
        .live_lanes
        .iter()
        .find(|lane| lane.name == "postgres")
        .unwrap();
    let commands = lane_commands(lane);
    let binaries: Vec<_> = commands
        .iter()
        .filter(|args| args.iter().any(|arg| arg == "--bin"))
        .collect();
    assert_eq!(binaries.len(), 1);
    assert_eq!(*binaries[0], vec![
            "test", "--locked", "-p", "foundation-outbox-publisher", "--bin", "foundation-outbox-publisher",
            "--", "--ignored", "--exact",
            "postgis_parcel_boundary_mirror_national_rebuild::tests::rebuild_records_one_provenance_pair_on_the_run_and_every_loaded_row",
            "--test-threads=1",
        ]);
    let target = lane
        .targets
        .iter()
        .find(|target| matches!(target, LaneTarget::Binary { .. }))
        .unwrap();
    assert!(lane_target_verdict(
        "foundation",
        "postgres",
        target,
        "test result: ok. 0 passed; 0 failed; 0 ignored;"
    )
    .is_err());
}

/// gongzzang's 20 integration targets carry zero `#[ignore]`; they are
/// compiled out by `#![cfg(feature = "integration")]`. Running them the
/// `#[ignore]` way fails twice over: without `--features integration` the
/// suite is an empty file that cannot even reference the code it tests, and
/// `--ignored` then selects none of the (nonexistent) tests. cargo exits 0.
/// That is ADR-0010's own defect reproduced one layer up, in the lane that
/// was supposed to end it.
#[test]
fn feature_gated_lane_compiles_its_suite_and_never_filters_on_ignored() {
    let area = AREAS.iter().find(|area| area.slug == "gongzzang").unwrap();
    let lane = area
        .live_lanes
        .iter()
        .find(|lane| lane.name == "postgres")
        .expect("gongzzang must declare a postgres live lane");

    let commands = lane_commands(lane);
    assert_eq!(commands.len(), lane.targets.len());
    for command in &commands {
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--features", "integration"]),
            "without the feature the suite is compiled out and nothing runs: {command:?}"
        );
        assert!(
            !command.iter().any(|arg| arg == "--ignored"),
            "nothing in this suite is #[ignore]; --ignored selects zero tests \
                 and cargo still exits 0: {command:?}"
        );
    }
}

/// The reverse mistake must stay impossible too: an `#[ignore]` lane that
/// gained a stray `--features` would be enabling a feature its area does not
/// define, and one that lost `--ignored` would run zero tests just as
/// silently.
#[test]
fn ignore_gated_lane_keeps_ignored_and_enables_no_feature() {
    for area in AREAS.iter().filter(|area| area.slug != "gongzzang") {
        for lane in area.live_lanes {
            for command in lane_commands(lane) {
                assert!(
                    command.iter().any(|arg| arg == "--ignored"),
                    "{}/{} is #[ignore]-gated: {command:?}",
                    area.slug,
                    lane.name
                );
                assert!(
                    !command.iter().any(|arg| arg == "--features"),
                    "{}/{} declares no feature gate: {command:?}",
                    area.slug,
                    lane.name
                );
            }
        }
    }
}

/// A lane must demand every variable its targets read — including the ones
/// that are switches rather than connection strings.
///
/// An audit of all 55 targets found three lanes whose contract was short.
/// `live_kafka_outage` returned `Ok(())` without FOUNDATION_TEST_KAFKA_REQUIRED
/// (a pass against no broker); its two siblings return `Err` without
/// FOUNDATION_PLATFORM_KAFKA_ENABLED; and `r2_smoke_contract`'s second
/// ignored test asserts on FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE,
/// which nothing in the repository exports. Only the first was silent, but
/// all three mean the lane's promised refusal could not fire — and r2 has
/// never run, so nothing would have told us.
#[test]
fn foundation_lanes_require_every_variable_their_targets_read() {
    let area = AREAS.iter().find(|area| area.slug == "foundation").unwrap();
    let lane = |name: &str| {
        area.live_lanes
            .iter()
            .find(|lane| lane.name == name)
            .unwrap_or_else(|| panic!("foundation must declare a {name} lane"))
    };

    assert_eq!(
        lane("kafka").required_env,
        &[
            "FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS",
            "FOUNDATION_TEST_KARAPACE_URL",
            "FOUNDATION_TEST_KAFKA_REQUIRED",
            "FOUNDATION_PLATFORM_KAFKA_ENABLED",
        ]
    );
    assert_eq!(
        lane("r2").required_env,
        &[
            "FOUNDATION_PLATFORM_R2_LIVE_SMOKE",
            "FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
            "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
            "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
            "FOUNDATION_PLATFORM_RUNTIME_ENV",
            "FOUNDATION_PLATFORM_EXECUTION_CONTEXT",
            "FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET",
            "FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM",
        ]
    );
}

/// `verify` is the DB-less run, so it must not switch a live suite on —
/// and it must learn which suites those are from the lane table, not from a
/// hand-maintained bool beside it.
///
/// `--all-features` enables `gongzzang-persistence/integration`, which
/// compiles the twenty live suites into a run that has no database; they
/// panic on the first connection. The old `two_stage_test: bool` encoded
/// the workaround as a separate fact that happened to agree with the lane.
/// Deriving it means the two can never disagree.
#[test]
fn a_feature_gated_suite_is_excluded_from_the_backendless_default_run() {
    let gongzzang = AREAS.iter().find(|area| area.slug == "gongzzang").unwrap();
    assert_eq!(
        verify_test_commands(gongzzang),
        vec![
            vec![
                "test",
                "--locked",
                "--workspace",
                "--all-features",
                "--exclude",
                "gongzzang-persistence"
            ],
            vec!["test", "--locked", "-p", "gongzzang-persistence"],
        ],
        "stage one keeps --all-features off the live package; stage two runs \
             that package with its backend-free defaults"
    );

    for slug in ["foundation", "identity", "intelligence"] {
        let area = AREAS.iter().find(|area| area.slug == slug).unwrap();
        assert_eq!(
            verify_test_commands(area),
            vec![vec!["test", "--locked", "--workspace", "--all-features"]],
            "{slug} gates with #[ignore], which --all-features cannot switch on"
        );
    }
}

/// A lane's `required_env` has to name what its targets actually read, or
/// the refusal it promises never fires.
///
/// identity's lane required `IDENTITY_TEST_DATABASE_URL`, which no test in
/// the repository reads, while `live_provisioning` reads
/// `IDENTITY_PROVISIONER_TEST_DATABASE_URL` and used to `return Ok(())` when
/// it was absent. So the lane started, ran a test that connected to nothing,
/// and reported a pass. `--ignored` did select that test, so the
/// zero-execution check cannot see this variant — only naming the variable
/// the target really needs closes it.
#[test]
fn identity_lane_requires_the_urls_its_targets_actually_read() {
    let area = AREAS.iter().find(|area| area.slug == "identity").unwrap();
    let lane = area
        .live_lanes
        .iter()
        .find(|lane| lane.name == "postgres")
        .expect("identity must declare a postgres live lane");

    assert_eq!(
        lane.required_env,
        &[
            "IDENTITY_ROLE_GRANT_TEST_DATABASE_URL",
            "IDENTITY_PROVISIONER_TEST_DATABASE_URL",
        ],
        "one entry per target, each the variable that target reads"
    );
}

/// libtest's own summary is the only evidence that anything ran.
///
/// cargo exits 0 when a filter matches nothing, so the exit code cannot
/// distinguish a verified target from one that was never selected. nextest
/// has `--no-tests=fail`; cargo test has no equivalent, so the count is read
/// back out of the output instead of inferred from success.
#[test]
fn executed_test_count_reads_what_libtest_actually_ran() {
    assert_eq!(
            executed_test_count(
                "running 12 tests\n\
                 test result: ok. 12 passed; 0 failed; 3 ignored; 0 measured; 1 filtered out; finished in 1.20s\n"
            ),
            12
        );
    // The shape this whole change exists to catch: a filter that selected
    // nothing, reported by cargo as a success.
    assert_eq!(
            executed_test_count(
                "running 0 tests\n\n\
                 test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 20 filtered out; finished in 0.00s\n"
            ),
            0
        );
    // No summary at all (a build that produced no test binary) is also zero,
    // never "assume it was fine".
    assert_eq!(executed_test_count("Compiling gongzzang-persistence\n"), 0);

    // The three variants of the summary line this parser must survive. Each
    // one is a real shape libtest or cargo emits, and each would silently
    // become 0 — i.e. a red lane on a run that was actually fine — if the
    // parser started depending on the wrong part of the line.
    //
    // (a) A failing run still reports how many executed. The count must come
    // from `N passed`, not from the `ok.` verdict, or a lane whose tests ran
    // and failed would be reported as "executed 0 tests" and mask the real
    // failure behind the wrong diagnosis.
    assert_eq!(
            executed_test_count(
                "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.41s\n"
            ),
            3
        );
    // (b) The trailing `; finished in …` is absent in some libtest paths.
    // Nothing may be anchored to the end of the line.
    assert_eq!(
        executed_test_count(
            "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"
        ),
        3
    );
    // (c) `CARGO_TERM_COLOR: always` is set in gongzzang-ci.yml and
    // foundation-ci.yml, so in CI the verdict arrives wrapped in SGR escapes
    // — the exact environment where a miscount would be most expensive. The
    // colour lands on `ok`/`FAILED`, never on the digits, which is why
    // reading the token before `passed` survives it.
    assert_eq!(
            executed_test_count(
                "test result: \x1b[32mok\x1b[0m. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.12s\n"
            ),
            7
        );
}

/// Executing nothing is a lane failure, not a lane pass.
///
/// Without this, every defect in this file's history stays invisible: the
/// original workspace sweep, the wrong gating flags, and any future filter
/// that silently stops matching. The lane's own report is what closes it.
#[test]
fn a_lane_target_that_executed_nothing_is_a_failure_not_a_pass() {
    let target = LaneTarget::Test {
        package: "gongzzang-persistence",
        test: "user_integration",
    };
    let zero = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s";

    let verdict = lane_target_verdict("gongzzang", "postgres", &target, zero);

    let message = verdict.expect_err("a target that ran nothing must not report success");
    assert!(message.contains("gongzzang-persistence"), "{message}");
    assert!(message.contains("user_integration"), "{message}");
    assert!(message.contains("0 tests"), "{message}");

    let ten = "test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.00s";
    assert_eq!(
        lane_target_verdict("gongzzang", "postgres", &target, ten),
        Ok(10)
    );
}

#[test]
fn foundation_python_plan_preserves_provider_and_discovers_spark_tests() {
    let area = AREAS.iter().find(|area| area.slug == "foundation").unwrap();
    let area_dir = Path::new("/repo/platforms/foundation-platform");

    let plans = python_test_plans(area, area_dir);

    assert_eq!(plans.len(), 7);
    assert_eq!(
        plans[0].current_dir,
        area_dir.join("services/foundation-provider-acquisition-worker")
    );
    assert_eq!(plans[0].python_path, Some("src"));
    assert_eq!(plans[0].args, &["-m", "pytest", "tests", "-q"]);
    assert_eq!(plans[1].current_dir, area_dir);
    assert_eq!(plans[1].python_path, None);
    assert_eq!(
        plans[1].args,
        &[
            "-m",
            "unittest",
            "discover",
            "-s",
            "infra/lakehouse/spark/tests",
            "-p",
            "test_*.py",
        ],
    );
    // The two suites ADR-0011 adopted. Naming their discovery roots here keeps
    // the assertion honest about *which* trees the harness took responsibility
    // for, not merely how many commands it emits.
    assert_eq!(
        plans[2].args,
        &[
            "-m",
            "unittest",
            "discover",
            "-s",
            "infra/lakehouse/dbt/tests",
            "-p",
            "test_*.py",
        ],
    );
    assert_eq!(
        plans[3].args,
        &[
            "-m",
            "unittest",
            "discover",
            "-s",
            "services/foundation-api/tests",
            "-p",
            "test_*.py",
        ],
    );
    assert_eq!(
        plans[4].args,
        &[
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests/static_release_toolchain",
            "-p",
            "test_*.py",
        ],
    );
    assert_eq!(
        plans[5].args,
        &[
            "-m",
            "unittest",
            "discover",
            "-s",
            "orchestration/tests",
            "-p",
            "test_*.py",
        ],
    );
    assert_eq!(
        plans[6].args,
        &[
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests/release_admission",
            "-p",
            "test_*.py",
        ],
    );
}

/// A suite that claims nothing cannot be checked for completeness, and an
/// unchecked claim is how four Python files ran nowhere (ADR-0011). The
/// shell guard proves the claims cover every discovered file; this proves
/// no suite ships without making one.
#[test]
fn every_python_suite_declares_the_roots_it_covers() {
    for area in AREAS {
        for suite in area.python_tests {
            assert!(
                !suite.covers.is_empty(),
                "{} python suite '{}' declares no covers root",
                area.slug,
                suite.dir
            );
            for root in suite.covers {
                assert!(
                    !root.starts_with('/') && !root.ends_with('/'),
                    "{} python suite '{}' covers root {root:?} must be a \
                         bare area-relative path; the guard matches on \"/<root>/\"",
                    area.slug,
                    suite.dir
                );
            }
        }
    }
}

/// `covers` states what a suite is responsible for; `args` states what it
/// runs. Joining `dir` with each argument and normalising away `.` lets the
/// two declaration shapes in use — a suite rooted in its own subdirectory
/// with relative arguments, and one rooted at the area with area-relative
/// arguments — be compared as the same path.
fn suite_runs_covered_root(suite: &PythonTests, root: &str) -> bool {
    let normalise = |value: &Path| -> PathBuf {
        value
            .components()
            .filter(|component| !matches!(component, std::path::Component::CurDir))
            .collect()
    };
    let target = normalise(Path::new(root));
    suite
        .args
        .iter()
        .any(|argument| normalise(&Path::new(suite.dir).join(argument)) == target)
}

/// The claim and the command must name the same tree. Without this a suite
/// could declare responsibility for one directory and run another: the
/// completeness guard would report every file claimed, the runner would exit
/// 0, and the claimed tree would still run nowhere. That is the exact shape
/// this ADR exists to remove, one level in.
#[test]
fn every_python_suite_runs_the_roots_it_claims() {
    for area in AREAS {
        for suite in area.python_tests {
            for root in suite.covers {
                assert!(
                    suite_runs_covered_root(suite, root),
                    "{} python suite '{}' claims {root:?} but its arguments {:?} \
                         do not name that tree",
                    area.slug,
                    suite.dir,
                    suite.args
                );
            }
        }
    }
}

/// The runners disagree with each other about zero tests, and one of them
/// disagrees with itself across versions: `unittest discover` matching
/// nothing exits 5 on Python 3.13 and 0 on the 3.11.2 in the pinned image.
/// Reading the count makes the verdict the same everywhere.
#[test]
fn executed_python_test_count_reads_both_runner_summaries() {
    assert_eq!(
        executed_python_test_count("Ran 17 tests in 0.009s\n\nOK\n"),
        17
    );
    assert_eq!(
        executed_python_test_count("16 passed, 1 skipped in 0.12s\n"),
        16
    );
    // The shape this exists to catch: nothing selected, runner content.
    assert_eq!(
        executed_python_test_count("Ran 0 tests in 0.000s\n\nOK\n"),
        0
    );
    // No summary at all is zero, never "assume it was fine".
    assert_eq!(executed_python_test_count("collecting ...\n"), 0);
}

#[test]
fn foundation_node_suite_plan_is_owned_by_verify_ssot() {
    let foundation = AREAS.iter().find(|area| area.slug == "foundation").unwrap();
    let plans = node_test_plans(foundation, Path::new("platforms/foundation-platform"));

    assert_eq!(plans.len(), 4);
    assert_eq!(
        plans[0].current_dir,
        PathBuf::from("platforms/foundation-platform/services/foundation-profile-gateway")
    );
    assert_eq!(
        plans[1].current_dir,
        PathBuf::from("platforms/foundation-platform/services/foundation-by-pnu-gateway")
    );
    assert_eq!(
        plans[2].current_dir,
        PathBuf::from("platforms/foundation-platform/services/foundation-tile-gateway")
    );
    assert_eq!(
        plans[3].current_dir,
        PathBuf::from("platforms/foundation-platform/services/foundation-map-edit-gateway")
    );
    for plan in &plans {
        assert_eq!(
            plan.scripts,
            &["config:check", "typecheck", "test", "build:check"]
        );
        assert_eq!(plan.test_script, "test");
    }
}

#[test]
fn executed_vitest_count_fails_closed_on_zero_or_missing_summary() {
    assert_eq!(executed_vitest_test_count("Tests  24 passed (24)\n"), 24);
    assert_eq!(executed_vitest_test_count("Tests  no tests\n"), 0);
    assert_eq!(executed_vitest_test_count("Test Files  1 passed (1)\n"), 0);
}

#[test]
fn suite_root_matching_normalises_both_declaration_shapes() {
    // Rooted in its own subdirectory, arguments relative to it.
    let nested = PythonTests {
        dir: "services/worker",
        python_path: Some("src"),
        args: &["-m", "pytest", "tests", "-q"],
        covers: &["services/worker/tests"],
    };
    assert!(suite_runs_covered_root(&nested, "services/worker/tests"));
    assert!(!suite_runs_covered_root(&nested, "services/worker/other"));

    // Rooted at the area, arguments already area-relative.
    let flat = PythonTests {
        dir: ".",
        python_path: None,
        args: &[
            "-m",
            "unittest",
            "discover",
            "-s",
            "infra/checks",
            "-p",
            "test_*.py",
        ],
        covers: &["infra/checks"],
    };
    assert!(suite_runs_covered_root(&flat, "infra/checks"));
    assert!(!suite_runs_covered_root(&flat, "infra/other"));
}
