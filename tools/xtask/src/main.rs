//! perfectory monorepo verification SSOT (ADR-0004).
//!
//! ONE definition of "how each area is verified", called by BOTH the local harness
//! (`scripts/verify/cargo-verify.sh`, inside Docker) and CI (`.github/workflows/
//! *-ci.yml` rust jobs). Drift between local and CI is structurally impossible:
//! there is a single source. Dependency-free (std only).
//!
//! Usage (from the repository root):
//!   cargo xtask verify <area|all>   # area slugs: tools/areas.contract.json, plus `tooling`
//!   cargo xtask verify <area> --affected   # a pull request: only the crates it reaches (ADR-0269)
//!   cargo xtask docs   # monorepo-wide lychee internal-link check (offline)
//!   cargo xtask npm <command>   # npm overrides, lockfiles and advisory patches (ADR-0158; src/npm.rs)
//!   cargo xtask ci <command>    # CI steps: install-cargo-deny, osv-gate, postgres-production-shape (src/ci.rs)

use std::path::{Path, PathBuf};
use std::process::{exit, Command};

mod cargo_verify;
mod ci;
mod live;
mod npm;
mod process;
mod python_node;
mod selection;

use cargo_verify::*;
use process::*;
use python_node::*;
use selection::*;

/// One monorepo area and its verification requirements.
struct Area {
    /// Short name used on the CLI.
    slug: &'static str,
    /// Path from the repository root.
    dir: &'static str,
    /// Debian packages needed to build native dependencies before verification
    /// (e.g. rdkafka's C library needs cmake + libsasl2). Empty for pure-Rust areas.
    apt_deps: &'static [&'static str],
    /// Non-Rust tests owned by this area and required by the same authoritative
    /// verification entrypoint. Empty means the area has no such suite.
    python_tests: &'static [PythonTests],
    /// Node package suites owned by this area and run by the same verification SSOT.
    node_tests: &'static [NodeTests],
    /// Live-resource lanes, each naming the exact targets that need that backend.
    ///
    /// This replaced an `Option<Integration>` whose `None` was an escape hatch:
    /// three of four areas carried it, so their live suites ran nowhere while CI
    /// stayed green. `scripts/guard/live-lane-completeness.sh` now proves every
    /// backend-gated target belongs to a lane, so an empty list is a checkable
    /// claim rather than an assumed one.
    live_lanes: &'static [LiveLane],
}

/// One external resource and the exact test targets that need it.
///
/// The existing `Integration` runs `--workspace --all-features -- --ignored`,
/// which sweeps up every ignored test regardless of what it depends on. In the
/// Postgres job that means the Kafka/R2/lakehouse/public-API tests also run,
/// find no such backend, take their "resource absent" branch, and are counted as
/// passes. A lane fixes that by naming its targets instead of sweeping: a test is
/// only ever run by the lane that provisions what it needs.
///
/// Targets are enumerated positively rather than excluded by name. An exclusion
/// list silently widens when someone adds a test; a positive list plus a
/// completeness guard turns that same omission into a failure.
struct LiveLane {
    /// Lane selector: `cargo xtask integration <area> <lane>`.
    name: &'static str,
    /// Env var(s) the provisioner must set. xtask refuses to run the lane
    /// without them, so a lane can never "pass" against a missing backend.
    required_env: &'static [&'static str],
    /// How this lane's targets keep out of the default run — which decides the
    /// flags that select them back in.
    gating: LaneGating,
    /// The exact test targets this lane owns.
    targets: &'static [LaneTarget],
}

/// The two ways a target stays out of `cargo test`, which need OPPOSITE flags.
///
/// The first lane table assumed one style for all four areas and appended
/// `-- --ignored` everywhere. gongzzang gates the other way, so its lane asked
/// cargo to run the ignored tests of a suite that had been compiled out of
/// existence: no `--features integration` means the file is empty, `--ignored`
/// then matches nothing, and cargo exits 0. Twenty tests reported as verified
/// without a line of them being built. Making the style a field means the flags
/// follow from it instead of from an assumption.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LaneGating {
    /// `#[ignore]` — always compiled, skipped by default, selected by
    /// `-- --ignored`. Used by foundation, identity and intelligence.
    Ignored,
    /// `#![cfg(feature = "…")]` — not compiled at all without the feature, and
    /// nothing inside is `#[ignore]`, so `--features <f>` selects it and
    /// `--ignored` would deselect everything. Used by gongzzang.
    Feature(&'static str),
}

/// A single cargo test target, addressed the way `cargo xtask live kafka`
/// already addresses it: `-p <package> --test <test>`.
enum LaneTarget {
    Test {
        package: &'static str,
        test: &'static str,
    },
    Binary {
        package: &'static str,
        binary: &'static str,
        filter: &'static str,
    },
}

impl LaneTarget {
    const fn package(&self) -> &'static str {
        match self {
            Self::Test { package, .. } | Self::Binary { package, .. } => package,
        }
    }

    const fn selection(&self) -> (&'static str, &'static str) {
        match self {
            Self::Test { test, .. } => ("--test", test),
            Self::Binary { binary, .. } => ("--bin", binary),
        }
    }
}

/// A test-shaped npm script that no workflow invokes ON PURPOSE.
///
/// `LiveLane.required_env` lets a Rust suite say "I need a backend CI does not
/// have" instead of quietly not running. TypeScript needed the same sentence.
/// Without it the completeness check has only two answers — invoked, or a
/// failure — and a script that legitimately cannot run on a GitHub-hosted runner
/// would force either a red CI or an exception list. Exception lists widen in
/// silence; a declaration carrying its own reason does not.
///
/// `requires` is prose on purpose: it is read by a human deciding whether the
/// constraint still holds, and its presence is what makes adding an entry a
/// visible, arguable change rather than a one-word suppression.
struct LocalOnlyScript {
    /// Repository-relative path of the declaring `package.json`.
    manifest: &'static str,
    /// The `scripts` key.
    script: &'static str,
    /// The resource a GitHub-hosted runner cannot provide.
    requires: &'static str,
}

/// Scripts deliberately outside CI, with the resource that keeps them out.
///
/// `scripts/guard/live-lane-completeness.sh` accepts a test-shaped script when a
/// workflow invokes it OR it appears here. Anything else fails.
const LOCAL_ONLY_SCRIPTS: &[LocalOnlyScript] = &[LocalOnlyScript {
    manifest: "products/gongzzang/apps/web/package.json",
    script: "probe:naver",
    // The vector-source reload probe launches Chromium with `--enable-gpu
    // --disable-software-rasterizer` (tests/probes/naver-sdk.probe.ts) and drives
    // the real Naver SDK. A GitHub-hosted runner has no GPU, so the probe is a
    // local capability check whose evidence is attached to the run, not a gate.
    requires: "hardware GL and the live Naver Maps SDK",
}];

/// The cargo invocation for each target in a lane — one per target, mirroring
/// the loop `cargo xtask live kafka` already runs.
///
/// The gating style, not a fixed suffix, decides the selecting flags:
/// `Ignored` needs `-- --ignored`, `Feature(f)` needs `--features f` and must
/// NOT pass `--ignored` (its tests are not ignored — the filter would select
/// nothing and cargo would still exit 0).
fn lane_commands(lane: &LiveLane) -> Vec<Vec<String>> {
    lane.targets
        .iter()
        .map(|target| {
            let mut command = vec!["test".to_owned(), "--locked".to_owned()];
            if let LaneGating::Feature(feature) = lane.gating {
                command.push("--features".to_owned());
                command.push(feature.to_owned());
            }
            command.extend(
                [
                    "-p",
                    target.package(),
                    target.selection().0,
                    target.selection().1,
                    "--",
                ]
                .iter()
                .map(|arg| (*arg).to_owned()),
            );
            if lane.gating == LaneGating::Ignored {
                command.push("--ignored".to_owned());
            }
            if let LaneTarget::Binary { filter, .. } = target {
                command.extend(["--exact".to_owned(), (*filter).to_owned()]);
            }
            // Serial: these suites share one database and truncate it between
            // cases, so parallel targets would tear each other's fixtures down.
            command.push("--test-threads=1".to_owned());
            command
        })
        .collect()
}

/// Packages a DB-less `verify` must not switch on, read straight off the lane
/// table: exactly the packages some lane gates behind a cargo feature.
///
/// `--all-features` would enable that feature, compiling a live suite into a run
/// that has no backend to talk to. This used to be a `two_stage_test: bool` on
/// the area — a second statement of the same fact, kept in agreement by hand.
/// Derived, the two cannot disagree: adding a `Feature`-gated lane excludes its
/// package automatically, and removing one stops excluding it.
fn feature_gated_packages(area: &Area) -> Vec<&'static str> {
    let mut packages: Vec<&'static str> = area
        .live_lanes
        .iter()
        .filter(|lane| matches!(lane.gating, LaneGating::Feature(_)))
        .flat_map(|lane| lane.targets.iter().map(LaneTarget::package))
        .collect();
    packages.sort_unstable();
    packages.dedup();
    packages
}

/// The `cargo test` invocations `verify` runs for an area.
///
/// One command for an area with no feature-gated suite. Two stages when there
/// is one: the workspace with `--all-features` minus those packages, then those
/// packages on their default (backend-free) features, so their non-live tests
/// still run.
fn verify_test_commands(area: &Area) -> Vec<Vec<&'static str>> {
    let gated = feature_gated_packages(area);
    if gated.is_empty() {
        return vec![vec!["test", "--locked", "--workspace", "--all-features"]];
    }
    let mut stage_one = vec!["test", "--locked", "--workspace", "--all-features"];
    for package in &gated {
        stage_one.push("--exclude");
        stage_one.push(package);
    }
    let mut commands = vec![stage_one];
    commands.extend(
        gated
            .iter()
            .map(|package| vec!["test", "--locked", "-p", package]),
    );
    commands
}

/// How many tests libtest reports as executed, summed over every
/// `test result: ok. N passed; …` summary in a captured run.
///
/// `cargo test` exits 0 when a filter selects nothing, so its exit code cannot
/// tell a verified target from one that was never selected — the confusion this
/// harness exists to remove, and precisely how twenty gongzzang tests were
/// reported green while being compiled out of existence. cargo has no
/// `--no-tests=fail` (nextest's flag for this), so the count is read back out
/// of the output rather than inferred from success.
fn executed_test_count(output: &str) -> usize {
    output
        .lines()
        .filter(|line| line.trim_start().starts_with("test result:"))
        .filter_map(|line| {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            let passed = tokens
                .iter()
                .position(|token| token.trim_end_matches(';') == "passed")?;
            tokens.get(passed.checked_sub(1)?)?.parse::<usize>().ok()
        })
        .sum()
}

/// A lane target that executed nothing verified nothing, whatever cargo's exit
/// code says. This is the backstop that keeps the *next* filter mistake from
/// hiding the way this one did.
fn lane_target_verdict(
    area: &str,
    lane: &str,
    target: &LaneTarget,
    output: &str,
) -> Result<usize, String> {
    match executed_test_count(output) {
        0 => Err(format!(
            "{area} lane '{lane}': `-p {} {} {}` executed 0 tests. cargo \
             exits 0 when a filter matches nothing, so this would have been \
             recorded as a pass. Check the lane's gating — `#[ignore]` targets \
             need `-- --ignored`, `#![cfg(feature = \"…\")]` targets need \
             `--features …` and must not be filtered on `--ignored`.",
            target.package(),
            target.selection().0,
            target.selection().1
        )),
        count => Ok(count),
    }
}

struct PythonTests {
    /// Working directory relative to the area root.
    dir: &'static str,
    /// Optional Python module search path relative to `dir`.
    python_path: Option<&'static str>,
    /// Arguments following `python3`.
    args: &'static [&'static str],
    /// Discovery roots (area-relative) this suite is responsible for.
    ///
    /// `dir`/`args` say HOW the suite runs; `covers` says WHAT it claims. Without
    /// the second statement there is nothing to check the first against, and that
    /// silence is where four Python files (26 tests) sat unrun while CI stayed
    /// green — one of them asserting `assertIn(X)` and `assertNotIn(S)` with
    /// `S ⊂ X`, so it could never have passed had it ever run (ADR-0011).
    ///
    /// `scripts/guard/live-lane-completeness.sh` proves every discovered
    /// `test_*.py`/`*_test.py` under `platforms`/`products` sits beneath exactly
    /// one of these roots. Zero means the file runs nowhere; more than one means
    /// ownership is ambiguous.
    covers: &'static [&'static str],
}

struct NodeTests {
    /// Working directory relative to the area root and containing the lockfile.
    dir: &'static str,
    /// Package scripts that collectively define verification for this package.
    scripts: &'static [&'static str],
    /// Script whose Vitest summary must prove that at least one test executed.
    test_script: &'static str,
}

#[derive(Debug, PartialEq, Eq)]
struct PythonCommandPlan {
    current_dir: PathBuf,
    python_path: Option<&'static str>,
    args: &'static [&'static str],
}

#[derive(Debug, PartialEq, Eq)]
struct NodeCommandPlan {
    current_dir: PathBuf,
    scripts: &'static [&'static str],
    test_script: &'static str,
}

const AREAS: &[Area] = &[
    Area {
        slug: "gongzzang",
        dir: "products/gongzzang",
        apt_deps: &[],
        python_tests: &[],
        node_tests: &[],

        // Gated by `#![cfg(feature = "integration")]` rather than `#[ignore]`.
        // `verify` reads that gating back off this table to keep the package out
        // of its `--all-features` run (`feature_gated_packages`), so the fact is
        // stated once here instead of twice.
        live_lanes: &[LiveLane {
            name: "postgres",
            required_env: &["DATABASE_URL"],
            gating: LaneGating::Feature("integration"),
            targets: &[
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "admin_action_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "analysis_report_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "audit_log_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "bookmark_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "business_verification_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "error_map_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "featured_content_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "foundation_anchor_import_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "foundation_anchor_visibility_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "listing_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "listing_marker_tile_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "listing_photo_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "listing_report_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "listing_review_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "notification_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "outbox_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "outbox_publisher_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "search_history_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "system_alert_integration",
                },
                LaneTarget::Test {
                    package: "gongzzang-persistence",
                    test: "user_integration",
                },
            ],
        }],
    },
    Area {
        slug: "foundation",
        dir: "platforms/foundation-platform",
        // The complete workspace/all-features build includes aws-lc-sys and
        // vendored librdkafka. Keep the native headers here (rather than relying
        // on a developer image) so CI and the Docker verification harness share
        // the same reproducible prerequisite contract.
        apt_deps: &[
            "cmake",
            "python3",
            "python3-pytest",
            "libssl-dev",
            "libsasl2-dev",
            "libcurl4-openssl-dev",
            "zlib1g-dev",
        ],
        python_tests: &[
            PythonTests {
                dir: "services/foundation-provider-acquisition-worker",
                python_path: Some("src"),
                args: &["-m", "pytest", "tests", "-q"],
                covers: &["services/foundation-provider-acquisition-worker/tests"],
            },
            PythonTests {
                dir: ".",
                python_path: None,
                args: &[
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "infra/lakehouse/spark/tests",
                    "-p",
                    "test_*.py",
                ],
                covers: &["infra/lakehouse/spark/tests"],
            },
            // The two suites below belonged to no runner until ADR-0011. The dbt
            // contract tests had never executed at all: not in a workflow, not in
            // `lefthook.yml`, not here. Twenty-five of their twenty-six assertions
            // pass; the twenty-sixth cannot, and nobody could have known.
            PythonTests {
                dir: ".",
                python_path: None,
                args: &[
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "infra/lakehouse/dbt/tests",
                    "-p",
                    "test_*.py",
                ],
                covers: &["infra/lakehouse/dbt/tests"],
            },
            PythonTests {
                dir: ".",
                python_path: None,
                args: &[
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "services/foundation-api/tests",
                    "-p",
                    "test_*.py",
                ],
                covers: &["services/foundation-api/tests"],
            },
            PythonTests {
                dir: ".",
                python_path: None,
                args: &[
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "tests/static_release_toolchain",
                    "-p",
                    "test_*.py",
                ],
                covers: &["tests/static_release_toolchain"],
            },
            // The scheduled-job list Airflow builds its DAGs from (root ADR-0122).
            PythonTests {
                dir: ".",
                python_path: None,
                args: &[
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "orchestration/tests",
                    "-p",
                    "test_*.py",
                ],
                covers: &["orchestration/tests"],
            },
            PythonTests {
                dir: ".",
                python_path: None,
                args: &[
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "tests/release_admission",
                    "-p",
                    "test_*.py",
                ],
                covers: &["tests/release_admission"],
            },
        ],
        node_tests: &[
            NodeTests {
                dir: "services/foundation-profile-gateway",
                scripts: &["config:check", "typecheck", "test", "build:check"],
                test_script: "test",
            },
            NodeTests {
                dir: "services/foundation-by-pnu-gateway",
                scripts: &["config:check", "typecheck", "test", "build:check"],
                test_script: "test",
            },
            NodeTests {
                dir: "services/foundation-tile-gateway",
                scripts: &["config:check", "typecheck", "test", "build:check"],
                test_script: "test",
            },
            NodeTests {
                dir: "services/foundation-map-edit-gateway",
                scripts: &["config:check", "typecheck", "test", "build:check"],
                test_script: "test",
            },
        ],
        // Foundation's DB-backed reads tests (catalog_*_reads, …) are `#[ignore]`
        // and need a migrated + seeded Postgres. scripts/verify/integration.sh
        // provisions one locally; CI's postgres-integration job provides its own.
        // The four backends that the Postgres sweep used to run against nothing.
        // `cargo xtask live kafka` already provisions the Kafka stack and runs
        // exactly these three targets; naming them here makes that grouping the
        // harness's own definition rather than a duplicate list inside a script.
        live_lanes: &[
            // Enumerated rather than "everything ignored minus the others": an
            // exclusion list widens silently when a target is added, a positive
            // list plus the completeness guard turns that omission into a failure.
            // Still additive today — `integration` keeps its workspace sweep until
            // that guard lands, because dropping the sweep first would make an
            // unlisted target silently stop running.
            LiveLane {
                name: "postgres",
                required_env: &["DATABASE_URL"],
                gating: LaneGating::Ignored,
                targets: &[
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "administrative_boundary_identity",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "catalog_round_trip",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "catalog_ssot_reads",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "industrial_complex_search",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "industrial_complex_transaction_participant",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "marker_tile_reads",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "operator_action_ledger",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "parcel_complex_membership",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "parcel_marker_anchor_rebuild",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "spatial_tile_publication",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "vector_tile_manifest_promote",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "vector_tile_manifest_reads",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "vector_tile_manifest_rollback",
                    },
                    LaneTarget::Test {
                        package: "catalog-infrastructure",
                        test: "vector_tile_runtime_manifest_promote",
                    },
                    LaneTarget::Test {
                        package: "collection-infrastructure",
                        test: "bronze_catalog_recovery_atomicity",
                    },
                    LaneTarget::Test {
                        package: "collection-infrastructure",
                        test: "bronze_ingest_round_trip",
                    },
                    LaneTarget::Test {
                        package: "foundation-api",
                        test: "schema_readiness_probe",
                    },
                    LaneTarget::Test {
                        package: "foundation-normalization-infrastructure",
                        test: "active_override_reader",
                    },
                    LaneTarget::Test {
                        package: "foundation-normalization-infrastructure",
                        test: "building_register_unit_transactions",
                    },
                    LaneTarget::Test {
                        package: "foundation-normalization-infrastructure",
                        test: "industrial_complex_ledger_integrity",
                    },
                    LaneTarget::Test {
                        package: "foundation-normalization-infrastructure",
                        test: "normalization_application_roundtrip",
                    },
                    LaneTarget::Test {
                        package: "foundation-normalization-infrastructure",
                        test: "normalization_atomicity",
                    },
                    LaneTarget::Test {
                        package: "foundation-normalization-infrastructure",
                        test: "normalization_proposal_roundtrip",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox",
                        test: "postgres_jobbus",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox",
                        test: "publish_roundtrip",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox-publisher",
                        test: "administrative_boundary_publication",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox-publisher",
                        test: "industrial_complex_boundary_publication",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox-publisher",
                        test: "parcel_boundary_publication",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox-publisher",
                        test: "parcel_publication_source_evidence",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox-publisher",
                        test: "unit_official_price_publication",
                    },
                    LaneTarget::Binary {
                        package: "foundation-outbox-publisher",
                        binary: "foundation-outbox-publisher",
                        filter: "postgis_parcel_boundary_mirror_national_rebuild::tests::rebuild_records_one_provenance_pair_on_the_run_and_every_loaded_row",
                    },
                    LaneTarget::Test {
                        package: "lakehouse-infrastructure",
                        test: "gold_publication_atomicity",
                    },
                    LaneTarget::Test {
                        package: "lakehouse-infrastructure",
                        test: "lakehouse_batch_run_audit",
                    },
                    LaneTarget::Test {
                        package: "lakehouse-infrastructure",
                        test: "lakehouse_registry_atomicity",
                    },
                    LaneTarget::Test {
                        package: "lakehouse-infrastructure",
                        test: "lakehouse_registry_repository",
                    },
                    LaneTarget::Test {
                        package: "stewardship-infrastructure",
                        test: "steward_decisions",
                    },
                ],
            },
            LiveLane {
                name: "kafka",
                // The third is not a connection string but a mode switch that
                // all three targets read internally; live_kafka_outage used to
                // return Ok(()) without it, so the lane could pass having
                // reached no broker. required_env names what the targets read,
                // not only what they connect to.
                required_env: &[
                    "FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS",
                    "FOUNDATION_TEST_KARAPACE_URL",
                    "FOUNDATION_TEST_KAFKA_REQUIRED",
                    "FOUNDATION_PLATFORM_KAFKA_ENABLED",
                ],
                gating: LaneGating::Ignored,
                targets: &[
                    LaneTarget::Test {
                        package: "foundation-outbox",
                        test: "live_kafka_karapace",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox",
                        test: "live_kafka_outbox_roundtrip",
                    },
                    LaneTarget::Test {
                        package: "foundation-outbox",
                        test: "live_kafka_outage",
                    },
                ],
            },
            LiveLane {
                name: "r2",
                // Both ignored tests in r2_smoke_contract are selected by
                // `--ignored`, and the second asserts on the INVENTORY switch,
                // which nothing in the repository exports. Naming it here turns
                // "the lane cannot work" from a surprise into a refusal.
                required_env: &[
                    "FOUNDATION_PLATFORM_R2_LIVE_SMOKE",
                    "FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE",
                    "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
                    "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
                    "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
                    "FOUNDATION_PLATFORM_RUNTIME_ENV",
                    "FOUNDATION_PLATFORM_EXECUTION_CONTEXT",
                    "FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET",
                    "FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM",
                ],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget::Test {
                    package: "foundation-outbox",
                    test: "r2_smoke_contract",
                }],
            },
            LiveLane {
                name: "lakehouse",
                required_env: &[
                    "FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE",
                    "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
                    "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
                    "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
                    "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN",
                ],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget::Test {
                    package: "lakehouse-infrastructure",
                    test: "lakehouse_live_smoke",
                }],
            },
            // Diagnostic, not a gate. The ten building-register data.go.kr endpoints in
            // `public-source-endpoint-catalog.v1.json` carry `source_acquisition_lane:
            // disabled_api_duplicate`: the building register comes from the hub.go.kr bulk files.
            // Real transactions come from their own collector (root ADR-0348), not from this lane.
            // This lane therefore proves the provider still answers, which is worth knowing and is
            // not worth blocking on.
            //
            // Treating it as a lane to "turn on" wastes a real service key on a path the platform
            // has decided not to use, and the provider is intermittently down besides — three
            // consecutive probes returned 200, 503 SERVICETIMEOUT_ERROR, 200. Run it deliberately
            // when the question is "does the API still work", never as a precondition for release.
            LiveLane {
                name: "data-go-kr",
                required_env: &[
                    "FOUNDATION_PLATFORM_DATA_GO_KR_LIVE_SMOKE",
                    "DATA_GO_KR_SERVICE_KEY",
                ],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget::Test {
                    package: "collection-infrastructure",
                    test: "data_go_kr_bld_rgst_live_smoke",
                }],
            },
        ],
    },
    Area {
        slug: "identity",
        dir: "platforms/identity-platform",
        apt_deps: &[],
        python_tests: &[],
        node_tests: &[],

        // identity-ci.yml runs these two through a raw `cargo test --ignored`
        // written straight into the workflow, so they exist in CI but cannot be
        // reproduced by any local xtask command. Naming them here is the first
        // half of removing that raw invocation.
        //
        // One variable per target, each the one that target actually reads. The
        // first table required IDENTITY_TEST_DATABASE_URL — which no test in the
        // repository has ever read — and omitted the provisioner URL that
        // `live_provisioning` does read, so the lane's promised refusal could
        // not fire for the target that needed it most.
        live_lanes: &[LiveLane {
            name: "postgres",
            required_env: &[
                "IDENTITY_ROLE_GRANT_TEST_DATABASE_URL",
                "IDENTITY_PROVISIONER_TEST_DATABASE_URL",
            ],
            gating: LaneGating::Ignored,
            targets: &[
                LaneTarget::Test {
                    package: "authorization-infrastructure",
                    test: "role_grant_postgres",
                },
                LaneTarget::Test {
                    package: "identity-service-provisioner",
                    test: "live_provisioning",
                },
            ],
        }],
    },
    Area {
        slug: "intelligence",
        dir: "platforms/intelligence-platform",
        // rdkafka-sys builds the vendored librdkafka from source under
        // `--all-features`, which links every optional transport. This is the
        // COMPLETE external -dev header set that build needs — declared here so a
        // clean CI runner reproduces what the fat local `rust:*-bookworm` image
        // happens to already carry (that gap is why `curl/curl.h not found` only
        // ever bit CI). cmake = build driver; libssl = SSL; libsasl2 = GSSAPI/SASL;
        // libcurl = OAUTHBEARER/OIDC; zlib = gzip. (zstd/lz4 are bundled by librdkafka.)
        apt_deps: &[
            "cmake",
            "libssl-dev",
            "libsasl2-dev",
            "libcurl4-openssl-dev",
            "zlib1g-dev",
        ],
        python_tests: &[],
        node_tests: &[],

        live_lanes: &[
            LiveLane {
                name: "kafka",
                required_env: &[
                    "INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS",
                    "INTELLIGENCE_TEST_KARAPACE_URL",
                ],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget::Test {
                    package: "messaging-infrastructure",
                    test: "live_kafka_karapace",
                }],
            },
            LiveLane {
                name: "redis",
                required_env: &["INTELLIGENCE_REDIS_LIVE_TEST_URL"],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget::Test {
                    package: "intelligence-normalization-infrastructure",
                    test: "redis_rate_limit_live",
                }],
            },
            LiveLane {
                name: "postgres",
                required_env: &["INTELLIGENCE_TEST_DATABASE_URL"],
                gating: LaneGating::Ignored,
                targets: &[
                    LaneTarget::Test {
                        package: "knowledge-infrastructure",
                        test: "knowledge_index_contract",
                    },
                    LaneTarget::Test {
                        package: "knowledge-infrastructure",
                        test: "knowledge_source_registry_contract",
                    },
                    LaneTarget::Test {
                        package: "intelligence-normalization-infrastructure",
                        test: "workflow_state_contract_suite",
                    },
                ],
            },
        ],
    },
    // The harness itself. Five Cargo workspaces exist in this repository and this
    // was the one no area declared, so `verify` never entered it and its ten unit
    // tests ran nowhere — among them the tests proving that a lane which selects
    // zero tests is a failure, and that the libtest summary parser reads what
    // actually ran. The mechanism guaranteeing every other suite runs was the one
    // suite nothing guaranteed (ADR-0011).
    //
    // ADR-0010 already found the neighbouring half of this: an xtask change
    // retriggered no area CI, fixed by `scripts/guard/xtask-path-coverage.sh`.
    // That made the workflows *rerun* on an xtask change; it did not make any of
    // them *test* xtask. A path filter is not coverage.
    // The staff console (root ADR-0114, ADR-0116): a pure-Rust BFF with no backend-gated tests —
    // its sign-in suite brings its own fake issuer and fake Foundation — and the screens, whose API
    // types must equal the committed Foundation OpenAPI document (`api:check`).
    Area {
        slug: "dawneer",
        dir: "consoles/dawneer",
        apt_deps: &[],
        python_tests: &[],
        node_tests: &[NodeTests {
            dir: "web",
            scripts: &["typecheck", "test", "build", "api:check"],
            test_script: "test",
        }],
        live_lanes: &[],
    },
    Area {
        slug: "tooling",
        dir: "tools/xtask",
        apt_deps: &[],
        python_tests: &[],
        node_tests: &[],
        live_lanes: &[],
    },
];

fn main() {
    let github_actions = std::env::var("GITHUB_ACTIONS").ok();
    let rust_setup = std::env::var("PERFECTORY_RUST_SETUP").ok();
    if ci_rust_setup_missing(github_actions.as_deref(), rust_setup.as_deref()) {
        fail_usage(
            "this GitHub Actions job did not run `bash scripts/ci/rust-setup.sh` first; every CI              job that runs cargo goes through that one setup (root ADR-0155)",
        );
    }
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    // A pull request verifies only the crates its change reaches (root ADR-0269).
    let affected = args.iter().any(|arg| arg == "--affected");
    args.retain(|arg| arg != "--affected");
    match args.first().map(String::as_str) {
        Some("verify") => {
            // One log for the whole gate. A failing repository guard used to end the run before any
            // area was verified, and under `verify all` the first failing area hid every area after
            // it — the same rule-1 defect one level up from the stages inside `verify`.
            let mut log = FailureLog::default();
            let part = verify_part(
                args.get(2).map(String::as_str),
                args.get(3).map(String::as_str),
            )
            .unwrap_or_else(|| {
                fail_usage(
                    "usage: cargo xtask verify <area|all> [--part cargo|suites] [--affected]",
                )
            });
            // The repository guard runs once per CI run, in `required/repository` (`verify tooling`),
            // and with `verify all`; never once per area (root ADR-0313: it ran six times per merge
            // group and took 9.3 of rust-quality's 10.7 minutes on a pull request that selected no crate).
            let target = args.get(1).map(String::as_str);
            if part != VerifyPart::Suites && matches!(target, Some("all" | "tooling" | "tools/xtask")) {
                repository_guard(&mut log);
            }
            match args.get(1).map(String::as_str) {
                Some("all") => {
                    for area in AREAS {
                        eprintln!("\n=== xtask verify {} ===", area.slug);
                        verify(area, part, affected, &mut log);
                    }
                }
                Some(name) => match AREAS.iter().find(|a| a.slug == name || a.dir == name) {
                    Some(area) => verify(area, part, affected, &mut log),
                    None => fail_usage(&format!(
                        "unknown area '{name}'. known: {}, all",
                        AREAS.iter().map(|a| a.slug).collect::<Vec<_>>().join(", ")
                    )),
                },
                None => fail_usage("missing area: cargo xtask verify <area|all>"),
            }
            log.finish("verify");
        }
        Some("integration") => {
            // One log across every area and lane, for the same reason `verify` keeps one: under
            // `integration all` the first failing area used to hide every area after it.
            let mut log = FailureLog::default();
            match args.get(1).map(String::as_str) {
                Some("all") => {
                    for area in AREAS.iter().filter(|a| !a.live_lanes.is_empty()) {
                        eprintln!("\n=== xtask integration {} ===", area.slug);
                        integration(area, affected, &mut log);
                    }
                }
                Some(name) => match AREAS.iter().find(|a| a.slug == name || a.dir == name) {
                    // A third argument selects one live-resource lane; without it the
                    // area's Postgres suite runs as before.
                    Some(area) => match args.get(2).map(String::as_str) {
                        Some(lane) => integration_lane(area, lane, affected, &mut log),
                        None => integration(area, affected, &mut log),
                    },
                    None => fail_usage(&format!(
                        "unknown area '{name}'. known: {}, all",
                        AREAS.iter().map(|a| a.slug).collect::<Vec<_>>().join(", ")
                    )),
                },
                None => fail_usage(
                    "missing area: cargo xtask integration <area|all> [lane] [--affected]",
                ),
            }
            log.finish("integration");
        }
        Some("docs") => docs(),
        Some("ci") => {
            if let Err(message) = ci::run(&repo_root(), &args[1..]) {
                eprintln!("xtask ci: {message}");
                exit(1);
            }
        }
        Some("live") => {
            if let Err(message) = live::run(&repo_root(), &args[1..]) {
                eprintln!("xtask live: {message}");
                exit(1);
            }
        }
        Some("npm") => {
            if let Err(message) = npm::run(&repo_root(), &args[1..]) {
                eprintln!("xtask npm: {message}");
                exit(1);
            }
        }
        _ => fail_usage("usage: cargo xtask <verify <area|all> | integration <area|all> | docs | live <kafka|r2-lakehouse> | npm <command> | ci <command>>"),
    }
}

/// Monorepo-wide documentation link check (Phase D recurrence gate).
///
/// Runs lychee in OFFLINE mode over every `**/*.md` in the repo, validating that
/// internal file links resolve. Config is the single SSOT at `<root>/lychee.toml`
/// (also consumed by `.github/workflows/docs.yml`). We shell out to the OFFICIAL
/// pinned `lycheeverse/lychee` Docker image so no host install of the Rust `lychee`
/// binary is required — Docker is already a repo dependency and this works
/// identically on Windows/macOS/Linux. The image's entrypoint IS `lychee`, so we
/// pass lychee arguments directly.
fn docs() {
    if !tool_exists("docker") {
        eprintln!(
            "xtask docs: Docker is required (the link check runs the official \
             lycheeverse/lychee image).\n\
             Install Docker Desktop / the Docker Engine and retry, or run lychee \
             directly against lychee.toml if you have it installed."
        );
        exit(1);
    }

    let root = repo_root();
    let lychee_image = container_image(&root, "LYCHEE_IMAGE");
    // Mount the repo read-only at /input; lychee reads config + files, writes nothing.
    // The container path must be the same for -v target and -w, and for --config.
    let mount = format!("{}:/input", root.display());
    let mut command = Command::new("docker");
    command.args([
        "run",
        "--rm",
        "-v",
        &mount,
        "-w",
        "/input",
        &lychee_image,
        "--config",
        "lychee.toml",
        // Offline: validate local file paths only; never touch the network.
        // Redundant with lychee.toml's `offline = true`, but explicit here so the
        // behaviour is obvious at the call site and independent of config drift.
        "--offline",
        // Non-interactive output for logs.
        "--no-progress",
        // Every Markdown file is an input; lychee.toml's exclude_path prunes
        // target/, node_modules/, and other generated or vendored trees.
        "./**/*.md",
    ]);
    run(&mut command);
}

/// Read one immutable image reference from the repository-wide image SSOT.
fn container_image(root: &Path, key: &str) -> String {
    let path = root.join("tools/container-images.env");
    let contents = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        eprintln!("xtask: cannot read {}: {error}", path.display());
        exit(1);
    });
    let prefix = format!("{key}=");
    contents
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            eprintln!("xtask: {key} is missing from {}", path.display());
            exit(1);
        })
}

/// Canonical verification for one area — the single policy (ADR-0004).
/// Which half of `verify` to run (root ADR-0199).
///
/// Run whole, the Python/Node suites share the machine with cargo. On a 4-vCPU CI runner that is
/// the CPU cargo needs: measured, the suites beside cargo stretched the test build from 143 to
/// 209 s and the verify step stayed at about 12 minutes. CI therefore runs the two halves as two
/// jobs on two runners; locally `verify <area>` still runs everything as one command (ADR-0004).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum VerifyPart {
    Whole,
    Cargo,
    Suites,
}

fn verify_part(flag: Option<&str>, value: Option<&str>) -> Option<VerifyPart> {
    match (flag, value) {
        (None, None) => Some(VerifyPart::Whole),
        (Some("--part"), Some("cargo")) => Some(VerifyPart::Cargo),
        (Some("--part"), Some("suites")) => Some(VerifyPart::Suites),
        _ => None,
    }
}

/// The crates to verify: all of them, or with `--affected` what the change scope selects. A
/// selector that cannot answer is a failure, never a quiet narrowing.
fn selection_for(area: &Area, affected: bool, log: &mut FailureLog) -> Option<Selection> {
    if !affected {
        return Some(Selection::All);
    }
    log.absorb(crate_selection(area))
}

fn verify(area: &Area, part: VerifyPart, affected: bool, log: &mut FailureLog) {
    let dir = repo_root().join(area.dir);
    if part == VerifyPart::Suites {
        ensure_apt(area.apt_deps);
        return verify_suites(area, &dir, affected, log);
    }
    let Some(selection) = selection_for(area, affected, log) else {
        return;
    };
    // The native headers are for the crates; a selection of none needs none (root ADR-0313: an
    // `apt-get` before the selection took 4m07s on a documentation-only pull request, #870).
    if part == VerifyPart::Whole || !selection.is_empty() {
        ensure_apt(area.apt_deps);
    }
    match part {
        VerifyPart::Cargo => return verify_cargo(area, &dir, &selection, log),
        VerifyPart::Suites => return verify_suites(area, &dir, affected, log),
        VerifyPart::Whole => {}
    }

    // The Python and Node suites build nothing cargo builds and read nothing it writes, so they run
    // on their own thread beside fmt, clippy and the Rust tests rather than after them (root
    // ADR-0199). Measured on Foundation CI (2026-10-12): cargo took about 7 minutes and the suites
    // behind it 6.5 more, one after the other. Each suite still prints its output as one block and
    // every failure from either side is in the one log the run is judged by.
    run_beside(
        log,
        &format!("{} python/node verification", area.slug),
        |log| verify_cargo(area, &dir, &selection, log),
        |log| verify_suites(area, &dir, affected, log),
    );
}

/// Run the fast repository-structure checks before any expensive area build.
/// This keeps publication, licensing, and workflow safety in the same authoritative
/// `cargo xtask verify <area>` entrypoint as compile/test policy.
/// A local-only declaration outlives the script it excuses unless something
/// checks. A stale entry is worse than no entry: it silently pre-approves a name
/// someone may reuse later for a script that genuinely belongs in CI.
fn validate_local_only_scripts() {
    let root = repo_root();
    for entry in LOCAL_ONLY_SCRIPTS {
        let manifest = root.join(entry.manifest);
        let Ok(text) = std::fs::read_to_string(&manifest) else {
            eprintln!(
                "xtask: FAILED: local-only declaration names {}, which cannot be read.",
                entry.manifest
            );
            exit(1);
        };
        if !text.contains(&format!("\"{}\"", entry.script)) {
            eprintln!(
                "xtask: FAILED: {} no longer declares a '{}' script. Remove the \
                 local-only entry rather than leaving it to pre-approve that name.",
                entry.manifest, entry.script
            );
            exit(1);
        }
        if entry.requires.trim().is_empty() {
            eprintln!(
                "xtask: FAILED: local-only script '{}' must state the resource that \
                 keeps it out of CI.",
                entry.script
            );
            exit(1);
        }
    }
}

fn repository_guard(log: &mut FailureLog) {
    validate_local_only_scripts();
    let root = repo_root();
    log.absorb(run_reporting(
        Command::new("bash")
            .current_dir(&root)
            .arg("scripts/guard/monorepo-guard.sh"),
    ));
}

/// Run an area's live-DB integration tests against an ALREADY-provisioned database
/// — the ADR-0004 SSOT for the *command*, so it can't drift across CI and local.
/// `verify` (offline, DB-less) skips these; the DB is supplied by the caller via
/// `url_vars` (CI's service container, or `scripts/verify/integration.sh`'s
/// disposable one). xtask refuses to run without them, so a DB-less invocation can
/// never masquerade as a pass — closing the "locally green, only CI runs the DB
/// tests" gap.
/// Run one named live-resource lane: only the targets it declares, and only
/// after the backend it needs is actually present.
///
/// This is the half of `integration` that does not sweep. `--ignored` over the
/// whole workspace cannot tell a Kafka test from a Postgres one, so the Postgres
/// job ran both and the Kafka half quietly passed against no broker. A lane runs
/// its own targets and refuses to start without its own env, so "did not run"
/// can never be recorded as "verified".
fn integration_lane(area: &Area, lane_name: &str, affected: bool, log: &mut FailureLog) {
    let Some(lane) = area.live_lanes.iter().find(|lane| lane.name == lane_name) else {
        fail_usage(&format!(
            "unknown lane '{lane_name}' for {}. known: {}",
            area.slug,
            area.live_lanes
                .iter()
                .map(|lane| lane.name)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    };
    for var in lane.required_env {
        if std::env::var(var).is_err() {
            fail_usage(&format!(
                "{} lane '{}' needs a live backend: {var} is unset. Provision it and \
                 export {var}, or do not request this lane.",
                area.slug, lane.name
            ));
        }
    }
    let Some(selection) = selection_for(area, affected, log) else {
        return;
    };
    let dir = repo_root().join(area.dir);
    if lane
        .targets
        .iter()
        .any(|target| selection.verifies(target.package()))
    {
        ensure_apt(area.apt_deps);
    }
    let mut lane_total = 0;
    let mut lane_targets = 0;
    for (target, command) in lane.targets.iter().zip(lane_commands(lane)) {
        if !selection.verifies(target.package()) {
            continue;
        }
        lane_targets += 1;
        let args: Vec<&str> = command.iter().map(String::as_str).collect();
        // Every remaining target still runs. A lane is an authoritative gate, and stopping here
        // meant one broken suite hid the state of every suite after it — on a lane that takes
        // twenty minutes to reach the end.
        let Some(output) = log.absorb(cargo_capturing_stdout(&dir, &args)) else {
            continue;
        };
        match lane_target_verdict(area.slug, lane.name, target, &output) {
            Ok(count) => lane_total += count,
            Err(message) => log.record(message),
        }
    }
    eprintln!(
        "xtask: {} lane '{}' executed {lane_total} test(s) across {lane_targets} of {} target(s).",
        area.slug,
        lane.name,
        lane.targets.len()
    );
}

/// Both callers of the bare form — `.github/workflows/foundation-ci.yml` and
/// `scripts/verify/integration.sh` — provision a Postgres and nothing else, so
/// the bare form means the Postgres lane.
///
/// It used to mean `--workspace --all-features -- --ignored`, which also ran the
/// Kafka, R2, lakehouse and public-API tests in that Postgres-only job. Each
/// found no backend of its own, took its "resource absent" branch, and was
/// counted as a pass, so a green Postgres job silently asserted nothing about
/// four other backends. Delegating to the lane runs exactly the targets Postgres
/// actually covers.
fn integration(area: &Area, affected: bool, log: &mut FailureLog) {
    let has_postgres_lane = area.live_lanes.iter().any(|lane| lane.name == "postgres");
    if !has_postgres_lane {
        fail_usage(&format!(
            "{} declares no 'postgres' lane. Known lanes: {}. Name one explicitly: \
             `cargo xtask integration {} <lane>`.",
            area.slug,
            if area.live_lanes.is_empty() {
                "(none)".to_owned()
            } else {
                area.live_lanes
                    .iter()
                    .map(|lane| lane.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            },
            area.slug
        ));
    }
    integration_lane(area, "postgres", affected, log);
}

#[cfg(test)]
mod tests;
