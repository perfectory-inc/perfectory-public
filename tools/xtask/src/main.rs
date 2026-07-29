//! perfectory monorepo verification SSOT (ADR-0004).
//!
//! ONE definition of "how each area is verified", called by BOTH the local harness
//! (`scripts/verify/cargo-verify.sh`, inside Docker) and CI (`.github/workflows/
//! *-ci.yml` rust jobs). Drift between local and CI is structurally impossible:
//! there is a single source. Dependency-free (std only).
//!
//! Usage (from the repository root):
//!   cargo xtask verify <gongzzang|foundation|identity|intelligence|all>
//!   cargo xtask docs   # monorepo-wide lychee internal-link check (offline)

use std::path::{Path, PathBuf};
use std::process::{exit, Command};

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

/// A single cargo test target, addressed the way `foundation-kafka-live.sh`
/// already addresses it: `-p <package> --test <test>`.
struct LaneTarget {
    package: &'static str,
    test: &'static str,
}

/// The cargo invocation for each target in a lane — one per target, mirroring
/// the loop `scripts/verify/foundation-kafka-live.sh` already runs.
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
                ["-p", target.package, "--test", target.test, "--"]
                    .iter()
                    .map(|arg| (*arg).to_owned()),
            );
            if lane.gating == LaneGating::Ignored {
                command.push("--ignored".to_owned());
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
        .flat_map(|lane| lane.targets.iter().map(|target| target.package))
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
            "{area} lane '{lane}': `-p {} --test {}` executed 0 tests. cargo \
             exits 0 when a filter matches nothing, so this would have been \
             recorded as a pass. Check the lane's gating — `#[ignore]` targets \
             need `-- --ignored`, `#![cfg(feature = \"…\")]` targets need \
             `--features …` and must not be filtered on `--ignored`.",
            target.package, target.test
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
}

#[derive(Debug, PartialEq, Eq)]
struct PythonCommandPlan {
    current_dir: PathBuf,
    python_path: Option<&'static str>,
    args: &'static [&'static str],
}

const AREAS: &[Area] = &[
    Area {
        slug: "gongzzang",
        dir: "products/gongzzang",
        apt_deps: &[],
        python_tests: &[],

        // Gated by `#![cfg(feature = "integration")]` rather than `#[ignore]`.
        // `verify` reads that gating back off this table to keep the package out
        // of its `--all-features` run (`feature_gated_packages`), so the fact is
        // stated once here instead of twice.
        live_lanes: &[LiveLane {
            name: "postgres",
            required_env: &["DATABASE_URL"],
            gating: LaneGating::Feature("integration"),
            targets: &[
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "admin_action_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "analysis_report_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "audit_log_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "bookmark_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "business_verification_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "error_map_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "featured_content_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "foundation_anchor_import_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "foundation_anchor_visibility_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "listing_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "listing_marker_tile_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "listing_photo_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "listing_report_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "listing_review_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "notification_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "outbox_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "outbox_publisher_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "search_history_integration",
                },
                LaneTarget {
                    package: "gongzzang-persistence",
                    test: "system_alert_integration",
                },
                LaneTarget {
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
            },
        ],
        // Foundation's DB-backed reads tests (catalog_*_reads, …) are `#[ignore]`
        // and need a migrated + seeded Postgres. scripts/verify/integration.sh
        // provisions one locally; CI's postgres-integration job provides its own.
        // The four backends that the Postgres sweep used to run against nothing.
        // `foundation-kafka-live.sh` already provisions the Kafka stack and runs
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
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "administrative_boundary_identity",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "catalog_round_trip",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "catalog_ssot_reads",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "complex_anchor_summary_reads",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "industrial_complex_transaction_participant",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "marker_tile_reads",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "parcel_marker_anchor_rebuild",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "vector_tile_manifest_promote",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "vector_tile_manifest_reads",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "vector_tile_manifest_rollback",
                    },
                    LaneTarget {
                        package: "catalog-infrastructure",
                        test: "vector_tile_runtime_manifest_promote",
                    },
                    LaneTarget {
                        package: "collection-infrastructure",
                        test: "bronze_catalog_recovery_atomicity",
                    },
                    LaneTarget {
                        package: "collection-infrastructure",
                        test: "bronze_ingest_round_trip",
                    },
                    LaneTarget {
                        package: "foundation-normalization-infrastructure",
                        test: "active_override_reader",
                    },
                    LaneTarget {
                        package: "foundation-normalization-infrastructure",
                        test: "building_register_unit_transactions",
                    },
                    LaneTarget {
                        package: "foundation-normalization-infrastructure",
                        test: "industrial_complex_ledger_integrity",
                    },
                    LaneTarget {
                        package: "foundation-normalization-infrastructure",
                        test: "normalization_application_roundtrip",
                    },
                    LaneTarget {
                        package: "foundation-normalization-infrastructure",
                        test: "normalization_atomicity",
                    },
                    LaneTarget {
                        package: "foundation-normalization-infrastructure",
                        test: "normalization_proposal_roundtrip",
                    },
                    LaneTarget {
                        package: "foundation-outbox",
                        test: "postgres_jobbus",
                    },
                    LaneTarget {
                        package: "foundation-outbox",
                        test: "publish_roundtrip",
                    },
                    LaneTarget {
                        package: "lakehouse-infrastructure",
                        test: "gold_publication_atomicity",
                    },
                    LaneTarget {
                        package: "lakehouse-infrastructure",
                        test: "lakehouse_batch_run_audit",
                    },
                    LaneTarget {
                        package: "lakehouse-infrastructure",
                        test: "lakehouse_registry_atomicity",
                    },
                    LaneTarget {
                        package: "lakehouse-infrastructure",
                        test: "lakehouse_registry_repository",
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
                    LaneTarget {
                        package: "foundation-outbox",
                        test: "live_kafka_karapace",
                    },
                    LaneTarget {
                        package: "foundation-outbox",
                        test: "live_kafka_outbox_roundtrip",
                    },
                    LaneTarget {
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
                ],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget {
                    package: "foundation-outbox",
                    test: "r2_smoke_contract",
                }],
            },
            LiveLane {
                name: "lakehouse",
                required_env: &["FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE"],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget {
                    package: "lakehouse-infrastructure",
                    test: "lakehouse_live_smoke",
                }],
            },
            LiveLane {
                name: "data-go-kr",
                required_env: &[
                    "FOUNDATION_PLATFORM_DATA_GO_KR_LIVE_SMOKE",
                    "DATA_GO_KR_SERVICE_KEY",
                ],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget {
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
                LaneTarget {
                    package: "authorization-infrastructure",
                    test: "role_grant_postgres",
                },
                LaneTarget {
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

        live_lanes: &[
            LiveLane {
                name: "kafka",
                required_env: &[
                    "INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS",
                    "INTELLIGENCE_TEST_KARAPACE_URL",
                ],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget {
                    package: "messaging-infrastructure",
                    test: "live_kafka_karapace",
                }],
            },
            LiveLane {
                name: "redis",
                required_env: &["INTELLIGENCE_REDIS_LIVE_TEST_URL"],
                gating: LaneGating::Ignored,
                targets: &[LaneTarget {
                    package: "intelligence-normalization-infrastructure",
                    test: "redis_rate_limit_live",
                }],
            },
        ],
    },
];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("verify") => {
            repository_guard();
            match args.get(1).map(String::as_str) {
                Some("all") => {
                    for area in AREAS {
                        eprintln!("\n=== xtask verify {} ===", area.slug);
                        verify(area);
                    }
                }
                Some(name) => match AREAS.iter().find(|a| a.slug == name || a.dir == name) {
                    Some(area) => verify(area),
                    None => fail_usage(&format!(
                        "unknown area '{name}'. known: {}, all",
                        AREAS.iter().map(|a| a.slug).collect::<Vec<_>>().join(", ")
                    )),
                },
                None => fail_usage("missing area: cargo xtask verify <area|all>"),
            }
        }
        Some("integration") => match args.get(1).map(String::as_str) {
            Some("all") => {
                for area in AREAS.iter().filter(|a| !a.live_lanes.is_empty()) {
                    eprintln!("\n=== xtask integration {} ===", area.slug);
                    integration(area);
                }
            }
            Some(name) => match AREAS.iter().find(|a| a.slug == name || a.dir == name) {
                // A third argument selects one live-resource lane; without it the
                // area's Postgres suite runs as before.
                Some(area) => match args.get(2).map(String::as_str) {
                    Some(lane) => integration_lane(area, lane),
                    None => integration(area),
                },
                None => fail_usage(&format!(
                    "unknown area '{name}'. known: {}, all",
                    AREAS.iter().map(|a| a.slug).collect::<Vec<_>>().join(", ")
                )),
            },
            None => fail_usage("missing area: cargo xtask integration <area|all> [lane]"),
        },
        Some("docs") => docs(),
        _ => fail_usage("usage: cargo xtask <verify <area|all> | integration <area|all> | docs>"),
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
fn verify(area: &Area) {
    let dir = repo_root().join(area.dir);
    ensure_apt(area.apt_deps);

    cargo(&dir, &["fmt", "--all", "--", "--check"]);
    cargo(
        &dir,
        &[
            "clippy",
            "--locked",
            "--workspace",
            "--all-features",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    );

    for command in verify_test_commands(area) {
        cargo(&dir, &command);
    }

    for plan in python_test_plans(area, &dir) {
        let mut command = Command::new("python3");
        command.current_dir(&plan.current_dir);
        if let Some(python_path) = plan.python_path {
            command.env("PYTHONPATH", python_path);
        }
        command.args(plan.args);
        run(&mut command);
    }
}

fn python_test_plans(area: &Area, area_dir: &Path) -> Vec<PythonCommandPlan> {
    area.python_tests
        .iter()
        .map(|suite| PythonCommandPlan {
            current_dir: area_dir.join(suite.dir),
            python_path: suite.python_path,
            args: suite.args,
        })
        .collect()
}

/// Run the fast repository-structure checks before any expensive area build.
/// This keeps publication, licensing, and workflow safety in the same authoritative
/// `cargo xtask verify <area>` entrypoint as compile/test policy.
fn repository_guard() {
    let root = repo_root();
    run(Command::new("bash")
        .current_dir(&root)
        .arg("scripts/guard/monorepo-guard.sh"));
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
fn integration_lane(area: &Area, lane_name: &str) {
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
    let dir = repo_root().join(area.dir);
    ensure_apt(area.apt_deps);
    let mut lane_total = 0;
    for (target, command) in lane.targets.iter().zip(lane_commands(lane)) {
        let args: Vec<&str> = command.iter().map(String::as_str).collect();
        let output = cargo_capturing_stdout(&dir, &args);
        match lane_target_verdict(area.slug, lane.name, target, &output) {
            Ok(count) => lane_total += count,
            Err(message) => {
                eprintln!("xtask: {message}");
                exit(1);
            }
        }
    }
    eprintln!(
        "xtask: {} lane '{}' executed {lane_total} test(s) across {} target(s).",
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
fn integration(area: &Area) {
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
    integration_lane(area, "postgres");
}

/// The repository root: xtask lives at `<root>/tools/xtask`, so climb two parents
/// from its manifest dir. This makes area paths resolve regardless of the caller's
/// working directory.
fn repo_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| {
            eprintln!("xtask: cannot locate repository root from {manifest:?}");
            exit(1);
        })
        .to_path_buf()
}

/// Run `cargo <args>` in `area_dir`. SQLX_OFFLINE matches CI + the harness so
/// compile-time query checks never need a live database.
fn cargo(area_dir: &Path, args: &[&str]) {
    let mut command = Command::new("cargo");
    command
        .current_dir(area_dir)
        .env("SQLX_OFFLINE", "true")
        .args(args);
    run(&mut command);
}

/// Like `cargo`, but keeps stdout so libtest's `test result:` summary can be
/// read back — the only way to tell "verified" from "selected nothing".
///
/// stderr stays inherited, so cargo's compile progress still streams live during
/// long builds; the captured stdout is echoed the moment the command ends, so
/// CI logs lose nothing.
fn cargo_capturing_stdout(area_dir: &Path, args: &[&str]) -> String {
    let mut command = Command::new("cargo");
    command
        .current_dir(area_dir)
        .env("SQLX_OFFLINE", "true")
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
    let rendered = format!("{command:?}");
    let output = command.output().unwrap_or_else(|error| {
        eprintln!("xtask: could not spawn {rendered}: {error}");
        exit(1);
    });
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    print!("{stdout}");
    if !output.status.success() {
        eprintln!("xtask: FAILED ({}): {rendered}", output.status);
        exit(output.status.code().unwrap_or(1));
    }
    stdout
}

/// Install Debian packages needed by an area's complete verification suite. No-op
/// when empty. On the rust Docker image we are root (no sudo); on the CI runner we
/// are not (sudo). apt is idempotent, so re-running is cheap.
fn ensure_apt(deps: &[&str]) {
    if deps.is_empty() {
        return;
    }
    if !tool_exists("apt-get") {
        eprintln!(
            "xtask: apt-get not found; install these manually before verifying: {}",
            deps.join(" ")
        );
        return;
    }
    let sudo = !is_root();
    run(apt(sudo).arg("update"));
    run(apt(sudo)
        .args(["install", "-y", "--no-install-recommends"])
        .args(deps));
}

fn apt(sudo: bool) -> Command {
    if sudo {
        let mut c = Command::new("sudo");
        c.arg("apt-get");
        c
    } else {
        Command::new("apt-get")
    }
}

fn is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim() == "0")
        .unwrap_or(false)
}

fn tool_exists(name: &str) -> bool {
    Command::new(name)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run a command; on failure print it and exit with its code. This is a gate, not
/// a library — failing fast with the exact command is the right behaviour.
fn run(command: &mut Command) {
    let rendered = format!("{command:?}");
    match command.status() {
        Ok(status) if status.success() => {}
        Ok(status) => {
            eprintln!("xtask: FAILED ({status}): {rendered}");
            exit(status.code().unwrap_or(1));
        }
        Err(error) => {
            eprintln!("xtask: could not spawn {rendered}: {error}");
            exit(1);
        }
    }
}

fn fail_usage(message: &str) -> ! {
    eprintln!("xtask: {message}");
    exit(2);
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// A feature-gated lane needs the OPPOSITE flags of an `#[ignore]` lane, and
    /// getting it wrong is silent.
    ///
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
    }

    /// Executing nothing is a lane failure, not a lane pass.
    ///
    /// Without this, every defect in this file's history stays invisible: the
    /// original workspace sweep, the wrong gating flags, and any future filter
    /// that silently stops matching. The lane's own report is what closes it.
    #[test]
    fn a_lane_target_that_executed_nothing_is_a_failure_not_a_pass() {
        let target = LaneTarget {
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

        assert_eq!(plans.len(), 2);
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
    }
}
