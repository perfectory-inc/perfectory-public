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
    /// gongzzang gates its DB-integration tests behind a feature and runs the
    /// persistence crate's non-DB suite separately (mirrors gongzzang-ci). This
    /// contract lives here, in one place — not scattered across YAML.
    two_stage_test: bool,
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
    /// The exact test targets this lane owns.
    targets: &'static [LaneTarget],
}

/// A single cargo test target, addressed the way `foundation-kafka-live.sh`
/// already addresses it: `-p <package> --test <test>`.
struct LaneTarget {
    package: &'static str,
    test: &'static str,
}

/// The cargo invocation for each target in a lane — one per target, mirroring
/// the loop `scripts/verify/foundation-kafka-live.sh` already runs.
fn lane_commands(lane: &LiveLane) -> Vec<Vec<String>> {
    lane.targets
        .iter()
        .map(|target| {
            [
                "test",
                "--locked",
                "-p",
                target.package,
                "--test",
                target.test,
                "--",
                "--ignored",
                "--test-threads=1",
            ]
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect()
        })
        .collect()
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
        two_stage_test: true,
        python_tests: &[],

        // Gated by `#![cfg(feature = "integration")]` rather than `#[ignore]`,
        // which is why `two_stage_test` exists: stage one excludes this package
        // so `--all-features` cannot switch the suite on without a database.
        // That exclusion kept them out of the default run but never gave them a
        // run of their own — the whole suite currently executes nowhere.
        live_lanes: &[LiveLane {
            name: "postgres",
            required_env: &["DATABASE_URL"],
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
        two_stage_test: false,
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
                required_env: &[
                    "FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS",
                    "FOUNDATION_TEST_KARAPACE_URL",
                ],
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
                required_env: &["FOUNDATION_PLATFORM_R2_LIVE_SMOKE"],
                targets: &[LaneTarget {
                    package: "foundation-outbox",
                    test: "r2_smoke_contract",
                }],
            },
            LiveLane {
                name: "lakehouse",
                required_env: &["FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE"],
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
        two_stage_test: false,
        python_tests: &[],

        // identity-ci.yml runs these two through a raw `cargo test --ignored`
        // written straight into the workflow, so they exist in CI but cannot be
        // reproduced by any local xtask command. Naming them here is the first
        // half of removing that raw invocation.
        live_lanes: &[LiveLane {
            name: "postgres",
            required_env: &[
                "IDENTITY_TEST_DATABASE_URL",
                "IDENTITY_ROLE_GRANT_TEST_DATABASE_URL",
            ],
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
        two_stage_test: false,
        python_tests: &[],

        live_lanes: &[
            LiveLane {
                name: "kafka",
                required_env: &[
                    "INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS",
                    "INTELLIGENCE_TEST_KARAPACE_URL",
                ],
                targets: &[LaneTarget {
                    package: "messaging-infrastructure",
                    test: "live_kafka_karapace",
                }],
            },
            LiveLane {
                name: "redis",
                required_env: &["INTELLIGENCE_REDIS_LIVE_TEST_URL"],
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

    if area.two_stage_test {
        cargo(
            &dir,
            &[
                "test",
                "--locked",
                "--workspace",
                "--all-features",
                "--exclude",
                "gongzzang-persistence",
            ],
        );
        cargo(&dir, &["test", "--locked", "-p", "gongzzang-persistence"]);
    } else {
        cargo(&dir, &["test", "--locked", "--workspace", "--all-features"]);
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
    for command in lane_commands(lane) {
        let args: Vec<&str> = command.iter().map(String::as_str).collect();
        cargo(&dir, &args);
    }
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

        assert_eq!(
            lane.required_env,
            &[
                "FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS",
                "FOUNDATION_TEST_KARAPACE_URL",
            ]
        );

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
