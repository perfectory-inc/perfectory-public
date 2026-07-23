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
    /// verification entrypoint. `None` means the area has no such suite.
    python_tests: Option<PythonTests>,
    /// Live-DB integration tests, if the area has any. `None` = none.
    integration: Option<Integration>,
}

struct PythonTests {
    /// Working directory relative to the area root.
    dir: &'static str,
    /// Python module search path relative to `dir`.
    python_path: &'static str,
    /// Arguments following `python3 -m pytest`.
    args: &'static [&'static str],
}

/// The live-DB integration test contract for an area — the SSOT for the *command*
/// so it never drifts across CI and local (the same drift `verify` already killed
/// for fmt/clippy/test). It runs ONLY against an already-provisioned Postgres:
/// `verify` (offline, DB-less) skips these, so both CI's service container and the
/// local `scripts/verify/integration.sh` (a disposable "Testcontainers"-style DB)
/// set `url_vars` and then invoke `cargo xtask integration <area>`.
struct Integration {
    /// Env var(s) the tests read the connection URL from (e.g. DATABASE_URL). The
    /// provisioner must set these; xtask refuses to run without them.
    url_vars: &'static [&'static str],
    /// The integration test command (cargo args), run from the area dir.
    test: &'static [&'static str],
}

const AREAS: &[Area] = &[
    Area {
        slug: "gongzzang",
        dir: "products/gongzzang",
        apt_deps: &[],
        two_stage_test: true,
        python_tests: None,
        integration: None, // gongzzang-persistence smoke — to wire next.
    },
    Area {
        slug: "foundation",
        dir: "platforms/foundation-platform",
        // aws-sdk-s3's maintained default HTTPS client builds aws-lc-sys with CMake.
        apt_deps: &["cmake", "python3", "python3-pytest"],
        two_stage_test: false,
        python_tests: Some(PythonTests {
            dir: "services/foundation-provider-acquisition-worker",
            python_path: "src",
            args: &["tests", "-q"],
        }),
        // Foundation's DB-backed reads tests (catalog_*_reads, …) are `#[ignore]`
        // and need a migrated + seeded Postgres. scripts/verify/integration.sh
        // provisions one locally; CI's postgres-integration job provides its own.
        integration: Some(Integration {
            url_vars: &["DATABASE_URL"],
            test: &[
                "test",
                "--locked",
                "--workspace",
                "--all-features",
                "--",
                "--ignored",
                "--test-threads=1",
            ],
        }),
    },
    Area {
        slug: "identity",
        dir: "platforms/identity-platform",
        apt_deps: &[],
        two_stage_test: false,
        python_tests: None,
        integration: None, // authorization role-grant PG tests — to wire next.
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
        python_tests: None,
        integration: None, // INTELLIGENCE_TEST_DATABASE_URL suite — to wire next.
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
                for area in AREAS.iter().filter(|a| a.integration.is_some()) {
                    eprintln!("\n=== xtask integration {} ===", area.slug);
                    integration(area);
                }
            }
            Some(name) => match AREAS.iter().find(|a| a.slug == name || a.dir == name) {
                Some(area) => integration(area),
                None => fail_usage(&format!(
                    "unknown area '{name}'. known: {}, all",
                    AREAS.iter().map(|a| a.slug).collect::<Vec<_>>().join(", ")
                )),
            },
            None => fail_usage("missing area: cargo xtask integration <area|all>"),
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

    if let Some(spec) = area.python_tests.as_ref() {
        let python_dir = dir.join(spec.dir);
        let mut command = Command::new("python3");
        command
            .current_dir(&python_dir)
            .env("PYTHONPATH", spec.python_path)
            .args(["-m", "pytest"])
            .args(spec.args);
        run(&mut command);
    }
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
fn integration(area: &Area) {
    let Some(spec) = area.integration.as_ref() else {
        eprintln!(
            "xtask integration: {} has no live-DB integration tests; nothing to run.",
            area.slug
        );
        return;
    };
    for var in spec.url_vars {
        if std::env::var(var).is_err() {
            fail_usage(&format!(
                "{} integration needs a live database: {var} is unset. Run \
                 `scripts/verify/integration.sh {}` (provisions a disposable Postgres), \
                 or set {var} to an existing one.",
                area.slug, area.slug
            ));
        }
    }
    let dir = repo_root().join(area.dir);
    ensure_apt(area.apt_deps);
    // DATABASE_URL et al. are inherited from the environment; cargo() adds
    // SQLX_OFFLINE so compilation uses cached metadata while the tests connect live.
    cargo(&dir, spec.test);
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
