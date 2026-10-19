//! `cargo xtask live <kafka | r2-lakehouse>`: Foundation's live-resource harnesses (formerly
//! `scripts/verify/foundation-{kafka,r2-lakehouse}-live.sh`; shell inventory, root ADR-0270).
//!
//! The lanes own WHICH targets run (`cargo xtask integration foundation <lane>`, root ADR-0011); these
//! commands own what the lanes run against.
//!
//! - `kafka`: the pinned Redpanda/Karapace stack of `c2-event-backbone.compose.yml`: up, readiness,
//!   the canonical raw-written topic, the test environment, and teardown whatever happens. The caller
//!   supplies `DATABASE_URL` (CI's PostGIS service or a disposable local Postgres).
//! - `r2-lakehouse`: the opt-in live Cloudflare R2 and R2 Data Catalog lanes. No emulator: R2 is the
//!   backend everywhere, so a missing setting is an error, never a skip. Settings come from the
//!   process, then from `FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE` (default the area's `.env.local`), parsed
//!   without ever being executed; the process wins. The lane is admitted only for local/developer,
//!   ci/ci, or production/developer with both production approvals, with the write confirmation and a
//!   smoke-bucket marker equal to the bucket.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

const USAGE: &str = "usage: cargo xtask live <kafka | r2-lakehouse>";
const KAFKA_COMPOSE: &str = "platforms/intelligence-platform/docker/c2-event-backbone.compose.yml";

pub(crate) fn run(root: &Path, args: &[String]) -> Result<(), String> {
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["kafka"] => kafka(root),
        ["r2-lakehouse"] => r2_lakehouse(root),
        _ => Err(USAGE.to_owned()),
    }
}

fn kafka(root: &Path) -> Result<(), String> {
    let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    if !Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return Err("Docker Desktop/Engine is required".to_owned());
    }
    let compose_file = root.join(KAFKA_COMPOSE);
    if !compose_file.is_file() {
        return Err(format!("missing compose file {}", compose_file.display()));
    }
    let database_url = var("DATABASE_URL")
        .ok_or("DATABASE_URL must point at a migrated Postgres/PostGIS database")?;
    let topic = var("FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC")
        .unwrap_or_else(|| "foundation-platform.catalog.collection-raw-written.v1".to_owned());
    let kafka_port = var("INTELLIGENCE_TEST_KAFKA_HOST_PORT").unwrap_or_else(|| "19092".to_owned());
    let karapace_port =
        var("INTELLIGENCE_TEST_KARAPACE_HOST_PORT").unwrap_or_else(|| "18081".to_owned());
    let project = compose_project(&var("FOUNDATION_KAFKA_COMPOSE_PROJECT").unwrap_or_else(|| {
        format!(
            "foundation-kafka-live-{}-{}",
            var("USER").unwrap_or_else(|| "local".to_owned()),
            std::process::id()
        )
    }));
    let compose = |args: &[&str]| {
        let mut command = Command::new("docker");
        command
            .args(["compose", "-p", &project, "-f"])
            .arg(&compose_file)
            .args(args);
        command.env("INTELLIGENCE_TEST_KAFKA_HOST_PORT", &kafka_port);
        command.env("INTELLIGENCE_TEST_KARAPACE_HOST_PORT", &karapace_port);
        command
    };
    // Torn down whatever happens below, as the script's EXIT trap did.
    struct Down<'a>(&'a dyn Fn(&[&str]) -> Command);
    impl Drop for Down<'_> {
        fn drop(&mut self) {
            let _ = (self.0)(&["down", "-v", "--remove-orphans"]).output();
        }
    }
    let _down = Down(&compose);

    println!("xtask live kafka: starting pinned Redpanda/Karapace project {project}");
    succeed(&mut compose(&["up", "-d"]), "docker compose up")?;
    println!("xtask live kafka: waiting for Karapace HTTP endpoint");
    let subjects = format!("http://127.0.0.1:{karapace_port}/subjects");
    wait_until("Karapace", || {
        Command::new("curl")
            .args(["--fail", "--silent", "--show-error", &subjects])
            .output()
            .map(|o| o.status.success())
    })?;
    println!("xtask live kafka: waiting for Redpanda and creating canonical topic");
    let rpk = |args: &[&str]| {
        let mut all = vec!["exec", "-T", "redpanda", "rpk"];
        all.extend_from_slice(args);
        all.extend_from_slice(&["--brokers", "redpanda:9092"]);
        compose(&all)
    };
    wait_until("Redpanda", || {
        rpk(&["cluster", "info"])
            .output()
            .map(|o| o.status.success())
    })?;
    if !rpk(&["topic", "describe", &topic])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        succeed(
            &mut rpk(&[
                "topic",
                "create",
                &topic,
                "--partitions",
                "1",
                "--replicas",
                "1",
            ]),
            "rpk topic create",
        )?;
    }
    succeed(
        &mut rpk(&["topic", "describe", &topic]),
        "rpk topic describe",
    )?;

    println!("xtask live kafka: running the foundation kafka lane");
    let bootstrap = format!("127.0.0.1:{kafka_port}");
    let karapace = format!("http://127.0.0.1:{karapace_port}");
    let mut lane = lane_command(root, "kafka")?;
    lane.env("DATABASE_URL", &database_url)
        .env("FOUNDATION_TEST_KAFKA_REQUIRED", "1")
        .env("FOUNDATION_PLATFORM_KAFKA_ENABLED", "1")
        .env("FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS", &bootstrap)
        .env("FOUNDATION_PLATFORM_KAFKA_BOOTSTRAP_SERVERS", &bootstrap)
        .env("FOUNDATION_TEST_KARAPACE_URL", &karapace)
        .env("FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_URL", &karapace)
        .env("FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC", &topic)
        .env(
            "FOUNDATION_PLATFORM_RUNTIME_ENV",
            var("FOUNDATION_PLATFORM_RUNTIME_ENV").unwrap_or_else(|| "ci".to_owned()),
        )
        .env(
            "FOUNDATION_PLATFORM_EXECUTION_CONTEXT",
            var("FOUNDATION_PLATFORM_EXECUTION_CONTEXT").unwrap_or_else(|| "ci".to_owned()),
        );
    succeed(&mut lane, "the foundation kafka lane")?;
    println!("xtask live kafka: PASS (Kafka, Karapace, Postgres outbox, and outage contracts)");
    Ok(())
}

fn r2_lakehouse(root: &Path) -> Result<(), String> {
    let process: BTreeMap<String, String> = std::env::vars().collect();
    let named_file = process
        .get("FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE")
        .filter(|v| !v.is_empty())
        .cloned();
    let file = named_file
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("platforms/foundation-platform/.env.local"));
    let dotenv = match std::fs::read_to_string(&file) {
        Ok(text) => parse_dotenv(&text),
        Err(_) if named_file.is_some() => {
            return Err(format!("env file does not exist: {}", file.display()))
        }
        Err(_) => Vec::new(),
    };
    let settings = merged(&process, dotenv);
    admit_r2_lane(&settings)?;
    let smoke = [
        ("FOUNDATION_PLATFORM_R2_LIVE_SMOKE", "1"),
        ("FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE", "1"),
        ("FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE", "1"),
        ("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER", "r2"),
    ];
    println!("xtask live r2-lakehouse: validating governed R2 runtime target");
    succeed(
        Command::new("cargo")
            .current_dir(root)
            .args([
                "run",
                "--locked",
                "--manifest-path",
                "platforms/foundation-platform/Cargo.toml",
            ])
            .args([
                "-p",
                "foundation-outbox-publisher",
                "--",
                "check-r2-runtime-target",
            ])
            .envs(&settings)
            .envs(smoke),
        "check-r2-runtime-target",
    )?;
    for lane in ["r2", "lakehouse"] {
        println!("xtask live r2-lakehouse: running the foundation {lane} lane");
        succeed(
            lane_command(root, lane)?.envs(&settings).envs(smoke),
            &format!("the foundation {lane} lane"),
        )?;
    }
    println!("xtask live r2-lakehouse: PASS (Cloudflare R2 and R2 Data Catalog)");
    Ok(())
}

/// `KEY=value` assignments of a dotenv file, never executed: comments and anything else skipped,
/// an optional `export`, whitespace trimmed, one pair of matching quotes removed.
fn parse_dotenv(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim_start();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let line = line
                .strip_prefix("export")
                .filter(|rest| rest.starts_with(char::is_whitespace))
                .map_or(line, str::trim_start);
            let (name, value) = line.split_once('=')?;
            let name = name.trim_end();
            let valid = name
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !valid {
                return None;
            }
            let value = value.trim();
            let unquoted = ['"', '\'']
                .iter()
                .find_map(|q| value.strip_prefix(*q).and_then(|v| v.strip_suffix(*q)))
                .unwrap_or(value);
            Some((name.to_owned(), unquoted.to_owned()))
        })
        .collect()
}

/// The process's variables, with the file's only where the process has none (CI's secrets win).
fn merged(
    process: &BTreeMap<String, String>,
    file: Vec<(String, String)>,
) -> BTreeMap<String, String> {
    let mut settings = process.clone();
    for (name, value) in file {
        settings.entry(name).or_insert(value);
    }
    settings
}

/// The fail-closed admission of the live R2 lanes.
fn admit_r2_lane(settings: &BTreeMap<String, String>) -> Result<(), String> {
    let get = |name: &str| {
        settings
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    };
    for name in [
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
        "FOUNDATION_PLATFORM_RUNTIME_ENV",
        "FOUNDATION_PLATFORM_EXECUTION_CONTEXT",
        "FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET",
        "FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN",
    ] {
        get(name).ok_or_else(|| format!("{name} is required"))?;
    }
    if get("FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT").is_none()
        && get("FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID").is_none()
    {
        return Err("FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT or FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID is required".to_owned());
    }
    match (get("FOUNDATION_PLATFORM_RUNTIME_ENV"), get("FOUNDATION_PLATFORM_EXECUTION_CONTEXT")) {
        (Some("local"), Some("developer")) | (Some("ci"), Some("ci")) => {}
        (Some("production"), Some("developer")) => {
            if get("FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION") != Some("1") {
                return Err("production requires FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION=1".to_owned());
            }
            if get("FOUNDATION_PLATFORM_PRELAUNCH_SHARED") != Some("1") {
                return Err("production requires FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1".to_owned());
            }
        }
        _ => {
            return Err("runtime/context must be local/developer, ci/ci, or explicitly approved production/developer".to_owned())
        }
    }
    if get("FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM") != Some("1") {
        return Err("FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM=1 is required".to_owned());
    }
    if get("FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET")
        != get("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET")
    {
        return Err("smoke bucket marker does not match R2 lakehouse bucket".to_owned());
    }
    if get("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER") != Some("r2_data_catalog") {
        return Err(
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER must be r2_data_catalog".to_owned(),
        );
    }
    Ok(())
}

/// A compose project name: lower case, anything but `a-z0-9_-` replaced by `-`.
fn compose_project(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// This binary's own `integration foundation <lane>` (the lane owns its targets).
fn lane_command(root: &Path, lane: &str) -> Result<Command, String> {
    let xtask = std::env::current_exe().map_err(|error| format!("cannot locate xtask: {error}"))?;
    let mut command = Command::new(xtask);
    command
        .current_dir(root)
        .args(["integration", "foundation", lane]);
    Ok(command)
}

fn wait_until(what: &str, mut ready: impl FnMut() -> std::io::Result<bool>) -> Result<(), String> {
    for _ in 0..90 {
        if ready().unwrap_or(false) {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    Err(format!("{what} did not become ready"))
}

fn succeed(command: &mut Command, what: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("cannot run {what}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{what} failed: {status}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready() -> BTreeMap<String, String> {
        [
            (
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
                "foundation-platform-lakehouse-dev",
            ),
            (
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
                "https://r2.example.invalid",
            ),
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
                "test-access",
            ),
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
                "test-secret",
            ),
            ("FOUNDATION_PLATFORM_RUNTIME_ENV", "local"),
            ("FOUNDATION_PLATFORM_EXECUTION_CONTEXT", "developer"),
            (
                "FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET",
                "foundation-platform-lakehouse-dev",
            ),
            ("FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM", "1"),
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER",
                "r2_data_catalog",
            ),
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
                "https://catalog.example.invalid",
            ),
            ("FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE", "test-warehouse"),
            (
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN",
                "test-token",
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }

    fn refused(edit: impl FnOnce(&mut BTreeMap<String, String>)) -> String {
        let mut settings = ready();
        edit(&mut settings);
        admit_r2_lane(&settings).expect_err("admitted")
    }

    #[test]
    fn complete_local_settings_are_admitted() {
        assert_eq!(admit_r2_lane(&ready()), Ok(()));
    }

    #[test]
    fn nothing_set_names_the_first_missing_setting() {
        assert!(admit_r2_lane(&BTreeMap::new())
            .unwrap_err()
            .contains("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET"));
    }

    #[test]
    fn production_needs_both_approvals() {
        let production = |s: &mut BTreeMap<String, String>| {
            s.insert(
                "FOUNDATION_PLATFORM_RUNTIME_ENV".into(),
                "production".into(),
            );
            s.insert("FOUNDATION_PLATFORM_PRELAUNCH_SHARED".into(), "1".into());
        };
        assert!(refused(production).contains("FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION=1"));
        let mut approved = ready();
        production(&mut approved);
        approved.insert(
            "FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION".into(),
            "1".into(),
        );
        assert_eq!(admit_r2_lane(&approved), Ok(()));
        approved.remove("FOUNDATION_PLATFORM_PRELAUNCH_SHARED");
        assert!(admit_r2_lane(&approved)
            .unwrap_err()
            .contains("PRELAUNCH_SHARED=1"));
        assert!(refused(|s| {
            s.insert("FOUNDATION_PLATFORM_EXECUTION_CONTEXT".into(), "ci".into());
        })
        .contains("runtime/context"));
    }

    #[test]
    fn each_fence_fails_closed_and_names_itself() {
        assert!(refused(|s| {
            s.remove("FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT");
        })
        .contains("ENDPOINT or FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID"));
        assert!(refused(|s| {
            s.remove("FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID");
        })
        .contains("FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID"));
        assert!(refused(|s| {
            s.insert(
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER".into(),
                "iceberg_rest".into(),
            );
        })
        .contains("must be r2_data_catalog"));
        assert!(refused(|s| {
            s.insert(
                "FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM".into(),
                "yes".into(),
            );
        })
        .contains("WRITE_CONFIRM=1"));
        assert!(refused(|s| {
            s.insert(
                "FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET".into(),
                "other".into(),
            );
        })
        .contains("smoke bucket marker"));
        // An empty value is missing, as `[ -n "$X" ]` treated it.
        assert!(refused(|s| {
            s.insert(
                "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE".into(),
                String::new(),
            );
        })
        .contains("FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"));
    }

    #[test]
    fn a_dotenv_file_is_read_never_executed_and_the_process_wins() {
        let file = parse_dotenv(
            "# comment\n  export A=1\nB = \"two words\"\nC='$(rm -rf /)'\nD=plain # not a comment\n1BAD=x\nnot an assignment\nexporter=kept\n",
        );
        assert_eq!(
            file,
            vec![
                ("A".to_owned(), "1".to_owned()),
                ("B".to_owned(), "two words".to_owned()),
                ("C".to_owned(), "$(rm -rf /)".to_owned()),
                ("D".to_owned(), "plain # not a comment".to_owned()),
                ("exporter".to_owned(), "kept".to_owned()),
            ]
        );
        let process = BTreeMap::from([("A".to_owned(), "from-ci".to_owned())]);
        let settings = merged(&process, file);
        assert_eq!(settings["A"], "from-ci");
        assert_eq!(settings["B"], "two words");
    }

    #[test]
    fn a_compose_project_name_is_lower_case_and_safe() {
        assert_eq!(
            compose_project("Foundation-Kafka-Live-Jo Doe-42"),
            "foundation-kafka-live-jo-doe-42"
        );
    }

    #[test]
    fn an_unknown_command_prints_the_usage() {
        assert_eq!(run(Path::new("."), &[]), Err(USAGE.to_owned()));
        assert_eq!(
            run(Path::new("."), &["kafka".to_owned(), "x".to_owned()]),
            Err(USAGE.to_owned())
        );
    }
}
