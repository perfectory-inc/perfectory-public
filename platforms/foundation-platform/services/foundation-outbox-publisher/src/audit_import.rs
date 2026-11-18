//! Imports the changes made outside Foundation into the audit chain (root ADR-0569 §6).
//!
//! Two sources need no credential and are read here:
//!
//! - `deploy`: every state file of the host's autodeploy, every run (`deployed`, `failed` and
//!   `refused`, about 500 files, read in well under a second). A deployed commit also names, one
//!   event each, the pull requests it shipped: the first-parent commits since the deploy before it,
//!   read from the host's git mirror, whose squash subjects end with `(#N)`.
//! - `host_command`: the journal lines `foundation-operator-start` that root wrote (`_UID=0`; any
//!   user can `logger -t` that tag, and those lines are left out), after the cursor of the last run.
//!   Only the command's action, its subject and its declared arguments are kept; its reason stays in
//!   the journal (the chain cannot forget what it holds, ADR-0569 §5).
//!
//! The rows go through `audit.append` as `foundation_audit_importer`: the job connects with the
//! host's database credential and takes that role with `SET SESSION AUTHORIZATION`, so the
//! database itself refuses a source the importer may not write (the console's `dawneer`). The role
//! cannot log in. Each source's rows and its cursor are written in batches of 50, the last with the
//! cursor, so a stopped run keeps what it committed and the next one goes on. A row read twice is
//! one row: `(source, source_event_id)` is unique and a second append writes nothing.
//!
//! The sources that need the GitHub App (`github`, `merge_queue`, `repo_settings`) are marked
//! `waiting_for_credential` once, so the console says why they are empty.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const DEPLOY_STATE_ENV: &str = "FOUNDATION_AUDIT_DEPLOY_STATE_DIR";
const GIT_MIRROR_ENV: &str = "FOUNDATION_AUDIT_GIT_MIRROR";
const CONTRACT_ENV: &str = "FOUNDATION_AUDIT_EVENTS_CONTRACT";
const JOURNALCTL_ENV: &str = "FOUNDATION_AUDIT_JOURNALCTL";
const PASSWD_ENV: &str = "FOUNDATION_AUDIT_PASSWD";
const HOST_ENV: &str = "FOUNDATION_AUDIT_HOST";

const DEFAULT_DEPLOY_STATE: &str = "/var/lib/perfectory/autodeploy";
const DEFAULT_GIT_MIRROR: &str = "/var/lib/perfectory/control-source.git";
const DEFAULT_CONTRACT: &str = "config/audit-events.contract.json";
const JOURNAL_TAG: &str = "foundation-operator-start";
/// The importer's own identity in the chain; only `audit.append` makes it mean anything.
const IMPORTER_ROLE: &str = "foundation_audit_importer";
/// Rows per transaction: a console request waits for at most one batch (ADR-0569 §3).
const BATCH: usize = 50;
/// The name space of the change ids of pull requests made outside the console (ADR-0569 §6).
const CHANGE_NAMESPACE: Uuid = Uuid::from_u128(0x6f1c_7f3a_0569_5a5a_8c1e_5c2e_6a1d_0569);
/// The sources that wait for the GitHub App (ADR-0569 §8), marked once.
const WAITING_SOURCES: [&str; 3] = ["github", "merge_queue", "repo_settings"];
/// The journal's own actor for the auto-promotion timer (`who=auto-promote`).
const SYSTEM_WHO: &[&str] = &["auto-promote"];

/// One event to append, in the columns `audit.append` takes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Imported {
    pub source: &'static str,
    pub source_event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub action: String,
    pub actor_kind: &'static str,
    pub actor_external: Option<String>,
    pub object_type: String,
    pub object_id: String,
    pub after: Option<Value>,
    pub applied_via: (&'static str, String),
    pub result: &'static str,
    pub error: Option<String>,
    pub change_id: Option<Uuid>,
}

/// The change id of a pull request: one per PR, whoever made it.
pub(crate) fn pull_request_change(number: u64) -> Uuid {
    Uuid::new_v5(&CHANGE_NAMESPACE, format!("pr:{number}").as_bytes())
}

// ---------------------------------------------------------------- deploy

/// One autodeploy state file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeployState {
    pub state: &'static str,
    pub sha: String,
    pub at: DateTime<Utc>,
    pub reason: Option<String>,
}

fn is_sha(name: &str) -> bool {
    name.len() == 40
        && name
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Reads every state file: `deployed` and `failed` hold the time on their first line, `refused` the
/// reason, and its time is the file's.
pub(crate) fn read_deploy_states(root: &Path) -> anyhow::Result<Vec<DeployState>> {
    let mut states = Vec::new();
    for state in ["deployed", "failed", "refused"] {
        let dir = root.join(state);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("reading {}", dir.display())),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !is_sha(&name) {
                continue;
            }
            let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
            let first = text.lines().next().unwrap_or("").trim().to_owned();
            let modified: DateTime<Utc> = entry.metadata()?.modified()?.into();
            let (at, reason) = if state == "refused" {
                (modified, Some(first).filter(|line| !line.is_empty()))
            } else {
                (
                    DateTime::parse_from_rfc3339(&first)
                        .map_or(modified, |at| at.with_timezone(&Utc)),
                    None,
                )
            };
            states.push(DeployState {
                state,
                sha: name,
                at,
                reason,
            });
        }
    }
    states.sort_by(|a, b| (a.at, &a.sha).cmp(&(b.at, &b.sha)));
    Ok(states)
}

/// The pull request a squash subject names, `... (#1234)`.
pub(crate) fn subject_pull_request(subject: &str) -> Option<u64> {
    let rest = subject.trim_end().strip_suffix(')')?;
    let open = rest.rfind("(#")?;
    rest[open + 2..].parse().ok()
}

/// The pull requests of the first-parent commits in `previous..sha` (all of `sha` alone without a
/// previous deploy), oldest first.
fn shipped_pull_requests(
    mirror: &Path,
    previous: Option<&str>,
    sha: &str,
) -> anyhow::Result<Vec<u64>> {
    let range = previous.map_or_else(
        || format!("{sha}^!"),
        |previous| format!("{previous}..{sha}"),
    );
    let output = std::process::Command::new("git")
        .arg("--git-dir")
        .arg(mirror)
        .args(["log", "--first-parent", "--reverse", "--format=%s", &range])
        .output()
        .context("running git log on the mirror")?;
    if !output.status.success() {
        // A commit the mirror does not hold (a deploy of a branch since rewritten): the deploy is
        // still recorded, without the pull requests.
        return Ok(Vec::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(subject_pull_request)
        .collect())
}

/// Every deploy event not yet in the chain, oldest first.
pub(crate) fn deploy_events(
    states: &[DeployState],
    known: &BTreeSet<String>,
    shipped: &mut dyn FnMut(Option<&str>, &str) -> anyhow::Result<Vec<u64>>,
) -> anyhow::Result<Vec<Imported>> {
    let mut events = Vec::new();
    let mut previous_deployed: Option<&str> = None;
    for state in states {
        let id = format!("deploy:{}:{}", state.state, state.sha);
        let (action, result) = match state.state {
            "deployed" => ("deploy.succeeded", "succeeded"),
            "failed" => ("deploy.failed", "failed"),
            _ => ("deploy.refused", "refused"),
        };
        if !known.contains(&id) {
            events.push(Imported {
                source: "deploy",
                source_event_id: id.clone(),
                occurred_at: state.at,
                action: action.to_owned(),
                actor_kind: "system",
                actor_external: None,
                object_type: "commit".to_owned(),
                object_id: state.sha.clone(),
                after: None,
                applied_via: ("deploy", state.sha.clone()),
                result,
                error: state
                    .reason
                    .clone()
                    .map(|reason| reason.chars().take(1000).collect()),
                change_id: None,
            });
            if state.state == "deployed" {
                for number in shipped(previous_deployed, &state.sha)? {
                    events.push(Imported {
                        source: "deploy",
                        source_event_id: format!("{id}:pr:{number}"),
                        occurred_at: state.at,
                        action: action.to_owned(),
                        actor_kind: "system",
                        actor_external: None,
                        object_type: "pull_request".to_owned(),
                        object_id: number.to_string(),
                        after: Some(json!({ "commit": state.sha })),
                        applied_via: ("deploy", state.sha.clone()),
                        result,
                        error: None,
                        change_id: Some(pull_request_change(number)),
                    });
                }
            }
        }
        if state.state == "deployed" {
            previous_deployed = Some(&state.sha);
        }
    }
    Ok(events)
}

// ---------------------------------------------------------------- host_command

#[derive(Debug, Deserialize)]
struct JournalLine {
    #[serde(rename = "__CURSOR")]
    cursor: String,
    #[serde(rename = "_BOOT_ID")]
    boot_id: String,
    #[serde(rename = "__SEQNUM")]
    seqnum: String,
    #[serde(rename = "__REALTIME_TIMESTAMP")]
    realtime: String,
    #[serde(rename = "_UID")]
    uid: Option<String>,
    #[serde(rename = "MESSAGE")]
    message: Value,
}

/// The words of a declared argument: a lane list, a generation, a unit; never a free text.
fn is_argument(word: &str) -> bool {
    word == "--dry-run"
        || (!word.is_empty()
            && word.len() <= 64
            && word
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_,.".contains(&b)))
}

/// The subjects that name a kind of target rather than lanes, and the kind each names.
fn target_kind(subject: &str) -> Option<&'static str> {
    match subject {
        "gold" => Some("gold_unit"),
        "job" => Some("job"),
        "silver" => Some("silver_lane"),
        "raon" => Some("raon"),
        "health" => Some("release"),
        _ => None,
    }
}

/// One operator start as the chain keeps it, or none for a line that is not one.
pub(crate) fn host_command_event(
    boot_id: &str,
    seqnum: &str,
    at: DateTime<Utc>,
    message: &str,
    actions: &BTreeSet<String>,
    uid_of: &dyn Fn(&str) -> Option<u32>,
    host: &str,
) -> Option<Imported> {
    let (command, rest) = message.strip_prefix("start ")?.split_once(" who=")?;
    let who = rest.split_whitespace().next().unwrap_or("");
    let mut words = command.split_whitespace();
    let subject = words.next()?;
    let mut rest_words: Vec<&str> = words.collect();
    let (object_type, object_id) = match target_kind(subject) {
        Some(kind) if !rest_words.is_empty() && kind != "release" => {
            // `gold rebuild <unit>`, `job status <id>`: the action first, then what it acts on.
            let action = rest_words.remove(0);
            let target = rest_words.first().copied().filter(|word| is_argument(word));
            rest_words.insert(0, action);
            (kind.to_owned(), target.unwrap_or(subject).to_owned())
        }
        Some(kind) => (kind.to_owned(), subject.to_owned()),
        None => ("lane".to_owned(), subject.to_owned()),
    };
    let verb = rest_words.first().copied().unwrap_or("").replace('-', "_");
    let listed = actions.contains(&format!("host_command.{verb}"));
    let action = if listed {
        format!("host_command.{verb}")
    } else {
        "host_command.other".to_owned()
    };
    let arguments: Vec<&str> = if listed {
        rest_words
            .iter()
            .skip(1)
            .copied()
            .filter(|word| is_argument(word))
            .collect()
    } else {
        Vec::new()
    };
    let (actor_kind, actor_external) = if SYSTEM_WHO.contains(&who) {
        ("system", None)
    } else {
        match uid_of(who) {
            Some(uid) => ("unlinked", Some(format!("unix:{uid}@{host}"))),
            None => ("unlinked", None),
        }
    };
    let id = format!("journal:{boot_id}:{seqnum}");
    Some(Imported {
        source: "host_command",
        source_event_id: id.clone(),
        occurred_at: at,
        action,
        actor_kind,
        actor_external,
        object_type,
        object_id: object_id.chars().take(300).collect(),
        after: Some(json!({ "arguments": arguments })),
        applied_via: ("host_command", id),
        // The journal holds the start as it was asked, before the script decided it may run.
        result: "requested",
        error: None,
        change_id: None,
    })
}

/// The uid of each login in a passwd file.
fn passwd_uids(path: &Path) -> BTreeMap<String, u32> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split(':');
            let name = fields.next()?;
            let uid = fields.nth(1)?.parse().ok()?;
            Some((name.to_owned(), uid))
        })
        .collect()
}

/// The host commands after `cursor`, and the cursor of the last line read. A cursor the journal no
/// longer holds is an error: the lines between are lost to the chain, which the caller records.
fn read_journal(
    journalctl: &str,
    cursor: Option<&str>,
) -> anyhow::Result<(Vec<JournalLine>, Option<String>)> {
    let mut command = std::process::Command::new(journalctl);
    command.args([
        "--no-pager",
        "-o",
        "json",
        &format!("SYSLOG_IDENTIFIER={JOURNAL_TAG}"),
        "_UID=0",
    ]);
    if let Some(cursor) = cursor {
        command.arg(format!("--after-cursor={cursor}"));
    }
    let output = command.output().context("running journalctl")?;
    if !output.status.success() {
        bail!(
            "journalctl refused the cursor or the journal: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let mut lines = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: JournalLine =
            serde_json::from_str(line).context("a journal line is not JSON")?;
        lines.push(parsed);
    }
    let last = lines.last().map(|line| line.cursor.clone());
    Ok((lines, last))
}

// ---------------------------------------------------------------- writing

async fn append_batch(
    conn: &mut PgConnection,
    events: &[Imported],
    cursor: Option<(&str, &str, &str)>,
) -> anyhow::Result<u64> {
    let mut tx = conn.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '10s'")
        .execute(&mut *tx)
        .await?;
    let mut appended = 0;
    for event in events {
        let seq: Option<i64> = sqlx::query_scalar(
            "SELECT audit.append($1, $2, $3, $4, $5, 'execute', $6, NULL, NULL, $7, false, NULL,
                                 $8, $9, NULL, $10, NULL, $11, $12, $13, $14, $15)",
        )
        .bind(Uuid::new_v4())
        .bind(event.occurred_at)
        .bind(event.source)
        .bind(&event.source_event_id)
        .bind(&event.action)
        .bind(event.actor_kind)
        .bind(&event.actor_external)
        .bind(&event.object_type)
        .bind(&event.object_id)
        .bind(&event.after)
        .bind(event.applied_via.0)
        .bind(&event.applied_via.1)
        .bind(event.result)
        .bind(&event.error)
        .bind(event.change_id)
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("appending {}", event.source_event_id))?;
        appended += u64::from(seq.is_some());
    }
    if let Some((source, cursor, state)) = cursor {
        sqlx::query("SELECT audit.advance_cursor($1, $2, $3)")
            .bind(source)
            .bind(cursor)
            .bind(state)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(appended)
}

async fn append_all(
    conn: &mut PgConnection,
    events: &[Imported],
    cursor: Option<(&str, &str, &str)>,
) -> anyhow::Result<u64> {
    let mut appended = 0;
    let chunks: Vec<&[Imported]> = events.chunks(BATCH).collect();
    if chunks.is_empty() {
        if cursor.is_some() {
            append_batch(conn, &[], cursor).await?;
        }
        return Ok(0);
    }
    let last = chunks.len() - 1;
    for (index, chunk) in chunks.into_iter().enumerate() {
        appended += append_batch(conn, chunk, if index == last { cursor } else { None }).await?;
    }
    Ok(appended)
}

#[derive(Deserialize)]
struct Contract {
    actions: Vec<ContractAction>,
}

#[derive(Deserialize)]
struct ContractAction {
    action: String,
}

fn listed_actions(path: &Path) -> anyhow::Result<BTreeSet<String>> {
    let contract: Contract = serde_json::from_str(
        &std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?,
    )?;
    Ok(contract
        .actions
        .into_iter()
        .map(|entry| entry.action)
        .collect())
}

pub async fn run() -> anyhow::Result<()> {
    let deploy_root = PathBuf::from(
        optional_env_value(DEPLOY_STATE_ENV)?.unwrap_or_else(|| DEFAULT_DEPLOY_STATE.to_owned()),
    );
    let mirror = PathBuf::from(
        optional_env_value(GIT_MIRROR_ENV)?.unwrap_or_else(|| DEFAULT_GIT_MIRROR.to_owned()),
    );
    let contract = PathBuf::from(
        optional_env_value(CONTRACT_ENV)?.unwrap_or_else(|| DEFAULT_CONTRACT.to_owned()),
    );
    let journalctl = optional_env_value(JOURNALCTL_ENV)?.unwrap_or_else(|| "journalctl".to_owned());
    let passwd =
        PathBuf::from(optional_env_value(PASSWD_ENV)?.unwrap_or_else(|| "/etc/passwd".to_owned()));
    let host = optional_env_value(HOST_ENV)?.unwrap_or_else(|| "ai-server".to_owned());
    let actions = listed_actions(&contract)?;

    let mut conn = PgConnection::connect(&required_env_value("DATABASE_URL")?).await?;
    // The host's credential is the database owner's; the chain is written as the importer, so the
    // database refuses what the importer may not write (ADR-0569 §3).
    sqlx::query(&format!("SET SESSION AUTHORIZATION {IMPORTER_ROLE}"))
        .execute(&mut conn)
        .await?;
    let cursors: BTreeMap<String, (String, String)> = sqlx::query(
        "SELECT DISTINCT ON (source) source, cursor, state FROM audit.import_cursor
         ORDER BY source, advanced_at DESC",
    )
    .fetch_all(&mut conn)
    .await?
    .iter()
    .map(|row| -> Result<_, sqlx::Error> {
        Ok((
            row.try_get("source")?,
            (row.try_get("cursor")?, row.try_get("state")?),
        ))
    })
    .collect::<Result<_, _>>()?;

    let mut appended = 0;
    for source in WAITING_SOURCES {
        if !cursors.contains_key(source) {
            append_all(
                &mut conn,
                &[],
                Some((source, "none", "waiting_for_credential")),
            )
            .await?;
        }
    }

    // deploy: every file, every run; the chain already holds most of them.
    let known: BTreeSet<String> =
        sqlx::query_scalar("SELECT source_event_id FROM audit.event WHERE source = 'deploy'")
            .fetch_all(&mut conn)
            .await?
            .into_iter()
            .collect();
    let states = read_deploy_states(&deploy_root)?;
    let mut shipped =
        |previous: Option<&str>, sha: &str| shipped_pull_requests(&mirror, previous, sha);
    let deploys = deploy_events(&states, &known, &mut shipped)?;
    let deploy_cursor = states
        .last()
        .map(|state| format!("{}:{}", state.state, state.sha));
    let deploy_moved = deploy_cursor
        .as_deref()
        .is_some_and(|cursor| cursors.get("deploy").map(|(c, _)| c.as_str()) != Some(cursor));
    appended += append_all(
        &mut conn,
        &deploys,
        deploy_cursor
            .as_deref()
            .filter(|_| deploy_moved)
            .map(|cursor| ("deploy", cursor, "running")),
    )
    .await?;

    // host_command: the journal after the last cursor.
    let uids = passwd_uids(&passwd);
    let uid_of = |who: &str| uids.get(who).copied();
    // After a gap the journal is read from its start again: what the chain holds is written once.
    let previous = cursors
        .get("host_command")
        .filter(|(_, state)| state == "running")
        .map(|(cursor, _)| cursor.as_str());
    match read_journal(&journalctl, previous) {
        Ok((lines, last)) => {
            let events: Vec<Imported> = lines
                .iter()
                .filter(|line| line.uid.as_deref() == Some("0"))
                .filter_map(|line| {
                    let micros: i64 = line.realtime.parse().ok()?;
                    let at = Utc.timestamp_micros(micros).single()?;
                    let message = line.message.as_str()?;
                    host_command_event(
                        &line.boot_id,
                        &line.seqnum,
                        at,
                        message,
                        &actions,
                        &uid_of,
                        &host,
                    )
                })
                .collect();
            appended += append_all(
                &mut conn,
                &events,
                last.as_deref()
                    .map(|cursor| ("host_command", cursor, "running")),
            )
            .await?;
        }
        Err(error) => {
            // The lines between the cursor and now are gone from the chain: say so, in the chain,
            // and fail so the unit's failure alert says it too (ADR-0569 §6).
            let gap = Imported {
                source: "audit",
                source_event_id: format!("import_gap:host_command:{}", Utc::now().timestamp()),
                occurred_at: Utc::now(),
                action: "audit.import_gap".to_owned(),
                actor_kind: "system",
                actor_external: None,
                object_type: "audit_source".to_owned(),
                object_id: "host_command".to_owned(),
                after: Some(json!({ "cursor": previous })),
                applied_via: ("api", "import-audit-events".to_owned()),
                result: "failed",
                error: Some(error.to_string().chars().take(1000).collect()),
                change_id: None,
            };
            append_batch(&mut conn, std::slice::from_ref(&gap), None).await?;
            append_all(
                &mut conn,
                &[],
                Some(("host_command", previous.unwrap_or("none"), "gap")),
            )
            .await?;
            return Err(error.context("the host command journal has a gap"));
        }
    }

    if appended > 0 {
        println!("audit-import changed appended={appended}");
    } else {
        println!("audit-import unchanged appended=0");
    }
    Ok(())
}

#[cfg(test)]
#[path = "audit_import_tests.rs"]
mod tests;
