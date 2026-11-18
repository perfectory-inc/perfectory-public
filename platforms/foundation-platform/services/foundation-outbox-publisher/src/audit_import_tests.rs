//! The audit importer's parsing (root ADR-0569 §6): what a deploy and a host command become, and
//! that reading them twice adds nothing.

use std::collections::BTreeSet;

use chrono::{TimeZone, Utc};

use super::{
    deploy_events, host_command_event, pull_request_change, read_deploy_states,
    subject_pull_request, DeployState,
};

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccc";

fn actions() -> BTreeSet<String> {
    [
        "host_command.cutover",
        "host_command.rebuild",
        "host_command.bake",
        "host_command.other",
    ]
    .iter()
    .map(|action| (*action).to_owned())
    .collect()
}

fn uid(who: &str) -> Option<u32> {
    (who == "perfectory").then_some(1000)
}

#[test]
fn a_squash_subject_names_its_pull_request() {
    assert_eq!(subject_pull_request("fix(x): a thing (#1418)"), Some(1418));
    assert_eq!(subject_pull_request("fix(x): (#12) in the middle"), None);
    assert_eq!(subject_pull_request("no number"), None);
}

#[test]
fn the_state_files_are_read_with_their_times_and_a_refusal_with_its_reason(
) -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for dir in ["deployed", "failed", "refused"] {
        std::fs::create_dir(root.path().join(dir))?;
    }
    std::fs::write(
        root.path().join("deployed").join(A),
        "2026-11-14T03:31:49Z\n",
    )?;
    std::fs::write(root.path().join("failed").join(B), "2026-11-13T23:34:56Z\n")?;
    std::fs::write(
        root.path().join("refused").join(C),
        "failed: Rust=failure\n",
    )?;
    std::fs::write(root.path().join("deployed").join("not-a-sha"), "x")?;
    let states = read_deploy_states(root.path())?;
    assert_eq!(states.len(), 3);
    let deployed = states
        .iter()
        .find(|s| s.state == "deployed")
        .ok_or("no deployed")?;
    assert_eq!(
        deployed.at,
        Utc.with_ymd_and_hms(2026, 11, 14, 3, 31, 49).unwrap()
    );
    let refused = states
        .iter()
        .find(|s| s.state == "refused")
        .ok_or("no refused")?;
    assert_eq!(refused.reason.as_deref(), Some("failed: Rust=failure"));
    Ok(())
}

#[test]
fn a_deploy_is_one_event_and_one_more_per_pull_request_it_shipped_and_only_once(
) -> Result<(), Box<dyn std::error::Error>> {
    let at = |hour| Utc.with_ymd_and_hms(2026, 11, 14, hour, 0, 0).unwrap();
    let states = vec![
        DeployState {
            state: "deployed",
            sha: A.to_owned(),
            at: at(1),
            reason: None,
        },
        DeployState {
            state: "failed",
            sha: B.to_owned(),
            at: at(2),
            reason: None,
        },
        DeployState {
            state: "deployed",
            sha: C.to_owned(),
            at: at(3),
            reason: None,
        },
    ];
    let mut asked = Vec::new();
    let mut shipped = |previous: Option<&str>, sha: &str| {
        asked.push((previous.map(ToOwned::to_owned), sha.to_owned()));
        Ok(if sha == C { vec![1410, 1418] } else { vec![] })
    };
    let events = deploy_events(&states, &BTreeSet::new(), &mut shipped)?;
    let ids: Vec<&str> = events.iter().map(|e| e.source_event_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            format!("deploy:deployed:{A}"),
            format!("deploy:failed:{B}"),
            format!("deploy:deployed:{C}"),
            format!("deploy:deployed:{C}:pr:1410"),
            format!("deploy:deployed:{C}:pr:1418"),
        ]
    );
    // The range of a deploy starts at the deploy before it, not at a failed one.
    assert_eq!(asked[1], (Some(A.to_owned()), C.to_owned()));
    assert_eq!(events[3].change_id, Some(pull_request_change(1410)));
    assert_eq!(events[1].result, "failed");

    let known: BTreeSet<String> = ids.iter().map(|id| (*id).to_owned()).collect();
    let again = deploy_events(&states, &known, &mut |_, _| Ok(vec![1410, 1418]))?;
    assert!(again.is_empty(), "a deploy read twice was appended twice");
    Ok(())
}

#[test]
fn a_host_command_keeps_its_action_subject_and_arguments_not_its_reason() {
    let at = Utc.with_ymd_and_hms(2026, 11, 6, 9, 0, 0).unwrap();
    let event = host_command_event(
        "b88b",
        "17708346",
        at,
        "start parcel-ownership cutover 2 --dry-run who=perfectory requested_at=1792828752 reason=secret words here",
        &actions(),
        &uid,
        "ai-server",
    )
    .expect("a start");
    assert_eq!(event.source_event_id, "journal:b88b:17708346");
    assert_eq!(event.action, "host_command.cutover");
    assert_eq!(
        (event.object_type.as_str(), event.object_id.as_str()),
        ("lane", "parcel-ownership")
    );
    assert_eq!(event.actor_external.as_deref(), Some("unix:1000@ai-server"));
    assert_eq!(
        event.after,
        Some(serde_json::json!({ "arguments": ["2", "--dry-run"] }))
    );
    let text = format!("{event:?}");
    assert!(
        !text.contains("secret"),
        "the reason reached the chain: {text}"
    );
}

#[test]
fn the_auto_promotion_is_the_system_and_an_unlisted_action_keeps_nothing() {
    let at = Utc.with_ymd_and_hms(2026, 11, 6, 9, 0, 0).unwrap();
    let auto = host_command_event(
        "b",
        "1",
        at,
        "start parcel-energy cutover 3 who=auto-promote requested_at=1 reason=x",
        &actions(),
        &uid,
        "ai-server",
    )
    .expect("a start");
    assert_eq!(
        (auto.actor_kind, auto.actor_external.as_deref()),
        ("system", None)
    );

    let other = host_command_event(
        "b",
        "2",
        at,
        "start parcel teleport somewhere who=perfectory requested_at=1 reason=x",
        &actions(),
        &uid,
        "ai-server",
    )
    .expect("a start");
    assert_eq!(other.action, "host_command.other");
    assert_eq!(other.after, Some(serde_json::json!({ "arguments": [] })));

    let gold = host_command_event(
        "b",
        "3",
        at,
        "start gold rebuild parcel_ownership who=nobody requested_at=1 reason=x",
        &actions(),
        &uid,
        "ai-server",
    )
    .expect("a start");
    assert_eq!(
        (gold.object_type.as_str(), gold.object_id.as_str()),
        ("gold_unit", "parcel_ownership")
    );
    assert_eq!(gold.action, "host_command.rebuild");
    assert_eq!(
        (gold.actor_kind, gold.actor_external.as_deref()),
        ("unlinked", None)
    );

    assert!(
        host_command_event("b", "4", at, "not a start", &actions(), &uid, "ai-server").is_none()
    );
}
