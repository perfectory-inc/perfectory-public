//! `describe-checks <command>...`: the checks each named command reports, and on which asset.
//!
//! The answer is read from the places the run reads its checks (`kind_versions_build::declared`),
//! so whoever declares the checks (the code location, from a manifest its image build writes with
//! this answer: root ADR-0615 §7 note 2026-11-25) declares what the program reports, not a list
//! beside it. A run reports a check only through the Pipes message of `crate::pipes`, and refuses a
//! name this answer does not hold.
//!
//! One JSON object on stdout, a key per command asked:
//! - `{"status": "described", "asset_key": "iceberg/gold/…", "checks": [{"name", "blocking"}]}`;
//! - `{"status": "undescribed"}`: a command that reports no check through this answer yet;
//! - `{"status": "unknown"}`: no such command.

use serde_json::{json, Map, Value};

use crate::kind_versions_build::{self, KIND_BUILDS};

/// The word that asks.
pub(crate) const COMMAND: &str = "describe-checks";

/// The asset and checks of a command that reports through this answer.
fn described(command: &str) -> anyhow::Result<Option<Value>> {
    let Some(kind) = KIND_BUILDS.iter().find(|kind| kind.job == command) else {
        return Ok(None);
    };
    let checks: Vec<Value> = kind_versions_build::declared(kind)?
        .iter()
        .map(|check| json!({"name": check.name, "blocking": check.blocking}))
        .collect();
    Ok(Some(json!({
        "status": "described",
        "asset_key": crate::pipes::asset_key(kind.table),
        "checks": checks,
    })))
}

/// The answer for `commands`; `known` says whether a word is a command of this program.
///
/// # Errors
/// Returns an error when a lane's checks cannot be read (its contract does not parse).
pub(crate) fn answer(commands: &[String], known: impl Fn(&str) -> bool) -> anyhow::Result<Value> {
    let mut all = Map::new();
    for command in commands {
        let one = if known(command) {
            described(command)?.unwrap_or_else(|| json!({"status": "undescribed"}))
        } else {
            json!({"status": "unknown"})
        };
        all.insert(command.clone(), one);
    }
    Ok(Value::Object(all))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asked(commands: &[&str]) -> anyhow::Result<Value> {
        let commands: Vec<String> = commands.iter().map(|word| (*word).to_owned()).collect();
        answer(&commands, |word| word != "no-such-command")
    }

    fn names(found: &Value) -> Vec<&str> {
        found["checks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|check| check["name"].as_str())
            .collect()
    }

    /// 2026-11-22: the 공매 shadow's documents were refused by the schema only in the takeover
    /// verdict; the build's own checks reached no one. Every lane says its checks, on its table.
    #[test]
    fn every_lane_says_the_counts_its_documents_and_its_own_checks_on_its_versions_table(
    ) -> anyhow::Result<()> {
        let jobs: Vec<&str> = KIND_BUILDS.iter().map(|kind| kind.job).collect();
        let found = asked(&jobs)?;
        for kind in KIND_BUILDS {
            let one = &found[kind.job];
            assert_eq!(one["status"], "described", "{}", kind.job);
            assert_eq!(
                one["asset_key"],
                format!("iceberg/{}", kind.table.replace('.', "/"))
            );
            let names = names(one);
            assert_eq!(names.first(), Some(&"rows_and_keys"), "{}", kind.job);
            for check in kind.stage.result_checks() {
                assert!(
                    names.contains(&check.name),
                    "{} lacks {}",
                    kind.job,
                    check.name
                );
            }
            for check in kind.stage.national_checks() {
                assert!(
                    names.contains(&check.name),
                    "{} lacks {}",
                    kind.job,
                    check.name
                );
            }
            let unique: std::collections::BTreeSet<&str> = names.iter().copied().collect();
            assert_eq!(
                unique.len(),
                names.len(),
                "{} names a check twice",
                kind.job
            );
        }
        let auction = names(&found["build-parcel-public-auction-versions"]);
        assert!(auction.contains(&"changed_documents_hold_their_schema"));
        assert!(auction.contains(&"rounds_carry_their_media"));
        // The served boundaries: no by-PNU lane bakes them, so their documents are not checked.
        let boundaries = names(&found["build-parcel-boundary-served-versions"]);
        assert!(!boundaries.contains(&"changed_documents_hold_their_schema"));
        Ok(())
    }

    #[test]
    fn a_command_that_reports_no_check_and_a_word_that_is_no_command_are_told_apart(
    ) -> anyhow::Result<()> {
        let found = asked(&["run", "no-such-command"])?;
        assert_eq!(found["run"], json!({"status": "undescribed"}));
        assert_eq!(found["no-such-command"], json!({"status": "unknown"}));
        Ok(())
    }

    /// A run refuses to report a check the answer does not hold: the code location would refuse
    /// the run.
    #[test]
    fn a_check_the_answer_does_not_hold_is_not_reported() -> anyhow::Result<()> {
        let declared = kind_versions_build::declared(&kind_versions_build::FACTORIES_BUILD)?;
        let record = json!({"table": "gold.parcel_factories_versions", "check": "not_declared",
                            "passed": true});
        let refused =
            kind_versions_build::report("gold.parcel_factories_versions", &declared, &record);
        assert!(refused.is_err());
        let counts = json!({"table": "gold.parcel_factories_versions", "check": "rows_and_keys",
                            "passed": true, "sido": "99", "rows": 1});
        kind_versions_build::report("gold.parcel_factories_versions", &declared, &counts)
    }
}
