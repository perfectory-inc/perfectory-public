//! `describe-requirements <command>...`: what each named command needs from its unit's environment.
//!
//! The answer comes from the requirements the command's own configuration is loaded by, so the
//! check that holds a unit's environment files to it (`scripts/deploy/runtime_secrets.py`, in CI
//! and at release verify) compares the unit with the program, not with a list beside it.
//! 2026-11-21..24 the parcel registry's steps read the lakehouse writer keys and its unit loaded
//! no file that holds them; every run failed at its first probe.
//!
//! One JSON object on stdout, a key per command asked:
//! - `{"status": "described", "requirements": ["required NAME", "one-of NAME NAME", "optional NAME"]}`
//! - `{"status": "undescribed"}`: a command whose configuration does not say its requirements yet;
//! - `{"status": "unknown"}`: no such command.

use foundation_shared_kernel::env_source::Requirement;
use serde_json::{json, Map, Value};

/// The word that asks.
pub(crate) const COMMAND: &str = "describe-requirements";

/// The requirements of a command that says them.
fn described(command: &str) -> Option<Vec<Requirement>> {
    match command {
        "probe-parcel-registry" | "load-parcel-registry" => {
            Some(crate::parcel_registry_load::requirements())
        }
        _ => None,
    }
}

/// The answer for `commands`; `known` says whether a word is a command of this program.
pub(crate) fn answer(commands: &[String], known: impl Fn(&str) -> bool) -> Value {
    let mut all = Map::new();
    for command in commands {
        let one = match (known(command), described(command)) {
            (false, _) => json!({"status": "unknown"}),
            (true, None) => json!({"status": "undescribed"}),
            (true, Some(requirements)) => json!({
                "status": "described",
                "requirements": requirements.iter().map(Requirement::describe).collect::<Vec<_>>(),
            }),
        };
        all.insert(command.clone(), one);
    }
    Value::Object(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asked(commands: &[&str]) -> Value {
        let commands: Vec<String> = commands.iter().map(|word| (*word).to_owned()).collect();
        answer(&commands, |word| word != "no-such-command")
    }

    #[test]
    fn the_registry_commands_say_the_writer_keys_the_unit_did_not_load() {
        let found = asked(&["probe-parcel-registry", "load-parcel-registry"]);
        for command in ["probe-parcel-registry", "load-parcel-registry"] {
            assert_eq!(found[command]["status"], "described");
            let lines: Vec<&str> = found[command]["requirements"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            for line in [
                "one-of FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID",
                "required FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
                "required FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
                "required FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
                "required FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
                // A DuckDB read cannot do without the token the catalog alone reads optionally.
                "required FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN",
            ] {
                assert!(lines.contains(&line), "{command} lacks `{line}` in {lines:?}");
            }
            assert!(
                !lines.contains(&"optional FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN"),
                "a required name is not also optional"
            );
        }
    }

    #[test]
    fn a_command_that_does_not_say_yet_and_a_word_that_is_no_command_are_told_apart() {
        let found = asked(&["run", "no-such-command"]);
        assert_eq!(found["run"], json!({"status": "undescribed"}));
        assert_eq!(found["no-such-command"], json!({"status": "unknown"}));
    }
}
