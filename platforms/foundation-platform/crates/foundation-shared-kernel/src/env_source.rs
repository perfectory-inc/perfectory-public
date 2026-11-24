//! Where a setting is read from, and what a program says it needs before it reads anything.
//!
//! A command's configuration is loaded from an [`EnvSource`] and described by the same
//! [`Requirement`]s it is loaded by, so the program itself can say which names it cannot run
//! without (`describe-requirements`) and a check can hold every unit's environment files to that
//! before a release goes live. 2026-11-21..24 the parcel registry's steps had moved into the
//! publisher, which reads the lakehouse writer keys, while its unit loaded no file that holds
//! them: nothing compared what the code reads with what the unit is given, and every run failed.

use std::collections::BTreeMap;

/// A source of settings by name. A value that is empty or only whitespace is absent.
pub trait EnvSource {
    /// The raw value under `name`, if the source holds one.
    fn raw(&self, name: &str) -> Option<String>;

    /// The trimmed value under `name`; `None` when it is absent or blank.
    fn get(&self, name: &str) -> Option<String> {
        self.raw(name)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    }
}

/// The process environment.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn raw(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

impl EnvSource for BTreeMap<String, String> {
    fn raw(&self, name: &str) -> Option<String> {
        self.get(name).cloned()
    }
}

/// One thing a configuration needs from its [`EnvSource`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Requirement {
    /// The name must hold a value.
    Required(&'static str),
    /// At least one of the names must hold a value; the first that does is used.
    OneOf(&'static [&'static str]),
    /// The name is read when present and has a default otherwise.
    Optional(&'static str),
}

impl Requirement {
    /// Whether `source` meets this requirement.
    #[must_use]
    pub fn is_met_by(&self, source: &impl EnvSource) -> bool {
        match self {
            Self::Required(name) => source.get(name).is_some(),
            Self::OneOf(names) => names.iter().any(|name| source.get(name).is_some()),
            Self::Optional(_) => true,
        }
    }

    /// The requirement as one line of `describe-requirements`: `required NAME`,
    /// `one-of NAME NAME` or `optional NAME`.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Required(name) => format!("required {name}"),
            Self::OneOf(names) => format!("one-of {}", names.join(" ")),
            Self::Optional(name) => format!("optional {name}"),
        }
    }
}

/// Several configurations' requirements as one list, each once, in the order first met.
///
/// A name one configuration reads optionally and another requires is required.
#[must_use]
pub fn merged(lists: &[&[Requirement]]) -> Vec<Requirement> {
    let mut all: Vec<Requirement> = Vec::new();
    for requirement in lists.iter().flat_map(|list| list.iter()) {
        if !all.contains(requirement) {
            all.push(*requirement);
        }
    }
    let required: Vec<&'static str> = all
        .iter()
        .filter_map(|requirement| match requirement {
            Requirement::Required(name) => Some(*name),
            _ => None,
        })
        .collect();
    all.retain(|requirement| {
        !matches!(requirement, Requirement::Optional(name) if required.contains(name))
    });
    all
}

/// The first of `requirements` that `source` does not meet.
#[must_use]
pub fn first_unmet<'a>(
    requirements: &'a [Requirement],
    source: &impl EnvSource,
) -> Option<&'a Requirement> {
    requirements
        .iter()
        .find(|requirement| !requirement.is_met_by(source))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn a_blank_value_is_absent_and_a_value_is_trimmed() {
        let held = source(&[("SYNTHETIC_A", "  value "), ("SYNTHETIC_B", "   ")]);
        assert_eq!(
            EnvSource::get(&held, "SYNTHETIC_A"),
            Some("value".to_owned())
        );
        assert_eq!(EnvSource::get(&held, "SYNTHETIC_B"), None);
        assert_eq!(EnvSource::get(&held, "SYNTHETIC_C"), None);
    }

    #[test]
    fn merged_lists_name_each_requirement_once_and_required_wins_over_optional() {
        let catalog = [
            Requirement::Required("SYNTHETIC_URI"),
            Requirement::Optional("SYNTHETIC_TOKEN"),
        ];
        let reader = [
            Requirement::Required("SYNTHETIC_URI"),
            Requirement::Required("SYNTHETIC_TOKEN"),
            Requirement::Optional("SYNTHETIC_REGION"),
        ];
        assert_eq!(
            merged(&[&catalog, &reader]),
            [
                Requirement::Required("SYNTHETIC_URI"),
                Requirement::Required("SYNTHETIC_TOKEN"),
                Requirement::Optional("SYNTHETIC_REGION"),
            ]
        );
    }

    #[test]
    fn a_requirement_is_met_by_what_it_names_and_says_itself_in_one_line() {
        let held = source(&[("SYNTHETIC_ENDPOINT", "https://synthetic.invalid")]);
        let one_of = Requirement::OneOf(&["SYNTHETIC_ENDPOINT", "SYNTHETIC_ACCOUNT_ID"]);
        let required = Requirement::Required("SYNTHETIC_BUCKET");
        let optional = Requirement::Optional("SYNTHETIC_REGION");
        assert!(one_of.is_met_by(&held));
        assert!(!required.is_met_by(&held));
        assert!(optional.is_met_by(&held));
        assert_eq!(
            first_unmet(&[one_of, required, optional], &held),
            Some(&required)
        );
        assert_eq!(
            [one_of.describe(), required.describe(), optional.describe()],
            [
                "one-of SYNTHETIC_ENDPOINT SYNTHETIC_ACCOUNT_ID".to_owned(),
                "required SYNTHETIC_BUCKET".to_owned(),
                "optional SYNTHETIC_REGION".to_owned(),
            ]
        );
    }
}
