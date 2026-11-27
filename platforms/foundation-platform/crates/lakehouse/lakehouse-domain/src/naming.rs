//! The table-name rules of the naming registry (root ADR-0626 rules 6-7, §5).
//!
//! The registry (`config/naming.contract.json`, generated from `definitions/platform.definition.json`,
//! never committed: root ADR-0418) is the one home of the namespaces, the role suffix, the words a
//! table name may not hold, and the temporary rename work list. This module reads it and holds every
//! table name to it: the contract test `tests/naming.rs` (G2) and the catalog reconciler before it
//! creates a table (G3).
//!
//! A table name is `<namespace>.<entity>[_<qualifier>…][__<role>]`: a namespace the registry declares,
//! lowercase snake_case words, no storage, version, date, environment or test word, a singular last
//! word, and as its only suffix a generated companion role (`__changes`). A table may keep an old name
//! only while the rename work list holds it; its component's switch PR deletes the entry.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::Deserialize;
use thiserror::Error;

const NAMING_CONTRACT: &str = include_str!("../../../../config/naming.contract.json");

/// The identifier grammar this module implements, which the registry must state (a drift test holds
/// them equal, so the grammar has one home and this is its reader).
pub const IDENTIFIER_PATTERN: &str = "^[a-z][a-z0-9]*(_[a-z0-9]+)*$";
/// The word patterns this module implements: a version word and a 6-8 digit date.
pub const FORBIDDEN_WORD_PATTERNS: [&str; 2] = ["v[0-9]+", "[0-9]{6,8}"];

/// The registry's table rules.
#[derive(Debug, Deserialize)]
pub struct NamingRegistry {
    identifier: Identifier,
    /// The catalog namespaces, one per layer.
    pub namespaces: Vec<String>,
    /// The generated companion roles (`<table>__<role>`).
    pub role_suffixes: Vec<String>,
    /// Last words that end in `s` and are singular.
    pub singular_words_ending_in_s: Vec<String>,
    /// Words a table name may not hold.
    pub forbidden_table_words: Vec<String>,
    forbidden_table_word_patterns: Vec<String>,
    /// The rename work list.
    pub renames: Renames,
}

#[derive(Debug, Deserialize)]
struct Identifier {
    pattern: String,
}

/// The temporary rename work list (root ADR-0626 §5).
#[derive(Debug, Deserialize)]
pub struct Renames {
    /// Old name -> the name its component's switch creates.
    pub tables: BTreeMap<String, String>,
    /// Old names replaced by their versioned twin under the entity name.
    pub not_recreated: Vec<String>,
}

impl NamingRegistry {
    /// The grammar strings the registry states, beside the ones this module implements.
    #[must_use]
    pub fn stated_grammar(&self) -> (&str, &[String]) {
        (
            &self.identifier.pattern,
            &self.forbidden_table_word_patterns,
        )
    }

    /// Whether `name` may keep its name for now: it is on the rename work list.
    #[must_use]
    pub fn is_pending_rename(&self, name: &str) -> bool {
        self.renames.tables.contains_key(name)
            || self.renames.not_recreated.iter().any(|n| n == name)
    }

    /// Holds `name` to the table rules (root ADR-0626 rule 7).
    ///
    /// # Errors
    /// The first rule `name` breaks.
    pub fn check_table_name(&self, name: &str) -> Result<(), TableNameError> {
        let Some((namespace, table)) = name.split_once('.') else {
            return Err(TableNameError::NotQualified(name.to_owned()));
        };
        if !self.namespaces.iter().any(|n| n == namespace) {
            return Err(TableNameError::Namespace {
                name: name.to_owned(),
                namespaces: self.namespaces.clone(),
            });
        }
        let (entity, role) = match table.split_once("__") {
            Some((entity, role)) => (entity, Some(role)),
            None => (table, None),
        };
        if let Some(role) = role {
            if !self.role_suffixes.iter().any(|r| r == role) {
                return Err(TableNameError::RoleSuffix {
                    name: name.to_owned(),
                    role: role.to_owned(),
                });
            }
        }
        if !is_identifier(entity) {
            return Err(TableNameError::Grammar(name.to_owned()));
        }
        let words: Vec<&str> = entity.split('_').collect();
        if let Some(word) = words.iter().find(|word| self.is_forbidden(word)) {
            return Err(TableNameError::ForbiddenWord {
                name: name.to_owned(),
                word: (*word).to_owned(),
            });
        }
        if let Some(last) = words.last() {
            if last.ends_with('s') && !self.singular_words_ending_in_s.iter().any(|w| w == last) {
                return Err(TableNameError::PluralLastWord {
                    name: name.to_owned(),
                    word: (*last).to_owned(),
                });
            }
        }
        Ok(())
    }

    fn is_forbidden(&self, word: &str) -> bool {
        self.forbidden_table_words.iter().any(|w| w == word)
            || is_version_word(word)
            || ((6..=8).contains(&word.len()) && word.bytes().all(|b| b.is_ascii_digit()))
    }
}

/// `^[a-z][a-z0-9]*(_[a-z0-9]+)*$`: lowercase words joined by single underscores, starting with a letter.
#[must_use]
pub fn is_identifier(text: &str) -> bool {
    text.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && text.split('_').all(|word| {
            !word.is_empty()
                && word
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}

fn is_version_word(word: &str) -> bool {
    word.strip_prefix('v')
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()))
}

/// A rule a table name breaks.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum TableNameError {
    /// Not `<namespace>.<table>`.
    #[error("{0}: a table name is <namespace>.<table> (root ADR-0626 rule 7)")]
    NotQualified(String),
    /// The namespace is not one the registry declares.
    #[error("{name}: the namespace is not one of {namespaces:?}; a namespace is the layer (root ADR-0626 rule 6)")]
    Namespace {
        /// The table name.
        name: String,
        /// The declared namespaces.
        namespaces: Vec<String>,
    },
    /// The words are not lowercase `snake_case`.
    #[error("{0}: a table name is lowercase snake_case words, `{IDENTIFIER_PATTERN}` (root ADR-0626 rule 3)")]
    Grammar(String),
    /// A storage, version, date, environment or test word.
    #[error("{name}: `{word}` is a storage, version, date, environment or test word; history is row_versions, never the name, and a test table lives in dev (root ADR-0626 rule 7)")]
    ForbiddenWord {
        /// The table name.
        name: String,
        /// The word.
        word: String,
    },
    /// The last word ends in `s` and is not a listed singular word.
    #[error("{name}: the last word `{word}` reads as a plural; a table name is the singular noun of one row, or the word goes on singular_words_ending_in_s (root ADR-0626 rule 7)")]
    PluralLastWord {
        /// The table name.
        name: String,
        /// The word.
        word: String,
    },
    /// A suffix no generator makes.
    #[error("{name}: `__{role}` is not a generated companion role; the only one is __changes (root ADR-0626 rule 7)")]
    RoleSuffix {
        /// The table name.
        name: String,
        /// The role.
        role: String,
    },
}

/// The registry, or why it could not be read.
///
/// # Errors
/// The generated registry is missing a field or does not parse (run `declared_definitions.py generate`).
pub fn naming_registry() -> Result<&'static NamingRegistry, &'static str> {
    static REGISTRY: OnceLock<Result<NamingRegistry, String>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| parse_naming_registry(NAMING_CONTRACT))
        .as_ref()
        .map_err(String::as_str)
}

/// The registry of a contract text.
///
/// # Errors
/// The text does not parse as the registry.
pub fn parse_naming_registry(text: &str) -> Result<NamingRegistry, String> {
    serde_json::from_str(text).map_err(|error| {
        format!("config/naming.contract.json does not parse (run declared_definitions.py generate): {error}")
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_naming_registry, TableNameError};

    const REGISTRY: &str = r#"{
      "identifier": {"pattern": "^[a-z][a-z0-9]*(_[a-z0-9]+)*$"},
      "namespaces": ["silver", "gold"],
      "role_suffixes": ["changes"],
      "singular_words_ending_in_s": ["gas"],
      "forbidden_table_words": ["smoke", "versions", "served"],
      "forbidden_table_word_patterns": ["v[0-9]+", "[0-9]{6,8}"],
      "renames": {"tables": {"silver.synthetic_rows": "silver.synthetic_row"}, "not_recreated": ["gold.synthetic_panel"]}
    }"#;

    #[test]
    fn a_conforming_name_passes_and_each_rule_refuses_its_own_break() -> Result<(), String> {
        let registry = parse_naming_registry(REGISTRY)?;
        for good in [
            "silver.synthetic_row",
            "gold.synthetic_row__changes",
            "silver.monthly_gas",
            "gold.a1_b2",
        ] {
            assert_eq!(registry.check_table_name(good), Ok(()), "{good}");
        }
        let refused = |name: &str| registry.check_table_name(name).err();
        assert!(matches!(
            refused("synthetic_row"),
            Some(TableNameError::NotQualified(_))
        ));
        assert!(matches!(
            refused("reference.synthetic_row"),
            Some(TableNameError::Namespace { .. })
        ));
        assert!(matches!(
            refused("silver.Synthetic_row"),
            Some(TableNameError::Grammar(_))
        ));
        assert!(matches!(
            refused("silver.synthetic-row"),
            Some(TableNameError::Grammar(_))
        ));
        assert!(matches!(
            refused("silver.synthetic__row"),
            Some(TableNameError::RoleSuffix { .. })
        ));
        assert!(matches!(
            refused("silver.synthetic_row_smoke"),
            Some(TableNameError::ForbiddenWord { .. })
        ));
        assert!(matches!(
            refused("gold.synthetic_row_versions"),
            Some(TableNameError::ForbiddenWord { .. })
        ));
        assert!(matches!(
            refused("gold.synthetic_row_v2"),
            Some(TableNameError::ForbiddenWord { .. })
        ));
        assert!(matches!(
            refused("silver.synthetic_row_202609"),
            Some(TableNameError::ForbiddenWord { .. })
        ));
        assert!(matches!(
            refused("silver.synthetic_rows"),
            Some(TableNameError::PluralLastWord { .. })
        ));
        assert!(registry.is_pending_rename("silver.synthetic_rows"));
        assert!(registry.is_pending_rename("gold.synthetic_panel"));
        assert!(!registry.is_pending_rename("silver.synthetic_row"));
        Ok(())
    }

    #[test]
    fn an_unreadable_registry_is_an_error() {
        assert!(parse_naming_registry("{}").is_err());
    }
}
