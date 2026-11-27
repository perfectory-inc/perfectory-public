//! G2 (root ADR-0626 §7): every table name the contracts declare follows the naming registry, or is
//! still on its rename work list, and the work list holds only what is left to do.
//!
//! What failing this prevents: production held four smoke tables in its catalog namespaces, and the
//! plainly named Gold tables (`gold.parcel_energy` and five more) were frozen while their `_versions`
//! twins were the live producers, so a reader picked the dead table by its name (root ADR-0626 F7, F10).
//! A new table outside the rules fails here the day its contract is written; an old name stays only
//! while the work list holds it, and the switch PR that renames it must delete its entry.

use std::collections::BTreeSet;
use std::error::Error;

use lakehouse_domain::industrial_complex_lakehouse_contracts;
use lakehouse_domain::naming::{naming_registry, FORBIDDEN_WORD_PATTERNS, IDENTIFIER_PATTERN};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn the_registry_states_the_grammar_this_crate_implements() -> TestResult {
    let registry = naming_registry()?;
    let (pattern, word_patterns) = registry.stated_grammar();
    assert_eq!(pattern, IDENTIFIER_PATTERN);
    assert_eq!(
        word_patterns,
        FORBIDDEN_WORD_PATTERNS.map(str::to_owned).as_slice()
    );
    Ok(())
}

#[test]
fn every_contract_table_follows_the_rules_or_is_on_the_rename_work_list() -> TestResult {
    let registry = naming_registry()?;
    let broken: Vec<String> = industrial_complex_lakehouse_contracts()
        .iter()
        .filter(|contract| !registry.is_pending_rename(contract.table_name))
        .filter_map(|contract| registry.check_table_name(contract.table_name).err())
        .map(|error| error.to_string())
        .collect();
    assert!(
        broken.is_empty(),
        "table names outside root ADR-0626:\n{}",
        broken.join("\n")
    );
    Ok(())
}

#[test]
fn every_new_name_follows_the_rules_and_names_one_table() -> TestResult {
    let registry = naming_registry()?;
    let kept: BTreeSet<&str> = industrial_complex_lakehouse_contracts()
        .iter()
        .map(|contract| contract.table_name)
        .filter(|name| !registry.is_pending_rename(name))
        .collect();
    let mut targets = BTreeSet::new();
    for (old, new) in &registry.renames.tables {
        registry.check_table_name(new)?;
        assert!(
            targets.insert(new.as_str()),
            "{new} is the new name of two tables"
        );
        assert!(
            !kept.contains(new.as_str()) || old == new,
            "{old} -> {new}: a kept table already has that name"
        );
    }
    Ok(())
}

#[test]
fn the_rename_work_list_names_only_tables_the_contracts_still_declare() -> TestResult {
    let registry = naming_registry()?;
    let declared: BTreeSet<&str> = industrial_complex_lakehouse_contracts()
        .iter()
        .map(|contract| contract.table_name)
        .collect();
    let stale: Vec<&str> = registry
        .renames
        .tables
        .keys()
        .map(String::as_str)
        .chain(registry.renames.not_recreated.iter().map(String::as_str))
        .filter(|name| !declared.contains(name))
        .collect();
    assert!(
        stale.is_empty(),
        "the switch that renamed these deletes their work-list entries in \
         definitions/platform.definition.json naming.renames: {stale:?}"
    );
    Ok(())
}
