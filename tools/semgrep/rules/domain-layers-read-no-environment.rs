// Fixture for tools/semgrep/rules/domain-layers-read-no-environment.yml (`opengrep scan --test`).
use std::env;

pub struct PublicationCapability(bool);

// ok: domain-layers-read-no-environment
pub fn enabled(capability: PublicationCapability) -> bool { capability.0 }

// The switch used to be read here with std::env::var("EXAMPLE_SWITCH").
pub const SCHEMA_VERSION: u32 = 2;

// ruleid: domain-layers-read-no-environment
pub fn qualified() -> bool { std::env::var("EXAMPLE_SWITCH").is_ok() }

// ruleid: domain-layers-read-no-environment
pub fn imported() -> bool { env::var_os("EXAMPLE_SWITCH").is_some() }

// ruleid: domain-layers-read-no-environment
pub fn iterated() -> usize { std::env::vars().count() }

// ruleid: domain-layers-read-no-environment
pub const SWITCH: Option<&str> = option_env!("EXAMPLE_SWITCH");

// ruleid: domain-layers-read-no-environment
pub const BUILT_ON: &str = env!("EXAMPLE_HOST");

// ok: domain-layers-read-no-environment
pub fn not_the_environment(map: &Settings) -> Option<&str> { map.var("EXAMPLE_SWITCH") }
