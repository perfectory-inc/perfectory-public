// Fixture for tools/semgrep/rules/lineage-is-derived.yml (`opengrep scan --test`). Not compiled.
// ruleid: a-command-does-not-read-the-name-of-the-input-it-opens
const INPUT: &str = "FOUNDATION_X_INPUT_OBJECT_KEY";
fn from_env() -> Result<String, Error> {
    let id = required_env("FOUNDATION_X_SOURCE_RECORD_ID")?;
    Ok(id)
}

// Optional is not a lesser version of the same thing: a value that may arrive may arrive wrong.
// ruleid: a-command-does-not-read-the-name-of-the-input-it-opens
const PATH: &str = "FOUNDATION_Y_INPUT_PATH";
fn optional() -> Result<Option<String>, Error> {
    optional_env("FOUNDATION_Y_SOURCE_RECORD_ID")
}
