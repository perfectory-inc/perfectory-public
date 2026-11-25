//! A stage's reads of R2 retry with the engine contract's numbers (root ADR-0102), through
//! httpfs's own settings: the contract is the one place they are written.

use super::super::r2_read_settings;

#[test]
fn a_stage_retries_r2_reads_with_the_contracts_numbers() -> anyhow::Result<()> {
    let policy = crate::lakehouse_engine_contract::r2_read_policy()?;
    let settings = r2_read_settings()?;
    for expected in [
        format!("SET http_retries = {};", policy.num_retries),
        format!("SET http_retry_wait_ms = {};", policy.min_wait_ms),
        "SET http_retry_backoff = 2;".to_owned(),
    ] {
        assert!(
            settings.contains(&expected),
            "{expected} is not in {settings}"
        );
    }
    // The contract's ADR-0102 values: ten more requests after the first, from 200 ms.
    assert_eq!((policy.num_retries, policy.min_wait_ms), (10, 200));
    Ok(())
}
