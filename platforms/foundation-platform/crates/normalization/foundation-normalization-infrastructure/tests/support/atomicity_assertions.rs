use foundation_normalization_domain::NormalizationError;

use crate::support::TestResult;

use crate::industrial_complex_fixture::TransactionState;

pub fn assert_forced_failure<T>(
    result: Result<T, NormalizationError>,
    expected_message: &str,
) -> TestResult {
    let Err(error) = result else {
        return Err(std::io::Error::other(format!(
            "normalization transaction should fail: {expected_message}"
        ))
        .into());
    };
    if !error.to_string().contains(expected_message) {
        return Err(std::io::Error::other(format!(
            "expected forced database failure '{expected_message}', received: {error}"
        ))
        .into());
    }
    Ok(())
}

pub fn assert_four_surfaces_unchanged(before: &TransactionState, after: &TransactionState) {
    assert_eq!(
        after.canonical_row, before.canonical_row,
        "canonical Catalog row changed despite transaction failure"
    );
    assert_eq!(
        after.outbox_rows, before.outbox_rows,
        "Catalog outbox changed despite transaction failure"
    );
    assert_eq!(
        after.application_rows, before.application_rows,
        "Normalization ledger changed despite transaction failure"
    );
    assert_eq!(
        after.proposal_row, before.proposal_row,
        "Normalization proposal changed despite transaction failure"
    );
}

pub fn assert_all_transaction_state_unchanged(before: &TransactionState, after: &TransactionState) {
    assert_four_surfaces_unchanged(before, after);
    assert_eq!(
        after.review_rows, before.review_rows,
        "Normalization reviews changed despite transaction failure"
    );
}
