//! The code.go.kr 법정동 source contract (root ADR-0143), embedded once.
//!
//! The collector reads its endpoints, form fields and request spacing; the hub exports' crosswalk
//! check reads how old a confirmed collection may be (`projection.max_age_days`). One embedding
//! keeps both on the bytes of the same file.

/// `infra/lakehouse/contracts/code-go-kr-legal-dong.contract.json`, as compiled in.
pub const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/code-go-kr-legal-dong.contract.json");
