//! The Onbid source contract restates the PNU rule for the Spark load (root ADR-0280 §3). This keeps
//! that copy equal to `Pnu::parse`, the rule's one definition.

use std::{collections::BTreeSet, error::Error, path::Path};

use foundation_shared_kernel::Pnu;
use serde_json::Value;

type TestResult = Result<(), Box<dyn Error>>;

fn contract() -> Result<Value, Box<dyn Error>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../infra/lakehouse/contracts/data-go-kr-onbid.contract.json");
    Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
}

#[test]
fn onbid_contract_pnu_rule_is_what_pnu_parse_accepts() -> TestResult {
    let pnu = &contract()?["pnu"];
    let digits = pnu["digits"]
        .as_u64()
        .ok_or("pnu.digits must be a number")?;
    let declared = pnu["ledger_kind_digits"]
        .as_array()
        .ok_or("pnu.ledger_kind_digits must be an array")?
        .iter()
        .map(|digit| {
            digit
                .as_str()
                .map(str::to_owned)
                .ok_or("a digit is a string")
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let accepted = ('0'..='9')
        .filter(|digit| Pnu::parse(format!("9999999999{digit}00010000")).is_ok())
        .map(String::from)
        .collect::<BTreeSet<_>>();

    assert_eq!(declared, accepted);
    assert!(Pnu::parse("9".repeat(usize::try_from(digits)?)).is_ok());
    assert!(Pnu::parse("9".repeat(usize::try_from(digits)? - 1)).is_err());
    Ok(())
}
