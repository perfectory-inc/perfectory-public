//! The Onbid source contract restates the PNU rule for the Spark load (root ADR-0280 §3). This keeps
//! that copy equal to `Pnu::parse`, the rule's one definition.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::Path,
};

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

#[test]
fn onbid_provider_ledger_digits_are_the_hub_dialect() -> TestResult {
    // ltnoPnu writes the 11th digit as the hub register does (0 ordinary, 1 mountain); the
    // contract's map must be exactly the kernel's conversion, the dialect's one definition.
    let map = contract()?["pnu"]["provider_ledger_kind_digits"]
        .as_object()
        .ok_or("pnu.provider_ledger_kind_digits must be an object")?
        .iter()
        .filter(|(key, _)| !key.starts_with('$'))
        .map(|(key, value)| {
            Ok((
                key.clone(),
                value.as_str().ok_or("a digit is a string")?.to_owned(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn Error>>>()?;
    let kernel = ('0'..='9')
        .filter_map(|digit| {
            foundation_shared_kernel::pnu::standard_pnu_from_hub_register_codes(
                "99999",
                "99999",
                &digit.to_string(),
                "1",
                "0",
            )
            .map(|pnu| (digit.to_string(), pnu[10..11].to_owned()))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(map, kernel);
    Ok(())
}
