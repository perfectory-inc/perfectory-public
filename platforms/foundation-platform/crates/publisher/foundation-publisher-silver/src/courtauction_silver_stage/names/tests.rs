//! The mask is the Spark job's `mask_names`: `fixtures/names.tsv` is what it made of synthetic text.

use super::NameMask;
use crate::courtauction_silver_stage::CourtAuctionSource;

#[test]
fn the_mask_is_the_spark_jobs() -> anyhow::Result<()> {
    let mask = CourtAuctionSource::compiled()?.name_mask()?;
    let cases = include_str!("../fixtures/names.tsv");
    let mut checked = 0;
    for line in cases.lines().filter(|line| !line.starts_with('#')) {
        let (text, expected) = line.split_once('\t').unwrap_or((line, ""));
        let (text, expected) = (text.replace("\\t", "\t"), expected.replace("\\t", "\t"));
        assert_eq!(mask.apply(&text)?, expected, "{text:?}");
        checked += 1;
    }
    assert_eq!(checked, 38);
    Ok(())
}

#[test]
fn rules_that_are_not_the_contracts_shape_are_refused() {
    assert!(NameMask::new(&serde_json::json!({"name_syllables": [2]})).is_err());
}
