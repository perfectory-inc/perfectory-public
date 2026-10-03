//! Loader for the 시군구 canonical crosswalk seed (ADR-0103 geography identity Wave 1).
//!
//! The HUB building-register feed carries the authority-current merged 시군구
//! code (12xxx, 전남광주통합특별시) while the cadastral map still keys parcels by
//! the superseded codes (29xxx 광주 / 46xxx 전남).
//!
//! This module reads the seed contract — the SSOT at
//! `infra/lakehouse/contracts/sigungu-canonical-crosswalk.contract.json`, embedded
//! at compile time like the other lakehouse contracts — into the
//! [`SigunguCrosswalk`] that the shared-kernel PNU composition consumes. The
//! kernel stays pure: it never reads infra files; every export layer loads the
//! crosswalk here and passes it in.
//!
//! The contract's `sido` entries are part of the crosswalk, not commentary: a
//! 시도 listed there is *governed*, so every hub 시군구 code under it must have a
//! mapping and an unmapped one stops the export by name. Codes outside the
//! governed 시도 pass through composition unchanged (identity).

use std::collections::{BTreeSet, HashMap};

use anyhow::{ensure, Context};
use foundation_shared_kernel::pnu::SigunguCrosswalk;
use serde::Deserialize;

const SEED_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/sigungu-canonical-crosswalk.contract.json");

#[derive(Deserialize)]
struct Seed {
    sido: Vec<SeedSido>,
    sigungu: Vec<SeedEntry>,
}

#[derive(Deserialize)]
struct SeedSido {
    current_code: String,
    supersedes: Vec<String>,
}

#[derive(Deserialize)]
struct SeedEntry {
    current_code: String,
    superseded_code: String,
}

fn five_digits(code: &str) -> bool {
    code.len() == 5 && code.bytes().all(|b| b.is_ascii_digit())
}

/// Loads the 시군구 crosswalk (`current_code → superseded_code`, governed 시도) from the seed contract.
///
/// # Errors
/// Fails when the seed contract is not valid JSON, holds a non-5-digit code,
/// duplicates a `current_code`, is empty, maps a code outside every governed
/// 시도, or maps onto a code outside that 시도's superseded 시도.
pub fn hub_sigungu_crosswalk() -> anyhow::Result<SigunguCrosswalk> {
    parse_crosswalk(SEED_JSON)
}

fn parse_crosswalk(raw: &str) -> anyhow::Result<SigunguCrosswalk> {
    let seed: Seed = serde_json::from_str(raw)
        .context("sigungu-canonical-crosswalk.contract.json is not valid seed JSON")?;
    ensure!(
        !seed.sigungu.is_empty(),
        "sigungu-canonical-crosswalk.contract.json has no sigungu entries"
    );
    let supersedes = seed
        .sido
        .iter()
        .map(|sido| {
            (
                sido.current_code.as_str(),
                sido.supersedes
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut crosswalk = HashMap::with_capacity(seed.sigungu.len());
    for entry in seed.sigungu {
        ensure!(
            five_digits(&entry.current_code) && five_digits(&entry.superseded_code),
            "sigungu crosswalk seed codes must be 5 digits: {} -> {}",
            entry.current_code,
            entry.superseded_code
        );
        ensure!(
            supersedes
                .get(&entry.current_code[..2])
                .is_some_and(|targets| targets.contains(&entry.superseded_code[..2])),
            "sigungu crosswalk maps {} -> {}, which no sido entry's supersedes list allows",
            entry.current_code,
            entry.superseded_code
        );
        let previous = crosswalk.insert(entry.current_code, entry.superseded_code);
        ensure!(
            previous.is_none(),
            "sigungu crosswalk seed repeats a current_code"
        );
    }
    SigunguCrosswalk::new(
        crosswalk,
        seed.sido.into_iter().map(|sido| sido.current_code),
    )
    .context("sigungu-canonical-crosswalk.contract.json is not a consistent crosswalk")
}

#[cfg(test)]
mod tests {
    use super::{hub_sigungu_crosswalk, parse_crosswalk};
    use foundation_shared_kernel::pnu::standard_pnu_from_hub_register_codes_via;

    #[test]
    fn loads_the_seed_contract_into_a_governed_crosswalk() -> anyhow::Result<()> {
        let crosswalk = hub_sigungu_crosswalk()?;
        // 광주 서구: 통합 현행 12240 → 지적도 29140
        assert_eq!(crosswalk.superseded_code("12240"), Some("29140"));
        // 비산술 사례(광양): 12190 → 46230
        assert_eq!(crosswalk.superseded_code("12190"), Some("46230"));
        // 통합되지 않은 시도의 코드는 조립에서 identity 로 통과
        assert_eq!(crosswalk.resolve("26110")?, "26110");
        Ok(())
    }

    #[test]
    fn a_governed_code_missing_from_the_seed_stops_composition() -> anyhow::Result<()> {
        // 심은 위반: 시도 99 를 통합으로 선언하고 짝은 99110 하나만 둔다.
        let crosswalk = parse_crosswalk(
            r#"{"sido":[{"current_code":"99","supersedes":["98"]}],
                "sigungu":[{"current_code":"99110","superseded_code":"98110"}]}"#,
        )?;
        let refused = standard_pnu_from_hub_register_codes_via(
            &crosswalk, "99990", "00101", "0", "0001", "0000",
        );
        assert!(
            refused
                .as_ref()
                .is_err_and(|error| error.to_string().contains("99990")),
            "an unmapped merged code must be refused by name, got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_seed_that_maps_outside_its_declared_sido_is_refused() {
        for raw in [
            // 99 를 다스리는 선언이 없는데 99110 을 매핑
            r#"{"sido":[{"current_code":"97","supersedes":["98"]}],
                "sigungu":[{"current_code":"99110","superseded_code":"98110"}]}"#,
            // 대상 96110 은 99 가 대체한 시도(98)가 아니다
            r#"{"sido":[{"current_code":"99","supersedes":["98"]}],
                "sigungu":[{"current_code":"99110","superseded_code":"96110"}]}"#,
        ] {
            assert!(parse_crosswalk(raw).is_err(), "must refuse: {raw}");
        }
    }
}
