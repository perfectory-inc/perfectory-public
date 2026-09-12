//! Loader for the 시군구 canonical crosswalk seed (ADR-0103 geography identity Wave 1).
//!
//! The HUB building-register feed carries the authority-current merged 시군구
//! code (12xxx, 전남광주통합특별시) while the cadastral map still keys parcels by
//! the superseded codes (29xxx 광주 / 46xxx 전남).
//!
//! This module reads the seed contract — the SSOT at
//! `infra/lakehouse/contracts/sigungu-canonical-crosswalk.seed.json`, embedded
//! at compile time like the other lakehouse contracts — into the
//! `current_code → superseded_code` map that the shared-kernel PNU composition
//! consumes. The kernel stays pure: it never reads infra files; every export
//! layer loads the map here and passes it in. Codes absent from the seed pass
//! through composition unchanged (identity).

use std::collections::HashMap;

use anyhow::{ensure, Context};
use serde::Deserialize;

const SEED_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/sigungu-canonical-crosswalk.seed.json");

#[derive(Deserialize)]
struct Seed {
    sigungu: Vec<SeedEntry>,
}

#[derive(Deserialize)]
struct SeedEntry {
    current_code: String,
    superseded_code: String,
}

fn five_digits(code: &str) -> bool {
    code.len() == 5 && code.bytes().all(|b| b.is_ascii_digit())
}

/// Loads the 시군구 crosswalk map (`current_code → superseded_code`) from the seed contract.
///
/// # Errors
/// Fails when the seed contract is not valid JSON, holds a non-5-digit code,
/// duplicates a `current_code`, or is empty.
pub fn hub_sigungu_crosswalk() -> anyhow::Result<HashMap<String, String>> {
    let seed: Seed = serde_json::from_str(SEED_JSON)
        .context("sigungu-canonical-crosswalk.seed.json is not valid seed JSON")?;
    ensure!(
        !seed.sigungu.is_empty(),
        "sigungu-canonical-crosswalk.seed.json has no sigungu entries"
    );
    let mut crosswalk = HashMap::with_capacity(seed.sigungu.len());
    for entry in seed.sigungu {
        ensure!(
            five_digits(&entry.current_code) && five_digits(&entry.superseded_code),
            "sigungu crosswalk seed codes must be 5 digits: {} -> {}",
            entry.current_code,
            entry.superseded_code
        );
        let previous = crosswalk.insert(entry.current_code, entry.superseded_code);
        ensure!(
            previous.is_none(),
            "sigungu crosswalk seed repeats a current_code"
        );
    }
    Ok(crosswalk)
}

#[cfg(test)]
mod tests {
    use super::hub_sigungu_crosswalk;

    #[test]
    fn loads_the_seed_contract_into_a_current_to_superseded_map() -> anyhow::Result<()> {
        let crosswalk = hub_sigungu_crosswalk()?;
        // 광주 서구: 통합 현행 12240 → 지적도 29140
        assert_eq!(crosswalk.get("12240").map(String::as_str), Some("29140"));
        // 비산술 사례(광양): 12190 → 46230
        assert_eq!(crosswalk.get("12190").map(String::as_str), Some("46230"));
        // 씨앗 밖 코드는 map 에 없다 → 조립은 identity 로 통과
        assert_eq!(crosswalk.get("26110"), None);
        // 매핑의 대상은 전부 29xxx(광주)/46xxx(전남)뿐이다
        assert!(crosswalk
            .values()
            .all(|code| code.starts_with("29") || code.starts_with("46")));
        Ok(())
    }
}
