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
//! governed 시도 pass through composition unchanged (identity), except the
//! declared `placeholder_sigungu` codes, which compose no PNU.
//!
//! An ungoverned code whose 시도 the cadastral parcel set does not carry composes
//! a PNU no parcel has — an orphan, invisible to every NULL-share check. The
//! [`SidoTally`] counts rows by 시도 against the cadastral set the parcel source
//! contract (`vworld-parcel-source-objects.json`) records, and refuses an export
//! whose rows under one such 시도 exceed the seed's `absent_sido_row_bound`
//! (ADR-0142).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{bail, ensure, Context};
use foundation_shared_kernel::pnu::SigunguCrosswalk;
use serde::Deserialize;
use serde_json::{json, Value};

const SEED_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/sigungu-canonical-crosswalk.contract.json");
const PARCEL_SOURCE_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");

#[derive(Deserialize)]
struct Seed {
    sido: Vec<SeedSido>,
    sigungu: Vec<SeedEntry>,
    placeholder_sigungu: SeedPlaceholders,
    absent_sido_row_bound: SeedBound,
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

#[derive(Deserialize)]
struct SeedPlaceholders {
    codes: Vec<String>,
}

#[derive(Deserialize)]
struct SeedBound {
    rows: u64,
}

#[derive(Deserialize)]
struct ParcelSource {
    objects: Vec<ParcelSourceObject>,
}

#[derive(Deserialize)]
struct ParcelSourceObject {
    region_code: String,
    granularity: String,
}

fn five_digits(code: &str) -> bool {
    code.len() == 5 && code.bytes().all(|b| b.is_ascii_digit())
}

/// Loads the 시군구 crosswalk (`current_code → superseded_code`, governed 시도, placeholders)
/// from the seed contract.
///
/// # Errors
/// Fails when the seed contract is not valid JSON, holds a non-5-digit code,
/// duplicates a `current_code`, is empty, maps a code outside every governed
/// 시도, maps onto a code outside that 시도's superseded 시도, or declares a mapped
/// code a placeholder.
pub fn hub_sigungu_crosswalk() -> anyhow::Result<SigunguCrosswalk> {
    parse_crosswalk(SEED_JSON)
}

/// The per-export 시도 tally over the seed contract and the cadastral parcel set.
///
/// # Errors
/// Fails when either contract is unreadable, the cadastral set is empty or inconsistent, or a
/// governed 시도 supersedes a 시도 the cadastral set does not carry.
pub fn hub_sido_tally() -> anyhow::Result<SidoTally> {
    SidoTally::from_contracts(SEED_JSON, PARCEL_SOURCE_JSON)
}

fn parse_seed(raw: &str) -> anyhow::Result<Seed> {
    serde_json::from_str(raw)
        .context("sigungu-canonical-crosswalk.contract.json is not valid seed JSON")
}

fn parse_crosswalk(raw: &str) -> anyhow::Result<SigunguCrosswalk> {
    let seed = parse_seed(raw)?;
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
    .and_then(|crosswalk| crosswalk.with_placeholders(seed.placeholder_sigungu.codes))
    .context("sigungu-canonical-crosswalk.contract.json is not a consistent crosswalk")
}

/// Hub rows counted by the 시도 of their raw 시군구 code, judged against the cadastral parcel set.
///
/// Feed it each row's `register_parcel_key` (whose first five characters are the raw hub 시군구
/// code, zero-padded) and call [`SidoTally::finish`] before the export counts as done.
#[derive(Debug)]
pub struct SidoTally {
    cadastral_sido: BTreeSet<String>,
    governed_sido: BTreeSet<String>,
    placeholders: BTreeSet<String>,
    absent_sido_row_bound: u64,
    rows_by_sido: BTreeMap<String, u64>,
    placeholder_rows: BTreeMap<String, u64>,
}

impl SidoTally {
    fn from_contracts(seed: &str, parcel_source: &str) -> anyhow::Result<Self> {
        let seed = parse_seed(seed)?;
        let parcel_source: ParcelSource = serde_json::from_str(parcel_source)
            .context("vworld-parcel-source-objects.json is not valid JSON")?;
        let region_prefixes = |granularity: &str| {
            parcel_source
                .objects
                .iter()
                .filter(|object| object.granularity == granularity)
                .map(|object| object.region_code.get(..2).unwrap_or_default().to_owned())
                .collect::<BTreeSet<_>>()
        };
        let cadastral_sido = region_prefixes("sido");
        ensure!(
            !cadastral_sido.is_empty() && cadastral_sido == region_prefixes("sigungu"),
            "vworld-parcel-source-objects.json must list its sido objects and cover the same \
             sido with its sigungu objects"
        );
        for sido in &seed.sido {
            for superseded in &sido.supersedes {
                ensure!(
                    cadastral_sido.contains(superseded),
                    "sigungu-canonical-crosswalk.contract.json maps {} onto sido {superseded}, \
                     which the cadastral parcel set no longer carries",
                    sido.current_code
                );
            }
        }
        Ok(Self {
            cadastral_sido,
            governed_sido: seed
                .sido
                .into_iter()
                .map(|sido| sido.current_code)
                .collect(),
            placeholders: seed
                .placeholder_sigungu
                .codes
                .iter()
                .map(|code| format!("{:0>5}", code.trim()))
                .collect(),
            absent_sido_row_bound: seed.absent_sido_row_bound.rows,
            rows_by_sido: BTreeMap::new(),
            placeholder_rows: BTreeMap::new(),
        })
    }

    /// Counts one row by the raw 시군구 code at the head of its register parcel key.
    pub fn observe(&mut self, register_parcel_key: &str) {
        let code = register_parcel_key.get(..5).unwrap_or(register_parcel_key);
        if self.placeholders.contains(code) {
            *self.placeholder_rows.entry(code.to_owned()).or_insert(0) += 1;
            return;
        }
        let sido = code.get(..2).unwrap_or(code).to_owned();
        *self.rows_by_sido.entry(sido).or_insert(0) += 1;
    }

    /// Refuses when one 시도 the cadastral parcel set does not carry, and the crosswalk does not
    /// govern, holds more rows than the bound; otherwise returns the counts for the summary.
    ///
    /// # Errors
    /// Fails naming the 시도, its row count, and the bound.
    pub fn finish(&self) -> anyhow::Result<Value> {
        let absent = self
            .rows_by_sido
            .iter()
            .filter(|(sido, _)| {
                !self.cadastral_sido.contains(*sido) && !self.governed_sido.contains(*sido)
            })
            .map(|(sido, rows)| (sido.clone(), *rows))
            .collect::<BTreeMap<_, _>>();
        if let Some((sido, rows)) = absent
            .iter()
            .find(|(_, rows)| **rows > self.absent_sido_row_bound)
        {
            bail!(
                "Refusing the export: {rows} hub rows carry 시도 {sido}, which the cadastral parcel \
                 set does not carry and no merged 시도 governs (bound {}). Their PNUs would be \
                 orphans. Declare the merged 시도 and its pairs in \
                 sigungu-canonical-crosswalk.contract.json, or its placeholder codes there.",
                self.absent_sido_row_bound
            );
        }
        Ok(json!({
            "rows_by_sido": self.rows_by_sido,
            "placeholder_rows": self.placeholder_rows,
            "absent_from_cadastre_rows": absent,
            "absent_sido_row_bound": self.absent_sido_row_bound,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{hub_sido_tally, hub_sigungu_crosswalk, parse_crosswalk, SidoTally, SEED_JSON};
    use foundation_shared_kernel::pnu::standard_pnu_from_hub_register_codes_via;

    /// A synthetic cadastral parcel set over 시도 97 and 98.
    const PARCEL_SOURCE: &str = r#"{"objects":[
        {"region_code":"97","granularity":"sido"},{"region_code":"98","granularity":"sido"},
        {"region_code":"97110","granularity":"sigungu"},{"region_code":"98110","granularity":"sigungu"}]}"#;

    /// A register parcel key headed by the raw hub 시군구 `code`, zero-padded like the real one.
    fn key(code: &str) -> String {
        format!("{code:0>5}{}", "00101000010000")
    }

    /// Merged 시도 99 superseding 98; placeholders 99999 and the malformed `0`; bound 2 rows.
    const SEED: &str = r#"{"sido":[{"current_code":"99","supersedes":["98"]}],
        "sigungu":[{"current_code":"99110","superseded_code":"98110"}],
        "placeholder_sigungu":{"codes":["99999","0"]},
        "absent_sido_row_bound":{"rows":2}}"#;

    #[test]
    fn loads_the_seed_contract_into_a_governed_crosswalk() -> anyhow::Result<()> {
        let crosswalk = hub_sigungu_crosswalk()?;
        assert!(!crosswalk.is_empty());
        // 다스리는 시도의 모든 짝이 그 시도 안에 있고, 자리표시자는 짝이 아니다.
        assert!(crosswalk.governs_sido("12"));
        for code in ["99999", "99990", "0"] {
            assert!(crosswalk.is_placeholder(code), "{code}");
            assert_eq!(
                standard_pnu_from_hub_register_codes_via(
                    &crosswalk, code, "00101", "0", "0001", "0000"
                )?,
                None,
                "a placeholder must compose no PNU: {code}"
            );
        }
        // 실물 계약 둘을 함께 읽어도 일관된다.
        hub_sido_tally()?.finish()?;
        Ok(())
    }

    #[test]
    fn a_governed_code_missing_from_the_seed_stops_composition() -> anyhow::Result<()> {
        // 심은 위반: 시도 99 를 통합으로 선언하고 짝은 99110 하나만 둔다.
        let crosswalk = parse_crosswalk(SEED)?;
        let refused = standard_pnu_from_hub_register_codes_via(
            &crosswalk, "99990", "00101", "0", "0001", "0000",
        );
        assert!(
            refused.as_ref().is_err_and(|error| {
                let message = error.to_string();
                message.contains("99990")
                    && message.contains("sigungu-canonical-crosswalk.contract.json")
            }),
            "an unmapped merged code must be refused by name, got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_seed_that_maps_outside_its_declared_sido_is_refused() {
        let tail = r#""placeholder_sigungu":{"codes":[]},"absent_sido_row_bound":{"rows":1}}"#;
        for head in [
            // 99 를 다스리는 선언이 없는데 99110 을 매핑
            r#"{"sido":[{"current_code":"97","supersedes":["98"]}],
                "sigungu":[{"current_code":"99110","superseded_code":"98110"}],"#,
            // 대상 96110 은 99 가 대체한 시도(98)가 아니다
            r#"{"sido":[{"current_code":"99","supersedes":["98"]}],
                "sigungu":[{"current_code":"99110","superseded_code":"96110"}],"#,
        ] {
            let raw = format!("{head}{tail}");
            assert!(parse_crosswalk(&raw).is_err(), "must refuse: {raw}");
        }
        // 짝이 있는 코드를 자리표시자로도 선언
        let raw = SEED.replace(r#"["99999","0"]"#, r#"["99110"]"#);
        assert!(parse_crosswalk(&raw).is_err(), "must refuse: {raw}");
    }

    #[test]
    fn a_sido_absent_from_the_cadastre_over_the_bound_is_refused() -> anyhow::Result<()> {
        // 심은 위반: 지적도에도 통합 선언에도 없는 시도 96 이 한계(2행)를 넘는다.
        let mut tally = SidoTally::from_contracts(SEED, PARCEL_SOURCE)?;
        for _ in 0..3 {
            tally.observe(&key("96110"));
        }
        let refused = tally.finish();
        assert!(
            refused.as_ref().is_err_and(|error| {
                let message = error.to_string();
                message.contains("시도 96") && message.contains("3 hub rows")
            }),
            "got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_small_unknown_sido_passes_and_is_reported() -> anyhow::Result<()> {
        let mut tally = SidoTally::from_contracts(SEED, PARCEL_SOURCE)?;
        for code in [
            "96110", // 지적도에 없는 시도 96, 한계 이하
            "96120", "97110", // 지적도의 시도
            "99110", // 통합 시도: 지적도에 없어도 크로스워크가 다스린다
            "99999", // 자리표시자
            "0",     // 자리표시자 `0` (키에서는 0 채움)
        ] {
            tally.observe(&key(code));
        }
        let summary = tally.finish()?;
        assert_eq!(summary["absent_from_cadastre_rows"]["96"], 2);
        assert!(summary["absent_from_cadastre_rows"].get("99").is_none());
        assert_eq!(summary["rows_by_sido"]["97"], 1);
        assert_eq!(summary["placeholder_rows"]["99999"], 1);
        assert_eq!(summary["placeholder_rows"]["00000"], 1);
        Ok(())
    }

    #[test]
    fn a_crosswalk_onto_a_sido_the_cadastre_dropped_is_refused() {
        // 지적도가 시도 98 을 더 이상 싣지 않으면, 99 → 98 짝은 거짓이 된다.
        let parcel_source = r#"{"objects":[{"region_code":"97","granularity":"sido"},
            {"region_code":"97110","granularity":"sigungu"}]}"#;
        assert!(SidoTally::from_contracts(SEED, parcel_source).is_err());
        assert!(SEED_JSON.contains("absent_sido_row_bound"));
    }
}
