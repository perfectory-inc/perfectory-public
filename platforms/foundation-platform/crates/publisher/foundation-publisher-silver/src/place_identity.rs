//! The place-identity rules every placing producer shares (root ADR-0288, ADR-0482).
//!
//! Ported once from `infra/lakehouse/spark/jobs/place_identity_resolver.py` (W81c): the legal-dong code crosswalk and the
//! earlier forms of a code a served parcel may still carry.
//!
//! The crosswalk is read from the code change table's pairs in force (`legal_dong_code_change_stage`'s
//! `pair_rows.sql`); a `DuckDB` stage joins the tables these functions return.

use std::collections::{BTreeMap, BTreeSet};

/// The current legal-dong code an `old` code's pair chain reaches, or `None` when the chain splits
/// (one old code, several new) or cycles: either is a steward's decision, not a guess.
#[must_use]
pub fn current_code(old: &str, pairs: &BTreeMap<String, Vec<String>>) -> Option<String> {
    let mut seen = BTreeSet::new();
    let mut code = old.to_owned();
    loop {
        if !seen.insert(code.clone()) {
            return None;
        }
        let Some(next) = pairs.get(&code) else {
            return Some(code);
        };
        let distinct: BTreeSet<&String> = next.iter().collect();
        if distinct.len() > 1 {
            return None;
        }
        match distinct.into_iter().next() {
            Some(only) => code.clone_from(only),
            None => return Some(code),
        }
    }
}

/// The old → current map of every code that changed, and the old codes whose chain splits or cycles,
/// from `(old, new)` pairs. A blank side, or a code recorded as changing to itself, is no change.
#[must_use]
pub fn code_crosswalk<'a>(
    pair_rows: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> (BTreeMap<String, String>, BTreeSet<String>) {
    let mut pairs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (old, new) in pair_rows {
        if !old.is_empty() && !new.is_empty() && old != new {
            pairs
                .entry(old.to_owned())
                .or_default()
                .push(new.to_owned());
        }
    }
    let mut remap = BTreeMap::new();
    let mut ambiguous = BTreeSet::new();
    for old in pairs.keys() {
        match current_code(old, &pairs) {
            None => {
                ambiguous.insert(old.clone());
            }
            Some(current) if &current != old => {
                remap.insert(old.clone(), current);
            }
            Some(_) => {}
        }
    }
    (remap, ambiguous)
}

/// `(code, an earlier form of it, rank)` for every code a recorded pair touches (root ADR-0482 §1).
///
/// The served cadastral edition can predate a code change, so a served parcel may still carry an
/// earlier form of its code. Rank 1 is the code itself when it is superseded (the source gave an old
/// code); rank 2 every other code the pairs carry to the same current code. The current form (rank 0)
/// is not listed: every reader tries it first.
#[must_use]
pub fn served_forms(remap: &BTreeMap<String, String>) -> Vec<(String, String, u8)> {
    let mut earlier: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (old, current) in remap {
        earlier
            .entry(current.as_str())
            .or_default()
            .insert(old.as_str());
    }
    let codes: BTreeSet<&str> = remap
        .keys()
        .map(String::as_str)
        .chain(earlier.keys().copied())
        .collect();
    let mut out = Vec::new();
    for code in codes {
        let current = remap.get(code).map_or(code, String::as_str);
        if current != code {
            out.push((code.to_owned(), code.to_owned(), 1));
        }
        for old in earlier.get(current).into_iter().flatten() {
            if *old != code {
                out.push((code.to_owned(), (*old).to_owned(), 2));
            }
        }
    }
    out
}

/// `pnu` with its 10-digit legal-dong code replaced: the lot (산 flag, 본번, 부번) is kept.
#[must_use]
pub fn with_current_code(pnu: &str, code: &str) -> String {
    format!("{code}{}", pnu.get(10..).unwrap_or_default())
}

/// A 법정동's full name with its runs of whitespace made one space, the key an address is looked up by.
#[must_use]
pub fn address_key(name: &str) -> String {
    name.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Full 법정동 name → code, from `(code, full name, status)` rows of one code snapshot.
///
/// `rt_molit_real_transaction.dong_index`: a current (`존재`) code wins over an abolished one, and a
/// name two codes of the same standing share is left out. 시도·시군구 rows (`…00000`) are not 법정동s.
#[must_use]
pub fn dong_index<'a>(
    rows: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
) -> BTreeMap<String, String> {
    let mut by_rank: BTreeMap<String, BTreeMap<u8, BTreeSet<&str>>> = BTreeMap::new();
    for (code, name, status) in rows {
        let ten_digits = code.len() == 10 && code.bytes().all(|b| b.is_ascii_digit());
        if !ten_digits || code.get(5..) == Some("00000") {
            continue;
        }
        let rank = u8::from(status != "존재");
        by_rank
            .entry(address_key(name))
            .or_default()
            .entry(rank)
            .or_default()
            .insert(code);
    }
    by_rank
        .into_iter()
        .filter_map(|(key, ranks)| {
            let codes = ranks.into_values().next()?;
            (codes.len() == 1)
                .then(|| (key, codes.into_iter().next().unwrap_or_default().to_owned()))
        })
        .collect()
}

/// `(address key, its code in the snapshot, its current code or None)` for every named 법정동: the code
/// carried through the pairs in force; a code whose chain splits has no current code.
#[must_use]
pub fn address_codes(
    index: &BTreeMap<String, String>,
    remap: &BTreeMap<String, String>,
    ambiguous: &BTreeSet<String>,
) -> Vec<(String, String, Option<String>)> {
    index
        .iter()
        .map(|(name, code)| {
            let current =
                (!ambiguous.contains(code)).then(|| remap.get(code).unwrap_or(code).clone());
            (name.clone(), code.clone(), current)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    //! The Python module's tests (`tests/test_place_identity_resolver.py`), case for case.
    use super::*;

    fn pairs(rows: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        rows.iter()
            .map(|(old, new)| {
                (
                    (*old).to_owned(),
                    new.iter().map(|n| (*n).to_owned()).collect(),
                )
            })
            .collect()
    }

    fn map(rows: &[(&str, &str)]) -> BTreeMap<String, String> {
        rows.iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect()
    }

    #[test]
    fn a_code_with_no_successor_is_already_current() {
        assert_eq!(
            current_code("9999900200", &BTreeMap::new()).as_deref(),
            Some("9999900200")
        );
    }

    #[test]
    fn a_chain_follows_old_to_current() {
        let chain = pairs(&[
            ("9999900100", &["9999900150"]),
            ("9999900150", &["9999900200"]),
        ]);
        assert_eq!(
            current_code("9999900100", &chain).as_deref(),
            Some("9999900200")
        );
    }

    #[test]
    fn a_split_is_ambiguous_not_a_guess() {
        let split = pairs(&[("9999900100", &["9999900200", "9999900300"])]);
        assert_eq!(current_code("9999900100", &split), None);
    }

    #[test]
    fn a_cycle_is_ambiguous() {
        assert_eq!(
            current_code("A", &pairs(&[("A", &["B"]), ("B", &["A"])])),
            None
        );
    }

    #[test]
    fn only_changed_codes_enter_and_a_split_is_flagged() {
        let (remap, ambiguous) = code_crosswalk([
            ("9999900100", "9999900200"),
            ("9999901000", "9999901000"),
            ("9999902000", "9999902001"),
            ("9999902000", "9999902002"),
            ("", "9999900200"),
            ("9999900100", ""),
        ]);
        assert_eq!(remap, map(&[("9999900100", "9999900200")]));
        assert_eq!(ambiguous, BTreeSet::from(["9999902000".to_owned()]));
    }

    #[test]
    fn the_lot_is_kept_when_the_legal_code_moves() {
        assert_eq!(
            with_current_code("9999900100109120008", "9999900200"),
            "9999900200109120008"
        );
    }

    #[test]
    fn an_old_code_is_its_own_first_earlier_form_and_a_sibling_comes_second() {
        let remap = map(&[("9999930100", "9999940100"), ("9999950100", "9999940100")]);
        let expected: Vec<(String, String, u8)> = [
            ("9999930100", "9999930100", 1),
            ("9999930100", "9999950100", 2),
            ("9999940100", "9999930100", 2),
            ("9999940100", "9999950100", 2),
            ("9999950100", "9999950100", 1),
            ("9999950100", "9999930100", 2),
        ]
        .iter()
        .map(|(a, b, r)| ((*a).to_owned(), (*b).to_owned(), *r))
        .collect();
        assert_eq!(served_forms(&remap), expected);
    }

    #[test]
    fn a_chain_lists_every_code_that_carries_to_the_current_one() {
        let remap = map(&[("9999900100", "9999900300"), ("9999900200", "9999900300")]);
        let forms: BTreeSet<(String, String)> = served_forms(&remap)
            .into_iter()
            .map(|(c, f, _)| (c, f))
            .collect();
        assert!(forms.contains(&("9999900300".to_owned(), "9999900100".to_owned())));
        assert!(forms.contains(&("9999900300".to_owned(), "9999900200".to_owned())));
    }

    #[test]
    fn a_current_code_wins_and_a_shared_name_is_left_out() {
        let index = dong_index([
            ("9999910100", "Synth  City   Eastgu Adong", "존재"),
            ("9999910200", "Synth City Eastgu Adong", "폐지"),
            ("9999910300", "Synth City Eastgu Bdong", "존재"),
            ("9999910400", "Synth City Eastgu Bdong", "존재"),
            ("9999900000", "Synth City Eastgu", "존재"),
            ("99999103", "Short", "존재"),
        ]);
        assert_eq!(index, map(&[("Synth City Eastgu Adong", "9999910100")]));
    }

    #[test]
    fn an_address_takes_its_current_code_through_the_pairs_in_force() {
        let index = map(&[
            ("A", "9999910100"),
            ("B", "9999920100"),
            ("C", "9999930100"),
        ]);
        let remap = map(&[("9999910100", "9999940100")]);
        let ambiguous = BTreeSet::from(["9999920100".to_owned()]);
        assert_eq!(
            address_codes(&index, &remap, &ambiguous),
            vec![
                (
                    "A".to_owned(),
                    "9999910100".to_owned(),
                    Some("9999940100".to_owned())
                ),
                ("B".to_owned(), "9999920100".to_owned(), None),
                (
                    "C".to_owned(),
                    "9999930100".to_owned(),
                    Some("9999930100".to_owned())
                ),
            ]
        );
    }

    #[test]
    fn no_pair_no_form() {
        assert!(served_forms(&BTreeMap::new()).is_empty());
    }
}
