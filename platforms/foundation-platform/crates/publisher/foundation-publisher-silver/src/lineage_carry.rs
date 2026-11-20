//! The parcel lineage's carry walk (root ADR-0113 §6).
//!
//! Ported once from `infra/lakehouse/spark/jobs/parcel_attribute_carry.py` (W81c): which earlier parcels a current
//! parcel may take a value from, nearest first, and the path its screen names.
//!
//! The walk follows one link back at a time while the chain is a single line: a merge, a link not in
//! effect, a cycle or more than `MAX_HOPS` stops it. It runs over the rows a walk from the asked
//! sources reads (a few per source), not over the table.

use std::collections::{BTreeMap, BTreeSet};

/// The lineage grades, strongest first (`parcel_lineage.GRADES`).
pub const GRADES: [&str; 6] = [
    "official",
    "code_derived",
    "evidence_strong",
    "evidence_weak",
    "needs_review",
    "pending",
];
/// Grades a walk never crosses (root ADR-0113 §4).
pub const NOT_IN_EFFECT: [&str; 2] = ["needs_review", "pending"];
/// The longest chain a walk follows.
pub const MAX_HOPS: usize = 8;
/// Relations that keep the parcel the same land (a code or register change, not a split).
pub const IDENTITY_RELATIONS: [&str; 3] = [
    "code_change",
    "jurisdiction_transfer",
    "registration_conversion",
];

/// One lineage link, as `steward_resolved` leaves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// The earlier parcel.
    pub predecessor_pnu: String,
    /// The later parcel.
    pub successor_pnu: String,
    /// `parcel_lineage.RELATIONS`.
    pub relation: String,
    /// `GRADES`.
    pub grade: String,
}

/// A source `successor_pnu` may carry from, `hop` links back, and the path its screen names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Candidate {
    /// The parcel that carries.
    pub successor_pnu: String,
    /// The parcel it carries from.
    pub source_pnu: String,
    /// Links back from the successor.
    pub hop: usize,
    /// `lineage:<grade>`, or `origin:<relation>:<grade>` past a split or other non-identity link.
    pub path: String,
}

/// Why walks stopped, counted (`merged`, `not_in_effect`, `too_long`, `cycle`).
pub type Stopped = BTreeMap<&'static str, usize>;

fn rank(grade: &str) -> anyhow::Result<usize> {
    GRADES
        .iter()
        .position(|known| *known == grade)
        .ok_or_else(|| anyhow::anyhow!("lineage grade {grade:?} is not one of {GRADES:?}"))
}

/// One link per (predecessor, successor), the best graded: reruns of the derivation append.
fn best_links(links: &[Link]) -> anyhow::Result<BTreeMap<(&str, &str), &Link>> {
    let mut best: BTreeMap<(&str, &str), &Link> = BTreeMap::new();
    for link in links {
        if link.predecessor_pnu.is_empty() || link.predecessor_pnu == link.successor_pnu {
            continue;
        }
        let key = (link.predecessor_pnu.as_str(), link.successor_pnu.as_str());
        let better = match best.get(&key) {
            Some(held) => rank(&link.grade)? < rank(&held.grade)?,
            None => true,
        };
        if better {
            best.insert(key, link);
        }
    }
    Ok(best)
}

/// Every successor's candidate sources, nearest first, and why chains stopped.
///
/// # Errors
/// A link whose grade is not a lineage grade.
pub fn carry_candidates(links: &[Link]) -> anyhow::Result<(Vec<Candidate>, Stopped)> {
    let mut incoming: BTreeMap<&str, Vec<&Link>> = BTreeMap::new();
    for link in best_links(links)?.into_values() {
        incoming
            .entry(link.successor_pnu.as_str())
            .or_default()
            .push(link);
    }
    let mut stopped = Stopped::new();
    let mut candidates = Vec::new();
    for &successor in incoming.keys() {
        let (mut pnu, mut weakest, mut crossed) = (successor, 0_usize, None::<&str>);
        let mut seen = BTreeSet::from([successor]);
        for hop in 1..=MAX_HOPS + 1 {
            let Some(preds) = incoming.get(pnu).filter(|preds| !preds.is_empty()) else {
                break;
            };
            if hop > MAX_HOPS {
                *stopped.entry("too_long").or_default() += 1;
                break;
            }
            let link = preds[0];
            let distinct: BTreeSet<&str> =
                preds.iter().map(|p| p.predecessor_pnu.as_str()).collect();
            if distinct.len() > 1 || link.relation == "merge" {
                *stopped.entry("merged").or_default() += 1;
                break;
            }
            if NOT_IN_EFFECT.contains(&link.grade.as_str()) {
                *stopped.entry("not_in_effect").or_default() += 1;
                break;
            }
            if !seen.insert(link.predecessor_pnu.as_str()) {
                *stopped.entry("cycle").or_default() += 1;
                break;
            }
            weakest = weakest.max(rank(&link.grade)?);
            if crossed.is_none() && !IDENTITY_RELATIONS.contains(&link.relation.as_str()) {
                crossed = Some(link.relation.as_str());
            }
            let grade = GRADES[weakest];
            let path = crossed.map_or_else(
                || format!("lineage:{grade}"),
                |relation| format!("origin:{relation}:{grade}"),
            );
            candidates.push(Candidate {
                successor_pnu: successor.to_owned(),
                source_pnu: link.predecessor_pnu.clone(),
                hop,
                path,
            });
            pnu = link.predecessor_pnu.as_str();
        }
    }
    Ok((candidates, stopped))
}

#[cfg(test)]
mod tests {
    //! `tests/test_parcel_attribute_carry.py`, case for case; `attach` is its reference semantics.
    use super::*;

    const OLD: &str = "9999910100";
    const MID: &str = "9999920100";
    const NEW: &str = "9999930100";

    fn pnu(dong: &str, main: u32) -> String {
        format!("{dong}1{main:04}0000")
    }

    fn link(old: &str, new: &str, relation: &str, grade: &str) -> Link {
        Link {
            predecessor_pnu: old.to_owned(),
            successor_pnu: new.to_owned(),
            relation: relation.to_owned(),
            grade: grade.to_owned(),
        }
    }

    fn renumber(old: &str, new: &str) -> Link {
        link(old, new, "code_change", "code_derived")
    }

    fn attach(
        own: &BTreeMap<String, i64>,
        held: &BTreeMap<String, i64>,
        candidates: &[Candidate],
    ) -> BTreeMap<String, (i64, String)> {
        let mut result: BTreeMap<String, (i64, String)> = own
            .iter()
            .map(|(p, v)| (p.clone(), (*v, "same_pnu".to_owned())))
            .collect();
        let mut sorted: Vec<&Candidate> = candidates.iter().collect();
        sorted.sort_by(|a, b| (&a.successor_pnu, a.hop).cmp(&(&b.successor_pnu, b.hop)));
        for c in sorted {
            if result.contains_key(&c.successor_pnu) {
                continue;
            }
            held.get(&c.source_pnu).into_iter().for_each(|value| {
                result.insert(c.successor_pnu.clone(), (*value, c.path.clone()));
            });
        }
        result
    }

    fn values(rows: &[(String, i64)]) -> BTreeMap<String, i64> {
        rows.iter().cloned().collect()
    }

    #[test]
    fn a_renumbered_parcel_takes_its_old_numbers_value() -> anyhow::Result<()> {
        let (candidates, _) = carry_candidates(&[renumber(&pnu(OLD, 1), &pnu(NEW, 1))])?;
        let result = attach(
            &BTreeMap::new(),
            &values(&[(pnu(OLD, 1), 52000)]),
            &candidates,
        );
        assert_eq!(
            result,
            BTreeMap::from([(pnu(NEW, 1), (52000, "lineage:code_derived".to_owned()))])
        );
        Ok(())
    }

    #[test]
    fn its_own_value_wins() -> anyhow::Result<()> {
        let (candidates, _) = carry_candidates(&[renumber(&pnu(OLD, 1), &pnu(NEW, 1))])?;
        let result = attach(
            &values(&[(pnu(NEW, 1), 61000)]),
            &values(&[(pnu(OLD, 1), 52000), (pnu(NEW, 1), 61000)]),
            &candidates,
        );
        assert_eq!(result[&pnu(NEW, 1)], (61000, "same_pnu".to_owned()));
        Ok(())
    }

    #[test]
    fn a_chain_takes_the_nearest_holder_and_the_weakest_grade() -> anyhow::Result<()> {
        let links = [
            link(&pnu(OLD, 1), &pnu(MID, 1), "code_change", "official"),
            link(
                &pnu(MID, 1),
                &pnu(NEW, 1),
                "jurisdiction_transfer",
                "evidence_strong",
            ),
        ];
        let (candidates, _) = carry_candidates(&links)?;
        let far = attach(&BTreeMap::new(), &values(&[(pnu(OLD, 1), 1)]), &candidates);
        assert_eq!(far[&pnu(NEW, 1)], (1, "lineage:evidence_strong".to_owned()));
        let near = attach(
            &BTreeMap::new(),
            &values(&[(pnu(OLD, 1), 1), (pnu(MID, 1), 2)]),
            &candidates,
        );
        assert_eq!(
            near[&pnu(NEW, 1)],
            (2, "lineage:evidence_strong".to_owned())
        );
        Ok(())
    }

    #[test]
    fn a_split_child_carries_the_original_parcels_value_labelled_as_such() -> anyhow::Result<()> {
        let links = [
            link(&pnu(OLD, 1), &pnu(NEW, 1), "split", "official"),
            link(&pnu(OLD, 1), &pnu(NEW, 2), "split", "official"),
        ];
        let (candidates, _) = carry_candidates(&links)?;
        let result = attach(&BTreeMap::new(), &values(&[(pnu(OLD, 1), 7)]), &candidates);
        let path = "origin:split:official".to_owned();
        assert_eq!(
            result,
            BTreeMap::from([(pnu(NEW, 1), (7, path.clone())), (pnu(NEW, 2), (7, path))])
        );
        Ok(())
    }

    #[test]
    fn a_merge_is_never_crossed() -> anyhow::Result<()> {
        let links = [
            link(&pnu(OLD, 1), &pnu(NEW, 1), "merge", "official"),
            link(&pnu(OLD, 2), &pnu(NEW, 1), "merge", "official"),
        ];
        let (candidates, stopped) = carry_candidates(&links)?;
        assert!(candidates.is_empty());
        assert_eq!(stopped["merged"], 1);
        let (_, stopped) =
            carry_candidates(&[link(&pnu(OLD, 1), &pnu(NEW, 1), "merge", "official")])?;
        assert_eq!(
            stopped["merged"], 1,
            "a merge with one known predecessor is still a merge"
        );
        Ok(())
    }

    #[test]
    fn links_not_in_effect_are_never_crossed() -> anyhow::Result<()> {
        for grade in ["needs_review", "pending"] {
            let (candidates, stopped) =
                carry_candidates(&[link(&pnu(OLD, 1), &pnu(NEW, 1), "code_change", grade)])?;
            assert!(candidates.is_empty());
            assert_eq!(stopped["not_in_effect"], 1);
        }
        Ok(())
    }

    #[test]
    fn a_rerun_with_a_better_grade_wins() -> anyhow::Result<()> {
        let links = [
            link(&pnu(OLD, 1), &pnu(NEW, 1), "code_change", "needs_review"),
            link(&pnu(OLD, 1), &pnu(NEW, 1), "code_change", "official"),
        ];
        let (candidates, _) = carry_candidates(&links)?;
        let paths: Vec<&str> = candidates.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(paths, ["lineage:official"]);
        Ok(())
    }

    #[test]
    fn a_cycle_stops() -> anyhow::Result<()> {
        let links = [
            renumber(&pnu(OLD, 1), &pnu(NEW, 1)),
            renumber(&pnu(NEW, 1), &pnu(OLD, 1)),
        ];
        let (_, stopped) = carry_candidates(&links)?;
        assert_eq!(stopped["cycle"], 2);
        Ok(())
    }

    #[test]
    fn an_unknown_grade_is_refused() {
        assert!(
            carry_candidates(&[link(&pnu(OLD, 1), &pnu(NEW, 1), "code_change", "maybe")]).is_err()
        );
    }
}
