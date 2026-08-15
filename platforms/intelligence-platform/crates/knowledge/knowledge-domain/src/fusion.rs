//! 여러 리트리버의 순위 목록을 하나로 합친다 (Reciprocal Rank Fusion).
//!
//! 왜 도메인에 있는가: 융합은 순수 함수다 — 순위 목록들이 들어가고 하나의 순위가 나온다.
//! I/O도 사전도 필요 없다. 어댑터에 두면 어댑터마다 다르게 구현될 수 있고, 그러면 같은
//! 질의가 저장소에 따라 다른 순서를 낸다. 여기 두면 규칙이 한 곳에만 있다.
//!
//! 왜 RRF인가: 리트리버들의 점수는 서로 비교할 수 없다. BM25 점수와 코사인 유사도를
//! 같은 자로 잴 방법이 없다. RRF는 점수를 버리고 **순위만** 쓰므로 척도가 다른 리트리버를
//! 정규화 없이 합칠 수 있다.
//!
//! 원전: Cormack, Clarke, Büttcher, *Reciprocal Rank Fusion Outperforms Condorcet and
//! Individual Rank Learning Methods*, SIGIR 2009. 조사 기록은
//! `docs/reference/knowledge-search-industry-cases.md`.

use std::collections::BTreeMap;

/// 융합의 완충 상수. 조사한 프로덕션 구현들이 쓰는 값이며 원 논문의 값이다.
///
/// 이 값이 클수록 상위 순위끼리의 점수 차가 줄어 **한 리트리버의 1등보다 여러 리트리버의
/// 합의가 이기기 쉬워진다.** 그것이 융합의 목적이므로 함부로 낮추지 말 것.
pub const DEFAULT_RRF_SMOOTHING: f64 = 60.0;

/// 한 리트리버가 낸 순위 목록.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RankedList<T> {
    /// 이 리트리버를 식별하는 이름. 진단과 로그용이며 점수에 영향을 주지 않는다.
    pub retriever: &'static str,
    /// 순위가 높은 것부터. 중복은 첫 등장만 센다.
    pub ids: Vec<T>,
}

/// 융합 결과 한 건.
#[derive(Clone, Debug, PartialEq)]
pub struct FusedItem<T> {
    pub id: T,
    pub score: f64,
    /// 이 항목을 올린 리트리버들. 몇 곳에서 합의했는지가 진단의 핵심이다.
    pub retrievers: Vec<&'static str>,
}

/// 순위 목록들을 RRF로 합친다.
///
/// `score(d) = Σ 1 / (k + rank_l(d))` — 목록 `l`에서 `d`의 순위가 `rank_l(d)`(1부터).
/// 어느 목록에도 없는 항목은 그 목록에서 0점을 받는다(항이 없다).
///
/// 정렬은 점수 내림차순이고, 동점은 `id` 오름차순으로 깬다. 같은 입력이 항상 같은 순서를
/// 내야 하기 때문이다 — 순서가 흔들리면 회귀 테스트가 성립하지 않는다.
pub fn reciprocal_rank_fusion<T>(lists: &[RankedList<T>], smoothing: f64) -> Vec<FusedItem<T>>
where
    T: Clone + Ord,
{
    let mut accumulated: BTreeMap<T, (f64, Vec<&'static str>)> = BTreeMap::new();

    for list in lists {
        let mut seen: Vec<&T> = Vec::new();
        for (index, id) in list.ids.iter().enumerate() {
            // 한 목록 안의 중복은 첫 등장만 센다. 같은 문서를 두 번 올린 리트리버가
            // 두 표를 갖는 것은 합의가 아니다.
            if seen.contains(&id) {
                continue;
            }
            seen.push(id);

            let rank = (index + 1) as f64;
            let entry = accumulated
                .entry(id.clone())
                .or_insert_with(|| (0.0, Vec::new()));
            entry.0 += 1.0 / (smoothing + rank);
            entry.1.push(list.retriever);
        }
    }

    let mut fused: Vec<FusedItem<T>> = accumulated
        .into_iter()
        .map(|(id, (score, retrievers))| FusedItem {
            id,
            score,
            retrievers,
        })
        .collect();

    // BTreeMap이 id 오름차순을 이미 보장하므로 안정 정렬로 점수만 다시 세우면
    // 동점이 id 오름차순으로 남는다.
    fused.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    fused
}

/// 한 출처가 결과를 독식하지 못하게 출처마다 상한을 둔다. 순서는 그대로 유지하고
/// 상한을 넘은 것만 걷어낸다.
///
/// 왜 필요한가: 긴 문서 하나가 여러 절에서 걸리면 그 문서의 조각들이 결과를 다 채운다.
/// 사용자는 "건폐율"을 물었는데 같은 고시의 5개 절만 보게 되고, 다른 고시는 아예 보이지
/// 않는다. 순위는 각 조각이 얼마나 맞는지만 보지 **결과 묶음 전체가 얼마나 쓸모 있는지**는
/// 보지 않기 때문이다.
///
/// 조사한 사례도 융합 직후 같은 일을 한다 —
/// *"we merge duplicate chunks back to one source, cap how many results each file can
/// contribute, and end up with a more diverse top twenty."* (Cerebras)
///
/// 상한을 1로 두지 않는 이유: 긴 고시는 여러 절이 실제로 관련 있을 수 있다. 상한은
/// 독식을 막는 것이지 한 문서를 한 번만 보여주려는 것이 아니다.
pub fn cap_per_group<T, K, F>(items: Vec<T>, max_per_group: usize, group_of: F) -> Vec<T>
where
    K: Ord,
    F: Fn(&T) -> K,
{
    if max_per_group == 0 {
        return Vec::new();
    }
    let mut taken: BTreeMap<K, usize> = BTreeMap::new();
    let mut kept = Vec::with_capacity(items.len());
    for item in items {
        let count = taken.entry(group_of(&item)).or_insert(0);
        if *count >= max_per_group {
            continue;
        }
        *count += 1;
        kept.push(item);
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(retriever: &'static str, ids: &[&str]) -> RankedList<String> {
        RankedList {
            retriever,
            ids: ids.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    fn order(fused: &[FusedItem<String>]) -> Vec<&str> {
        fused.iter().map(|item| item.id.as_str()).collect()
    }

    /// 융합의 존재 이유. 한 목록에서만 1등인 문서보다, 여러 목록에서 상위인 문서가 이겨야 한다.
    /// 이 성질이 깨지면 융합이 아니라 그냥 첫 리트리버를 쓰는 것과 같다.
    #[test]
    fn consensus_beats_a_single_strong_vote() {
        let fused = reciprocal_rank_fusion(
            &[
                list("a", &["only-first", "agreed", "x"]),
                list("b", &["y", "agreed", "z"]),
                list("c", &["w", "agreed", "v"]),
            ],
            DEFAULT_RRF_SMOOTHING,
        );

        assert_eq!(
            order(&fused)[0],
            "agreed",
            "세 목록에서 2등인 문서가 한 목록의 1등을 이겨야 한다: {fused:?}"
        );
    }

    #[test]
    fn an_item_records_every_retriever_that_raised_it() {
        let fused = reciprocal_rank_fusion(
            &[list("morpheme", &["doc"]), list("raw", &["doc"])],
            DEFAULT_RRF_SMOOTHING,
        );

        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].retrievers, vec!["morpheme", "raw"]);
    }

    #[test]
    fn a_higher_rank_scores_more_than_a_lower_one() {
        let fused =
            reciprocal_rank_fusion(&[list("a", &["first", "second"])], DEFAULT_RRF_SMOOTHING);
        assert_eq!(order(&fused), vec!["first", "second"]);
        assert!(fused[0].score > fused[1].score);
    }

    #[test]
    fn scores_match_the_published_formula() {
        let fused = reciprocal_rank_fusion(&[list("a", &["one", "two"])], 60.0);
        assert!((fused[0].score - 1.0 / 61.0).abs() < f64::EPSILON);
        assert!((fused[1].score - 1.0 / 62.0).abs() < f64::EPSILON);
    }

    /// 같은 리트리버가 한 문서를 두 번 올려도 표는 하나다. 중복을 세면 한 리트리버가
    /// 목록을 부풀려 합의를 흉내 낼 수 있다.
    #[test]
    fn a_duplicate_inside_one_list_counts_once() {
        let duplicated =
            reciprocal_rank_fusion(&[list("a", &["doc", "doc", "doc"])], DEFAULT_RRF_SMOOTHING);
        let single = reciprocal_rank_fusion(&[list("a", &["doc"])], DEFAULT_RRF_SMOOTHING);

        assert_eq!(duplicated.len(), 1);
        assert!((duplicated[0].score - single[0].score).abs() < f64::EPSILON);
        assert_eq!(duplicated[0].retrievers, vec!["a"]);
    }

    #[test]
    fn ties_break_deterministically_by_id() {
        let first = reciprocal_rank_fusion(
            &[list("a", &["b-doc"]), list("b", &["a-doc"])],
            DEFAULT_RRF_SMOOTHING,
        );
        let second = reciprocal_rank_fusion(
            &[list("b", &["a-doc"]), list("a", &["b-doc"])],
            DEFAULT_RRF_SMOOTHING,
        );

        assert_eq!(order(&first), vec!["a-doc", "b-doc"]);
        assert_eq!(
            order(&first),
            order(&second),
            "목록 순서가 결과 순서를 바꾸면 안 된다"
        );
    }

    #[test]
    fn an_empty_input_yields_an_empty_result() {
        let fused: Vec<FusedItem<String>> = reciprocal_rank_fusion(&[], DEFAULT_RRF_SMOOTHING);
        assert!(fused.is_empty());
    }

    /// 완충 상수를 낮추면 상위 순위의 점수 차가 벌어져 합의가 이기기 어려워진다.
    /// 상수의 의미를 테스트로 고정해 둔다 — 튜닝하려는 다음 사람에게 보이도록.
    #[test]
    fn a_smaller_smoothing_constant_lets_a_single_first_place_win() {
        // 합의는 두 목록의 3등이다. k=60에서는 1/63+1/63 = 0.0317 로 1/61 = 0.0164 를 이기고,
        // k=0.1에서는 1/3.1+1/3.1 = 0.645 로 1/1.1 = 0.909 에 진다. 상수 하나가 판을 뒤집는다.
        let lists = [
            list("a", &["only-first", "filler", "agreed"]),
            list("b", &["y", "z", "agreed"]),
        ];

        let with_default = reciprocal_rank_fusion(&lists, DEFAULT_RRF_SMOOTHING);
        assert_eq!(
            order(&with_default)[0],
            "agreed",
            "기본 상수에서는 합의가 이긴다: {with_default:?}"
        );

        let with_tiny = reciprocal_rank_fusion(&lists, 0.1);
        assert_eq!(
            order(&with_tiny)[0],
            "only-first",
            "완충 상수가 작으면 1등 한 표가 합의를 이긴다: {with_tiny:?}"
        );
    }

    // ---- cap_per_group ----

    fn chunks(pairs: &[(&str, i32)]) -> Vec<(String, i32)> {
        pairs
            .iter()
            .map(|(source, ordinal)| ((*source).to_string(), *ordinal))
            .collect()
    }

    /// 상한의 존재 이유. 한 출처의 조각들이 결과를 다 채우면 다른 출처가 보이지 않는다.
    #[test]
    fn one_source_cannot_fill_the_whole_result() {
        let items = chunks(&[
            ("notice-a", 0),
            ("notice-a", 1),
            ("notice-a", 2),
            ("notice-a", 3),
            ("notice-b", 0),
        ]);

        let capped = cap_per_group(items, 2, |(source, _)| source.clone());

        assert_eq!(
            capped,
            chunks(&[("notice-a", 0), ("notice-a", 1), ("notice-b", 0)]),
            "상한을 넘은 조각만 빠지고 순서는 그대로여야 한다"
        );
    }

    #[test]
    fn ranking_order_is_preserved_among_survivors() {
        let items = chunks(&[
            ("a", 0),
            ("b", 0),
            ("a", 1),
            ("c", 0),
            ("a", 2), // 상한 초과
            ("b", 1),
        ]);

        let capped = cap_per_group(items, 2, |(source, _)| source.clone());

        assert_eq!(
            capped,
            chunks(&[("a", 0), ("b", 0), ("a", 1), ("c", 0), ("b", 1)])
        );
    }

    #[test]
    fn a_cap_larger_than_any_group_changes_nothing() {
        let items = chunks(&[("a", 0), ("a", 1), ("b", 0)]);
        let capped = cap_per_group(items.clone(), 10, |(source, _)| source.clone());
        assert_eq!(capped, items);
    }

    /// 상한 0은 전부 버린다. 호출부가 실수로 0을 넘기면 결과가 사라져 바로 드러나야 한다 —
    /// 조용히 무제한으로 해석하면 상한이 없는 것과 구분되지 않는다.
    #[test]
    fn a_zero_cap_keeps_nothing() {
        let capped = cap_per_group(chunks(&[("a", 0)]), 0, |(source, _)| source.clone());
        assert!(capped.is_empty());
    }

    #[test]
    fn an_empty_input_stays_empty() {
        let capped: Vec<(String, i32)> = cap_per_group(Vec::new(), 3, |(source, _)| source.clone());
        assert!(capped.is_empty());
    }
}
