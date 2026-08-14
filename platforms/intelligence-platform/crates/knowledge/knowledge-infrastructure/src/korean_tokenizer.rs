//! 한국어 형태소 분석. 근거는 [Intelligence ADR-0003].
//!
//! 왜 필요한가: 한국어는 조사가 명사에 붙어 한 덩어리가 된다. Postgres `simple`
//! 설정은 공백으로만 자르므로 `건폐율을`이 통째로 한 토큰이 되고, 질의 `건폐율`은
//! 아무것도 맞히지 못한다. 고시문은 거의 모든 명사가 조사를 달고 나오므로 이 상태로는
//! 검색이 성립하지 않는다.
//!
//! 사전은 mecab-ko-dic이며 Elasticsearch의 Nori가 쓰는 사전과 같다. 품질 상한은 엔진이
//! 아니라 사전에서 오므로, Postgres 확장을 깔지 않고도 같은 상한에 닿는다.
//!
//! 품사로 거르지 않고 **모든 형태소를 남긴다.** 조사·어미는 모든 문서에 나타나므로
//! BM25가 알아서 가중치를 낮춘다. 품사 규칙을 손으로 짜면 그 규칙이 틀리는 곳이
//! 새 결함이 되고, 그 결함은 "왜 안 찾히지"로만 드러나 찾기 어렵다.
//!
//! [Intelligence ADR-0003]: ../../../../docs/adr/0003-korean-morphology-in-rust.md

use std::borrow::Cow;
use std::sync::OnceLock;

use lindera::dictionary::load_dictionary;
use lindera::mode::Mode;
use lindera::segmenter::Segmenter;

/// 색인에 넣을 텍스트. **형태소 토큰과 원문 토큰을 함께** 낸다.
///
/// 형태소만으로는 부족하다는 것을 실측으로 확인했다. mecab-ko-dic에 없는 단어는
/// 앞뒤 문맥에 따라 Viterbi 경로가 달라져 **같은 단어가 다르게 쪼개진다**:
///
/// ```text
/// "용적률"       -> 용적률        (한 토큰)
/// "용적률 산정"  -> 용적 률 산정  (두 토큰)
/// ```
///
/// 그래서 질의 `용적률`이 본문 `용적률 산정`을 못 맞혔다. `Mode::Decompose`로 바꿔도
/// 같았다. 고시문에는 사전에 없는 행정 용어가 많으므로 이 상태로 두면 검색이 조용히
/// 실패한다 — 오류가 아니라 "결과 없음"으로 나타나 알아채기 어렵다.
///
/// 원문을 함께 넣으면 그 경우를 원문 쪽이 받아 준다. Elasticsearch Nori의 `mixed`
/// decompound 모드가 복합어와 그 부분을 함께 넣는 것과 같은 발상이다. 대가는 색인이
/// 대략 두 배가 되는 것이고, 지금 규모에서는 문제가 되지 않는다.
pub fn build_index_text(text: &str) -> String {
    let morphemes = tokenize_korean(text);
    if morphemes == text {
        return morphemes;
    }
    let mut out = String::with_capacity(morphemes.len() + text.len() + 1);
    out.push_str(&morphemes);
    out.push(' ');
    out.push_str(text);
    out
}

/// 질의에 쓸 텍스트. **형태소만** 낸다 — 색인과 일부러 다르다.
///
/// Postgres의 `websearch_to_tsquery`는 토큰을 AND로 묶으므로, 질의에 원문 토큰까지
/// 넣으면 과잉 제약이 된다. 실제로 질의 `지구단위계획구역`의 원문 토큰이 본문
/// `지구단위계획구역에서는`(조사가 붙어 한 덩어리)에 없어서 4개가 맞는데도 0건이 나왔다.
///
/// 색인은 넓게, 질의는 좁게 — 재현율은 색인 쪽 원문 토큰이 담당한다.
pub fn build_query_text(query: &str) -> String {
    tokenize_korean(query)
}

/// 형태소 경계로만 쪼갠다. 색인·질의 입력은 위 두 함수를 쓸 것.
pub fn tokenize_korean(text: &str) -> String {
    let Some(segmenter) = segmenter() else {
        // 사전 적재 실패는 색인·질의 양쪽에서 같게 동작해야 한다. 원문을 그대로
        // 돌려주면 `simple` 설정과 같은 동작으로 물러날 뿐 결과가 어긋나지 않는다.
        return text.to_string();
    };
    match segmenter.segment(Cow::Borrowed(text)) {
        Ok(tokens) => {
            let mut out = String::with_capacity(text.len() + text.len() / 4);
            for token in &tokens {
                let surface = token.surface.as_ref().trim();
                if surface.is_empty() {
                    continue;
                }
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(surface);
            }
            out
        }
        Err(_) => text.to_string(),
    }
}

/// 사전 적재는 비싸므로 프로세스당 한 번만 한다.
fn segmenter() -> Option<&'static Segmenter> {
    static SEGMENTER: OnceLock<Option<Segmenter>> = OnceLock::new();
    SEGMENTER
        .get_or_init(|| {
            let dictionary = load_dictionary("embedded://ko-dic").ok()?;
            Some(Segmenter::new(Mode::Normal, dictionary, None))
        })
        .as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_a_josa_off_its_noun() {
        let out = tokenize_korean("건폐율을 완화한다");
        assert!(
            out.split_whitespace().any(|t| t == "건폐율"),
            "조사가 분리되어 '건폐율'이 독립 토큰이어야 한다: {out}"
        );
    }

    #[test]
    fn splits_an_eomi_off_its_verb_stem() {
        let out = tokenize_korean("완화하여 적용한다");
        assert!(
            out.split_whitespace().any(|t| t == "완화"),
            "'완화하여'에서 어간이 떨어져야 한다: {out}"
        );
    }

    #[test]
    fn a_bare_noun_survives_unchanged() {
        // 질의어는 보통 조사가 없다. 색인과 같은 함수를 통과해도 그대로여야 맞는다.
        assert_eq!(tokenize_korean("건폐율").trim(), "건폐율");
    }

    #[test]
    fn empty_input_yields_empty_output() {
        assert_eq!(tokenize_korean(""), "");
    }

    /// 사전에 없는 단어는 문맥에 따라 다르게 쪼개진다. 이 사실을 테스트로 고정해 둔다 —
    /// 다음 사람이 "형태소만 쓰면 되지 않나"라고 되돌리는 것을 막는 자리다.
    #[test]
    fn an_unknown_word_segments_differently_depending_on_context() {
        assert_eq!(tokenize_korean("용적률"), "용적률");
        assert_eq!(tokenize_korean("용적률 산정"), "용적 률 산정");
    }

    /// 검색이 성립하는 조건: 질의 토큰이 색인 토큰에 **모두** 있어야 한다
    /// (`websearch_to_tsquery`가 AND로 묶으므로).
    #[test]
    fn every_query_token_is_present_in_the_index_text() {
        for (body, query) in [
            ("해당 구역에서는 건폐율을 완화한다.", "건폐율"), // 조사
            ("용적률 산정 기준", "용적률"),                   // 미등재어 문맥 의존
            ("지구단위계획구역에서는", "지구단위계획구역"),   // 복합명사 + 조사
            ("평택첨단복합 일반산업단지계획 변경 승인", "산업단지"),
        ] {
            let indexed = build_index_text(body);
            let indexed_tokens: Vec<&str> = indexed.split_whitespace().collect();
            for token in build_query_text(query).split_whitespace() {
                assert!(
                    indexed_tokens.contains(&token),
                    "질의 '{query}'의 토큰 '{token}'이 본문 '{body}'의 색인 입력에 없다: {indexed}"
                );
            }
        }
    }

    /// 아직 못 막는 경우를 명시해 둔다: **사전에 없는 단어에 조사가 붙은 것**.
    /// 본문 `용적률을`은 형태소로 `용적 률 을`이 되고 원문 토큰은 `용적률을`이라,
    /// 질의 `용적률`(= `용적률` 한 토큰)이 어느 쪽에도 없다.
    ///
    /// 고칠 방법은 사용자 사전이다 — lindera `Segmenter::new`의 세 번째 인자.
    /// 코퍼스가 오면 자주 나오는 미등재어를 뽑아 사전에 넣으면 이 테스트가 뒤집힌다.
    /// 그때 이 테스트의 이름과 단정을 함께 바꾼다.
    #[test]
    fn known_gap_unknown_word_with_a_josa_is_not_matched_yet() {
        let indexed = build_index_text("용적률을 산정한다");
        let indexed_tokens: Vec<&str> = indexed.split_whitespace().collect();
        assert!(
            !indexed_tokens.contains(&"용적률"),
            "이 한계가 사라졌다면 사용자 사전이 붙은 것이다. 테스트를 뒤집을 것: {indexed}"
        );
    }
}
