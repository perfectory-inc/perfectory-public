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

/// 형태소 리트리버에 넣을 질의 텍스트.
///
/// 형태소 신호는 색인도 질의도 **같은 함수**를 통과해야 한다. 한쪽만 쪼개면 토큰 경계가
/// 어긋나 아무것도 맞지 않는다. 원문 리트리버는 질의를 쪼개지 않고 그대로 쓴다.
///
/// 두 신호를 나눠 둔 이유는 실측이다. mecab-ko-dic에 없는 단어는 앞뒤 문맥에 따라
/// Viterbi 경로가 달라져 **같은 단어가 다르게 쪼개진다**:
///
/// ```text
/// "용적률"       -> 용적률        (한 토큰)
/// "용적률 산정"  -> 용적 률 산정  (두 토큰)
/// ```
///
/// 형태소 신호 하나만 있으면 질의 `용적률`이 본문 `용적률 산정`을 못 맞힌다
/// (`Mode::Decompose`로 바꿔도 같았다). 원문 신호가 그 경우를 받아 주고, 조사가 붙은
/// 경우는 형태소 신호가 받아 준다. 둘의 순위 목록은 RRF가 합친다.
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

    /// 형태소 신호가 성립하는 조건: 질의 토큰이 색인 토큰에 **모두** 있어야 한다
    /// (`websearch_to_tsquery`가 AND로 묶으므로). 조사가 붙은 명사가 이 신호의 몫이다.
    #[test]
    fn the_morpheme_signal_matches_a_noun_that_carries_a_josa() {
        for (body, query) in [
            ("해당 구역에서는 건폐율을 완화한다.", "건폐율"),
            ("지구단위계획구역에서는", "지구단위계획구역"),
            ("평택첨단복합 일반산업단지계획 변경 승인", "산업단지"),
        ] {
            let indexed = tokenize_korean(body);
            let indexed_tokens: Vec<&str> = indexed.split_whitespace().collect();
            for token in build_query_text(query).split_whitespace() {
                assert!(
                    indexed_tokens.contains(&token),
                    "질의 '{query}'의 토큰 '{token}'이 본문 '{body}'의 형태소 색인에 없다: {indexed}"
                );
            }
        }
    }

    /// 형태소 신호 **하나만으로는** 미등재어를 놓친다는 사실을 고정한다. 이것이 원문
    /// 리트리버가 존재하는 이유이며, 리트리버를 하나로 되돌리려는 다음 사람이 볼 자리다.
    #[test]
    fn the_morpheme_signal_alone_misses_an_unknown_word() {
        let indexed = tokenize_korean("용적률 산정 기준");
        assert!(
            !indexed.split_whitespace().any(|token| token == "용적률"),
            "형태소만으로 '용적률'이 잡힌다면 사용자 사전이 붙은 것이다. \
             그때 원문 리트리버의 필요성을 다시 판단할 것: {indexed}"
        );
        // 원문 신호는 이 경우를 받아 준다 — 원문에는 '용적률'이 그대로 있다.
        assert!("용적률 산정 기준".contains("용적률"));
    }
}
