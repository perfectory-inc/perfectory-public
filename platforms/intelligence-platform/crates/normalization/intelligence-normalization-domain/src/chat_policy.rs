use serde::{Deserialize, Serialize};

pub const KOREAN_MODEL_PROFILE_ID: &str = "korean-default";
pub const KOREAN_ANSWER_POLICY_ID: &str = "ko-KR-answer-policy";
pub const KOREAN_ANSWER_POLICY_VERSION: &str = "v1";
pub const KOREAN_OUTPUT_VALIDATOR_ID: &str = "ko-KR-output-validator";
pub const KOREAN_OUTPUT_VALIDATOR_VERSION: &str = "v1";
pub const KOREAN_REPAIR_POLICY_ID: &str = "ko-KR-repair-policy";
pub const KOREAN_REPAIR_POLICY_VERSION: &str = "v1";
pub const DEFAULT_TARGET_LANGUAGE: &str = "ko-KR";

const MIN_HANGUL_RATIO: f64 = 0.65;
const MAX_CJK_RATIO: f64 = 0.05;
const MAX_JAPANESE_RATIO: f64 = 0.02;
const ALLOWED_LATIN_TERMS: &[&str] = &[
    "api",
    "ai",
    "bm25",
    "building_link_method",
    "building_mgm_bldrgst_pk",
    "ai_required",
    "entity_context",
    "fastapi",
    "floor_index",
    "floor_number",
    "gemma",
    "ko-kr",
    "minio",
    "model_id",
    "neighbor_unit_examples",
    "normalization_reason",
    "normalization_status",
    "ollama",
    "openai",
    "opensearch",
    "pgvector",
    "postgresql",
    "proposal_required",
    "qdrant",
    "qwen",
    "rag",
    "reasons",
    "redis",
    "required_locale",
    "review_message_ko",
    "s3",
    "same_scope_unit_summary",
    "second_pass_decision",
    "source_id",
    "unit_identity_candidate",
    "unit_name_raw",
    "unit_number",
    "vllm",
];

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LanguageValidationResult {
    pub policy_id: String,
    pub policy_version: String,
    pub target_language: String,
    pub passed: bool,
    pub hangul_ratio: f64,
    pub latin_ratio: f64,
    pub cjk_ratio: f64,
    pub japanese_ratio: f64,
    pub reason: String,
}

pub fn korean_answer_system_prompt() -> String {
    [
        "너는 ko-KR로 답하는 AI 비서다.",
        "최종 답변은 한국어로 작성한다.",
        "영어, 중국어, 일본어 문장을 섞지 않는다.",
        "다만 코드, API 필드명, 제품명, 고유명사는 원문을 유지할 수 있다.",
    ]
    .join(" ")
}

pub fn korean_repair_instruction(original_text: &str) -> String {
    format!(
        "{}\n\n{}",
        [
            "아래 답변을 ko-KR로 다시 작성하라.",
            "코드, API 필드명, 제품명, 고유명사, 허용된 기술 용어는 유지할 수 있다.",
            "영어, 중국어, 일본어 설명 문장은 한국어로 바꾼다.",
        ]
        .join(" "),
        original_text
    )
}

pub fn validate_korean_answer(text: &str) -> LanguageValidationResult {
    let total_letters = text.chars().filter(|ch| ch.is_alphabetic()).count();
    let hangul_count = text.chars().filter(|ch| is_hangul(*ch)).count();
    let latin_count = text.chars().filter(|ch| ch.is_ascii_alphabetic()).count();
    let cjk_count = text.chars().filter(|ch| is_cjk(*ch)).count();
    let japanese_count = text.chars().filter(|ch| is_japanese(*ch)).count();
    let allowed_latin_letters = allowed_latin_letter_count(text);

    let effective_letters = total_letters.saturating_sub(allowed_latin_letters);
    let effective_latin_count = latin_count.saturating_sub(allowed_latin_letters);

    let hangul_ratio = ratio(hangul_count, effective_letters);
    let latin_ratio = ratio(effective_latin_count, effective_letters);
    let cjk_ratio = ratio(cjk_count, effective_letters);
    let japanese_ratio = ratio(japanese_count, effective_letters);
    let passed = hangul_ratio >= MIN_HANGUL_RATIO
        && cjk_ratio <= MAX_CJK_RATIO
        && japanese_ratio <= MAX_JAPANESE_RATIO;

    LanguageValidationResult {
        policy_id: KOREAN_OUTPUT_VALIDATOR_ID.to_string(),
        policy_version: KOREAN_OUTPUT_VALIDATOR_VERSION.to_string(),
        target_language: DEFAULT_TARGET_LANGUAGE.to_string(),
        passed,
        hangul_ratio,
        latin_ratio,
        cjk_ratio,
        japanese_ratio,
        reason: if passed {
            "passed".to_string()
        } else {
            "answer contains too much non-Korean text".to_string()
        },
    }
}

fn ratio(count: usize, total_letters: usize) -> f64 {
    if total_letters == 0 {
        return 0.0;
    }
    count as f64 / total_letters as f64
}

fn is_hangul(ch: char) -> bool {
    ('가'..='힣').contains(&ch)
}

fn is_cjk(ch: char) -> bool {
    ('\u{4e00}'..='\u{9fff}').contains(&ch)
}

fn is_japanese(ch: char) -> bool {
    ('\u{3040}'..='\u{30ff}').contains(&ch)
}

fn allowed_latin_letter_count(text: &str) -> usize {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == ':'))
        .filter(|token| ALLOWED_LATIN_TERMS.contains(&token.to_ascii_lowercase().as_str()))
        .map(|token| token.chars().filter(|ch| ch.is_ascii_alphabetic()).count())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::{korean_answer_system_prompt, korean_repair_instruction, validate_korean_answer};

    #[test]
    fn korean_answer_system_prompt_is_valid_korean_text() {
        let prompt = korean_answer_system_prompt();

        assert!(prompt.contains("한국어"));
        assert!(prompt.contains("최종 답변"));
        assert!(!prompt.contains('�'));
    }

    #[test]
    fn korean_answer_passes_validation_for_plain_korean_sentence() {
        let result = validate_korean_answer("이 답변은 한국어로 작성된 정상적인 문장입니다.");

        assert!(result.passed);
    }

    #[test]
    fn korean_answer_passes_validation_with_allowed_technical_terms() {
        let result =
            validate_korean_answer("RAG는 qdrant와 API 필드명을 유지하면서 한국어로 설명합니다.");

        assert!(result.passed);
    }

    #[test]
    fn korean_normalization_reason_allows_schema_and_machine_tokens() {
        let result = validate_korean_answer(
            "unit_name_raw '지층+J11864001호'는 숫자 추출이 불가능한 비표준 형식입니다.\n\
             동일 건물 내 이웃 단위(102~106호)의 패턴과 일치하지 않아 AI 추론이 불확실합니다.\n\
             따라서 unit_number를 null로 설정하고 normalization_status를 proposal_required로 제안합니다.",
        );

        assert!(result.passed, "{result:?}");
    }

    #[test]
    fn korean_normalization_reason_allows_context_pack_machine_tokens() {
        let result = validate_korean_answer(
            "second_pass_decision.ai_required가 true이므로 AI는 제안만 가능하고 결정을 내릴 수 없습니다.\n\
             same_scope_unit_summary와 entity_context의 neighbor_unit_examples는 단순 숫자 형식(102, 103 등)을 보여주지만, 현재 unit_name_raw('지층+J11864001호')는 이 패턴과 일치하지 않습니다.\n\
             unit_identity_candidate의 unit_number가 null이며, entity_context의 neighbor_unit_examples는 단순 숫자 형식만 포함하여 현재 레코드의 변환을 직접 지원하지 않습니다.\n\
             '지층'은 floor_index 또는 floor_number에 해당할 수 있으나, 'J11864001'이 unit_number인지 아니면 다른 식별자인지 불명확합니다.\n\
             unit_name_raw만으로 unit_number를 추론하기에는 모호성이 너무 크므로, normalization_status는 proposal_required로 설정하고 unit_number는 null로 유지합니다.\n\
             required_locale가 ko-KR이므로 review_message_ko 및 reasons는 한국어로 작성합니다.",
        );

        assert!(result.passed, "{result:?}");
    }

    #[test]
    fn korean_normalization_reason_allows_qwen_unit_smoke_output() {
        let result = validate_korean_answer(
            "second_pass_decision.ai_required가 true이므로 AI 제안 필요\n\
             same_scope_unit_summary에서 동일 범위 단위들은 102~308의 단순 숫자 형식을 사용하지만, 현재 unit_name_raw는 '지층+J...'로 복합 구조임\n\
             unit_identity_candidate의 unit_number가 null이며, entity_context의 neighbor_unit_examples는 단순 숫자 형식만 포함하여 현재 레코드의 변환을 직접 지원하지 않음\n\
             building_mgm_bldrgst_pk와 building_link_method는 entity_context에서 유효하므로 유지\n\
             required_locale가 ko-KR이므로 review_message_ko 및 reasons는 한국어로 작성",
        );

        assert!(result.passed, "{result:?}");
    }

    #[test]
    fn english_answer_fails_validation() {
        let result = validate_korean_answer("This answer is written only in English.");

        assert!(!result.passed);
    }

    #[test]
    fn repair_instruction_keeps_original_text_for_rewrite() {
        let instruction = korean_repair_instruction("This answer is English.");

        assert!(instruction.contains("다시 작성"));
        assert!(instruction.contains("This answer is English."));
    }
}
