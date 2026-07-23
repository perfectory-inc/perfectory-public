use intelligence_normalization_domain::validate_korean_answer;

#[test]
fn domain_owns_language_policy() {
    let result = validate_korean_answer("옥탑 1층");

    assert!(result.passed);
}
