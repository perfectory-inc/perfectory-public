//! Conservative normalized unit designation (ADR-0107 §1–2).
//!
//! This is the Rust twin of the dbt macro `foundation_normalized_unit_name`.
//! Both implementations are graded by the same golden-vector seed
//! (`infra/lakehouse/dbt/seeds/unit_name_normalization_vectors.csv`), so a
//! change to one side without the other fails loudly. The vectors freeze the
//! macro's *actual* behavior, quirks included — the virtue of this form is
//! that both registries normalize identically, not that it is pretty.

/// Applies the conservative normal form shared with the dbt macro:
/// 공백·괄호주석·동접두·앞`제`·뒤`호`·층접두 제거, 지층/지하층→`B`,
/// 한글 음역 6종 영문화, 구분자(`·.,`) 제거, 숫자런별 선행 0 제거.
/// 붙임표는 보존한다 — `6-2`와 `62`는 다른 호다 (ADR-0106 §3).
#[must_use]
pub fn normalized_unit_designation(unit_name: &str, dong_name: &str) -> Option<String> {
    let mut s = upper_without_spaces(unit_name);
    s = strip_paren_groups(&s);
    let dong = upper_without_spaces(dong_name);
    if !dong.is_empty() && dong != "0000" && s.starts_with(dong.as_str()) {
        s.drain(..dong.len());
    }
    if s.starts_with('제') {
        s.drain(..'제'.len_utf8());
    }
    if s.ends_with('호') {
        s.truncate(s.len() - '호'.len_utf8());
    }
    s = strip_floor_prefix(&s);
    s = mark_basement_prefix(&s);
    for (spelled, letter) in [
        ("에이치", "H"),
        ("에프", "F"),
        ("에이", "A"),
        ("비", "B"),
        ("씨", "C"),
        ("디", "D"),
    ] {
        s = s.replace(spelled, letter);
    }
    s.retain(|value| !matches!(value, '·' | '.' | ','));
    s = strip_leading_zeros_per_digit_run(&s);
    (!s.is_empty()).then_some(s)
}

/// Uppercases ASCII letters and drops space characters — the macro's
/// `replace(upper(x), ' ', '')`.
fn upper_without_spaces(value: &str) -> String {
    value
        .chars()
        .filter(|value| *value != ' ')
        .map(|value| value.to_ascii_uppercase())
        .collect()
}

/// Removes every `(...)` annotation the way the macro's global
/// `regexp_replace(x, '[(][^)]*[)]', '')` does: an opening paren swallows
/// everything up to the next closing paren; an unclosed paren survives.
fn strip_paren_groups(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '(' {
            if let Some(close) = (index + 1..chars.len()).find(|&at| chars[at] == ')') {
                index = close + 1;
                continue;
            }
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

/// Strips one `^(지하|지상)?[0-9]{1,2}층` prefix; three or more digits mean the
/// run is a unit code, not a floor, and nothing is stripped.
fn strip_floor_prefix(value: &str) -> String {
    let body = value
        .strip_prefix("지하")
        .or_else(|| value.strip_prefix("지상"))
        .unwrap_or(value);
    let digit_count = body.chars().take_while(char::is_ascii_digit).count();
    if (1..=2).contains(&digit_count) {
        if let Some(rest) = body[digit_count..].strip_prefix('층') {
            return rest.to_owned();
        }
    }
    value.to_owned()
}

/// Rewrites one leading `지층`/`지하층` as the basement marker `B`.
fn mark_basement_prefix(value: &str) -> String {
    value
        .strip_prefix("지층")
        .or_else(|| value.strip_prefix("지하층"))
        .map_or_else(|| value.to_owned(), |rest| format!("B{rest}"))
}

/// Trims leading zeros inside every maximal digit run, keeping at least one
/// digit — equivalent to the macro's global
/// `regexp_replace(x, '(^|[^0-9])0+([0-9])', '$1$2')`.
fn strip_leading_zeros_per_digit_run(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut run = String::new();
    for current in value.chars() {
        if current.is_ascii_digit() {
            run.push(current);
        } else {
            flush_digit_run(&mut out, &mut run);
            out.push(current);
        }
    }
    flush_digit_run(&mut out, &mut run);
    out
}

fn flush_digit_run(out: &mut String, run: &mut String) {
    if run.is_empty() {
        return;
    }
    let trimmed = run.trim_start_matches('0');
    out.push_str(if trimmed.is_empty() { "0" } else { trimmed });
    run.clear();
}

#[cfg(test)]
mod tests {
    use super::normalized_unit_designation;

    const GOLDEN_VECTORS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../infra/lakehouse/dbt/seeds/unit_name_normalization_vectors.csv"
    ));

    /// Minimal CSV field split for the seed's shape: three columns, optional
    /// double quotes around a field, no embedded quotes.
    fn split_seed_line(line: &str) -> Option<(String, String, String)> {
        let mut fields = Vec::new();
        let mut rest = line;
        for _ in 0..3 {
            let (field, remaining) = if let Some(quoted) = rest.strip_prefix('"') {
                let end = quoted.find('"')?;
                let field = &quoted[..end];
                let after = &quoted[end + 1..];
                (field, after.strip_prefix(',').unwrap_or(after))
            } else if let Some(comma) = rest.find(',') {
                (&rest[..comma], &rest[comma + 1..])
            } else {
                (rest, "")
            };
            fields.push(field.to_owned());
            rest = remaining;
        }
        let mut fields = fields.into_iter();
        Some((fields.next()?, fields.next()?, fields.next()?))
    }

    /// 골든 벡터 교차 시험 (ADR-0107 §2): dbt 매크로를 실행해 채취한 정답을
    /// 이 함수가 전량 재현해야 한다. dbt 쪽은 같은 seed 를
    /// `tests/assert_normalizer_matches_golden_vectors.sql` 이 채점한다.
    #[test]
    fn every_golden_vector_matches_the_dbt_macro() {
        let mut graded = 0;
        for line in GOLDEN_VECTORS.lines().skip(1) {
            if line.trim().is_empty() {
                continue;
            }
            let split = split_seed_line(line);
            assert!(split.is_some(), "seed line is not three fields: {line}");
            let Some((raw_name, dong_name, expected)) = split else {
                continue;
            };
            let actual = normalized_unit_designation(&raw_name, &dong_name);
            let expected = (!expected.is_empty()).then_some(expected);
            assert_eq!(
                actual, expected,
                "vector raw={raw_name:?} dong={dong_name:?}"
            );
            graded += 1;
        }
        assert!(graded >= 30, "seed unexpectedly small: {graded} vectors");
    }
}
