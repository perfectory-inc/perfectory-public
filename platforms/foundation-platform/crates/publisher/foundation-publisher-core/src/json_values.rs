//! A JSON document as the values it says, not as one engine writes it (rules.txt "no Spark byte
//! emulation"; root ADR-0564 §6-5 and its Revision 2026-11-22 (둘째)).
//!
//! The one reading both ends of a takeover hold two engines' writings to: the switch's content check
//! (`section_packs::expansion::same_content` of the by-PNU publisher) compares the documents it
//! bakes with the served ones through it, and the takeover verdict (`gold_takeover_sides` of the
//! outbox publisher) compares the Gold's `*_json` columns through it. A null member is the field's
//! absence (an optional field an older writer never wrote); a number is its value (`1.0E7`,
//! `10000000.0` and `10000000` are one); an instant is its time (`…00.000Z` and `…00Z` are one).
//! Text, the members of an object and the order of a list stay as written; members are compared
//! as a set (an object is a map).

use serde_json::Value as JsonValue;

/// The largest integer an `f64` holds exactly (2^53): a whole number above it stays a float.
const EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

/// `value` as the values it says: null members dropped, each number as its value (a whole float
/// as the integer it is) and each instant as UTC with the fraction it needs.
pub fn canonical(value: &mut JsonValue) {
    drop_nulls(value);
    canonical_values(value);
}

/// `text` read as JSON and written again as [`canonical`] reads it, its members in one order
/// (sorted by name), so two writings of one value are one text; `None` when `text` is not JSON.
#[must_use]
pub fn canonical_text(text: &str) -> Option<String> {
    let mut value: JsonValue = serde_json::from_str(text).ok()?;
    canonical(&mut value);
    let mut written = String::with_capacity(text.len());
    write_sorted(&value, &mut written);
    Some(written)
}

fn canonical_values(value: &mut JsonValue) {
    match value {
        JsonValue::Number(number) => {
            if let Some(float) = number.as_f64().filter(|_| number.is_f64()) {
                if float.fract() == 0.0 && float.abs() <= EXACT_INTEGER {
                    #[allow(clippy::cast_possible_truncation)]
                    let whole = float as i64;
                    *value = JsonValue::from(whole);
                }
            }
        }
        JsonValue::String(text) => {
            if let Ok(instant) = chrono::DateTime::parse_from_rfc3339(text) {
                *text = instant
                    .with_timezone(&chrono::Utc)
                    .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
            }
        }
        JsonValue::Object(fields) => fields.values_mut().for_each(canonical_values),
        JsonValue::Array(items) => items.iter_mut().for_each(canonical_values),
        _ => {}
    }
}

fn drop_nulls(value: &mut JsonValue) {
    match value {
        JsonValue::Object(fields) => {
            fields.retain(|_, field| !field.is_null());
            fields.values_mut().for_each(drop_nulls);
        }
        JsonValue::Array(items) => items.iter_mut().for_each(drop_nulls),
        _ => {}
    }
}

/// `value` as compact JSON with every object's members sorted by name, whatever order the map
/// keeps (the workspace's `serde_json` may one day preserve insertion order).
fn write_sorted(value: &JsonValue, out: &mut String) {
    match value {
        JsonValue::Object(fields) => {
            let mut names: Vec<&String> = fields.keys().collect();
            names.sort();
            out.push('{');
            for (index, name) in names.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&JsonValue::String(name.clone()).to_string());
                out.push(':');
                if let Some(field) = fields.get(name) {
                    write_sorted(field, out);
                }
            }
            out.push('}');
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_sorted(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spark's and `DuckDB`'s writings of one value are one text; a null member is an absent one.
    #[test]
    fn two_writings_of_one_value_are_one_text() {
        let spark = r#"{"area_m2":1.0E7,"usage":19.500,"rows":3,"at":"2099-01-02T03:04:05.000Z",
            "name":"\u001F합성","list":[{"b":1,"a":null}],"note":null}"#;
        let duckdb = r#"{"list":[{"b":1.0}],"name":"\u001f합성","rows":3.0,"usage":19.5,
            "area_m2":10000000.0,"at":"2099-01-02T12:04:05+09:00"}"#;
        assert_eq!(canonical_text(spark), canonical_text(duckdb));
        assert_eq!(
            canonical_text(duckdb).as_deref(),
            Some(
                "{\"area_m2\":10000000,\"at\":\"2099-01-02T03:04:05Z\",\"list\":[{\"b\":1}],\
                 \"name\":\"\\u001f합성\",\"rows\":3,\"usage\":19.5}"
            )
        );
    }

    /// Another value, a number written as text, another instant and a list in another order differ;
    /// text that is not JSON has no canonical text.
    #[test]
    fn a_different_value_stays_different() {
        let base = canonical_text(r#"{"a":1,"l":[1,2],"t":"x","at":"2099-01-02T03:04:05Z"}"#);
        for other in [
            r#"{"a":1.5,"l":[1,2],"t":"x","at":"2099-01-02T03:04:05Z"}"#,
            r#"{"a":"1","l":[1,2],"t":"x","at":"2099-01-02T03:04:05Z"}"#,
            r#"{"a":1,"l":[2,1],"t":"x","at":"2099-01-02T03:04:05Z"}"#,
            r#"{"a":1,"l":[1,2],"t":"X","at":"2099-01-02T03:04:05Z"}"#,
            r#"{"a":1,"l":[1,2],"t":"x","at":"2099-01-02T03:04:06Z"}"#,
        ] {
            assert_ne!(base, canonical_text(other), "{other}");
        }
        for not_json in ["", "nul", "{"] {
            assert_eq!(canonical_text(not_json), None, "{not_json:?}");
        }
        assert_eq!(canonical_text("null").as_deref(), Some("null"));
        assert_eq!(canonical_text("[]").as_deref(), Some("[]"));
    }
}
