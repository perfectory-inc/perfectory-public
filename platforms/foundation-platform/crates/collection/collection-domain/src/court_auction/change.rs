use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// The SHA-256 of a row's watched fields.
pub type Fingerprint = [u8; 32];

/// The fingerprint of `row` over `fields`, in the contract's order.
///
/// Each field contributes its name and its JSON value (a missing field counts as `null`), each
/// length-prefixed, so a value cannot run into the next field and the same row always gives the
/// same bytes. Fields outside the list do not count: a row whose watched fields are unchanged is
/// unchanged.
#[must_use]
pub fn fingerprint(row: &Value, fields: &[String]) -> Fingerprint {
    let mut hasher = Sha256::new();
    for field in fields {
        let value = row.get(field).unwrap_or(&Value::Null).to_string();
        for part in [field.as_str(), value.as_str()] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn fields() -> Vec<String> {
        vec!["sale_date".to_owned(), "status".to_owned()]
    }

    #[test]
    fn only_watched_fields_count() {
        let before = json!({"sale_date": "20990101", "status": "a", "views": 1});
        let after = json!({"sale_date": "20990101", "status": "a", "views": 9});
        assert_eq!(
            fingerprint(&before, &fields()),
            fingerprint(&after, &fields())
        );
    }

    #[test]
    fn a_watched_change_or_a_missing_field_changes_it() {
        let before = json!({"sale_date": "20990101", "status": "a"});
        let moved = json!({"sale_date": "20990108", "status": "a"});
        let missing = json!({"sale_date": "20990101"});
        assert_ne!(
            fingerprint(&before, &fields()),
            fingerprint(&moved, &fields())
        );
        assert_ne!(
            fingerprint(&before, &fields()),
            fingerprint(&missing, &fields())
        );
    }

    #[test]
    fn values_cannot_run_into_the_next_field() {
        let one = json!({"a": "xy", "b": ""});
        let other = json!({"a": "x", "b": "y"});
        let fields = vec!["a".to_owned(), "b".to_owned()];
        assert_ne!(fingerprint(&one, &fields), fingerprint(&other, &fields));
    }
}
