use serde_json::Value as JsonValue;

pub(in crate::national_data_collection_ledger_execute) fn string_prop(
    value: &JsonValue,
    name: &str,
) -> String {
    value
        .get(name)
        .map(value_to_string)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

pub(in crate::national_data_collection_ledger_execute) fn string_prop_default(
    value: &JsonValue,
    name: &str,
    default: &str,
) -> String {
    let raw = string_prop(value, name);
    if raw.is_empty() {
        default.to_owned()
    } else {
        raw
    }
}

pub(in crate::national_data_collection_ledger_execute) fn string_at(
    value: &JsonValue,
    path: &[&str],
) -> String {
    value_at(value, path)
        .map(value_to_string)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

pub(in crate::national_data_collection_ledger_execute) fn value_at<'a>(
    value: &'a JsonValue,
    path: &[&str],
) -> Option<&'a JsonValue> {
    let mut cursor = value;
    for segment in path {
        cursor = cursor.get(*segment)?;
    }
    Some(cursor)
}

fn value_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(raw) => raw.clone(),
        JsonValue::Number(raw) => raw.to_string(),
        JsonValue::Bool(raw) => raw.to_string(),
        JsonValue::Null => String::new(),
        JsonValue::Array(_) => "[array]".to_owned(),
        JsonValue::Object(_) => "[object]".to_owned(),
    }
}

pub(in crate::national_data_collection_ledger_execute) fn bool_prop(
    value: &JsonValue,
    name: &str,
    default: bool,
) -> bool {
    value
        .get(name)
        .and_then(JsonValue::as_bool)
        .unwrap_or(default)
}

pub(in crate::national_data_collection_ledger_execute) fn u64_prop(
    value: &JsonValue,
    name: &str,
    default: u64,
) -> u64 {
    value
        .get(name)
        .and_then(|raw| {
            raw.as_u64()
                .or_else(|| raw.as_i64().and_then(|number| u64::try_from(number).ok()))
                .or_else(|| raw.as_str().and_then(|text| text.parse::<u64>().ok()))
        })
        .unwrap_or(default)
}
