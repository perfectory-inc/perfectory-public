use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context};
use serde_json::{Map as JsonMap, Value as JsonValue};

const EVIDENCE_SCHEMA_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/spatial_tile_wap_evidence.schema.json");

fn object<'a>(value: &'a JsonValue, label: &str) -> anyhow::Result<&'a JsonMap<String, JsonValue>> {
    value
        .as_object()
        .with_context(|| format!("{label} must be an object"))
}

fn exact_object<'a>(
    value: &'a JsonValue,
    label: &str,
    expected: &[&str],
) -> anyhow::Result<&'a JsonMap<String, JsonValue>> {
    let object = object(value, label)?;
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        bail!("{label} has unsupported or missing keys");
    }
    Ok(object)
}

pub(super) fn string_list<'a>(value: &'a JsonValue, label: &str) -> anyhow::Result<Vec<&'a str>> {
    let values = value
        .as_array()
        .filter(|values| !values.is_empty())
        .with_context(|| format!("{label} must be a non-empty array"))?;
    let strings = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .with_context(|| format!("{label} must contain non-empty strings"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if strings.iter().copied().collect::<BTreeSet<_>>().len() != strings.len() {
        bail!("{label} must not contain duplicates");
    }
    Ok(strings)
}

pub(super) fn evidence_contract() -> anyhow::Result<JsonValue> {
    let contract: JsonValue = serde_json::from_str(EVIDENCE_SCHEMA_JSON)
        .context("failed to parse WAP evidence schema")?;
    validate_evidence_contract(&contract)?;
    Ok(contract)
}

fn properties(contract: &JsonValue) -> anyhow::Result<&JsonMap<String, JsonValue>> {
    object(&contract["properties"], "contract properties")
}

pub(super) fn property_const(contract: &JsonValue, field: &str) -> anyhow::Result<String> {
    properties(contract)?
        .get(field)
        .and_then(|rule| rule.get("const"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .with_context(|| format!("contract property {field} must have a string const"))
}

fn result_fast_forward_map(contract: &JsonValue) -> anyhow::Result<BTreeMap<String, String>> {
    let clauses = contract["allOf"]
        .as_array()
        .filter(|clauses| !clauses.is_empty())
        .context("contract allOf must be a non-empty array")?;
    let mut mapping = BTreeMap::new();
    for (index, clause) in clauses.iter().enumerate() {
        let clause = exact_object(clause, &format!("allOf[{index}]"), &["if", "then"])?;
        let condition = exact_object(
            &clause["if"],
            &format!("allOf[{index}].if"),
            &["properties", "required"],
        )?;
        let consequence = exact_object(
            &clause["then"],
            &format!("allOf[{index}].then"),
            &["properties", "required"],
        )?;
        if string_list(&condition["required"], "allOf condition required")? != ["result"]
            || string_list(&consequence["required"], "allOf consequence required")?
                != ["fast_forward"]
        {
            bail!("allOf condition and consequence require the wrong fields");
        }
        let condition_properties = exact_object(
            &condition["properties"],
            "allOf condition properties",
            &["result"],
        )?;
        let consequence_properties = exact_object(
            &consequence["properties"],
            "allOf consequence properties",
            &["fast_forward"],
        )?;
        let result_rule = exact_object(
            &condition_properties["result"],
            "allOf result rule",
            &["enum"],
        )?;
        let status_rule = exact_object(
            &consequence_properties["fast_forward"],
            "allOf fast_forward rule",
            &["const"],
        )?;
        let status = status_rule["const"]
            .as_str()
            .context("allOf fast_forward const must be a string")?;
        for result in string_list(&result_rule["enum"], "allOf result enum")? {
            if mapping
                .insert(result.to_owned(), status.to_owned())
                .is_some()
            {
                bail!("result {result} has duplicate allOf rules");
            }
        }
    }
    Ok(mapping)
}

pub(super) fn validate_evidence_contract(contract: &JsonValue) -> anyhow::Result<()> {
    let contract = exact_object(
        contract,
        "evidence contract",
        &[
            "$schema",
            "$id",
            "title",
            "type",
            "additionalProperties",
            "required",
            "properties",
            "allOf",
            "x-perfectory-cross-field-invariants",
            "x-perfectory-branch-pair",
        ],
    )?;
    if contract["$schema"] != "https://json-schema.org/draft/2020-12/schema"
        || contract["type"] != "object"
        || contract["additionalProperties"] != false
        || contract["$id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .is_none()
        || contract["title"]
            .as_str()
            .filter(|value| !value.is_empty())
            .is_none()
    {
        bail!("evidence contract header is invalid");
    }
    let required = string_list(&contract["required"], "contract required")?;
    let properties = object(&contract["properties"], "contract properties")?;
    if required.iter().copied().collect::<BTreeSet<_>>()
        != properties
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
    {
        bail!("contract required fields must equal properties");
    }
    let allowed = ["type", "const", "enum", "minimum"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    for (name, raw_rule) in properties {
        let rule = object(raw_rule, &format!("property {name}"))?;
        if !rule
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            .is_subset(&allowed)
        {
            bail!("property {name} has unsupported keywords");
        }
        match rule["type"].as_str() {
            Some("string") => {
                if rule.contains_key("minimum")
                    || rule.get("const").is_some_and(|value| !value.is_string())
                    || rule
                        .get("enum")
                        .is_some_and(|value| string_list(value, name).is_err())
                {
                    bail!("string property {name} has invalid constraints");
                }
            }
            Some("integer") => {
                let minimum = rule["minimum"]
                    .as_u64()
                    .filter(|minimum| *minimum > 0)
                    .with_context(|| format!("integer property {name} needs a positive minimum"))?;
                if rule
                    .get("const")
                    .is_some_and(|value| value.as_u64().is_none_or(|value| value < minimum))
                    || rule.contains_key("enum")
                {
                    bail!("integer property {name} has invalid constraints");
                }
            }
            _ => bail!("property {name} has unsupported type"),
        }
        if rule.contains_key("const") && rule.contains_key("enum") {
            bail!("property {name} cannot define const and enum");
        }
    }
    let contract_value = JsonValue::Object(contract.clone());
    let mapping = result_fast_forward_map(&contract_value)?;
    let result_values = string_list(&properties["result"]["enum"], "result enum")?;
    if mapping.keys().map(String::as_str).collect::<BTreeSet<_>>()
        != result_values.into_iter().collect::<BTreeSet<_>>()
    {
        bail!("allOf mapping must cover the result enum exactly");
    }
    let fast_values = string_list(&properties["fast_forward"]["enum"], "fast_forward enum")?;
    if !mapping
        .values()
        .all(|status| fast_values.contains(&status.as_str()))
    {
        bail!("allOf uses an invalid fast_forward status");
    }
    let invariants = contract["x-perfectory-cross-field-invariants"]
        .as_array()
        .filter(|values| !values.is_empty())
        .context("cross-field invariants must be a non-empty array")?;
    for (index, invariant) in invariants.iter().enumerate() {
        let invariant = exact_object(invariant, &format!("invariant[{index}]"), &["op", "fields"])?;
        let operation = invariant["op"]
            .as_str()
            .context("invariant op must be a string")?;
        let fields = string_list(&invariant["fields"], "invariant fields")?;
        if !matches!(
            (operation, fields.len()),
            ("equal", 2) | ("all_distinct", 2..)
        ) || fields
            .iter()
            .any(|field| properties[*field]["type"] != "integer")
        {
            bail!("unsupported or malformed invariant {operation}");
        }
    }
    let branch_pair = exact_object(
        &contract["x-perfectory-branch-pair"],
        "branch pair",
        &[
            "historical",
            "publication",
            "suffix_encoding",
            "suffix_length",
            "same_suffix",
        ],
    )?;
    if branch_pair["suffix_encoding"] != "lowercase_hex"
        || branch_pair["suffix_length"]
            .as_u64()
            .filter(|value| *value > 0)
            .is_none()
        || branch_pair["same_suffix"] != true
    {
        bail!("branch pair suffix contract is invalid");
    }
    let mut branch_fields = BTreeSet::new();
    for role in ["historical", "publication"] {
        let specification = exact_object(&branch_pair[role], role, &["field", "prefix"])?;
        let field = specification["field"]
            .as_str()
            .context("branch field must be a string")?;
        if properties[field]["type"] != "string"
            || specification["prefix"]
                .as_str()
                .filter(|value| !value.is_empty())
                .is_none()
            || !branch_fields.insert(field)
        {
            bail!("{role} branch specification is invalid");
        }
    }
    Ok(())
}

pub(super) fn branch_prefix<'a>(contract: &'a JsonValue, role: &str) -> anyhow::Result<&'a str> {
    contract["x-perfectory-branch-pair"][role]["prefix"]
        .as_str()
        .with_context(|| format!("{role} branch prefix is missing"))
}

pub(super) fn validate_evidence_payload_against_contract(
    payload: &JsonValue,
    contract: &JsonValue,
) -> anyhow::Result<()> {
    validate_evidence_contract(contract)?;
    let payload = object(payload, "WAP evidence")?;
    let required = string_list(&contract["required"], "contract required")?;
    if payload.keys().map(String::as_str).collect::<BTreeSet<_>>()
        != required.into_iter().collect::<BTreeSet<_>>()
    {
        bail!("WAP evidence fields do not match the strict schema");
    }
    let properties = properties(contract)?;
    for (name, rule) in properties {
        let value = &payload[name];
        match rule["type"].as_str() {
            Some("string") if value.is_string() => {}
            Some("integer")
                if value
                    .as_u64()
                    .is_some_and(|value| value >= rule["minimum"].as_u64().unwrap_or(u64::MAX)) => {
            }
            _ => bail!("WAP evidence {name} has the wrong JSON type"),
        }
        if rule.get("const").is_some_and(|expected| value != expected)
            || rule.get("enum").is_some_and(|values| {
                values
                    .as_array()
                    .is_none_or(|values| !values.contains(value))
            })
        {
            bail!("WAP evidence {name} violates its contract");
        }
    }
    let mapping = result_fast_forward_map(contract)?;
    let result = payload["result"]
        .as_str()
        .context("result must be a string")?;
    let expected_status = mapping.get(result).context("result is not allowed")?;
    if payload["fast_forward"] != *expected_status {
        bail!("result and fast_forward are inconsistent");
    }
    for invariant in contract["x-perfectory-cross-field-invariants"]
        .as_array()
        .context("contract invariants must be an array")?
    {
        let operation = invariant["op"]
            .as_str()
            .context("invariant op is missing")?;
        let fields = string_list(&invariant["fields"], "invariant fields")?;
        let values = fields
            .iter()
            .map(|field| &payload[*field])
            .collect::<Vec<_>>();
        match operation {
            "equal" if values[0] == values[1] => {}
            "all_distinct"
                if values
                    .iter()
                    .enumerate()
                    .all(|(index, value)| !values[index + 1..].contains(value)) => {}
            "equal" | "all_distinct" => {
                bail!(
                    "WAP evidence invariant {operation} failed for {}",
                    fields.join(",")
                )
            }
            _ => bail!("unsupported cross-field operation {operation}"),
        }
    }
    let pair = &contract["x-perfectory-branch-pair"];
    let suffix_length = pair["suffix_length"]
        .as_u64()
        .context("branch suffix length is missing")? as usize;
    let mut suffixes = Vec::new();
    for role in ["historical", "publication"] {
        let field = pair[role]["field"]
            .as_str()
            .context("branch field is missing")?;
        let prefix = branch_prefix(contract, role)?;
        let value = payload[field]
            .as_str()
            .context("branch value must be a string")?;
        let suffix = value
            .strip_prefix(prefix)
            .filter(|suffix| {
                suffix.len() == suffix_length
                    && suffix
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            .with_context(|| format!("WAP evidence {field} suffix is invalid"))?;
        suffixes.push(suffix);
    }
    if suffixes[0] != suffixes[1] {
        bail!("WAP evidence branch suffixes must match");
    }
    Ok(())
}
