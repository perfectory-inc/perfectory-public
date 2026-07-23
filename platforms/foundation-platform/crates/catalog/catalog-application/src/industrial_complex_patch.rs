//! Catalog-owned parsing for industrial-complex canonical patch commands.

use catalog_domain::{CatalogError, ComplexMutation, IndustrialComplex};
use foundation_shared_kernel::ids::ComplexId;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::industrial_complex_input::validate_clean_required;

/// Opaque Catalog-owned patch that has passed canonical industrial-complex validation.
///
/// Direct construction is intentionally unavailable; callers must use
/// [`parse_industrial_complex_proposed_record`].
///
/// ```compile_fail
/// use catalog_application::industrial_complex_patch::IndustrialComplexPatch;
/// use catalog_domain::ComplexMutation;
///
/// let _ = IndustrialComplexPatch(ComplexMutation {
///     name: Some("Bypass".to_owned()),
///     area_m2: Some(1),
/// });
/// ```
#[derive(Clone, Debug)]
pub struct IndustrialComplexPatch(ComplexMutation);

impl IndustrialComplexPatch {
    /// Verifies that the patch changes at least one canonical field.
    ///
    /// # Errors
    /// Returns `CatalogError::InvalidIndustrialComplexInput` when every supplied value already
    /// equals the locked canonical state. Such an application would create a version and audit
    /// event without a compensatable state change.
    pub fn validate_changes(&self, current: &IndustrialComplex) -> Result<(), CatalogError> {
        let mutation = &self.0;
        let name_changes = mutation
            .name
            .as_ref()
            .is_some_and(|name| name != &current.name);
        let area_changes = mutation
            .area_m2
            .is_some_and(|area_m2| area_m2 != current.area_m2);
        if name_changes || area_changes {
            return Ok(());
        }
        Err(CatalogError::InvalidIndustrialComplexInput(
            "industrial complex mutation must change canonical state".to_owned(),
        ))
    }

    /// Consumes the patch and keeps only fields that differ from the locked canonical state.
    ///
    /// # Errors
    /// Returns `CatalogError::InvalidIndustrialComplexInput` when every supplied value already
    /// equals the locked canonical state.
    pub fn into_effective_mutation(
        self,
        current: &IndustrialComplex,
    ) -> Result<ComplexMutation, CatalogError> {
        let mut mutation = self.0;
        if mutation
            .name
            .as_ref()
            .is_some_and(|name| name == &current.name)
        {
            mutation.name = None;
        }
        if mutation
            .area_m2
            .is_some_and(|area_m2| area_m2 == current.area_m2)
        {
            mutation.area_m2 = None;
        }
        if mutation.name.is_none() && mutation.area_m2.is_none() {
            return Err(CatalogError::InvalidIndustrialComplexInput(
                "industrial complex mutation must change canonical state".to_owned(),
            ));
        }
        Ok(mutation)
    }

    /// Consumes the validated patch and returns the canonical Catalog mutation.
    #[must_use]
    pub fn into_mutation(self) -> ComplexMutation {
        self.0
    }
}

/// Catalog command derived from a persisted canonical snapshot for a compensating restore.
///
/// Direct construction is intentionally unavailable; callers must use
/// [`parse_industrial_complex_restore_input`].
///
/// ```compile_fail
/// use catalog_application::industrial_complex_patch::{
///     IndustrialComplexPatch, RestoreIndustrialComplexInput,
/// };
/// use foundation_shared_kernel::ids::ComplexId;
///
/// fn bypass(target_id: ComplexId, patch: IndustrialComplexPatch) {
///     let _ = RestoreIndustrialComplexInput {
///         target_id,
///         applied_version: 1,
///         expected_current: patch.clone(),
///         patch,
///     };
/// }
/// ```
#[derive(Clone, Debug)]
pub struct RestoreIndustrialComplexInput {
    target_id: ComplexId,
    expected_current: SnapshotFields,
    patch: IndustrialComplexPatch,
}

impl RestoreIndustrialComplexInput {
    /// Returns the canonical target id guarded by this compensation.
    #[must_use]
    pub const fn target_id(&self) -> ComplexId {
        self.target_id
    }

    /// Verifies that canonical state still equals the ledger head approved for compensation.
    ///
    /// # Errors
    /// Returns `CatalogError::ComplexStateConflict` when version or canonical values differ from
    /// the validated normalization ledger head.
    pub fn validate_current(&self, current: &IndustrialComplex) -> Result<(), CatalogError> {
        if current.version == self.expected_current.version
            && current.name == self.expected_current.name
            && current.area_m2 == self.expected_current.area_m2
        {
            return Ok(());
        }
        Err(CatalogError::ComplexStateConflict(
            self.target_id.to_string(),
        ))
    }

    /// Consumes the validated restore input into the exact inverse patch.
    #[must_use]
    pub fn into_patch(self) -> IndustrialComplexPatch {
        self.patch
    }
}

/// Parses an industrial-complex target identity into its canonical Catalog id.
///
/// # Errors
/// Returns `CatalogError` when `complex_id` is missing or is not an exact UUID string.
pub fn parse_industrial_complex_target_identity(
    target_identity: &JsonValue,
) -> Result<ComplexId, CatalogError> {
    let raw = target_identity
        .get("complex_id")
        .and_then(JsonValue::as_str)
        .ok_or_else(invalid_target_identity)?;
    let uuid = Uuid::parse_str(raw).map_err(|_| invalid_target_identity())?;
    Ok(ComplexId::new(uuid))
}

/// Parses a proposed industrial-complex record into a canonical Catalog mutation.
///
/// # Errors
/// Returns `CatalogError` when the payload is not an object, contains unsupported fields, or
/// does not contain a valid `name` or `area_m2` change.
pub fn parse_industrial_complex_proposed_record(
    proposed_record: &JsonValue,
) -> Result<IndustrialComplexPatch, CatalogError> {
    let object = proposed_record.as_object().ok_or_else(|| {
        CatalogError::InvalidIndustrialComplexInput(
            "proposed_record must be a JSON object".to_owned(),
        )
    })?;

    for field in object.keys() {
        if !matches!(field.as_str(), "name" | "area_m2") {
            return Err(CatalogError::InvalidIndustrialComplexInput(format!(
                "unsupported industrial_complex normalization field: {field}"
            )));
        }
    }

    let name = object
        .get("name")
        .map(parse_industrial_complex_name)
        .transpose()?;
    let area_m2 = object
        .get("area_m2")
        .map(parse_industrial_complex_area_m2)
        .transpose()?;

    if name.is_none() && area_m2.is_none() {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "at least one industrial complex field must be changed".to_owned(),
        ));
    }

    Ok(IndustrialComplexPatch(ComplexMutation { name, area_m2 }))
}

/// Parses a pre-change snapshot into a Catalog restore command for the expected target.
///
/// # Errors
/// Returns `CatalogError` when the snapshot is not an object, its identity does not match the
/// target, or any canonical restore field is missing or invalid.
pub fn parse_industrial_complex_restore_input(
    before_snapshot: &JsonValue,
    after_snapshot: &JsonValue,
    expected_current_snapshot: &JsonValue,
    target_id: ComplexId,
) -> Result<RestoreIndustrialComplexInput, CatalogError> {
    let before = parse_snapshot(before_snapshot, target_id, "before_snapshot")?;
    let after = parse_snapshot(after_snapshot, target_id, "after_snapshot")?;
    let expected_current = parse_snapshot(
        expected_current_snapshot,
        target_id,
        "expected_current_snapshot",
    )?;
    if after.version != before.version + 1 {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "industrial complex application snapshot must increment version by one".to_owned(),
        ));
    }
    if expected_current.version < after.version
        || expected_current.name != after.name
        || expected_current.area_m2 != after.area_m2
    {
        return Err(CatalogError::ComplexStateConflict(target_id.to_string()));
    }
    let restore_record = serde_json::json!({
        "name": (before.name != after.name).then_some(before.name),
        "area_m2": (before.area_m2 != after.area_m2).then_some(before.area_m2),
    });
    let restore_object = restore_record.as_object().ok_or_else(|| {
        CatalogError::InvalidIndustrialComplexInput(
            "industrial complex restore record must be an object".to_owned(),
        )
    })?;
    let restore_record = JsonValue::Object(
        restore_object
            .iter()
            .filter(|(_, value)| !value.is_null())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    );
    let patch = parse_industrial_complex_proposed_record(&restore_record)?;
    Ok(RestoreIndustrialComplexInput {
        target_id,
        expected_current,
        patch,
    })
}

#[derive(Clone, Debug)]
struct SnapshotFields {
    name: String,
    area_m2: u64,
    version: i64,
}

fn parse_snapshot(
    snapshot: &JsonValue,
    target_id: ComplexId,
    field_name: &str,
) -> Result<SnapshotFields, CatalogError> {
    let object = snapshot.as_object().ok_or_else(|| {
        CatalogError::InvalidIndustrialComplexInput(format!("{field_name} must be a JSON object"))
    })?;
    let snapshot_id = object
        .get("id")
        .and_then(JsonValue::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .map(ComplexId::new)
        .ok_or_else(|| {
            CatalogError::InvalidIndustrialComplexInput(format!(
                "{field_name}.id must be a UUID string"
            ))
        })?;
    if snapshot_id != target_id {
        return Err(CatalogError::InvalidIndustrialComplexInput(format!(
            "{field_name}.id must match application target_id"
        )));
    }

    let name = object
        .get("name")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            CatalogError::InvalidIndustrialComplexInput(format!(
                "{field_name}.name must be a non-empty string"
            ))
        })?;
    let area_m2 = object
        .get("area_m2")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            CatalogError::InvalidIndustrialComplexInput(format!(
                "{field_name}.area_m2 must be a non-negative integer"
            ))
        })?;
    let version = object
        .get("version")
        .and_then(JsonValue::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            CatalogError::InvalidIndustrialComplexInput(format!(
                "{field_name}.version must be a positive integer"
            ))
        })?;
    Ok(SnapshotFields {
        name,
        area_m2,
        version,
    })
}

fn invalid_target_identity() -> CatalogError {
    CatalogError::InvalidIndustrialComplexInput(
        "target_identity.complex_id must be a UUID string".to_owned(),
    )
}

fn parse_industrial_complex_name(value: &JsonValue) -> Result<String, CatalogError> {
    let raw = value.as_str().ok_or_else(|| {
        CatalogError::InvalidIndustrialComplexInput("name must be a string".to_owned())
    })?;
    validate_clean_required("name", raw)?;
    Ok(raw.to_owned())
}

fn parse_industrial_complex_area_m2(value: &JsonValue) -> Result<u64, CatalogError> {
    let area_m2 = value.as_u64().ok_or_else(|| {
        CatalogError::InvalidIndustrialComplexInput("area_m2 must be a positive integer".to_owned())
    })?;
    if area_m2 == 0 {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "area_m2 must be positive".to_owned(),
        ));
    }
    Ok(area_m2)
}
