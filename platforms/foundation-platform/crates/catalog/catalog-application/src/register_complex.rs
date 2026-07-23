//! Use case for registering canonical industrial complex metadata.

use std::sync::Arc;

use catalog_domain::{CatalogError, IndustrialComplex, IndustrialComplexKind};
use chrono::Utc;
use foundation_shared_kernel::ids::ComplexId;

use crate::industrial_complex_input::{
    validate_clean_required, validate_primary_bjdong_code, validate_source_official_complex_code,
};
use crate::ports::CatalogUnitOfWork;

/// Input required to register a canonical industrial complex.
pub struct RegisterIndustrialComplexInput {
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Domain-level industrial complex kind.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code shared by parcels that belong to the complex.
    pub primary_bjdong_code: String,
    /// Official complex area in square meters.
    pub area_m2: u64,
}

/// Registers a new industrial complex through the Catalog unit of work.
pub struct RegisterIndustrialComplex {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl RegisterIndustrialComplex {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Creates a complex and records the matching Catalog outbox event atomically.
    ///
    /// # Errors
    /// Returns `CatalogError` when the primary legal-dong code already exists or persistence fails.
    pub async fn execute(
        &self,
        input: RegisterIndustrialComplexInput,
    ) -> Result<IndustrialComplex, CatalogError> {
        validate_register_input(&input)?;
        let now = Utc::now();
        let complex = IndustrialComplex {
            id: ComplexId::new(uuid::Uuid::now_v7()),
            official_complex_code: input.official_complex_code,
            name: input.name,
            kind: input.kind,
            primary_bjdong_code: input.primary_bjdong_code,
            area_m2: input.area_m2,
            created_at: now,
            updated_at: now,
            archived_at: None,
            version: 1,
        };

        self.uow.create_complex(&complex).await?;
        Ok(complex)
    }
}

fn validate_register_input(input: &RegisterIndustrialComplexInput) -> Result<(), CatalogError> {
    validate_clean_required(
        "official_complex_code",
        input.official_complex_code.as_str(),
    )?;
    validate_source_official_complex_code(input.official_complex_code.as_str())?;
    validate_clean_required("name", input.name.as_str())?;
    validate_primary_bjdong_code(input.primary_bjdong_code.as_str())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_register_input, RegisterIndustrialComplexInput};
    use catalog_domain::{CatalogError, IndustrialComplexKind};

    #[test]
    fn register_input_rejects_blank_official_complex_code() -> Result<(), &'static str> {
        let input = RegisterIndustrialComplexInput {
            official_complex_code: " ".to_owned(),
            name: "Synthetic Industrial Complex Alpha".to_owned(),
            kind: IndustrialComplexKind::General,
            primary_bjdong_code: "9999900101".to_owned(),
            area_m2: 123_456,
        };

        let error = validate_register_input(&input)
            .err()
            .ok_or("blank code must be invalid")?;

        assert!(matches!(
            error,
            CatalogError::InvalidIndustrialComplexInput(_)
        ));
        Ok(())
    }

    #[test]
    fn register_input_rejects_invalid_primary_bjdong_code_shape() -> Result<(), &'static str> {
        let input = RegisterIndustrialComplexInput {
            official_complex_code: "SYNTHETIC-COMPLEX-001".to_owned(),
            name: "Synthetic Industrial Complex Alpha".to_owned(),
            kind: IndustrialComplexKind::General,
            primary_bjdong_code: "28200".to_owned(),
            area_m2: 123_456,
        };

        let error = validate_register_input(&input)
            .err()
            .ok_or("short primary legal-dong code must be invalid")?;

        assert!(matches!(
            error,
            CatalogError::InvalidIndustrialComplexInput(_)
        ));
        Ok(())
    }

    #[test]
    fn register_input_rejects_placeholder_official_complex_code() -> Result<(), &'static str> {
        let input = RegisterIndustrialComplexInput {
            official_complex_code: "foundation-platform:00000000-0000-7000-8000-000000000001"
                .to_owned(),
            name: "Synthetic Industrial Complex Alpha".to_owned(),
            kind: IndustrialComplexKind::General,
            primary_bjdong_code: "9999900101".to_owned(),
            area_m2: 123_456,
        };

        let error = validate_register_input(&input)
            .err()
            .ok_or("placeholder official code must be invalid")?;

        assert!(matches!(
            error,
            CatalogError::InvalidIndustrialComplexInput(_)
        ));
        Ok(())
    }
}
