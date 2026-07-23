//! Normalization `PostgreSQL` persistence and Catalog transaction coordination.

#![deny(missing_docs)]

mod application;
mod building_register_unit;
mod industrial_complex;
mod postgres_error;
mod proposal;
mod review;
mod row_mapping;

/// `PostgreSQL` reader for active building-register-unit overrides.
pub mod active_override_reader;

/// `PostgreSQL` Normalization unit of work.
pub mod postgres_unit_of_work;

pub use active_override_reader::PgActiveBuildingRegisterUnitOverrideReader;
pub use postgres_unit_of_work::PgNormalizationUnitOfWork;
