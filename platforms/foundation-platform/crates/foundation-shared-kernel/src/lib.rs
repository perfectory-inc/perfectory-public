//! Shared kernel types used across foundation-platform bounded contexts.
//!
//! Keep this crate intentionally small. It contains only cross-context identifiers, value
//! objects, and event wire contracts shared by Foundation capabilities.

/// Shared kernel error types.
pub mod errors;

/// Cross-context event wire contracts.
pub mod events;

/// Strongly typed identifiers shared across contexts.
pub mod ids;

/// Provider-neutral object storage key value objects.
pub mod object_key;

/// Parcel Number Unit value object.
pub mod pnu;

pub use errors::KernelError;
pub use ids::{BuildingId, ComplexId, ManufacturerId, ParcelId, PrincipalId, StaffId};
pub use object_key::{ObjectKey, ObjectKeyError, ObjectKeyPrefix};
pub use pnu::Pnu;
