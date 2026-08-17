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

/// Address template that turns an object key into a fetchable URL.
pub mod object_url_template;

/// Parcel Number Unit value object.
pub mod pnu;

pub use errors::KernelError;
pub use ids::{
    BuildingId, ComplexId, ManufacturerId, ParcelId, PostgisProjectionRevisionId, PrincipalId,
    StaffId, VectorTileDataRevisionId, VectorTileReleaseId, VectorTileRuntimeManifestId,
};
pub use object_key::{ObjectKey, ObjectKeyError, ObjectKeyPrefix};
pub use object_url_template::{ObjectUrlTemplate, ObjectUrlTemplateError, OBJECT_KEY_PLACEHOLDER};
pub use pnu::Pnu;
