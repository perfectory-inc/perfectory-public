//! Manufacturer metadata assigned to industrial complex parcels.
//!
//! Some fields can be sensitive and should be exposed only through authorized application
//! surfaces.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ManufacturerId, ParcelId};
use serde::{Deserialize, Serialize};

/// Manufacturer or tenant company associated with a parcel.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manufacturer {
    /// Stable foundation-platform manufacturer identifier.
    pub id: ManufacturerId,
    /// Primary parcel occupied by the manufacturer.
    pub primary_parcel_id: ParcelId,
    /// Manufacturer display name.
    pub name: String,
    /// Korean Standard Industrial Classification code.
    pub ksic_code: String,
    /// Business registration number. Treat as sensitive operational data.
    pub business_registration_number: String,
    /// UTC timestamp when the manufacturer record was last updated.
    pub updated_at: DateTime<Utc>,
}
