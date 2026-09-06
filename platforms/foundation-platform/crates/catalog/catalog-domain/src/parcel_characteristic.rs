//! Per-parcel cadastral characteristics from the `AL_D194` ledger (root ADR-0087).

/// One parcel's newest cadastral characteristic row.
#[derive(Clone, Debug, PartialEq)]
pub struct ParcelCharacteristic {
    /// Cadastral land category name (`지목명`) exactly as the source wrote it.
    pub land_category: Option<String>,
    /// Official cadastral area in square meters. Projection constraints keep this positive.
    pub area_m2: f64,
    /// Land-use situation name exactly as the source wrote it.
    pub land_use_situation: Option<String>,
    /// Terrain height name exactly as the source wrote it.
    pub terrain_height: Option<String>,
    /// Terrain shape name exactly as the source wrote it.
    pub terrain_shape: Option<String>,
    /// Road-contact name exactly as the source wrote it.
    pub road_contact: Option<String>,
    /// The source vintage this value was projected from.
    pub source_snapshot_id: String,
}
