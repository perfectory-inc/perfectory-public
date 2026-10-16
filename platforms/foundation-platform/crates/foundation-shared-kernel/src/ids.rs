//! Strongly typed identifiers shared by foundation-platform bounded contexts.
//!
//! These newtypes prevent accidental interchange between identifiers that share the same UUID
//! representation but belong to different domain concepts.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Wraps a UUID in the strongly typed identifier.
            #[must_use]
            pub const fn new(uuid: Uuid) -> Self {
                Self(uuid)
            }

            /// Returns the underlying UUID value.
            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_newtype!(
    ComplexId,
    "Industrial complex identifier: the one id a complex has everywhere (root ADR-0250).\n\n\
     Derived, never minted: [`ComplexId::for_official_complex_code`] is the same `UUIDv5` the\n\
     Bronze-to-Silver job writes as `complex_id`, so Postgres, tiles, Gold and R2 profiles name a\n\
     complex by the same value."
);

/// Seed prefix of the industrial-complex id derivation.
///
/// The Bronze-to-Silver job (`COMPLEX_ID_SEED_PREFIX`) and the SQL function
/// `catalog.industrial_complex_id_for` spell the same text; all three pin the same vectors.
pub const COMPLEX_ID_SEED_PREFIX: &str = "foundation-platform:catalog:industrial_complex:";

impl ComplexId {
    /// The id of the complex with this official code: `uuid5(NAMESPACE_URL, prefix + code)`.
    #[must_use]
    pub fn for_official_complex_code(official_complex_code: &str) -> Self {
        let seed = format!("{COMPLEX_ID_SEED_PREFIX}{official_complex_code}");
        Self(Uuid::new_v5(&Uuid::NAMESPACE_URL, seed.as_bytes()))
    }
}
id_newtype!(ParcelId, "Catalog parcel identifier.");
id_newtype!(BuildingId, "Catalog building identifier.");
id_newtype!(ManufacturerId, "Catalog manufacturer identifier.");
id_newtype!(SourceRecordId, "Catalog source record identifier.");
id_newtype!(SourceCatalogId, "Catalog Bronze source catalog identifier.");
id_newtype!(IngestionRunId, "Catalog Bronze ingestion run identifier.");
id_newtype!(BronzeObjectId, "Catalog Bronze object metadata identifier.");
id_newtype!(SchemaProfileId, "Catalog Bronze schema profile identifier.");
id_newtype!(FileAssetId, "Catalog file asset identifier.");
id_newtype!(NoticeId, "Catalog notice identifier.");
id_newtype!(BlueprintId, "Catalog blueprint identifier.");
id_newtype!(SpatialLayerId, "Catalog spatial layer identifier.");
id_newtype!(DigitalTwinAssetId, "Catalog digital twin asset identifier.");
id_newtype!(IndustryGroupId, "Catalog industry group identifier.");
id_newtype!(
    IndustryAssignmentId,
    "Catalog parcel industry assignment identifier."
);
id_newtype!(
    VectorTileManifestId,
    "Catalog vector tile manifest identifier."
);
id_newtype!(
    VectorTileArtifactId,
    "Catalog vector tile artifact identifier."
);
id_newtype!(
    VectorTileRuntimeManifestId,
    "Catalog vector tile runtime manifest v2 identifier."
);
id_newtype!(
    VectorTileReleaseId,
    "Catalog immutable vector tile release identifier."
);
id_newtype!(
    VectorTileDataRevisionId,
    "Catalog logical vector tile data revision identifier."
);
id_newtype!(
    PostgisProjectionRevisionId,
    "Catalog complete `PostGIS` serving projection revision identifier."
);
id_newtype!(
    VectorTileBuildJobId,
    "Catalog static vector tile build job identifier."
);
id_newtype!(
    LakehouseStorageNamespaceId,
    "Lakehouse Registry storage namespace identifier."
);
id_newtype!(
    LakehouseDataAssetId,
    "Lakehouse Registry data asset identifier."
);
id_newtype!(
    LakehouseDatasetVersionId,
    "Lakehouse Registry dataset version identifier."
);
id_newtype!(
    LakehouseObjectArtifactId,
    "Lakehouse Registry object artifact identifier."
);
id_newtype!(
    PrincipalId,
    "Opaque Foundation-local identifier for an authorized principal.\n\n\
     A Catalog entity identifier cannot be used where a principal is required.\n\n\
     ```compile_fail,E0308\n\
     use foundation_shared_kernel::{ComplexId, PrincipalId};\n\
     use uuid::Uuid;\n\n\
     fn requires_principal(_: PrincipalId) {}\n\n\
     let complex_id = ComplexId::new(Uuid::nil());\n\
     requires_principal(complex_id);\n\
     ```"
);
id_newtype!(StaffId, "Legacy-named Foundation audit staff identifier.");

#[cfg(test)]
mod tests {
    use super::ComplexId;

    /// The vectors `test_industrial_complex_bronze_to_silver.py` pins (`RFC4122_VECTORS`) and the
    /// `catalog.industrial_complex_id_for` migration test checks: one derivation, three languages.
    #[test]
    fn complex_id_is_the_lakehouse_derivation_of_the_official_code() {
        for (code, expected) in [
            ("000000", "b796bcb0-f180-5220-8007-4cf27bf4c87b"),
            ("000001", "20c71d10-9b9a-5e7a-95fc-0ffa834b3867"),
            ("111010", "7df3859c-768f-51fa-a78d-6398acd5f052"),
        ] {
            assert_eq!(
                ComplexId::for_official_complex_code(code).to_string(),
                expected,
                "{code}"
            );
        }
    }
}
