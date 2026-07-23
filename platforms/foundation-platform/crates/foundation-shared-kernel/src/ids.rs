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

id_newtype!(ComplexId, "Catalog industrial complex identifier.");
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
