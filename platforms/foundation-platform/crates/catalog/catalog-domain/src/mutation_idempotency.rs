//! Request identity for keyed Catalog publication mutations.
//!
//! A retry is only answerable with the first outcome if the two requests can be proved identical.
//! That proof is a fingerprint over the request, stored beside the key, and the interesting part is
//! what it must refuse: a key reused with *different* parameters is not a retry, and answering it
//! with the earlier outcome would report a publication the caller never asked for.
//!
//! The encoding is built by hand from an explicit field list rather than derived from `serde`. Two
//! reasons, both about failure modes rather than taste. A derived encoding changes silently when a
//! field is added or reordered, which turns every stored key into a false mismatch at exactly the
//! moment nobody is looking; with an explicit list, adding a field to a command does not compile
//! until someone decides whether it is part of request identity. And `serde_json`'s `Map` is a
//! `BTreeMap` only while the `preserve_order` feature is off — Cargo unifies features across the
//! whole graph, so any crate anywhere enabling it would flip map ordering under a JSON-based
//! fingerprint. Length prefixing sidesteps that class of problem entirely.
//!
//! UUIDs are hashed in their RFC 4122 hyphenated form. That keeps `uuid` out of this crate's
//! dependencies and the spelling is fixed by the standard rather than by a crate version.

use sha2::{Digest as _, Sha256};

/// Version tag stored beside every fingerprint, naming the algorithm that produced it.
///
/// Stored rather than assumed so a future change to the encoding is a new version instead of a
/// silent mismatch against every key already on the ledger. Follows the platform's existing
/// `foundation-platform.bronze_request_fingerprint.v1` convention.
pub const CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION: &str =
    "foundation-platform.catalog_mutation_fingerprint.v1";

/// Domain separation prefix. Distinct from any other digest this platform computes.
const FINGERPRINT_DOMAIN: &str = "foundation-platform.catalog_mutation_fingerprint.v1";

/// The keyed Catalog publication commands the idempotency ledger covers.
///
/// Mirrors `catalog_mutation_idempotency_command_kind_check` exactly, the way
/// [`VectorTileBuildStatus`](crate::VectorTileBuildStatus) mirrors its own status check.
///
/// `record_vector_tile_build_result` is absent on purpose: it carries no idempotency key because its
/// request identity is already the pair `(build_job_id, outcome)`, and a key would be a second
/// spelling of that fact.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogMutationKind {
    /// Activation of a complete dynamic `PostGIS` source for one publication unit.
    MarkTileLayerDynamic,
    /// Recording of a static build attempt against one publication unit.
    StartVectorTileBuild,
    /// Promotion of a validated static build to the active serving source.
    PromoteTileLayerStatic,
    /// Return of one publication unit to its preserved same-revision dynamic release.
    RollbackTileLayerSource,
}

impl CatalogMutationKind {
    /// Database spelling of this command kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarkTileLayerDynamic => "mark_tile_layer_dynamic",
            Self::StartVectorTileBuild => "start_vector_tile_build",
            Self::PromoteTileLayerStatic => "promote_tile_layer_static",
            Self::RollbackTileLayerSource => "rollback_tile_layer_source",
        }
    }

    /// Whether this command answers with a runtime manifest.
    ///
    /// The ledger's outcome column is present for exactly these, which is what
    /// `catalog_mutation_idempotency_manifest_outcome_check` enforces.
    #[must_use]
    pub const fn answers_with_manifest(self) -> bool {
        match self {
            Self::MarkTileLayerDynamic
            | Self::PromoteTileLayerStatic
            | Self::RollbackTileLayerSource => true,
            Self::StartVectorTileBuild => false,
        }
    }

    /// Parses the database spelling.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not one of the four covered commands.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "mark_tile_layer_dynamic" => Ok(Self::MarkTileLayerDynamic),
            "start_vector_tile_build" => Ok(Self::StartVectorTileBuild),
            "promote_tile_layer_static" => Ok(Self::PromoteTileLayerStatic),
            "rollback_tile_layer_source" => Ok(Self::RollbackTileLayerSource),
            other => Err(format!("unknown Catalog mutation kind: {other}")),
        }
    }
}

/// Lowercase hexadecimal SHA-256 of a canonical request encoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestFingerprint(String);

impl RequestFingerprint {
    /// Accepts a stored fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not 64 lowercase hexadecimal characters.
    pub fn new(value: String) -> Result<Self, String> {
        if value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(value))
        } else {
            Err("request fingerprint must be 64 lowercase hexadecimal characters".to_owned())
        }
    }

    /// Returns the stored value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Accumulates a request encoding one named component at a time.
///
/// Every component is length-prefixed with a big-endian `u64`, so no concatenation of components can
/// be read as a different sequence — `("ab", "c")` and `("a", "bc")` produce different digests. Every
/// optional component writes a presence byte, so `None` and `Some("")` cannot collide. Collections
/// write their length before their items, so a shorter list is never a prefix of a longer one.
///
/// Floating point is deliberately unsupported. No covered command carries one today, and stating the
/// rule while it is free is cheaper than discovering that two encoders disagreed about `-0.0`.
pub struct RequestFingerprintBuilder {
    hasher: Sha256,
}

impl RequestFingerprintBuilder {
    /// Starts an encoding for one command kind.
    ///
    /// The kind is inside the digest, not merely beside it. A key reused from one command on another
    /// would otherwise find a row whose fingerprint it could match, and be answered with an outcome
    /// of the wrong type.
    #[must_use]
    pub fn new(command_kind: CatalogMutationKind) -> Self {
        let mut builder = Self {
            hasher: Sha256::new(),
        };
        builder.write(FINGERPRINT_DOMAIN.as_bytes());
        builder.write(command_kind.as_str().as_bytes());
        builder
    }

    fn write(&mut self, bytes: &[u8]) {
        self.hasher.update((bytes.len() as u64).to_be_bytes());
        self.hasher.update(bytes);
    }

    /// Adds a text component.
    #[must_use]
    pub fn text(mut self, value: &str) -> Self {
        self.write(value.as_bytes());
        self
    }

    /// Adds a component rendered through [`std::fmt::Display`], for identifier newtypes.
    #[must_use]
    pub fn displayed(self, value: &impl std::fmt::Display) -> Self {
        let rendered = value.to_string();
        self.text(&rendered)
    }

    /// Adds an optional component rendered through [`std::fmt::Display`].
    #[must_use]
    pub fn optional_displayed(mut self, value: Option<&impl std::fmt::Display>) -> Self {
        match value {
            None => {
                self.hasher.update([0_u8]);
                self
            }
            Some(present) => {
                self.hasher.update([1_u8]);
                self.displayed(present)
            }
        }
    }

    /// Adds an unsigned integer component.
    #[must_use]
    pub fn integer(mut self, value: u64) -> Self {
        self.write(&value.to_be_bytes());
        self
    }

    /// Adds an optional unsigned integer component.
    #[must_use]
    pub fn optional_integer(mut self, value: Option<u64>) -> Self {
        match value {
            None => {
                self.hasher.update([0_u8]);
                self
            }
            Some(present) => {
                self.hasher.update([1_u8]);
                self.integer(present)
            }
        }
    }

    /// Declares how many items of a collection follow.
    ///
    /// Written before the items so a shorter collection can never encode as a prefix of a longer one.
    #[must_use]
    pub fn count(self, items: usize) -> Self {
        self.integer(items as u64)
    }

    /// Finishes the encoding.
    #[must_use]
    pub fn finish(self) -> RequestFingerprint {
        RequestFingerprint(format!("{:x}", self.hasher.finalize()))
    }
}
