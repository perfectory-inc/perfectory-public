//! The by-PNU serving manifest and the tombstone document (root ADR-0141 §2, §4).
//!
//! The serving state of a lane is one base generation plus a list of patch generations, newest
//! first. The manifest is the lane's one mutable object; the gateway Workers read the same
//! shape (`services/foundation-{parcel,building}-gateway/src/index.ts`).
//!
//! A v1 manifest (one generation, no patches) is read as a v2 manifest with an empty patch list,
//! so the first patch publish after the change can start from what the bucket already serves.
//!
//! Reading and writing check different things. The contract's `max_patches` and
//! `pnu_prefix_length` bind only the manifest being written; a manifest being read is held to the
//! fixed `manifest_patch_ceiling` and to the prefix length it declares itself. Lowering either
//! value therefore leaves the live manifest readable, and the full bake that compacts it can
//! still replace it.

use anyhow::{bail, ensure, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::by_pnu_gateway_contract::{by_pnu_serving_patch_policy, ByPnuLane, PNU_PREFIX_LENGTHS};
use crate::by_pnu_section_pack_manifest::SectionPacksState;

/// The manifest every publish writes.
///
/// Readers tolerate fields they do not know: a release rolled back to an older publisher must
/// still read the manifest a newer one wrote. Writers stay strict, because a manifest is only ever
/// serialized from this struct, which has no place for an unknown field.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ServingManifest {
    pub(crate) schema_version: u32,
    pub(crate) unit: String,
    pub(crate) base_generation: u64,
    /// Objects of the base generation; the denominator of the cumulative change ratio.
    pub(crate) base_object_count: u64,
    /// The document schema the base generation was baked with. A patch keeps it; a different
    /// one needs a new base (root ADR-0141 §5).
    pub(crate) document_schema_version: String,
    pub(crate) gold_table: String,
    /// The Gold snapshot the base generation was baked from.
    pub(crate) gold_iceberg_snapshot_id: String,
    /// The latest Gold snapshot whose changes the served state fully reflects: the next change
    /// set is computed against it. A change set with nothing in it advances only this.
    pub(crate) reflected_gold_iceberg_snapshot_id: String,
    /// The length of every prefix in `patches`; the Worker slices the requested PNU by it.
    pub(crate) pnu_prefix_length: usize,
    /// Newest first. Every entry holds objects.
    pub(crate) patches: Vec<PatchEntry>,
    /// PNUs that answer: base + new − deleted over every patch.
    pub(crate) object_count: u64,
    pub(crate) published_at_utc: String,
    /// The verified re-base whose result this manifest published (root ADR-0146 §1). Only that
    /// manifest carries it; the next publish moves it into the manifest history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) verified_rebase: Option<VerifiedRebase>,
    /// The Iceberg tag that pins `reflected_gold_iceberg_snapshot_id` for this manifest (root
    /// ADR-0146 §2). A publish releases only the lane's tags older than the one the live manifest
    /// names. Absent on a manifest written before pinning, or on a rollback whose snapshot had
    /// already expired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reflected_gold_snapshot_tag: Option<String>,
    /// The section packs the lane serves from (root ADR-0147); absent while it serves objects.
    /// The v2 fields above keep describing the object lane, so a v2-only reader still serves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) section_packs: Option<SectionPacksState>,
}

/// What a verified re-base compared and found (`verify-parcel-by-pnu-serving-rebase`).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct VerifiedRebase {
    pub(crate) run_id: String,
    pub(crate) reason: String,
    pub(crate) method: String,
    /// The reflected snapshot the change set could not be computed against.
    pub(crate) baseline_gold_iceberg_snapshot_id: String,
    pub(crate) served_objects_read: u64,
    pub(crate) equal: u64,
    pub(crate) changed: u64,
    pub(crate) only_served: u64,
    pub(crate) only_gold: u64,
}

/// One patch generation of the base.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PatchEntry {
    pub(crate) generation: u64,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) upserted: u64,
    pub(crate) deleted: u64,
    /// The distinct PNU prefixes (the manifest's `pnu_prefix_length`) the patch holds objects
    /// under; the Worker skips a patch whose list lacks the requested PNU's prefix.
    pub(crate) prefixes: Vec<String>,
}

/// What the publish found in the bucket: the manifest, its exact bytes, and the version the
/// replacing write must still find there.
#[derive(Clone, Debug)]
pub(crate) struct StoredManifest {
    pub(crate) manifest: ServedManifest,
    pub(crate) bytes: Vec<u8>,
    pub(crate) version: String,
}

/// A manifest as read, v1 or v2. A v1 manifest never recorded its document schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServedManifest {
    pub(crate) wire_schema_version: u32,
    pub(crate) unit: String,
    pub(crate) base_generation: u64,
    pub(crate) base_object_count: u64,
    pub(crate) document_schema_version: Option<String>,
    pub(crate) gold_table: String,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) reflected_gold_iceberg_snapshot_id: String,
    /// `None` for a v1 manifest, which holds no patches.
    pub(crate) pnu_prefix_length: Option<usize>,
    pub(crate) patches: Vec<PatchEntry>,
    pub(crate) object_count: u64,
    pub(crate) published_at_utc: String,
    /// The tag pinning the reflected snapshot; `None` for a v1 manifest or an unpinned one.
    pub(crate) reflected_gold_snapshot_tag: Option<String>,
    /// The section packs block, when the lane serves from packs.
    pub(crate) section_packs: Option<SectionPacksState>,
}

#[derive(Deserialize)]
struct ManifestV1 {
    schema_version: u32,
    unit: String,
    current_generation: u64,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    object_count: u64,
    published_at_utc: String,
}

impl ServedManifest {
    /// Parses a stored manifest of `lane`, v1 or v2.
    ///
    /// # Errors
    /// Refuses another lane's manifest, an unknown schema, and a v2 manifest that breaks
    /// [`ServingManifest::check_readable`]. The contract's tunable bounds are not applied here.
    pub(crate) fn parse(lane: ByPnuLane, bytes: &[u8]) -> anyhow::Result<Self> {
        let raw: JsonValue =
            serde_json::from_slice(bytes).context("the serving manifest is not JSON")?;
        let version = raw
            .get("schema_version")
            .and_then(JsonValue::as_u64)
            .context("the serving manifest has no schema_version")?;
        let manifest = match version {
            1 => {
                let v1: ManifestV1 = serde_json::from_value(raw)
                    .context("the v1 serving manifest does not parse")?;
                Self {
                    wire_schema_version: v1.schema_version,
                    unit: v1.unit,
                    base_generation: v1.current_generation,
                    base_object_count: v1.object_count,
                    document_schema_version: None,
                    gold_table: v1.gold_table,
                    reflected_gold_iceberg_snapshot_id: v1.gold_iceberg_snapshot_id.clone(),
                    gold_iceberg_snapshot_id: v1.gold_iceberg_snapshot_id,
                    pnu_prefix_length: None,
                    patches: Vec::new(),
                    object_count: v1.object_count,
                    published_at_utc: v1.published_at_utc,
                    reflected_gold_snapshot_tag: None,
                    section_packs: None,
                }
            }
            2 => {
                let v2: ServingManifest = serde_json::from_value(raw)
                    .context("the v2 serving manifest does not parse")?;
                v2.check_readable()?;
                Self {
                    wire_schema_version: v2.schema_version,
                    unit: v2.unit,
                    base_generation: v2.base_generation,
                    base_object_count: v2.base_object_count,
                    document_schema_version: Some(v2.document_schema_version),
                    gold_table: v2.gold_table,
                    gold_iceberg_snapshot_id: v2.gold_iceberg_snapshot_id,
                    reflected_gold_iceberg_snapshot_id: v2.reflected_gold_iceberg_snapshot_id,
                    pnu_prefix_length: Some(v2.pnu_prefix_length),
                    patches: v2.patches,
                    object_count: v2.object_count,
                    published_at_utc: v2.published_at_utc,
                    reflected_gold_snapshot_tag: v2.reflected_gold_snapshot_tag,
                    section_packs: v2.section_packs,
                }
            }
            other => {
                bail!("serving manifest schema_version {other} is not one this publisher reads")
            }
        };
        ensure!(
            manifest.unit == lane.unit(),
            "the serving manifest is a {} manifest, not {}",
            manifest.unit,
            lane.unit()
        );
        ensure!(
            manifest.base_generation >= 1,
            "the serving manifest names generation 0"
        );
        Ok(manifest)
    }

    /// The highest patch generation the manifest names, 0 when it names none.
    pub(crate) fn newest_patch(&self) -> u64 {
        self.patches.first().map_or(0, |patch| patch.generation)
    }

    /// Changes the patches carry together: what the next bake weighs against the base.
    pub(crate) fn cumulative_changes(&self) -> u64 {
        self.patches
            .iter()
            .map(|patch| patch.upserted + patch.deleted)
            .sum()
    }
}

impl ServingManifest {
    /// Checks the invariants every reader relies on — and nothing a contract change can move.
    ///
    /// # Errors
    /// Refuses a wrong schema version, more patches than the fixed `manifest_patch_ceiling`, a
    /// prefix length no PNU has, patches not strictly newest first, an empty patch, and a prefix
    /// list that is not sorted distinct digits of the declared length.
    pub(crate) fn check_readable(&self) -> anyhow::Result<()> {
        let policy = by_pnu_serving_patch_policy()?;
        ensure!(
            self.schema_version == policy.manifest_schema_version,
            "a v2 serving manifest must say schema_version {}",
            policy.manifest_schema_version
        );
        ensure!(
            self.patches.len() <= policy.manifest_patch_ceiling,
            "{} patches exceed the contract's manifest_patch_ceiling {}",
            self.patches.len(),
            policy.manifest_patch_ceiling
        );
        ensure!(
            PNU_PREFIX_LENGTHS.contains(&self.pnu_prefix_length),
            "pnu_prefix_length {} is not a PNU prefix length",
            self.pnu_prefix_length
        );
        for pair in self.patches.windows(2) {
            ensure!(
                pair[0].generation > pair[1].generation,
                "patches must be listed newest first with distinct generations"
            );
        }
        if let Some(packs) = &self.section_packs {
            packs.check_readable()?;
        }
        for patch in &self.patches {
            ensure!(patch.generation >= 1, "patch generation 0 does not exist");
            ensure!(
                patch.upserted + patch.deleted > 0 && !patch.prefixes.is_empty(),
                "patch {} holds no objects; an empty change set advances \
                 reflected_gold_iceberg_snapshot_id instead",
                patch.generation
            );
            ensure!(
                patch.prefixes.windows(2).all(|pair| pair[0] < pair[1])
                    && patch.prefixes.iter().all(|prefix| {
                        prefix.len() == self.pnu_prefix_length
                            && prefix.bytes().all(|byte| byte.is_ascii_digit())
                    }),
                "patch {} prefixes must be sorted distinct {}-digit PNU prefixes",
                patch.generation,
                self.pnu_prefix_length
            );
        }
        Ok(())
    }

    /// Checks a manifest about to be written: readable, and within the contract as it is now.
    ///
    /// # Errors
    /// Refuses what [`Self::check_readable`] refuses, more patches than the contract's
    /// `max_patches`, and a prefix length other than the contract's `pnu_prefix_length`.
    pub(crate) fn check_writable(&self) -> anyhow::Result<()> {
        self.check_readable()?;
        let policy = by_pnu_serving_patch_policy()?;
        ensure!(
            self.patches.len() <= policy.max_patches,
            "{} patches exceed the contract's max_patches {}; a full bake compacts them",
            self.patches.len(),
            policy.max_patches
        );
        ensure!(
            self.pnu_prefix_length == policy.pnu_prefix_length,
            "the patches are listed by {}-digit prefixes but the contract's pnu_prefix_length is \
             {}; a full bake compacts them",
            self.pnu_prefix_length,
            policy.pnu_prefix_length
        );
        if let Some(packs) = &self.section_packs {
            let lane = ByPnuLane::from_unit(&self.unit)
                .with_context(|| format!("{} is not a by-PNU lane", self.unit))?;
            packs.check_writable(lane)?;
        }
        Ok(())
    }

    /// The v2 bytes the store writes, newline-terminated.
    ///
    /// # Errors
    /// Refuses a manifest that breaks [`Self::check_writable`].
    pub(crate) fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        self.check_writable()?;
        let mut body =
            serde_json::to_vec_pretty(self).context("failed to serialize the serving manifest")?;
        body.push(b'\n');
        Ok(body)
    }
}

/// The distinct, sorted contract-length prefixes of `pnus`.
///
/// # Errors
/// Returns an error when the contract cannot be read.
pub(crate) fn pnu_prefixes<'a>(pnus: impl Iterator<Item = &'a str>) -> anyhow::Result<Vec<String>> {
    let length = by_pnu_serving_patch_policy()?.pnu_prefix_length;
    let mut prefixes = pnus
        .filter_map(|pnu| pnu.get(..length))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    prefixes.sort_unstable();
    prefixes.dedup();
    Ok(prefixes)
}

/// The tombstone written for a PNU gone from Gold: the Worker answers 404 on it without looking
/// further down (root ADR-0141 §2).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tombstone {
    pub(crate) schema_version: String,
    pub(crate) pnu: String,
    pub(crate) deleted: bool,
    pub(crate) source: TombstoneSource,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TombstoneSource {
    pub(crate) table: String,
    pub(crate) iceberg_snapshot_id: String,
}

/// The exact bytes and checksum of `pnu`'s tombstone in a patch of `gold_table` snapshot
/// `snapshot`.
///
/// # Errors
/// Refuses a body larger than the contract's `tombstone_max_bytes`: the Worker reads only objects
/// at most that large as possible tombstones.
pub(crate) fn tombstone_body(
    pnu: &str,
    gold_table: &str,
    snapshot: &str,
) -> anyhow::Result<(Vec<u8>, String)> {
    use sha2::{Digest, Sha256};

    let policy = by_pnu_serving_patch_policy()?;
    let mut body = serde_json::to_vec(&Tombstone {
        schema_version: policy.tombstone_schema_version.clone(),
        pnu: pnu.to_owned(),
        deleted: true,
        source: TombstoneSource {
            table: gold_table.to_owned(),
            iceberg_snapshot_id: snapshot.to_owned(),
        },
    })
    .context("failed to serialize a tombstone")?;
    body.push(b'\n');
    ensure!(
        body.len() <= policy.tombstone_max_bytes,
        "the tombstone of {pnu} is {} bytes, over the contract's tombstone_max_bytes {}",
        body.len(),
        policy.tombstone_max_bytes
    );
    let checksum = format!("{:x}", Sha256::digest(&body));
    Ok((body, checksum))
}

/// Whether a stored object is a tombstone, and of which PNU and snapshot.
pub(crate) fn read_tombstone(bytes: &[u8]) -> Option<Tombstone> {
    let policy = by_pnu_serving_patch_policy().ok()?;
    if bytes.len() > policy.tombstone_max_bytes {
        return None;
    }
    serde_json::from_slice::<Tombstone>(bytes)
        .ok()
        .filter(|tombstone| {
            tombstone.deleted && tombstone.schema_version == policy.tombstone_schema_version
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAPSHOT: &str = "999990000000000001";

    fn v2(patches: Vec<PatchEntry>) -> ServingManifest {
        ServingManifest {
            schema_version: 2,
            unit: "parcel-by-pnu".to_owned(),
            base_generation: 3,
            base_object_count: 10,
            document_schema_version: "doc.v1".to_owned(),
            gold_table: "gold.parcel_panel".to_owned(),
            gold_iceberg_snapshot_id: SNAPSHOT.to_owned(),
            reflected_gold_iceberg_snapshot_id: SNAPSHOT.to_owned(),
            pnu_prefix_length: 5,
            patches,
            object_count: 10,
            published_at_utc: "2026-01-01T00:00:00Z".to_owned(),
            verified_rebase: None,
            reflected_gold_snapshot_tag: None,
            section_packs: None,
        }
    }

    fn patch(generation: u64, prefixes: &[&str]) -> PatchEntry {
        PatchEntry {
            generation,
            gold_iceberg_snapshot_id: SNAPSHOT.to_owned(),
            upserted: 1,
            deleted: 0,
            prefixes: prefixes.iter().map(|p| (*p).to_owned()).collect(),
        }
    }

    #[test]
    fn a_v1_manifest_reads_as_a_base_with_no_patches() -> anyhow::Result<()> {
        let v1 = format!(
            "{{\"schema_version\":1,\"unit\":\"building-by-pnu\",\"current_generation\":4,\
             \"gold_table\":\"gold.building_panel\",\"gold_iceberg_snapshot_id\":\"{SNAPSHOT}\",\
             \"object_count\":7,\"published_at_utc\":\"2026-01-01T00:00:00Z\"}}"
        );
        let read = ServedManifest::parse(ByPnuLane::Building, v1.as_bytes())?;
        assert_eq!(read.base_generation, 4);
        assert!(read.patches.is_empty());
        assert_eq!(read.reflected_gold_iceberg_snapshot_id, SNAPSHOT);
        assert_eq!(read.base_object_count, 7);
        assert_eq!(read.document_schema_version, None);
        assert!(ServedManifest::parse(ByPnuLane::Parcel, v1.as_bytes()).is_err());
        Ok(())
    }

    #[test]
    fn a_v2_manifest_round_trips() -> anyhow::Result<()> {
        let manifest = v2(vec![patch(2, &["99999"]), patch(1, &["99999"])]);
        let read = ServedManifest::parse(ByPnuLane::Parcel, &manifest.to_bytes()?)?;
        assert_eq!(read.patches, manifest.patches);
        assert_eq!(read.newest_patch(), 2);
        assert_eq!(read.cumulative_changes(), 2);
        let mut pinned = manifest;
        pinned.reflected_gold_snapshot_tag = Some("served-parcel-by-pnu-tag".to_owned());
        let read = ServedManifest::parse(ByPnuLane::Parcel, &pinned.to_bytes()?)?;
        assert_eq!(
            read.reflected_gold_snapshot_tag.as_deref(),
            Some("served-parcel-by-pnu-tag")
        );
        Ok(())
    }

    /// A release rolled back to an older publisher still reads what a newer one wrote: fields it
    /// does not know, at the top and inside a patch, are not a reason to refuse the live manifest.
    #[test]
    fn a_manifest_with_fields_this_reader_does_not_know_is_read() -> anyhow::Result<()> {
        let mut raw = serde_json::to_value(v2(vec![patch(1, &["99999"])]))?;
        raw["a_field_from_a_newer_publisher"] = serde_json::json!({"any": "shape"});
        raw["patches"][0]["another_new_field"] = serde_json::json!(1);
        let read = ServedManifest::parse(ByPnuLane::Parcel, &serde_json::to_vec(&raw)?)?;
        assert_eq!(read.newest_patch(), 1);
        // What this publisher writes holds only the fields it knows.
        let rewritten: JsonValue = serde_json::from_slice(&v2(Vec::new()).to_bytes()?)?;
        assert!(rewritten.get("a_field_from_a_newer_publisher").is_none());
        Ok(())
    }

    #[test]
    fn a_patch_list_that_breaks_its_invariants_is_refused() {
        for (label, patches) in [
            (
                "oldest first",
                vec![patch(1, &["99999"]), patch(2, &["99999"])],
            ),
            ("repeated", vec![patch(2, &["99999"]), patch(2, &["99999"])]),
            ("repeated prefixes", vec![patch(1, &["99999", "99999"])]),
            ("short prefix", vec![patch(1, &["9999"])]),
            ("no prefixes", vec![patch(1, &[])]),
        ] {
            let manifest = v2(patches);
            assert!(manifest.to_bytes().is_err(), "{label} was written");
            assert!(manifest.check_readable().is_err(), "{label} was read");
        }
        let mut empty = patch(1, &["99999"]);
        empty.upserted = 0;
        assert!(
            v2(vec![empty]).to_bytes().is_err(),
            "an empty patch was accepted"
        );
    }

    /// Lowering `max_patches` or changing `pnu_prefix_length` must not make the live manifest
    /// unreadable: only a manifest being written is held to them.
    #[test]
    fn the_tunable_bounds_bind_writes_and_only_the_ceiling_binds_reads() -> anyhow::Result<()> {
        let policy = by_pnu_serving_patch_policy()?;
        let prefix = "9".repeat(policy.pnu_prefix_length);
        let patches = |count: usize| -> anyhow::Result<Vec<PatchEntry>> {
            Ok((1..=u64::try_from(count)?)
                .rev()
                .map(|g| patch(g, &[prefix.as_str()]))
                .collect())
        };
        let over_k = v2(patches(policy.max_patches + 1)?);
        assert!(
            over_k.to_bytes().is_err(),
            "more than max_patches was written"
        );
        let read = ServedManifest::parse(ByPnuLane::Parcel, &serde_json::to_vec(&over_k)?)?;
        assert_eq!(read.patches.len(), policy.max_patches + 1);

        let other = if policy.pnu_prefix_length == 1 {
            2
        } else {
            policy.pnu_prefix_length - 1
        };
        let mut relisted = v2(vec![patch(1, &["9".repeat(other).as_str()])]);
        relisted.pnu_prefix_length = other;
        assert!(
            relisted.to_bytes().is_err(),
            "a prefix length other than the contract's was written"
        );
        let read = ServedManifest::parse(ByPnuLane::Parcel, &serde_json::to_vec(&relisted)?)?;
        assert_eq!(read.pnu_prefix_length, Some(other));

        let over_ceiling = v2(patches(policy.manifest_patch_ceiling + 1)?);
        assert!(
            ServedManifest::parse(ByPnuLane::Parcel, &serde_json::to_vec(&over_ceiling)?).is_err(),
            "more than manifest_patch_ceiling was read"
        );
        let mut impossible = v2(Vec::new());
        impossible.pnu_prefix_length = 20;
        assert!(
            impossible.check_readable().is_err(),
            "a 20-digit prefix was read"
        );
        Ok(())
    }

    /// The section packs block rides in the v2 envelope (root ADR-0147): a reader that knows it
    /// reads it, the v2 fields stay as they were, and a block a reader cannot trust refuses the
    /// whole manifest instead of being dropped.
    #[test]
    fn a_v2_manifest_carrying_section_packs_is_read_with_its_block() -> anyhow::Result<()> {
        let mut manifest = v2(vec![patch(1, &["99999"])]);
        manifest.unit = "building-by-pnu".to_owned();
        manifest.section_packs = Some(crate::by_pnu_section_pack_manifest::tests::state(4)?);
        let bytes = manifest.to_bytes()?;
        let read = ServedManifest::parse(ByPnuLane::Building, &bytes)?;
        assert_eq!(read.wire_schema_version, 2);
        assert_eq!(read.base_generation, 3);
        assert_eq!(read.section_packs, manifest.section_packs);

        // What a reader that predates the block sees: the same v2 manifest, block ignored.
        let raw: JsonValue = serde_json::from_slice(&bytes)?;
        assert_eq!(raw["schema_version"], 2);
        let mut without = raw.clone();
        without
            .as_object_mut()
            .context("manifest is an object")?
            .remove("section_packs");
        let old = ServedManifest::parse(ByPnuLane::Building, &serde_json::to_vec(&without)?)?;
        assert_eq!(
            (old.base_generation, old.patches),
            (read.base_generation, read.patches)
        );

        let mut broken = raw;
        broken["section_packs"]["schema_version"] = serde_json::json!(99);
        assert!(
            ServedManifest::parse(ByPnuLane::Building, &serde_json::to_vec(&broken)?).is_err(),
            "an unreadable section_packs block was ignored"
        );
        Ok(())
    }

    #[test]
    fn a_tombstone_is_small_and_reads_back() -> anyhow::Result<()> {
        let (body, _) = tombstone_body("9999900000100000000", "gold.parcel_panel", SNAPSHOT)?;
        let read = read_tombstone(&body).context("a tombstone did not read back")?;
        assert_eq!(read.pnu, "9999900000100000000");
        assert_eq!(read.source.iceberg_snapshot_id, SNAPSHOT);
        assert!(read_tombstone(b"{\"pnu\":\"9999900000100000000\"}").is_none());
        Ok(())
    }

    #[test]
    fn prefixes_are_sorted_and_distinct() -> anyhow::Result<()> {
        let prefixes = pnu_prefixes(
            [
                "9999900000100000000",
                "9999900000200000000",
                "9999900000100000003",
            ]
            .into_iter(),
        )?;
        assert_eq!(prefixes, vec!["99999".to_owned()]);
        Ok(())
    }
}
