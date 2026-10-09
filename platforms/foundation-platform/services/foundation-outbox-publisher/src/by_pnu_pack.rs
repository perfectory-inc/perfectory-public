//! The by-PNU section pack: one section of every document of one legal dong (root ADR-0147 §2).
//!
//! A pack is one R2 object, written once. One range read of its head (prefix, header, index)
//! answers which PNUs it holds; one more range read returns one document, gzip-compressed by
//! itself so it can be decoded alone. The writer and the reader are both here so they share
//! every constant; the gateway Worker's reader (`services/foundation-by-pnu-gateway`) is held
//! to the same bytes by the golden pack the tests below pin and the Worker's tests read.
//!
//! ```text
//! offset 0   magic             8 bytes   the contract's `magic`
//!        8   format_version    u16 LE    the contract's `format_version`
//!       10   reserved          u16 LE    0
//!       12   header_length     u32 LE    bytes of the header JSON
//!       16   index_length      u32 LE    bytes of the index = entry_count x INDEX_ENTRY_BYTES
//!       20   header            JSON      [`PackHeader`], compact, UTF-8
//!            index             entries   sorted by PNU, distinct
//!            body              bytes     the documents' gzip members back to back
//! index entry: pnu (19 ASCII digits) | offset u32 LE (from the body start) | length u32 LE
//!              | state u8 (1 document, 2 tombstone; a tombstone has length 0)
//! ```
//!
//! Gzip is the one compression the Worker can both decode natively (`DecompressionStream`) and
//! hand to a client unchanged as `Content-Encoding: gzip`. Every member is written with mtime 0
//! and no file name, so the same document always compresses to the same bytes under one pinned
//! `flate2`; the golden test notices any change.

use std::io::{Read as _, Write as _};

use anyhow::{bail, ensure, Context};
use flate2::{read::GzDecoder, Compression, GzBuilder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::by_pnu_gateway_contract::section_pack_policy;

/// Bytes before the header: magic, format version, reserved, header length, index length.
pub(crate) const PREFIX_BYTES: usize = crate::by_pnu_gateway_contract::PACK_PREFIX_BYTES;
/// Bytes of one index entry.
pub(crate) const INDEX_ENTRY_BYTES: usize = 28;
const PNU_BYTES: usize = 19;
const STATE_DOCUMENT: u8 = 1;
const STATE_TOMBSTONE: u8 = 2;
/// The only compression a pack names.
pub(crate) const COMPRESSION: &str = "gzip";

/// What a pack says about itself; the first thing after the fixed prefix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PackHeader {
    pub(crate) format_version: u16,
    /// The manifest unit of the lane (`building-by-pnu`).
    pub(crate) lane: String,
    pub(crate) section: String,
    pub(crate) generation: u64,
    /// The patch this pack belongs to; `None` for a base pack.
    pub(crate) patch: Option<u64>,
    /// The legal dong (PNU prefix of the contract's `unit_prefix_length`) every entry is under.
    pub(crate) unit: String,
    pub(crate) gold_table: String,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) compression: String,
    pub(crate) entry_count: u64,
    pub(crate) document_count: u64,
    pub(crate) tombstone_count: u64,
    pub(crate) index_length: u64,
    pub(crate) body_length: u64,
    pub(crate) body_sha256: String,
}

/// What the writer is told; the counts, lengths and checksum are the writer's to fill in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackIdentity {
    pub(crate) lane: String,
    pub(crate) section: String,
    pub(crate) generation: u64,
    pub(crate) patch: Option<u64>,
    pub(crate) unit: String,
    pub(crate) gold_table: String,
    pub(crate) gold_iceberg_snapshot_id: String,
}

/// One PNU's entry: a document, or a tombstone (the PNU is gone as of this patch).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EntryState {
    Document,
    Tombstone,
}

/// One index entry as read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexEntry {
    pub(crate) pnu: String,
    pub(crate) offset: u32,
    pub(crate) length: u32,
    pub(crate) state: EntryState,
}

/// Collects one pack's entries in PNU order and lays out its bytes.
pub(crate) struct PackWriter {
    identity: PackIdentity,
    entries: Vec<IndexEntry>,
    body: Vec<u8>,
}

impl PackWriter {
    /// A writer for one pack.
    ///
    /// # Errors
    /// Refuses a unit that is not the contract's dong length of digits.
    pub(crate) fn new(identity: PackIdentity) -> anyhow::Result<Self> {
        // A legal dong, or one part of one (root ADR-0163): the contract's unit grammar.
        crate::r2_layout::by_pnu_packs::check_unit(&identity.unit)?;
        ensure!(
            identity.patch.is_none_or(|patch| patch >= 1) && identity.generation >= 1,
            "pack generation and patch start at 1"
        );
        Ok(Self {
            identity,
            entries: Vec::new(),
            body: Vec::new(),
        })
    }

    /// Appends `pnu`'s document, compressed alone.
    ///
    /// # Errors
    /// Refuses a PNU out of order or outside the unit, and a body past 4 GiB.
    pub(crate) fn push_document(&mut self, pnu: &str, document: &[u8]) -> anyhow::Result<()> {
        self.check_next(pnu)?;
        let compressed = gzip(document)?;
        let offset = u32::try_from(self.body.len()).context("pack body passed 4 GiB")?;
        let length = u32::try_from(compressed.len()).context("one document passed 4 GiB")?;
        u32::try_from(self.body.len() + compressed.len()).context("pack body passed 4 GiB")?;
        self.body.extend_from_slice(&compressed);
        self.entries.push(IndexEntry {
            pnu: pnu.to_owned(),
            offset,
            length,
            state: EntryState::Document,
        });
        Ok(())
    }

    /// Appends a tombstone for `pnu`: a patch's record that the PNU no longer answers.
    ///
    /// # Errors
    /// Refuses a PNU out of order or outside the unit, and a tombstone in a base pack.
    pub(crate) fn push_tombstone(&mut self, pnu: &str) -> anyhow::Result<()> {
        ensure!(
            self.identity.patch.is_some(),
            "a base pack holds documents only; a tombstone belongs in a patch"
        );
        self.check_next(pnu)?;
        self.entries.push(IndexEntry {
            pnu: pnu.to_owned(),
            offset: u32::try_from(self.body.len()).context("pack body passed 4 GiB")?,
            length: 0,
            state: EntryState::Tombstone,
        });
        Ok(())
    }

    fn check_next(&self, pnu: &str) -> anyhow::Result<()> {
        ensure!(
            pnu.len() == PNU_BYTES
                && is_digits(pnu)
                && pnu.starts_with(crate::r2_layout::by_pnu_packs::dong_of(&self.identity.unit)),
            "{pnu:?} is not a 19-digit PNU of legal dong {}",
            self.identity.unit
        );
        if let Some(last) = self.entries.last() {
            ensure!(
                last.pnu.as_str() < pnu,
                "pack entries must be in strictly ascending PNU order: {pnu} after {}",
                last.pnu
            );
        }
        Ok(())
    }

    /// Lays out the pack.
    ///
    /// # Errors
    /// Refuses an empty pack and one with more entries than the contract allows.
    pub(crate) fn finish(self) -> anyhow::Result<Vec<u8>> {
        let policy = section_pack_policy()?;
        ensure!(!self.entries.is_empty(), "a pack holds at least one entry");
        ensure!(
            self.entries.len() <= policy.max_index_entries,
            "{} entries exceed the contract's max_index_entries {}",
            self.entries.len(),
            policy.max_index_entries
        );
        let mut index = Vec::with_capacity(self.entries.len() * INDEX_ENTRY_BYTES);
        for entry in &self.entries {
            index.extend_from_slice(entry.pnu.as_bytes());
            index.extend_from_slice(&entry.offset.to_le_bytes());
            index.extend_from_slice(&entry.length.to_le_bytes());
            index.push(match entry.state {
                EntryState::Document => STATE_DOCUMENT,
                EntryState::Tombstone => STATE_TOMBSTONE,
            });
        }
        let tombstone_count = self
            .entries
            .iter()
            .filter(|entry| entry.state == EntryState::Tombstone)
            .count();
        let header = PackHeader {
            format_version: policy.format_version,
            lane: self.identity.lane,
            section: self.identity.section,
            generation: self.identity.generation,
            patch: self.identity.patch,
            unit: self.identity.unit,
            gold_table: self.identity.gold_table,
            gold_iceberg_snapshot_id: self.identity.gold_iceberg_snapshot_id,
            compression: COMPRESSION.to_owned(),
            entry_count: u64::try_from(self.entries.len())?,
            document_count: u64::try_from(self.entries.len() - tombstone_count)?,
            tombstone_count: u64::try_from(tombstone_count)?,
            index_length: u64::try_from(index.len())?,
            body_length: u64::try_from(self.body.len())?,
            body_sha256: format!("{:x}", Sha256::digest(&self.body)),
        };
        let header = serde_json::to_vec(&header).context("failed to serialize a pack header")?;
        let mut pack =
            Vec::with_capacity(PREFIX_BYTES + header.len() + index.len() + self.body.len());
        pack.extend_from_slice(magic(policy)?);
        pack.extend_from_slice(&policy.format_version.to_le_bytes());
        pack.extend_from_slice(&0_u16.to_le_bytes());
        pack.extend_from_slice(&u32::try_from(header.len())?.to_le_bytes());
        pack.extend_from_slice(
            &u32::try_from(index.len())
                .context("pack index passed 4 GiB")?
                .to_le_bytes(),
        );
        pack.extend_from_slice(&header);
        pack.extend_from_slice(&index);
        pack.extend_from_slice(&self.body);
        Ok(pack)
    }
}

/// A whole pack, read and checked.
#[derive(Clone, Debug)]
pub(crate) struct Pack {
    pub(crate) header: PackHeader,
    pub(crate) entries: Vec<IndexEntry>,
    body: Vec<u8>,
}

/// The lengths the fixed prefix declares.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PackPrefix {
    pub(crate) header_length: usize,
    pub(crate) index_length: usize,
}

impl PackPrefix {
    /// Bytes of the head: prefix, header and index — what one range read must cover to search.
    pub(crate) const fn head_length(self) -> usize {
        PREFIX_BYTES + self.header_length + self.index_length
    }
}

/// Reads the fixed prefix.
///
/// # Errors
/// Refuses another magic, another format version, and a set reserved field.
pub(crate) fn read_prefix(bytes: &[u8]) -> anyhow::Result<PackPrefix> {
    let policy = section_pack_policy()?;
    ensure!(
        bytes.len() >= PREFIX_BYTES,
        "a pack is shorter than its fixed prefix"
    );
    ensure!(
        &bytes[..8] == magic(policy)?,
        "not a section pack: wrong magic"
    );
    let version = u16::from_le_bytes([bytes[8], bytes[9]]);
    ensure!(
        version == policy.format_version,
        "pack format version {version} is not the contract's {}",
        policy.format_version
    );
    ensure!(
        bytes[10] == 0 && bytes[11] == 0,
        "the pack's reserved field is set"
    );
    let header_length = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let index_length = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    let index_length = usize::try_from(index_length)?;
    ensure!(
        index_length % INDEX_ENTRY_BYTES == 0,
        "pack index length {index_length} is not a whole number of entries"
    );
    Ok(PackPrefix {
        header_length: usize::try_from(header_length)?,
        index_length,
    })
}

/// Reads the head: the header and every index entry. `bytes` may run past the head.
///
/// # Errors
/// Refuses what [`read_prefix`] refuses, a head shorter than declared, a header that disagrees
/// with the index, and an index out of order.
pub(crate) fn read_head(bytes: &[u8]) -> anyhow::Result<(PackHeader, Vec<IndexEntry>)> {
    let prefix = read_prefix(bytes)?;
    ensure!(
        bytes.len() >= prefix.head_length(),
        "the pack head is {} bytes but only {} were read",
        prefix.head_length(),
        bytes.len()
    );
    let header_end = PREFIX_BYTES + prefix.header_length;
    let header: PackHeader = serde_json::from_slice(&bytes[PREFIX_BYTES..header_end])
        .context("the pack header is not a section pack header")?;
    ensure!(
        header.compression == COMPRESSION,
        "pack compression {:?} is not {COMPRESSION}",
        header.compression
    );
    ensure!(
        header.index_length == u64::try_from(prefix.index_length)?
            && header.entry_count == u64::try_from(prefix.index_length / INDEX_ENTRY_BYTES)?
            && header.document_count + header.tombstone_count == header.entry_count,
        "the pack header's counts disagree with its index"
    );
    let mut entries = Vec::with_capacity(prefix.index_length / INDEX_ENTRY_BYTES);
    for raw in bytes[header_end..prefix.head_length()].chunks_exact(INDEX_ENTRY_BYTES) {
        let pnu = std::str::from_utf8(&raw[..PNU_BYTES])
            .ok()
            .filter(|pnu| {
                is_digits(pnu)
                    && pnu.starts_with(crate::r2_layout::by_pnu_packs::dong_of(&header.unit))
            })
            .context("a pack index entry names no PNU of the pack's dong")?;
        let state = match raw[27] {
            STATE_DOCUMENT => EntryState::Document,
            STATE_TOMBSTONE => EntryState::Tombstone,
            other => bail!("pack index entry {pnu} has unknown state {other}"),
        };
        let entry = IndexEntry {
            pnu: pnu.to_owned(),
            offset: u32::from_le_bytes([raw[19], raw[20], raw[21], raw[22]]),
            length: u32::from_le_bytes([raw[23], raw[24], raw[25], raw[26]]),
            state,
        };
        ensure!(
            entry.state == EntryState::Document || entry.length == 0,
            "tombstone {pnu} has a body"
        );
        ensure!(
            u64::from(entry.offset) + u64::from(entry.length) <= header.body_length,
            "pack index entry {pnu} points past the body"
        );
        if let Some(last) = entries.last() {
            let last: &IndexEntry = last;
            ensure!(
                last.pnu < entry.pnu,
                "the pack index is not in ascending PNU order"
            );
        }
        entries.push(entry);
    }
    let tombstones = entries
        .iter()
        .filter(|entry| entry.state == EntryState::Tombstone)
        .count();
    ensure!(
        u64::try_from(tombstones)? == header.tombstone_count,
        "the pack header's tombstone count disagrees with its index"
    );
    ensure!(
        header.patch.is_some() || tombstones == 0,
        "a base pack holds a tombstone"
    );
    Ok((header, entries))
}

impl Pack {
    /// Reads a whole pack and checks its body against the header's checksum.
    ///
    /// # Errors
    /// Refuses what [`read_head`] refuses, and a body of another length or checksum.
    pub(crate) fn read(bytes: &[u8]) -> anyhow::Result<Self> {
        let (header, entries) = read_head(bytes)?;
        let body_start = read_prefix(bytes)?.head_length();
        let body = &bytes[body_start..];
        ensure!(
            u64::try_from(body.len())? == header.body_length,
            "the pack body is {} bytes but its header says {}",
            body.len(),
            header.body_length
        );
        ensure!(
            format!("{:x}", Sha256::digest(body)) == header.body_sha256,
            "the pack body does not match its header's sha256"
        );
        Ok(Self {
            header,
            entries,
            body: body.to_vec(),
        })
    }

    /// The entry of `pnu`, by binary search over the sorted index.
    pub(crate) fn find(&self, pnu: &str) -> Option<&IndexEntry> {
        self.entries
            .binary_search_by(|entry| entry.pnu.as_str().cmp(pnu))
            .ok()
            .and_then(|position| self.entries.get(position))
    }

    /// The decompressed document of a document entry; `None` for a tombstone.
    ///
    /// # Errors
    /// Refuses an entry that does not decode.
    pub(crate) fn document(&self, entry: &IndexEntry) -> anyhow::Result<Option<Vec<u8>>> {
        if entry.state == EntryState::Tombstone {
            return Ok(None);
        }
        let start = usize::try_from(entry.offset)?;
        let end = start + usize::try_from(entry.length)?;
        let compressed = self
            .body
            .get(start..end)
            .with_context(|| format!("pack entry {} points past the body", entry.pnu))?;
        gunzip(compressed)
            .map(Some)
            .with_context(|| format!("pack entry {} does not decode", entry.pnu))
    }
}

/// One document as one gzip member: mtime 0, no name, best compression.
fn gzip(document: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .operating_system(255)
        .write(Vec::new(), Compression::best());
    encoder.write_all(document)?;
    Ok(encoder.finish()?)
}

fn gunzip(compressed: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    GzDecoder::new(compressed).read_to_end(&mut decoded)?;
    Ok(decoded)
}

fn magic(policy: &crate::by_pnu_gateway_contract::SectionPackPolicy) -> anyhow::Result<&[u8]> {
    let magic = policy.magic.as_bytes();
    ensure!(
        magic.len() == 8,
        "the contract's pack magic must be 8 bytes"
    );
    Ok(magic)
}

fn is_digits(raw: &str) -> bool {
    !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
pub(crate) mod tests;
