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
//!       16   index_length      u32 LE    bytes of the index = entry_count x entry bytes (28)
//!       20   header            JSON      [`PackHeader`], compact, UTF-8
//!            index             entries   sorted by PNU, distinct
//!            body              bytes     the documents' gzip members back to back
//! index entry: key (19 ASCII digits) | offset u32 LE (from the body start) | length u32 LE
//!              | state u8 (1 document, 2 tombstone; a tombstone has length 0)
//! ```
//!
//! That is format 1, whose keys are PNUs. Format 2 (root ADR-0204) is the same layout for a lane
//! whose document key is a PNU and a suffix (unit-price: the PNU and a building id): its header
//! states `key_length`, every key is that long, and an entry is `key_length + 9` bytes. A pack
//! whose keys are PNUs is always written as format 1, so the packs already served keep their bytes.
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
pub const PREFIX_BYTES: usize = crate::by_pnu_gateway_contract::PACK_PREFIX_BYTES;
const PNU_BYTES: usize = 19;
/// Bytes after an entry's key: offset, length, state.
const ENTRY_TAIL_BYTES: usize = 9;
const STATE_DOCUMENT: u8 = 1;
const STATE_TOMBSTONE: u8 = 2;
/// The only compression a pack names.
pub const COMPRESSION: &str = "gzip";

/// What a pack says about itself; the first thing after the fixed prefix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PackHeader {
    pub format_version: u16,
    /// The manifest unit of the lane (`building-by-pnu`).
    pub lane: String,
    pub section: String,
    pub generation: u64,
    /// The patch this pack belongs to; `None` for a base pack.
    pub patch: Option<u64>,
    /// The legal dong (PNU prefix of the contract's `unit_prefix_length`) every entry is under.
    pub unit: String,
    pub gold_table: String,
    pub gold_iceberg_snapshot_id: String,
    pub compression: String,
    pub entry_count: u64,
    pub document_count: u64,
    pub tombstone_count: u64,
    pub index_length: u64,
    pub body_length: u64,
    pub body_sha256: String,
    /// Bytes of every key of a format 2 pack (root ADR-0204); absent in format 1, whose keys are
    /// 19-digit PNUs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_length: Option<u64>,
}

/// What the writer is told; the counts, lengths and checksum are the writer's to fill in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackIdentity {
    pub lane: String,
    pub section: String,
    pub generation: u64,
    pub patch: Option<u64>,
    pub unit: String,
    pub gold_table: String,
    pub gold_iceberg_snapshot_id: String,
}

/// One key's entry: a document, or a tombstone (the key is gone as of this patch).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryState {
    Document,
    Tombstone,
}

/// One index entry as read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexEntry {
    /// The entry's key: a PNU, or a PNU and its lane's suffix (root ADR-0204).
    pub pnu: String,
    pub offset: u32,
    pub length: u32,
    pub state: EntryState,
}

/// Collects one pack's entries in PNU order and lays out its bytes.
pub struct PackWriter {
    identity: PackIdentity,
    entries: Vec<IndexEntry>,
    body: Vec<u8>,
}

impl PackWriter {
    /// A writer for one pack.
    ///
    /// # Errors
    /// Refuses a unit that is not the contract's dong length of digits.
    pub fn new(identity: PackIdentity) -> anyhow::Result<Self> {
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
    /// Refuses a PNU out of order or outside the unit, a body past 4 GiB, and a document whose
    /// member is past the contract's request CPU budget (root ADR-0233).
    pub fn push_document(&mut self, pnu: &str, document: &[u8]) -> anyhow::Result<()> {
        self.check_next(pnu)?;
        let compressed = gzip(document)?;
        let bound = section_pack_policy()?
            .read_path
            .request_cpu_budget
            .document_max_bytes;
        ensure!(
            u64::try_from(compressed.len())? <= bound,
            "the document of {pnu} is {} bytes compressed, above the {bound} one request may copy              (request_cpu_budget.document_max_bytes); narrow the lane's document key (root              ADR-0233)",
            compressed.len()
        );
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
    pub fn push_tombstone(&mut self, pnu: &str) -> anyhow::Result<()> {
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
            is_key(
                pnu,
                &self.identity.unit,
                section_pack_policy()?.max_key_bytes
            ),
            "{pnu:?} is not a key of legal dong {}: its 19-digit PNU, then printable ASCII other \
             than '/'",
            self.identity.unit
        );
        if let Some(last) = self.entries.last() {
            ensure!(
                last.pnu.len() == pnu.len(),
                "every key of one pack has one length: {pnu} after {}",
                last.pnu
            );
            ensure!(
                last.pnu.as_str() < pnu,
                "pack entries must be in strictly ascending key order: {pnu} after {}",
                last.pnu
            );
        }
        Ok(())
    }

    /// Lays out the pack.
    ///
    /// # Errors
    /// Refuses an empty pack, one with more entries than the contract allows, and one whose head
    /// is past the contract's request CPU budget (root ADR-0233).
    pub fn finish(self) -> anyhow::Result<Vec<u8>> {
        let policy = section_pack_policy()?;
        ensure!(!self.entries.is_empty(), "a pack holds at least one entry");
        ensure!(
            self.entries.len() <= policy.max_index_entries,
            "{} entries exceed the contract's max_index_entries {}",
            self.entries.len(),
            policy.max_index_entries
        );
        let key_length = self.entries[0].pnu.len();
        // A pack of PNU keys is format 1, byte for byte as before root ADR-0204.
        let (version, header_key_length) = if key_length == PNU_BYTES {
            (policy.format_version, None)
        } else {
            (
                policy.wide_key_format_version,
                Some(u64::try_from(key_length)?),
            )
        };
        let mut index = Vec::with_capacity(self.entries.len() * (key_length + ENTRY_TAIL_BYTES));
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
            format_version: version,
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
            key_length: header_key_length,
        };
        let header = serde_json::to_vec(&header).context("failed to serialize a pack header")?;
        let head = PREFIX_BYTES + header.len() + index.len();
        let bound = policy.read_path.request_cpu_budget.pack_head_max_bytes;
        ensure!(
            u64::try_from(head)? <= bound,
            "the pack of {} has a {head}-byte head ({} entries), above the {bound} every read of              it copies (request_cpu_budget.pack_head_max_bytes; root ADR-0233)",
            self.entries[0].pnu,
            self.entries.len()
        );
        let mut pack =
            Vec::with_capacity(PREFIX_BYTES + header.len() + index.len() + self.body.len());
        pack.extend_from_slice(magic(policy)?);
        pack.extend_from_slice(&version.to_le_bytes());
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
pub struct Pack {
    pub header: PackHeader,
    pub entries: Vec<IndexEntry>,
    body: Vec<u8>,
}

/// The lengths the fixed prefix declares.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackPrefix {
    pub format_version: u16,
    pub header_length: usize,
    pub index_length: usize,
}

impl PackPrefix {
    /// Bytes of the head: prefix, header and index — what one range read must cover to search.
    pub const fn head_length(self) -> usize {
        PREFIX_BYTES + self.header_length + self.index_length
    }
}

/// Reads the fixed prefix.
///
/// # Errors
/// Refuses another magic, another format version, and a set reserved field.
pub fn read_prefix(bytes: &[u8]) -> anyhow::Result<PackPrefix> {
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
        version == policy.format_version || version == policy.wide_key_format_version,
        "pack format version {version} is not the contract's {} or {}",
        policy.format_version,
        policy.wide_key_format_version
    );
    ensure!(
        bytes[10] == 0 && bytes[11] == 0,
        "the pack's reserved field is set"
    );
    let header_length = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let index_length = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    Ok(PackPrefix {
        format_version: version,
        header_length: usize::try_from(header_length)?,
        index_length: usize::try_from(index_length)?,
    })
}

/// Reads the head: the header and every index entry. `bytes` may run past the head.
///
/// # Errors
/// Refuses what [`read_prefix`] refuses, a head shorter than declared, a header that disagrees
/// with the index, and an index out of order.
pub fn read_head(bytes: &[u8]) -> anyhow::Result<(PackHeader, Vec<IndexEntry>)> {
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
        header.format_version == prefix.format_version,
        "the pack header's format version disagrees with its prefix"
    );
    let policy = section_pack_policy()?;
    // Format 1 keys are PNUs; format 2 states its key length (root ADR-0204).
    let key_length = if prefix.format_version == policy.format_version {
        ensure!(
            header.key_length.is_none(),
            "a format {} pack states a key length",
            policy.format_version
        );
        PNU_BYTES
    } else {
        header
            .key_length
            .and_then(|length| usize::try_from(length).ok())
            .filter(|length| (PNU_BYTES + 1..=policy.max_key_bytes).contains(length))
            .with_context(|| {
                format!(
                    "a format {} pack states no key length between {} and {}",
                    policy.wide_key_format_version,
                    PNU_BYTES + 1,
                    policy.max_key_bytes
                )
            })?
    };
    let entry_bytes = key_length + ENTRY_TAIL_BYTES;
    ensure!(
        prefix.index_length % entry_bytes == 0,
        "pack index length {} is not a whole number of entries",
        prefix.index_length
    );
    ensure!(
        header.index_length == u64::try_from(prefix.index_length)?
            && header.entry_count == u64::try_from(prefix.index_length / entry_bytes)?
            && header.document_count + header.tombstone_count == header.entry_count,
        "the pack header's counts disagree with its index"
    );
    let mut entries = Vec::with_capacity(prefix.index_length / entry_bytes);
    for raw in bytes[header_end..prefix.head_length()].chunks_exact(entry_bytes) {
        let pnu = std::str::from_utf8(&raw[..key_length])
            .ok()
            .filter(|key| is_key(key, &header.unit, policy.max_key_bytes))
            .context("a pack index entry names no key of the pack's dong")?;
        let tail = &raw[key_length..];
        let state = match tail[8] {
            STATE_DOCUMENT => EntryState::Document,
            STATE_TOMBSTONE => EntryState::Tombstone,
            other => bail!("pack index entry {pnu} has unknown state {other}"),
        };
        let entry = IndexEntry {
            pnu: pnu.to_owned(),
            offset: u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]),
            length: u32::from_le_bytes([tail[4], tail[5], tail[6], tail[7]]),
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
                "the pack index is not in ascending key order"
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
    pub fn read(bytes: &[u8]) -> anyhow::Result<Self> {
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
    pub fn find(&self, pnu: &str) -> Option<&IndexEntry> {
        self.entries
            .binary_search_by(|entry| entry.pnu.as_str().cmp(pnu))
            .ok()
            .and_then(|position| self.entries.get(position))
    }

    /// The decompressed document of a document entry; `None` for a tombstone.
    ///
    /// # Errors
    /// Refuses an entry that does not decode.
    pub fn document(&self, entry: &IndexEntry) -> anyhow::Result<Option<Vec<u8>>> {
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

/// A key of a pack of `unit`: the 19-digit PNU of its dong, then nothing (format 1) or a suffix of
/// printable ASCII other than '/' (format 2, root ADR-0204), at most `max` bytes in all.
fn is_key(key: &str, unit: &str, max: usize) -> bool {
    key.len() <= max
        && key.get(..PNU_BYTES).is_some_and(|pnu| {
            is_digits(pnu) && pnu.starts_with(crate::r2_layout::by_pnu_packs::dong_of(unit))
        })
        && key
            .bytes()
            .skip(PNU_BYTES)
            .all(|byte| byte.is_ascii_graphic() && byte != b'/')
}

#[cfg(test)]
pub mod tests;
