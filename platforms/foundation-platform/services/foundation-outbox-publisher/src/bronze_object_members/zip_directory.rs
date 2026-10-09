//! Reads a ZIP's member list from its end records and central directory alone (APPNOTE 4.3.12,
//! 4.3.14–4.3.16, 4.5.3), so an object of several gigabytes is measured with a few kilobytes.
//!
//! The `zip` crate is not used here on purpose (root ADR-0169): opening an archive with it scans
//! backwards for the end record as far as the start of the object (zip 2.4
//! `spec::find_central_directory`), so a ranged reader over an object that is not a ZIP reads the
//! whole object, and it decodes a name without the UTF-8 flag as CP437, which turns the Korean
//! names the providers write into other letters. The tests build their archives with the `zip`
//! crate and read them back with it, so the two readings are compared on every run.
//!
//! Every function here is pure: the caller fetches the bytes it is told to fetch.

use chrono::{NaiveDate, NaiveDateTime};
use encoding_rs::EUC_KR;

/// End of central directory record, fixed part (APPNOTE 4.3.16).
const END_RECORD_BYTES: u64 = 22;
/// The longest comment an end record can carry.
const MAX_COMMENT_BYTES: u64 = u16::MAX as u64;
/// ZIP64 end of central directory locator (APPNOTE 4.3.15).
const ZIP64_LOCATOR_BYTES: u64 = 20;
/// ZIP64 end of central directory record, fixed part (APPNOTE 4.3.14).
pub(crate) const ZIP64_END_RECORD_BYTES: u64 = 56;
/// Central directory file header, fixed part (APPNOTE 4.3.12).
const DIRECTORY_HEADER_BYTES: usize = 46;

const END_SIGNATURE: [u8; 4] = *b"PK\x05\x06";
const ZIP64_LOCATOR_SIGNATURE: [u8; 4] = *b"PK\x06\x07";
const ZIP64_END_SIGNATURE: [u8; 4] = *b"PK\x06\x06";
const DIRECTORY_HEADER_SIGNATURE: [u8; 4] = *b"PK\x01\x02";

/// Extra field holding 64-bit sizes and offsets (APPNOTE 4.5.3).
const ZIP64_EXTRA_ID: u16 = 0x0001;
/// Info-ZIP Unicode Path extra field: a UTF-8 name beside a legacy-encoded one.
const UNICODE_PATH_EXTRA_ID: u16 = 0x7075;
/// General purpose flag bit 11: the name is UTF-8 (APPNOTE 4.4.4).
const UTF8_NAME_FLAG: u16 = 1 << 11;

/// How many bytes from the end of an object the first read takes.
///
/// It always holds the end record with the longest possible comment and both ZIP64 records, so
/// the end is found in one request; the rest of the window usually holds the whole central
/// directory too (a directory entry is about a hundred bytes), so a ZIP of up to several hundred
/// members is measured in that one request.
pub(crate) const TAIL_BYTES: u64 = 128 * 1024;
const _: () = assert!(
    TAIL_BYTES
        >= END_RECORD_BYTES + MAX_COMMENT_BYTES + ZIP64_LOCATOR_BYTES + ZIP64_END_RECORD_BYTES
);

/// The largest central directory a measurement will fetch.
///
/// About half a million members. The ledger has nothing near it; a directory claiming more is
/// treated as unreadable rather than fetched.
pub(crate) const MAX_DIRECTORY_BYTES: u64 = 64 * 1024 * 1024;

/// One entry of a central directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Member {
    /// Position in the central directory, from zero. Names may repeat in a ZIP; this does not.
    pub index: u32,
    /// The decoded name.
    pub name: String,
    /// How the name was decoded.
    pub name_encoding: NameEncoding,
    /// Uncompressed size, from the ZIP64 extra field when the 32-bit field is saturated.
    pub uncompressed_size: u64,
    /// Compressed size, likewise.
    pub compressed_size: u64,
    /// The MS-DOS date and time in the header, as written (no time zone); `None` when invalid.
    pub modified: Option<NaiveDateTime>,
}

/// How a member name's bytes became text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameEncoding {
    /// The header's UTF-8 flag was set and the bytes are UTF-8.
    Utf8Flagged,
    /// An Info-ZIP Unicode Path extra field matched the name and supplied it.
    UnicodePathExtra,
    /// No flag, but the bytes are valid UTF-8 (every ASCII name is here).
    Utf8Unflagged,
    /// Not UTF-8; decoded as CP949 (the Korean Windows code page) without a replacement.
    Cp949,
    /// Neither decoded cleanly; replacement characters stand for the bytes that did not.
    Lossy,
}

impl NameEncoding {
    /// The value stored in `catalog.bronze_object_member.member_name_encoding`.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Utf8Flagged => "utf8_flagged",
            Self::UnicodePathExtra => "unicode_path_extra",
            Self::Utf8Unflagged => "utf8_unflagged",
            Self::Cp949 => "cp949",
            Self::Lossy => "lossy",
        }
    }
}

/// Where the central directory is and how many entries it claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Directory {
    /// Absolute offset of the first directory header in the object.
    pub start: u64,
    /// Bytes of the central directory.
    pub size: u64,
    /// Entries the end record claims.
    pub entries: u64,
}

/// What the end of an object says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Tail {
    /// No end record: the object is not a ZIP.
    NotZip(String),
    /// The structure is broken in a way more bytes would not fix.
    Unreadable(String),
    /// The central directory is here.
    Directory(Directory),
    /// A ZIP64 end record lies before the bytes read; fetch `ZIP64_END_RECORD_BYTES` at `offset`
    /// and pass them to [`read_zip64_end`] with `directory_end` = `offset`.
    Zip64EndAt {
        /// Absolute offset of the ZIP64 end record.
        offset: u64,
    },
}

/// The byte range `[start, end)` of an object a measurement reads first.
pub(crate) const fn tail_range(object_size: u64) -> (u64, u64) {
    let length = if object_size < TAIL_BYTES {
        object_size
    } else {
        TAIL_BYTES
    };
    (object_size - length, object_size)
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0_u8; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(word)
}

/// Finds the end record in the last bytes of an object and says where the directory is.
///
/// `tail` must be the bytes `[tail_start, object_size)`.
pub(crate) fn read_tail(tail: &[u8], tail_start: u64, object_size: u64) -> Tail {
    let tail_len = tail.len() as u64;
    if tail_start + tail_len != object_size {
        return Tail::Unreadable(format!(
            "read {tail_len} bytes from {tail_start} of an object of {object_size}"
        ));
    }
    if tail_len < END_RECORD_BYTES {
        return Tail::NotZip(format!("{object_size} bytes is shorter than an end record"));
    }
    // From the last place an end record can start, backwards over the longest comment.
    let last = tail_len - END_RECORD_BYTES;
    let first = last.saturating_sub(MAX_COMMENT_BYTES);
    let mut position = last;
    let mut seen_signature = false;
    loop {
        let at = position as usize;
        if tail[at..at + 4] == END_SIGNATURE {
            seen_signature = true;
            let comment = u64::from(u16_at(tail, at + 20));
            if position + END_RECORD_BYTES + comment <= tail_len {
                return read_end_record(tail, at, tail_start);
            }
        }
        if position == first {
            break;
        }
        position -= 1;
    }
    if seen_signature {
        Tail::Unreadable("an end record signature whose comment runs past the object".to_owned())
    } else {
        Tail::NotZip(format!(
            "no end of central directory record in the last {tail_len} bytes"
        ))
    }
}

fn read_end_record(tail: &[u8], at: usize, tail_start: u64) -> Tail {
    let end_record = tail_start + at as u64;
    let disk = u16_at(tail, at + 4);
    let directory_disk = u16_at(tail, at + 6);
    let entries = u64::from(u16_at(tail, at + 10));
    let size = u64::from(u32_at(tail, at + 12));
    let offset = u64::from(u32_at(tail, at + 16));

    // A ZIP64 archive puts its locator immediately before the end record.
    if at as u64 >= ZIP64_LOCATOR_BYTES {
        let locator = at - ZIP64_LOCATOR_BYTES as usize;
        if tail[locator..locator + 4] == ZIP64_LOCATOR_SIGNATURE {
            let zip64_end = u64_at(tail, locator + 8);
            let disks = u32_at(tail, locator + 16);
            if disks > 1 {
                return Tail::Unreadable(format!("a ZIP spread over {disks} disks"));
            }
            let locator_start = tail_start + locator as u64;
            if zip64_end + ZIP64_END_RECORD_BYTES > locator_start {
                return Tail::Unreadable(format!(
                    "the ZIP64 end record at {zip64_end} overlaps its locator at {locator_start}"
                ));
            }
            if zip64_end >= tail_start {
                let from = (zip64_end - tail_start) as usize;
                return read_zip64_end(
                    &tail[from..from + ZIP64_END_RECORD_BYTES as usize],
                    zip64_end,
                );
            }
            return Tail::Zip64EndAt { offset: zip64_end };
        }
    }
    if disk != 0 || directory_disk != 0 {
        return Tail::Unreadable(format!(
            "a ZIP on disk {disk}, directory on disk {directory_disk}"
        ));
    }
    locate(end_record, size, offset, entries)
}

/// Reads a ZIP64 end record fetched separately; `directory_end` is its own offset.
pub(crate) fn read_zip64_end(record: &[u8], directory_end: u64) -> Tail {
    if record.len() < ZIP64_END_RECORD_BYTES as usize || record[..4] != ZIP64_END_SIGNATURE {
        return Tail::Unreadable(format!("no ZIP64 end record at {directory_end}"));
    }
    let disk = u32_at(record, 16);
    let directory_disk = u32_at(record, 20);
    if disk != 0 || directory_disk != 0 {
        return Tail::Unreadable(format!(
            "a ZIP64 on disk {disk}, directory on disk {directory_disk}"
        ));
    }
    let entries = u64_at(record, 32);
    let size = u64_at(record, 40);
    let offset = u64_at(record, 48);
    locate(directory_end, size, offset, entries)
}

/// The directory ends where the end records begin. Anything before the archive (a
/// self-extractor's stub) shifts every offset alike, so the directory is placed from its end
/// back, and the recorded offset only has to be no larger (as Python's `zipfile` does).
fn locate(directory_end: u64, size: u64, offset: u64, entries: u64) -> Tail {
    if size > MAX_DIRECTORY_BYTES {
        return Tail::Unreadable(format!(
            "a central directory of {size} bytes, above the {MAX_DIRECTORY_BYTES} cap"
        ));
    }
    let Some(start) = directory_end.checked_sub(size) else {
        return Tail::Unreadable(format!(
            "a central directory of {size} bytes ending at {directory_end}"
        ));
    };
    if offset > start {
        return Tail::Unreadable(format!(
            "the central directory is recorded at {offset} but ends at {directory_end}"
        ));
    }
    if entries.saturating_mul(DIRECTORY_HEADER_BYTES as u64) > size {
        return Tail::Unreadable(format!("{entries} entries cannot fit in {size} bytes"));
    }
    Tail::Directory(Directory {
        start,
        size,
        entries,
    })
}

/// Parses the central directory's bytes into its members.
///
/// # Errors
/// Returns the reason when a header is truncated or its signature is wrong, or when the count
/// differs from what the end record claimed.
pub(crate) fn read_directory(bytes: &[u8], directory: Directory) -> Result<Vec<Member>, String> {
    let mut members = Vec::new();
    let mut at = 0_usize;
    while at < bytes.len() && (members.len() as u64) < directory.entries {
        let index = u32::try_from(members.len())
            .map_err(|_| "more members than a u32 counts".to_owned())?;
        let header = bytes
            .get(at..at + DIRECTORY_HEADER_BYTES)
            .ok_or_else(|| format!("directory header {index} is truncated"))?;
        if header[..4] != DIRECTORY_HEADER_SIGNATURE {
            return Err(format!("directory header {index} has no signature"));
        }
        let flags = u16_at(header, 8);
        let time = u16_at(header, 12);
        let date = u16_at(header, 14);
        let compressed = u64::from(u32_at(header, 20));
        let uncompressed = u64::from(u32_at(header, 24));
        let name_len = usize::from(u16_at(header, 28));
        let extra_len = usize::from(u16_at(header, 30));
        let comment_len = usize::from(u16_at(header, 32));
        let name_start = at + DIRECTORY_HEADER_BYTES;
        let extra_start = name_start + name_len;
        let next = extra_start + extra_len + comment_len;
        if next > bytes.len() {
            return Err(format!("directory entry {index} runs past the directory"));
        }
        let raw_name = &bytes[name_start..extra_start];
        let extra = &bytes[extra_start..extra_start + extra_len];
        let (uncompressed_size, compressed_size) = zip64_sizes(extra, uncompressed, compressed)
            .map_err(|reason| format!("directory entry {index}: {reason}"))?;
        let (name, name_encoding) = decode_name(raw_name, flags, extra);
        if name.is_empty() {
            return Err(format!("directory entry {index} has no name"));
        }
        members.push(Member {
            index,
            name,
            name_encoding,
            uncompressed_size,
            compressed_size,
            modified: dos_date_time(date, time),
        });
        at = next;
    }
    if members.len() as u64 != directory.entries {
        return Err(format!(
            "the end record claims {} entries; the directory holds {}",
            directory.entries,
            members.len()
        ));
    }
    Ok(members)
}

/// The extra field's blocks as `(id, data)`, stopping at the first block that runs past it.
fn extra_blocks(extra: &[u8]) -> impl Iterator<Item = (u16, &[u8])> {
    let mut at = 0_usize;
    std::iter::from_fn(move || {
        let id = u16_at(extra.get(at..at + 4)?, 0);
        let len = usize::from(u16_at(extra, at + 2));
        let data = extra.get(at + 4..at + 4 + len)?;
        at += 4 + len;
        Some((id, data))
    })
}

/// The ZIP64 extra field holds, in this order, each of uncompressed size, compressed size and
/// local header offset whose 32-bit field is saturated, and only those (APPNOTE 4.5.3).
fn zip64_sizes(extra: &[u8], uncompressed: u64, compressed: u64) -> Result<(u64, u64), String> {
    let wants_uncompressed = uncompressed == u64::from(u32::MAX);
    let wants_compressed = compressed == u64::from(u32::MAX);
    if !wants_uncompressed && !wants_compressed {
        return Ok((uncompressed, compressed));
    }
    let data = extra_blocks(extra)
        .find(|(id, _)| *id == ZIP64_EXTRA_ID)
        .map(|(_, data)| data)
        .ok_or_else(|| "a saturated size without a ZIP64 extra field".to_owned())?;
    let mut words = data.chunks_exact(8).map(|word| u64_at(word, 0));
    let mut next = |what: &str| {
        words
            .next()
            .ok_or_else(|| format!("ZIP64 extra lacks the {what}"))
    };
    let uncompressed = if wants_uncompressed {
        next("uncompressed size")?
    } else {
        uncompressed
    };
    let compressed = if wants_compressed {
        next("compressed size")?
    } else {
        compressed
    };
    Ok((uncompressed, compressed))
}

fn decode_name(raw: &[u8], flags: u16, extra: &[u8]) -> (String, NameEncoding) {
    if flags & UTF8_NAME_FLAG != 0 {
        if let Ok(name) = std::str::from_utf8(raw) {
            return clean(name.to_owned(), NameEncoding::Utf8Flagged);
        }
    }
    if let Some(name) = unicode_path(raw, extra) {
        return clean(name, NameEncoding::UnicodePathExtra);
    }
    if let Ok(name) = std::str::from_utf8(raw) {
        return clean(name.to_owned(), NameEncoding::Utf8Unflagged);
    }
    let (name, _, had_errors) = EUC_KR.decode(raw);
    if had_errors {
        (
            String::from_utf8_lossy(raw).replace('\0', "\u{fffd}"),
            NameEncoding::Lossy,
        )
    } else {
        clean(name.into_owned(), NameEncoding::Cp949)
    }
}

/// A name with a NUL cannot be stored as text; it is kept, marked lossy.
fn clean(name: String, encoding: NameEncoding) -> (String, NameEncoding) {
    if name.contains('\0') {
        (name.replace('\0', "\u{fffd}"), NameEncoding::Lossy)
    } else {
        (name, encoding)
    }
}

/// The Info-ZIP Unicode Path field: version 1, the CRC-32 of the header's name, the UTF-8 name.
/// A CRC that does not match means the header name was changed after; the field is ignored.
fn unicode_path(raw: &[u8], extra: &[u8]) -> Option<String> {
    let (_, data) = extra_blocks(extra).find(|(id, _)| *id == UNICODE_PATH_EXTRA_ID)?;
    if data.len() < 5 || data[0] != 1 {
        return None;
    }
    let mut crc = flate2::Crc::new();
    crc.update(raw);
    if crc.sum() != u32_at(data, 1) {
        return None;
    }
    std::str::from_utf8(&data[5..]).ok().map(ToOwned::to_owned)
}

/// MS-DOS date and time (APPNOTE 4.4.6): seconds in units of two. Zero or out-of-range fields
/// mean the writer recorded no time.
fn dos_date_time(date: u16, time: u16) -> Option<NaiveDateTime> {
    let year = 1980 + i32::from(date >> 9);
    let month = u32::from((date >> 5) & 0x0f);
    let day = u32::from(date & 0x1f);
    let hour = u32::from(time >> 11);
    let minute = u32::from((time >> 5) & 0x3f);
    let second = u32::from(time & 0x1f) * 2;
    NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, minute, second)
}
