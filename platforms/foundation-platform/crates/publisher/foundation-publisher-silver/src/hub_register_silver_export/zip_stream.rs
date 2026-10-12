use std::io::{BufReader, Read, Seek, SeekFrom, Take};

use anyhow::{ensure, Context};
use flate2::bufread::DeflateDecoder;

pub(super) struct Member<R: Read> {
    pub decoder: DeflateDecoder<BufReader<Take<R>>>,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub crc32: u32,
}

/// `zip` owns ZIP64 and central-directory interpretation; this adapter only opens the
/// single raw-deflate byte range and cross-checks its local framing (APPNOTE 4.3.7).
pub(super) fn open<R: Read + Seek>(source: R, expected_name: &str) -> anyhow::Result<Member<R>> {
    let mut archive = zip::ZipArchive::new(source).context("invalid HUB ZIP directory")?;
    ensure!(
        archive.len() == 1,
        "HUB ZIP must contain exactly one member"
    );
    let member = archive.by_index(0).context("invalid HUB ZIP member")?;
    ensure!(
        member.name() == expected_name,
        "unexpected HUB ZIP member name"
    );
    ensure!(
        !member.encrypted() && member.compression() == zip::CompressionMethod::Deflated,
        "HUB ZIP requires unencrypted raw deflate"
    );
    let (header_start, data_start, compressed_bytes, uncompressed_bytes, crc32) = (
        member.header_start(),
        member.data_start(),
        member.compressed_size(),
        member.size(),
        member.crc32(),
    );
    drop(member);
    let mut source = archive.into_inner();
    let object_bytes = source.seek(SeekFrom::End(0))?;
    let data_end = data_start
        .checked_add(compressed_bytes)
        .context("ZIP data range overflow")?;
    ensure!(
        data_end <= object_bytes,
        "ZIP compressed range exceeds object"
    );
    source.seek(SeekFrom::Start(header_start))?;
    let mut header = [0_u8; 30];
    source
        .read_exact(&mut header)
        .context("truncated ZIP local header")?;
    ensure!(&header[..4] == b"PK\x03\x04", "invalid ZIP local signature");
    let flags = u16::from_le_bytes([header[6], header[7]]);
    let method = u16::from_le_bytes([header[8], header[9]]);
    ensure!(
        flags & 1 == 0 && method == 8,
        "unsupported ZIP local flags/method"
    );
    let name_bytes = u16::from_le_bytes([header[26], header[27]]);
    let extra_bytes = u16::from_le_bytes([header[28], header[29]]);
    let offset = header_start
        .checked_add(30 + u64::from(name_bytes) + u64::from(extra_bytes))
        .context("ZIP local offset overflow")?;
    ensure!(
        offset == data_start,
        "ZIP local and central offsets disagree"
    );
    let mut name = vec![0; usize::from(name_bytes)];
    source.read_exact(&mut name)?;
    ensure!(
        name == expected_name.as_bytes(),
        "ZIP local and central names disagree"
    );
    source.seek(SeekFrom::Start(offset))?;
    Ok(Member {
        decoder: DeflateDecoder::new(BufReader::new(source.take(compressed_bytes))),
        compressed_bytes,
        uncompressed_bytes,
        crc32,
    })
}
