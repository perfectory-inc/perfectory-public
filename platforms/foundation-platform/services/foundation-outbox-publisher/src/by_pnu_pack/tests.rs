use super::*;

const UNIT: &str = "9999900000";
const PNU_A: &str = "9999900000100000000";
const PNU_B: &str = "9999900000100010000";
const PNU_C: &str = "9999900000200000000";

/// Where the golden packs live: beside the Worker's tests, which read the same files.
pub(crate) fn golden_dir() -> anyhow::Result<std::path::PathBuf> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    Ok(std::path::Path::new(&manifest_dir)
        .join("../foundation-building-gateway/test/fixtures/section-packs"))
}

/// Compares `bytes` with the golden file `name`, or writes it when
/// `FOUNDATION_PLATFORM_UPDATE_GOLDEN=1` (then review the diff and commit it).
pub(crate) fn assert_golden(name: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let path = golden_dir()?.join(name);
    if std::env::var("FOUNDATION_PLATFORM_UPDATE_GOLDEN").as_deref() == Ok("1") {
        std::fs::create_dir_all(golden_dir()?)?;
        std::fs::write(&path, bytes)?;
        return Ok(());
    }
    let golden =
        std::fs::read(&path).with_context(|| format!("golden {} is missing", path.display()))?;
    ensure!(
        golden == bytes,
        "{} drifted from what the writer writes now: the pack format or the gzip output changed; \
         a format change is a new format_version (root ADR-0147 §2)",
        path.display()
    );
    Ok(())
}

fn identity(patch: Option<u64>) -> PackIdentity {
    PackIdentity {
        lane: "building-by-pnu".to_owned(),
        section: "floors".to_owned(),
        generation: 1,
        patch,
        unit: UNIT.to_owned(),
        gold_table: "gold.building_panel".to_owned(),
        gold_iceberg_snapshot_id: "999990000000000001".to_owned(),
    }
}

fn sample_patch() -> anyhow::Result<Vec<u8>> {
    let mut writer = PackWriter::new(identity(Some(2)))?;
    writer.push_document(PNU_A, br#"[{"building_id":"a","floors":[]}]"#)?;
    writer.push_tombstone(PNU_B)?;
    writer.push_document(PNU_C, br#"[]"#)?;
    writer.finish()
}

#[test]
fn a_pack_reads_back_every_entry_by_binary_search() -> anyhow::Result<()> {
    let bytes = sample_patch()?;
    let pack = Pack::read(&bytes)?;
    assert_eq!(pack.header.entry_count, 3);
    assert_eq!(
        (pack.header.document_count, pack.header.tombstone_count),
        (2, 1)
    );
    assert_eq!(pack.header.patch, Some(2));
    let a = pack.find(PNU_A).context("A is in the index")?;
    assert_eq!(
        pack.document(a)?.as_deref(),
        Some(br#"[{"building_id":"a","floors":[]}]"#.as_slice())
    );
    let b = pack.find(PNU_B).context("B is in the index")?;
    assert_eq!(b.state, EntryState::Tombstone);
    assert_eq!(pack.document(b)?, None);
    assert!(pack.find("9999900000300000000").is_none());
    // The head alone (one range read) is enough to search.
    let head = read_prefix(&bytes)?.head_length();
    let (header, entries) = read_head(&bytes[..head])?;
    assert_eq!(header, pack.header);
    assert_eq!(entries, pack.entries);
    Ok(())
}

/// One document's bytes are one gzip member on their own: what a range read returns can be sent
/// as `Content-Encoding: gzip` without touching it.
#[test]
fn one_entry_is_one_standalone_gzip_member() -> anyhow::Result<()> {
    let bytes = sample_patch()?;
    let pack = Pack::read(&bytes)?;
    let entry = pack.find(PNU_C).context("C is in the index")?;
    let start = read_prefix(&bytes)?.head_length() + usize::try_from(entry.offset)?;
    let member = &bytes[start..start + usize::try_from(entry.length)?];
    assert_eq!(&member[..2], &[0x1f, 0x8b]);
    assert_eq!(gunzip(member)?, b"[]");
    Ok(())
}

#[test]
fn the_format_bytes_are_pinned() -> anyhow::Result<()> {
    assert_golden("format.golden.pack", &sample_patch()?)
}

#[test]
fn the_writer_refuses_what_the_format_cannot_hold() -> anyhow::Result<()> {
    let mut writer = PackWriter::new(identity(None))?;
    writer.push_document(PNU_B, b"{}")?;
    assert!(writer.push_document(PNU_A, b"{}").is_err(), "out of order");
    assert!(writer.push_document(PNU_B, b"{}").is_err(), "repeated");
    assert!(
        writer.push_document("9999900001100000000", b"{}").is_err(),
        "another dong"
    );
    assert!(
        writer.push_tombstone(PNU_C).is_err(),
        "a tombstone in a base pack"
    );
    assert!(
        PackWriter::new(identity(None))?.finish().is_err(),
        "an empty pack"
    );
    let mut short = identity(None);
    short.unit = "99999".to_owned();
    assert!(
        PackWriter::new(short).is_err(),
        "a unit shorter than a dong"
    );
    Ok(())
}

#[test]
fn the_reader_refuses_a_corrupt_pack() -> anyhow::Result<()> {
    let bytes = sample_patch()?;
    let mut magic = bytes.clone();
    magic[0] = b'X';
    assert!(Pack::read(&magic).is_err(), "wrong magic");
    let mut version = bytes.clone();
    version[8] = 99;
    assert!(Pack::read(&version).is_err(), "unknown format version");
    let mut body = bytes.clone();
    let last = body.len() - 1;
    body[last] ^= 0xff;
    assert!(Pack::read(&body).is_err(), "a body that fails its checksum");
    let head = read_prefix(&bytes)?.head_length();
    assert!(read_head(&bytes[..head - 1]).is_err(), "a short head");
    let mut state = bytes.clone();
    let header_length = usize::try_from(u32::from_le_bytes([
        bytes[12], bytes[13], bytes[14], bytes[15],
    ]))?;
    state[PREFIX_BYTES + header_length + 27] = 9;
    assert!(read_head(&state).is_err(), "an unknown entry state");
    Ok(())
}
