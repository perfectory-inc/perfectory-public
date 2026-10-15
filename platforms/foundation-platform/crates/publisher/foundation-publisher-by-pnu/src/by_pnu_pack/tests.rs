use super::*;

const UNIT: &str = "9999900000";
const PNU_A: &str = "9999900000100000000";
const PNU_B: &str = "9999900000100010000";
const PNU_C: &str = "9999900000200000000";

/// Where the golden packs live: beside the Worker's tests, which read the same files.
pub fn golden_dir() -> anyhow::Result<std::path::PathBuf> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    Ok(std::path::Path::new(&manifest_dir)
        .join("../../../services/foundation-by-pnu-gateway/test/fixtures/section-packs"))
}

/// Compares `bytes` with the golden file `name`. On a difference the test fails and leaves what
/// the writer writes now beside the system temp directory's `section-pack-golden/`, to review and
/// copy over the golden file deliberately.
pub fn assert_golden(name: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let path = golden_dir()?.join(name);
    let golden = std::fs::read(&path).unwrap_or_default();
    if golden != bytes {
        let actual = std::env::temp_dir().join("section-pack-golden");
        std::fs::create_dir_all(&actual)?;
        std::fs::write(actual.join(name), bytes)?;
        anyhow::bail!(
            "{} drifted from what the writer writes now (written to {}): the pack format or the \
             gzip output changed; a format change is a new format_version (root ADR-0147 §2)",
            path.display(),
            actual.join(name).display()
        );
    }
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

/// Bytes gzip cannot shrink, so a document's member is about as long as the document.
fn incompressible(length: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Root ADR-0233: every pack writer holds a document to what one request may copy, whatever the
/// lane, so a new kind of document past it is refused at the bake and never served.
#[test]
fn a_document_past_the_request_budget_is_refused() -> anyhow::Result<()> {
    let bound = usize::try_from(
        section_pack_policy()?
            .read_path
            .request_cpu_budget
            .document_max_bytes,
    )?;
    let mut writer = PackWriter::new(identity(None))?;
    writer.push_document(PNU_A, &incompressible(bound / 2))?;
    let refused = writer
        .push_document(PNU_B, &incompressible(bound + 1))
        .err()
        .context("a document past the budget was written")?;
    assert!(
        refused.to_string().contains("document_max_bytes"),
        "{refused:#}"
    );
    // The refused document left nothing behind: the pack holds the one before it.
    let pack = Pack::read(&writer.finish()?)?;
    assert_eq!(pack.entries.len(), 1);
    Ok(())
}

/// Root ADR-0233: a pack whose head (what every read of it copies) is past the budget is refused,
/// so a dong of more entries than one read may copy never reaches the gateway.
#[test]
fn a_pack_head_past_the_request_budget_is_refused() -> anyhow::Result<()> {
    let bound = usize::try_from(
        section_pack_policy()?
            .read_path
            .request_cpu_budget
            .pack_head_max_bytes,
    )?;
    let entries = |count: usize| -> anyhow::Result<PackWriter> {
        let mut writer = PackWriter::new(identity(None))?;
        for n in 0..count {
            writer.push_document(&format!("{UNIT}{n:09}"), b"{}")?;
        }
        Ok(writer)
    };
    // 28 index bytes an entry: one more than the bound holds.
    let over = bound / 28 + 1;
    let refused = entries(over)?
        .finish()
        .err()
        .context("a head past the budget was written")?;
    assert!(
        refused.to_string().contains("pack_head_max_bytes"),
        "{refused:#}"
    );
    entries(over / 2)?.finish()?;
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

/// A key of the wide format (root ADR-0204): the PNU, then a building id.
fn wide(pnu: &str, building: char) -> String {
    format!("{pnu}{}", building.to_string().repeat(36))
}

fn sample_wide_patch() -> anyhow::Result<Vec<u8>> {
    let mut writer = PackWriter::new(identity(Some(2)))?;
    writer.push_document(&wide(PNU_A, '0'), br#"{"units":[]}"#)?;
    writer.push_document(&wide(PNU_A, 'a'), br#"{"units":[1]}"#)?;
    writer.push_tombstone(&wide(PNU_B, 'b'))?;
    writer.finish()
}

#[test]
fn a_pack_of_wide_keys_is_format_two_and_reads_back_by_key() -> anyhow::Result<()> {
    let policy = crate::by_pnu_gateway_contract::section_pack_policy()?;
    let bytes = sample_wide_patch()?;
    let pack = Pack::read(&bytes)?;
    assert_eq!(pack.header.format_version, policy.wide_key_format_version);
    assert_eq!(pack.header.key_length, Some(55));
    assert_eq!(
        u16::from_le_bytes([bytes[8], bytes[9]]),
        policy.wide_key_format_version
    );
    let a = pack
        .find(&wide(PNU_A, 'a'))
        .context("A's second building is in the index")?;
    assert_eq!(
        pack.document(a)?.as_deref(),
        Some(br#"{"units":[1]}"#.as_slice())
    );
    assert_eq!(
        pack.find(&wide(PNU_B, 'b')).map(|entry| entry.state),
        Some(EntryState::Tombstone)
    );
    assert!(
        pack.find(PNU_A).is_none(),
        "a PNU alone is not a key of this pack"
    );
    let head = read_prefix(&bytes)?.head_length();
    assert_eq!(read_head(&bytes[..head])?.1, pack.entries);
    // A pack of PNU keys is still format 1, without a key length: the served packs keep their bytes.
    let narrow = Pack::read(&sample_patch()?)?;
    assert_eq!(narrow.header.format_version, policy.format_version);
    assert_eq!(narrow.header.key_length, None);
    Ok(())
}

#[test]
fn the_wide_format_bytes_are_pinned() -> anyhow::Result<()> {
    assert_golden("format-wide-key.golden.pack", &sample_wide_patch()?)
}

#[test]
fn a_wide_key_is_one_length_a_pnu_of_the_dong_and_no_slash() -> anyhow::Result<()> {
    let mut writer = PackWriter::new(identity(None))?;
    writer.push_document(&wide(PNU_A, 'a'), b"{}")?;
    assert!(
        writer.push_document(&format!("{PNU_B}x"), b"{}").is_err(),
        "a key of another length"
    );
    assert!(
        writer.push_document(PNU_B, b"{}").is_err(),
        "a PNU key beside wide ones"
    );
    let mut writer = PackWriter::new(identity(None))?;
    assert!(
        writer.push_document(&format!("{PNU_A}a/b"), b"{}").is_err(),
        "a slash"
    );
    assert!(
        writer.push_document(&format!("{PNU_A}a b"), b"{}").is_err(),
        "a space"
    );
    assert!(
        writer
            .push_document(&format!("{PNU_A}{}", "a".repeat(64)), b"{}")
            .is_err(),
        "longer than max_key_bytes"
    );
    assert!(
        writer
            .push_document(&wide("9999900001100000000", 'a'), b"{}")
            .is_err(),
        "another dong"
    );
    Ok(())
}

#[test]
fn the_reader_holds_the_version_and_the_key_length_together() -> anyhow::Result<()> {
    let policy = crate::by_pnu_gateway_contract::section_pack_policy()?;
    // A wide pack that claims format 1 would be read with 28-byte entries: refused.
    let mut claims_one = sample_wide_patch()?;
    claims_one[8..10].copy_from_slice(&policy.format_version.to_le_bytes());
    assert!(Pack::read(&claims_one).is_err());
    // A PNU pack that claims format 2 states no key length: refused.
    let mut claims_two = sample_patch()?;
    claims_two[8..10].copy_from_slice(&policy.wide_key_format_version.to_le_bytes());
    assert!(Pack::read(&claims_two).is_err());
    Ok(())
}
