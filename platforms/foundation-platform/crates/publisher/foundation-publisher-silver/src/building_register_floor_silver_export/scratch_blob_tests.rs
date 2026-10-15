use super::ScratchBlobCodec;
use crate::serving_scratch::CURSOR_BYTES;
use anyhow::Context;

#[test]
fn compressed_blobs_round_trip_across_reused_streams_and_output_chunks() -> anyhow::Result<()> {
    let mut codec = ScratchBlobCodec::new();
    for ordinal in 0..4 {
        let original = format!("building-{ordinal}: 지하층, lineage, checksum\n")
            .repeat(4_000)
            .into_bytes();
        let stored = codec.encode(original.clone())?;
        assert!(stored.len() < original.len());
        assert_eq!(
            codec.decode(&stored, i64::try_from(original.len())?, original.len())?,
            original
        );
    }
    Ok(())
}

#[test]
fn mixed_entropy_blob_round_trips_across_compression_output_chunks() -> anyhow::Result<()> {
    use sha2::{Digest, Sha256};

    let mut original = Vec::new();
    for index in 0_u32..2_048 {
        original.extend_from_slice(&Sha256::digest(index.to_le_bytes()));
    }
    original.resize(128 * 1024, 0);
    let mut codec = ScratchBlobCodec::new();
    let stored = codec.encode(original.clone())?;
    assert!(stored.len() > 32 * 1024 && stored.len() < original.len());
    assert_eq!(
        codec.decode(&stored, i64::try_from(original.len())?, CURSOR_BYTES)?,
        original
    );
    Ok(())
}

#[test]
fn small_and_incompressible_blobs_keep_the_original_bytes() -> anyhow::Result<()> {
    let mut codec = ScratchBlobCodec::new();
    for original in [Vec::new(), vec![42], b"{}".to_vec(), (0..=255).collect()] {
        let stored = codec.encode(original.clone())?;
        assert_eq!(stored, original, "compression must never enlarge a BLOB");
        assert_eq!(
            codec.decode(&stored, i64::try_from(original.len())?, original.len())?,
            original
        );
    }
    Ok(())
}

#[test]
fn decode_rejects_invalid_lengths_and_remaining_byte_budgets() -> anyhow::Result<()> {
    let mut codec = ScratchBlobCodec::new();
    let original = vec![b'x'; 4_096];
    let stored = codec.encode(original.clone())?;
    for declared in [-1, i64::MAX, i64::try_from(CURSOR_BYTES)? + 1] {
        assert!(codec.decode(&stored, declared, CURSOR_BYTES).is_err());
    }
    assert!(codec.decode(&stored, 4_096, 4_095).is_err());
    assert!(codec.decode(b"raw", 2, CURSOR_BYTES).is_err());
    assert!(codec.decode(&stored, 4_096, 0).is_err());
    // Neither a smaller declaration nor a larger declaration can hide a
    // decompressed-length mismatch, even when both fit the caller's budget.
    assert!(codec.decode(&stored, 4_095, CURSOR_BYTES).is_err());
    assert!(codec.decode(&stored, 4_097, CURSOR_BYTES).is_err());
    assert_eq!(codec.decode(&stored, 4_096, 4_096)?, original);
    Ok(())
}

#[test]
fn encode_rejects_an_input_larger_than_the_existing_cursor_bound() {
    let mut codec = ScratchBlobCodec::new();
    assert!(codec.encode(vec![b'x'; CURSOR_BYTES + 1]).is_err());
}

#[test]
fn decode_rejects_corruption_truncation_trailing_bytes_and_concatenated_streams(
) -> anyhow::Result<()> {
    let mut codec = ScratchBlobCodec::new();
    let original = vec![b'x'; 4_096];
    let stored = codec.encode(original.clone())?;
    assert!(stored.len() * 2 < original.len());
    let mut bad_checksum = stored.clone();
    *bad_checksum.last_mut().context("zlib checksum")? ^= 0xff;
    let mut trailing = stored.clone();
    trailing.push(0);
    let mut concatenated = stored.clone();
    concatenated.extend_from_slice(&stored);
    for malformed in [
        Vec::new(),
        vec![0],
        stored[..1].to_vec(),
        stored[..stored.len() - 1].to_vec(),
        bad_checksum,
        trailing,
        concatenated,
    ] {
        assert!(
            codec
                .decode(&malformed, i64::try_from(original.len())?, CURSOR_BYTES)
                .is_err(),
            "a malformed or incomplete zlib stream must fail closed"
        );
        // Failed streams cannot contaminate the next independent scratch row.
        assert_eq!(
            codec.decode(&stored, i64::try_from(original.len())?, CURSOR_BYTES)?,
            original
        );
    }
    Ok(())
}
