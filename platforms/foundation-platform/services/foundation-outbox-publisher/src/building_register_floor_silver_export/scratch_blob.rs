//! Private SQLAR-style storage for already-serialized floor scratch bytes.
//! Equal stored/original lengths mean raw bytes; shorter BLOBs are zlib streams.
use anyhow::{ensure, Context};
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};

use crate::serving_scratch::CURSOR_BYTES;

pub(super) struct ScratchBlobCodec {
    compressor: Compress,
    decompressor: Decompress,
    output: [u8; 8 * 1024],
}

impl ScratchBlobCodec {
    pub(super) fn new() -> Self {
        Self {
            compressor: Compress::new(Compression::fast(), true),
            decompressor: Decompress::new(true),
            output: [0; 8 * 1024],
        }
    }

    pub(super) fn encode(&mut self, original: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        ensure!(
            original.len() <= CURSOR_BYTES,
            "floor scratch BLOB exceeds byte bound"
        );
        self.compressor.reset();
        // Never reserve a compress-bound buffer larger than the input. Once
        // compression cannot save space, retain the original allocation.
        let mut stored = Vec::with_capacity(original.len());
        loop {
            let available = (original.len() - stored.len()).min(self.output.len());
            if available == 0 {
                return Ok(original);
            }
            let consumed = usize::try_from(self.compressor.total_in())?;
            let previous_output = self.compressor.total_out();
            let status = self.compressor.compress(
                &original[consumed..],
                &mut self.output[..available],
                FlushCompress::Finish,
            )?;
            let produced = usize::try_from(self.compressor.total_out() - previous_output)?;
            stored.extend_from_slice(&self.output[..produced]);
            if stored.len() == original.len() {
                return Ok(original);
            }
            if status == Status::StreamEnd {
                ensure!(
                    self.compressor.total_in() == original.len() as u64,
                    "floor scratch compression did not consume all input"
                );
                return Ok(stored);
            }
            ensure!(
                self.compressor.total_in() > consumed as u64 || produced > 0,
                "floor scratch compression made no progress"
            );
        }
    }

    pub(super) fn decode(
        &mut self,
        stored: &[u8],
        original_bytes: i64,
        maximum_bytes: usize,
    ) -> anyhow::Result<Vec<u8>> {
        let original_bytes = usize::try_from(original_bytes)
            .context("invalid floor scratch original BLOB length")?;
        // Validate both lengths before allocating, including the remaining
        // combined Silver/proposal budget supplied by the caller.
        ensure!(
            original_bytes <= maximum_bytes && original_bytes <= CURSOR_BYTES,
            "floor scratch original BLOB exceeds byte bound"
        );
        ensure!(
            stored.len() <= original_bytes,
            "floor scratch stored BLOB exceeds original length"
        );
        if stored.len() == original_bytes {
            return Ok(stored.to_vec());
        }
        self.decompressor.reset(true);
        let mut original = Vec::with_capacity(original_bytes);
        loop {
            let consumed = usize::try_from(self.decompressor.total_in())?;
            let previous_output = self.decompressor.total_out();
            let remaining = original_bytes - original.len();
            // One extra byte detects a false original length without growing
            // the declared-size allocation. It also lets the zlib trailer be
            // consumed after the exact output length has been reached.
            let available = self.output.len().min(remaining + 1);
            let status = self
                .decompressor
                .decompress(
                    &stored[consumed..],
                    &mut self.output[..available],
                    FlushDecompress::None,
                )
                .context("invalid floor scratch zlib stream")?;
            let produced = usize::try_from(self.decompressor.total_out() - previous_output)?;
            ensure!(
                produced <= remaining,
                "floor scratch decoded BLOB exceeds original length"
            );
            original.extend_from_slice(&self.output[..produced]);
            if status == Status::StreamEnd {
                ensure!(
                    self.decompressor.total_in() == stored.len() as u64,
                    "floor scratch zlib stream has trailing bytes"
                );
                ensure!(
                    original.len() == original_bytes,
                    "floor scratch decoded BLOB differs from original length"
                );
                return Ok(original);
            }
            ensure!(
                self.decompressor.total_in() > consumed as u64 || produced > 0,
                "incomplete floor scratch zlib stream"
            );
        }
    }
}

#[cfg(test)]
#[path = "scratch_blob_tests.rs"]
mod tests;
