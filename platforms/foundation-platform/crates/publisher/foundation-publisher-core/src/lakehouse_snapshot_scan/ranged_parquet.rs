//! Reads one Parquet data file by byte range: its footer first, then only the column chunks (and,
//! where the file carries an offset index, only the pages) of the row groups the scan decodes.
//!
//! A by-PNU shard used to download every data file it opened in full, though the row-group
//! statistics and the PNU filter then threw most of it away undecoded (root ADR-0319). The
//! Parquet layout puts everything a reader needs to plan in the footer, so this is what every
//! object-store Parquet reader does — Trino's `ParquetReader`, Spark's `ParquetFileReader`,
//! DuckDB's `httpfs` and arrow-rs `ParquetObjectReader`: read the footer, plan, read the planned
//! ranges. The planning is arrow-rs's own push decoder
//! ([`ParquetPushDecoder`](parquet::arrow::push_decoder::ParquetPushDecoder)), the IO-free core
//! under its async reader; this module only answers the byte ranges it asks for, so the row-group
//! pruning, the filter on the prefix column before any other column decodes, and the page
//! skipping are the decoder's, not a second implementation of them.

use std::{ops::Range, sync::Arc};

use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use lakehouse_domain::LakehouseTableContract;
use parquet::{
    arrow::push_decoder::ParquetPushDecoderBuilder,
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataPushDecoder},
    DecodeResult,
};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::{
    iceberg_scan::{self, ScannedDataFile},
    LakehouseByteReader,
};

/// Bytes read from the end of a file before its footer is decoded, so that the footer length, the
/// footer and usually the offset index arrive in one request instead of three. DataFusion's
/// `metadata_size_hint` defaults to the same 512 KiB; a footer longer than this costs one more
/// request, never a wrong read.
pub(super) const FOOTER_PREFETCH_BYTES: u64 = 512 * 1024;

/// Ranges closer than this are read as one request: a gap this small costs less to download than
/// a request does to make. arrow-rs `object_store` coalesces at the same 1 MiB.
pub(super) const COALESCE_GAP_BYTES: u64 = 1024 * 1024;

/// What one ranged decode read and decoded.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RangedDecode {
    /// Rows handed to `visit`.
    pub decoded: u64,
    /// Rows of the file never handed over (skipped row groups, rows filtered out under the
    /// prefix); with `decoded` they are the footer's row count when the decode reached the end.
    pub skipped: u64,
    /// Bytes the reader fetched for this file, footer included.
    pub fetched_bytes: u64,
    /// Range requests the reader made for this file, after coalescing.
    pub range_requests: u64,
}

/// Decodes the rows of `data_file` under `prefix` (see
/// [`iceberg_scan::decode_rows_under`], whose rows this returns for any file) reading only the
/// byte ranges the decode needs. The file's length is the one its manifest recorded: Iceberg
/// records the written length, and a footer read against another length fails rather than reads
/// the wrong bytes.
pub(super) async fn decode_rows_under(
    contract: &LakehouseTableContract,
    lakehouse: &impl LakehouseByteReader,
    data_file: &ScannedDataFile,
    prefix: Option<(&str, &str)>,
    mut visit: impl FnMut(JsonMap<String, JsonValue>) -> bool,
) -> anyhow::Result<RangedDecode> {
    let location = data_file.file_path.as_str();
    let mut fetch = RangeFetch {
        lakehouse,
        location,
        file_size: data_file.file_size_in_bytes,
        fetched_bytes: 0,
        range_requests: 0,
    };
    let metadata = Arc::new(fetch.metadata().await?);
    let file_rows = iceberg_scan::file_row_count(&metadata)?;
    let builder = ParquetPushDecoderBuilder::try_new_decoder(metadata)
        .with_context(|| format!("failed to plan the Parquet read of {location}"))?;
    let (builder, group_skipped) = iceberg_scan::keep_prefix(builder, prefix)?;
    let mut decoder = builder
        .build()
        .with_context(|| format!("failed to build the Parquet decoder of {location}"))?;

    let mut decoded: u64 = 0;
    loop {
        match decoder
            .try_decode()
            .with_context(|| format!("failed to decode {location}"))?
        {
            DecodeResult::NeedsData(ranges) => {
                let buffers = fetch.ranges(&ranges).await?;
                decoder
                    .push_ranges(ranges, buffers)
                    .with_context(|| format!("failed to hand {location} to its decoder"))?;
            }
            DecodeResult::Data(batch) => {
                if !iceberg_scan::visit_batch(contract, &batch, &mut decoded, &mut visit)? {
                    return Ok(fetch.decoded(decoded, group_skipped));
                }
            }
            DecodeResult::Finished => break,
        }
    }
    let skipped = iceberg_scan::rows_not_handed_over(file_rows, decoded)?;
    Ok(fetch.decoded(decoded, skipped))
}

/// The ranged reads of one data file, and what they cost.
struct RangeFetch<'a, R> {
    lakehouse: &'a R,
    location: &'a str,
    file_size: u64,
    fetched_bytes: u64,
    range_requests: u64,
}

impl<R: LakehouseByteReader> RangeFetch<'_, R> {
    /// The footer, and the offset index when the file carries one: it is what lets the decoder
    /// read only the pages holding the rows the filter keeps. The column index is not read; the
    /// row-group statistics in the footer are what the scan prunes by.
    async fn metadata(&mut self) -> anyhow::Result<ParquetMetaData> {
        let location = self.location;
        let mut decoder = ParquetMetaDataPushDecoder::try_new(self.file_size)
            .with_context(|| format!("{location} is too short to be a Parquet file"))?
            .with_column_index_policy(PageIndexPolicy::Skip)
            .with_offset_index_policy(PageIndexPolicy::Optional);
        let tail = self.file_size.saturating_sub(FOOTER_PREFETCH_BYTES)..self.file_size;
        let [bytes] = self
            .ranges(std::slice::from_ref(&tail))
            .await?
            .try_into()
            .map_err(|_| anyhow::anyhow!("one range read returned another number of buffers"))?;
        decoder.push_range(tail, bytes)?;
        loop {
            match decoder.try_decode().with_context(|| {
                format!(
                    "failed to read the Parquet footer of {location} ({} bytes by its manifest)",
                    self.file_size
                )
            })? {
                DecodeResult::NeedsData(ranges) => {
                    let buffers = self.ranges(&ranges).await?;
                    decoder.push_ranges(ranges, buffers)?;
                }
                DecodeResult::Data(metadata) => return Ok(metadata),
                DecodeResult::Finished => {
                    bail!("the Parquet footer of {location} decoded to nothing")
                }
            }
        }
    }

    /// Reads `wanted`, each range as its own buffer, in as few requests as the ranges' gaps allow.
    async fn ranges(&mut self, wanted: &[Range<u64>]) -> anyhow::Result<Vec<Bytes>> {
        let mut buffers = vec![Bytes::new(); wanted.len()];
        for request in coalesce(wanted) {
            ensure!(
                request.range.end <= self.file_size,
                "the decoder asked for bytes {}..{} of {}, past its {} bytes",
                request.range.start,
                request.range.end,
                self.location,
                self.file_size
            );
            let fetched = Bytes::from(
                self.lakehouse
                    .read_range(self.location, request.range.clone())
                    .await?,
            );
            let length = request.range.end - request.range.start;
            ensure!(
                u64::try_from(fetched.len()).ok() == Some(length),
                "read {} bytes of {} for range {}..{}, expected {length}",
                fetched.len(),
                self.location,
                request.range.start,
                request.range.end
            );
            self.fetched_bytes = self
                .fetched_bytes
                .checked_add(length)
                .context("fetched byte count overflow")?;
            self.range_requests += 1;
            for index in request.members {
                let range = &wanted[index];
                let start = usize::try_from(range.start - request.range.start)?;
                let end = usize::try_from(range.end - request.range.start)?;
                buffers[index] = fetched.slice(start..end);
            }
        }
        Ok(buffers)
    }

    const fn decoded(&self, decoded: u64, skipped: u64) -> RangedDecode {
        RangedDecode {
            decoded,
            skipped,
            fetched_bytes: self.fetched_bytes,
            range_requests: self.range_requests,
        }
    }
}

/// One request covering several wanted ranges.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct CoalescedRange {
    pub(super) range: Range<u64>,
    /// Indexes into the wanted ranges this request serves.
    pub(super) members: Vec<usize>,
}

/// Merges ranges that overlap or lie within [`COALESCE_GAP_BYTES`] of each other into one request.
/// Each wanted range is still handed to the decoder exactly as it asked: the decoder frees a
/// buffer only when it matches a range it requested.
pub(super) fn coalesce(wanted: &[Range<u64>]) -> Vec<CoalescedRange> {
    let mut order = (0..wanted.len())
        .filter(|&index| wanted[index].start < wanted[index].end)
        .collect::<Vec<_>>();
    order.sort_by_key(|&index| (wanted[index].start, wanted[index].end));
    let mut requests: Vec<CoalescedRange> = Vec::new();
    for index in order {
        let range = &wanted[index];
        match requests.last_mut() {
            Some(last) if range.start <= last.range.end.saturating_add(COALESCE_GAP_BYTES) => {
                last.range.end = last.range.end.max(range.end);
                last.members.push(index);
            }
            _ => requests.push(CoalescedRange {
                range: range.clone(),
                members: vec![index],
            }),
        }
    }
    requests
}
