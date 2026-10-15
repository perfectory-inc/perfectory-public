//! How many workers encode a HUB export's parts: as many as the cores, while the chunks they hold
//! fit the memory this process may use.
//!
//! Inflating the ZIP member and splitting its lines is one sequential stream; building the JSON
//! rows and gzipping them is what took one core 2h20m for the 202M-row price register (2026-09-08)
//! and still did on 2026-10-10 (one core at 85%, 1.8 MB/s out). Each part is one worker's, so the
//! parts are the bytes a single thread writes, and more cores are more parts at once. The limit is
//! memory: every worker holds whole chunks of `rows_per_part` rows (about 300 MB for 1M price rows).
//!
//! Neither number is written here. The cores are `std::thread::available_parallelism` (affinity
//! and the cgroup CPU quota included); the memory is what the process's cgroup and the host still
//! allow (`process_memory`), so the same binary sizes itself under the Silver lane unit's
//! `MemoryMax`, inside a container, or on a larger host.

use std::mem::size_of;

use crate::silver_handoff_io::R2_UPLOAD_PART_BYTES;

/// The memory one chunk holds: its row bytes and the row index, as the reader measured them.
pub(super) fn chunk_bytes(bytes: usize, rows: usize) -> u64 {
    let index = rows.saturating_mul(size_of::<(u64, usize)>());
    u64::try_from(bytes.saturating_add(index)).unwrap_or(u64::MAX)
}

/// What one worker holds besides its chunks while it writes a part: the multipart upload buffer
/// it fills before sending a part to R2, and one row's field list, JSON value and serialized line,
/// each bounded by `max_row_bytes`. The gzip state is a few hundred KiB and falls in the share.
pub(super) fn worker_overhead_bytes(max_row_bytes: u64) -> u64 {
    u64::try_from(R2_UPLOAD_PART_BYTES)
        .unwrap_or(u64::MAX)
        .saturating_add(max_row_bytes.saturating_mul(3))
}

/// The worker count for `cores` cores and `available` bytes, given the first chunk's measured
/// `chunk` bytes (later chunks of the same register are the same million rows of the same columns).
///
/// The channel holds up to `workers` chunks and each worker encodes one, so each worker accounts
/// for two chunks. The reader holds one more: while filling it the `Vec` may grow to twice its
/// length and shrinking it copies once more, three chunks at most. So, of the share of `available`:
///
/// `workers = min(cores, (share − 3·chunk) / (2·chunk + overhead))`, at least one.
///
/// With no known limit (`available` is `None`, a platform that publishes none) it runs one worker:
/// the one way that cannot hold more than the single-threaded export always did.
pub(super) fn export_workers(
    cores: usize,
    available: Option<u64>,
    chunk: u64,
    overhead: u64,
) -> usize {
    crate::process_memory::workers_that_fit(
        cores,
        available,
        chunk.saturating_mul(3),
        chunk.saturating_mul(2).saturating_add(overhead),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    #[test]
    fn cores_bound_the_workers_when_memory_is_plenty() {
        assert_eq!(export_workers(20, Some(256 * GIB), 300 * MIB, 20 * MIB), 20);
        assert_eq!(export_workers(4, Some(256 * GIB), 300 * MIB, 20 * MIB), 4);
    }

    #[test]
    fn memory_bounds_the_workers_when_chunks_are_large() {
        // The Silver lane unit: 16 GiB cap, about 1 GiB already used, 300 MiB chunks.
        // share = 15 GiB × 3/4 = 11,520 MiB; minus 900 MiB the reader holds = 10,620 MiB;
        // per worker 600 + 20 MiB → 17 workers, under the 20 cores.
        assert_eq!(export_workers(20, Some(15 * GIB), 300 * MIB, 20 * MIB), 17);
        // Twice the chunk, under half the workers: (11,520 − 1,800) / 1,220 MiB.
        assert_eq!(export_workers(20, Some(15 * GIB), 600 * MIB, 20 * MIB), 7);
    }

    #[test]
    fn the_formula_never_holds_more_than_the_share() {
        for available_gib in [1, 2, 4, 15, 64] {
            for chunk_mib in [1, 50, 300, 900] {
                let (available, chunk, overhead) = (available_gib * GIB, chunk_mib * MIB, 20 * MIB);
                let workers = export_workers(64, Some(available), chunk, overhead);
                let held =
                    3 * chunk + u64::try_from(workers).unwrap_or(u64::MAX) * (2 * chunk + overhead);
                assert!(
                    workers == 1 || held <= available / 4 * 3,
                    "{workers} workers hold {held} of {available}"
                );
            }
        }
    }

    #[test]
    fn at_least_one_worker_runs_and_unknown_memory_runs_one() {
        assert_eq!(export_workers(20, Some(MIB), 300 * MIB, 20 * MIB), 1);
        assert_eq!(export_workers(0, Some(256 * GIB), MIB, MIB), 1);
        assert_eq!(export_workers(20, None, MIB, MIB), 1);
    }

    #[test]
    fn a_chunk_counts_its_bytes_and_its_row_index() {
        assert_eq!(chunk_bytes(1000, 10), 1000 + 10 * 16);
        assert!(worker_overhead_bytes(MIB) >= 16 * MIB + 3 * MIB);
    }
}
