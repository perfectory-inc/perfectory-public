use anyhow::{bail, Context};

/// Value passed for `allow_partial_page_window` by the async collection lanes
/// (`national_data_collection_async`), which collect page-window SHARD FRAGMENTS rather than a
/// full provider scope. With this set, [`assert_page_window_slice_complete`] intentionally does
/// NOT assert full provider coverage: a single async run only ever sees one shard/window, so it
/// cannot — and must not — claim "전국 누락 없음" (no national gaps).
///
/// **The single arbiter of full national completeness is the national coverage manifest**
/// (`national_bronze_object_manifest`, checked by `check-national-bronze-object-manifest`). An
/// async run's own evidence must never be read as a completeness claim; only the manifest check
/// asserts the full scope was collected with no gaps.
///
/// The sync `ingest-building-register` path does NOT use this constant — it passes its own
/// runtime `config.allow_partial_page_window` and, when that is `false`, runs the full coverage
/// check here (the check that bails on `max_pages=1`).
pub const ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST: bool = true;

pub fn assert_page_window_complete(
    source_name: &str,
    last_page: u32,
    page_size: u32,
    last_page_logical_record_count: u64,
    fetched_record_total: u64,
    provider_total_count: Option<u64>,
    max_pages: u32,
) -> anyhow::Result<()> {
    assert_page_window_slice_complete(
        source_name,
        last_page,
        page_size,
        last_page_logical_record_count,
        fetched_record_total,
        provider_total_count,
        max_pages,
        false,
    )
}

/// Asserts a paginated provider window was fully collected.
///
/// `last_page_logical_record_count` is the record count of the FINAL page only (used as the
/// end-of-data heuristic when the provider gives no total). `fetched_record_total` is the sum
/// of logical records across every fetched page — it is compared against `provider_total_count`
/// so a short or empty MIDDLE page (page coverage arithmetic satisfied, but records actually
/// missing) is rejected instead of silently recorded as a complete success. Over-fetching
/// (more records than the provider total, e.g. rows duplicated across page boundaries) is
/// tolerated because it is not data loss; only a deficit fails the window.
///
/// When `allow_partial_page_window == true` this guard returns `Ok(())` immediately, after the
/// invariant checks (`page_size`/`max_pages`), WITHOUT asserting provider coverage. That is the
/// SHARD-FRAGMENT mode used by the async lanes, which pass
/// [`ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST`]: an async run collects one window/shard and
/// is *not* a full-scope claim. **Full "전국 누락 없음" completeness is asserted only by the
/// national coverage manifest (`check-national-bronze-object-manifest`)**, never by an individual
/// async run's evidence. The sync path passes `false` here and gets the full coverage check.
pub fn assert_page_window_slice_complete(
    source_name: &str,
    last_page: u32,
    page_size: u32,
    last_page_logical_record_count: u64,
    fetched_record_total: u64,
    provider_total_count: Option<u64>,
    max_pages: u32,
    allow_partial_page_window: bool,
) -> anyhow::Result<()> {
    if page_size == 0 {
        bail!("{source_name} page_size must be greater than zero");
    }
    if max_pages == 0 {
        bail!("{source_name} max_pages must be greater than zero");
    }
    if allow_partial_page_window {
        // Shard-fragment mode: do NOT assert provider coverage here. Full national completeness is
        // the national coverage manifest's job (check-national-bronze-object-manifest), not this
        // per-run guard. See ASYNC_SHARD_WINDOW_DEFERS_TO_COVERAGE_MANIFEST.
        return Ok(());
    }

    if let Some(total_count) = provider_total_count {
        let covered_rows = u64::from(last_page)
            .checked_mul(u64::from(page_size))
            .context("page coverage calculation overflowed")?;
        if covered_rows < total_count {
            bail!(
                "{source_name} page cap exhausted before provider result set was complete: \
                 last_page={last_page} page_size={page_size} provider_total_count={total_count} \
                 fetched_record_total={fetched_record_total} max_pages={max_pages}"
            );
        }
        // Page coverage is sufficient; verify the records were actually delivered. A short or
        // empty middle page leaves the window covered on paper but missing rows.
        if fetched_record_total < total_count {
            bail!(
                "{source_name} provider reported {total_count} records but only \
                 {fetched_record_total} were fetched across the window (short or missing page): \
                 last_page={last_page} page_size={page_size} max_pages={max_pages}"
            );
        }
        return Ok(());
    }

    if last_page_logical_record_count < u64::from(page_size) {
        return Ok(());
    }

    bail!(
        "{source_name} page cap exhausted without a provider total count or short final page: \
         last_page={last_page} page_size={page_size} \
         last_page_logical_record_count={last_page_logical_record_count} max_pages={max_pages}"
    );
}

#[cfg(test)]
mod tests {
    use super::{assert_page_window_complete, assert_page_window_slice_complete};

    #[test]
    fn allows_provider_total_count_covered_with_all_records_fetched() -> anyhow::Result<()> {
        // last page has 50 rows; summed across 3 pages = 250 == provider total.
        assert_page_window_complete("source", 3, 100, 50, 250, Some(250), 3)
    }

    #[test]
    fn allows_short_final_page_without_provider_total_count() -> anyhow::Result<()> {
        assert_page_window_complete("source", 3, 100, 25, 225, None, 3)
    }

    #[test]
    fn allows_explicit_partial_page_window() -> anyhow::Result<()> {
        assert_page_window_slice_complete("source", 50, 100, 100, 5_000, Some(9_845), 50, true)
    }

    #[test]
    fn rejects_implicit_partial_page_window() -> anyhow::Result<()> {
        let error = match assert_page_window_slice_complete(
            "source",
            50,
            100,
            100,
            5_000,
            Some(9_845),
            50,
            false,
        ) {
            Ok(()) => anyhow::bail!("implicit partial page window must fail"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("page cap exhausted"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn rejects_short_middle_page_that_loses_records() -> anyhow::Result<()> {
        // Coverage arithmetic is satisfied (10*100 >= 1000) but a short middle page means only
        // 920 of 1000 provider records were actually fetched — this must not record success.
        let error = match assert_page_window_complete(
            "building-register",
            10,
            100,
            100,
            920,
            Some(1_000),
            10,
        ) {
            Ok(()) => anyhow::bail!("short middle page must fail the completeness guard"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("only 920"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn allows_over_fetch_beyond_provider_total() -> anyhow::Result<()> {
        // Duplicate rows across page boundaries can yield more records than the provider total;
        // that is not data loss, so the window is accepted.
        assert_page_window_complete("source", 10, 100, 100, 1_010, Some(1_000), 10)
    }

    #[test]
    fn rejects_invalid_zero_page_size() -> anyhow::Result<()> {
        let error = match assert_page_window_complete("source", 1, 0, 0, 0, Some(0), 1) {
            Ok(()) => anyhow::bail!("zero page size must fail"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("page_size"),
            "unexpected error: {error}"
        );
        Ok(())
    }
}
