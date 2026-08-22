//! Live `PostgreSQL` behaviour of the paged industrial-complex search.
//!
//! These assertions cannot be made without a database: what is being checked is what Postgres does
//! with `ILIKE … ESCAPE`, with a `LEFT JOIN` against an empty page, and with a Korean substring —
//! none of which a Rust-side unit test can answer. The bounds themselves are unit-tested in
//! `catalog_application::complex_search`.
//!
//! Every fixture name carries a run-unique suffix, so the suite is correct against an empty CI
//! database and against a workstation database already carrying the 1,448 canonical rows.

#![allow(clippy::expect_used, clippy::print_stderr, clippy::unwrap_used)]

use catalog_application::complex_search::{
    ComplexSearchPaging, ComplexSearchQuery, ComplexSearchSort, ComplexSearchText, SidoCodeFilter,
};
use catalog_application::ports::CatalogRepository;
use catalog_domain::IndustrialComplexStatus;
use catalog_infrastructure::PgCatalogRepository;
use sqlx::PgPool;
use uuid::Uuid;

/// Connection for this `#[ignore]`d live suite. Both failure modes abort the test: a
/// configured-but-unreachable database must not silently downgrade a contract test into a no-op
/// that still reports success.
async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation`");
    PgPool::connect(&url)
        .await
        .expect("connect to the database in DATABASE_URL")
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn industrial_complex_search_reads_the_canonical_table() {
    let pool = pool().await;
    let repo = PgCatalogRepository::new(pool.clone());
    let fixture = SearchFixture::new();
    fixture.insert(&pool).await;

    assert_korean_substring_matches_inside_a_name(&repo, &fixture).await;
    assert_official_code_substring_matches(&repo, &fixture).await;
    assert_caller_wildcards_are_literal(&repo).await;
    assert_filters_narrow_the_page(&repo, &fixture).await;
    assert_a_page_past_the_end_still_reports_the_total(&repo, &fixture).await;
    assert_name_order_is_total_across_pages(&repo, &fixture).await;

    fixture.cleanup(&pool).await;
}

/// `반월` sits in the middle of every fixture name, so a match proves substring matching on Korean
/// text rather than a prefix comparison that happened to succeed.
async fn assert_korean_substring_matches_inside_a_name(
    repo: &PgCatalogRepository,
    fixture: &SearchFixture,
) {
    let page = repo
        .search_complexes(&search(Some("반월"), 100))
        .await
        .expect("korean substring search");

    let names = page
        .rows
        .iter()
        .map(|complex| complex.name.as_str())
        .collect::<Vec<_>>();
    assert!(
        names.contains(&fixture.banwol_first.as_str()),
        "{:?} must contain {}",
        names,
        fixture.banwol_first
    );
    assert!(
        names.contains(&fixture.banwol_second.as_str()),
        "{:?} must contain {}",
        names,
        fixture.banwol_second
    );
    assert!(
        !names.contains(&fixture.gangneung.as_str()),
        "{:?} must not contain {}",
        names,
        fixture.gangneung
    );

    // Scoped to this run so the count is exact on a database that also holds the real 반월 rows.
    let scoped = repo
        .search_complexes(&search(Some(&format!("반월-{}", fixture.suffix)), 100))
        .await
        .expect("run-scoped korean search");
    assert_eq!(scoped.total, 2);
    assert_eq!(scoped.rows.len(), 2);
}

async fn assert_official_code_substring_matches(
    repo: &PgCatalogRepository,
    fixture: &SearchFixture,
) {
    let page = repo
        .search_complexes(&search(Some(&fixture.suffix), 100))
        .await
        .expect("official code search");

    assert_eq!(page.total, 3, "the suffix is in all three fixture codes");
}

/// A caller typing `%` means the character, not "every row".
async fn assert_caller_wildcards_are_literal(repo: &PgCatalogRepository) {
    let page = repo
        .search_complexes(&search(Some("%"), 100))
        .await
        .expect("wildcard search");

    // Disabling experiment: bind `format!("%{q}%")` instead of `ComplexSearchText::contains_pattern`
    // and this becomes the whole canonical table.
    assert_eq!(
        page.total, 0,
        "no canonical name or code contains a literal percent sign"
    );
}

async fn assert_filters_narrow_the_page(repo: &PgCatalogRepository, fixture: &SearchFixture) {
    let mut query = search(Some(&fixture.suffix), 100);
    query.sido_code = Some(SidoCodeFilter::try_new("41").expect("two digits"));
    let by_sido = repo.search_complexes(&query).await.expect("sido filter");
    assert_eq!(by_sido.total, 2, "two fixtures resolve to 경기도");

    let mut query = search(Some(&fixture.suffix), 100);
    query.statuses = vec![IndustrialComplexStatus::Planned];
    let by_status = repo.search_complexes(&query).await.expect("status filter");
    assert_eq!(by_status.total, 1);
    assert_eq!(by_status.rows[0].name, fixture.gangneung);

    let mut query = search(Some(&fixture.suffix), 100);
    query.statuses = vec![
        IndustrialComplexStatus::Planned,
        IndustrialComplexStatus::Operating,
    ];
    let by_statuses = repo
        .search_complexes(&query)
        .await
        .expect("multi status filter");
    assert_eq!(by_statuses.total, 2);
}

/// A page past the end is empty, and the collection behind it is not.
async fn assert_a_page_past_the_end_still_reports_the_total(
    repo: &PgCatalogRepository,
    fixture: &SearchFixture,
) {
    let mut query = search(Some(&fixture.suffix), 20);
    query.paging = ComplexSearchPaging::try_new(Some(9), Some(20)).expect("bounded size");

    let page = repo.search_complexes(&query).await.expect("far page");

    assert!(page.rows.is_empty());
    // Disabling experiment: replace the `total` CTE with `COUNT(*) OVER ()` on the page and this
    // reports 0 — a screen would then say "0 곳" for a collection holding three.
    assert_eq!(page.total, 3);
    assert!(!query.paging.has_next(page.total));
}

/// Paging with a size of one must visit each row exactly once, including the two that share a name.
async fn assert_name_order_is_total_across_pages(
    repo: &PgCatalogRepository,
    fixture: &SearchFixture,
) {
    let mut seen = Vec::new();
    for page_number in 0..3 {
        let mut query = search(Some(&fixture.suffix), 1);
        query.paging = ComplexSearchPaging::try_new(Some(page_number), Some(1)).expect("size 1");
        let page = repo
            .search_complexes(&query)
            .await
            .expect("single-row page");
        assert_eq!(page.total, 3);
        assert_eq!(page.rows.len(), 1);
        seen.push(page.rows[0].official_complex_code.clone());
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 3, "every fixture appears on exactly one page");
}

/// One search at the default order.
fn search(text: Option<&str>, size: u32) -> ComplexSearchQuery {
    ComplexSearchQuery {
        text: text.map(|value| ComplexSearchText::try_new(value).expect("a word")),
        sido_code: None,
        statuses: Vec::new(),
        paging: ComplexSearchPaging::try_new(Some(0), Some(size)).expect("bounded size"),
        sort: ComplexSearchSort::Name,
    }
}

/// One canonical row this run inserts.
struct FixtureRow<'a> {
    name: &'a str,
    kind: &'a str,
    sido_code: &'a str,
    status: &'a str,
}

struct SearchFixture {
    suffix: String,
    banwol_first: String,
    banwol_second: String,
    gangneung: String,
    codes: Vec<String>,
    ids: Vec<Uuid>,
}

impl SearchFixture {
    fn new() -> Self {
        let suffix = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .filter(char::is_ascii_digit)
            .chain("0000000000".chars())
            .take(10)
            .collect::<String>();
        // The two 반월 rows deliberately share a name: without the `official_complex_code, id`
        // tiebreak in the ORDER BY, they are exactly the pair Postgres may reorder between pages.
        let shared = format!("테스트반월-{suffix}단지");
        Self {
            banwol_first: shared.clone(),
            banwol_second: shared,
            gangneung: format!("테스트강릉-{suffix}단지"),
            codes: vec![
                format!("T1{suffix}"),
                format!("T2{suffix}"),
                format!("T3{suffix}"),
            ],
            ids: vec![Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()],
            suffix,
        }
    }

    /// The three rows this run inserts, in the order their codes and ids are held.
    const fn rows(&self) -> [FixtureRow<'_>; 3] {
        [
            FixtureRow {
                name: self.banwol_first.as_str(),
                kind: "national",
                sido_code: "41",
                status: "operating",
            },
            FixtureRow {
                name: self.banwol_second.as_str(),
                kind: "general",
                sido_code: "41",
                status: "developing",
            },
            FixtureRow {
                name: self.gangneung.as_str(),
                kind: "general",
                sido_code: "51",
                status: "planned",
            },
        ]
    }

    async fn insert(&self, pool: &PgPool) {
        for (index, row) in self.rows().into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO catalog.industrial_complex
                 (id, official_complex_code, name, kind, area_m2, sido_code, status, version)
                 VALUES ($1, $2, $3, $4, 1000, $5, $6, 1)",
            )
            .bind(self.ids[index])
            .bind(&self.codes[index])
            .bind(row.name)
            .bind(row.kind)
            .bind(row.sido_code)
            .bind(row.status)
            .execute(pool)
            .await
            .expect("insert search fixture complex");
        }
    }

    async fn cleanup(&self, pool: &PgPool) {
        sqlx::query("DELETE FROM catalog.industrial_complex WHERE id = ANY($1)")
            .bind(&self.ids)
            .execute(pool)
            .await
            .expect("delete search fixture complexes");
    }
}
