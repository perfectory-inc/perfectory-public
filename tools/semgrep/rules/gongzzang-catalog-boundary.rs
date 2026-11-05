// Fixture for tools/semgrep/rules/gongzzang-catalog-boundary.yml (`opengrep scan --test`). Not
// compiled. The crate-directory rule is a path rule, proven by planting a file under one of those
// directories (scripts/guard/semgrep-rules.sh); `--test` does not apply paths.
const PARCELS: &'static str = "/api/v1/parcels";
static BUILDINGS: &str = "/api/v1/buildings";
const LISTINGS: &str = "/api/v1/listings";

fn router() -> Router {
    Router::new()
        // ok: gongzzang-catalog-routes-are-get-or-head
        .route("/api/v1/parcels/{pnu}", get(show).head(show))
        // ok: gongzzang-catalog-routes-are-get-or-head
        .route("/api/v1/parcel-summary", get(post_user))
        // ok: gongzzang-catalog-routes-are-get-or-head
        .route(LISTINGS, post(create_listing))
        // ruleid: gongzzang-catalog-routes-are-get-or-head
        .route("/api/v1/parcels", post(create))
        // ruleid: gongzzang-catalog-routes-are-get-or-head
        .route("/api/v1/industrial-complexes", get(list).put(replace))
        // ruleid: gongzzang-catalog-routes-are-get-or-head
        .route(PARCELS, axum::routing::delete(remove))
        // ruleid: gongzzang-catalog-routes-are-get-or-head
        .route(BUILDINGS, patch(update))
        // ruleid: gongzzang-catalog-routes-are-get-or-head
        .route("/api/v1/manufacturers", MethodRouter::new().on(MethodFilter::POST, create))
        // ruleid: gongzzang-catalog-paths-are-not-opaque-services
        .nest("/api/v1/parcels", catalog_router())
        // ruleid: gongzzang-catalog-paths-are-not-opaque-services
        .route_service("/api/v1/buildings/{id}", catalog_service())
        // ruleid: gongzzang-catalog-paths-are-not-opaque-services
        .nest_service(BUILDINGS, catalog_service())
        // ok: gongzzang-catalog-paths-are-not-opaque-services
        .nest("/api/v1/listings", listing_router())
}

async fn write(pool: &PgPool) {
    // ruleid: gongzzang-writes-no-catalog-table
    sqlx::query!("INSERT INTO catalog.parcels (pnu) VALUES ($1)", "9999910100100010000");
    // ruleid: gongzzang-writes-no-catalog-table
    let statement = r#"UPDATE
        "building" SET name = $1"#;
    // ruleid: gongzzang-writes-no-catalog-table
    sqlx::query("DELETE FROM industrial_complexes WHERE id = $1");
    // ok: gongzzang-writes-no-catalog-table
    sqlx::query("INSERT INTO listings (pnu) VALUES ($1)");
    // ok: gongzzang-writes-no-catalog-table
    sqlx::query("SELECT pnu FROM catalog.parcels");
}
