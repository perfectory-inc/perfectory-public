// Fixture for tools/semgrep/rules/health-routes.yml (`opengrep scan --test`). Not compiled.
fn router() -> Router {
    Router::new()
        // ok: health-routes-are-healthz-and-readyz
        .route("/healthz", get(healthz))
        // ok: health-routes-are-healthz-and-readyz
        .route("/readyz", get(readyz))
        // ok: health-routes-are-healthz-and-readyz
        .route("/readyz/postgres", get(ready_postgres))
}

fn drifted() -> Router {
    let router = Router::new();
    // ruleid: health-routes-are-healthz-and-readyz
    let router = router.route("/health", get(health));
    // ruleid: health-routes-are-healthz-and-readyz
    let router = router.route("/health/live", get(live));
    // ruleid: health-routes-are-healthz-and-readyz
    let router = router.route("/health/ready", get(ready));
    // ruleid: health-routes-are-healthz-and-readyz
    let router = router.route("/ready", get(ready));
    // ruleid: health-routes-are-healthz-and-readyz
    router.route("/healthz/ready", get(ready))
}
