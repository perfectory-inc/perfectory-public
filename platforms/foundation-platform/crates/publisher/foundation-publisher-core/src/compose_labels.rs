//! The Compose labels a job's per-invocation containers carry, so its cleanup can find them
//! (root ADR-0200: the Silver lane's cleanup and the tile lane's tool containers both use them).

/// The label key Compose names a container's project with; the cleanup lists by it.
pub const PROJECT_LABEL: &str = "com.docker.compose.project";
/// The label Compose puts on a `run` container; the cleanup removes only those.
pub const ONEOFF_LABEL: &str = "com.docker.compose.oneoff=True";
