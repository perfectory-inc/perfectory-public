#!/usr/bin/env bash
# Runs Foundation's real Kafka/Schema Registry contract tests against the pinned local stack.
# The caller supplies DATABASE_URL (CI's PostGIS service or a disposable local Postgres). Kafka
# itself is always provisioned by this script so local and CI exercise the same broker contract.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
COMPOSE_FILE="$ROOT/platforms/intelligence-platform/docker/c2-event-backbone.compose.yml"
TOPIC="${FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC:-foundation-platform.catalog.collection-raw-written.v1}"
KAFKA_PORT="${INTELLIGENCE_TEST_KAFKA_HOST_PORT:-19092}"
KARAPACE_PORT="${INTELLIGENCE_TEST_KARAPACE_HOST_PORT:-18081}"
PROJECT="${FOUNDATION_KAFKA_COMPOSE_PROJECT:-foundation-kafka-live-${USER:-local}-${BASHPID}}"
PROJECT="$(printf '%s' "$PROJECT" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '-')"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "foundation-kafka-live: Docker Desktop/Engine is required" >&2
  exit 1
fi
if [ ! -f "$COMPOSE_FILE" ]; then
  echo "foundation-kafka-live: missing compose file $COMPOSE_FILE" >&2
  exit 1
fi
if [ -z "${DATABASE_URL:-}" ]; then
  echo "foundation-kafka-live: DATABASE_URL must point at a migrated Postgres/PostGIS database" >&2
  exit 2
fi

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "foundation-kafka-live: starting pinned Redpanda/Karapace project $PROJECT"
INTELLIGENCE_TEST_KAFKA_HOST_PORT="$KAFKA_PORT" \
  INTELLIGENCE_TEST_KARAPACE_HOST_PORT="$KARAPACE_PORT" \
  compose up -d

echo "foundation-kafka-live: waiting for Karapace HTTP endpoint"
ready=0
for _ in $(seq 1 90); do
  if curl --fail --silent --show-error "http://127.0.0.1:${KARAPACE_PORT}/subjects" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[ "$ready" = 1 ] || { echo "foundation-kafka-live: Karapace did not become ready" >&2; exit 1; }

echo "foundation-kafka-live: waiting for Redpanda and creating canonical topic"
ready=0
for _ in $(seq 1 90); do
  if compose exec -T redpanda rpk cluster info --brokers redpanda:9092 >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[ "$ready" = 1 ] || { echo "foundation-kafka-live: Redpanda did not become ready" >&2; exit 1; }

if ! compose exec -T redpanda rpk topic describe "$TOPIC" --brokers redpanda:9092 >/dev/null 2>&1; then
  compose exec -T redpanda rpk topic create "$TOPIC" \
    --brokers redpanda:9092 --partitions 1 --replicas 1
fi
compose exec -T redpanda rpk topic describe "$TOPIC" --brokers redpanda:9092

export FOUNDATION_TEST_KAFKA_REQUIRED=1
export FOUNDATION_PLATFORM_KAFKA_ENABLED=1
export FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${KAFKA_PORT}"
export FOUNDATION_PLATFORM_KAFKA_BOOTSTRAP_SERVERS="$FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS"
export FOUNDATION_TEST_KARAPACE_URL="http://127.0.0.1:${KARAPACE_PORT}"
export FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_URL="$FOUNDATION_TEST_KARAPACE_URL"
export FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC="$TOPIC"
export FOUNDATION_PLATFORM_RUNTIME_ENV="${FOUNDATION_PLATFORM_RUNTIME_ENV:-ci}"
export FOUNDATION_PLATFORM_EXECUTION_CONTEXT="${FOUNDATION_PLATFORM_EXECUTION_CONTEXT:-ci}"

# The lane owns WHICH targets run (ADR-0011). This script owns the stack they run
# against: compose up, readiness, topic creation, credentials, teardown. It used
# to also keep its own copy of the three target names, so the set the completeness
# guard proved and the set this harness executed were two statements of one fact.
# Dropping `--nocapture` is deliberate: ADR-0010 debt #9 records that the
# executed-count parser can be inflated by test-written stdout, and the lane does
# not pass that flag.
run_lane_host() {
  (cd "$ROOT" && cargo xtask integration foundation kafka)
}

run_lane_container() {
  local container_database_url="${FOUNDATION_KAFKA_TEST_CONTAINER_DATABASE_URL:-$DATABASE_URL}"
  # A developer commonly exposes disposable Postgres on localhost. From the Rust container,
  # Docker Desktop exposes that host as host.docker.internal. Container-network URLs can be
  # supplied explicitly through FOUNDATION_KAFKA_TEST_CONTAINER_DATABASE_URL.
  container_database_url="${container_database_url//127.0.0.1/host.docker.internal}"
  container_database_url="${container_database_url//localhost/host.docker.internal}"
  # shellcheck disable=SC1091
  source "$ROOT/tools/container-images.env"
  local repo_mount
  repo_mount="$(cd "$ROOT" && { pwd -W 2>/dev/null || pwd; })"
  MSYS_NO_PATHCONV=1 docker run --rm \
    --network "${PROJECT}_default" \
    --add-host host.docker.internal:host-gateway \
    -v "$repo_mount:/work" \
    -v perfectory-cargo-registry:/usr/local/cargo/registry \
    -v perfectory-rustup:/usr/local/rustup \
    -v perfectory-target-foundation:/work/platforms/foundation-platform/target \
    -v perfectory-target-xtask:/work/tools/xtask/target \
    -w /work \
    -e SQLX_OFFLINE=true \
    -e CARGO_TERM_COLOR=always \
    -e DATABASE_URL="$container_database_url" \
    -e FOUNDATION_TEST_KAFKA_REQUIRED=1 \
    -e FOUNDATION_PLATFORM_KAFKA_ENABLED=1 \
    -e FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    -e FOUNDATION_PLATFORM_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    -e FOUNDATION_TEST_KARAPACE_URL=http://karapace:8081 \
    -e FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_URL=http://karapace:8081 \
    -e FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC="$TOPIC" \
    -e FOUNDATION_PLATFORM_RUNTIME_ENV=local \
    -e FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer \
    "$RUST_TOOLCHAIN_IMAGE" bash -c \
    'apt-get update -qq && apt-get install -y -qq cmake >/dev/null && \
     PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
     cargo xtask integration foundation kafka'
}

echo "foundation-kafka-live: running the foundation kafka lane"
if command -v cargo >/dev/null 2>&1; then
  run_lane_host
else
  run_lane_container
fi

echo "foundation-kafka-live: PASS (Kafka, Karapace, Postgres outbox, and outage contracts)"
