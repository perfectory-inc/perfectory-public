#!/usr/bin/env bash
# Mechanical boundary for the Foundation Kafka rollout.
# Prevents collection code, live-test gating, and CI cleanup from drifting apart.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$root"
fail=0

collection_matches=$(git grep -n -E 'rdkafka|KafkaEventBroadcaster|FOUNDATION_PLATFORM_KAFKA' -- \
  platforms/foundation-platform/crates/collection || true)
if [ -n "$collection_matches" ]; then
  echo 'FAIL foundation-kafka-contract: collection crates must not publish directly to Kafka' >&2
  printf '%s\n' "$collection_matches" >&2
  fail=1
fi

adapter='platforms/foundation-platform/crates/foundation-outbox/src/kafka_broadcaster.rs'
if ! grep -Fq 'allow.auto.create.topics", "false' "$adapter"; then
  echo 'FAIL foundation-kafka-contract: Kafka producer must disable auto topic creation' >&2
  fail=1
fi
if ! grep -Fq 'catalog.collection.raw_written.v1' "$adapter"; then
  echo 'FAIL foundation-kafka-contract: adapter lost the canonical raw_written event type' >&2
  fail=1
fi
if ! grep -Fq 'foundation-platform.catalog.collection-raw-written.v1' "$adapter"; then
  echo 'FAIL foundation-kafka-contract: adapter lost the canonical topic' >&2
  fail=1
fi
if ! grep -Fq 'include_str!("../../../schemas/foundation-platform.catalog.collection-raw-written.v1.avsc")' "$adapter"; then
  echo 'FAIL foundation-kafka-contract: adapter must use the checked-in Avro schema' >&2
  fail=1
fi

schema='platforms/foundation-platform/schemas/foundation-platform.catalog.collection-raw-written.v1.avsc'
if ! grep -Eq '"name"[[:space:]]*:[[:space:]]*"event_id"' "$schema" \
  || ! grep -Eq '"name"[[:space:]]*:[[:space:]]*"bronze_object_key"' "$schema"; then
  echo 'FAIL foundation-kafka-contract: raw_written schema must retain event identity and R2 claim-check fields' >&2
  fail=1
fi

for test_file in \
  platforms/foundation-platform/crates/foundation-outbox/tests/live_kafka_karapace.rs \
  platforms/foundation-platform/crates/foundation-outbox/tests/live_kafka_outbox_roundtrip.rs \
  platforms/foundation-platform/crates/foundation-outbox/tests/live_kafka_outage.rs; do
  if ! grep -Fq '#[ignore' "$test_file"; then
    echo "FAIL foundation-kafka-contract: live test must remain ignored by default: $test_file" >&2
    fail=1
  fi
  if ! grep -Fq 'FOUNDATION_TEST_KAFKA_REQUIRED' "$test_file"; then
    echo "FAIL foundation-kafka-contract: live test lacks required-mode credential gate: $test_file" >&2
    fail=1
  fi
done

workflow='.github/workflows/foundation-ci.yml'
for test_name in live_kafka_karapace live_kafka_outbox_roundtrip live_kafka_outage; do
  if ! grep -Fq "$test_name" scripts/verify/foundation-kafka-live.sh; then
    echo "FAIL foundation-kafka-contract: CI harness does not run $test_name" >&2
    fail=1
  fi
done
if ! grep -Fq 'consumer_contract_deduplicates_event_id_and_reads_bronze_claim_check' \
  platforms/foundation-platform/crates/foundation-outbox/tests/kafka_broadcaster_contract.rs; then
  echo 'FAIL foundation-kafka-contract: consumer claim-check/dedup contract test is missing' >&2
  fail=1
fi
for required_text in \
  'kafka-integration:' \
  'scripts/verify/foundation-kafka-live.sh' \
  'if: always()' \
  'FOUNDATION_TEST_KAFKA_REQUIRED'; do
  if ! grep -Fq "$required_text" "$workflow"; then
    echo "FAIL foundation-kafka-contract: Foundation CI is missing $required_text" >&2
    fail=1
  fi
done

harness='scripts/verify/foundation-kafka-live.sh'
for required_text in \
  'trap cleanup EXIT' \
  'compose down -v --remove-orphans' \
  'rpk topic create' \
  'FOUNDATION_TEST_KAFKA_REQUIRED=1' \
  'cargo test --locked'; do
  if ! grep -Fq "$required_text" "$harness"; then
    echo "FAIL foundation-kafka-contract: live harness is missing $required_text" >&2
    fail=1
  fi
done

if [ "$fail" -eq 0 ]; then
  echo 'OK foundation-kafka-contract'
fi
exit "$fail"
