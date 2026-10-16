#!/usr/bin/env bash
# Proves r2-reads-have-one-path refuses what it exists to refuse (root ADR-0261): a Rust S3 client
# built outside its area's place, a Python S3 client import, an s3a setting in a Spark job, and an
# S3 client in a Worker — each planted alone in a fixture repository. The clean fixture, a client
# inside its place, a test asserting an s3a key and a comment naming a client must stay accepted.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/r2-reads-have-one-path.sh"
name="r2-reads-have-one-path-self-test"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/tools"
cp "$root/tools/r2-read-paths.contract.json" "$fixture/tools/"
git -C "$fixture" init -q
git -C "$fixture" add .

plant() {
  local path="$1" text="$2"
  mkdir -p "$(dirname "$fixture/$path")"
  printf '%s\n' "$text" >"$fixture/$path"
  git -C "$fixture" add "$path"
}
unplant() {
  git -C "$fixture" rm -q --cached "$1"
  rm -f "$fixture/$1"
}
expect() {
  local verdict="$1" label="$2" path="$3" text="$4" output
  plant "$path" "$text"
  if output="$(bash "$checker" "$fixture" 2>&1)"; then
    [[ "$verdict" == accept ]] || { echo "FAIL $name: $label was accepted" >&2; exit 1; }
  else
    [[ "$verdict" == refuse ]] || { echo "FAIL $name: $label was refused:" >&2; echo "$output" >&2; exit 1; }
    grep -qF "$path:" <<<"$output" || { echo "FAIL $name: $label was refused for another file:" >&2; echo "$output" >&2; exit 1; }
  fi
  unplant "$path"
  printf '  %s %s\n' "$verdict" "$label"
}

bash "$checker" "$fixture" >/dev/null || { echo "FAIL $name: the clean fixture was refused" >&2; exit 1; }

expect refuse "a Rust S3 client outside its place" \
  platforms/foundation-platform/crates/x/src/reader.rs \
  'use aws_sdk_s3::Client; fn c(conf: aws_sdk_s3::Config) -> Client { Client::from_conf(conf) }'
expect refuse "a Python S3 client" \
  platforms/foundation-platform/infra/lakehouse/spark/jobs/reader.py \
  'import boto3'
expect refuse "an s3a setting in a Spark job" \
  platforms/foundation-platform/infra/lakehouse/spark/jobs/reader.py \
  'builder.config("spark.hadoop.fs.s3a.retry.limit", "3")'
expect refuse "an S3 client in a Worker" \
  platforms/foundation-platform/services/x-gateway/src/index.ts \
  'import { S3Client } from "@aws-sdk/client-s3"; const c = new S3Client({});'
expect accept "a Rust client inside its place" \
  platforms/foundation-platform/crates/foundation-outbox/src/object_storage/r2/extra.rs \
  'use aws_sdk_s3::Client; fn c(conf: aws_sdk_s3::Config) -> Client { Client::from_conf(conf) }'
expect accept "a test asserting an s3a key" \
  platforms/foundation-platform/infra/lakehouse/spark/tests/test_reader.py \
  'assert settings["spark.hadoop.fs.s3a.retry.limit"] == "10"'
expect accept "a comment naming a client" \
  platforms/foundation-platform/crates/x/src/notes.rs \
  '// aws_sdk_s3 Client::from_conf belongs in foundation-outbox'

printf 'OK %s\n' "$name"
