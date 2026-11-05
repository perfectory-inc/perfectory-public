#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/cargo-goes-through-xtask.yml (`opengrep scan --test`). Not run.
# ruleid: scripts-run-cargo-test-through-xtask
cargo test -p foundation-outbox --test kafka_live
# ruleid: scripts-run-cargo-test-through-xtask
cargo +1.90 test --workspace
# ruleid: scripts-run-cargo-test-through-xtask
output="$(cargo test --no-run 2>&1)"
# ok: scripts-run-cargo-test-through-xtask
cargo xtask test --lane foundation-kafka-live
# ok: scripts-run-cargo-test-through-xtask
bash scripts/guard/cargo-verify-isolation-self-test.sh
# ok: scripts-run-cargo-test-through-xtask
echo "run cargo test through xtask"
