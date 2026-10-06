#!/usr/bin/env bash
# Plants every way a Rust CI job can bypass the shared setup and requires
# scripts/guard/rust-ci-setup.sh to reject each one; then runs the setup script
# against a fake rustup to prove it exports the registry settings and installs
# the pinned channel (root ADR-0155).
set -euo pipefail
cd "$(dirname "$0")/../.."

guard="scripts/guard/rust-ci-setup.sh"
setup="scripts/ci/rust-setup.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) [ ! -e "$test_root" ] || rm -rf -- "$test_root" ;;
    *) echo "rust-ci-setup-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

fail() {
  echo "FAIL rust-ci-setup-self-test: $*" >&2
  exit 1
}

mkdir "$test_root/valid"
cat >"$test_root/valid/example.yml" <<'YAML'
name: synthetic
on:
  pull_request:
    branches: [main]
  merge_group:
permissions:
  contents: read
jobs:
  rust:
    name: Rust
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@2222222222222222222222222222222222222222
        with:
          persist-credentials: false
      # A comment that mentions cargo build is not a cargo step.
      - name: Set up Rust
        working-directory: ${{ github.workspace }}
        shell: bash
        run: bash scripts/ci/rust-setup.sh
      - name: Rust cache
        uses: Swatinem/rust-cache@1111111111111111111111111111111111111111  # v2
        with:
          workspaces: |
            area
            tools/xtask
          shared-key: example
          save-if: ${{ github.ref == 'refs/heads/main' }}
      - name: Verify
        run: cargo xtask verify area
  docs:
    name: Docs
    runs-on: ubuntu-24.04
    steps:
      - run: echo "no Rust here; ~/.cargo/bin is only a path"
YAML

if ! bash "$guard" "$test_root/valid" >/dev/null 2>&1; then
  bash "$guard" "$test_root/valid" || true
  fail "rejected the canonical Rust job"
fi

expect_rejected() {
  local label="$1"
  local expected="$2"
  local output
  if output="$(bash "$guard" "$test_root/$label" 2>&1)"; then
    fail "accepted planted bypass: $label"
  fi
  printf '%s\n' "$output" | grep -Fq -- "$expected" \
    || fail "$label was rejected for the wrong reason: $output"
}

plant() {
  local label="$1"
  shift
  mkdir "$test_root/$label"
  "$@" <"$test_root/valid/example.yml" >"$test_root/$label/example.yml"
  if cmp -s "$test_root/valid/example.yml" "$test_root/$label/example.yml"; then
    fail "planting $label changed nothing"
  fi
}

# The setup step is gone: the job installs nothing and inherits no retry settings.
plant no-setup sed '/name: Set up Rust/,/run: bash scripts\/ci\/rust-setup.sh/d'
expect_rejected no-setup 'has 0 "Set up Rust" steps'

# The setup is there but a second, private installer runs beside it.
plant private-rustup sed 's#^        run: cargo xtask verify area#        run: rustup toolchain install stable \&\& cargo xtask verify area#'
expect_rejected private-rustup 'installs a toolchain outside scripts/ci/rust-setup.sh'

# The pre-ADR-0155 action that only some jobs used.
plant toolchain-action sed 's#^      - name: Verify$#      - uses: dtolnay/rust-toolchain@3333333333333333333333333333333333333333\n      - name: Verify#'
expect_rejected toolchain-action 'installs a toolchain outside scripts/ci/rust-setup.sh'

# The setup step runs something other than the one script.
plant altered-setup sed 's#run: bash scripts/ci/rust-setup.sh#run: bash scripts/ci/rust-setup.sh || true#'
expect_rejected altered-setup 'must run exactly bash scripts/ci/rust-setup.sh'

# No cache: every run compiles every dependency again.
plant no-cache sed '/name: Rust cache/,/save-if:/d'
expect_rejected no-cache 'has 0 Rust cache steps'

# A cache that pull requests write fills the 10 GB budget with per-PR copies.
plant pr-writes-cache sed "s#save-if: .*#save-if: true#"
expect_rejected pr-writes-cache 'Rust cache must use save-if'

# A cache option outside the admitted set.
plant cache-extra-option sed 's#^          shared-key: example#          cache-all-crates: true#'
expect_rejected cache-extra-option 'admits only workspaces, shared-key and save-if'

# Cargo runs before the setup exported the registry settings.
plant cargo-before-setup awk '
  /^      - name: Set up Rust$/ { print "      - name: Early"; print "        run: cargo fetch" }
  { print }'
expect_rejected cargo-before-setup 'runs cargo before "Set up Rust"'

# The cache restores before the setup, so its key misses the exported settings.
plant cache-before-setup awk '
  /^      - name: Set up Rust$/ { hold=1 }
  hold && /^      - name: Rust cache$/ { hold=0; held_cache=1 }
  hold { setup = setup $0 "\n"; next }
  held_cache && /^      - name: Verify$/ { printf "%s", setup; held_cache=0 }
  { print }'
expect_rejected cache-before-setup 'Rust cache must follow "Set up Rust"'

# A job that reaches cargo only through cargo-deny is still a Rust job.
plant deny-without-setup awk '
  /^      - name: Set up Rust$/ { skip=1 }
  skip && /^      - name: Verify$/ { skip=0; print "      - run: cargo-deny --locked check"; next }
  skip { next }
  { print }'
expect_rejected deny-without-setup 'has 0 "Set up Rust" steps'

# A parser that sees no Rust job at all proves nothing.
mkdir "$test_root/no-rust"
sed '/^  rust:$/,/^  docs:$/{/^  docs:$/!d}' "$test_root/valid/example.yml" >"$test_root/no-rust/example.yml"
expect_rejected no-rust 'found no job that runs cargo'

# The setup script itself: run it against a fake rustup and read what it exported.
fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/rustup" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_RUSTUP_LOG"
SH
for tool in rustc cargo; do
  printf '#!/usr/bin/env bash\necho "%s fake"\n' "$tool" >"$fake_bin/$tool"
done
chmod +x "$fake_bin"/*
github_env="$test_root/github-env"
: >"$github_env"
PATH="$fake_bin:$PATH" GITHUB_ENV="$github_env" FAKE_RUSTUP_LOG="$test_root/rustup.log" \
  bash "$setup" >/dev/null

channel="$(sed -n 's/^channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' rust-toolchain.toml)"
grep -Eq "^toolchain install $channel( |$)" "$test_root/rustup.log" \
  || fail "rust-setup.sh did not install the channel rust-toolchain.toml pins ($channel)"
grep -Fxq "default $channel" "$test_root/rustup.log" \
  || fail "rust-setup.sh did not make $channel the default"
value_of() { sed -n "s/^$1=//p" "$github_env" | tail -n 1; }
[ "$(value_of CARGO_HTTP_MULTIPLEXING)" = false ] \
  || fail "rust-setup.sh must disable HTTP/2 multiplexing for cargo"
retries="$(value_of CARGO_NET_RETRY)"
[ -n "$retries" ] && [ "$retries" -ge 5 ] \
  || fail "rust-setup.sh must give cargo at least five network retries, found '$retries'"
[ "$(value_of CARGO_REGISTRIES_CRATES_IO_PROTOCOL)" = sparse ] \
  || fail "rust-setup.sh must use the sparse crates.io index"
[ "$(value_of PERFECTORY_RUST_SETUP)" = "$channel" ] \
  || fail "rust-setup.sh must export the marker xtask checks"

if GITHUB_ENV="" PATH="$fake_bin:$PATH" bash "$setup" >/dev/null 2>&1; then
  fail "rust-setup.sh ran without a GitHub Actions environment file"
fi

echo "OK rust-ci-setup-self-test"
