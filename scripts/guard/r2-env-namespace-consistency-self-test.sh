#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/r2-env-namespace-consistency.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/platforms/foundation-platform/config" "$fixture/scripts"
cat > "$fixture/platforms/foundation-platform/.env.example" <<'EOF'
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION=auto
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL=
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID=
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT=
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET=
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION=auto
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX=gold/vector-tiles/releases
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET=
FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_ENDPOINT_HOST=
FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_WRITER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_WRITER_SECRET_ACCESS_KEY=
FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID=
FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY=
EOF
{
  printf '{\n  "canonical_keys": [\n'
  for key in \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_REGION \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY \
    FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_ACCESS_KEY_ID \
    FOUNDATION_PLATFORM_R2_PARCEL_PUBLICATION_EVIDENCE_WRITER_SECRET_ACCESS_KEY \
    FOUNDATION_PLATFORM_R2_LAKEHOUSE_PUBLIC_BASE_URL \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_BUCKET \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_PREFIX \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_ACCESS_KEY_ID \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_WRITER_SECRET_ACCESS_KEY \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID \
    FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY \
    FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET \
    FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_ENDPOINT_HOST \
    FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_WRITER_ACCESS_KEY_ID \
    FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_WRITER_SECRET_ACCESS_KEY \
    FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID \
    FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY; do
    printf '    "%s",\n' "$key"
  done
  printf '    "sentinel"\n  ]\n}\n'
} > "$fixture/platforms/foundation-platform/config/r2-connections.contract.json"
printf '%s\n' 'R2_MODE=false' > "$fixture/scripts/internal.sh"

git -C "$fixture" init -q
git -C "$fixture" config user.email guard@example.invalid
git -C "$fixture" config user.name guard
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

printf '%s\n' 'R2_BUCKET_NAME=legacy' > "$fixture/platforms/foundation-platform/.env.local"
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL r2-env-namespace-consistency-self-test: stale local key was accepted' >&2
  exit 1
fi
rm -f "$fixture/platforms/foundation-platform/.env.local"

printf '%s\n' 'R2_BUCKET_NAME=legacy' >> "$fixture/scripts/internal.sh"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL r2-env-namespace-consistency-self-test: legacy assignment was accepted' >&2
  exit 1
fi
# The fixture is only ever staged, never committed, so `git checkout --` would
# restore the polluted index rather than the clean file. Rewrite it instead.
printf '%s\n' 'R2_MODE=false' > "$fixture/scripts/internal.sh"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

# --- the allow-list dimension --------------------------------------------------
# The checks above reject spellings we already know are legacy, which leaves a new
# name free to walk in. These two prove the opposite rule: a name no list
# authorises fails, whether it looks like a credential or like a switch.

env_example="$fixture/platforms/foundation-platform/.env.example"
cp -- "$env_example" "$fixture/.env.example.clean"

unlisted_probe() {
  local label="$1" name="$2" should_fail="$3"
  printf '%s=\n' "$name" >> "$env_example"
  git -C "$fixture" add .
  if bash "$checker" "$fixture" >/dev/null 2>&1; then
    if [ "$should_fail" = "yes" ]; then
      echo "FAIL r2-env-namespace-consistency-self-test: $label was accepted" >&2
      exit 1
    fi
  elif [ "$should_fail" = "no" ]; then
    echo "FAIL r2-env-namespace-consistency-self-test: $label was rejected" >&2
    exit 1
  fi
  cp -- "$fixture/.env.example.clean" "$env_example"
  git -C "$fixture" add .
}

# A credential nobody added to canonical_keys. This is the shape the deny-list
# could never catch: it is not a legacy spelling, it is simply new.
unlisted_probe "an unlisted connection field" \
  FOUNDATION_PLATFORM_R2_ARCHIVE_SECRET_ACCESS_KEY yes

# A switch outside every declared control family.
unlisted_probe "a control outside every declared family" \
  FOUNDATION_PLATFORM_R2_WEIRD_KNOB yes

# A switch inside a declared family is fine — the rule constrains families, and
# forcing every operational flag through a guard edit would make it a changelog.
unlisted_probe "a control inside a declared family" \
  FOUNDATION_PLATFORM_R2_INVENTORY_PAGE_SIZE no

echo 'OK r2-env-namespace-consistency-self-test'
