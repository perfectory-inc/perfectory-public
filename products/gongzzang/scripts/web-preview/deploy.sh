#!/usr/bin/env bash
# Builds the read-only Gongzzang web preview from a git ref and deploys it as the one Cloudflare
# Worker `gongzzang-web-preview` on the free plan (root ADR-0249). teardown.sh removes it.
# Runbook: products/gongzzang/docs/runbooks/web-preview.md
#
#   deploy.sh [git-ref]          default origin/main
#
# Needs: git, docker (the build runs in Linux: OpenNext cannot bundle on Windows), node + npx,
# curl, ssh to the data host, and a wrangler login (`npx wrangler login`) or CLOUDFLARE_API_TOKEN.
# NAVER_MAPS_CLIENT_ID defaults to the value in this checkout's apps/web/.env.local.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh
source "$here/common.sh"
repo=$(git -C "$here" rev-parse --show-toplevel)
ref=${1:-origin/main}
# The runtime manifest is answered only inside the data host (its public edge is planned), so the
# preview carries a snapshot. Releases are immutable; redeploy when the map shows an old release.
manifest_source=${RUNTIME_MANIFEST_SOURCE:-"ssh ai-server curl -fsS -m 20 http://127.0.0.1:18080/catalog/v1/vector-tiles/runtime-manifest"}
max_gzip_kib=3072 # the free plan's compressed Worker size limit

if [[ -z "${NAVER_MAPS_CLIENT_ID:-}" && -f "$repo/products/gongzzang/apps/web/.env.local" ]]; then
  NAVER_MAPS_CLIENT_ID=$(sed -n 's/^NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID=//p' "$repo/products/gongzzang/apps/web/.env.local" | tr -d '\r')
fi
: "${NAVER_MAPS_CLIENT_ID:?set NAVER_MAPS_CLIENT_ID (the Naver Maps client id; public, shipped to browsers)}"

work=$(mktemp -d)
trap 'rm -rf "$work" "$SCRATCH"' EXIT

CF_TOKEN=$(cf_token)
CF_ACCOUNT=$(cf_account)
origin=$(preview_origin)
commit=$(git -C "$repo" rev-parse --verify "${ref}^{commit}")
echo "preview ${origin} from ${ref} (${commit})"

# The preview has no identity, no session store and no Rust API (lib/preview.ts). These values only
# satisfy apps/web/lib/env.ts: reserved .invalid hosts and throwaway random strings, never production.
random() { node -e 'process.stdout.write(require("crypto").randomBytes(32).toString("base64url"))'; }
vars=(
  "NEXT_PUBLIC_GONGZZANG_READ_ONLY_PREVIEW=1"
  "NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID=${NAVER_MAPS_CLIENT_ID}"
  "NEXT_PUBLIC_API_BASE_URL=https://api.gongzzang-preview.invalid"
  "NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL=${origin}"
  "NEXT_PUBLIC_PARCEL_EDGE_BASE_URL=${origin}/edge/parcels"
  "NEXT_PUBLIC_BUILDING_EDGE_BASE_URL=${origin}/edge/buildings"
  "NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL=${origin}/edge/unit-prices"
  "NEXT_PUBLIC_MAP_EDIT_OVERLAY_BASE_URL=${origin}/edge/map-edits"
  "ZITADEL_ISSUER=https://identity.gongzzang-preview.invalid"
  "ZITADEL_CLIENT_ID=gongzzang-web-preview"
  "ZITADEL_AUDIENCE=gongzzang-web-preview"
  "ZITADEL_REDIRECT_URI=${origin}/api/auth/callback"
  "REDIS_URL=rediss://session.gongzzang-preview.invalid:6379"
  "SESSION_SECRET=$(random)"
  "INTERNAL_AUTH_SECRET=$(random)"
  "FOUNDATION_PLATFORM_WEBHOOK_SECRET=$(random)"
)

echo "== build (${NODE_IMAGE}, @opennextjs/cloudflare ${OPENNEXT_VERSION})"
env_args=()
for v in "${vars[@]}"; do env_args+=(-e "$v"); done
git -C "$repo" archive --format=tar "$commit" products/gongzzang platforms/foundation-platform/config |
  docker run -i --rm "${env_args[@]}" "$NODE_IMAGE" bash -c "
    set -euo pipefail
    mkdir /src && tar -xf - -C /src
    cd /src/products/gongzzang
    corepack enable >&2
    # OpenNext bundles from a flat node_modules; pnpm's isolated layout hides next's optional peers.
    echo 'node-linker=hoisted' >>.npmrc
    pnpm install --frozen-lockfile --filter '@gongzzang/web...' >&2
    pnpm --filter @gongzzang/web add -D @opennextjs/cloudflare@${OPENNEXT_VERSION} wrangler@${WRANGLER_VERSION} >&2
    cd apps/web
    cp ../../scripts/web-preview/open-next.config.ts ../../scripts/web-preview/wrangler.jsonc .
    npx opennextjs-cloudflare build >&2
    tar -cf - .open-next
  " >"$work/build.tar"

app="$work/app"
mkdir -p "$app"
tar -xf "$work/build.tar" -C "$app"
cp "$here"/{wrangler.jsonc,worker.mjs,edge-relay.mjs} "$app/"
node --test "$here/edge-relay.test.mjs" >/dev/null

echo "== runtime manifest snapshot"
manifest="$app/.open-next/assets/catalog/v1/vector-tiles/runtime-manifest"
mkdir -p "$(dirname "$manifest")"
$manifest_source | node -e '
  // Tiles are fetched through the preview (edge-relay.mjs), so each unit points at /edge/tiles.
  let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
    const m = JSON.parse(s);
    if (m.schema_version !== 2) throw new Error(`runtime manifest schema ${m.schema_version}, expected 2`);
    for (const unit of Object.values(m.publication_units)) {
      const template = new URL(unit.source.tiles_url_template.replace(/[{}]/g, "_"));
      if (template.host !== "tiles.perfectory.io") throw new Error(`unexpected tile host ${template.host}`);
      unit.source.tiles_url_template = unit.source.tiles_url_template.replace("https://tiles.perfectory.io", process.argv[1] + "/edge/tiles");
    }
    process.stdout.write(JSON.stringify(m));
  });' "$origin" >"$manifest"

echo "== size"
(cd "$app" && wrangler deploy --dry-run --outdir "$work/dist") | tee "$work/dry-run.log" | grep -i "total upload" || true
gzip_kib=$(sed -n 's/.*gzip: \([0-9.]*\) KiB.*/\1/p' "$work/dry-run.log" | tail -1)
[[ -n "$gzip_kib" ]] || { echo "could not read the compressed size" >&2; exit 1; }
if node -e "process.exit(Number(process.argv[1]) > ${max_gzip_kib} ? 0 : 1)" "$gzip_kib"; then
  echo "compressed Worker is ${gzip_kib} KiB, over the free plan's ${max_gzip_kib} KiB: not deploying" >&2
  exit 1
fi

echo "== deploy"
var_args=()
for v in "${vars[@]}"; do var_args+=(--var "${v/=/:}"); done
(cd "$app" && wrangler deploy "${var_args[@]}" --message "preview ${commit}")

echo "== verify"
expect() {
  local want=$1 url=$2 got
  for _ in $(seq 1 12); do
    got=$(curl -s -o /dev/null -w '%{http_code}' "$url")
    [[ "$got" == "$want" ]] && { echo "ok ${got} ${url}"; return; }
    sleep 5
  done
  echo "FAILED ${url}: ${got}, want ${want}" >&2
  return 1
}
expect 200 "${origin}/listings"
expect 200 "${origin}/catalog/v1/vector-tiles/runtime-manifest"
expect 200 "${origin}/edge/map-edits/overlay/admin"
expect 404 "${origin}/profile"
expect 404 "${origin}/api/proxy/listings"
echo "preview is live: ${origin}/listings"
