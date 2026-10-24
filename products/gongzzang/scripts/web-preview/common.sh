# Shared by deploy.sh and teardown.sh (root ADR-0249). Sourced, not run.
# shellcheck shell=bash

WORKER=gongzzang-web-preview
WRANGLER_VERSION=4.151.0
OPENNEXT_VERSION=1.20.13

wrangler() { npx --yes "wrangler@${WRANGLER_VERSION}" "$@"; }

# Wrangler writes a .wrangler/ state directory wherever it runs; calls that need no project run here.
# The caller removes it on exit.
SCRATCH=$(mktemp -d)
CF_BODY="$SCRATCH/body"

# The Cloudflare API under the caller's own wrangler login (or CLOUDFLARE_API_TOKEN); no token is kept.
# A wrangler without the `auth token` command (4.56 has none) or without a login prints nothing: that
# fails here by name instead of as a JSON parse error or an unauthenticated API call later.
cf_token() {
  local token=${CLOUDFLARE_API_TOKEN:-}
  if [[ -z "$token" ]]; then
    token=$( (cd "$SCRATCH" && wrangler auth token --json 2>/dev/null) |
      node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{process.stdout.write(JSON.parse(s.slice(s.indexOf("{"))).token??"")}catch{}})') || true
  fi
  if [[ -z "$token" ]]; then
    echo "no Cloudflare API token: set CLOUDFLARE_API_TOKEN, or log in with \`npx wrangler@${WRANGLER_VERSION} login\`" >&2
    return 1
  fi
  printf '%s' "$token"
}

# Prints the HTTP status and leaves the body in $CF_BODY (one file, so it survives `$(cf_get ...)`).
cf_get() {
  curl -sS -o "$CF_BODY" -w '%{http_code}' -H "Authorization: Bearer ${CF_TOKEN}" "https://api.cloudflare.com/client/v4$1"
}

cf_account() {
  if [[ -n "${CLOUDFLARE_ACCOUNT_ID:-}" ]]; then
    printf '%s' "$CLOUDFLARE_ACCOUNT_ID"
    return
  fi
  [[ "$(cf_get /accounts)" == 200 ]] || { echo "cannot list Cloudflare accounts" >&2; return 1; }
  node -e 'const r=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).result;
    if(r.length!==1){console.error(`${r.length} accounts: set CLOUDFLARE_ACCOUNT_ID`);process.exit(1)}
    process.stdout.write(r[0].id)' "$CF_BODY"
}

preview_origin() {
  [[ "$(cf_get "/accounts/${CF_ACCOUNT}/workers/subdomain")" == 200 ]] || { echo "no workers.dev subdomain" >&2; return 1; }
  node -e 'process.stdout.write(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).result.subdomain)' "$CF_BODY" |
    sed "s|^|https://${WORKER}.|; s|$|.workers.dev|"
}
