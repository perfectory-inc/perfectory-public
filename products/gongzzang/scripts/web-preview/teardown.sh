#!/usr/bin/env bash
# Removes the read-only Gongzzang web preview completely and checks that nothing is left
# (root ADR-0249). Safe to run again: an absent preview is the goal, not an error.
# Runbook: products/gongzzang/docs/runbooks/web-preview.md
#
# Needs: node + npx, curl, and a wrangler login (`npx wrangler login`) or CLOUDFLARE_API_TOKEN.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh
source "$here/common.sh"
trap 'rm -rf "$SCRATCH"' EXIT

CF_TOKEN=$(cf_token)
CF_ACCOUNT=$(cf_account)
origin=$(preview_origin)
script="/accounts/${CF_ACCOUNT}/workers/scripts/${WORKER}"

# Deleting the Worker deletes what deploy.sh gave it: its versions, static assets, variables and
# workers.dev route. deploy.sh makes no KV, D1, R2, queue, secret or custom domain.
if [[ "$(cf_get "$script")" == 200 ]]; then
  (cd "$SCRATCH" && wrangler delete --name "$WORKER" --force)
else
  echo "no Worker ${WORKER}"
fi

echo "== verify nothing is left"
left=0
status=$(cf_get "$script")
[[ "$status" == 404 ]] || { echo "LEFT: Worker ${WORKER} (API ${status})" >&2; left=1; }
# Anything else carrying the preview's name would be a leftover of a changed deploy.sh.
for listing in workers/scripts storage/kv/namespaces d1/database r2/buckets; do
  [[ "$(cf_get "/accounts/${CF_ACCOUNT}/${listing}?per_page=100")" == 200 ]] || { echo "cannot list ${listing}" >&2; left=1; continue; }
  if grep -q "$WORKER" "$CF_BODY"; then
    echo "LEFT: ${listing} still names ${WORKER}" >&2
    left=1
  fi
done
gone=""
for _ in $(seq 1 12); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "${origin}/listings")
  [[ "$code" == 404 ]] && { gone=yes; break; }
  sleep 5
done
[[ -n "$gone" ]] || { echo "LEFT: ${origin} still answers ${code}" >&2; left=1; }
[[ "$left" == 0 ]] || exit 1
echo "removed: no Worker, no storage, ${origin} answers 404"
echo "owner, by hand: remove ${origin} from the Naver Maps application's Web service URLs"
