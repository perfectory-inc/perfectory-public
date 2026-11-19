#!/usr/bin/env bash
# One step of a Worker deploy, in the pinned Node container (root ADR-0175). worker_autodeploy.py
# runs it; nothing else does.
#
#   worker-wrangler.sh <workspace> <cache> <service-dir> install
#   worker-wrangler.sh <workspace> <cache> <service-dir> wrangler <wrangler arguments...>
#
# <workspace> is the deploy's copy of the commit's files (worker_autodeploy.py stages it, owned by
# the account the container runs as); <cache> is where pnpm keeps its store and corepack the pnpm it
# fetched, kept across runs (the lockfile's integrity hashes still decide what is installed); <service-dir> is the gateway's directory inside it. `install`
# installs the gateway's dependencies from its lockfile and checks that its Wrangler config is what
# the contract renders (`config:check`); `wrangler` runs the Wrangler that lockfile pins. The image is
# the Node pin of tools/technology-versions.contract.json; the memory cap is the worker-deploys
# contract's. The account token and id pass into the container by name from this process's
# environment (the unit's EnvironmentFile), never as a value on a command line, and only to Wrangler:
# the packages' install scripts run without them.
set -euo pipefail
usage="usage: worker-wrangler.sh <workspace> <cache> <service-dir> install | wrangler <args...>"
workspace="${1:?${usage}}"
cache="${2:?${usage}}"
service="${3:?${usage}}"
action="${4:?${usage}}"
shift 4
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "${here}/../../../../tools/container-images.env"
memory="$(python3 -I -B -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["containers"]["wrangler"]["memory_limit"])' \
  "${here}/../../config/worker-deploys.contract.json")"
owner="$(stat -c '%u:%g' "${workspace}")"

# Each container command below spells its options out: the container runtime policy guard reads the
# image from the command itself.
case "${action}" in
  install)
    exec docker run --rm --init --memory "${memory}" --memory-swap="${memory}" --pids-limit 1024 \
      --user "${owner}" -e HOME=/cache/home -e COREPACK_HOME=/cache/corepack -e npm_config_store_dir=/cache/pnpm-store \
      -e COREPACK_ENABLE_DOWNLOAD_PROMPT=0 -e CI=true -e WRANGLER_SEND_METRICS=false \
      -v "${workspace}:/work" -v "${cache}:/cache" -w "/work/${service}" "${WORKER_DEPLOY_IMAGE}" \
      sh -c 'corepack pnpm install --frozen-lockfile && corepack pnpm run config:check'
    ;;
  wrangler)
    # wrangler reads its token only as CLOUDFLARE_API_TOKEN; the host holds it under the name derived from
    # the token's Cloudflare name (root ADR-0247). Passed through the environment, never on the command line.
    CLOUDFLARE_API_TOKEN="${FOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN:?}" exec docker run --rm --init --memory "${memory}" --memory-swap="${memory}" --pids-limit 1024 \
      --user "${owner}" -e HOME=/cache/home -e COREPACK_HOME=/cache/corepack -e npm_config_store_dir=/cache/pnpm-store \
      -e COREPACK_ENABLE_DOWNLOAD_PROMPT=0 -e CI=true -e WRANGLER_SEND_METRICS=false \
      -e CLOUDFLARE_API_TOKEN -e CLOUDFLARE_ACCOUNT_ID \
      -v "${workspace}:/work" -v "${cache}:/cache" -w "/work/${service}" "${WORKER_DEPLOY_IMAGE}" corepack pnpm exec wrangler "$@"
    ;;
  *)
    echo "${usage}" >&2
    exit 64
    ;;
esac
