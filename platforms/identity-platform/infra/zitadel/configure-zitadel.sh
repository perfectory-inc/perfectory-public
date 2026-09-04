#!/usr/bin/env bash
# Idempotent Zitadel configuration for the identity runtime (root ADR-0080/0081).
#
# Everything this script creates it first looks for: a second run reports
# `exists` on every line and changes nothing. The list of machine users is not
# written here — it is read from the workload principal policy artifact, the
# same file the provisioner compiles in, so there is exactly one list of
# service principals in the repository. The claim-injecting action body is
# read from actions/principal-kind.js, the only copy of that script.
#
# Inputs (env):
#   ZITADEL_PAT_FILE      bearer for the management API
#                         (default /etc/identity-platform/secrets/zitadel-bootstrap-pat)
#   ZITADEL_PROJECT_NAME  project to ensure (default perfectory)
# The base URL is derived from config/identity-runtime-endpoints.contract.json.
#
# Options:
#   --emit-bindings PATH  after ensuring users, write the workload principal
#                         bindings document with the real subjects, sorted.
set -Eeuo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
platform_root="$(cd "${here}/../.." && pwd)"
contract="${platform_root}/config/identity-runtime-endpoints.contract.json"
policy="${platform_root}/config/workload-principal-policy.v1.json"
action_file="${here}/actions/principal-kind.js"
pat_file="${ZITADEL_PAT_FILE:-/etc/identity-platform/secrets/zitadel-bootstrap-pat}"
project_name="${ZITADEL_PROJECT_NAME:-perfectory}"

emit_bindings=""
if [[ "${1:-}" == "--emit-bindings" ]]; then
  emit_bindings="${2:?--emit-bindings needs a path}"
fi

for f in "${contract}" "${policy}" "${action_file}" "${pat_file}"; do
  [[ -r "${f}" ]] || { printf 'FAIL configure-zitadel: unreadable %s\n' "${f}" >&2; exit 66; }
done

base_url="$(python3 -c "
import json, sys
issuer = json.load(open(sys.argv[1]))['issuer']
print(f\"{issuer['scheme']}://{issuer['host']}:{issuer['loopback_port']}\")
" "${contract}")"

api() {
  # Management calls carry the PAT; -f keeps HTTP failures loud, and every
  # caller captures stdout so nothing secret lands on the terminal.
  curl -sf -H "Authorization: Bearer $(cat "${pat_file}")" "$@"
}

json_field() { python3 -c "
import json, sys
value = json.load(sys.stdin)
for key in sys.argv[1:]:
    value = value[key]
print(value)
" "$@"; }

# --- project ------------------------------------------------------------
project_id="$(api -X POST "${base_url}/management/v1/projects/_search" \
  -H 'Content-Type: application/json' \
  -d "{\"queries\":[{\"nameQuery\":{\"name\":\"${project_name}\",\"method\":\"TEXT_QUERY_METHOD_EQUALS\"}}]}" \
  | python3 -c "
import json, sys
rows = json.load(sys.stdin).get('result') or []
print(rows[0]['id'] if rows else '')")"
if [[ -z "${project_id}" ]]; then
  project_id="$(api -X POST "${base_url}/management/v1/projects" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${project_name}\"}" | json_field id)"
  printf 'created project %s id=%s\n' "${project_name}" "${project_id}"
else
  printf 'exists  project %s id=%s\n' "${project_name}" "${project_id}"
fi

# --- action -------------------------------------------------------------
action_id="$(api -X POST "${base_url}/management/v1/actions/_search" \
  -H 'Content-Type: application/json' \
  -d '{"queries":[{"actionNameQuery":{"name":"principalKind","method":"TEXT_QUERY_METHOD_EQUALS"}}]}' \
  | python3 -c "
import json, sys
rows = json.load(sys.stdin).get('result') or []
print(rows[0]['id'] if rows else '')")"
if [[ -z "${action_id}" ]]; then
  action_id="$(python3 -c "
import json, sys
script = open(sys.argv[1], encoding='utf-8').read().strip()
print(json.dumps({'name': 'principalKind', 'script': script,
                  'timeout': '10s', 'allowedToFail': False}))
" "${action_file}" \
    | api -X POST "${base_url}/management/v1/actions" \
        -H 'Content-Type: application/json' --data-binary @- | json_field id)"
  printf 'created action principalKind id=%s\n' "${action_id}"
else
  printf 'exists  action principalKind id=%s\n' "${action_id}"
fi

# --- flow trigger (complement token, pre access token creation) ---------
# The trigger call is a POST; PUT answers 405 and vanishes inside && chains,
# which is how the first bring-up shipped tokens without the claim. The call
# SETS the list, so existing action ids are carried along, ours unioned in.
flow_state="$(api "${base_url}/management/v1/flows/2")"
attach_ids="$(printf '%s' "${flow_state}" | python3 -c "
import json, sys
flow = json.load(sys.stdin).get('flow') or {}
wanted = sys.argv[1]
for trigger in flow.get('triggerActions') or []:
    if trigger.get('triggerType', {}).get('id') == '5':
        ids = [a['id'] for a in trigger.get('actions') or []]
        print('' if wanted in ids else ','.join(dict.fromkeys(ids + [wanted])))
        break
else:
    print(wanted)
" "${action_id}")"
if [[ -n "${attach_ids}" ]]; then
  payload="$(python3 -c "
import json, sys
print(json.dumps({'actionIds': sys.argv[1].split(',')}))
" "${attach_ids}")"
  api -H 'Content-Type: application/json' --data-binary "${payload}" \
    "${base_url}/management/v1/flows/2/trigger/5" >/dev/null
  printf 'attached flow=2 trigger=5 action=%s\n' "${action_id}"
else
  printf 'exists  flow=2 trigger=5 action=%s\n' "${action_id}"
fi

# --- machine users, one per policy slug ---------------------------------
slugs="$(python3 -c "
import json, sys
policy = json.load(open(sys.argv[1]))
for principal in policy['principals']:
    print(principal['service_slug'], principal['display_name'].replace(' ', ''))
" "${policy}")"

bindings_rows=""
while read -r slug display; do
  subject="$(api -X POST "${base_url}/management/v1/users/_search" \
    -H 'Content-Type: application/json' \
    -d "{\"queries\":[{\"userNameQuery\":{\"userName\":\"${slug}\",\"method\":\"TEXT_QUERY_METHOD_EQUALS\"}}]}" \
    | python3 -c "
import json, sys
rows = json.load(sys.stdin).get('result') or []
print(rows[0]['id'] if rows else '')")"
  if [[ -z "${subject}" ]]; then
    subject="$(api -X POST "${base_url}/management/v1/users/machine" \
      -H 'Content-Type: application/json' \
      -d "{\"userName\":\"${slug}\",\"name\":\"${display}\",\"accessTokenType\":\"ACCESS_TOKEN_TYPE_JWT\"}" \
      | json_field userId)"
    printf 'created machine %s subject=%s\n' "${slug}" "${subject}"
  else
    printf 'exists  machine %s subject=%s\n' "${slug}" "${subject}"
  fi
  bindings_rows="${bindings_rows}${slug} ${subject}"$'\n'
done <<<"${slugs}"

# --- bindings document ---------------------------------------------------
if [[ -n "${emit_bindings}" ]]; then
  printf '%s' "${bindings_rows}" | python3 -c "
import json, sys
rows = [line.split() for line in sys.stdin.read().splitlines() if line]
document = {
    'schema_version': 'identity.workload-principal-bindings.v1',
    'bindings': [
        {'service_slug': slug, 'zitadel_subject': subject}
        for slug, subject in sorted(rows)
    ],
}
with open(sys.argv[1], 'w', encoding='utf-8', newline='\n') as handle:
    json.dump(document, handle, indent=2, ensure_ascii=False)
    handle.write('\n')
" "${emit_bindings}"
  printf 'wrote   bindings %s\n' "${emit_bindings}"
fi
