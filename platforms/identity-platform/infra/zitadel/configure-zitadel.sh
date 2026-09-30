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
#   ZITADEL_SECRETS_DIR   where a staff console's client credentials are written, one
#                         <name>-oidc-client.env per application, mode 0600
#                         (default /etc/identity-platform/secrets)
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
consoles="${platform_root}/config/staff-console-applications.v1.json"
secrets_dir="${ZITADEL_SECRETS_DIR:-/etc/identity-platform/secrets}"
action_file="${here}/actions/principal-kind.js"
pat_file="${ZITADEL_PAT_FILE:-/etc/identity-platform/secrets/zitadel-bootstrap-pat}"
project_name="${ZITADEL_PROJECT_NAME:-perfectory}"

emit_bindings=""
if [[ "${1:-}" == "--emit-bindings" ]]; then
  emit_bindings="${2:?--emit-bindings needs a path}"
fi

for f in "${contract}" "${policy}" "${consoles}" "${action_file}" "${pat_file}"; do
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

# --- staff console web applications (root ADR-0116) ---------------------
# One confidential OIDC web app per entry of the console list: authorization code
# with PKCE and refresh tokens, JWT access tokens (foundation-api verifies them
# locally), basic client authentication. The client secret is shown by Zitadel
# once; it goes straight into a 0600 file and never to stdout. A later run
# converges the app to the list (redirect URIs change with the console's origin)
# and only mints a new secret when its file is missing.
console_rows="$(python3 -c "
import json, sys
for app in json.load(open(sys.argv[1]))['applications']:
    print(app['name'])
" "${consoles}")"
while read -r console; do
  [[ -n "${console}" ]] || continue
  desired="$(python3 -c "
import json, sys
app = next(a for a in json.load(open(sys.argv[1]))['applications'] if a['name'] == sys.argv[2])
print(json.dumps({
    'redirectUris': app['redirect_uris'],
    'postLogoutRedirectUris': app['post_logout_redirect_uris'],
    'responseTypes': ['OIDC_RESPONSE_TYPE_CODE'],
    'grantTypes': ['OIDC_GRANT_TYPE_AUTHORIZATION_CODE', 'OIDC_GRANT_TYPE_REFRESH_TOKEN'],
    'appType': 'OIDC_APP_TYPE_WEB',
    'authMethodType': 'OIDC_AUTH_METHOD_TYPE_BASIC',
    'version': 'OIDC_VERSION_1_0',
    'devMode': app['dev_mode'],
    'accessTokenType': 'OIDC_TOKEN_TYPE_JWT',
    'idTokenUserinfoAssertion': True,
}, sort_keys=True))
" "${consoles}" "${console}")"
  app_id="$(api -X POST "${base_url}/management/v1/projects/${project_id}/apps/_search" \
    -H 'Content-Type: application/json' \
    -d "{\"queries\":[{\"nameQuery\":{\"name\":\"${console}\",\"method\":\"TEXT_QUERY_METHOD_EQUALS\"}}]}" \
    | python3 -c "
import json, sys
rows = json.load(sys.stdin).get('result') or []
print(rows[0]['id'] if rows else '')")"
  credentials="${secrets_dir}/${console}-oidc-client.env"
  if [[ -z "${app_id}" ]]; then
    created="$(printf '%s' "${desired}" | python3 -c "
import json, sys
body = json.load(sys.stdin); body['name'] = sys.argv[1]; print(json.dumps(body))
" "${console}" | api -X POST "${base_url}/management/v1/projects/${project_id}/apps/oidc" \
        -H 'Content-Type: application/json' --data-binary @-)"
    ( umask 077; printf '%s' "${created}" | python3 -c "
import json, sys
body = json.load(sys.stdin)
print(f\"OIDC_CLIENT_ID={body['clientId']}\")
print(f\"OIDC_CLIENT_SECRET={body['clientSecret']}\")
" > "${credentials}" )
    printf 'created console app %s client_id=%s credentials=%s\n' "${console}" \
      "$(printf '%s' "${created}" | json_field clientId)" "${credentials}"
    continue
  fi
  current="$(api "${base_url}/management/v1/projects/${project_id}/apps/${app_id}" | python3 -c "
import json, sys
c = json.load(sys.stdin)['app']['oidcConfig']
keys = ['redirectUris','postLogoutRedirectUris','responseTypes','grantTypes','appType',
        'authMethodType','version','devMode','accessTokenType','idTokenUserinfoAssertion']
# Zitadel leaves an enum out of its answer when it holds the enum's zero value, so a
# missing key reads as that default rather than as a difference to converge.
defaults = {'appType': 'OIDC_APP_TYPE_WEB', 'authMethodType': 'OIDC_AUTH_METHOD_TYPE_BASIC',
            'version': 'OIDC_VERSION_1_0', 'accessTokenType': 'OIDC_TOKEN_TYPE_BEARER',
            'devMode': False, 'idTokenUserinfoAssertion': False}
print(json.dumps({k: c.get(k, defaults.get(k, [])) for k in keys}, sort_keys=True))")"
  if [[ "${current}" != "${desired}" ]]; then
    printf '%s' "${desired}" | api -X PUT \
      "${base_url}/management/v1/projects/${project_id}/apps/${app_id}/oidc_config" \
      -H 'Content-Type: application/json' --data-binary @- >/dev/null
    printf 'updated console app %s id=%s\n' "${console}" "${app_id}"
  else
    printf 'exists  console app %s id=%s\n' "${console}" "${app_id}"
  fi
  if [[ ! -s "${credentials}" ]]; then
    client_id="$(api "${base_url}/management/v1/projects/${project_id}/apps/${app_id}" \
      | json_field app oidcConfig clientId)"
    ( umask 077; api -X POST -H 'Content-Type: application/json' -d '{}' \
        "${base_url}/management/v1/projects/${project_id}/apps/${app_id}/oidc_config/_generate_client_secret" \
      | python3 -c "
import json, sys
print(f'OIDC_CLIENT_ID={sys.argv[1]}')
print(f\"OIDC_CLIENT_SECRET={json.load(sys.stdin)['clientSecret']}\")
" "${client_id}" > "${credentials}" )
    printf 'minted  console app %s credentials=%s (the file was missing)\n' "${console}" "${credentials}"
  fi
done <<<"${console_rows}"

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
