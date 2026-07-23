#!/usr/bin/env bash
# Exercises the fail-closed, read-only GitHub publication-authority preflight.
set -euo pipefail

cd "$(dirname "$0")/../.."

test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$test_root" ] || rm -rf -- "$test_root"
      ;;
    *)
      echo "publication-authority-self-test: refusing unsafe cleanup" >&2
      ;;
  esac
}
trap cleanup EXIT

mkdir -p "$test_root/bin"
cat >"$test_root/bin/gh" <<'FAKE_GH'
#!/usr/bin/env bash
set -euo pipefail

case_name="${FAKE_AUTHORITY_CASE:-valid}"
call_log="${GH_CALL_LOG:?GH_CALL_LOG is required}"

[ "${1:-}" = api ] || {
  echo "fake-gh: expected the api subcommand" >&2
  exit 90
}
shift

host_seen=0
accept_seen=0
version_seen=0
paginate_seen=0
slurp_seen=0
endpoint=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --hostname)
      [ "${2:-}" = github.com ] || exit 91
      host_seen=1
      shift 2
      ;;
    -H)
      case "${2:-}" in
        'Accept: application/vnd.github+json') accept_seen=1 ;;
        'X-GitHub-Api-Version: 2026-03-10') version_seen=1 ;;
        *) echo "fake-gh: unexpected header '${2:-}'" >&2; exit 92 ;;
      esac
      shift 2
      ;;
    --paginate)
      paginate_seen=1
      shift
      ;;
    --slurp)
      slurp_seen=1
      shift
      ;;
    --method|-X|-f|-F|--raw-field|--field|--input)
      echo "fake-gh: mutation-capable option '$1' is forbidden" >&2
      exit 93
      ;;
    --*)
      echo "fake-gh: unexpected option '$1'" >&2
      exit 94
      ;;
    *)
      [ -z "$endpoint" ] || {
        echo "fake-gh: multiple endpoints" >&2
        exit 95
      }
      endpoint="$1"
      shift
      ;;
  esac
done

[ "$host_seen" -eq 1 ] && [ "$accept_seen" -eq 1 ] && [ "$version_seen" -eq 1 ] || {
  echo "fake-gh: host or API headers were not pinned" >&2
  exit 96
}
[ -n "$endpoint" ] || exit 97

case "$endpoint" in
  orgs/perfectory-inc/members\?*|\
  orgs/perfectory-inc/outside_collaborators\?*|\
  orgs/perfectory-inc/invitations\?*|\
  orgs/perfectory-inc/installations\?*|\
  repos/perfectory-inc/perfectory-public/collaborators\?*|\
  repos/perfectory-inc/perfectory-public/teams\?*|\
  repos/perfectory-inc/perfectory-public/invitations\?*|\
  repos/perfectory-inc/perfectory-public/keys\?*)
    [ "$paginate_seen" -eq 1 ] && [ "$slurp_seen" -eq 1 ] || {
      echo "fake-gh: list endpoint was not completely paginated: $endpoint" >&2
      exit 98
    }
    ;;
  *)
    [ "$paginate_seen" -eq 0 ] && [ "$slurp_seen" -eq 0 ] || {
      echo "fake-gh: object endpoint unexpectedly used list pagination" >&2
      exit 99
    }
    ;;
esac
printf '%s\n' "$endpoint" >>"$call_log"

case "$endpoint" in
  user)
    if [ "$case_name" = malformed-login ]; then
      printf '%s\n' '{"login":"not/a/login"}'
    else
      printf '%s\n' '{"login":"sole-user"}'
    fi
    ;;
  orgs/perfectory-inc)
    permission=read
    [ "$case_name" != valid-default-none ] || permission=none
    [ "$case_name" != default-write ] || permission=write
    org_login=perfectory-inc
    [ "$case_name" != wrong-org ] || org_login=other-org
    printf '{"login":"%s","default_repository_permission":"%s"}\n' \
      "$org_login" "$permission"
    ;;
  orgs/perfectory-inc/members\?*)
    case "$case_name" in
      extra-member)
        printf '%s\n' '[[{"login":"sole-user"}],[{"login":"other-user"}]]'
        ;;
      malformed-members)
        printf '%s\n' '{"login":"sole-user"}'
        ;;
      missing-member)
        printf '%s\n' '[[]]'
        ;;
      *)
        printf '%s\n' '[[{"login":"sole-user"}]]'
        ;;
    esac
    ;;
  orgs/perfectory-inc/memberships/sole-user)
    state=active
    role=admin
    [ "$case_name" != inactive-membership ] || state=pending
    [ "$case_name" != non-owner-membership ] || role=member
    printf '{"state":"%s","role":"%s","organization":{"login":"perfectory-inc"},"user":{"login":"sole-user"}}\n' \
      "$state" "$role"
    ;;
  orgs/perfectory-inc/outside_collaborators\?*)
    if [ "$case_name" = outside-collaborator ]; then
      printf '%s\n' '[[{"login":"outside-user"}]]'
    else
      printf '%s\n' '[[]]'
    fi
    ;;
  orgs/perfectory-inc/invitations\?*)
    if [ "$case_name" = org-invitation ]; then
      printf '%s\n' '[[{"id":101,"login":"invited-user","role":"admin"}]]'
    else
      printf '%s\n' '[[]]'
    fi
    ;;
  orgs/perfectory-inc/installations\?*)
    if [ "$case_name" = org-app ]; then
      printf '%s\n' '[{"total_count":1,"installations":[{"id":201,"app_slug":"writer-app"}]}]'
    elif [ "$case_name" = inconsistent-installations ]; then
      printf '%s\n' '[{"total_count":1,"installations":[]}]'
    else
      printf '%s\n' '[{"total_count":0,"installations":[]}]'
    fi
    ;;
  repos/perfectory-inc/perfectory-public)
    admin=true
    [ "$case_name" != no-repo-admin ] || admin=false
    full_name=perfectory-inc/perfectory-public
    [ "$case_name" != wrong-repository ] || full_name=perfectory-inc/other
    printf '{"full_name":"%s","owner":{"login":"perfectory-inc"},"permissions":{"admin":%s}}\n' \
      "$full_name" "$admin"
    ;;
  repos/perfectory-inc/perfectory-public/collaborators\?*)
    case "$case_name" in
      other-direct-collaborator)
        printf '%s\n' '[[{"login":"other-user","permissions":{"admin":true}}]]'
        ;;
      self-direct-non-admin)
        printf '%s\n' '[[{"login":"sole-user","permissions":{"admin":false}}]]'
        ;;
      valid-self-direct)
        printf '%s\n' '[[{"login":"sole-user","permissions":{"admin":true}}]]'
        ;;
      *)
        printf '%s\n' '[[]]'
        ;;
    esac
    ;;
  repos/perfectory-inc/perfectory-public/teams\?*)
    [ "$case_name" != api-failure ] || exit 42
    if [ "$case_name" = repo-team ]; then
      printf '%s\n' '[[{"id":301,"slug":"writers"}]]'
    else
      printf '%s\n' '[[]]'
    fi
    ;;
  repos/perfectory-inc/perfectory-public/invitations\?*)
    if [ "$case_name" = repo-invitation ]; then
      printf '%s\n' '[[{"id":401,"permissions":"write"}]]'
    else
      printf '%s\n' '[[]]'
    fi
    ;;
  repos/perfectory-inc/perfectory-public/keys\?*)
    case "$case_name" in
      write-deploy-key)
        printf '%s\n' '[[{"id":501,"read_only":false}]]'
        ;;
      malformed-deploy-key)
        printf '%s\n' '[[{"id":502}]]'
        ;;
      valid-read-only-key)
        printf '%s\n' '[[{"id":503,"read_only":true}]]'
        ;;
      *)
        printf '%s\n' '[[]]'
        ;;
    esac
    ;;
  *)
    echo "fake-gh: unexpected endpoint '$endpoint'" >&2
    exit 100
    ;;
esac
FAKE_GH
chmod +x "$test_root/bin/gh"

guard="scripts/github/check-publication-authority.sh"
run_case() {
  local case_name="$1"
  local output_file="$test_root/$case_name.out"
  : >"$test_root/calls.log"
  PATH="$test_root/bin:$PATH" \
    GH_CALL_LOG="$test_root/calls.log" \
    FAKE_AUTHORITY_CASE="$case_name" \
    bash "$guard" >"$output_file" 2>&1
}

expect_reject() {
  local case_name="$1"
  if run_case "$case_name"; then
    echo "FAIL publication-authority-self-test: accepted $case_name" >&2
    exit 1
  fi
}

run_case valid
grep -Fq 'OK publication-authority' "$test_root/valid.out"

required_endpoints=(
  user
  orgs/perfectory-inc
  'orgs/perfectory-inc/members?filter=all&role=all&per_page=100'
  orgs/perfectory-inc/memberships/sole-user
  'orgs/perfectory-inc/outside_collaborators?filter=all&per_page=100'
  'orgs/perfectory-inc/invitations?per_page=100'
  'orgs/perfectory-inc/installations?per_page=100'
  repos/perfectory-inc/perfectory-public
  'repos/perfectory-inc/perfectory-public/collaborators?affiliation=direct&per_page=100'
  'repos/perfectory-inc/perfectory-public/teams?per_page=100'
  'repos/perfectory-inc/perfectory-public/invitations?per_page=100'
  'repos/perfectory-inc/perfectory-public/keys?per_page=100'
)
for endpoint in "${required_endpoints[@]}"; do
  [ "$(grep -Fxc -- "$endpoint" "$test_root/calls.log")" -eq 1 ] || {
    echo "FAIL publication-authority-self-test: endpoint not read exactly once: $endpoint" >&2
    exit 1
  }
done
[ "$(wc -l <"$test_root/calls.log" | tr -d '[:space:]')" -eq "${#required_endpoints[@]}" ]

run_case valid-self-direct
run_case valid-read-only-key
run_case valid-default-none

for invalid_case in \
  malformed-login \
  api-failure \
  default-write \
  wrong-org \
  extra-member \
  malformed-members \
  missing-member \
  inactive-membership \
  non-owner-membership \
  outside-collaborator \
  org-invitation \
  org-app \
  inconsistent-installations \
  no-repo-admin \
  wrong-repository \
  other-direct-collaborator \
  self-direct-non-admin \
  repo-team \
  repo-invitation \
  write-deploy-key \
  malformed-deploy-key; do
  expect_reject "$invalid_case"
done

if PATH="$test_root/bin:$PATH" GH_CALL_LOG="$test_root/calls.log" \
  GH_HOST=example.invalid bash "$guard" >/dev/null 2>&1; then
  echo "FAIL publication-authority-self-test: accepted non-GitHub host" >&2
  exit 1
fi

echo "OK publication-authority-self-test"
