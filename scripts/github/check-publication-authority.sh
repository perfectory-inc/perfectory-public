#!/usr/bin/env bash
# Read-only preflight for the one approved public repository writer.
#
# GitHub does not offer an atomic "freeze every principal" read. Concurrent
# reads narrow the TOCTOU window, but this proves an observed sole-principal
# assumption, not cryptographic exclusion: PAT, OAuth, or SSH credentials held
# by that same account remain authority. Run it immediately before the first
# publication write and abort when any response is unavailable or ambiguous.
set -euo pipefail

organization="perfectory-inc"
repository="perfectory-public"
target="$organization/$repository"
api_version="2026-03-10"

if [ -n "${GH_HOST:-}" ] && [ "$GH_HOST" != github.com ]; then
  echo "FAIL publication-authority: GH_HOST must be unset or github.com" >&2
  exit 1
fi
for command_name in gh mktemp python3; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL publication-authority: missing command '$command_name'" >&2
    exit 1
  }
done

snapshot="$(mktemp -d)"
cleanup() {
  case "${snapshot:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$snapshot" ] || rm -rf -- "$snapshot"
      ;;
    *)
      echo "publication-authority: refusing unsafe cleanup" >&2
      ;;
  esac
}
trap cleanup EXIT

# No fields, input, or method override are accepted here: every request is GET.
api() {
  env -u GH_HOST gh api --hostname github.com \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: $api_version" \
    "$@"
}

api_list() {
  api --paginate --slurp "$1"
}

# The login is intentionally obtained from the active gh identity. No personal
# account identifier belongs in the repository policy.
api user >"$snapshot/user.json"
authenticated_login="$(python3 - "$snapshot/user.json" <<'PY'
import json
import re
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as handle:
        payload = json.load(handle)
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"FAIL publication-authority: invalid authenticated-user response: {error}")

login = payload.get("login") if isinstance(payload, dict) else None
if not isinstance(login, str) or not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})", login):
    raise SystemExit("FAIL publication-authority: authenticated login is missing or malformed")
print(login)
PY
)"

# Fetch independent views concurrently to reduce, not eliminate, the interval
# in which GitHub-side authority could change between observations.
pids=()
run_object_read() {
  local output="$1"
  local endpoint="$2"
  api "$endpoint" >"$snapshot/$output" &
  pids+=("$!")
}
run_list_read() {
  local output="$1"
  local endpoint="$2"
  api_list "$endpoint" >"$snapshot/$output" &
  pids+=("$!")
}

run_object_read organization.json "orgs/$organization"
run_list_read members.json "orgs/$organization/members?filter=all&role=all&per_page=100"
run_object_read membership.json "orgs/$organization/memberships/$authenticated_login"
run_list_read outside-collaborators.json \
  "orgs/$organization/outside_collaborators?filter=all&per_page=100"
run_list_read organization-invitations.json \
  "orgs/$organization/invitations?per_page=100"
run_list_read installations.json "orgs/$organization/installations?per_page=100"
run_object_read repository.json "repos/$target"
run_list_read direct-collaborators.json \
  "repos/$target/collaborators?affiliation=direct&per_page=100"
run_list_read teams.json "repos/$target/teams?per_page=100"
run_list_read repository-invitations.json "repos/$target/invitations?per_page=100"
run_list_read deploy-keys.json "repos/$target/keys?per_page=100"

read_failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    read_failed=1
  fi
done
if [ "$read_failed" -ne 0 ]; then
  echo "FAIL publication-authority: one or more GitHub authority reads failed" >&2
  exit 1
fi

python3 - "$snapshot" "$authenticated_login" "$organization" "$target" <<'PY'
import json
import os
import sys

snapshot, authenticated_login, organization, target = sys.argv[1:]


def fail(message):
    raise SystemExit(f"FAIL publication-authority: {message}")


def load(name):
    path = os.path.join(snapshot, name)
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        fail(f"invalid {name} response: {error}")


def object_response(name):
    payload = load(name)
    if not isinstance(payload, dict):
        fail(f"{name} must be a JSON object")
    return payload


def complete_list(name):
    pages = load(name)
    if not isinstance(pages, list) or not pages:
        fail(f"{name} must be a non-empty slurped page list")
    flattened = []
    for page in pages:
        if not isinstance(page, list):
            fail(f"{name} contains a non-list page")
        flattened.extend(page)
    return flattened


user = object_response("user.json")
if user.get("login") != authenticated_login:
    fail("authenticated identity changed while validating")

org = object_response("organization.json")
if org.get("login") != organization:
    fail("organization identity does not match the fixed target")
if org.get("default_repository_permission") not in {"none", "read"}:
    fail("organization default repository permission must be none or read")

members = complete_list("members.json")
member_logins = []
for member in members:
    if not isinstance(member, dict) or not isinstance(member.get("login"), str):
        fail("organization member inventory contains an invalid member")
    member_logins.append(member["login"])
if member_logins != [authenticated_login]:
    fail("complete organization member inventory must contain only the authenticated user")

membership = object_response("membership.json")
membership_org = membership.get("organization")
membership_user = membership.get("user")
if membership.get("state") != "active" or membership.get("role") != "admin":
    fail("authenticated user must be an active organization owner")
if not isinstance(membership_org, dict) or membership_org.get("login") != organization:
    fail("organization membership response has the wrong organization")
if not isinstance(membership_user, dict) or membership_user.get("login") != authenticated_login:
    fail("organization membership response has the wrong user")

if complete_list("outside-collaborators.json"):
    fail("organization outside collaborators must be empty")
if complete_list("organization-invitations.json"):
    fail("pending organization invitations must be empty")

installation_pages = load("installations.json")
if not isinstance(installation_pages, list) or not installation_pages:
    fail("installations.json must be a non-empty slurped page list")
for page in installation_pages:
    if not isinstance(page, dict):
        fail("installations.json contains a non-object page")
    count = page.get("total_count")
    installations = page.get("installations")
    if isinstance(count, bool) or not isinstance(count, int):
        fail("organization installation total_count is missing or malformed")
    if not isinstance(installations, list):
        fail("organization installation page is missing its list")
    if count != 0 or installations:
        fail("organization GitHub App installations must be empty")

repo = object_response("repository.json")
owner = repo.get("owner")
permissions = repo.get("permissions")
if repo.get("full_name") != target:
    fail("repository identity does not match the fixed target")
if not isinstance(owner, dict) or owner.get("login") != organization:
    fail("repository owner does not match the fixed organization")
if not isinstance(permissions, dict) or permissions.get("admin") is not True:
    fail("authenticated user must have repository admin permission")

direct_collaborators = complete_list("direct-collaborators.json")
if len(direct_collaborators) > 1:
    fail("repository has more than the admitted direct collaborator")
if direct_collaborators:
    collaborator = direct_collaborators[0]
    if not isinstance(collaborator, dict):
        fail("repository direct collaborator response is malformed")
    collaborator_permissions = collaborator.get("permissions")
    if collaborator.get("login") != authenticated_login:
        fail("repository direct collaborator is not the authenticated user")
    if not isinstance(collaborator_permissions, dict) or collaborator_permissions.get("admin") is not True:
        fail("authenticated direct collaborator must have admin permission")

if complete_list("teams.json"):
    fail("repository teams must be empty")
if complete_list("repository-invitations.json"):
    fail("pending repository invitations must be empty")

for deploy_key in complete_list("deploy-keys.json"):
    if not isinstance(deploy_key, dict) or deploy_key.get("read_only") is not True:
        fail("repository deploy keys must be explicitly read-only")
PY

echo "OK publication-authority: sole write-capable GitHub principal observed for $target"
