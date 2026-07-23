#!/usr/bin/env bash
# Proves the legal publication wrapper is strict by default and anchored to
# the exact tree that contains it. This test performs no network operations.
set -euo pipefail
cd "$(dirname "$0")/../.."
control_root="$(pwd -P)"

validator="scripts/github/validate-legal-publication.sh"
if [ ! -f "$validator" ]; then
  echo "FAIL legal-publication-self-test: missing $validator" >&2
  exit 1
fi
ci_gate="scripts/guard/legal-publication-ci.sh"
if [ ! -f "$ci_gate" ]; then
  echo "FAIL legal-publication-self-test: missing $ci_gate" >&2
  exit 1
fi
prepare_checker="scripts/guard/check-legal-publication-prepare.py"
if [ ! -f "$prepare_checker" ]; then
  echo "FAIL legal-publication-self-test: missing $prepare_checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

make_legal_root() {
  local fixture_root="$1"
  local holder="$2"
  local confirmed="$3"
  mkdir -p \
    "$fixture_root/scripts/github" \
    "$fixture_root/scripts/guard" \
    "$fixture_root/tools/github" \
    "$fixture_root/LICENSES" \
    "$fixture_root/products/gongzzang/apps/web/public/fonts"
  cp -- "$validator" \
    "$fixture_root/scripts/github/validate-legal-publication.sh"
  cp -- scripts/github/github-policy-json.py \
    "$fixture_root/scripts/github/github-policy-json.py"
  cp -- "$ci_gate" "$fixture_root/scripts/guard/legal-publication-ci.sh"
  cp -- tools/github/third-party-artifact-policy.json \
    "$fixture_root/tools/github/third-party-artifact-policy.json"
  cp -- .gitattributes "$fixture_root/.gitattributes"
  cp -- THIRD_PARTY_NOTICES.md "$fixture_root/THIRD_PARTY_NOTICES.md"
  cp -- LICENSES/OFL-1.1.txt "$fixture_root/LICENSES/OFL-1.1.txt"
  for artifact in \
    LICENSE-PRETENDARD.txt \
    pretendardvariable-dynamic-subset.css \
    pretendard-v1.3.9.sha256; do
    cp -- "products/gongzzang/apps/web/public/fonts/$artifact" \
      "$fixture_root/products/gongzzang/apps/web/public/fonts/$artifact"
  done
  cat >"$fixture_root/tools/github/legal-identity.json" <<JSON
{
  "copyright_holder": "$holder",
  "first_party_ownership_or_assignment_confirmed": $confirmed
}
JSON
  cat >"$fixture_root/LICENSE" <<'EOF'
This repository is source-available proprietary software.

The authoritative license for first-party material is:

  LICENSES/LicenseRef-Proprietary.txt

Separately identified third-party material remains under its own license.
See THIRD_PARTY_NOTICES.md and REUSE.toml.
EOF
  cat >"$fixture_root/LICENSES/LicenseRef-Proprietary.txt" <<EOF
Copyright (c) 2026 $holder. All rights reserved.

This source code and its accompanying first-party materials are proprietary.
Except under a separate written agreement with the copyright holder, no
permission is granted to use, reproduce, modify, translate, publish,
distribute, sublicense, sell, deploy, publicly perform, publicly display, or
create derivative works from them.

Public availability on GitHub does not create an open-source license. Nothing
in this notice limits the rights necessarily granted to GitHub or exercised by
GitHub users solely through GitHub's service features under the GitHub Terms of
Service, including viewing and forking within the service. Those service-level
rights do not grant permission to use or distribute the software outside the
scope of those terms.

Separately identified third-party materials are excluded from this proprietary
license and remain governed by their respective license notices.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, TITLE, AND NON-INFRINGEMENT. IN NO EVENT
SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER
LIABILITY ARISING FROM OR RELATED TO THE SOFTWARE OR ITS USE.
EOF
  cat >"$fixture_root/REUSE.toml" <<TOML
version = 1

[[annotations]]
path = ["**"]
precedence = "override"
SPDX-FileCopyrightText = "2026 $holder"
SPDX-License-Identifier = "LicenseRef-Proprietary"

[[annotations]]
path = [
  "products/gongzzang/apps/web/public/fonts/**/*.woff2",
  "products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css",
  "products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt",
]
precedence = "override"
SPDX-FileCopyrightText = "2021 Kil Hyung-jin"
SPDX-License-Identifier = "OFL-1.1"
TOML
}

bash "$validator" --allow-unconfirmed
confirmation="$(python3 -c 'import json; print(str(json.load(open("tools/github/legal-identity.json", encoding="utf-8"))["first_party_ownership_or_assignment_confirmed"]).lower())')"

set +e
strict_output="$(bash "$validator" 2>&1)"
strict_status=$?
set -e
if [ "$confirmation" = true ]; then
  if [ "$strict_status" -ne 0 ]; then
    echo "FAIL legal-publication-self-test: confirmed SSOT failed strict validation" >&2
    printf '%s\n' "$strict_output" >&2
    exit 1
  fi
elif [ "$confirmation" = false ]; then
  if [ "$strict_status" -eq 0 ] \
    || ! printf '%s\n' "$strict_output" | grep -q 'publication denied'; then
    echo "FAIL legal-publication-self-test: provisional SSOT did not fail strict validation" >&2
    printf '%s\n' "$strict_output" >&2
    exit 1
  fi
else
  echo "FAIL legal-publication-self-test: unexpected confirmation state" >&2
  exit 1
fi

if bash "$validator" --unknown >/dev/null 2>&1 \
  || bash "$validator" --allow-unconfirmed extra >/dev/null 2>&1; then
  echo "FAIL legal-publication-self-test: wrapper accepted an unsupported argument" >&2
  exit 1
fi

exercise_hidden_index_flag() {
  local flag="$1"
  local expected_defense="${2:-early}"
  local label="${flag#--}-$expected_defense"
  local exploit_root="$test_root/$label-root"
  local destination="$test_root/$label-public.git"
  local safe_git source_commit prepare_output prepare_status

  make_legal_root "$exploit_root" "Hidden Index Example" false
  cp -- \
    "$control_root/scripts/github/prepare-public-root.sh" \
    "$control_root/scripts/github/build-public-root.sh" \
    "$control_root/scripts/github/safe-git-transport.sh" \
    "$control_root/scripts/github/validate-public-repository-identity.sh" \
    "$exploit_root/scripts/github/"
  safe_git="$exploit_root/scripts/github/safe-git-transport.sh"
  if [ "$expected_defense" = clone ]; then
    sed '/^source_index_flags=/,/^fi$/d' \
      "$exploit_root/scripts/github/prepare-public-root.sh" \
      >"$exploit_root/scripts/github/prepare-public-root.mutant.sh"
    mv -- "$exploit_root/scripts/github/prepare-public-root.mutant.sh" \
      "$exploit_root/scripts/github/prepare-public-root.sh"
  elif [ "$expected_defense" != early ]; then
    echo "FAIL legal-publication-self-test: invalid expected defense" >&2
    exit 1
  fi
  cp -- "$control_root/tools/github/public-root-identity.json" \
    "$exploit_root/tools/github/public-root-identity.json"
  cat >"$exploit_root/tools/github/repository-identity.json" <<'JSON'
{
  "hostname": "github.com",
  "full_name": "perfectory-inc/perfectory-public",
  "repository_id": 123456789,
  "repository_node_id": "R_kgDOLegalFixture",
  "owner": {
    "login": "perfectory-inc",
    "id": 306911903,
    "node_id": "O_kgDOEksanw"
  }
}
JSON
  bash "$safe_git" --no-repository \
    init -q --initial-branch=main "$exploit_root"
  bash "$safe_git" --repository "$exploit_root" add -- .
  GIT_AUTHOR_NAME=Legal-Test \
  GIT_AUTHOR_EMAIL=legal-test@perfectory.invalid \
  GIT_COMMITTER_NAME=Legal-Test \
  GIT_COMMITTER_EMAIL=legal-test@perfectory.invalid \
  GIT_AUTHOR_DATE='@1784764800 +0000' \
  GIT_COMMITTER_DATE='@1784764800 +0000' \
    bash "$safe_git" --trusted-commit-identity \
      --repository "$exploit_root" commit -qm "test: provisional legal tree"
  source_commit="$(
    bash "$safe_git" --repository "$exploit_root" rev-parse HEAD
  )"
  bash "$safe_git" --repository "$exploit_root" update-index "$flag" -- \
    tools/github/legal-identity.json
  cat >"$exploit_root/tools/github/legal-identity.json" <<'JSON'
{
  "copyright_holder": "Hidden Index Example",
  "first_party_ownership_or_assignment_confirmed": true
}
JSON
  if [ -n "$(
    bash "$safe_git" --repository "$exploit_root" \
      status --porcelain=v1 --untracked-files=all
  )" ]; then
    echo "FAIL legal-publication-self-test: $label fixture is not status-clean" >&2
    exit 1
  fi

  set +e
  prepare_output="$(
    bash "$exploit_root/scripts/github/prepare-public-root.sh" \
      "$exploit_root" "$source_commit" "$destination" 2>&1
  )"
  prepare_status=$?
  set -e
  if [ "$expected_defense" = early ]; then
    if [ "$prepare_status" -eq 0 ] \
      || ! printf '%s\n' "$prepare_output" \
        | grep -q 'source index contains skip-worktree or assume-unchanged entries' \
      || [ -e "$destination" ]; then
      echo "FAIL legal-publication-self-test: $label concealed a false committed attestation" >&2
      printf '%s\n' "$prepare_output" >&2
      exit 1
    fi
  elif [ "$prepare_status" -eq 0 ] \
    || ! printf '%s\n' "$prepare_output" | grep -q 'publication denied' \
    || printf '%s\n' "$prepare_output" | grep -q 'OK workflow-policy' \
    || [ ! -d "$destination" ]; then
    echo "FAIL legal-publication-self-test: cloned false commit bypassed authoritative strict validation" >&2
    printf '%s\n' "$prepare_output" >&2
    exit 1
  fi
}

exercise_hidden_index_flag --skip-worktree early
exercise_hidden_index_flag --assume-unchanged early
exercise_hidden_index_flag --skip-worktree clone

python3 "$prepare_checker" scripts/github/prepare-public-root.sh
wiring_root="$test_root/wiring-mutants"
mkdir -p "$wiring_root"
sed '/^  bash scripts\/github\/validate-legal-publication\.sh$/d' \
  scripts/github/prepare-public-root.sh >"$wiring_root/removed.sh"
sed 's#^  bash scripts/github/validate-legal-publication.sh$#  bash scripts/github/validate-legal-publication.sh --allow-unconfirmed#' \
  scripts/github/prepare-public-root.sh >"$wiring_root/allowed.sh"
awk '
  $0 == "  bash scripts/github/validate-legal-publication.sh" { next }
  { print }
  $0 == "  bash scripts/guard/monorepo-guard.sh" {
    print "  bash scripts/github/validate-legal-publication.sh"
  }
' scripts/github/prepare-public-root.sh >"$wiring_root/reordered.sh"
for mutation in removed allowed reordered; do
  if python3 "$prepare_checker" "$wiring_root/$mutation.sh" \
    >/dev/null 2>&1; then
    echo "FAIL legal-publication-self-test: checker accepted $mutation clone gate" >&2
    exit 1
  fi
done

fake_cwd="$test_root/fake-cwd"
make_legal_root "$fake_cwd" "CWD Impostor" true
provisional_root="$test_root/provisional-root"
make_legal_root "$provisional_root" "Provisional Example" false
set +e
provisional_ci_output="$(
  GITHUB_REPOSITORY=perfectory-inc/perfectory-public \
    bash "$provisional_root/scripts/guard/legal-publication-ci.sh" 2>&1
)"
provisional_ci_status=$?
set -e
if [ "$provisional_ci_status" -eq 0 ] \
  || ! printf '%s\n' "$provisional_ci_output" | grep -q 'publication denied'; then
  echo "FAIL legal-publication-self-test: canonical CI accepted provisional fixture" >&2
  printf '%s\n' "$provisional_ci_output" >&2
  exit 1
fi
GITHUB_REPOSITORY=perfectory-inc/perfectory-private \
  bash "$provisional_root/scripts/guard/legal-publication-ci.sh"
env -u GITHUB_REPOSITORY \
  bash "$provisional_root/scripts/guard/legal-publication-ci.sh"
set +e
cwd_output="$(
  cd "$fake_cwd"
  bash "$provisional_root/scripts/github/validate-legal-publication.sh" 2>&1
)"
cwd_status=$?
set -e
if [ "$cwd_status" -eq 0 ] \
  || ! printf '%s\n' "$cwd_output" | grep -q 'publication denied'; then
  echo "FAIL legal-publication-self-test: hostile CWD replaced the control-root policy" >&2
  printf '%s\n' "$cwd_output" >&2
  exit 1
fi

confirmed_root="$test_root/confirmed-root"
make_legal_root "$confirmed_root" "Confirmed Example" true
bash "$confirmed_root/scripts/github/validate-legal-publication.sh"
GITHUB_REPOSITORY=perfectory-inc/perfectory-public \
  bash "$confirmed_root/scripts/guard/legal-publication-ci.sh"

permissive_root="$test_root/permissive-root"
make_legal_root "$permissive_root" "Confirmed Example" true
printf '\nPermission is hereby granted to use, copy, modify, and distribute.\n' \
  >>"$permissive_root/LICENSE"
if bash "$permissive_root/scripts/github/validate-legal-publication.sh" \
  >/dev/null 2>&1; then
  echo "FAIL legal-publication-self-test: wrapper accepted a root license grant" >&2
  exit 1
fi

GITHUB_REPOSITORY=perfectory-inc/perfectory-private bash "$ci_gate"
env -u GITHUB_REPOSITORY bash "$ci_gate"
set +e
canonical_output="$(
  GITHUB_REPOSITORY=perfectory-inc/perfectory-public bash "$ci_gate" 2>&1
)"
canonical_status=$?
set -e
if [ "$confirmation" = true ]; then
  if [ "$canonical_status" -ne 0 ]; then
    echo "FAIL legal-publication-self-test: canonical required CI rejected confirmed legal identity" >&2
    printf '%s\n' "$canonical_output" >&2
    exit 1
  fi
elif [ "$canonical_status" -eq 0 ] \
  || ! printf '%s\n' "$canonical_output" | grep -q 'publication denied'; then
  echo "FAIL legal-publication-self-test: canonical required CI accepted provisional legal identity" >&2
  printf '%s\n' "$canonical_output" >&2
  exit 1
fi
if GITHUB_REPOSITORY=perfectory-inc/perfectory-private \
  bash "$ci_gate" --allow-unconfirmed >/dev/null 2>&1; then
  echo "FAIL legal-publication-self-test: CI gate accepted a caller-controlled mode" >&2
  exit 1
fi

echo "OK legal-publication-self-test"
