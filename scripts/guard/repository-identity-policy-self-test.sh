#!/usr/bin/env bash
# Exercises repository identity policy with synthetic pinned and unset roots.
# No live GitHub or network operation is performed.
set -euo pipefail
cd "$(dirname "$0")/../.."
control_root="$(pwd -P)"

validator="scripts/github/validate-public-repository-identity.sh"
ci_gate="scripts/guard/repository-identity-ci.sh"
for required_file in "$validator" "$ci_gate"; do
  if [ ! -f "$required_file" ]; then
    echo "FAIL repository-identity-policy-self-test: missing $required_file" >&2
    exit 1
  fi
done
prepare_checker="scripts/guard/check-repository-identity-prepare.py"
if [ ! -f "$prepare_checker" ]; then
  echo "FAIL repository-identity-policy-self-test: missing $prepare_checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

make_identity_root() {
  local fixture_root="$1"
  local repository_id="$2"
  local repository_node_id="$3"
  mkdir -p \
    "$fixture_root/scripts/github" \
    "$fixture_root/scripts/guard" \
    "$fixture_root/tools/github"
  cp -- "$validator" \
    "$fixture_root/scripts/github/validate-public-repository-identity.sh"
  cp -- scripts/github/github-policy-json.py \
    "$fixture_root/scripts/github/github-policy-json.py"
  cp -- "$ci_gate" "$fixture_root/scripts/guard/repository-identity-ci.sh"
  cat >"$fixture_root/tools/github/repository-identity.json" <<JSON
{
  "hostname": "github.com",
  "full_name": "perfectory-inc/perfectory-public",
  "repository_id": $repository_id,
  "repository_node_id": "$repository_node_id",
  "owner": {
    "login": "perfectory-inc",
    "id": 306911903,
    "node_id": "O_kgDOEksanw"
  }
}
JSON
}

unset_root="$test_root/unset-root"
make_identity_root "$unset_root" 0 UNSET_AFTER_REPOSITORY_CREATION
bash "$unset_root/scripts/github/validate-public-repository-identity.sh" \
  --allow-unset
set +e
unset_output="$(
  bash "$unset_root/scripts/github/validate-public-repository-identity.sh" 2>&1
)"
unset_status=$?
set -e
if [ "$unset_status" -eq 0 ] \
  || ! printf '%s\n' "$unset_output" | grep -q 'immutable identity is unset'; then
  echo "FAIL repository-identity-policy-self-test: strict wrapper accepted unset fixture" >&2
  printf '%s\n' "$unset_output" >&2
  exit 1
fi

set +e
canonical_output="$(
  GITHUB_REPOSITORY=perfectory-inc/perfectory-public \
  GITHUB_REPOSITORY_ID=123456789 \
  GITHUB_REPOSITORY_OWNER_ID=306911903 \
    bash "$unset_root/scripts/guard/repository-identity-ci.sh" 2>&1
)"
canonical_status=$?
set -e
if [ "$canonical_status" -eq 0 ] \
  || ! printf '%s\n' "$canonical_output" | grep -q 'immutable identity is unset'; then
  echo "FAIL repository-identity-policy-self-test: canonical CI accepted unset fixture" >&2
  printf '%s\n' "$canonical_output" >&2
  exit 1
fi
GITHUB_REPOSITORY=perfectory-inc/perfectory-private \
  bash "$unset_root/scripts/guard/repository-identity-ci.sh"
env -u GITHUB_REPOSITORY \
  bash "$unset_root/scripts/guard/repository-identity-ci.sh"

pinned_root="$test_root/pinned-root"
make_identity_root "$pinned_root" 123456789 R_kgDOIdentityFixture
bash "$pinned_root/scripts/github/validate-public-repository-identity.sh"
GITHUB_REPOSITORY=perfectory-inc/perfectory-public \
GITHUB_REPOSITORY_ID=123456789 \
GITHUB_REPOSITORY_OWNER_ID=306911903 \
  bash "$pinned_root/scripts/guard/repository-identity-ci.sh"

set +e
missing_runtime_output="$(
  env -u GITHUB_REPOSITORY_ID -u GITHUB_REPOSITORY_OWNER_ID \
    GITHUB_REPOSITORY=perfectory-inc/perfectory-public \
    bash "$pinned_root/scripts/guard/repository-identity-ci.sh" 2>&1
)"
missing_runtime_status=$?
set -e
if [ "$missing_runtime_status" -eq 0 ] \
  || ! printf '%s\n' "$missing_runtime_output" \
    | grep -q 'canonical CI requires immutable runtime repository and owner IDs'; then
  echo "FAIL repository-identity-policy-self-test: canonical CI accepted missing runtime IDs" >&2
  printf '%s\n' "$missing_runtime_output" >&2
  exit 1
fi

for mismatch in repository owner; do
  runtime_repository_id=123456789
  runtime_owner_id=306911903
  if [ "$mismatch" = repository ]; then
    runtime_repository_id=987654321
  else
    runtime_owner_id=1
  fi
  set +e
  mismatch_output="$(
    GITHUB_REPOSITORY=perfectory-inc/perfectory-public \
    GITHUB_REPOSITORY_ID="$runtime_repository_id" \
    GITHUB_REPOSITORY_OWNER_ID="$runtime_owner_id" \
      bash "$pinned_root/scripts/guard/repository-identity-ci.sh" 2>&1
  )"
  mismatch_status=$?
  set -e
  if [ "$mismatch_status" -eq 0 ] \
    || ! printf '%s\n' "$mismatch_output" | grep -q 'runtime immutable identity drift'; then
    echo "FAIL repository-identity-policy-self-test: canonical CI accepted mismatched $mismatch ID" >&2
    printf '%s\n' "$mismatch_output" >&2
    exit 1
  fi
done

if bash "$validator" --unknown >/dev/null 2>&1 \
  || bash "$validator" --allow-unset extra >/dev/null 2>&1 \
  || GITHUB_REPOSITORY=perfectory-inc/perfectory-private \
    bash "$ci_gate" --allow-unset >/dev/null 2>&1; then
  echo "FAIL repository-identity-policy-self-test: caller selected an unsupported mode" >&2
  exit 1
fi

set +e
cwd_output="$(
  cd "$pinned_root"
  bash "$unset_root/scripts/github/validate-public-repository-identity.sh" 2>&1
)"
cwd_status=$?
set -e
if [ "$cwd_status" -eq 0 ] \
  || ! printf '%s\n' "$cwd_output" | grep -q 'immutable identity is unset'; then
  echo "FAIL repository-identity-policy-self-test: hostile CWD replaced identity policy" >&2
  printf '%s\n' "$cwd_output" >&2
  exit 1
fi

exploit_root="$test_root/hidden-identity-root"
destination="$test_root/hidden-identity-public.git"
mkdir -p \
  "$exploit_root/scripts/github" \
  "$exploit_root/tools/github" \
  "$exploit_root/LICENSES" \
  "$exploit_root/products/gongzzang/apps/web/public/fonts"
cp -- \
  "$control_root/scripts/github/prepare-public-root.sh" \
  "$control_root/scripts/github/build-public-root.sh" \
  "$control_root/scripts/github/safe-git-transport.sh" \
  "$control_root/scripts/github/github-policy-json.py" \
  "$control_root/scripts/github/validate-legal-publication.sh" \
  "$control_root/scripts/github/validate-public-repository-identity.sh" \
  "$exploit_root/scripts/github/"
sed '/^source_index_flags=/,/^fi$/d' \
  "$exploit_root/scripts/github/prepare-public-root.sh" \
  >"$exploit_root/scripts/github/prepare-public-root.mutant.sh"
mv -- "$exploit_root/scripts/github/prepare-public-root.mutant.sh" \
  "$exploit_root/scripts/github/prepare-public-root.sh"
cp -- \
  "$control_root/.gitattributes" \
  "$control_root/LICENSE" \
  "$control_root/REUSE.toml" \
  "$control_root/THIRD_PARTY_NOTICES.md" \
  "$exploit_root/"
cp -- \
  "$control_root/LICENSES/LicenseRef-Proprietary.txt" \
  "$control_root/LICENSES/OFL-1.1.txt" \
  "$exploit_root/LICENSES/"
cp -- \
  "$control_root/tools/github/third-party-artifact-policy.json" \
  "$exploit_root/tools/github/third-party-artifact-policy.json"
cp -- \
  "$control_root/products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt" \
  "$control_root/products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256" \
  "$control_root/products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css" \
  "$exploit_root/products/gongzzang/apps/web/public/fonts/"
cp -- "$control_root/tools/github/public-root-identity.json" \
  "$exploit_root/tools/github/public-root-identity.json"
cat >"$exploit_root/tools/github/legal-identity.json" <<'JSON'
{
  "copyright_holder": "Perfectory",
  "first_party_ownership_or_assignment_confirmed": true
}
JSON
cat >"$exploit_root/tools/github/repository-identity.json" <<'JSON'
{
  "hostname": "github.com",
  "full_name": "perfectory-inc/perfectory-public",
  "repository_id": 0,
  "repository_node_id": "UNSET_AFTER_REPOSITORY_CREATION",
  "owner": {
    "login": "perfectory-inc",
    "id": 306911903,
    "node_id": "O_kgDOEksanw"
  }
}
JSON
safe_git="$exploit_root/scripts/github/safe-git-transport.sh"
bash "$safe_git" --no-repository \
  init -q --initial-branch=main "$exploit_root"
bash "$safe_git" --repository "$exploit_root" add -- .
GIT_AUTHOR_NAME=Identity-Test \
GIT_AUTHOR_EMAIL=identity-test@perfectory.invalid \
GIT_COMMITTER_NAME=Identity-Test \
GIT_COMMITTER_EMAIL=identity-test@perfectory.invalid \
GIT_AUTHOR_DATE='@1784764800 +0000' \
GIT_COMMITTER_DATE='@1784764800 +0000' \
  bash "$safe_git" --trusted-commit-identity \
    --repository "$exploit_root" commit -qm "test: unset repository identity"
source_commit="$(
  bash "$safe_git" --repository "$exploit_root" rev-parse HEAD
)"
bash "$safe_git" --repository "$exploit_root" update-index \
  --skip-worktree -- tools/github/repository-identity.json
cat >"$exploit_root/tools/github/repository-identity.json" <<'JSON'
{
  "hostname": "github.com",
  "full_name": "perfectory-inc/perfectory-public",
  "repository_id": 123456789,
  "repository_node_id": "R_kgDOIdentityFixture",
  "owner": {
    "login": "perfectory-inc",
    "id": 306911903,
    "node_id": "O_kgDOEksanw"
  }
}
JSON
if [ -n "$(
  bash "$safe_git" --repository "$exploit_root" \
    status --porcelain=v1 --untracked-files=all
)" ]; then
  echo "FAIL repository-identity-policy-self-test: hidden identity fixture is not clean" >&2
  exit 1
fi
set +e
prepare_output="$(
  bash "$exploit_root/scripts/github/prepare-public-root.sh" \
    "$exploit_root" "$source_commit" "$destination" 2>&1
)"
prepare_status=$?
set -e
if [ "$prepare_status" -eq 0 ] \
  || ! printf '%s\n' "$prepare_output" | grep -q 'immutable identity is unset' \
  || printf '%s\n' "$prepare_output" | grep -q 'OK workflow-policy' \
  || [ ! -d "$destination" ]; then
  echo "FAIL repository-identity-policy-self-test: cloned unset identity bypassed strict validation" >&2
  printf '%s\n' "$prepare_output" >&2
  exit 1
fi

python3 "$prepare_checker" scripts/github/prepare-public-root.sh
wiring_root="$test_root/wiring-mutants"
mkdir -p "$wiring_root"
sed '/^  bash scripts\/github\/validate-public-repository-identity\.sh$/d' \
  scripts/github/prepare-public-root.sh >"$wiring_root/removed.sh"
sed 's#^  bash scripts/github/validate-public-repository-identity.sh$#  bash scripts/github/validate-public-repository-identity.sh --allow-unset#' \
  scripts/github/prepare-public-root.sh >"$wiring_root/allowed.sh"
awk '
  $0 == "  bash scripts/github/validate-public-repository-identity.sh" { next }
  { print }
  $0 == "  bash scripts/guard/monorepo-guard.sh" {
    print "  bash scripts/github/validate-public-repository-identity.sh"
  }
' scripts/github/prepare-public-root.sh >"$wiring_root/reordered.sh"
for mutation in removed allowed reordered; do
  if python3 "$prepare_checker" "$wiring_root/$mutation.sh" \
    >/dev/null 2>&1; then
    echo "FAIL repository-identity-policy-self-test: checker accepted $mutation clone gate" >&2
    exit 1
  fi
done

echo "OK repository-identity-policy-self-test"
