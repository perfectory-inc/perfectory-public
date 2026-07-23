#!/usr/bin/env bash
# Exercises the stdlib-only JSON normalizer used by the GitHub configurator on
# both Linux and Git for Windows (where an external jq binary is not assumed).
set -euo pipefail
cd "$(dirname "$0")/../.."

helper="scripts/github/github-policy-json.py"
if [ ! -f "$helper" ]; then
  echo "FAIL github-policy-json-self-test: missing $helper" >&2
  exit 1
fi
helper_absolute="$(pwd -P)/$helper"

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

cat >"$test_root/ruleset.json" <<'JSON'
{
  "id": 42,
  "name": "synthetic",
  "target": "branch",
  "enforcement": "active",
  "source": "example/repository",
  "bypass_actors": [],
  "conditions": {"ref_name": {"exclude": [], "include": ["~DEFAULT_BRANCH"]}},
  "rules": [
    {
      "type": "required_status_checks",
      "parameters": {
        "strict_required_status_checks_policy": true,
        "required_status_checks": [
          {"integration_id": 15368, "context": "required/zeta"},
          {"context": "required/alpha", "integration_id": 15368}
        ],
        "do_not_enforce_on_create": true
      }
    },
    {
      "type": "pull_request",
      "parameters": {
        "required_reviewers": [],
        "required_review_thread_resolution": true,
        "required_approving_review_count": 0,
        "require_last_push_approval": false,
        "require_code_owner_review": false,
        "dismissal_restriction": {"enabled": false, "allowed_actors": []},
        "dismiss_stale_reviews_on_push": true,
        "allowed_merge_methods": ["squash"]
      }
    },
    {"type": "deletion"}
  ]
}
JSON

python3 "$helper" normalize-ruleset "$test_root/ruleset.json" \
  >"$test_root/normalized.json"
grep -q '"dismissal_restriction"' "$test_root/normalized.json"
grep -q '"required_reviewers"' "$test_root/normalized.json"
if grep -q '"id"\|"source"' "$test_root/normalized.json"; then
  echo "FAIL github-policy-json-self-test: server metadata survived normalization" >&2
  exit 1
fi

python3 "$helper" normalize-ruleset --without-status "$test_root/ruleset.json" \
  >"$test_root/baseline.json"
if grep -q 'required_status_checks' "$test_root/baseline.json"; then
  echo "FAIL github-policy-json-self-test: baseline retained status checks" >&2
  exit 1
fi

contexts="$(python3 "$helper" required-contexts "$test_root/ruleset.json")"
if [ "$contexts" != $'required/alpha\nrequired/zeta' ]; then
  echo "FAIL github-policy-json-self-test: required contexts were not canonical" >&2
  exit 1
fi

python3 -c 'import json,sys; value=json.load(open(sys.argv[1], encoding="utf-8")); value["source_type"]="Repository"; json.dump([value], open(sys.argv[2], "w", encoding="utf-8"))' \
  "$test_root/ruleset.json" "$test_root/rulesets.json"
python3 "$helper" ruleset-summaries --expected "$test_root/ruleset.json" \
  >"$test_root/expected-summaries.json"
python3 "$helper" ruleset-summaries "$test_root/rulesets.json" \
  >"$test_root/actual-summaries.json"
if ! diff -u "$test_root/expected-summaries.json" "$test_root/actual-summaries.json"; then
  echo "FAIL github-policy-json-self-test: ruleset summaries drifted" >&2
  exit 1
fi

cat >"$test_root/repository-identity.json" <<'JSON'
{
  "hostname": "github.com",
  "full_name": "perfectory-inc/perfectory-public",
  "repository_id": 123456789,
  "repository_node_id": "R_kgDOSynthetic",
  "owner": {
    "login": "perfectory-inc",
    "id": 306911903,
    "node_id": "O_kgDOEksanw"
  }
}
JSON
python3 "$helper" validate-repository-identity \
  "$test_root/repository-identity.json"

cat >"$test_root/duplicate-repository-id.json" <<'JSON'
{
  "hostname": "github.com",
  "full_name": "perfectory-inc/perfectory-public",
  "repository_id": 0,
  "repository_id": 123456789,
  "repository_node_id": "R_kgDOSynthetic",
  "owner": {
    "login": "perfectory-inc",
    "id": 306911903,
    "node_id": "O_kgDOEksanw"
  }
}
JSON
if python3 "$helper" validate-repository-identity \
  "$test_root/duplicate-repository-id.json" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: accepted an ambiguous duplicate JSON key" >&2
  exit 1
fi

sed 's/123456789/0/; s/R_kgDOSynthetic/UNSET_AFTER_REPOSITORY_CREATION/' \
  "$test_root/repository-identity.json" >"$test_root/unset-identity.json"
python3 "$helper" validate-repository-identity --allow-unset \
  "$test_root/unset-identity.json"
if python3 "$helper" validate-repository-identity \
  "$test_root/unset-identity.json" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: publication accepted unset repository identity" >&2
  exit 1
fi

sed 's/306911903/1/' "$test_root/unset-identity.json" \
  >"$test_root/wrong-owner.json"
if python3 "$helper" validate-repository-identity --allow-unset \
  "$test_root/wrong-owner.json" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: accepted wrong immutable owner identity" >&2
  exit 1
fi

cat >"$test_root/root-LICENSE" <<'EOF'
This repository is source-available proprietary software.

The authoritative license for first-party material is:

  LICENSES/LicenseRef-Proprietary.txt

Separately identified third-party material remains under its own license.
See THIRD_PARTY_NOTICES.md and REUSE.toml.
EOF
cat >"$test_root/legal-license.txt" <<'EOF'
Copyright (c) 2026 Perfectory. All rights reserved.

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
cat >"$test_root/legal-reuse.toml" <<'TOML'
version = 1

[[annotations]]
path = ["**"]
precedence = "override"
SPDX-FileCopyrightText = "2026 Perfectory"
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
mkdir -p \
  "$test_root/tools/github" \
  "$test_root/LICENSES" \
  "$test_root/products/gongzzang/apps/web/public/fonts"
cp -- .gitattributes "$test_root/.gitattributes"
cp -- tools/github/third-party-artifact-policy.json \
  "$test_root/tools/github/third-party-artifact-policy.json"
cp -- THIRD_PARTY_NOTICES.md "$test_root/THIRD_PARTY_NOTICES.md"
cp -- LICENSES/OFL-1.1.txt "$test_root/LICENSES/OFL-1.1.txt"
for artifact in \
  LICENSE-PRETENDARD.txt \
  pretendardvariable-dynamic-subset.css \
  pretendard-v1.3.9.sha256; do
  cp -- "products/gongzzang/apps/web/public/fonts/$artifact" \
    "$test_root/products/gongzzang/apps/web/public/fonts/$artifact"
done
cat >"$test_root/confirmed-legal-identity.json" <<'JSON'
{
  "copyright_holder": "Perfectory",
  "first_party_ownership_or_assignment_confirmed": true
}
JSON
python3 "$helper" validate-legal-identity \
  "$test_root/confirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml"

cat >"$test_root/unconfirmed-legal-identity.json" <<'JSON'
{
  "copyright_holder": "Perfectory",
  "first_party_ownership_or_assignment_confirmed": false
}
JSON
python3 "$helper" validate-legal-identity --allow-unconfirmed \
  "$test_root/unconfirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml"
if python3 "$helper" validate-legal-identity \
  "$test_root/unconfirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: publication accepted unconfirmed first-party ownership" >&2
  exit 1
fi

cp -- "$test_root/legal-reuse.toml" "$test_root/crates-mit-reuse.toml"
cat >>"$test_root/crates-mit-reuse.toml" <<'TOML'

[[annotations]]
path = ["crates/**"]
precedence = "override"
SPDX-FileCopyrightText = "2026 Unauthorized"
SPDX-License-Identifier = "MIT"
TOML
cp -- "$test_root/legal-reuse.toml" "$test_root/docs-apache-reuse.toml"
cat >>"$test_root/docs-apache-reuse.toml" <<'TOML'

[[annotations]]
path = ["docs/**"]
precedence = "override"
SPDX-FileCopyrightText = "2026 Unauthorized"
SPDX-License-Identifier = "Apache-2.0"
TOML
for mutation in crates-mit docs-apache; do
  if python3 "$helper" validate-legal-identity \
    "$test_root/confirmed-legal-identity.json" \
    "$test_root/root-LICENSE" \
    "$test_root/legal-license.txt" \
    "$test_root/$mutation-reuse.toml" >/dev/null 2>&1; then
    echo "FAIL github-policy-json-self-test: publication accepted unauthorized $mutation REUSE override" >&2
    exit 1
  fi
  if python3 "$helper" validate-legal-identity --allow-unconfirmed \
    "$test_root/unconfirmed-legal-identity.json" \
    "$test_root/root-LICENSE" \
    "$test_root/legal-license.txt" \
    "$test_root/$mutation-reuse.toml" >/dev/null 2>&1; then
    echo "FAIL github-policy-json-self-test: local lint accepted unauthorized $mutation REUSE override" >&2
    exit 1
  fi
done

cat >"$test_root/extra-legal-field.json" <<'JSON'
{
  "copyright_holder": "Perfectory",
  "first_party_ownership_or_assignment_confirmed": true,
  "reviewed_by": "nobody"
}
JSON
if python3 "$helper" validate-legal-identity --allow-unconfirmed \
  "$test_root/extra-legal-field.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: local lint accepted an unexpected legal identity field" >&2
  exit 1
fi
if python3 "$helper" validate-legal-identity \
  "$test_root/extra-legal-field.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: publication accepted an unexpected legal identity field" >&2
  exit 1
fi

cat >"$test_root/non-boolean-confirmation.json" <<'JSON'
{
  "copyright_holder": "Perfectory",
  "first_party_ownership_or_assignment_confirmed": "false"
}
JSON
if python3 "$helper" validate-legal-identity --allow-unconfirmed \
  "$test_root/non-boolean-confirmation.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: local lint accepted a non-boolean confirmation" >&2
  exit 1
fi

for unsafe_holder in '""' '" Perfectory"' '"Perfectory\\nInjected"'; do
  cat >"$test_root/unsafe-holder.json" <<JSON
{
  "copyright_holder": $unsafe_holder,
  "first_party_ownership_or_assignment_confirmed": false
}
JSON
  if python3 "$helper" validate-legal-identity --allow-unconfirmed \
    "$test_root/unsafe-holder.json" \
    "$test_root/root-LICENSE" \
    "$test_root/legal-license.txt" \
    "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
    echo "FAIL github-policy-json-self-test: local lint accepted unsafe copyright holder $unsafe_holder" >&2
    exit 1
  fi
done

sed 's/2026 Perfectory/2026 Somebody Else/' "$test_root/legal-license.txt" \
  >"$test_root/wrong-holder-license.txt"
if python3 "$helper" validate-legal-identity --allow-unconfirmed \
  "$test_root/unconfirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/wrong-holder-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: local lint accepted license holder drift" >&2
  exit 1
fi

sed 's/2026 Perfectory/2026 Somebody Else/' "$test_root/legal-reuse.toml" \
  >"$test_root/wrong-holder-reuse.toml"
if python3 "$helper" validate-legal-identity \
  "$test_root/confirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/wrong-holder-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: publication accepted REUSE holder drift" >&2
  exit 1
fi

if (
  cd "$test_root"
  python3 "$helper_absolute" validate-legal-identity \
    "$test_root/confirmed-legal-identity.json" \
    root-LICENSE legal-license.txt legal-reuse.toml >/dev/null 2>&1
); then
  echo "FAIL github-policy-json-self-test: accepted CWD-relative legal documents" >&2
  exit 1
fi

sed 's/precedence = "override"/precedence = "closest"/' \
  "$test_root/legal-reuse.toml" >"$test_root/wrong-precedence-reuse.toml"
if python3 "$helper" validate-legal-identity \
  "$test_root/confirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/wrong-precedence-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: accepted non-canonical root annotation precedence" >&2
  exit 1
fi

cat >"$test_root/duplicate-root-reuse.toml" <<'TOML'
version = 1

[[annotations]]
path = ["**"]
precedence = "override"
SPDX-FileCopyrightText = "2026 Perfectory"
SPDX-License-Identifier = "LicenseRef-Proprietary"

[[annotations]]
path = ["**"]
precedence = "override"
SPDX-FileCopyrightText = "2026 Somebody Else"
SPDX-License-Identifier = "MIT"
TOML
if python3 "$helper" validate-legal-identity \
  "$test_root/confirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/duplicate-root-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: accepted a second root-wide REUSE annotation" >&2
  exit 1
fi

cp -- "$test_root/legal-license.txt" "$test_root/appended-grant-license.txt"
printf '\nPermission is hereby granted to use, copy, modify, and distribute.\n' \
  >>"$test_root/appended-grant-license.txt"
if python3 "$helper" validate-legal-identity --allow-unconfirmed \
  "$test_root/unconfirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/appended-grant-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: local lint accepted an added proprietary grant" >&2
  exit 1
fi

cat >"$test_root/replaced-grant-license.txt" <<'EOF'
Copyright (c) 2026 Perfectory. All rights reserved.

Apache License, Version 2.0
Licensed under the Apache License, Version 2.0.
EOF
if python3 "$helper" validate-legal-identity \
  "$test_root/confirmed-legal-identity.json" \
  "$test_root/root-LICENSE" \
  "$test_root/replaced-grant-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: publication accepted a replaced proprietary grant" >&2
  exit 1
fi

cp -- "$test_root/root-LICENSE" "$test_root/permissive-root-LICENSE"
printf '\nPermission is hereby granted to use, copy, modify, and distribute.\n' \
  >>"$test_root/permissive-root-LICENSE"
if python3 "$helper" validate-legal-identity --allow-unconfirmed \
  "$test_root/unconfirmed-legal-identity.json" \
  "$test_root/permissive-root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: local lint accepted an added root license grant" >&2
  exit 1
fi

cat >"$test_root/replaced-root-LICENSE" <<'EOF'
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy.
EOF
if python3 "$helper" validate-legal-identity \
  "$test_root/confirmed-legal-identity.json" \
  "$test_root/replaced-root-LICENSE" \
  "$test_root/legal-license.txt" \
  "$test_root/legal-reuse.toml" >/dev/null 2>&1; then
  echo "FAIL github-policy-json-self-test: publication accepted a replaced root license grant" >&2
  exit 1
fi

echo "OK github-policy-json-self-test"
