#!/usr/bin/env bash
# Proves that legal publication pins the reviewed third-party projection and
# provenance artifacts, rather than trusting mutable notices in the candidate.
set -euo pipefail
cd "$(dirname "$0")/../.."

validator="scripts/github/validate-legal-publication.sh"
helper="scripts/github/github-policy-json.py"
test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

if [ ! -f .gitattributes ]; then
  echo "FAIL third-party-artifact-policy-self-test: missing root .gitattributes byte contract" >&2
  exit 1
fi
attribute_root="$test_root/attribute-root"
mkdir -p "$attribute_root"
cp -- .gitattributes "$attribute_root/.gitattributes"
git init -q --initial-branch=main "$attribute_root"
for text_artifact in \
  .gitattributes \
  THIRD_PARTY_NOTICES.md \
  LICENSES/OFL-1.1.txt \
  products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt \
  products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css \
  products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256; do
  attributes="$(git -C "$attribute_root" check-attr text eol -- "$text_artifact")"
  if ! printf '%s\n' "$attributes" \
    | grep -Fq "$text_artifact: text: auto" \
    || ! printf '%s\n' "$attributes" \
      | grep -Fq "$text_artifact: eol: lf"; then
    echo "FAIL third-party-artifact-policy-self-test: exact-hashed text lacks LF checkout contract: $text_artifact" >&2
    printf '%s\n' "$attributes" >&2
    exit 1
  fi
done
for windows_script in \
  fixture.cmd FIXTURE.CMD fixture.bat FIXTURE.BAT \
  products/gongzzang/fixture.cmd products/gongzzang/FIXTURE.CMD \
  products/gongzzang/fixture.bat products/gongzzang/FIXTURE.BAT; do
  attributes="$(git -C "$attribute_root" check-attr text eol -- "$windows_script")"
  if ! printf '%s\n' "$attributes" \
    | grep -Fq "$windows_script: text: set" \
    || ! printf '%s\n' "$attributes" \
      | grep -Fq "$windows_script: eol: crlf"; then
    echo "FAIL third-party-artifact-policy-self-test: Windows script lacks CRLF checkout contract: $windows_script" >&2
    printf '%s\n' "$attributes" >&2
    exit 1
  fi
done
font_sample="products/gongzzang/apps/web/public/fonts/woff2-dynamic-subset/fixture.woff2"
font_attributes="$(git -C "$attribute_root" check-attr text -- "$font_sample")"
if ! printf '%s\n' "$font_attributes" \
    | grep -Fq "$font_sample: text: unset"; then
  echo "FAIL third-party-artifact-policy-self-test: WOFF2 assets lack binary checkout contract" >&2
  printf '%s\n' "$font_attributes" >&2
  exit 1
fi

fixture_root="$test_root/root"
mkdir -p \
  "$fixture_root/scripts/github" \
  "$fixture_root/tools/github" \
  "$fixture_root/LICENSES" \
  "$fixture_root/products/gongzzang/apps/web/public/fonts"
cp -- "$validator" "$fixture_root/scripts/github/validate-legal-publication.sh"
cp -- "$helper" "$fixture_root/scripts/github/github-policy-json.py"
cp -- .gitattributes LICENSE REUSE.toml THIRD_PARTY_NOTICES.md "$fixture_root/"
cp -- LICENSES/LicenseRef-Proprietary.txt LICENSES/OFL-1.1.txt \
  "$fixture_root/LICENSES/"
for artifact in \
  LICENSE-PRETENDARD.txt \
  pretendardvariable-dynamic-subset.css \
  pretendard-v1.3.9.sha256; do
  cp -- "products/gongzzang/apps/web/public/fonts/$artifact" \
    "$fixture_root/products/gongzzang/apps/web/public/fonts/$artifact"
done
cat >"$fixture_root/tools/github/legal-identity.json" <<'JSON'
{
  "copyright_holder": "Perfectory",
  "first_party_ownership_or_assignment_confirmed": true
}
JSON
cat >"$fixture_root/tools/github/third-party-artifact-policy.json" <<'JSON'
{
  "version": 1,
  "artifacts": {
    ".gitattributes": "12f83490d9118abfea02bc0f591ba8049bf52b3567989e076e28e0f601a891ad",
    "LICENSES/OFL-1.1.txt": "85fce85e25260b03777bf10373d3bd9363b9da96d9e0ca86a280dd37ed7667a0",
    "THIRD_PARTY_NOTICES.md": "40fc0d6d731a98ba7e0ff8d81d8480c18d9105a36869fdf96ac8285a09d0b298",
    "products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt": "85fce85e25260b03777bf10373d3bd9363b9da96d9e0ca86a280dd37ed7667a0",
    "products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256": "7f5bcd2f7cc28bbaaae586db37f07691094048a09fc25167dfd02a88bec863de",
    "products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css": "bdb7c52485b4bb6a737c931be9aa26604e3513abca57c6d9c12eae739e02a4e6"
  }
}
JSON

expect_rejected_in_both_modes() {
  local label="$1"
  if bash "$fixture_root/scripts/github/validate-legal-publication.sh" \
    >/dev/null 2>&1 \
    || bash "$fixture_root/scripts/github/validate-legal-publication.sh" \
      --allow-unconfirmed >/dev/null 2>&1; then
    echo "FAIL third-party-artifact-policy-self-test: legal validation accepted $label" >&2
    exit 1
  fi
}

bash "$fixture_root/scripts/github/validate-legal-publication.sh"
bash "$fixture_root/scripts/github/validate-legal-publication.sh" \
  --allow-unconfirmed

printf '\nUnauthorized claim: crates/** is third-party MIT.\n' \
  >>"$fixture_root/THIRD_PARTY_NOTICES.md"
expect_rejected_in_both_modes "a reclassified third-party notice"
cp -- THIRD_PARTY_NOTICES.md "$fixture_root/THIRD_PARTY_NOTICES.md"

cp -- "$fixture_root/tools/github/third-party-artifact-policy.json" \
  "$test_root/baseline-policy.json"
sed 's/bdb7c52485b4bb6a737c931be9aa26604e3513abca57c6d9c12eae739e02a4e6/0000000000000000000000000000000000000000000000000000000000000000/' \
  "$test_root/baseline-policy.json" \
  >"$fixture_root/tools/github/third-party-artifact-policy.json"
expect_rejected_in_both_modes "a drifted artifact hash"
cp -- "$test_root/baseline-policy.json" \
  "$fixture_root/tools/github/third-party-artifact-policy.json"

python3 - "$fixture_root/tools/github/third-party-artifact-policy.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    policy = json.load(handle)
del policy["artifacts"][
    "products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css"
]
with open(path, "w", encoding="utf-8", newline="\n") as handle:
    json.dump(policy, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
expect_rejected_in_both_modes "a missing artifact policy path"
cp -- "$test_root/baseline-policy.json" \
  "$fixture_root/tools/github/third-party-artifact-policy.json"

python3 - "$fixture_root/tools/github/third-party-artifact-policy.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    policy = json.load(handle)
policy["artifacts"]["../outside.txt"] = "0" * 64
with open(path, "w", encoding="utf-8", newline="\n") as handle:
    json.dump(policy, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
expect_rejected_in_both_modes "an unsafe extra artifact path"
cp -- "$test_root/baseline-policy.json" \
  "$fixture_root/tools/github/third-party-artifact-policy.json"

bundled_license="$fixture_root/products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt"
printf '\nmutated copy\n' >>"$bundled_license"
mutated_hash="$(sha256sum "$bundled_license" | awk '{print $1}')"
python3 - \
  "$fixture_root/tools/github/third-party-artifact-policy.json" \
  "$mutated_hash" <<'PY'
import json
import sys

path, digest = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    policy = json.load(handle)
policy["artifacts"][
    "products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt"
] = digest
with open(path, "w", encoding="utf-8", newline="\n") as handle:
    json.dump(policy, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
expect_rejected_in_both_modes "non-identical canonical and bundled OFL copies"

echo "OK third-party-artifact-policy-self-test"
