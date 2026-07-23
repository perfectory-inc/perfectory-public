#!/usr/bin/env bash
# Validates the legal publication self-attestation and its public notices from
# this exact control tree. Strict validation is the default.
set -euo pipefail

validation_args=()
case "$#" in
  0)
    ;;
  1)
    if [ "$1" != --allow-unconfirmed ]; then
      echo "usage: $0 [--allow-unconfirmed]" >&2
      exit 2
    fi
    validation_args=(--allow-unconfirmed)
    ;;
  *)
    echo "usage: $0 [--allow-unconfirmed]" >&2
    exit 2
    ;;
esac

control_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
json_helper="$control_root/scripts/github/github-policy-json.py"
legal_identity="$control_root/tools/github/legal-identity.json"
root_license_file="$control_root/LICENSE"
proprietary_license_file="$control_root/LICENSES/LicenseRef-Proprietary.txt"
reuse_file="$control_root/REUSE.toml"
third_party_artifact_policy="$control_root/tools/github/third-party-artifact-policy.json"

command -v python3 >/dev/null || {
  echo "FAIL legal-publication: missing command 'python3'" >&2
  exit 1
}
for required_file in \
  "$json_helper" "$legal_identity" "$root_license_file" \
  "$proprietary_license_file" "$reuse_file" \
  "$third_party_artifact_policy"; do
  if [ ! -f "$required_file" ]; then
    echo "FAIL legal-publication: missing $required_file" >&2
    exit 1
  fi
done

exec python3 "$json_helper" validate-legal-identity \
  "${validation_args[@]}" \
  "$legal_identity" "$root_license_file" \
  "$proprietary_license_file" "$reuse_file"
