#!/usr/bin/env bash
# Validates the immutable public repository identity from this exact control
# tree. Strict validation is the default.
set -euo pipefail

validation_args=()
case "$#" in
  0)
    ;;
  1)
    if [ "$1" != --allow-unset ]; then
      echo "usage: $0 [--allow-unset]" >&2
      exit 2
    fi
    validation_args=(--allow-unset)
    ;;
  *)
    echo "usage: $0 [--allow-unset]" >&2
    exit 2
    ;;
esac

control_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
json_helper="$control_root/scripts/github/github-policy-json.py"
repository_identity="$control_root/tools/github/repository-identity.json"

command -v python3 >/dev/null || {
  echo "FAIL public-repository-identity: missing command 'python3'" >&2
  exit 1
}
for required_file in "$json_helper" "$repository_identity"; do
  if [ ! -f "$required_file" ]; then
    echo "FAIL public-repository-identity: missing $required_file" >&2
    exit 1
  fi
done

exec python3 "$json_helper" validate-repository-identity \
  "${validation_args[@]}" "$repository_identity"
