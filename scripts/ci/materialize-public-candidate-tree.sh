#!/usr/bin/env bash
# Stable shell entry point for the structural candidate-tree materializer.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
for command_name in bash python3 realpath uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-candidate-tree: missing command '$command_name'" >&2
    exit 1
  }
done
export PERFECTORY_SAFE_GIT_TRANSPORT="$root/scripts/github/safe-git-transport.sh"
bash_executable="$(command -v bash)"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    command -v cygpath >/dev/null || {
      echo "FAIL public-candidate-tree: missing command 'cygpath'" >&2
      exit 1
    }
    bash_executable="$(cygpath -w "$bash_executable")"
    ;;
  *) bash_executable="$(realpath "$bash_executable")" ;;
esac
export PERFECTORY_BASH_EXECUTABLE="$bash_executable"
exec python3 "$root/scripts/ci/materialize-public-candidate-tree.py" "$@"
