#!/usr/bin/env bash
# The Alertmanager config passes amtool's check, and every alert in alertmanager.routes.test reaches the
# receiver it names (root ADR-0591).
#
# What real incident does failing this prevent? 2026-11-18/19 #alerts got about 480 messages a day and
# nobody could read it: tickets were sent one per job with a resolved message each. The fix routes
# every ticket but the security advisories to a batched receiver; a matcher typo, a route moved above
# it, or a new service route that catches tickets first sends them back to one message per job, and
# nothing in the repository would say so. amtool, Alertmanager's own route tester, resolves each case
# through the routing tree the way the server does.
#
# amtool comes from the pinned Alertmanager release (tools/amtool.env, root ADR-0197); its version must
# equal the Alertmanager image compose runs, so the routes are tested by the engine that routes them.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
name="alertmanager-routes-pass-their-tests"
fetcher="$(cd "$(dirname "$0")/../.." && pwd -P)/scripts/ci/fetch_pinned.py"
for command_name in mktemp python3 tar uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL ${name}: missing command '$command_name'" >&2
    exit 1
  }
done

case "$(uname -s)" in
  Linux) os=linux; executable_name=amtool ;;
  Darwin) os=darwin; executable_name=amtool ;;
  MINGW*|MSYS*|CYGWIN*) os=windows; executable_name=amtool.exe ;;
  *) echo "FAIL ${name}: unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "FAIL ${name}: unsupported machine $(uname -m)" >&2; exit 1 ;;
esac

# The pin and the image: one version for both.
pinned="$(sed -n 's/^AMTOOL_VERSION=//p' "$root/tools/amtool.env")"
image="$(sed -n 's/^ *image: prom\/alertmanager:v\([^@]*\)@.*/\1/p' "$root/platforms/foundation-platform/compose.observability.yml")"
if [ -z "$pinned" ] || [ "$pinned" != "$image" ]; then
  echo "FAIL ${name}: amtool ${pinned:-<none>} (tools/amtool.env) is not the Alertmanager image ${image:-<none>} (compose.observability.yml)" >&2
  exit 1
fi

config_dir="$root/platforms/foundation-platform/infra/observability/alertmanager"
config="$config_dir/alertmanager.yml"
cases="$config_dir/alertmanager.routes.test"
[ -f "$config" ] && [ -f "$cases" ] || {
  echo "FAIL ${name}: $config or $cases is missing" >&2
  exit 1
}

runtime_root="$(mktemp -d)"
cleanup() {
  if [ -n "${runtime_root:-}" ] && [ -d "$runtime_root" ]; then
    rm -rf -- "$runtime_root"
  fi
}
trap cleanup EXIT
archive="$runtime_root/alertmanager.tar.gz"
python3 "$fetcher" tool amtool --output "$archive" >&2
member="alertmanager-${pinned}.${os}-${arch}/${executable_name}"
tar -xzf "$archive" -C "$runtime_root" "$member"
amtool="$runtime_root/$member"

rc=0
# check-config reads the receivers' credential files only when it sends; the token file is not needed.
"$amtool" check-config "$config" >/dev/null || {
  echo "FAIL ${name}: amtool check-config refused alertmanager.yml" >&2
  rc=1
}
count=0
while read -r receiver labels; do
  case "$receiver" in ''|'#'*) continue ;; esac
  count=$((count + 1))
  # shellcheck disable=SC2086 # the labels are separate words: name=value name=value
  if ! got="$("$amtool" config routes test --config.file="$config" --verify.receivers="$receiver" $labels 2>&1)"; then
    echo "FAIL ${name}: {${labels// /, }} reaches ${got%%$'\n'*}, not ${receiver}" >&2
    rc=1
  fi
done <"$cases"
if [ "$count" -eq 0 ]; then
  echo "FAIL ${name}: no route cases in $cases" >&2
  rc=1
fi

if [ "$rc" -ne 0 ]; then
  exit 1
fi
echo "OK ${name}: cases=${count} amtool=${pinned}"
