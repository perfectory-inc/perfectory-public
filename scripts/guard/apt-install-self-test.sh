#!/usr/bin/env bash
# Proves scripts/ci/apt-install.sh survives the outage it was written for.
#
# The incident: from about 13:50Z on 2026-11-11 the hosted runners could not resolve any ubuntu.com
# mirror, and every apt-get in CI ran 20 s per name per file until the job failed (#1267, #1313,
# #1323, #1329). On a GitHub-hosted runner this test blocks those mirrors in /etc/hosts and requires
# a real install to pass through a fallback mirror, then blocks every mirror and requires the same
# install from the package cache with no network. Anywhere, it requires a package the machine already
# has, by dpkg or by its command, never to reach apt.
set -euo pipefail
cd "$(dirname "$0")/../.."

script="scripts/ci/apt-install.sh"
test_root="$(mktemp -d)"
hosts_backup=""
cleanup() {
  if [ -n "$hosts_backup" ] && [ -f "$hosts_backup" ]; then
    sudo cp "$hosts_backup" /etc/hosts
  fi
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) [ ! -e "$test_root" ] || sudo -n rm -rf -- "$test_root" 2>/dev/null || rm -rf -- "$test_root" ;;
    *) echo "apt-install-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

fail() {
  echo "FAIL apt-install-self-test: $*" >&2
  exit 1
}

# --- anywhere: what is already there is never fetched ---------------------------------------------
fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/dpkg-query" <<'SH'
#!/usr/bin/env bash
# Reports only libpresent-dev as installed.
for argument in "$@"; do package="$argument"; done
[ "$package" = libpresent-dev ] && printf installed
exit 0
SH
for command in cmake psql; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"$fake_bin/$command"
done
chmod +x "$fake_bin"/*
if ! output="$(PATH="$fake_bin:$PATH" GITHUB_ACTIONS='' bash "$script" cmake libpresent-dev postgresql-client 2>&1)"; then
  fail "a package present by dpkg or by its command still went to apt: $output"
fi
grep -q 'already installed' <<<"$output" || fail "expected 'already installed', got: $output"
echo "ok apt-install-self-test: installed packages are not fetched (dpkg, a command, psql for postgresql-client)"

# --- a GitHub-hosted runner: the outage itself --------------------------------------------------
if [ "${GITHUB_ACTIONS:-}" != true ] || [ -z "${ImageVersion:-}" ] || ! sudo -n true 2>/dev/null \
  || ! grep -q '^ID=ubuntu' /etc/os-release 2>/dev/null; then
  echo "ok apt-install-self-test: the mirror outage cases run on a GitHub-hosted Ubuntu runner only"
  exit 0
fi

package=libsasl2-dev
if [ "$(dpkg-query -W -f='${db:Status-Status}' "$package" 2>/dev/null)" = installed ]; then
  sudo dpkg --purge "$package" >/dev/null
fi
cache="$test_root/cache"
hosts_backup="$test_root/hosts"
cp /etc/hosts "$hosts_backup"

block() {
  local host
  sudo cp "$hosts_backup" /etc/hosts
  for host in "$@"; do
    # TEST-NET-1 (RFC 5737) is never routed, so a connection stalls like the outage's resolver did; the probe must bound it.
    printf '192.0.2.1 %s\n' "$host" | sudo tee -a /etc/hosts >/dev/null
  done
}

official=(azure.archive.ubuntu.com archive.ubuntu.com security.ubuntu.com)
mapfile -t fallbacks < <(sed -n 's#^  "http://\([^/"]*\)/.*#\1#p' "$script")
[ "${#fallbacks[@]}" -ge 3 ] || fail "could not read the fallback mirrors from $script"

block "${official[@]}"
started=$SECONDS
if ! output="$(PERFECTORY_APT_CACHE_DIR="$cache" GITHUB_ENV="$test_root/env" bash "$script" "$package" 2>&1)"; then
  printf '%s\n' "$output" >&2
  fail "with every ubuntu.com mirror blocked, the install did not reach a fallback"
fi
took=$((SECONDS - started))
[ "$(dpkg-query -W -f='${db:Status-Status}' "$package")" = installed ] || fail "$package is not installed after the fallback install"
grep -q 'mirror did not answer.*azure.archive.ubuntu.com' <<<"$output" || fail "the blocked azure mirror was not probed out: $output"
grep -q '^PERFECTORY_APT_CACHE_SAVE=[0-9a-f]\{64\}$' "$test_root/env" || fail "the download did not ask the workflow to save the cache"
ls "$cache"/*/complete >/dev/null 2>&1 || fail "the downloaded set was not kept in the cache"
echo "ok apt-install-self-test: every ubuntu.com mirror blocked, installed through a fallback in ${took}s"

sudo dpkg --purge "$package" >/dev/null
block "${official[@]}" "${fallbacks[@]}"
started=$SECONDS
if ! output="$(PERFECTORY_APT_CACHE_DIR="$cache" GITHUB_ENV="$test_root/env" bash "$script" "$package" 2>&1)"; then
  printf '%s\n' "$output" >&2
  fail "with every mirror blocked, the cached set did not install"
fi
took=$((SECONDS - started))
grep -q '(no network)' <<<"$output" || fail "the second install did not come from the cache: $output"
[ "$(dpkg-query -W -f='${db:Status-Status}' "$package")" = installed ] || fail "$package is not installed from the cache"
echo "ok apt-install-self-test: every mirror blocked, installed from the package cache in ${took}s"
