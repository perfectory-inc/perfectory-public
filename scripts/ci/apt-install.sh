#!/usr/bin/env bash
# The one way CI installs Debian packages: `bash scripts/ci/apt-install.sh PACKAGE...`.
#
# xtask's area prerequisites (`apt_deps`) and every workflow step that needs a package come through
# here; scripts/guard/apt-goes-through-one-script.sh refuses an `apt-get` anywhere else in
# .github/workflows. The incident it prevents: from about 13:50Z on 2026-11-11 the hosted runners
# could not resolve azure.archive.ubuntu.com, archive.ubuntu.com or security.ubuntu.com. apt waited
# 20 s on every name for every file, ran past 20 minutes and failed Rust fmt/clippy/test, the
# Postgres integration and the Kafka integration on #1267, #1313, #1323, #1329 and more with no
# code error. Four things make that impossible:
#
#  1. A package the runner already has is never fetched. dpkg's answer counts, and so does a command
#     on PATH for a package that ships one (the runner's cmake is in /usr/local/bin, outside dpkg, so
#     every run used to download cmake 3.28 only for PATH to keep picking 3.31).
#  2. On a GitHub-hosted runner, the .deb files of a run that had to download are kept under
#     ~/.cache/perfectory/apt, keyed by the runner image and the package set. A workflow restores
#     and saves that directory with actions/cache (key prefix apt-packages-v1-, budgeted in
#     tools/ci/actions-cache-budget.contract.json); a hit installs with no network at all.
#     A set kept for another image still saves the download: the files apt asks for by the same
#     name (package, version, architecture) are copied from it, and apt fetches only the rest.
#     GitHub runs several image versions at once and main saves one; 2026-11-17..19, 18 of 30
#     Postgres integration runs were on an image the cache did not name and downloaded 29.8 MB
#     again, 12 of them for 450-640 s.
#  3. A download goes only to mirrors that answered a short probe a moment ago: the runner's own
#     list first, then mirrors outside ubuntu.com. A dead mirror costs one probe, not 20 s a file.
#     The archive's signature is what apt trusts, so any mirror of it is as good as the official.
#  4. Retries everywhere: apt retries each file, and the whole probe/update/download repeats three
#     times with a wait, because `apt-get update` is judged by --error-on=any, not by its exit code.
#  5. A download that stalls is stopped and resumed elsewhere. A mirror that answers the probe can
#     still serve at 50-75 kB/s ("Fetched 29.8 MB in 9min 42s", 2026-11-19), and a stalled download
#     stays stalled. The first two attempts give the download a time limit; the next attempt puts
#     the mirrors outside the runner's list first and continues the partial files. The last attempt
#     has no limit, so a slow archive is never worse than it was.
#
# Off a GitHub runner (the Docker verification image, a developer's machine) the cache is off and
# the system's own sources are used, still with the retries.
set -euo pipefail

say() { printf 'apt-install: %s\n' "$*" >&2; }

[ "$#" -gt 0 ] || { say "usage: $0 PACKAGE..."; exit 2; }

# A package that ships a command under another name. The runner's PostgreSQL 16 client is installed
# as postgresql-client-16, not the postgresql-client metapackage the workflows name.
command_for() {
  case "$1" in
    postgresql-client) echo psql ;;
    lib*|*-dev) ;;
    *) echo "$1" ;;
  esac
}

installed() {
  local package="$1" command
  [ "$(dpkg-query -W -f='${db:Status-Status}' "$package" 2>/dev/null)" = installed ] && return 0
  command="$(command_for "$package")"
  [ -n "$command" ] && command -v "$command" >/dev/null 2>&1
}

missing=()
for package in "$@"; do
  installed "$package" || missing+=("$package")
done
if [ "${#missing[@]}" -eq 0 ]; then
  say "already installed: $*"
  exit 0
fi
command -v apt-get >/dev/null 2>&1 || { say "apt-get not found; install these first: ${missing[*]}"; exit 1; }
mapfile -t missing < <(printf '%s\n' "${missing[@]}" | LC_ALL=C sort -u)
say "missing: ${missing[*]}"

sudo=()
[ "$(id -u)" = 0 ] || sudo=(sudo)

# --- 2. The package cache --------------------------------------------------------------------------
cache_root=""
if [ "${GITHUB_ACTIONS:-}" = true ] && [ -n "${ImageVersion:-}" ]; then
  cache_root="${PERFECTORY_APT_CACHE_DIR:-$HOME/.cache/perfectory/apt}"
fi
image="${ImageOS:-unknown}-${ImageVersion:-unknown}-$(dpkg --print-architecture)"
set_hash="$(printf '%s\n' "$image" "${missing[@]}" | sha256sum | cut -c1-16)"
set_dir=""
[ -z "$cache_root" ] || set_dir="$cache_root/$image-$set_hash"

install_from_cache() {
  [ -n "$set_dir" ] && [ -f "$set_dir/complete" ] || return 1
  local debs=("$set_dir"/*.deb)
  [ -e "${debs[0]}" ] || return 1
  say "installing ${#debs[@]} cached package files from $set_dir (no network)"
  # dpkg, not apt: the set is exactly what apt downloaded for this image, and dpkg cannot reach a
  # mirror (apt's --no-download also refuses to copy local files in, measured on #1336).
  "${sudo[@]}" env DEBIAN_FRONTEND=noninteractive dpkg -i "${debs[@]}"
}

if install_from_cache; then
  exit 0
fi
[ -z "$set_dir" ] || [ ! -e "$set_dir" ] || { say "cached set is incomplete or stale; downloading"; "${sudo[@]}" rm -rf "$set_dir"; }

# --- 3. Mirrors that answer ------------------------------------------------------------------------
# The fallbacks are outside ubuntu.com, because the 2026-11-11 outage took every ubuntu.com name at
# once. Each is a full archive mirror (all four pockets) on the Ubuntu mirror list.
FALLBACK_MIRRORS=(
  "http://archive.ubuntu.com/ubuntu/"
  "http://security.ubuntu.com/ubuntu/"
  "http://mirrors.edge.kernel.org/ubuntu/"
  "http://mirror.math.princeton.edu/pub/ubuntu/"
  "http://mirrors.ocf.berkeley.edu/ubuntu/"
)
PROBE_SECONDS="${PERFECTORY_APT_PROBE_SECONDS:-8}"
# Foundation's 29.8 MB took 3-28 s from a healthy mirror (2026-11-17..19, 18 downloads).
DOWNLOAD_SECONDS="${PERFECTORY_APT_DOWNLOAD_SECONDS:-180}"
attempt=1

work="$(mktemp -d)"
trap '"${sudo[@]}" rm -rf "$work"' EXIT
apt_options=(
  -o Acquire::Retries=5
  -o Acquire::http::Timeout=30
  -o Acquire::https::Timeout=30
  -o APT::Sandbox::User=root
  -o Dpkg::Use-Pty=0
)
codename=""
ubuntu_sources=false
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  codename="${VERSION_CODENAME:-}"
  if [ "${ID:-}" = ubuntu ] && [ -n "$codename" ] && [ -r /usr/share/keyrings/ubuntu-archive-keyring.gpg ]; then
    ubuntu_sources=true
  fi
fi

configure_mirrors() {
  local candidates=() mirror reachable=()
  if [ -r /etc/apt/apt-mirrors.txt ]; then
    while read -r mirror _; do
      [ -n "$mirror" ] && [ "${mirror#\#}" = "$mirror" ] && candidates+=("${mirror%/}/")
    done </etc/apt/apt-mirrors.txt
  fi
  if [ "$attempt" -gt 1 ]; then
    # The runner's own mirror failed or stalled a moment ago: it goes last.
    candidates=("${FALLBACK_MIRRORS[@]}" "${candidates[@]}")
  else
    candidates+=("${FALLBACK_MIRRORS[@]}")
  fi
  # One probe per host: the runner lists https://archive.ubuntu.com, the fallbacks http://.
  mapfile -t candidates < <(printf '%s\n' "${candidates[@]}" | awk -F'://' '!seen[$2]++')
  for mirror in "${candidates[@]}"; do
    if curl -fsSI -o /dev/null --max-time "$PROBE_SECONDS" "${mirror}dists/${codename}/Release"; then
      reachable+=("$mirror")
    else
      say "mirror did not answer in ${PROBE_SECONDS}s: $mirror"
    fi
  done
  if [ "${#reachable[@]}" -eq 0 ]; then
    say "no mirror answered; letting apt try them all"
    reachable=("${candidates[@]}")
  fi
  say "mirrors, in order: ${reachable[*]}"
  local priority=1
  : >"$work/mirrors.txt"
  for mirror in "${reachable[@]}"; do
    printf '%s\tpriority:%d\n' "$mirror" "$priority" >>"$work/mirrors.txt"
    priority=$((priority + 1))
  done
  mkdir -p "$work/sources.d" "$work/lists/partial"
  cat >"$work/sources.d/ubuntu.sources" <<EOF
Types: deb
URIs: mirror+file:$work/mirrors.txt
Suites: $codename $codename-updates $codename-backports $codename-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
EOF
}

if [ "$ubuntu_sources" = true ]; then
  # Only the Ubuntu archive, in a private list directory: the runner's other sources (Microsoft,
  # Docker, ...) are not needed for these packages and cannot fail or slow the update.
  apt_options+=(
    -o Dir::Etc::SourceList=/dev/null
    -o "Dir::Etc::SourceParts=$work/sources.d"
    -o "Dir::State::Lists=$work/lists"
  )
fi

download_dir=""
if [ -n "$set_dir" ]; then
  download_dir="$set_dir"
  mkdir -p "$download_dir/partial"
fi

# Copies, from the sets kept for other images, the files apt is about to fetch. A file is taken by
# the name apt gives it (package_version_architecture.deb), so a superseded version is never
# copied, and only when its SHA-256 is the one in the signed index: apt 2.8.4 accepts a file already
# in its archive directory by name and size alone (measured: zeros of the right length passed), and
# --print-uris names an MD5 sum, so the SHA-256 is read from the package's index entry.
indexed_sha256() {
  local name="$1" package version
  package="${name%%_*}"
  version="${name#*_}"
  version="${version%_*}"
  apt-cache "${apt_options[@]}" show "$package=${version//%3a/:}" 2>/dev/null | awk '$1 == "SHA256:" { print $2; exit }'
}

reuse_kept_files() {
  local wanted name other digest actual copied=0 total=0
  wanted="$("${sudo[@]}" env DEBIAN_FRONTEND=noninteractive apt-get "${apt_options[@]}" \
    -o "Dir::Cache::Archives=$download_dir" install -y --no-install-recommends --print-uris -qq "${missing[@]}")" || return 1
  while read -r _ name _; do
    [ -n "$name" ] || continue
    total=$((total + 1))
    digest=""
    for other in "$cache_root"/*/; do
      [ "${other%/}" != "$download_dir" ] && [ -f "$other$name" ] || continue
      [ -n "$digest" ] || digest="$(indexed_sha256 "$name")"
      actual="$(sha256sum "$other$name")" || continue
      [ -n "$digest" ] && [ "${actual%% *}" = "$digest" ] || continue
      cp "$other$name" "$download_dir/$name" && copied=$((copied + 1))
      break
    done
  done <<<"$wanted"
  say "package files to fetch: $total; kept from another runner image: $copied"
}

fetch_and_install() {
  [ "$ubuntu_sources" != true ] || configure_mirrors
  "${sudo[@]}" apt-get "${apt_options[@]}" update --error-on=any || return 1
  if [ -z "$download_dir" ]; then
    "${sudo[@]}" env DEBIAN_FRONTEND=noninteractive apt-get "${apt_options[@]}" \
      install -y --no-install-recommends "${missing[@]}"
    return
  fi
  reuse_kept_files || return 1
  local limit=()
  [ "$attempt" -ge 3 ] || limit=(timeout --signal=TERM --kill-after=10 "$DOWNLOAD_SECONDS")
  "${sudo[@]}" "${limit[@]}" env DEBIAN_FRONTEND=noninteractive apt-get "${apt_options[@]}" \
    -o "Dir::Cache::Archives=$download_dir" install -y --no-install-recommends --download-only "${missing[@]}" || {
    say "the download failed or passed its limit (${DOWNLOAD_SECONDS}s on the first two attempts); partial files are kept"
    return 1
  }
  "${sudo[@]}" chown -R "$(id -u):$(id -g)" "$download_dir"
  rm -rf "$download_dir/partial" "$download_dir/lock"
  touch "$download_dir/complete"
  install_from_cache
}

waits=(15 45)
for attempt in 1 2 3; do
  if fetch_and_install; then
    break
  fi
  [ "$attempt" -lt 3 ] || { say "could not install ${missing[*]} after $attempt attempts"; exit 1; }
  say "attempt $attempt failed; retrying in ${waits[$((attempt - 1))]}s"
  sleep "${waits[$((attempt - 1))]}"
done

if [ -n "$cache_root" ]; then
  # Keep only this image's sets, so the saved cache never carries a retired image's files, and ask
  # the workflow's save step for a key that names exactly what is in it.
  find "$cache_root" -mindepth 1 -maxdepth 1 -type d ! -name "$image-*" -exec rm -rf {} +
  listing="$(cd "$cache_root" && find . -mindepth 1 -maxdepth 1 -type d | LC_ALL=C sort | sha256sum | cut -d' ' -f1)"
  [ -z "${GITHUB_ENV:-}" ] || echo "PERFECTORY_APT_CACHE_SAVE=$listing" >>"$GITHUB_ENV"
  say "saved $set_dir for the cache (key suffix $listing)"
fi
