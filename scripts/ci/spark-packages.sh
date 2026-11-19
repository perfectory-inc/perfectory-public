#!/usr/bin/env bash
# Fills one Ivy directory with the Spark packages a caller names, and leaves <ivy-dir>/jars holding
# exactly their jars. The release build (scripts/deploy/foundation-release-build.py) and the Spark
# runtime tests (scripts/verify/foundation-spark-runtime.sh) both resolve through this, so CI asks
# Maven Central once per package set and every other run resolves with no network.
#
#   spark-packages.sh <ivy-dir> <spark-image> <packages>
#
# 1. Resolve with `--network none`. Ivy returns a fixed revision from its cache without asking the
#    repository (BasicResolver.getDependency reads findModuleInCache first), so a directory a
#    previous run filled, or the Actions cache restored, resolves here in seconds.
# 2. Otherwise resolve with the network, at most three times with a pause between: the second layer.
# 3. Then resolve with `--network none` again: a directory that leaves this script resolves offline,
#    so a saved cache is never one that still needs the network.
#
# 2026-11-19: Maven Central answered the runners' requests with a 4xx for a while. Ivy 2.5.1 logs a
# 4xx only at verbose level (BasicURLHandler.checkStatusCode), so spark-submit printed "module not
# found" for all five packages after one second. That failed the release dry run in the merge queue
# (runs 44457507126, 44464012637), each failure dropped the entry and rebuilt the queue behind it,
# and nothing merged from 04:19Z to 06:55Z with more than twenty entries waiting. The same answer
# failed the Spark runtime shards on pull requests four times that day.
set -euo pipefail

if (($# != 3)); then
  echo "usage: spark-packages.sh <ivy-dir> <spark-image> <packages>" >&2
  exit 2
fi
ivy="$1" image="$2" packages="$3"
attempts=3

mkdir -p "$ivy"
ivy="$(cd "$ivy" && pwd -P)"
application="$(mktemp -d)"
trap 'rm -rf "$application"' EXIT
echo "print('spark packages resolved')" >"$application/resolve.py"
chmod 0755 "$application"
chmod 0644 "$application/resolve.py"

# Spark runs as uid 185 and writes owner-only; a restored cache belongs to the runner's account.
# Either way both must read and write the directory, so root in the same image opens it.
open() {
  docker run --rm --network none --user 0 --mount "type=bind,src=$ivy,dst=/resolve" \
    --entrypoint chmod "$image" -R a+rwX /resolve
}

# Ivy copies the resolved jars from <ivy-dir>/cache into <ivy-dir>/jars. The folder is emptied first:
# a jar of a package set this directory held before must not travel into a release.
resolve() {
  local network="$1" host=()
  # With no network the container has only loopback, and its generated hostname resolves nowhere.
  if [ "$network" = none ]; then
    host=(--hostname localhost -e SPARK_LOCAL_IP=127.0.0.1)
  fi
  open
  rm -rf "$ivy/jars"
  docker run --rm --network "$network" "${host[@]}" --mount "type=bind,src=$ivy,dst=/resolve" \
    --mount "type=bind,src=$application,dst=/application,readonly" \
    --entrypoint /opt/spark/bin/spark-submit "$image" \
    --conf spark.jars.ivy=/resolve --packages "$packages" /application/resolve.py
}

SECONDS=0
if resolve none >"$application/offline.log" 2>&1; then
  open
  echo "spark-packages: resolved from $ivy with no network in ${SECONDS}s"
  exit 0
fi
echo "spark-packages: $ivy does not hold every package; resolving from the network"

for ((attempt = 1; attempt <= attempts; attempt++)); do
  if resolve bridge; then
    break
  fi
  if ((attempt == attempts)); then
    echo "spark-packages: FAILED: the network resolution failed $attempts times" >&2
    exit 1
  fi
  pause=$((attempt * 30))
  echo "spark-packages: network resolution $attempt of $attempts failed; again in ${pause}s" >&2
  sleep "$pause"
done

if ! resolve none; then
  echo "spark-packages: FAILED: the directory the network filled does not resolve with no network" >&2
  exit 1
fi
open
echo "spark-packages: resolved from the network in ${SECONDS}s"
# The workflow saves the directory's cache only when this run had to download (and only on main).
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "PERFECTORY_SPARK_PACKAGES_SAVE=1" >>"$GITHUB_ENV"
fi
