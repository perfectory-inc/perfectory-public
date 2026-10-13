#!/usr/bin/env bash
# Runs the runtime-gated Spark test files of one directory side by side, each in its own interpreter.
#
# Usage: foundation-spark-runtime-files.sh <tests directory>
#   SPARK_RUNTIME_JOBS  how many files run at once (default: one per 2 CPUs and per 2 GiB free).
#
# foundation-spark-runtime.sh calls this inside the pinned Spark container. A file joins by naming
# the runtime in its skip reason ("requires pyspark", "requires the pinned ..."). Each file keeps its
# own interpreter (the pure-logic tests beside them install a stand-in `pyspark` that would shadow
# the real one in a shared process) and so its own JVM; what runs side by side is whole files.
# Files share nothing on disk: each gets its own TMPDIR and spark.local.dir. A job may own its
# scratch directory outright (gold_incremental deletes sibling stage directories, as production
# serializes its runs), so a shared /tmp would let one file delete another's stages mid-run.
# Files start largest first, so a long file does not begin last and become the tail.
# Their output is printed one file at a time in name order, then the files by duration, slowest
# first, so a slow file is named in the log rather than guessed at. Any failing file fails the run,
# whatever the others did.
set -euo pipefail

tests="${1:?usage: foundation-spark-runtime-files.sh <tests directory>}"
# One file per two CPUs (each test session is local[2]; more at once only made every file slower
# on a 4-CPU runner, see foundation-spark-runtime.sh), and no more than one per 2 GiB of available
# memory: a test JVM peaks near that, and a small Docker VM runs out before its CPUs do.
default_jobs() {
  local cpus memory
  cpus="$(($(nproc) / 2))"
  memory="$(awk '/^MemAvailable:/ { print int($2 / 2097152) }' /proc/meminfo 2>/dev/null || true)"
  if [[ -n "$memory" ]] && ((memory < cpus)); then cpus="$memory"; fi
  echo "$((cpus > 0 ? cpus : 1))"
}
jobs="${SPARK_RUNTIME_JOBS:-$(default_jobs)}"
cd "$tests"

mapfile -t files < <(grep -l -E "skip(Unless|If)\(.*\"requires (pyspark|the pinned)" test_*.py | sort)
((${#files[@]})) || { echo "no runtime-gated test file found" >&2; exit 1; }

logs="$(mktemp -d)"
export SPARK_RUNTIME_LOGS="$logs"
trap 'rm -rf "$logs"' EXIT
echo "foundation-spark-runtime: ${#files[@]} files, ${jobs} at a time"

run_one() {
  local file="$1" logs="$SPARK_RUNTIME_LOGS" started status=0 scratch
  scratch="$logs/scratch/${file%.py}"
  mkdir -p "$scratch"
  started="$(date +%s)"
  TMPDIR="$scratch" PYSPARK_SUBMIT_ARGS="--conf spark.local.dir=$scratch ${PYSPARK_SUBMIT_ARGS:-pyspark-shell}" \
    python3 -m unittest -v "${file%.py}" >"$logs/$file.log" 2>&1 || status=$?
  rm -rf "$scratch"
  printf '%s %s\n' "$status" "$(($(date +%s) - started))" >"$logs/$file.result"
}
export -f run_one
started="$(date +%s)"
ls -S -- "${files[@]}" | tr '\n' '\0' | xargs -0 -n 1 -P "$jobs" bash -c 'run_one "$1"' _ || true
wall="$(($(date +%s) - started))"

failed=0
summary=""
for file in "${files[@]}"; do
  if [[ -f "$logs/$file.result" ]]; then
    read -r status seconds <"$logs/$file.result"
  else
    status="not-run" seconds=0
  fi
  echo "== ${file} (${seconds}s, exit ${status})"
  cat "$logs/$file.log" 2>/dev/null || true
  [[ "$status" == 0 ]] || failed=1
  summary+="${seconds} ${file} ${status}"$'\n'
done

echo "== durations, slowest first (wall ${wall}s, ${jobs} at a time)"
printf '%s' "$summary" | sort -rn | while read -r seconds file status; do
  printf '%6ss  %s%s\n' "$seconds" "$file" "$([[ "$status" == 0 ]] || echo "  FAILED (exit $status)")"
done
exit "$failed"
