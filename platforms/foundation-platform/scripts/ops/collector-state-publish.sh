#!/usr/bin/env bash
# Sourced by the host collectors whose job has a lane on AWS (config/aws-platform.contract.json collect.lanes):
# after every run the host copy writes its state directory to R2 as one new object, so the AWS lane can start from
# it (root ADR-0616 §7 and §8 C (다), coordinator 2026-11-27). The state lives in the service account's directory
# under /var/lib, which nobody else may read, and a collector never starts from empty state: from empty it would
# ask its whole history again and, for a quota source, spend the day's budget twice.
#
#   collector_state_publish <job id> <state dir> <scratch dir>
#       writes <collect.state_prefix><job id>/host/<UTC time>.tar.gz when the state differs from the one it wrote
#       last (the digest of its files, kept beside the scratch dir); never overwrites (a new key each time).
#
# It never fails the run: it is called from an EXIT trap, after the collection, and a state that could not be
# written is written by the next run. The writer key is the unit's own lakehouse writer key, which the collector
# already holds; the copy runs in the pinned aws-cli image, as the parcel-number-change read-back does.

# The publisher of such a job ends its run log with `foundation-run-usage seconds=.. peak_rss_kib=..
# r2_bytes_written=..`, as its AWS lane does, so the two copies are measured the same way (main.rs RUN_USAGE_ENV).
export FOUNDATION_PLATFORM_RUN_USAGE=1

collector_state_publish() {
  local job="$1" state="$2" scratch="$3" stamp digest last marker tarball prefix uid gid
  [ -d "${state}" ] && [ -n "${job}" ] && [ -d "${scratch}" ] || return 0
  [ -n "${FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID:-}" ] || return 0
  # Only a job the contract declares as a lane has a state prefix; for anything else nothing is written.
  prefix="$(python3 -I -c 'import json, sys
collect = json.load(open(sys.argv[1], encoding="utf-8"))["collect"]
if sys.argv[2] not in collect["lanes"]:
    sys.exit(1)
print(collect["state_prefix"])' "${RELEASE_ROOT}/config/aws-platform.contract.json" "${job}" 2>/dev/null)" || return 0
  # The digest of every file's name and bytes, in name order: the same state is not written twice.
  digest="$(python3 -I -c 'import hashlib, pathlib, sys
root = pathlib.Path(sys.argv[1])
digest = hashlib.sha256()
for path in sorted(p for p in root.rglob("*") if p.is_file()):
    digest.update(str(path.relative_to(root)).encode() + b"\0" + path.read_bytes())
print(digest.hexdigest())' "${state}")" || return 0
  marker="$(dirname "${scratch}")/collector-state-published-${job}.sha256"
  last=""
  if [ -f "${marker}" ]; then
    last="$(cat "${marker}")" || last=""
  fi
  [ "${last}" != "${digest}" ] || return 0
  stamp="$(date -u +%Y%m%dT%H%M%SZ)" || return 0
  tarball="collector-state-${stamp}.tar.gz"
  tar -C "${state}" -czf "${scratch}/${tarball}" . 2>/dev/null || { rm -f "${scratch}/${tarball}"; return 0; }
  uid="$(id -u)" || return 0
  gid="$(id -g)" || return 0
  # The pinned reference tools/technology-versions.contract.json lists.
  local COLLECTOR_STATE_AWS_IMAGE="amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"
  # --memory 512m: the same image's measured peak (133 MiB anonymous over `s3 cp` of 1 GiB, ai-server 2026-10-18,
  # parcel-number-change-collect.sh). These comments stay above the assignments: a comment line after them ends
  # the command there, and docker then starts without the keys (2026-11-13).
  if AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID}" \
    AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY}" \
    docker run --rm --memory 512m --user "${uid}:${gid}" -e HOME=/tmp -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
      -e AWS_DEFAULT_REGION=auto -v "${scratch}:/w:ro" "${COLLECTOR_STATE_AWS_IMAGE}" s3 cp --only-show-errors \
      --endpoint-url "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT}" \
      "/w/${tarball}" "s3://${FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET}/${prefix}${job}/host/${stamp}.tar.gz" >&2; then
    printf '%s\n' "${digest}" > "${marker}"
    echo "collector state of ${job} written to ${prefix}${job}/host/${stamp}.tar.gz"
  else
    echo "collector state of ${job} not written (the next run writes it)" >&2
  fi
  rm -f "${scratch}/${tarball}"
  return 0
}
