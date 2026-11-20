#!/usr/bin/env bash
# The audit digest's witness (root ADR-0569 §4, ADR-0584 §2): root's step of
# foundation-audit-digest.service, run from the control checkout like the shape ledger's writer key.
# It signs each digest the service user kept and has not seen witnessed (pending/<no>.txt, oldest
# first) with the witness key, using the pinned cosign, which records the signature in the public
# Rekor transparency log and answers the bundle (pending/<no>.bundle.json). Only the digest's hash
# and signature leave the host. A signing that fails stops here; the digest stays pending for the
# next run, and the ones signed before it are still recorded.
#
# The key is made here once and never leaves root: /var/lib/perfectory/audit-witness/cosign.key
# (0600). Its public half, cosign.pub (0644), is what anyone checks the Rekor entries against.
# On the weekly check it verifies every kept digest's bundle against Rekor with that public key.
set -euo pipefail
log() { printf 'audit-witness: %s\n' "$*" >&2; }

dir="${RUNTIME_DIRECTORY:?the witness runs inside foundation-audit-digest.service}"
dir="${dir%%:*}"
keys="${FOUNDATION_AUDIT_WITNESS_KEYS:-/var/lib/perfectory/audit-witness}"
control="${FOUNDATION_AUDIT_WITNESS_CONTROL:-/opt/perfectory-control/current}"
cache="${FOUNDATION_AUDIT_WITNESS_PINNED:-/var/lib/perfectory/pinned}"
owner="$(stat -c %U:%G "${dir}")"

# cosign keeps Sigstore's trust root under $HOME/.sigstore/root (sigstore pkg/tuf, os.UserHomeDir).
# A `+` step runs as root but keeps the unit's User= environment, so HOME is the job account's
# /nonexistent: 2026-11-20 03:05Z the first run made /nonexistent (root, 0700), and every job
# account's Docker CLI, which reads $HOME/.docker, lost its compose plugin ("unknown flag:
# --project-directory") until it was gone. The witness's home is beside its key.
install -d -m 0755 "${keys}"
export HOME="${keys}/home"
install -d -m 0700 "${HOME}"
# What that first run left: only its trust-root cache, which rmdir proves.
if [[ -d /nonexistent/.sigstore && -O /nonexistent ]]; then
  rm -rf /nonexistent/.sigstore
  if rmdir /nonexistent 2>/dev/null; then
    log "removed the /nonexistent a run before this fix made"
  else
    log "left /nonexistent: it holds more than that run's .sigstore"
  fi
fi

if [[ ! -d "${dir}/pending" && ! -d "${dir}/verify" ]]; then
  log "no digest to witness and no weekly check"
  exit 0
fi

umask 077
install -d -m 0700 "${cache}"
cosign="${cache}/cosign"
# The pinned version, checked against its pinned SHA-256 (root ADR-0197, tools/pinned-artifacts.json).
env -i PATH=/usr/bin:/bin LANG=C.UTF-8 HOME=/root PERFECTORY_PINNED_CACHE="${cache}" \
  /usr/bin/python3 -I -B "${control}/scripts/ci/fetch_pinned.py" tool cosign --output "${cosign}" >/dev/null
chmod 0700 "${cosign}"
export COSIGN_PASSWORD=""

if [[ ! -f "${keys}/cosign.key" ]]; then
  install -d -m 0755 "${keys}"
  (cd "${keys}" && "${cosign}" generate-key-pair --output-key-prefix cosign >/dev/null)
  chmod 0600 "${keys}/cosign.key"
  chmod 0644 "${keys}/cosign.pub"
  log "made the witness key; its public half is ${keys}/cosign.pub"
fi

if [[ -d "${dir}/verify" ]]; then
  checked=0
  for text in "${dir}"/verify/*.txt; do
    [[ -e "${text}" ]] || continue
    bundle="${text%.txt}.bundle.json"
    "${cosign}" verify-blob --key "${keys}/cosign.pub" --bundle "${bundle}" "${text}" >/dev/null 2>&1 \
      || { name="${text##*/}"; log "digest ${name%.txt} does not verify against Rekor"; exit 1; }
    checked=$((checked + 1))
  done
  log "verified ${checked} digests against Rekor"
fi

if [[ -d "${dir}/pending" ]]; then
  # The pending digests oldest first. Each step is its own command, so a failed listing stops the
  # witness (set -e) instead of looking like "nothing pending".
  listing="$(find "${dir}/pending" -maxdepth 1 -name '*.txt' -printf '%f\n')"
  numbers="$(sed -n 's/^\([1-9][0-9]*\)\.txt$/\1/p' <<<"${listing}")"
  sorted="$(sort -n <<<"${numbers}")"
  while IFS= read -r no; do
    [[ -n "${no}" ]] || continue
    text="${dir}/pending/${no}.txt"
    bundle="${dir}/pending/${no}.bundle.json"
    if ! "${cosign}" sign-blob --yes --key "${keys}/cosign.key" --bundle "${bundle}" "${text}" >/dev/null; then
      rm -f "${bundle}"
      log "digest ${no} was not witnessed; it stays pending for the next run"
      break
    fi
    chown "${owner}" "${bundle}"
    chmod 0640 "${bundle}"
    log "witnessed digest ${no}"
  done <<<"${sorted}"
fi
