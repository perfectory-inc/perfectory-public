#!/usr/bin/env bash
# The audit digest's witness (root ADR-0569 §4): root's step of foundation-audit-digest.service, run
# from the control checkout like the shape ledger's writer key. It signs the digest the service user
# prepared with the witness key, using the pinned cosign, which records the signature in the public
# Rekor transparency log and answers the bundle. Only the digest's hash and signature leave the host.
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

if [[ ! -f "${dir}/digest.txt" && ! -d "${dir}/verify" ]]; then
  log "no new row and no weekly check: nothing to witness"
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
      || { log "digest $(basename "${text}" .txt) does not verify against Rekor"; exit 1; }
    checked=$((checked + 1))
  done
  log "verified ${checked} digests against Rekor"
fi

if [[ -f "${dir}/digest.txt" ]]; then
  "${cosign}" sign-blob --yes --key "${keys}/cosign.key" --bundle "${dir}/bundle.json" "${dir}/digest.txt" >/dev/null
  chown "${owner}" "${dir}/bundle.json"
  chmod 0640 "${dir}/bundle.json"
  log "witnessed $(cat "${dir}/digest.json")"
fi
