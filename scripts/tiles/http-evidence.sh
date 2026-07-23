#!/usr/bin/env bash
# Shared safety primitives for retaining HTTP proof evidence without retaining
# credentials or signed redirect targets. The caller owns its EXIT trap.

tiles_redact_response_headers() {
  local source="${1:?raw response-header path is required}"
  local destination="${2:?sanitized response-header path is required}"

  # Preserve only fields used by the proof. An allowlist prevents a future
  # URL-bearing or credential-bearing response header from silently becoming
  # retained evidence.
  sed -n -E \
    -e '/^HTTP\/[0-9.]+[[:space:]]+[0-9]{3}/p' \
    -e '/^(etag|content-length|content-range|x-amz-meta-sha256):/Ip' \
    "$source" > "$destination"
  rm -f -- "$source"
}

tiles_remove_http_artifacts() {
  [[ "$#" -eq 0 ]] || rm -f -- "$@"
}
