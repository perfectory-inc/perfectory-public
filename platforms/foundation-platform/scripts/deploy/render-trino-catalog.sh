#!/usr/bin/env bash
# Source from the remote job so Compose consumes the same external state path.
# No generated configuration belongs in the immutable release (root ADR-0126).
export FOUNDATION_PLATFORM_TRINO_CATALOG_DIR="${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR:-${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:-/var/lib/foundation-platform/lakehouse}/trino/catalog}" || return
trino_source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)" || return
trino_target_root="$(realpath -m "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}")" || return
case "${trino_target_root}/" in
  "${trino_source_root}/"*)
    printf 'Trino generated configuration must be outside the release\n' >&2
    return 65 ;;
esac
export FOUNDATION_PLATFORM_TRINO_CATALOG_DIR="${trino_target_root}" || return
mkdir -p -m 0700 "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}" || return
chmod 0700 "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}" || return
trino_catalog_pending="$(mktemp "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}/.r2.XXXXXX")" || return
if python3 -I -B - "${trino_source_root}/infra/lakehouse/trino/templates/r2-iceberg.properties.template" \
  "${trino_catalog_pending}" <<'PY'
import os, re, sys
from pathlib import Path
values = dict(os.environ)
values["FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI"] = (
    values.get("FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI")
    or values.get("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI", "").rstrip("/") + "/v1/oauth/tokens"
)
def replace(match):
    name = match.group(1)
    value = values.get(name, "")
    if not value or "\n" in value or "\r" in value:
        raise SystemExit("Trino requires a nonempty single-line value for " + name)
    return value
text = re.sub(r"<([A-Z0-9_]+)>", replace, Path(sys.argv[1]).read_text())
Path(sys.argv[2]).write_text(text)
PY
then
  :
else
  trino_render_status=$?
  rm -f -- "${trino_catalog_pending}"
  return "${trino_render_status}"
fi
# The host directory is private. Mount only this file: Trino's container uid differs from the
# writer uid and must read it without opening that private host directory to other users.
chmod 0644 "${trino_catalog_pending}" || { rm -f -- "${trino_catalog_pending}"; return 65; }
mv -f "${trino_catalog_pending}" "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}/r2.properties" \
  || { rm -f -- "${trino_catalog_pending}"; return 65; }
unset trino_catalog_pending trino_source_root trino_target_root
