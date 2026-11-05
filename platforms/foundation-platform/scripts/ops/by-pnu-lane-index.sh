#!/usr/bin/env bash
# The by-PNU lanes, read from the lane index the declared definitions generate (root ADR-0418;
# config/by-pnu-lane-index.v1.json, scripts/catalog/declared_definitions.py `lane_index`). Sourced by
# the lane scripts, so none of them re-derives a lane list or a lane's environment prefix from the
# connection contract (inventory B-F02: the same derivation was written in four languages).
#
#   by_pnu_lane_names [all|packs]   every lane, or the pack-only lanes (not parcel or building)
#   by_pnu_lane_known <lane>        exit 0 when the index names it
#   by_pnu_lane_env_prefix <lane>   FOUNDATION_PLATFORM_<LANE>_BY_PNU_SERVING, as the publisher reads it
#   by_pnu_lane_bake_services       every systemd service that bakes a lane, once each

by_pnu_lane_index="$(dirname "${BASH_SOURCE[0]}")/../../config/by-pnu-lane-index.v1.json"

by_pnu_lane_query() {
  python3 -I - "${by_pnu_lane_index}" "$@" <<'PY'
import json, sys
index, what, *rest = sys.argv[1:]
lanes = json.load(open(index, encoding="utf-8"))["lanes"]
if what == "names":
    print("\n".join(l["lane"] for l in lanes if rest[0] == "all" or l["lane"] not in ("parcel", "building")))
elif what == "known":
    sys.exit(0 if any(l["lane"] == rest[0] for l in lanes) else 1)
elif what == "bake_services":
    print("\n".join(dict.fromkeys(job["systemd_service"] for l in lanes for job in l["bake_jobs"])))
elif what == "env_prefix":
    found = [l["env_prefix"] for l in lanes if l["lane"] == rest[0]]
    if not found:
        sys.exit(f"by-pnu-lane-index: no lane {rest[0]!r}")
    print(found[0])
PY
}

by_pnu_lane_names() { by_pnu_lane_query names "${1:-all}"; }
by_pnu_lane_known() { [[ -n "${1:-}" ]] && by_pnu_lane_query known "$1"; }
by_pnu_lane_env_prefix() { by_pnu_lane_query env_prefix "$1"; }
by_pnu_lane_bake_services() { by_pnu_lane_query bake_services; }
