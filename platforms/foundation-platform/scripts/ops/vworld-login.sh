#!/usr/bin/env bash
# Sourced by the scripts that log in to VWorld (daily-source-sweep.sh, raon-large-files.sh; root
# ADR-0168 §7, ADR-0170). The login has a canonical name and deprecated aliases for each role;
# config/environment-variable-naming.contract.json is the one list of them, so nothing here or in
# the callers spells them. Values are never printed — only names.
#
#   vworld_login_missing <naming contract>   canonical names of the roles no name gives a value
#   vworld_login_names <naming contract>     every name a role may be read from, one per line

vworld_login_missing() {
  python3 -I - "$1" <<'PY'
import json, os, sys
credentials = json.load(open(sys.argv[1], encoding="utf-8"))[
    "compatibility_migrations"]["foundation-vworld-credentials"]["credentials"]
for role in ("username", "password"):
    names = [credentials[role]["canonical"], *credentials[role]["deprecated_aliases"]]
    if not any(os.environ.get(name) for name in names):
        print(credentials[role]["canonical"])
PY
}

vworld_login_names() {
  python3 -I - "$1" <<'PY'
import json, sys
credentials = json.load(open(sys.argv[1], encoding="utf-8"))[
    "compatibility_migrations"]["foundation-vworld-credentials"]["credentials"]
for role in ("username", "password"):
    print("\n".join([credentials[role]["canonical"], *credentials[role]["deprecated_aliases"]]))
PY
}
