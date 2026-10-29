#!/usr/bin/env bash
# Proves onbid-field-coverage refuses what it claims to refuse (root ADR-0340): a field that is not
# shown and gives no reason, a broken claim chain, a Silver column, served path or web leaf that does
# not exist, a private name in Silver, a Silver column or served path nobody claims, a same_as that
# points nowhere; and that the real inventory passes.
set -euo pipefail
cd "$(dirname "$0")/../.."

py() { if command -v python3 >/dev/null 2>&1; then python3 "$@"; else python "$@"; fi; }

PYTHONIOENCODING=utf-8 py - <<'PY'
import copy, importlib.util, json, sys
spec = importlib.util.spec_from_file_location("coverage", "scripts/guard/onbid-field-coverage.py")
cov = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cov)

inventory = json.loads(cov.INVENTORY.read_text(encoding="utf-8"))
source = json.loads(cov.SOURCE.read_text(encoding="utf-8"))
lakehouse = json.loads(cov.LAKEHOUSE.read_text(encoding="utf-8"))
record, reads = cov.registry_view()
panel = cov.PANEL.read_text(encoding="utf-8")

def run(inv=inventory, lh=lakehouse, rec=record, w=reads, p=panel):
    return cov.check(inv, source, lh, rec, w, p)

assert run() == [], run()

def field(inv, api, predicate):
    return next(f for f in inv["apis"][api]["fields"] if predicate(f))

failed = []
def expect(name, failures, needle):
    if not any(needle in f for f in failures):
        failed.append(f"{name}: expected a failure naming {needle!r}, got {failures}")

inv = copy.deepcopy(inventory)
f = field(inv, "item_list", lambda f: not f.get("web") and "same_as" not in f)
f.pop("reason")
expect("no reason", run(inv), "gives no reason")

inv = copy.deepcopy(inventory)
f = field(inv, "item_list", lambda f: f.get("web"))
f["gold"] = []
expect("shown not served", run(inv), "shown but not served")

inv = copy.deepcopy(inventory)
f = field(inv, "item_list", lambda f: f.get("silver"))
f["silver"] = f["silver"] + ["silver.onbid_real_estate_item.no_such_column"]
expect("missing column", run(inv), "no_such_column is no column")

inv = copy.deepcopy(inventory)
f = field(inv, "item_list", lambda f: f.get("gold"))
f["gold"] = f["gold"] + ["public_auction.rounds[].no_such_path"]
expect("missing path", run(inv), "no_such_path is no path")

expect("web pact", run(w=set()), "does not read")
expect("panel leaf", run(p=""), "does not use")

inv = copy.deepcopy(inventory)
f = field(inv, "item_list", lambda f: f.get("silver"))
f["pii"] = True
expect("pii", run(inv), "must not reach Silver")

lh = copy.deepcopy(lakehouse)
lh["contracts"]["silver.onbid_real_estate_item"]["columns"].append({"name": "unclaimed_column"})
expect("unclaimed column", run(lh=lh), "unclaimed_column: no inventory field claims it")

expect("unclaimed path", run(rec=record + ["public_auction.rounds[].unclaimed_path"]), "unclaimed_path: no inventory")

inv = copy.deepcopy(inventory)
f = field(inv, "item_detail", lambda f: "same_as" in f)
f["same_as"] = "item_list.noSuchField"
expect("same_as", run(inv), "is no inventory field")

if failed:
    print("\n".join(failed), file=sys.stderr)
    print("FAIL onbid-field-coverage-self-test", file=sys.stderr)
    sys.exit(1)
print("OK onbid-field-coverage-self-test")
PY
