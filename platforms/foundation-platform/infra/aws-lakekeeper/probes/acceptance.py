"""Prod's acceptance task (acceptance.tf): the A1 tests that need prod, root ADR-0624 decisions 4-4 and 4-7.

python -I -c <loader; this file> <step> [args]: the shared part (common.py) comes as the module `common`. Steps:
health; lake-mints <count> [workers] [namespace.table] (T13, common); lake-migrate <namespace.table> (T17);
lake-probe-drop. Its tables live in ACCEPTANCE_NAMESPACE only, never in a namespace the warehouse declares, and
lake-probe-drop removes that namespace with every table and file in it right after, so the reconciler finds no drift.

It runs in the release's code-location image (orchestration/dagster/Dockerfile.aws), whose user writes only /tmp:
WORK is its scratch. Its Python takes DuckDB by hash, the wheel of the DuckDB the publisher bundles, and loads the
DuckDB extension files the image carries, by path, as the publisher's duckdb_extensions::load does: nothing is
downloaded at run time (root ADR-0559 section 6). Its execution role is the reconciler's, the one role that reads
the warehouse credential (coordinator 2026-11-28, option b); T17 reads Basin READ_ONLY with that credential's token.
"""

import json
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from common import (
    CATALOG,
    SCOPE,
    SECRETS,
    TOKEN_URL,
    call,
    check,
    emit,
    my_token,
    prefix,
    root,
    run,
    step_health,
    step_lake_mints,
    warehouse,
    warehouse_id,
)

WORK = os.environ["WORK"]
SITE = f"{WORK}/site"
ACCEPTANCE_NAMESPACE = os.environ["ACCEPTANCE_NAMESPACE"]
# The release image's own paths (Dockerfile.aws): the extension files the publisher loads, their contract, and the
# table contracts.
RELEASE_EXTENSIONS = "/opt/foundation-platform/release/duckdb-extensions"
EXTENSIONS_CONTRACT = "/opt/foundation-platform/current/infra/lakehouse/contracts/duckdb-extensions.contract.json"
TABLE_CONTRACTS = "/opt/foundation-platform/current/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
# The abi3 wheel for linux aarch64 of DuckDB v2.0.0, the version duckdb-extensions.contract.json pins (PyPI,
# 2026-11-28); it has no dependency.
REQUIREMENTS = "duckdb==2.0.0 --hash=sha256:e38daf0acbad9de7ce892d402b68409519291ee16af3c2cba85ace3b085c7eaa\n"


def duckdb_connect():
    """A DuckDB connection with httpfs, avro and iceberg loaded from the image's files, automatic install and load off,
    each held to its contract's build."""
    if not os.path.isdir(SITE):
        os.makedirs(WORK, exist_ok=True)
        with open(f"{WORK}/requirements.txt", "w", encoding="utf-8") as file:
            file.write(REQUIREMENTS)
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--disable-pip-version-check", "--no-cache-dir",
                        "--require-hashes", "--only-binary=:all:", "--no-deps", "--target", SITE,
                        "-r", f"{WORK}/requirements.txt"], check=True, stdout=subprocess.DEVNULL,
                       env={**os.environ, "HOME": WORK, "PIP_CACHE_DIR": f"{WORK}/cache"})
    sys.path.insert(0, SITE)
    import duckdb
    con = duckdb.connect()
    os.makedirs(f"{WORK}/duckdb_tmp", exist_ok=True)
    con.execute(f"SET home_directory='{WORK}'; SET temp_directory='{WORK}/duckdb_tmp'; SET enable_progress_bar=false; "
                "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false")
    with open(EXTENSIONS_CONTRACT, encoding="utf-8") as file:
        contract = json.load(file)
    library = con.execute("SELECT library_version FROM pragma_version()").fetchall()[0][0]
    if not check("DuckDB is the version the image's extensions are built for", contract["duckdb_version"], library):
        sys.exit(1)
    machine = f"{platform.system().lower()}-{platform.machine()}"
    for name in contract["load_order"]:
        if name == "spatial":
            continue
        distribution = contract["distributions"][name]
        con.execute(f"LOAD '{RELEASE_EXTENSIONS}/{distribution['platforms'][machine]['file']['name']}'")
        version = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = ? AND loaded",
                              [name]).fetchall()
        if not check(f"extension {name} is the pinned build", distribution["extension_version"],
                     version[0][0] if version else None):
            sys.exit(1)
    return con


def acceptance_namespace():
    target = ACCEPTANCE_NAMESPACE
    if not target or target in warehouse()["namespaces"]:
        check("the acceptance namespace is set and is not a declared one", "an undeclared namespace", target)
        sys.exit(1)
    return target


def basin():
    """Basin (the Cloudflare R2 Data Catalog of the warehouse's bucket) through the warehouse credential's token: an
    Admin Read & Write token of the account, which Basin accepts (coordinator 2026-11-28, option b). Read only:
    DuckDB attaches it READ_ONLY and the REST calls here are GETs."""
    parts = os.environ.get("R2_CREDENTIAL", "").split(":")
    if len(parts) != 4 or not all(parts):
        check("R2 credential has its four parts", 4, sum(1 for part in parts if part))
        sys.exit(1)
    SECRETS.extend(part for part in parts if len(part) >= 8)
    account, token = parts[0], parts[3]
    if not token.replace("-", "").replace("_", "").isalnum():
        check("the Basin token is one plain word", True, False)
        sys.exit(1)
    bucket = warehouse()["bucket"]
    return f"https://catalog.cloudflarestorage.com/{account}/{bucket}", f"{account}_{bucket}", token


def basin_get(uri, token, path):
    request = urllib.request.Request(uri + path, headers={
        "Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": "iceberg-rest-client"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def step_lake_migrate(source):
    """T17: the migration method of root ADR-0624 A1 on one Basin table. Its shape (schema, partition spec, sort order)
    comes from Basin, and an empty table of that shape is made in the acceptance namespace (at a switch the reconciler
    makes it from the table's contract). DuckDB attaches Basin READ_ONLY and this catalog, pins Basin's head and copies
    it with INSERT ... SELECT. Then both sides must have equal row counts, unique keys (the contract's key and
    newest_first, its quality gate) and equal hashes per column and per row."""
    target = acceptance_namespace()
    namespace, name = source.split(".")
    with open(TABLE_CONTRACTS, encoding="utf-8") as file:
        contract = json.load(file)["contracts"][source]
    latest = contract.get("current_row_latest") or {}
    key = [*latest.get("key", []), *latest.get("newest_first", [])]
    basin_uri, basin_warehouse, basin_token = basin()
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    try:
        config = basin_get(basin_uri, basin_token, f"/v1/config?warehouse={urllib.parse.quote(basin_warehouse)}")
        basin_prefix = {**(config.get("defaults") or {}), **(config.get("overrides") or {})}.get("prefix")
        base = f"/v1/{basin_prefix}" if basin_prefix else "/v1"
        meta = basin_get(basin_uri, basin_token, f"{base}/namespaces/{namespace}/tables/{name}")["metadata"]
    except urllib.error.HTTPError as error:
        check("Basin load-table", 200, error.code)
        return
    schema = next(item for item in meta["schemas"] if item["schema-id"] == meta["current-schema-id"])
    spec = next(item for item in meta["partition-specs"] if item["spec-id"] == meta["default-spec-id"])
    order = next((item for item in meta.get("sort-orders", []) if item["order-id"] == meta.get("default-sort-order-id")), None)
    emit(note="Basin table", table=source, fields=len(schema["fields"]), partition_fields=len(spec["fields"]),
         sort_fields=len((order or {}).get("fields", [])), key=key)

    jwt = my_token()
    pre = prefix(jwt)
    status, kind, _ = call("POST", f"/catalog/v1/{pre}/namespaces", jwt, {"namespace": [target]})
    check(f"namespace {target}", [200, "exists"], "exists" if status == 409 else status, error=kind)
    request = {"name": name, "schema": schema, "partition-spec": spec}
    if order:
        request["write-order"] = order
    status, kind, body = call("POST", f"/catalog/v1/{pre}/namespaces/{target}/tables", jwt, request)
    if not check(f"create {target}.{name} in Basin's shape", 200, status, error=kind):
        return
    check(f"the location of {target}.{name}", f"{root()}/{target}/{name}", body["metadata"]["location"].rstrip("/"))

    con = duckdb_connect()
    client_id, client_secret = os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"]
    if not (client_id.isalnum() and client_secret.isalnum()):
        check("client credentials are alphanumeric", True, False)
        return

    def run(sql):
        try:
            return 200, con.execute(sql).fetchall()
        except Exception as error:  # the message is redacted of every held secret by emit
            return f"{type(error).__name__}: {str(error)[:240]}", None

    # The secrets go into SQL text: never let a statement's text reach a traceback (run prints only the error).
    for label, sql in (
            ("Basin", f"CREATE SECRET basin (TYPE ICEBERG, TOKEN '{basin_token}')"),
            ("Lakekeeper", f"CREATE SECRET lk (TYPE ICEBERG, CLIENT_ID '{client_id}', CLIENT_SECRET '{client_secret}', "
                           f"OAUTH2_SERVER_URI '{TOKEN_URL}', OAUTH2_SCOPE '{SCOPE}')")):
        status, _ = run(sql)
        if not check(f"CREATE SECRET for {label}", 200, status):
            return
    status, _ = run(f"ATTACH '{basin_warehouse}' AS basin (TYPE ICEBERG, ENDPOINT '{basin_uri}', SECRET basin, READ_ONLY)")
    if not check("ATTACH Basin READ_ONLY", 200, status):
        return
    status, result = run("SELECT readonly FROM duckdb_databases() WHERE database_name = 'basin'")
    check("DuckDB holds Basin read-only", True, result[0][0] if result else status)
    status, _ = run(f"ATTACH '{warehouse()['bucket']}' AS lk (TYPE ICEBERG, ENDPOINT '{CATALOG}/catalog', SECRET lk)")
    if not check("ATTACH Lakekeeper with the OAuth2 secret", 200, status):
        return
    status, snapshots = run(f"SELECT snapshot_id FROM iceberg_snapshots(basin.{namespace}.{name}) ORDER BY timestamp_ms DESC LIMIT 1")
    if not check("Basin's head snapshot", 200, status if snapshots else "none"):
        return
    snapshot = snapshots[0][0]
    columns = ", ".join(f'"{field["name"]}"' for field in schema["fields"])
    # Basin's head, the snapshot this run listed a moment before: read in this run only, nothing has to keep it.
    pinned = f"(SELECT * FROM basin.{namespace}.{name} AT (VERSION => {snapshot}))"
    copy = f"lk.{target}.{name}"
    started = time.time()
    status, _ = run(f"INSERT INTO {copy} ({columns}) SELECT {columns} FROM {pinned} s")
    if not check("INSERT ... SELECT from Basin into Lakekeeper", 200, status, seconds=round(time.time() - started, 1),
                 basin_snapshot=snapshot):
        return

    def profile(relation):
        keyed = ", ".join(f'"{column}"' for column in key)
        parts = ["count(*)::VARCHAR", f"sum(hash({columns}))::VARCHAR"]
        parts += [f'concat(count("{field["name"]}"), \':\', sum(hash("{field["name"]}")))' for field in schema["fields"]]
        status, rows = run(f"SELECT {', '.join(parts)} FROM {relation} s")
        if status != 200:
            return status, None
        distinct = run(f"SELECT count(*) FROM (SELECT DISTINCT {keyed} FROM {relation} s)")[1] if key else None
        values = rows[0]
        return 200, {"rows": values[0], "row_hash": values[1], "distinct_keys": str(distinct[0][0]) if distinct else None,
                     "columns": {field["name"]: values[2 + index] for index, field in enumerate(schema["fields"])}}

    status, basin_side = profile(pinned)
    check("profile of Basin's side", 200, status)
    status, copied = profile(copy)
    check("profile of the copy", 200, status)
    if not (basin_side and copied):
        return
    emit(note="T17 sides", basin_rows=basin_side["rows"], copy_rows=copied["rows"], key=key)
    check("T17 equal row counts", basin_side["rows"], copied["rows"])
    check("T17 keys unique in Basin", basin_side["rows"], basin_side["distinct_keys"])
    check("T17 keys unique in the copy", copied["rows"], copied["distinct_keys"])
    check("T17 equal hash of every row", basin_side["row_hash"], copied["row_hash"])
    differ = sorted(column for column, value in basin_side["columns"].items() if copied["columns"].get(column) != value)
    check("T17 equal count and hash per column", 0, len(differ), differ=differ, columns=len(basin_side["columns"]))


def step_lake_probe_drop():
    """As the operator: the acceptance namespace and everything in it at once, its files purged (upstream concepts,
    v0.15.1: in a soft-deletion warehouse a recursive drop needs force, and removes every table below it at once,
    files only with purge). Then it must be gone, with nothing of it waiting for expiry."""
    target = acceptance_namespace()
    jwt = my_token()
    pre = prefix(jwt)
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    status, kind, _ = call("DELETE", f"/catalog/v1/{pre}/namespaces/{target}?recursive=true&force=true&purge=true", jwt)
    check(f"drop namespace {target} with everything in it, purged", [200, 204], status, error=kind)
    status, kind, _ = call("GET", f"/catalog/v1/{pre}/namespaces/{target}", jwt)
    check(f"namespace {target} is gone", 404, status, error=kind)
    wid = warehouse_id(jwt)
    status, kind, body = call("GET", f"/management/v1/warehouse/{wid}/deleted-tabulars", jwt)

    def namespace_of(item):
        value = item.get("namespace")
        return value[0] if isinstance(value, list) and value else value

    waiting = [item.get("name") for item in (body or {}).get("tabulars", []) if namespace_of(item) == target]
    check(f"nothing of {target} waits for expiry", 0, len(waiting), waiting=waiting, status=status)
    status, kind, body = call("POST", f"/management/v1/warehouse/{wid}/task/list", jwt, {})
    for task in (body or {}).get("tasks", [])[:20]:
        emit(note="task", **{key: task.get(key) for key in ("queue-name", "status", "entity-name", "scheduled-for")})


STEPS = {
    "health": lambda: step_health(),
    "lake-mints": lambda count, workers="50", table="silver.w109_smoke": step_lake_mints(count, workers, table),
    "lake-migrate": lambda source: step_lake_migrate(source),
    "lake-probe-drop": lambda: step_lake_probe_drop(),
}

sys.exit(run(STEPS, sys.argv[1:]))
