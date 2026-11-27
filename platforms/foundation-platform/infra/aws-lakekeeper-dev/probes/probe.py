"""The Lakekeeper dev rig's probe: one test step, run as a principal from inside the VPC (probes.tf).

python -I -c <this file> <step> [args]. The standard library only. Every line it prints is one JSON object:
a check with what was expected, what came back and whether they agree, or a note. It never prints a token, a
secret or a response body that could hold one: only status codes, error types and the claims named in CLAIMS.
The exit code is 0 when every check agreed, 1 when one did not, 2 on a usage error.

Environment (the task definition's): CATALOG_URL, TOKEN_URL, SCOPE, OTHER_SCOPE, PRINCIPAL, PRINCIPALS (a JSON
map of principal name to client id; ids are identifiers, not secrets), and CLIENT_ID / CLIENT_SECRET for a
principal that has a client (the anonymous probe has none).
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

CATALOG = os.environ["CATALOG_URL"].rstrip("/")
TOKEN_URL = os.environ["TOKEN_URL"]
SCOPE = os.environ["SCOPE"]
OTHER_SCOPE = os.environ["OTHER_SCOPE"]
PRINCIPAL = os.environ["PRINCIPAL"]
PRINCIPALS = json.loads(os.environ["PRINCIPALS"])
# The claims a check may show: none of them is a credential.
CLAIMS = ("iss", "sub", "client_id", "token_use", "scope", "version")
FAILED = []
# The parts of a credential this run holds: no printed line may contain one.
SECRETS = [os.environ[key] for key in ("CLIENT_SECRET",) if os.environ.get(key)]


def emit(**fields):
    line = json.dumps({"principal": PRINCIPAL, **fields}, sort_keys=True)
    for secret in SECRETS:
        line = line.replace(secret, "<redacted>")
    print(line, flush=True)


def check(name, expected, actual, **extra):
    ok = actual in expected if isinstance(expected, (list, tuple, set)) else actual == expected
    if not ok:
        FAILED.append(name)
    emit(check=name, expected=expected, actual=actual, ok=ok, **extra)
    return ok


def token(scope=SCOPE):
    """An access token of this principal's client (client credentials), or None with the error type."""
    basic = base64.b64encode(f"{os.environ['CLIENT_ID']}:{os.environ['CLIENT_SECRET']}".encode()).decode()
    body = urllib.parse.urlencode({"grant_type": "client_credentials", "scope": scope}).encode()
    request = urllib.request.Request(TOKEN_URL, data=body, method="POST", headers={
        "Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)["access_token"], None
    except urllib.error.HTTPError as error:
        return None, f"{error.code} {json.loads(error.read() or b'{}').get('error', '')}"


def claims(jwt):
    payload = jwt.split(".")[1]
    data = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    shown = {key: data[key] for key in CLAIMS if key in data}
    shown["lifetime_seconds"] = data["exp"] - data["iat"]
    shown["aud_present"] = "aud" in data
    return shown, data


def call(method, path, jwt=None, body=None, project=None, headers=None):
    """(status, error type, body) of one catalog call; the body is never printed."""
    headers = {"Accept": "application/json", **(headers or {})}
    if jwt is not None:
        headers["Authorization"] = f"Bearer {jwt}"
    if project is not None:
        headers["x-project-id"] = project
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(CATALOG + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
            return response.status, None, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            detail = json.loads(raw).get("error", {})
            kind = detail.get("type")
            if os.environ.get("SHOW_ERROR_MESSAGES") == "true":
                kind = f"{kind}: {detail.get('message')}"
        except (ValueError, AttributeError):
            # Not Lakekeeper's error object (a request the server could not read): its text, when asked for.
            kind = raw.decode("utf-8", "replace")[:300] if os.environ.get("SHOW_ERROR_MESSAGES") == "true" else None
        return error.code, kind, None
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        return None, type(error).__name__, None


def my_token():
    jwt, error = token()
    if jwt is None:
        check("token", "issued", error)
        sys.exit(1)
    return jwt


def user_id(name):
    return f"oidc~{PRINCIPALS[name]}"


# ---- steps -------------------------------------------------------------------------------------------------


def step_health():
    status, kind, body = call("GET", "/health")
    check("health", 200, status, error=kind, health=body)


def step_info():
    status, kind, body = call("GET", "/management/v1/info", my_token())
    fields = {key: body.get(key) for key in ("version", "bootstrapped", "authz-backend", "server-id")} if body else {}
    check("info", 200, status, error=kind, **fields)


def step_claims():
    shown, _ = claims(my_token())
    emit(note="claims", **shown)


def step_bootstrap():
    status, kind, _ = call("POST", "/management/v1/bootstrap", my_token(), {"accept-terms-of-use": True, "is-operator": True})
    check("bootstrap", 204, status, error=kind)


def step_whoami():
    status, kind, body = call("GET", "/management/v1/whoami", my_token())
    check("whoami", 200, status, error=kind, id=(body or {}).get("id"), user_type=(body or {}).get("user-type"))


def step_identity():
    """T6 for this principal: the token must be the reason a call passes or fails."""
    path = "/management/v1/whoami"
    status, kind, _ = call("GET", path)
    check("no token", 401, status, error=kind)
    status, kind, _ = call("GET", path, "not-a-jwt")
    check("garbage token", 401, status, error=kind)
    if "CLIENT_ID" not in os.environ:
        return
    jwt = my_token()
    shown, _ = claims(jwt)
    emit(note="claims", **shown)
    head, payload, signature = jwt.split(".")
    flipped = signature[:-2] + ("AA" if signature[-2:] != "AA" else "BB")
    status, kind, _ = call("GET", path, f"{head}.{payload}.{flipped}")
    check("tampered signature", 401, status, error=kind)
    declared = os.environ.get("DECLARED") == "true"
    status, kind, body = call("GET", path, jwt)
    # A declared principal is authenticated (not 401); whoami answers 404 UserNotFound until it is registered.
    check("own token authenticates", [200, 404] if declared else 401, status, error=kind, id=(body or {}).get("id"))
    if declared and status == 200:
        check("mapped to its client", user_id(PRINCIPAL), (body or {}).get("id"))
    if OTHER_SCOPE in shown.get("scope", "") or PRINCIPAL == "lane-b":
        other, error = token(OTHER_SCOPE)
        if other is None:
            check("token of the other scope", "issued", error)
        else:
            status, kind, _ = call("GET", path, other)
            check("token without the catalog scope", 401, status, error=kind, scope=claims(other)[0].get("scope"))


def step_expiry():
    """T6: a token past its exp is refused, and a new one passes (lane-b's tokens live 5 minutes)."""
    jwt = my_token()
    shown, data = claims(jwt)
    # jsonwebtoken's default leeway is 60 s (limes, the validator Lakekeeper uses): 30 s past exp still passes.
    time.sleep(max(0, data["exp"] - int(time.time())) + 30)
    status, kind, _ = call("GET", "/management/v1/whoami", jwt)
    emit(note="30 s past exp, inside the leeway", status=status, error=kind, lifetime_seconds=shown["lifetime_seconds"])
    time.sleep(max(0, data["exp"] - int(time.time())) + 90)
    status, kind, _ = call("GET", "/management/v1/whoami", jwt)
    check("expired token, 90 s past exp", 401, status, error=kind)
    status, kind, _ = call("GET", "/management/v1/whoami", my_token())
    check("new token after expiry", 200, status, error=kind)


def step_authz(expect_role_create):
    """T7 without storage: what this principal may do in the default project, decided by OpenFGA."""
    jwt = my_token()
    status, kind, body = call("GET", "/management/v1/project/actions", jwt)
    emit(note="project actions", status=status, error=kind, actions=sorted((body or {}).get("allowed-actions", [])))
    status, kind, _ = call("POST", "/management/v1/project", jwt, {"project-name": f"probe-{PRINCIPAL}-{int(time.time())}"})
    check("create project", 403, status, error=kind)
    status, kind, _ = call("POST", "/management/v1/server/grants", jwt, {"writes": [
        {"privilege": "admin", "principal": {"user": user_id(PRINCIPAL)}}]})
    check("grant itself server admin", [400, 403], status, error=kind)
    name = f"probe-{PRINCIPAL}-{int(time.time())}"
    status, kind, body = call("POST", "/management/v1/role", jwt, {"name": name})
    check("create role", 201 if expect_role_create else 403, status, error=kind)
    if status == 201:
        status, kind, _ = call("DELETE", f"/management/v1/role/{body['id']}", jwt)
        check("delete own role", 204, status, error=kind)


def step_config():
    """The catalog's /config, the call upstream says registers a new principal on its first connect."""
    jwt = my_token()
    status, kind, _ = call("GET", "/catalog/v1/config", jwt)
    emit(note="config without a warehouse", status=status, error=kind)
    status, kind, body = call("GET", "/management/v1/whoami", jwt)
    check("whoami after config", 200, status, error=kind, id=(body or {}).get("id"))
    if status == 200:
        check("mapped to its client", user_id(PRINCIPAL), body.get("id"))


def step_provision(principal):
    """As the operator: register a declared principal under its client id (the reconciler's move)."""
    status, kind, body = call("POST", "/management/v1/user", my_token(), {
        "id": user_id(principal), "name": principal, "user-type": "application", "update-if-exists": True})
    check(f"register {principal}", [200, 201], status, error=kind, id=(body or {}).get("id"))


def step_warehouse_create():
    """As the operator: make the contract's warehouse once, with the R2 credential the owner put (warehouse.tf).
    The credential goes into the request body only; every printed line is redacted of its parts."""
    parts = os.environ.get("R2_CREDENTIAL", "").split(":")
    if len(parts) != 4 or not all(parts):
        check("R2 credential has its four parts", 4, sum(1 for part in parts if part))
        return
    SECRETS.extend(part for part in parts[1:] if len(part) >= 8)
    account, key_id, secret_key, r2_token = parts
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    warehouse = json.loads(os.environ["WAREHOUSE"])
    jwt = my_token()
    status, kind, body = call("GET", "/management/v1/warehouse", jwt)
    found = [item for item in (body or {}).get("warehouses", []) if item.get("name") == warehouse["bucket"]]
    if found:
        emit(note="warehouse exists", id=found[0].get("id"), status=found[0].get("status"))
        return
    # The API needs sts-enabled although upstream's R2 example leaves it out (the R2 type forces it to true anyway).
    profile = {"type": "s3", "bucket": warehouse["bucket"], "region": warehouse["region"], "sts-enabled": True,
               "flavor": "s3-compat", "endpoint": f"https://{account}.r2.cloudflarestorage.com",
               "storage-layout": warehouse["layout"]}
    if warehouse["key_prefix"]:
        profile["key-prefix"] = warehouse["key_prefix"]
    status, kind, body = call("POST", "/management/v1/warehouse", jwt, {
        "warehouse-name": warehouse["bucket"], "delete-profile": warehouse["delete_profile"], "storage-profile": profile,
        "storage-credential": {"type": "s3", "credential-type": "cloudflare-r2", "account-id": account,
                               "access-key-id": key_id, "secret-access-key": secret_key, "token": r2_token}})
    check("create warehouse", [200, 201], status, error=kind, id=(body or {}).get("warehouse-id") or (body or {}).get("id"))


def step_warehouse_drop():
    """As the operator: delete the warehouse so that warehouse-create makes it again from the contract. A storage
    profile's key prefix cannot change (upstream S3Profile::update_with, v0.15.1: ImmutableField key_prefix), so a
    new key prefix is a new warehouse. Every namespace goes first, recursively, at once and with its files (force
    skips the soft deletion); the delete waits while the purge tasks run (409 WarehouseHasUnfinishedTasks). Dev
    only: the rig's tables are probe tables."""
    jwt = my_token()
    wid = warehouse_id(jwt)
    pre = prefix(jwt)
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    status, kind, body = call("GET", f"/catalog/v1/{pre}/namespaces", jwt)
    for namespace in (body or {}).get("namespaces", []):
        name = urllib.parse.quote("\x1f".join(namespace))
        status, kind, _ = call("DELETE", f"/catalog/v1/{pre}/namespaces/{name}?recursive=true&force=true&purge=true", jwt)
        check(f"drop namespace {'.'.join(namespace)} with everything in it", 204, status, error=kind)
    deadline = time.time() + 600
    while True:
        status, kind, _ = call("DELETE", f"/management/v1/warehouse/{wid}", jwt)
        if status != 409 or time.time() > deadline:
            break
        time.sleep(15)
    check("delete the warehouse", [200, 204], status, error=kind)


def step_grant(write, privilege, principal):
    """As the operator: write or delete one project assignment of a principal (the reconciler's move)."""
    change = {"writes" if write else "deletes": [{"type": privilege, "user": user_id(principal)}]}
    status, kind, _ = call("POST", "/management/v1/permissions/project/assignments", my_token(), change)
    check(f"{'write' if write else 'delete'} {privilege} for {principal}", 204, status, error=kind)


def step_refused():
    """T7: with OpenFGA (or the issuer) down, calls that need a decision must not pass."""
    jwt, error = token()
    if jwt is None:
        check("token while refused", "issued", error)
        return
    for method, path, body in (("GET", "/management/v1/whoami", None), ("GET", "/management/v1/project/actions", None),
                               ("GET", "/management/v1/role", None),
                               ("POST", "/management/v1/role", {"name": f"probe-refused-{int(time.time())}"})):
        status, kind, _ = call(method, path, jwt, body)
        emit(note="while refused", call=f"{method} {path}", status=status, error=kind,
             passed=status is not None and 200 <= status < 300)


def step_loop(seconds):
    """T14/T16: calls for a while, counting answers: reads (whoami, list roles) and writes (create + delete a role)."""
    deadline = time.time() + float(seconds)
    counts = {}
    jwt, expires = my_token(), time.time() + 240
    while time.time() < deadline:
        if time.time() > expires:
            jwt, expires = my_token(), time.time() + 240
        for label, method, path, body in (("read whoami", "GET", "/management/v1/whoami", None),
                                          ("read roles", "GET", "/management/v1/role", None)):
            status, kind, _ = call(method, path, jwt, body)
            counts.setdefault(label, {}).setdefault(str(status if status else kind), 0)
            counts[label][str(status if status else kind)] += 1
        status, kind, body = call("POST", "/management/v1/role", jwt, {"name": f"loop-{PRINCIPAL}-{time.time_ns()}"})
        counts.setdefault("write create role", {}).setdefault(str(status if status else kind), 0)
        counts["write create role"][str(status if status else kind)] += 1
        if status == 201:
            status, kind, _ = call("DELETE", f"/management/v1/role/{body['id']}", jwt)
            counts.setdefault("write delete role", {}).setdefault(str(status if status else kind), 0)
            counts["write delete role"][str(status if status else kind)] += 1
        time.sleep(0.2)
    emit(note="loop", seconds=float(seconds), counts=counts)
    bad = {label: {code: n for code, n in by_code.items() if code not in ("200", "201", "204")}
           for label, by_code in counts.items()}
    check("loop: every call answered 2xx", {}, {label: by for label, by in bad.items() if by})


# ---- the warehouse's tests (T3, T4, T5, T7/T31, T9, T11a, T13, T14, T33, T36) ------------------------------
# These need the warehouse and third-party clients: PyIceberg (REST + OAuth2 + vended credentials), DuckDB (ATTACH
# with an OAuth2 secret) and boto3 (raw S3 calls with the vended credentials). They are installed into the task's
# scratch volume at the start of the step, pinned. Every table here is a dev _smoke table of silver.

SITE = "/work/site"
PACKAGES = ("pyiceberg[pyarrow]==0.12.0", "duckdb==2.0.0", "boto3==1.44.12")
SMOKE = ("w109_smoke", "w109_smoke_versions", "w109_smoke_history", "w109_smoke_drop", "w109_smoke_escape")
# The dev declaration the grants are generated from: what each lane writes and reads.
DECLARED = {"lane-a": {"w109_smoke": "write", "w109_smoke_history": "write", "w109_smoke_escape": "write",
                       "w109_smoke_versions": "select"},
            "lane-b": {"w109_smoke": "select"}}
VENDED = {"X-Iceberg-Access-Delegation": "vended-credentials"}


def deps():
    import subprocess
    # The root file system is read-only: temporary files go to the scratch volume too.
    os.makedirs("/work/tmp", exist_ok=True)
    os.environ["TMPDIR"] = "/work/tmp"
    if not os.path.isdir(SITE):
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--disable-pip-version-check", "--no-cache-dir",
                        "--target", SITE, *PACKAGES], check=True, stdout=subprocess.DEVNULL,
                       env={**os.environ, "HOME": "/work", "PIP_CACHE_DIR": "/work/cache"})
    if SITE not in sys.path:
        sys.path.insert(0, SITE)
    os.environ.setdefault("HOME", "/work")


def warehouse():
    return json.loads(os.environ["WAREHOUSE"])


def root():
    """s3://<bucket>/<key prefix>: where the warehouse's namespaces and tables live (decision 2, 2026-11-27: a
    tables-only key prefix, so no location outside it can hold a table)."""
    return f"s3://{warehouse()['bucket']}/{warehouse()['key_prefix']}".rstrip("/")


def prefix(jwt):
    status, kind, body = call("GET", f"/catalog/v1/config?warehouse={urllib.parse.quote(warehouse()['bucket'])}", jwt)
    if status != 200:
        check("catalog config", 200, status, error=kind)
        sys.exit(1)
    settings = {**(body.get("defaults") or {}), **(body.get("overrides") or {})}
    return settings["prefix"]


def warehouse_id(jwt):
    status, kind, body = call("GET", "/management/v1/warehouse", jwt)
    found = [item["id"] for item in (body or {}).get("warehouses", []) if item.get("name") == warehouse()["bucket"]]
    if not found:
        check("warehouse listed", 1, 0, status=status, error=kind)
        sys.exit(1)
    return found[0]


def load(jwt, table, vended=True):
    status, kind, body = call("GET", f"/catalog/v1/{prefix(jwt)}/namespaces/silver/tables/{table}", jwt,
                              headers=VENDED if vended else None)
    return status, kind, body


def vended_s3(body):
    """The table's vended S3 settings: from storage-credentials (REST spec) or, failing that, config."""
    config = dict(body.get("config") or {})
    for credential in body.get("storage-credentials") or []:
        config.update(credential.get("config") or {})
    for key in ("s3.secret-access-key", "s3.session-token", "s3.access-key-id"):
        if config.get(key):
            SECRETS.append(config[key])
    return config


def s3_client(config):
    deps()
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=config["s3.endpoint"], region_name=config.get("s3.region") or "auto",
                        aws_access_key_id=config["s3.access-key-id"], aws_secret_access_key=config["s3.secret-access-key"],
                        aws_session_token=config.get("s3.session-token"),
                        config=Config(retries={"max_attempts": 1}, s3={"addressing_style": "path"}))


def s3_status(action):
    """HTTP status of one raw S3 call (200 on success), never its body."""
    from botocore.exceptions import ClientError
    try:
        action()
        return 200
    except ClientError as error:
        return error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") or error.response["Error"].get("Code")


def catalog():
    deps()
    from pyiceberg.catalog.rest import RestCatalog
    return RestCatalog(PRINCIPAL, **{
        "uri": CATALOG + "/catalog", "warehouse": warehouse()["bucket"],
        "credential": f"{os.environ['CLIENT_ID']}:{os.environ['CLIENT_SECRET']}",
        "oauth2-server-uri": TOKEN_URL, "scope": SCOPE, "header.X-Iceberg-Access-Delegation": "vended-credentials"})


def outcome(action):
    """200 when the call went through, 403 when the catalog refused it, else the exception's type."""
    from pyiceberg.exceptions import ForbiddenError, UnauthorizedError
    try:
        action()
        return 200
    except ForbiddenError:
        return 403
    except UnauthorizedError:
        return 401
    except Exception as error:  # the type is the evidence; the message may quote a path, never a secret
        return f"{type(error).__name__}: {str(error)[:200]}"


def rows(batch, count=1, start=0):
    import pyarrow as pa
    return pa.table({"id": pa.array(range(start, start + count), pa.int64()),
                     "name": pa.array([f"synthetic-{i}" for i in range(start, start + count)], pa.string()),
                     "batch": pa.array([batch] * count, pa.string())})


def schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType
    return Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "name", StringType(), required=False),
                  NestedField(3, "batch", StringType(), required=False))


def step_lake_setup():
    """As the operator (the reconciler's moves): namespaces, the smoke tables, the lanes' users and their grants.
    T3: each table lands at <bucket>/silver/<table>, and string-prefix siblings both exist."""
    jwt = my_token()
    cat = catalog()
    for namespace in warehouse()["namespaces"]:
        cat.create_namespace_if_not_exists(namespace)
    ids = {}
    for name in SMOKE:
        table = cat.create_table_if_not_exists(f"silver.{name}", schema=schema())
        ids[name] = table.metadata.table_uuid
        check(f"T3 location of silver.{name}", [f"{root()}/silver/{name}", f"{root()}/silver/{name}/"], table.location())
    for lane in DECLARED:
        step_provision(lane)
    wid = warehouse_id(jwt)
    for lane, tables in DECLARED.items():
        for name, privilege in tables.items():
            status, kind, _ = call("POST", f"/management/v1/permissions/warehouse/{wid}/table/{ids[name]}/assignments", jwt,
                                   {"writes": [{"type": privilege, "user": user_id(lane)}]})
            check(f"grant {privilege} on silver.{name} to {lane}", 204, status, error=kind)


def step_lake_scope(canary=None):
    """T5: the vended credential of silver.w109_smoke reaches that table's prefix and nothing else."""
    jwt = my_token()
    status, kind, body = load(jwt, "w109_smoke")
    if not check("load silver.w109_smoke with vended credentials", 200, status, error=kind):
        return
    config = vended_s3(body)
    check("vended: a session token is present", True, bool(config.get("s3.session-token")))
    check("vended: refresh endpoint and expiry are given", True,
          bool(config.get("client.refresh-credentials-endpoint")) and bool(config.get("s3.session-token-expires-at-ms")))
    emit(note="vended config keys", keys=sorted(key for key in config if not key.endswith(("secret-access-key", "session-token"))))
    s3 = s3_client(config)
    bucket = warehouse()["bucket"]
    writer = DECLARED.get(PRINCIPAL, {}).get("w109_smoke") == "write"
    metadata_key = body["metadata-location"].split(f"s3://{bucket}/", 1)[1]
    probe_key = f"silver/w109_smoke/data/w109-probe-{time.time_ns()}.txt"
    check("GET the table's metadata", 200, s3_status(lambda: s3.get_object(Bucket=bucket, Key=metadata_key)))
    check("LIST the table's own prefix", 200, s3_status(lambda: s3.list_objects_v2(Bucket=bucket, Prefix="silver/w109_smoke/")))
    check("PUT inside the table", 200 if writer else 403,
          s3_status(lambda: s3.put_object(Bucket=bucket, Key=probe_key, Body=b"w109 probe")))
    if writer:
        emit(note="DELETE inside the table with a write credential (R2 cannot forbid it)",
             status=s3_status(lambda: s3.delete_object(Bucket=bucket, Key=probe_key)))
    check("LIST the string-prefix sibling silver/w109_smoke_versions/", 403,
          s3_status(lambda: s3.list_objects_v2(Bucket=bucket, Prefix="silver/w109_smoke_versions/")))
    sibling_object = "silver/w109_smoke_versions/metadata/x.json"
    check("GET in the sibling", 403, s3_status(lambda: s3.get_object(Bucket=bucket, Key=sibling_object)))
    check("LIST the bucket root", 403, s3_status(lambda: s3.list_objects_v2(Bucket=bucket, Delimiter="/")))
    if canary:
        check("GET the bronze canary", 403, s3_status(lambda: s3.get_object(Bucket=bucket, Key=canary)))
    absent_object = "w109-probe-absent"
    check("GET in another bucket (prod lakehouse, read attempt only)", 403,
          s3_status(lambda: s3.get_object(Bucket="foundation-platform-lakehouse-prod", Key=absent_object)))


def step_lake_privileges():
    """T7/T31: a lane may append and move refs on what it writes, and nothing that changes a table's shape."""
    cat = catalog()
    writer = DECLARED.get(PRINCIPAL, {}).get("w109_smoke") == "write"
    status = outcome(lambda: cat.load_table("silver.w109_smoke"))
    check("load silver.w109_smoke", 200, status)
    if status != 200:
        return
    table = cat.load_table("silver.w109_smoke")
    check("append", 200 if writer else 403, outcome(lambda: table.append(rows(f"priv-{PRINCIPAL}"))))
    table = cat.load_table("silver.w109_smoke")
    snapshot = table.current_snapshot()
    if snapshot is not None:
        tag = f"w109_{PRINCIPAL.replace('-', '_')}_{int(time.time())}"
        check("create a tag", 200 if writer else 403,
              outcome(lambda: table.manage_snapshots().create_tag(snapshot.snapshot_id, tag).commit()))
    check("create a table", 403, outcome(lambda: cat.create_table(f"silver.w109_smoke_by_{PRINCIPAL.replace('-', '_')}", schema=schema())))
    check("rename the table", 403, outcome(lambda: cat.rename_table("silver.w109_smoke", "silver.w109_smoke_renamed")))
    check("add a column", 403, outcome(lambda: _add_column(cat)))
    check("set a property", 403, outcome(lambda: _set_property(cat)))
    check("drop the table", 403, outcome(lambda: cat.drop_table("silver.w109_smoke")))


def _add_column(cat):
    from pyiceberg.types import StringType
    with cat.load_table("silver.w109_smoke").update_schema() as update:
        update.add_column(f"w109_{int(time.time())}", StringType())


def _set_property(cat):
    with cat.load_table("silver.w109_smoke").transaction() as transaction:
        transaction.set_properties({"w109.probe": str(int(time.time()))})


def step_lake_duckdb():
    """T9: DuckDB ATTACHes the warehouse with an OAuth2 secret and vended credentials, with no S3 key of its own."""
    deps()
    import duckdb
    writer = DECLARED.get(PRINCIPAL, {}).get("w109_smoke") == "write"
    con = duckdb.connect()
    con.execute("SET home_directory='/work'; SET extension_directory='/work/duckdb_extensions'; SET enable_progress_bar=false")
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs")
    emit(note="duckdb", version=duckdb.__version__)
    # The secret goes into SQL text: refuse a value that could end the literal, and never let the statement's text
    # reach a traceback (only the exception's type is printed).
    client_id, client_secret = os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"]
    if not (client_id.isalnum() and client_secret.isalnum()):
        check("client credentials are alphanumeric", True, False)
        return
    try:
        con.execute(f"CREATE SECRET lk (TYPE ICEBERG, CLIENT_ID '{client_id}', CLIENT_SECRET '{client_secret}', "
                    f"OAUTH2_SERVER_URI '{TOKEN_URL}', OAUTH2_SCOPE '{SCOPE}')")
    except Exception as error:
        check("CREATE SECRET", 200, type(error).__name__)
        return

    def run(sql):
        try:
            return 200, con.execute(sql).fetchall()
        except Exception as error:
            return f"{type(error).__name__}: {str(error)[:240]}", None

    status, _ = run(f"ATTACH '{warehouse()['bucket']}' AS lk (TYPE ICEBERG, ENDPOINT '{CATALOG}/catalog', SECRET lk)")
    if not check("ATTACH with the OAuth2 secret", 200, status):
        return
    status, result = run("SELECT count(*) FROM lk.silver.w109_smoke")
    check("SELECT through vended credentials", 200, status, rows=result[0][0] if result else None)
    status, _ = run(f"INSERT INTO lk.silver.w109_smoke (id, name, batch) VALUES (900, 'synthetic-duckdb', 'duckdb-{PRINCIPAL}')")
    if writer:
        check("INSERT", 200, status)
    else:
        check("INSERT refused for a reader", True, status != 200, status=status)
    status, snapshots = run("SELECT snapshot_id FROM iceberg_snapshots(lk.silver.w109_smoke) ORDER BY timestamp_ms LIMIT 1")
    if snapshots:
        status, result = run(f"SELECT count(*) FROM lk.silver.w109_smoke AT (VERSION => {snapshots[0][0]})")
        check("SELECT a pinned snapshot", 200, status, rows=result[0][0] if result else None)
    else:
        emit(note="iceberg_snapshots", status=status)
    bucket = warehouse()["bucket"]
    con.execute(f"CREATE SECRET bronze (TYPE S3, KEY_ID 'W109PROBEFAKEKEYID000', SECRET 'w109-probe-fake-secret', "
                f"SCOPE 's3://{bucket}/bronze/')")
    status, result = run("SELECT count(*) FROM lk.silver.w109_smoke")
    check("SELECT with a bronze-scoped S3 secret also present", 200, status)
    con.execute(f"CREATE SECRET table_scoped (TYPE S3, KEY_ID 'W109PROBEFAKEKEYID001', SECRET 'w109-probe-fake-secret', "
                f"SCOPE '{root()}/silver/w109_smoke/')")
    status, result = run("SELECT count(*) FROM lk.silver.w109_smoke")
    emit(note="SELECT with a wrong S3 secret scoped to the table's own path (which one wins)", status=status)


def step_lake_history(commits):
    """T33: after more than 100 commits a read by snapshot id or by tag still works; the first metadata file is gone."""
    commits = int(commits)
    cat = catalog()
    table = cat.load_table("silver.w109_smoke_history")
    table.append(rows("history-0"))
    table = cat.load_table("silver.w109_smoke_history")
    first, first_metadata = table.current_snapshot().snapshot_id, table.metadata_location
    tag = f"w109_pin_{int(time.time())}"
    check("tag the first snapshot", 200, outcome(lambda: table.manage_snapshots().create_tag(first, tag).commit()))
    for index in range(1, commits + 1):
        cat.load_table("silver.w109_smoke_history").append(rows(f"history-{index}", start=index))
    table = cat.load_table("silver.w109_smoke_history")
    emit(note="commits made", commits=commits, metadata_log=len(table.metadata.metadata_log))
    check("read by snapshot id", 1, table.scan(snapshot_id=first).to_arrow().num_rows)
    pinned = table.metadata.refs.get(tag)
    check("read by tag", 1, table.scan(snapshot_id=pinned.snapshot_id).to_arrow().num_rows if pinned else None)
    status, kind, body = load(my_token(), "w109_smoke_history")
    s3 = s3_client(vended_s3(body))
    bucket = warehouse()["bucket"]
    key = first_metadata.split(f"s3://{bucket}/", 1)[1]
    check("the first metadata file is gone (a pin by metadata_location fails)", [404, "NoSuchKey"],
          s3_status(lambda: s3.head_object(Bucket=bucket, Key=key)))


def step_lake_escape():
    """T36: a commit whose manifest list lies outside the table must be refused (lakekeeper#2217, unreleased)."""
    jwt = my_token()
    status, kind, body = load(jwt, "w109_smoke_escape", vended=False)
    metadata = body["metadata"]
    current = metadata.get("current-snapshot-id")
    current = None if current in (None, -1) else current
    new_id = time.time_ns() % (2 ** 62)
    outside = f"{root()}/silver/w109_smoke_versions/metadata/w109-escape-{new_id}.avro"
    request = {"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current}],
               "updates": [{"action": "add-snapshot", "snapshot": {
                   "snapshot-id": new_id, "parent-snapshot-id": current,
                   "sequence-number": metadata.get("last-sequence-number", 0) + 1, "timestamp-ms": int(time.time() * 1000),
                   "manifest-list": outside, "summary": {"operation": "append"}, "schema-id": metadata["current-schema-id"]}},
                   {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": new_id}]}
    if current is None:
        del request["updates"][0]["snapshot"]["parent-snapshot-id"]
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    status, kind, _ = call("POST", f"/catalog/v1/{prefix(jwt)}/namespaces/silver/tables/w109_smoke_escape", jwt, request)
    check("a commit pointing at another table's files is refused", [400, 403, 409, 422], status, error=kind)
    if status == 200:
        undo = [{"action": "remove-snapshot-ref", "ref-name": "main"}] if current is None else [
            {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": current}]
        status, kind, _ = call("POST", f"/catalog/v1/{prefix(jwt)}/namespaces/silver/tables/w109_smoke_escape", jwt,
                               {"requirements": [], "updates": undo})
        emit(note="main moved back", status=status, error=kind)


def step_lake_drop(phase, name="w109_smoke_drop"):
    """T11a as the operator: soft drop with purge requested and undrop, the same name made again while the dropped
    one waits, then (phase purge, after the delete profile's expiry) the name made again once the files are purged.
    PyIceberg sends purgeRequested=false unless asked, and Lakekeeper then never purges: the files stay and the
    name cannot be made again (seen 2026-11-27 on w109_smoke_drop)."""
    jwt = my_token()
    wid = warehouse_id(jwt)
    cat = catalog()
    ident = f"silver.{name}"
    if phase == "drop":
        table = cat.create_table_if_not_exists(ident, schema=schema())
        table.append(rows("drop-0"))
        tid = cat.load_table(ident).metadata.table_uuid
        status, kind, _ = call("POST", f"/management/v1/warehouse/{wid}/table/{tid}/protection", jwt, {"protected": True})
        check("protect the table", 200, status, error=kind)
        refused = outcome(lambda: cat.drop_table(ident, purge_requested=True))
        check("drop a protected table is refused", True, refused != 200, status=refused)
        call("POST", f"/management/v1/warehouse/{wid}/table/{tid}/protection", jwt, {"protected": False})
        check("soft drop with purge requested", 200, outcome(lambda: cat.drop_table(ident, purge_requested=True)))
        status, kind, body = call("GET", f"/management/v1/warehouse/{wid}/deleted-tabulars", jwt)
        listed = [item for item in (body or {}).get("tabulars", []) if item.get("id") == str(tid)]
        check("listed as deleted", 1, len(listed), expires=listed[0].get("expiration-date") if listed else None)
        status, kind, _ = call("POST", f"/management/v1/warehouse/{wid}/deleted-tabulars/undrop", jwt,
                               {"targets": [{"type": "table", "id": str(tid)}]})
        check("undrop", [200, 204], status, error=kind)
        check("readable after undrop", 1, cat.load_table(ident).scan().to_arrow().num_rows)
        check("drop again, purge requested", 200, outcome(lambda: cat.drop_table(ident, purge_requested=True)))
        emit(note="same name while the dropped table waits for purge",
             status=outcome(lambda: cat.create_table(ident, schema=schema())))
    else:
        emit(note="same name after the purge", status=outcome(lambda: cat.create_table_if_not_exists(ident, schema=schema())))


def step_lake_escape_create():
    """T4: where a table may be created. A lane has no create grant. The operator (the reconciler, the one creator)
    may create only under the warehouse's tables-only key prefix (decision 2): every location outside it, a
    string-prefix sibling of the prefix included, is refused by Lakekeeper's own location rules. With no key prefix
    the warehouse is the whole bucket and the operator's tries are only recorded (the first run, 2026-11-27)."""
    bucket, key_prefix = warehouse()["bucket"], warehouse()["key_prefix"]
    cat = catalog()
    lane = PRINCIPAL != "bootstrap"
    tries = {"bronze": f"s3://{bucket}/bronze/w109_escape_probe", "media": f"s3://{bucket}/media/w109_escape_probe",
             "another table's path": f"{root()}/silver/w109_smoke", "inside another table": f"{root()}/silver/w109_smoke/x",
             "the bucket root": f"s3://{bucket}", "another bucket": "s3://w109-nonexistent-probe-bucket/x"}
    if key_prefix:
        tries["a sibling of the key prefix"] = f"s3://{bucket}/{key_prefix}x/silver/w109_escape_probe"
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    for label, location in tries.items():
        ident = f"silver.w109_escape_{label.split()[-1].replace(chr(39), '')}_{int(time.time())}"
        status = outcome(lambda ident=ident, location=location: cat.create_table(ident, schema=schema(), location=location))
        if lane:
            check(f"lane creates at {label}", 403, status)
        elif key_prefix:
            check(f"operator creates at {label}", True, status != 200, status=status)
        else:
            emit(note="operator creates", at=label, location=location, status=status)
        if status == 200:
            emit(note="dropped again with purge", status=outcome(lambda ident=ident: cat.drop_table(ident, purge_requested=True)))


def step_lake_reconcile(phase):
    """T32: the reconciler's drift check. phase inject: a grant outside the declaration (lane-b select on
    w109_smoke_history). phase reconcile: per declared table, read the live assignments, remove every user grant the
    declaration does not hold (the operator's ownership stays), read again and compare."""
    jwt = my_token()
    wid = warehouse_id(jwt)
    cat = catalog()
    ids = {name: cat.load_table(f"silver.{name}").metadata.table_uuid for name in SMOKE if name != "w109_smoke_drop"}
    if phase == "inject":
        status, kind, _ = call("POST", f"/management/v1/permissions/warehouse/{wid}/table/{ids['w109_smoke_history']}/assignments",
                               jwt, {"writes": [{"type": "select", "user": user_id("lane-b")}]})
        check("grant outside the declaration", 204, status, error=kind)
        return
    operator = user_id("bootstrap")
    removed = 0
    for name, tid in ids.items():
        path = f"/management/v1/permissions/warehouse/{wid}/table/{tid}/assignments"
        status, kind, body = call("GET", path, jwt)
        live = {(item.get("type"), item.get("user") or f"role:{item.get('role')}") for item in (body or {}).get("assignments", [])}
        declared = {(privilege, user_id(lane)) for lane, tables in DECLARED.items() for table, privilege in tables.items() if table == name}
        extra = sorted(item for item in live - declared if item[1] != operator)
        missing = sorted(declared - live)
        emit(note="drift", table=name, extra=extra, missing=missing)
        if extra or missing:
            status, kind, _ = call("POST", path, jwt, {"deletes": [{"type": kind_, "user": user} for kind_, user in extra],
                                                       "writes": [{"type": kind_, "user": user} for kind_, user in missing]})
            check(f"repair {name}", 204, status, error=kind)
            removed += len(extra)
        status, kind, body = call("GET", path, jwt)
        live = {(item.get("type"), item.get("user")) for item in (body or {}).get("assignments", []) if item.get("user") != operator}
        check(f"live grants equal the declaration on {name}", True, sorted(live) == sorted(declared),
              live=sorted(live), declared=sorted(declared))
    emit(note="reconciled", removed=removed)


def step_lake_maint(phase):
    """T11b around the Spark runner (maintenance.tf), as the operator. setup: silver.w109_smoke_maint with snapshots
    s1-s5, a tag on s2 and a branch on s3, and silver.w109_smoke_maint_drop to be dropped by Spark. verify: every ref
    still reads, which snapshots are left, and whether Spark's drop went through soft deletion."""
    cat = catalog()
    ident = "silver.w109_smoke_maint"
    if phase == "setup":
        table = cat.create_table_if_not_exists(ident, schema=schema())
        snapshots = []
        for index in range(1, 6):
            cat.load_table(ident).append(rows(f"maint-s{index}", start=index))
            snapshots.append(cat.load_table(ident).current_snapshot().snapshot_id)
        table = cat.load_table(ident)
        table.manage_snapshots().create_tag(snapshots[1], "w109_tag_s2").create_branch(snapshots[2], "w109_branch_s3").commit()
        cat.create_table_if_not_exists(f"{ident}_drop", schema=schema()).append(rows("maint-drop"))
        emit(note="snapshots", s=snapshots, location=table.location())
        return
    table = cat.load_table(ident)
    refs = {name: ref.snapshot_id for name, ref in table.metadata.refs.items()}
    emit(note="refs", refs=refs, snapshots=[snapshot.snapshot_id for snapshot in table.metadata.snapshots])
    for name, snapshot_id in refs.items():
        check(f"ref {name} still reads", True, table.scan(snapshot_id=snapshot_id).to_arrow().num_rows > 0)
    jwt = my_token()
    status, kind, body = call("GET", f"/management/v1/warehouse/{warehouse_id(jwt)}/deleted-tabulars", jwt)
    names = [item.get("name") for item in (body or {}).get("tabulars", [])]
    check("Spark's DROP ... PURGE went through soft deletion", True, "w109_smoke_maint_drop" in names, deleted=names)


def step_lake_vended_keys():
    """T8's premise: where load-table puts the vended credential. iceberg-rust 0.11 builds a table's file access
    from the answer's config only (it parses storage-credentials and does not use it), so the credential must be
    in config. Key names only, and the values of the keys that are no credential."""
    status, kind, body = load(my_token(), "w109_smoke")
    check("load with vended credentials", 200, status, error=kind)
    shown = ("s3.region", "s3.path-style-access", "s3.remote-signing-enabled", "client.region")
    config = (body or {}).get("config") or {}
    emit(note="config", keys=sorted(config), values={key: config[key] for key in shown if key in config},
         endpoint_is_r2=(urllib.parse.urlparse(config.get("s3.endpoint", "")).hostname or "").endswith(".r2.cloudflarestorage.com"))
    for credential in (body or {}).get("storage-credentials") or []:
        emit(note="storage-credential", prefix=credential.get("prefix"), keys=sorted(credential.get("config") or {}))
    check("the credential is in config (what iceberg-rust 0.11 reads)", True,
          all(config.get(key) for key in ("s3.access-key-id", "s3.secret-access-key", "s3.session-token")))


def step_lake_refresh():
    """T12 (the part that fits a sitting): the vended credential says when it ends and where to renew it, and the
    renewal endpoint answers with a credential for the same table."""
    jwt = my_token()
    status, kind, body = load(jwt, "w109_smoke")
    config = vended_s3(body)
    ends = int(config.get("s3.session-token-expires-at-ms", 0)) / 1000 - time.time()
    emit(note="vended credential lifetime left", seconds=round(ends), refresh=config.get("client.refresh-credentials-endpoint", "")[-60:])
    status, kind, renewed = call("GET", f"/catalog/v1/{prefix(jwt)}/namespaces/silver/tables/w109_smoke/credentials", jwt)
    check("the renewal endpoint answers", 200, status, error=kind)
    credentials = (renewed or {}).get("storage-credentials", [])
    for credential in credentials:
        for key in ("s3.secret-access-key", "s3.session-token"):
            if (credential.get("config") or {}).get(key):
                SECRETS.append(credential["config"][key])
    check("it covers the table's own prefix", True,
          any(credential.get("prefix", "").startswith(f"{root()}/silver/w109_smoke") for credential in credentials),
          prefixes=[credential.get("prefix") for credential in credentials])


def step_lake_register_from(name, metadata_location):
    """T15 (the catalog half), the way a restore needs it: a table the catalog no longer knows, whose files are still
    there (dropped without purge, expired), registered again from its metadata.json alone."""
    cat = catalog()
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    check("register from metadata.json alone", 200, outcome(lambda: cat.register_table(f"silver.{name}", metadata_location)))
    table = cat.load_table(f"silver.{name}")
    emit(note="registered", snapshots=len(table.metadata.snapshots), rows=table.scan().to_arrow().num_rows)


def step_lake_cleanup():
    """As the operator: every dev _smoke table dropped with purge (so its files go too)."""
    cat = catalog()
    for ident in cat.list_tables("silver"):
        name = ".".join(ident)
        if ident[-1].startswith("w109_"):
            emit(note="drop with purge", table=name, status=outcome(lambda name=name: cat.drop_table(name, purge_requested=True)))


def step_lake_tasks():
    """As the operator: the warehouse's task queue (soft deletion, purge) and its deleted tables."""
    jwt = my_token()
    wid = warehouse_id(jwt)
    os.environ["SHOW_ERROR_MESSAGES"] = "true"
    status, kind, body = call("POST", f"/management/v1/warehouse/{wid}/task/list", jwt, {})
    for task in (body or {}).get("tasks", [])[:20]:
        emit(note="task", **{key: task.get(key) for key in ("queue-name", "status", "attempt", "scheduled-for", "entity-name",
                                                             "last-heartbeat-at", "picked-up-at")})
    emit(note="task list", status=status, error=kind, count=len((body or {}).get("tasks", [])))
    status, kind, body = call("GET", f"/management/v1/warehouse/{wid}/deleted-tabulars", jwt)
    emit(note="deleted tabulars", status=status, items=[{k: item.get(k) for k in ("name", "deleted-at", "expiration-date")}
                                                       for item in (body or {}).get("tabulars", [])])


def step_lake_mints(count, workers="50"):
    """T13: many concurrent loads of one table by one principal: answers, and how many distinct credentials came back."""
    from concurrent.futures import ThreadPoolExecutor
    count = int(count)
    jwt = my_token()
    pre = prefix(jwt)

    def one(_):
        status, kind, body = call("GET", f"/catalog/v1/{pre}/namespaces/silver/tables/w109_smoke", jwt, headers=VENDED)
        key = vended_s3(body).get("s3.access-key-id") if status == 200 else None
        return status, kind, key

    started = time.time()
    with ThreadPoolExecutor(max_workers=int(workers)) as pool:
        results = list(pool.map(one, range(count)))
    statuses = {}
    for status, kind, _ in results:
        statuses[str(status if status else kind)] = statuses.get(str(status if status else kind), 0) + 1
    keys = {key for _, _, key in results if key}
    emit(note="mints", loads=count, workers=int(workers), seconds=round(time.time() - started, 1), statuses=statuses, distinct_credentials=len(keys))
    check("every load answered 200 (no 429)", {"200": count}, statuses)


def step_lake_commits(seconds):
    """T14/T16 with commits: appends for a while, each its own batch id in the snapshot summary; then count
    snapshots per batch (exactly one each) and failed appends."""
    deadline = time.time() + float(seconds)
    cat = catalog()
    batches, failures = [], {}
    while time.time() < deadline:
        batch = f"commit-{time.time_ns()}"
        try:
            cat.load_table("silver.w109_smoke").append(rows(batch), snapshot_properties={"w109.batch": batch})
            batches.append(batch)
        except Exception as error:
            key = type(error).__name__
            failures[key] = failures.get(key, 0) + 1
            batches.append(batch)
        time.sleep(0.5)
    table = cat.load_table("silver.w109_smoke")
    per_batch = {}
    for snapshot in table.metadata.snapshots:
        batch = (snapshot.summary.additional_properties if snapshot.summary else {}).get("w109.batch")
        if batch:
            per_batch[batch] = per_batch.get(batch, 0) + 1
    tried = [batch for batch in batches]
    emit(note="commits", tried=len(tried), failures=failures,
         committed=sum(1 for batch in tried if per_batch.get(batch, 0) == 1),
         doubled=sum(1 for batch in tried if per_batch.get(batch, 0) > 1),
         missing=sum(1 for batch in tried if per_batch.get(batch, 0) == 0))
    check("no batch committed twice", 0, sum(1 for batch in tried if per_batch.get(batch, 0) > 1))


def bucket_client():
    """The dev bucket with the token's own S3 keys: the warehouse task only, the dev bucket only."""
    parts = os.environ.get("R2_CREDENTIAL", "").split(":")
    SECRETS.extend(part for part in parts[1:] if len(part) >= 8)
    account, key_id, secret_key, _ = parts
    return s3_client({"s3.endpoint": f"https://{account}.r2.cloudflarestorage.com", "s3.region": "auto",
                      "s3.access-key-id": key_id, "s3.secret-access-key": secret_key}), warehouse()["bucket"]


def step_warehouse_vend_check():
    """T5's last part, in the warehouse task (which alone holds the parent token): what Lakekeeper vends for a table
    holds no part of the parent, and its key differs from the parent's."""
    parts = os.environ.get("R2_CREDENTIAL", "").split(":")
    SECRETS.extend(part for part in parts[1:] if len(part) >= 8)
    _, key_id, secret_key, r2_token = parts
    jwt = my_token()
    status, kind, body = load(jwt, "w109_smoke")
    text = json.dumps(body)
    check("load with vended credentials", 200, status, error=kind)
    # The access key id names a key and is no secret; the secret access key and the token are.
    check("the response holds no parent secret access key", False, secret_key in text)
    check("the response holds no parent token", False, r2_token in text)
    emit(note="the response names the parent's access key id", present=key_id in text,
         vended_key_is_parent_key=vended_s3(body).get("s3.access-key-id") == key_id,
         vended_secret_differs=bool(vended_s3(body).get("s3.secret-access-key")) and vended_s3(body).get("s3.secret-access-key") != secret_key,
         session_token=bool(vended_s3(body).get("s3.session-token")))


def step_warehouse_put(*keys):
    """Plant small probe objects (the T5 canary, T11b's orphans) in the dev bucket."""
    s3, bucket = bucket_client()
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=f"w109 probe {key} {time.time_ns()}".encode())
        emit(note="planted", key=key, etag=s3.head_object(Bucket=bucket, Key=key)["ETag"])


def step_warehouse_hash(*keys):
    """The ETag (content hash) of each key, or that it is gone."""
    s3, bucket = bucket_client()
    for key in keys:
        status = s3_status(lambda key=key: s3.head_object(Bucket=bucket, Key=key))
        etag = s3.head_object(Bucket=bucket, Key=key)["ETag"] if status == 200 else None
        emit(note="hash", key=key, status=status, etag=etag)


def step_warehouse_delete(*keys):
    """Remove probe objects from the dev bucket."""
    s3, bucket = bucket_client()
    for key in keys:
        emit(note="deleted", key=key, status=s3_status(lambda key=key: s3.delete_object(Bucket=bucket, Key=key)))


def step_warehouse_list(*prefixes):
    """In the warehouse task: list prefixes of the dev bucket with the token's own S3 keys (T1's stray objects at
    the root, the bronze canary, T11a's purge). Keys and counts only; the dev bucket only."""
    s3, bucket = bucket_client()
    for wanted in prefixes or ("",):
        listing = s3.list_objects_v2(Bucket=bucket, Prefix=wanted, Delimiter="/", MaxKeys=50)
        emit(note="listing", prefix=wanted, folders=[item["Prefix"] for item in listing.get("CommonPrefixes", [])],
             objects=[item["Key"] for item in listing.get("Contents", [])][:20], count=listing.get("KeyCount"))


STEPS = {
    "health": lambda: step_health(),
    "info": lambda: step_info(),
    "claims": lambda: step_claims(),
    "bootstrap": lambda: step_bootstrap(),
    "whoami": lambda: step_whoami(),
    "identity": lambda: step_identity(),
    "expiry": lambda: step_expiry(),
    "authz-denied": lambda: step_authz(expect_role_create=False),
    "authz-role-creator": lambda: step_authz(expect_role_create=True),
    "config": lambda: step_config(),
    "provision": lambda principal: step_provision(principal),
    "grant": lambda privilege, principal: step_grant(True, privilege, principal),
    "revoke": lambda privilege, principal: step_grant(False, privilege, principal),
    "refused": lambda: step_refused(),
    "warehouse-create": lambda: step_warehouse_create(),
    "warehouse-drop": lambda: step_warehouse_drop(),
    "loop": lambda seconds: step_loop(seconds),
    "lake-setup": lambda: step_lake_setup(),
    "lake-scope": lambda *canary: step_lake_scope(*canary),
    "lake-privileges": lambda: step_lake_privileges(),
    "lake-duckdb": lambda: step_lake_duckdb(),
    "lake-history": lambda commits: step_lake_history(commits),
    "lake-escape": lambda: step_lake_escape(),
    "lake-drop": lambda phase, name="w109_smoke_drop": step_lake_drop(phase, name),
    "lake-tasks": lambda: step_lake_tasks(),
    "lake-cleanup": lambda: step_lake_cleanup(),
    "lake-refresh": lambda: step_lake_refresh(),
    "lake-vended-keys": lambda: step_lake_vended_keys(),
    "lake-register-from": lambda name, location: step_lake_register_from(name, location),
    "lake-maint": lambda phase: step_lake_maint(phase),
    "lake-escape-create": lambda: step_lake_escape_create(),
    "lake-reconcile": lambda phase: step_lake_reconcile(phase),
    "lake-mints": lambda count, workers="50": step_lake_mints(count, workers),
    "lake-commits": lambda seconds: step_lake_commits(seconds),
    "warehouse-list": lambda *prefixes: step_warehouse_list(*prefixes),
    "warehouse-put": lambda *keys: step_warehouse_put(*keys),
    "warehouse-vend-check": lambda: step_warehouse_vend_check(),
    "warehouse-hash": lambda *keys: step_warehouse_hash(*keys),
    "warehouse-delete": lambda *keys: step_warehouse_delete(*keys),
}


def main(argv):
    if not argv or argv[0] not in STEPS:
        print(json.dumps({"usage": sorted(STEPS)}), file=sys.stderr)
        return 2
    STEPS[argv[0]](*argv[1:])
    emit(note="done", step=argv[0], failed=FAILED)
    return 1 if FAILED else 0


sys.exit(main(sys.argv[1:]))
