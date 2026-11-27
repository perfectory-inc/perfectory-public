"""What the Lakekeeper probes share (probes.tf, acceptance.tf): the environment, the printed lines, the token, the
catalog call, the warehouse's names, the one load step both environments run (T13) and the step runner.

The task definition hands this file in PROBE_COMMON and runs its step file (dev: probe.py, prod: acceptance.py)
after a two-line loader that makes it the module `common` (local.probe_program): one ECS container definition holds
at most 64 KiB, and dev's steps alone use most of it. The standard library only. Every line it prints is one JSON
object: a check with what was expected, what came back and whether they agree, or a note. It never prints a token,
a secret or a response body that could hold one: only status codes, error types and the claims named in CLAIMS.
The exit code is 0 when every check agreed, 1 when one did not, 2 on a usage error.

Environment (the task definition's): CATALOG_URL, TOKEN_URL, SCOPE, OTHER_SCOPE, PRINCIPAL, PRINCIPALS (a JSON
map of principal name to client id; ids are identifiers, not secrets), WAREHOUSE (the contract's warehouse block),
and CLIENT_ID / CLIENT_SECRET for a principal that has a client (the anonymous probe has none).
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


VENDED = {"X-Iceberg-Access-Delegation": "vended-credentials"}


def step_health():
    status, kind, body = call("GET", "/health")
    check("health", 200, status, error=kind, health=body)


def warehouse():
    return json.loads(os.environ["WAREHOUSE"])


def object_key(rest):
    """The object key of <key prefix>/<rest> in the warehouse's bucket."""
    return f"{root()}/{rest}".split(f"s3://{warehouse()['bucket']}/", 1)[1]


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


def vended_s3(body):
    """The table's vended S3 settings: from storage-credentials (REST spec) or, failing that, config."""
    config = dict(body.get("config") or {})
    for credential in body.get("storage-credentials") or []:
        config.update(credential.get("config") or {})
    for key in ("s3.secret-access-key", "s3.session-token", "s3.access-key-id"):
        if config.get(key):
            SECRETS.append(config[key])
    return config



def step_lake_mints(count, workers="50", table="silver.w109_smoke"):
    """T13: many concurrent loads of one table by one principal: answers, and how many distinct credentials came back.
    dev loads its smoke table; prod's acceptance task the table its lake-migrate made in its own namespace."""
    from concurrent.futures import ThreadPoolExecutor
    count = int(count)
    namespace, name = table.split(".")
    jwt = my_token()
    pre = prefix(jwt)

    def one(_):
        status, kind, body = call("GET", f"/catalog/v1/{pre}/namespaces/{namespace}/tables/{name}", jwt, headers=VENDED)
        key = vended_s3(body).get("s3.access-key-id") if status == 200 else None
        return status, kind, key

    started = time.time()
    with ThreadPoolExecutor(max_workers=int(workers)) as pool:
        results = list(pool.map(one, range(count)))
    statuses = {}
    for status, kind, _ in results:
        statuses[str(status if status else kind)] = statuses.get(str(status if status else kind), 0) + 1
    keys = {key for _, _, key in results if key}
    emit(note="mints", table=table, loads=count, workers=int(workers), seconds=round(time.time() - started, 1),
         statuses=statuses, distinct_credentials=len(keys))
    check("every load answered 200 (no 429)", {"200": count}, statuses)


def run(steps, argv):
    """Runs the step argv names with its arguments: 0 when every check agreed, 1 when one did not, 2 on a usage error."""
    if not argv or argv[0] not in steps:
        print(json.dumps({"usage": sorted(steps)}), file=sys.stderr)
        return 2
    steps[argv[0]](*argv[1:])
    emit(note="done", step=argv[0], failed=FAILED)
    return 1 if FAILED else 0
