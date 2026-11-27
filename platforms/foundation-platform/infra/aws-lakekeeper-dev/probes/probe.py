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


def call(method, path, jwt=None, body=None, project=None):
    """(status, error type) of one catalog call; the body is never printed."""
    headers = {"Accept": "application/json"}
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
            kind = None
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
    profile = {"type": "s3", "bucket": warehouse["bucket"], "region": warehouse["region"],
               "endpoint": f"https://{account}.r2.cloudflarestorage.com", "storage-layout": warehouse["layout"]}
    if warehouse["key_prefix"]:
        profile["key-prefix"] = warehouse["key_prefix"]
    status, kind, body = call("POST", "/management/v1/warehouse", jwt, {
        "warehouse-name": warehouse["bucket"], "delete-profile": warehouse["delete_profile"], "storage-profile": profile,
        "storage-credential": {"type": "s3", "credential-type": "cloudflare-r2", "account-id": account,
                               "access-key-id": key_id, "secret-access-key": secret_key, "token": r2_token}})
    check("create warehouse", [200, 201], status, error=kind, id=(body or {}).get("warehouse-id") or (body or {}).get("id"))


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
    "loop": lambda seconds: step_loop(seconds),
}


def main(argv):
    if not argv or argv[0] not in STEPS:
        print(json.dumps({"usage": sorted(STEPS)}), file=sys.stderr)
        return 2
    STEPS[argv[0]](*argv[1:])
    emit(note="done", step=argv[0], failed=FAILED)
    return 1 if FAILED else 0


sys.exit(main(sys.argv[1:]))
