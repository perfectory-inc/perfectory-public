"""R2 temporary credentials minted from an API token (`POST /accounts/{id}/r2/temp-access-credentials`): the one
copy of the mint code (root ADR-0623 §2-11). The AWS credential vendor (vend.py) mints a lane's credentials with
it; the host's shape-ledger key (scripts/ops/shape-ledger-writer-key.py, root ADR-0476) imports it.

A temporary credential is narrower than its parent token: one object permission, on the prefixes asked, for
the lifetime asked. Its parent access key is the token's own id, read from Cloudflare's verify endpoint (the
user's, or the account's for an account-owned token). An empty prefix list would be the whole bucket, so it is
refused unless the caller says `whole_bucket` (root ADR-0623 §2-3).

Errors name the HTTP status and Cloudflare's error codes, never a value.
"""

import json
import urllib.error
import urllib.request

API = "https://api.cloudflare.com/client/v4"
PERMISSIONS = ("object-read-write", "object-read-only")
# Cloudflare's ceiling for a temporary credential (7 days).
MAX_TTL_SECONDS = 7 * 24 * 3600


class MintError(RuntimeError):
    """Cloudflare refused, or the request was one this module does not make."""


def cloudflare(token, method, path, body=None):
    """(HTTP status, JSON answer) of one Cloudflare API call."""
    request = urllib.request.Request(f"{API}{path}", method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        try:
            return error.code, json.loads(error.read() or b"{}")
        except ValueError:
            return error.code, {}


def codes(answer):
    return [error.get("code") for error in answer.get("errors") or []]


def token_id(token, account, call=cloudflare):
    """The token's id: the parent access key of every credential minted from it."""
    for path in ("/user/tokens/verify", f"/accounts/{account}/tokens/verify"):
        status, answer = call(token, "GET", path)
        if status == 200 and (answer.get("result") or {}).get("id"):
            return answer["result"]["id"]
    raise MintError(f"the minting token does not verify (HTTP {status}, codes {codes(answer)})")


def mint(token, account, bucket, permission, prefixes, ttl_seconds, call=cloudflare, parent=None, whole_bucket=False):
    """Temporary credentials for `bucket`: `permission` on `prefixes`, for `ttl_seconds`.
    Returns {access_key_id, secret_access_key, session_token}."""
    if permission not in PERMISSIONS:
        raise MintError(f"only {', '.join(PERMISSIONS)} are minted, not {permission!r}")
    if not 0 < ttl_seconds <= MAX_TTL_SECONDS:
        raise MintError(f"a temporary credential lives 1 to {MAX_TTL_SECONDS} seconds, not {ttl_seconds}")
    prefixes = list(prefixes or [])
    if not prefixes and not whole_bucket:
        raise MintError(f"no prefix for {permission} on {bucket}: an empty scope would be the whole bucket")
    if any(not isinstance(prefix, str) or not prefix for prefix in prefixes):
        raise MintError(f"an empty prefix for {permission} on {bucket}")
    body = {"bucket": bucket, "parentAccessKeyId": parent or token_id(token, account, call),
            "permission": permission, "ttlSeconds": ttl_seconds}
    if prefixes:
        body["prefixes"] = prefixes
    status, answer = call(token, "POST", f"/accounts/{account}/r2/temp-access-credentials", body)
    result = answer.get("result") or {}
    if status != 200 or not answer.get("success") or not all(
            result.get(field) for field in ("accessKeyId", "secretAccessKey", "sessionToken")):
        raise MintError(f"R2 refused {permission} credentials for {bucket} (HTTP {status}, codes {codes(answer)})")
    return {"access_key_id": result["accessKeyId"], "secret_access_key": result["secretAccessKey"],
            "session_token": result["sessionToken"]}
