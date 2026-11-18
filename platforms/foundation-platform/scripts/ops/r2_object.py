"""Put one small R2 object with the standard library: AWS Signature Version 4, path style.

For hosts that hold an R2 access key but no AWS tooling: the shape ledger check writes its captures
with the short-lived key its unit mints for it (root ADR-0476), whose session token rides in
x-amz-security-token. One object per call, whole in memory; not for data files.
"""

from __future__ import annotations

import hashlib
import hmac
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

REGION = "auto"  # R2's region for SigV4
SERVICE = "s3"


def _hmac(key: bytes, text: str) -> bytes:
    return hmac.new(key, text.encode("utf-8"), hashlib.sha256).digest()


def signature(secret: str, when: datetime, region: str, canonical_request: str) -> str:
    """The SigV4 signature of `canonical_request` at `when` (UTC)."""
    day, stamp = when.strftime("%Y%m%d"), when.strftime("%Y%m%dT%H%M%SZ")
    scope = f"{day}/{region}/{SERVICE}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", stamp, scope,
                         hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()])
    key = _hmac(_hmac(_hmac(_hmac(f"AWS4{secret}".encode("utf-8"), day), region), SERVICE), "aws4_request")
    return hmac.new(key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


def canonical_request(method: str, path: str, headers: dict[str, str], payload_sha256: str) -> tuple[str, str]:
    """The canonical request and its signed-header list; `headers` are lower-case names."""
    names = sorted(headers)
    lines = "".join(f"{name}:{headers[name].strip()}\n" for name in names)
    signed = ";".join(names)
    return "\n".join([method, path, "", lines, signed, payload_sha256]), signed


def signed_request(method: str, endpoint: str, bucket: str, key: str, access_key: str, secret: str,
                   body: bytes = b"", when: datetime | None = None, region: str = REGION,
                   session_token: str = "", extra: dict[str, str] | None = None) -> urllib.request.Request:
    """A ready request for `bucket/key` at `endpoint` (https://<account>.r2.cloudflarestorage.com).
    `extra` are further headers, lower-case and signed (e.g. if-none-match)."""
    when = when or datetime.now(timezone.utc)
    host = urllib.parse.urlsplit(endpoint).netloc
    path = "/" + urllib.parse.quote(f"{bucket}/{key}", safe="/-_.~")
    payload = hashlib.sha256(body).hexdigest()
    headers = {"host": host, "x-amz-content-sha256": payload, "x-amz-date": when.strftime("%Y%m%dT%H%M%SZ"),
               **(extra or {})}
    if session_token:
        headers["x-amz-security-token"] = session_token
    request, signed = canonical_request(method, path, headers, payload)
    scope = f"{when.strftime('%Y%m%d')}/{region}/{SERVICE}/aws4_request"
    authorization = (f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed}, "
                     f"Signature={signature(secret, when, region, request)}")
    return urllib.request.Request(f"https://{host}{path}", data=body if method == "PUT" else None, method=method,
                                  headers={**{k: v for k, v in headers.items() if k != "host"},
                                           "Authorization": authorization})


def exists(endpoint: str, bucket: str, key: str, access_key: str, secret: str, session_token: str = "") -> bool:
    """Whether `bucket/key` holds an object (HEAD: 200 yes, 404 no; anything else raises)."""
    request = signed_request("HEAD", endpoint, bucket, key, access_key, secret, session_token=session_token)
    try:
        with urllib.request.urlopen(request, timeout=60):
            return True
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise


def put(endpoint: str, bucket: str, key: str, access_key: str, secret: str, body: bytes,
        session_token: str = "", only_new: bool = False) -> bool:
    """Writes the object; False when `only_new` and the key already holds one, which leaves it as it was.

    `only_new` asks first (HEAD) and then writes with If-None-Match: * for a writer that raced it (HTTP
    412). Asking first is not an optimisation: a key under an R2 bucket lock rule answers a conditional
    PUT with 409 Conflict, not 412, when it holds an object. 2026-11-11..13 every shape-ledger run after
    the first published nothing (`HTTP Error 409: Conflict`): its shapes were the ones already kept
    under the lock of shape-ledger/objects/ (infra/shape-ledger/main.tf)."""
    if only_new and exists(endpoint, bucket, key, access_key, secret, session_token):
        return False
    extra = {"if-none-match": "*"} if only_new else None
    request = signed_request("PUT", endpoint, bucket, key, access_key, secret, body,
                             session_token=session_token, extra=extra)
    try:
        with urllib.request.urlopen(request, timeout=60):
            return True
    except urllib.error.HTTPError as error:
        if only_new and error.code == 412:
            return False
        raise

