"""Put and get one small R2 object with the standard library: AWS Signature Version 4, path style.

For hosts and runners that hold an R2 access key but no AWS tooling: the shape ledger check writes
its captures with it, and the shape ledger refresh workflow reads them (root ADR-0279). One object
per call, whole in memory; not for data files.
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
                   body: bytes = b"", when: datetime | None = None, region: str = REGION) -> urllib.request.Request:
    """A ready request for `bucket/key` at `endpoint` (https://<account>.r2.cloudflarestorage.com)."""
    when = when or datetime.now(timezone.utc)
    host = urllib.parse.urlsplit(endpoint).netloc
    path = "/" + urllib.parse.quote(f"{bucket}/{key}", safe="/-_.~")
    payload = hashlib.sha256(body).hexdigest()
    headers = {"host": host, "x-amz-content-sha256": payload, "x-amz-date": when.strftime("%Y%m%dT%H%M%SZ")}
    request, signed = canonical_request(method, path, headers, payload)
    scope = f"{when.strftime('%Y%m%d')}/{region}/{SERVICE}/aws4_request"
    authorization = (f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed}, "
                     f"Signature={signature(secret, when, region, request)}")
    return urllib.request.Request(f"https://{host}{path}", data=body if method == "PUT" else None, method=method,
                                  headers={**{k: v for k, v in headers.items() if k != "host"},
                                           "Authorization": authorization})


def put(endpoint: str, bucket: str, key: str, access_key: str, secret: str, body: bytes) -> None:
    with urllib.request.urlopen(signed_request("PUT", endpoint, bucket, key, access_key, secret, body), timeout=60):
        pass


def get(endpoint: str, bucket: str, key: str, access_key: str, secret: str) -> bytes | None:
    """The object's bytes, or None when it does not exist."""
    try:
        with urllib.request.urlopen(signed_request("GET", endpoint, bucket, key, access_key, secret), timeout=60) as r:
            return r.read()
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise
