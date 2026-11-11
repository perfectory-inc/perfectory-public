#!/usr/bin/env python3
"""Mints the shape ledger check's own short-lived writer key, before each run (root ADR-0476).

foundation-shape-ledger-check.service runs this as root (ExecStartPre=+) from the control checkout.
It reads the account token from its runtime-secrets group (cloudflare-deploy, root 0600) itself, so
the check's own process never sees the token, and asks R2 for temporary credentials: object read and
write, on the ledger bucket only, under the prefix shape-ledger/ only, for TTL_SECONDS. It writes
them, with the bucket and its endpoint, to writer.json in the unit's RuntimeDirectory, readable by
the check's account alone; systemd removes the directory when the run ends.

    shape-ledger-writer-key.py [<prefix>]

The cadastral edition readiness unit mints its own the same way for parcel-editions/, where it copies
the host promotions ledger (root ADR-0490 §5). A prefix is one of PREFIXES, never another: each unit
gets a key to its own prefix only.

No long-lived writer key exists anywhere: a host that leaks this file leaks 20 minutes of writes
under one prefix of one bucket. It never fails the run: without the token, or when R2 refuses, it
says why and writes nothing, and the check reports "not published" (its 48 hours still stand).
Names only; never a value.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import urllib.error
import urllib.request

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
API = "https://api.cloudflare.com/client/v4"
TOKEN_NAME = "FOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN"
ACCOUNT_NAME = "CLOUDFLARE_ACCOUNT_ID"
PREFIX = "shape-ledger/"
# Each prefix a unit may be given a key to: the shape ledger's, and the promotions ledger's copy.
PREFIXES = (PREFIX, "parcel-editions/")
# Longer than the unit's TimeoutStartSec (15 min) so a slow capture still publishes; no longer.
TTL_SECONDS = 20 * 60
KEY_FILE = "writer.json"
USER = "foundation-platform"  # the unit's User=


def log(message: str) -> None:
    print(f"shape-ledger-writer-key: {message}", flush=True)


def bucket(platform: pathlib.Path = PLATFORM) -> str:
    """The ledger bucket, as the R2 connection contract pins it."""
    contract = json.loads((platform / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
    return contract["connections"]["shape_ledger"]["expected_values"]["FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_BUCKET"]


def account_token(platform: pathlib.Path = PLATFORM, root: pathlib.Path = pathlib.Path("/")) -> tuple[str, str]:
    """(token, account id) from the cloudflare-deploy group's file, or empty strings."""
    contract = json.loads((platform / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
    path = pathlib.Path(next(g["path"] for g in contract["groups"] if g["name"] == "cloudflare-deploy"))
    try:
        text = (root / str(path).lstrip("/")).read_text(encoding="utf-8")
    except FileNotFoundError:
        return "", ""
    values = dict(line.split("=", 1) for line in text.splitlines() if "=" in line and not line.startswith("#"))
    return values.get(TOKEN_NAME, "").strip(), values.get(ACCOUNT_NAME, "").strip()


def cloudflare(token: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(f"{API}{path}", method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def mint(token: str, account: str, name: str, call=cloudflare, prefix: str = PREFIX) -> dict:
    """The temporary credentials of `name`/`prefix`, read and write, TTL_SECONDS; raises with the
    HTTP status (never a value) when Cloudflare refuses."""
    if prefix not in PREFIXES:
        raise RuntimeError(f"no key is minted for the prefix {prefix!r}; only {', '.join(PREFIXES)}")
    status, verified = call(token, "GET", "/user/tokens/verify")
    if status != 200:
        raise RuntimeError(f"the account token does not verify (HTTP {status})")
    # The parent of a temporary credential is an R2 access key: an API token's is its id.
    status, answer = call(token, "POST", f"/accounts/{account}/r2/temp-access-credentials", {
        "bucket": name, "parentAccessKeyId": verified["result"]["id"], "permission": "object-read-write",
        "ttlSeconds": TTL_SECONDS, "prefixes": [prefix]})
    if status != 200 or not answer.get("success"):
        codes = [error.get("code") for error in answer.get("errors") or []]
        raise RuntimeError(f"R2 refused temporary credentials for {name}/{prefix} (HTTP {status}, codes {codes})")
    result = answer["result"]
    return {"endpoint": f"https://{account}.r2.cloudflarestorage.com", "bucket": name, "prefix": prefix,
            "access_key_id": result["accessKeyId"], "secret_access_key": result["secretAccessKey"],
            "session_token": result["sessionToken"], "ttl_seconds": TTL_SECONDS}


def write_key(directory: pathlib.Path, key: dict, user: str | None) -> pathlib.Path:
    """writer.json, 0400, owned by the unit's account."""
    path = directory / KEY_FILE
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o400)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(key, handle)
    if user:
        import pwd  # the host's; not on a Windows laptop

        account = pwd.getpwnam(user)
        os.chown(path, account.pw_uid, account.pw_gid)
    return path


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    prefix = argv[0] if argv else PREFIX
    directory = os.environ.get("RUNTIME_DIRECTORY", "").split(":")[0]
    if not directory:
        log("no RUNTIME_DIRECTORY; run it as a unit's ExecStartPre")
        return 0
    token, account = account_token()
    if not token or not account:
        log(f"no key minted: the cloudflare-deploy group holds no {TOKEN_NAME} and {ACCOUNT_NAME}")
        return 0
    try:
        key = mint(token, account, bucket(), prefix=prefix)
    except (OSError, RuntimeError, KeyError) as error:
        log(f"no key minted: {error}")
        return 0
    path = write_key(pathlib.Path(directory), key, USER)
    log(f"minted a {TTL_SECONDS // 60}-minute key for {key['bucket']}/{prefix} into {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
