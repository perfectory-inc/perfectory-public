#!/usr/bin/env python3
"""Copies the host promotions ledger to the ledger bucket, where CI reads it (root ADR-0490 §5, ADR-0476).

    parcel-promotions-publish.py <ledger>     run by parcel-edition-readiness.sh, after the judgement

The host ledger (`host_promotions_ledger`) is what serves: every reader lays it over the source
contract. A pull request that changes the contract is judged against it only if CI can see it, so each
run puts it where CI reads production's table shapes already, the same way:

- parcel-editions/objects/<SHA-256>.jsonl: the ledger's bytes, named by their hash, written only when
  new (If-None-Match: *) and held by the bucket's lock rule, so a ledger once published stays;
- parcel-editions/history/<instant>.json: the head, when it names an object it did not name before;
- parcel-editions/latest.json: the one mutable head, the object's key and hash, its line count, when
  this host last confirmed it and from which release.

Only the head is written on a run where nothing was promoted or rolled back. The key is the unit's own,
minted by its root ExecStartPre for parcel-editions/ only (shape-ledger-writer-key.py); without one, or
when R2 refuses, it says so and exits 0: the copy never stops the judgement. No ledger file yet is the
empty ledger, published like any other, so CI knows the host holds no promotion of its own.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True
RELEASE = Path(__file__).resolve().parents[2]
PREFIX = "parcel-editions/"
OBJECTS = f"{PREFIX}objects/"
HISTORY = f"{PREFIX}history/"
HEAD = f"{PREFIX}latest.json"
HEAD_SCHEMA = "foundation-platform.parcel_promotions_head.v1"
DECISION = "docs/adr/0490-a-new-cadastral-edition-becomes-the-served-edition-with-no-person-and-a-lagging-served-edition-is-said.md"
WRITER_KEY = "writer.json"  # in the unit's RuntimeDirectory, minted by shape-ledger-writer-key.py


def log(message: str) -> None:
    print(f"parcel-promotions-publish: {message}", file=sys.stderr, flush=True)


def ledger_digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def object_key(digest: str) -> str:
    return f"{OBJECTS}{digest}.jsonl"


def head(body: bytes, now: datetime, release: str) -> dict:
    digest = ledger_digest(body)
    return {"schema_version": HEAD_SCHEMA, "decision": DECISION, "checked_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "release": release, "key": object_key(digest), "sha256": digest,
            "lines": sum(1 for line in body.decode("utf-8").splitlines() if line.strip())}


def writer_key() -> dict | None:
    directory = os.environ.get("RUNTIME_DIRECTORY", "").split(":")[0]
    if not directory:
        return None
    try:
        key = json.loads((Path(directory) / WRITER_KEY).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    # A key minted for another prefix (a control checkout from before this one) cannot write here.
    return key if key.get("prefix") == PREFIX else None


def publish(body: bytes, now: datetime, key: dict, put) -> tuple[dict, bool]:
    """(the head written, whether the ledger object was new). `put(name, body, only_new)`."""

    made = head(body, now, RELEASE.resolve().name)
    written = put(made["key"], body, True)
    text = (json.dumps(made, indent=1, sort_keys=True) + "\n").encode("utf-8")
    if written:
        put(f"{HISTORY}{made['checked_at']}.json", text, True)
    put(HEAD, text, False)
    return made, written


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        log("usage: parcel-promotions-publish.py <ledger>")
        return 64
    ledger = Path(argv[0])
    body = ledger.read_bytes() if ledger.is_file() else b""
    key = writer_key()
    if key is None:
        log("not published: no writer key for parcel-editions/ was minted for this run (the unit's ExecStartPre says why)")
        return 0
    spec = importlib.util.spec_from_file_location("r2_object", RELEASE / "scripts/ops/r2_object.py")
    r2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r2)

    def put(name: str, data: bytes, only_new: bool) -> bool:
        return r2.put(key["endpoint"], key["bucket"], name, key["access_key_id"], key["secret_access_key"], data,
                      session_token=key["session_token"], only_new=only_new)

    try:
        made, written = publish(body, datetime.now(timezone.utc), key, put)
    except OSError as error:
        log(f"not published: {error}")
        return 0
    log(f"published {made['lines']} promotions to {key['bucket']}/{HEAD} ({'a new ledger' if written else 'a ledger it already held'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
