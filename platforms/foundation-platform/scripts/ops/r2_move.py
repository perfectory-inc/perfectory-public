#!/usr/bin/env python3
"""Move one R2 area of the lakehouse bucket to its root ADR-0626 place, with evidence (root ADR-0626 §6-2).

A plan maps every key under its old prefixes to a new key, or to nothing (the key is derived debris and is
deleted). One move is these commands, each safe to run again:

  manifest  list the old prefixes, write every key with its new key, size, ETag and last-modified as gzip
            JSON Lines under control/evidence/r2_move/ (create-only), with a summary beside it;
  copy      copy every `copy` line server-side and create-only: CopyObject with
            `cf-copy-destination-if-none-match: *`, because R2 ignores the standard If-None-Match on
            CopyObject and overwrites (W108 3e), and `x-amz-copy-source-if-match` on the listed ETag. A key
            already there counts when its size and ETag match;
  rewrite   write the documents a plan rewrites rather than copies (the industrial complex index names each
            profile's key, so its next generation names the new keys);
  verify    HEAD every new key: its size and ETag equal the old object's (a single CopyObject keeps the MD5
            ETag, W108 3c); hash a sample of content-addressed keys against the hash in their name; write the
            verdict beside the manifest;
  expire    add a lifecycle rule per old prefix (Cloudflare deletes asynchronously); `expired` counts what is
            left and removes the rules of prefixes that hold nothing.

Never Iceberg files, never `bronze/`, `__r2_data_catalog/` or `control/`: every plan prefix and every listed
key is checked against that. Credentials are the lakehouse connection's reader and writer pairs and the
Cloudflare account token from the environment, never printed.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import gzip
import hashlib
import json
import os
import pathlib
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Callable, Iterable, Iterator

EVIDENCE = "control/evidence/r2_move"
FORBIDDEN_ROOTS = ("bronze/", "__r2_data_catalog/", "control/")
MAX_SINGLE_COPY = 5 * 1024**3
HEX64 = re.compile(r"[0-9a-f]{64}")


class MoveError(Exception):
    pass


# --- plans: old key -> new key ---------------------------------------------------------------------


def media_key(key: str) -> str:
    """onbid/media -> media/ (root ADR-0626 §6-1): the served store is `display`, a received file no screen
    reads is `received`, a ladder is `rendition/ladder=<n>`."""
    rest = key.removeprefix("onbid/media/")
    if re.fullmatch(r"sha256/[0-9a-f]{64}\.[a-z0-9]+", rest):
        return f"media/display/{rest}"
    if re.fullmatch(r"sources/sha256/[0-9a-f]{64}\.[a-z0-9]+", rest):
        return f"media/received/{rest.removeprefix('sources/')}"
    match = re.fullmatch(r"renditions/r([1-9][0-9]*)/([0-9a-f]{64}(?:/[0-9]{2,4}\.[a-z0-9]+|\.json))", rest)
    if match:
        return f"media/rendition/ladder={match.group(1)}/{match.group(2)}"
    raise MoveError(f"{key}: not a media key this plan knows")


def document_key(key: str) -> str:
    """The court's sale-specification PDFs -> media/document (root ADR-0626 §6-3, public)."""
    match = re.fullmatch(r"courtauction-go-kr/sale-specifications/sha256/([0-9a-f]{64}\.pdf)", key)
    if not match:
        raise MoveError(f"{key}: not a sale-specification key this plan knows")
    return f"media/document/sha256/{match.group(1)}"


COMPLEX_INDEX = "serving/industrial_complex/index/current.json"


def complex_key(key: str) -> str:
    """gold/industrial-complex -> serving/industrial_complex (root ADR-0626 §6-1)."""
    rest = key.removeprefix("gold/industrial-complex/")
    match = re.fullmatch(r"profiles/([0-9a-f-]{36}\.json)", rest)
    if match:
        return f"serving/industrial_complex/profile/{match.group(1)}"
    match = re.fullmatch(r"index-history/([0-9]+)\.json", rest)
    if match:
        return f"serving/industrial_complex/index/{match.group(1)}.json"
    if rest == "index.json":
        return COMPLEX_INDEX
    raise MoveError(f"{key}: not an industrial complex serving key this plan knows")


BY_PNU_GENERATION = re.compile(r"^serving/(?:[a-z0-9-]+/)+by-pnu/v[1-9][0-9]*/")


@dataclasses.dataclass(frozen=True)
class Plan:
    name: str
    prefixes: tuple[str, ...]
    new_key: Callable[[str], str] | None
    # Keys under the prefixes the plan leaves alone (not listed in the manifest): by-PNU manifests.
    keeps: Callable[[str], bool] = lambda _key: False
    # Keys whose bytes are rewritten, not copied.
    rewrites: tuple[str, ...] = ()
    # The new keys are named by the sha256 of their bytes: verify hashes a sample.
    content_addressed: bool = False
    # The prefixes `expire` puts rules on, from the listed keys; default: the plan's own prefixes.
    expire_prefix: Callable[[str], str] | None = None


PLANS = {
    "media": Plan("media", ("onbid/media/",), media_key, content_addressed=True),
    "court_sale_specification": Plan("court_sale_specification", ("courtauction-go-kr/sale-specifications/",),
                                     document_key, content_addressed=True),
    "industrial_complex_serving": Plan("industrial_complex_serving", ("gold/industrial-complex/",), complex_key,
                                       rewrites=("gold/industrial-complex/index.json",)),
    # The per-PNU documents: the gateways answer from packs only (root ADR-0274); each lane's manifest and
    # manifest history stay where the gateways read them until the serving rebuild (root ADR-0626 §6-1).
    "by_pnu_documents": Plan("by_pnu_documents", ("serving/",), None,
                             keeps=lambda key: not BY_PNU_GENERATION.match(key),
                             expire_prefix=lambda key: BY_PNU_GENERATION.match(key).group(0)),
    "silver_handoff": Plan("silver_handoff", ("silver-handoff/",), None),
}


def check_plan(plan: Plan) -> None:
    for prefix in plan.prefixes:
        if not prefix.endswith("/") or prefix.startswith(FORBIDDEN_ROOTS):
            raise MoveError(f"plan {plan.name}: prefix {prefix!r} is not a movable area")


def manifest_line(plan: Plan, item: dict) -> dict | None:
    key = item["Key"]
    if plan.keeps(key):
        return None
    if key.startswith(FORBIDDEN_ROOTS):
        raise MoveError(f"{key}: never moved")
    line = {"key": key, "size": item["Size"], "etag": item["ETag"].strip('"'),
            "last_modified": item["LastModified"].astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")}
    if plan.new_key is None:
        return {**line, "action": "delete"}
    if item["Size"] > MAX_SINGLE_COPY:
        raise MoveError(f"{key}: {item['Size']} bytes is over one CopyObject; this plan copies small objects only")
    action = "rewrite" if key in plan.rewrites else "copy"
    return {**line, "action": action, "new_key": plan.new_key(key)}


# --- R2 ----------------------------------------------------------------------------------------------


def client(role: str):
    import boto3
    from botocore.config import Config

    env = os.environ
    s3 = boto3.client(
        "s3", endpoint_url=env["FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT"], region_name="auto",
        aws_access_key_id=env[f"FOUNDATION_PLATFORM_LAKEHOUSE_{role}_ACCESS_KEY_ID"],
        aws_secret_access_key=env[f"FOUNDATION_PLATFORM_LAKEHOUSE_{role}_SECRET_ACCESS_KEY"],
        config=Config(max_pool_connections=64, retries={"max_attempts": 12, "mode": "standard"},
                      connect_timeout=20, read_timeout=120, tcp_keepalive=True))

    def create_only(request, **_):
        request.headers["cf-copy-destination-if-none-match"] = "*"

    s3.meta.events.register("before-sign.s3.CopyObject", create_only)
    return s3


_CLIENTS: dict[str, object] = {}


def s3(role: str = "READER"):
    if role not in _CLIENTS:
        _CLIENTS[role] = client(role)
    return _CLIENTS[role]


def bucket() -> str:
    name = os.environ["FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET"]
    if name not in ("foundation-platform-lakehouse-prod", "foundation-platform-lakehouse-dev"):
        raise MoveError(f"bucket {name}: not the lakehouse bucket")
    return name


ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"
BOUNDARIES = [a + b for a in ALPHABET for b in ALPHABET]


def shards(name: str, prefix: str) -> list[tuple]:
    """Ranges to list in parallel. Folders are walked down to the leaf folders; a wide folder (a by-PNU
    generation, a hash folder, a rendition ladder) is cut into contiguous key ranges at two-character
    boundaries: shard i holds the keys k with b_i < k <= b_(i+1), so every key is in exactly one shard
    whatever its first characters are."""
    out, pending = [], [prefix]
    while pending:
        current = pending.pop()
        if re.search(r"/(?:v|r)[1-9][0-9]*/$|/sha256/$|/profiles/$", current) and s3().list_objects_v2(
                Bucket=name, Prefix=current, MaxKeys=1000).get("IsTruncated"):
            edges = [None, *(current + b for b in BOUNDARIES), None]
            out += [("range", current, lo, hi) for lo, hi in zip(edges, edges[1:])]
            continue
        folders, has_keys = [], False
        for page in s3().get_paginator("list_objects_v2").paginate(Bucket=name, Prefix=current, Delimiter="/"):
            folders += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
            has_keys = has_keys or bool(page.get("Contents"))
        if has_keys:
            out.append(("only", current, None, None))
        pending += folders
    return out


def list_shard(name: str, shard: tuple) -> Iterator[dict]:
    """The keys of one shard, page by page; a dropped connection retries the page from the last key."""
    kind, prefix, lo, hi = shard
    after = lo
    while True:
        kwargs = {"Bucket": name, "Prefix": prefix, "MaxKeys": 1000}
        if kind == "only":
            kwargs["Delimiter"] = "/"
        if after is not None:
            kwargs["StartAfter"] = after
        page = retrying(s3().list_objects_v2, **kwargs)
        for item in page.get("Contents", []):
            if hi is not None and item["Key"] > hi:
                return
            yield item
            after = item["Key"]
        if not page.get("IsTruncated") or not page.get("Contents"):
            return


def retrying(call: Callable, *args, **kwargs):
    """One R2 call, again after a dropped connection: botocore does not retry an SSL EOF (measured from the
    laptop at 48 threads, 2026-11-28), and every call here is safe to repeat (create-only or read)."""
    import botocore.exceptions

    for attempt in range(10):
        try:
            return call(*args, **kwargs)
        except (botocore.exceptions.SSLError, botocore.exceptions.ConnectionError,
                botocore.exceptions.ReadTimeoutError, botocore.exceptions.ConnectionClosedError):
            if attempt == 9:
                raise
            time.sleep(min(60, 2 ** attempt))


def put_create_only(name: str, key: str, body: bytes, **extra) -> None:
    import botocore.exceptions

    try:
        s3("WRITER").put_object(Bucket=name, Key=key, Body=body, IfNoneMatch="*", **extra)
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] in ("PreconditionFailed", "412"):
            raise MoveError(f"{key}: already there; evidence and moved objects are create-only") from error
        raise


def bounded(work: Iterable, run: Callable, threads: int) -> None:
    """Runs `run` over `work` on `threads` threads with at most 4 x threads items in flight."""
    with concurrent.futures.ThreadPoolExecutor(threads) as pool:
        flight = set()
        for item in work:
            flight.add(pool.submit(run, item))
            if len(flight) >= threads * 4:
                done, flight = concurrent.futures.wait(flight, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    future.result()
        for future in concurrent.futures.as_completed(flight):
            future.result()


# --- commands ----------------------------------------------------------------------------------------


def evidence_key(plan: Plan, stamp: str, suffix: str) -> str:
    return f"{EVIDENCE}/{stamp}-{plan.name}{suffix}"


def summary_path(manifest: pathlib.Path) -> pathlib.Path:
    return manifest.with_name(manifest.name.replace(".jsonl.gz", "") + ".summary.json")


def cmd_manifest(plan: Plan, out: pathlib.Path, stamp: str, upload: bool, threads: int,
                 since: dt.datetime | None = None) -> dict:
    """`since`: only keys written after it (the delta a writer added after an earlier manifest)."""
    check_plan(plan)
    name = bucket()
    work = [shard for prefix in plan.prefixes for shard in shards(name, prefix)]
    lock = threading.Lock()
    totals = {"objects": 0, "bytes": 0, "copy": 0, "rewrite": 0, "delete": 0, "newest": "", "expire": set()}
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8", newline="\n", compresslevel=6) as handle:
        def run(shard):
            batch = []
            for item in list_shard(name, shard):
                if since is not None and item["LastModified"] <= since:
                    continue
                line = manifest_line(plan, item)
                if line is None:
                    continue
                batch.append(line)
                if len(batch) >= 5000:
                    flush(batch)
                    batch = []
            flush(batch)

        def flush(batch):
            if not batch:
                return
            text = "".join(json.dumps(line, sort_keys=True, separators=(",", ":")) + "\n" for line in batch)
            with lock:
                handle.write(text)
                for line in batch:
                    totals["objects"] += 1
                    totals["bytes"] += line["size"]
                    totals[line["action"]] += 1
                    totals["newest"] = max(totals["newest"], line["last_modified"])
                    if plan.expire_prefix:
                        totals["expire"].add(plan.expire_prefix(line["key"]))

        bounded(work, run, threads)
    summary = {"plan": plan.name, "stamp": stamp, "objects": totals["objects"], "bytes": totals["bytes"],
               "by_action": {a: totals[a] for a in ("copy", "rewrite", "delete")},
               "newest_last_modified": totals["newest"] or None,
               "old_prefixes": sorted(totals["expire"]) if plan.expire_prefix else list(plan.prefixes),
               "manifest": evidence_key(plan, stamp, ".jsonl.gz"),
               "since": since.isoformat().replace("+00:00", "Z") if since else None,
               "listed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")}
    summary_path(out).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    if upload:
        put_create_only(name, summary["manifest"], out.read_bytes(), ContentType="application/gzip")
        put_create_only(name, evidence_key(plan, stamp, ".summary.json"), summary_path(out).read_bytes(),
                        ContentType="application/json")
    return summary


def read_manifest(path: pathlib.Path) -> Iterator[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            yield json.loads(line)


def same_object(head: dict, line: dict) -> bool:
    return head["ContentLength"] == line["size"] and head["ETag"].strip('"') == line["etag"]


def cmd_copy(lines: Iterable[dict], threads: int) -> dict:
    import botocore.exceptions

    name, writer = bucket(), s3("WRITER")
    counts = {"copied": 0, "already": 0}
    lock = threading.Lock()

    def one(line):
        try:
            retrying(writer.copy_object, Bucket=name, Key=line["new_key"],
                     CopySource={"Bucket": name, "Key": line["key"]}, MetadataDirective="COPY",
                     CopySourceIfMatch=f'"{line["etag"]}"')
            outcome = "copied"
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
                raise
            head = retrying(writer.head_object, Bucket=name, Key=line["new_key"])
            if not same_object(head, line):
                raise MoveError(f"{line['new_key']}: exists with other bytes than {line['key']}") from error
            outcome = "already"
        with lock:
            counts[outcome] += 1
            done = counts["copied"] + counts["already"]
            if done % 50000 == 0:
                print(f"copy: {done} ({counts})", file=sys.stderr, flush=True)

    bounded((line for line in lines if line["action"] == "copy"), one, threads)
    return counts


def rewrite_complex_index(document: dict) -> dict:
    """The next generation of the industrial complex index with every profile key under the new root (root
    ADR-0626 §6-4): the same complexes and hashes, so the exporter's next run keeps each profile."""
    out = json.loads(json.dumps(document))
    out["generation"] = document["generation"] + 1
    for entry in out["complexes"]:
        entry["profile_object_key"] = complex_key(entry["profile_object_key"])
    return out


def cmd_rewrite(plan: Plan, lines: Iterable[dict]) -> dict:
    name, writer = bucket(), s3("WRITER")
    done = {}
    for line in (line for line in lines if line["action"] == "rewrite"):
        if plan.name != "industrial_complex_serving":
            raise MoveError(f"plan {plan.name} rewrites nothing")
        old = writer.get_object(Bucket=name, Key=line["key"], IfMatch=f'"{line["etag"]}"')
        document = rewrite_complex_index(json.loads(old["Body"].read()))
        body = (json.dumps(document, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        history = f"serving/industrial_complex/index/{document['generation']}.json"
        put_create_only(name, history, body, ContentType=old["ContentType"])
        put_create_only(name, line["new_key"], body, ContentType=old["ContentType"],
                        CacheControl=old.get("CacheControl") or "no-cache, max-age=0")
        done[line["new_key"]] = {"generation": document["generation"], "history": history,
                                 "complexes": len(document["complexes"])}
    return done


def cmd_verify(plan: Plan, lines: Iterable[dict], threads: int, sample: int) -> dict:
    name, reader = bucket(), s3()
    bad, lock = [], threading.Lock()
    checked = [0]
    rng, reservoir = random.Random(626), []

    def one(line):
        head = retrying(reader.head_object, Bucket=name, Key=line["new_key"])
        with lock:
            checked[0] += 1
            if checked[0] % 100000 == 0:
                print(f"verify: {checked[0]} checked, {len(bad)} differ", file=sys.stderr, flush=True)
            if not same_object(head, line):
                bad.append(line["new_key"])

    def copies():
        seen = 0
        for line in lines:
            if line["action"] != "copy":
                continue
            seen += 1
            if plan.content_addressed and "/sha256/" in line["new_key"]:
                if len(reservoir) < sample:
                    reservoir.append(line)
                elif rng.randrange(seen) < sample:
                    reservoir[rng.randrange(sample)] = line
            yield line

    bounded(copies(), one, threads)
    hashed = []
    for line in reservoir:
        digest = hashlib.sha256(retrying(reader.get_object, Bucket=name, Key=line["new_key"])["Body"].read()).hexdigest()
        hashed.append(digest == HEX64.search(line["new_key"]).group(0))
    return {"copies_checked": checked[0], "size_or_etag_differ_count": len(bad), "size_or_etag_differ": bad[:20],
            "sha256_sampled": len(hashed), "sha256_mismatch": hashed.count(False), "ok": not bad and all(hashed)}


def cloudflare(method: str, path: str, body: dict | None = None) -> dict:
    env = os.environ
    request = urllib.request.Request(
        f"https://api.cloudflare.com/client/v4/accounts/{env['CLOUDFLARE_ACCOUNT_ID']}{path}", method=method,
        data=None if body is None else json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {env['CLOUDFLARE_API_TOKEN']}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        raise MoveError(f"Cloudflare {method} {path}: HTTP {error.code} {error.read()[:300]!r}") from error


def rule_id(plan: Plan, prefix: str) -> str:
    return f"adr-0626 {plan.name} {prefix}"


def cmd_expire(plan: Plan, prefixes: list[str]) -> dict:
    """Adds one delete rule per old prefix beside the bucket's own rules (the PUT replaces the list, so the
    rules already there are sent back unchanged)."""
    for prefix in prefixes:
        if prefix.startswith(FORBIDDEN_ROOTS) or not prefix.endswith("/") or not any(
                prefix.startswith(own) for own in plan.prefixes):
            raise MoveError(f"{prefix}: never expired by plan {plan.name}")
    path = f"/r2/buckets/{bucket()}/lifecycle"
    rules = cloudflare("GET", path)["result"]["rules"]
    ours = {rule["id"] for rule in rules}
    added = [prefix for prefix in prefixes if rule_id(plan, prefix) not in ours]
    rules += [{"id": rule_id(plan, prefix), "enabled": True, "conditions": {"prefix": prefix},
               "deleteObjectsTransition": {"condition": {"type": "Age", "maxAge": 86400}}} for prefix in added]
    cloudflare("PUT", path, {"rules": rules})
    return {"rules_added": added, "rules_now": len(rules)}


def cmd_expired(plan: Plan, prefixes: list[str], remove: bool) -> dict:
    name = bucket()
    left = {prefix: s3().list_objects_v2(Bucket=name, Prefix=prefix, MaxKeys=1).get("KeyCount", 0)
            for prefix in prefixes}
    removed = []
    if remove:
        path = f"/r2/buckets/{name}/lifecycle"
        rules = cloudflare("GET", path)["result"]["rules"]
        empty = {rule_id(plan, prefix) for prefix, count in left.items() if count == 0}
        removed = sorted(rule["id"] for rule in rules if rule["id"] in empty)
        if removed:
            cloudflare("PUT", path, {"rules": [rule for rule in rules if rule["id"] not in empty]})
    return {"prefixes_with_objects_left": sorted(p for p, c in left.items() if c), "rules_removed": removed}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("manifest", "copy", "rewrite", "verify", "expire", "expired"))
    parser.add_argument("plan", choices=sorted(PLANS))
    parser.add_argument("--manifest", type=pathlib.Path, required=True, help="the local gzip JSON Lines manifest")
    parser.add_argument("--stamp", default=dt.date.today().isoformat(), help="the evidence name's date")
    parser.add_argument("--no-upload", action="store_true", help="manifest: keep it local only (a dry run)")
    parser.add_argument("--threads", type=int, default=64)
    parser.add_argument("--sample", type=int, default=200)
    parser.add_argument("--remove-empty-rules", action="store_true")
    parser.add_argument("--since", type=dt.datetime.fromisoformat,
                        help="manifest: only keys last modified after this UTC instant (a delta)")
    args = parser.parse_args(argv)
    plan = PLANS[args.plan]
    if args.command == "manifest":
        result = cmd_manifest(plan, args.manifest, args.stamp, not args.no_upload, args.threads, args.since)
    elif args.command == "copy":
        result = cmd_copy(read_manifest(args.manifest), args.threads)
    elif args.command == "rewrite":
        result = cmd_rewrite(plan, read_manifest(args.manifest))
    elif args.command == "verify":
        result = cmd_verify(plan, read_manifest(args.manifest), args.threads, args.sample)
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        put_create_only(bucket(), evidence_key(plan, args.stamp, f".verify-{stamp}.json"),
                        (json.dumps(result, indent=2) + "\n").encode("utf-8"), ContentType="application/json")
    else:
        prefixes = json.loads(summary_path(args.manifest).read_text(encoding="utf-8"))["old_prefixes"]
        result = (cmd_expire(plan, prefixes) if args.command == "expire"
                  else cmd_expired(plan, prefixes, args.remove_empty_rules))
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
