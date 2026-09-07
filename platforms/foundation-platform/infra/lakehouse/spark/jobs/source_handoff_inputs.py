"""Plan exact Silver inputs from the source contract and a completed export summary.

No object-prefix listing: an incomplete export cannot present its existing parts as
a complete national snapshot. The Rust exporter writes the local summary only after
publishing the immutable R2 manifest (ADR-0092).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import re


def require(condition, message):
    if not condition:
        raise ValueError(message)


def count(value, label, minimum=0):
    require(type(value) is int and value >= minimum, f"invalid {label}: {value!r}")
    return value


def plan_inputs(contract, bucket, manifest=None, output_prefix=None):
    require(contract.get("schema_version") == 1, "unsupported source contract schema_version")
    require(re.fullmatch(r"[A-Za-z0-9.-]+", bucket), "invalid bucket")
    picked = [o for o in contract["objects"] if o["vintage"] == contract["selected_vintage"]]
    granularity = contract["load_granularity"]
    expected = contract["granularity_counts"][granularity]
    require(len(picked) == expected and expected > 0, "selected vintage is incomplete or duplicated")
    require(len({o["object_key"] for o in picked}) == expected, "duplicate source object")
    if granularity == "sido":
        require(len({o["region_code"] for o in picked}) == expected, "duplicate province")
    else:
        require(granularity == "national" and expected == 1, "unsupported load granularity")
    prefix = output_prefix or contract["handoff_prefix"]
    suffix = contract["handoff_suffix"]
    if contract.get("handoff_layout") != "manifest_parts":
        return [(f"s3a://{bucket}/{prefix}/{PurePosixPath(o['object_key']).stem}{suffix}", None) for o in picked]

    require(isinstance(manifest, dict), "a completed export manifest/summary is required")
    require(manifest.get("schema_version") == 1 and manifest.get("status") == "complete", "export manifest is not complete")
    require(manifest.get("input_object_key") == picked[0]["object_key"], "manifest source differs from selected object")
    require(manifest.get("vintage") == contract["selected_vintage"], "manifest vintage differs from selected vintage")
    require(manifest.get("output_object_prefix") == prefix, "manifest output prefix differs from contract")
    require(bool(manifest.get("source_snapshot_id")), "manifest source_snapshot_id is missing")
    limit = count(manifest.get("rows_per_part"), "rows_per_part", 1)
    require(limit == contract["rows_per_part"], "manifest rotation differs from contract")
    emitted = count(manifest.get("rows_emitted"), "rows_emitted", 1)
    read = count(manifest.get("rows_read"), "rows_read", 1)
    rejected = count(manifest.get("rejected_rows"), "rejected_rows")
    require(read == emitted + rejected, "manifest row accounting disagrees")
    require(count(manifest.get("pnu_ok"), "pnu_ok") + count(manifest.get("pnu_bad"), "pnu_bad") == emitted, "manifest PNU accounting disagrees")
    parts = manifest.get("parts")
    require(isinstance(parts, list) and len(parts) == (emitted + limit - 1) // limit, "manifest part count disagrees")
    base = f"{prefix}/{PurePosixPath(picked[0]['object_key']).stem}"
    attempt = None
    inputs = []
    for index, part in enumerate(parts, 1):
        key = part["object_key"]
        match = re.fullmatch(re.escape(base) + r"/attempt=([A-Za-z0-9-]+)/part-" + f"{index:04d}" + re.escape(suffix), key)
        require(match is not None, "manifest part path/sequence disagrees")
        current_attempt = match.group(1)
        if attempt is None:
            attempt = current_attempt
        require(attempt == current_attempt, "manifest mixes export attempts")
        rows = count(part.get("rows"), "part rows", 1)
        require(rows == min(limit, emitted - (index - 1) * limit), "manifest part rows disagree")
        count(part.get("bytes"), "part bytes", 1)
        inputs.append((f"s3a://{bucket}/{key}", rows))
    return inputs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output-prefix")
    args = parser.parse_args()
    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    manifest = json.loads(args.manifest.read_text(encoding="utf-8")) if args.manifest else None
    for uri, rows in plan_inputs(contract, args.bucket, manifest, args.output_prefix):
        print(f"{uri}\t{rows if rows is not None else '-'}")


if __name__ == "__main__":
    main()
