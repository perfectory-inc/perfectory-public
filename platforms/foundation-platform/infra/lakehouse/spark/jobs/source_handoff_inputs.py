"""Plan exact Silver inputs from a source contract's measured objects.

No object-prefix listing: a partial state cannot present itself as a complete national
snapshot. The handoff keys follow from the same contract the exporter read.

A contract whose release the Bronze ledger picks (a `silver_refresh` lane, root ADR-0169) has no
measured objects to plan from; its lane loads itself (`scripts/ops/silver-refresh.sh`), and the hub
handoff manifests it publishes are checked there (`remote_lakehouse_job/silver_refresh.rs`).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import re


def require(condition, message):
    if not condition:
        raise ValueError(message)


def plan_inputs(contract, bucket, output_prefix=None):
    require(contract.get("schema_version") == 1, "unsupported source contract schema_version")
    require(re.fullmatch(r"[A-Za-z0-9.-]+", bucket), "invalid bucket")
    require(
        "silver_refresh" not in contract and "objects" in contract,
        "this contract's release comes from the Bronze ledger and its lane loads itself "
        "(root ADR-0169, scripts/ops/silver-refresh.sh); there are no measured objects to plan",
    )
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
    return [(f"s3a://{bucket}/{prefix}/{PurePosixPath(o['object_key']).stem}{suffix}", None) for o in picked]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--output-prefix")
    args = parser.parse_args()
    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    for uri, rows in plan_inputs(contract, args.bucket, args.output_prefix):
        print(f"{uri}\t{rows if rows is not None else '-'}")


if __name__ == "__main__":
    main()
