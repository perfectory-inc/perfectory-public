#!/usr/bin/env bash
# Every language reads R2 through one path (root ADR-0261): code outside the places
# tools/r2-read-paths.contract.json names may not build its own S3/R2 client or spell R2
# read-retry settings.
#
# What real incident does failing this prevent? R2 resets idle connections, and the retry that
# survives it (root ADR-0102) was written once per program: the Spark catalog settings in
# September, a by-PNU gate reader on 2026-10-14 (#559), the lakehouse maintenance on 2026-10-15
# (#594), while the seekable reader, the s3a reads and a gongzzang photo reader still had none.
# Each new reader was a new place to forget it. This refuses:
#   - Rust: an aws-sdk-s3 client built (`Client::from_conf`, `Client::new`, `config::Builder`) in a
#     file outside its area's place;
#   - Python: boto3, botocore, s3fs, pyarrow.fs or fsspec anywhere; an `fs.s3a.` key or an S3FileIO
#     retry or HTTP-client key outside the Spark places (tests excluded, they assert those keys);
#   - TypeScript/JavaScript: an S3 client (`@aws-sdk/client-s3`, `new S3Client`, aws4fetch); a
#     Worker reads R2 through its binding.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
command -v python3 >/dev/null 2>&1 || { echo "FAIL r2-reads-have-one-path: python3 is required" >&2; exit 1; }
python3 - "$root" <<'PY'
import json
import re
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
name = "r2-reads-have-one-path"
contract = json.loads((root / "tools/r2-read-paths.contract.json").read_text(encoding="utf-8"))
rust_homes = [home for area, homes in contract["rust"].items() if area.endswith("_reason") is False
              and isinstance(homes, list) for home in homes]
spark_homes = contract["spark"]

files = subprocess.run(["git", "-C", str(root), "ls-files", "-z"], capture_output=True, check=True).stdout
paths = [p for p in files.decode("utf-8").split("\0") if p]

RUST_CLIENT = re.compile(r"(?:aws_sdk_s3::|S3)?Client::(?:from_conf|new)\s*\(|aws_sdk_s3::config::Builder")
PY_FORBIDDEN = re.compile(r"^\s*(?:import|from)\s+(?:boto3|botocore|s3fs|pyarrow\.fs|fsspec)\b")
PY_SETTING = re.compile(r"""["'][^"']*(?:fs\.s3a\.|\.s3\.retry\.|\.http-client\.)""")
TS_CLIENT = re.compile(r"""@aws-sdk/client-s3|new\s+S3Client\s*\(|\baws4fetch\b""")
COMMENT = re.compile(r"^\s*(//|#|\*)")


def inside(path: str, homes: list[str]) -> bool:
    return any(path == home or (home.endswith("/") and path.startswith(home)) for home in homes)


problems = []
for path in paths:
    suffix = Path(path).suffix
    if suffix not in {".rs", ".py", ".ts", ".tsx", ".js", ".mjs"} or "/node_modules/" in path:
        continue
    try:
        text = (root / path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        continue
    lines = text.splitlines()
    if suffix == ".rs":
        if "aws_sdk_s3" not in text or inside(path, rust_homes):
            continue
        for number, line in enumerate(lines, 1):
            if not COMMENT.match(line) and RUST_CLIENT.search(line):
                problems.append(f"{path}:{number}: builds an S3/R2 client outside its area's R2 read path")
    elif suffix == ".py":
        test = "/tests/" in path or Path(path).name.startswith("test_")
        for number, line in enumerate(lines, 1):
            if COMMENT.match(line):
                continue
            if PY_FORBIDDEN.search(line):
                problems.append(f"{path}:{number}: imports an S3 client; Spark reads R2 through the catalog or s3a settings")
            elif not test and not inside(path, spark_homes) and PY_SETTING.search(line):
                problems.append(f"{path}:{number}: spells an R2 read setting outside lakehouse_engine/lakehouse_object_store")
    else:
        for number, line in enumerate(lines, 1):
            if not COMMENT.match(line) and TS_CLIENT.search(line):
                problems.append(f"{path}:{number}: builds an S3 client; a Worker reads R2 through its binding")

if problems:
    print(f"FAIL {name}: {len(problems)} R2 read path(s) outside tools/r2-read-paths.contract.json (root ADR-0261):", file=sys.stderr)
    for problem in problems:
        print(f"  {problem}", file=sys.stderr)
    sys.exit(1)
print(f"OK {name} (rust homes={len(rust_homes)} spark homes={len(spark_homes)})")
PY
