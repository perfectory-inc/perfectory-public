#!/usr/bin/env bash
# Proves scripts/deploy/foundation-release-build.py builds what the release defines (root ADR-0276 §9):
# admission's images first with the build contract's arguments, then every image a compose file
# builds, once per Dockerfile. The build itself runs only in .github/workflows/foundation-release-build.yml.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
python3 -B - "$root" <<'PY'
import importlib.util, json, pathlib, sys, tempfile

root = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("foundation_release_build", root / "scripts/deploy/foundation-release-build.py")
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)

def check(condition, message):
    if not condition:
        sys.exit(f"FAIL release-build-self-test: {message}")

release = build.admission()
builds = build.image_builds(release)
contract = json.loads((root / "tools/release-build.contract.json").read_text(encoding="utf-8"))["builders"]
names = [name for name, *_ in builds]
check(names[:len(release.RELEASE_IMAGES)] == [name for name, *_ in release.RELEASE_IMAGES], "admission's images do not come first")
args = {name: arguments for name, _, _, arguments in builds}
check(args["publisher"] == {"CARGO_BUILD_JOBS": contract["publisher"]["cargo_build_jobs"]}, "the publisher's job count is not the contract's")
check(args["tippecanoe"] == {"MAKE_JOBS": contract["tippecanoe"]["make_jobs"]}, "tippecanoe's job count is not the contract's")
dockerfiles = [dockerfile for _, dockerfile, _, _ in builds]
check(len(dockerfiles) == len(set(dockerfiles)), "a Dockerfile is built twice")
for _, dockerfile, _ in build.compose_builds():
    check(dockerfile in dockerfiles, f"{dockerfile}, which a compose file builds, is not built")
# The two images the server builds outside admission today (ADR-0276 Context).
for dockerfile in ("services/foundation-api/Dockerfile", "infra/postgres/Dockerfile.recovery"):
    check(dockerfile in dockerfiles, f"{dockerfile} is not built")
for _, dockerfile, context, _ in builds:
    check((build.PLATFORM / dockerfile).is_file() and (build.PLATFORM / context).is_dir(), f"{dockerfile} or {context} is missing")
with tempfile.TemporaryDirectory() as tmp:
    sample = pathlib.Path(tmp) / "Dockerfile"
    sample.write_text(f"FROM --platform=linux/amd64 rust:1@sha256:{'a' * 64} AS build\nFROM scratch\nfrom debian:12@sha256:{'b' * 64}\n", encoding="utf-8")
    check(build.base_images(sample) == [f"debian:12@sha256:{'b' * 64}", f"rust:1@sha256:{'a' * 64}"], "pinned FROM lines are not read")
check(build.toolchain().count(".") == 2, "rust-toolchain.toml's channel is not read")
PY
echo "OK release-build-self-test"
