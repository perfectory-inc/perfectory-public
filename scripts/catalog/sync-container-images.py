#!/usr/bin/env python3
"""Every pinned container image in the repository comes from one list (root ADR-0118 §5).

tools/technology-versions.contract.json `container_images` maps `repository:tag` to the digest
the repository uses. Compose files, Dockerfiles and scripts still spell the full reference --
Docker cannot read the contract -- but they are copies of it, the way generated documents are:

    --check   every `repository:tag@sha256:...` in a tracked file is in the list with that digest,
              and every entry in the list is used somewhere (exit 1 otherwise); and every OpenTofu
              stack's `required_version` is the tag of the list's OpenTofu image
    --write   rewrite every reference to the digest the list states (after you change a digest), and
              every `required_version` to that tag

The OpenTofu version is the image's because the data host applies its stacks in that image and the
laptop installs that version. 2026-11-25 Renovate (#1620) moved the image to 1.13.10 and left the four
stacks at `required_version = "1.13.9"`, which that image refuses ("Unsupported OpenTofu Core version").

To move to a new tag, change the entry's key and digest in the contract, replace the old
`repository:tag` in the files that use it, then run --write and --check.
"""

import argparse
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = "tools/technology-versions.contract.json"
REFERENCE = re.compile(
    r"(?P<ref>[a-z0-9][a-z0-9._-]*(?:/[a-z0-9._-]+)*:[A-Za-z0-9][A-Za-z0-9._-]*)@sha256:(?P<digest>[0-9a-f]{64})"
)
# Guard self-tests build fixture repositories with invented images and digests on purpose.
FIXTURE_FILES = {
    "scripts/guard/container-runtime-policy-self-test.sh",
    "scripts/guard/technology-version-consistency-self-test.sh",
    "scripts/guard/container-images-match-the-contract-self-test.sh",
}
SKIPPED_SUFFIXES = (".lock", "pnpm-lock.yaml", ".png", ".jpg", ".pmtiles")
OPENTOFU = "ghcr.io/opentofu/opentofu"
# OpenTofu's `terraform { required_version = "<version>" }`: the one attribute, as `tofu fmt` writes it.
REQUIRED_VERSION = re.compile(r'^(?P<lead>\s*required_version\s*=\s*")(?P<version>[^"]*)(?P<tail>")', re.MULTILINE)


def tracked_files(root):
    listing = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True
    ).stdout
    return [item.decode("utf-8") for item in listing.split(b"\0") if item]


def load_images(root):
    contract = json.loads((root / CONTRACT).read_text(encoding="utf-8"))
    images = contract.get("container_images")
    if not isinstance(images, dict) or not images:
        sys.exit(f"{CONTRACT} has no container_images")
    for ref, digest in images.items():
        if not re.fullmatch(r"[^@\s]+:[^@\s]+", ref) or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(digest)):
            sys.exit(f"{CONTRACT}: container_images entry {ref!r} must map repository:tag to sha256:<64 hex>")
    return images


def scan(root):
    """(path, text, [(ref, digest)]) for every tracked file that pins an image."""
    found = []
    for path in tracked_files(root):
        if path in FIXTURE_FILES or path == CONTRACT or path.endswith(SKIPPED_SUFFIXES):
            continue
        try:
            text = (root / path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        refs = [(m.group("ref"), "sha256:" + m.group("digest")) for m in REFERENCE.finditer(text)]
        if refs:
            found.append((path, text, refs))
    return found


def opentofu_version(images):
    """The tag of the list's one OpenTofu image, or None when the list has none."""
    tags = [ref.split(":", 1)[1] for ref in images if ref.split(":", 1)[0] == OPENTOFU]
    if len(tags) > 1:
        sys.exit(f"{CONTRACT} lists {OPENTOFU} more than once: {tags}")
    return tags[0] if tags else None


def stack_versions(root, version, write):
    """(errors, rewritten) for every tracked OpenTofu file whose required_version is not `version`."""
    errors, rewritten = [], 0
    for path in tracked_files(root):
        if not path.endswith(".tf"):
            continue
        text = (root / path).read_text(encoding="utf-8")
        found = [match.group("version") for match in REQUIRED_VERSION.finditer(text)]
        if not found or found == [version] * len(found):
            continue
        if version is None:
            errors.append(f"{path}: requires OpenTofu {found}, but {CONTRACT} lists no {OPENTOFU} image")
        elif write:
            (root / path).write_text(REQUIRED_VERSION.sub(lambda m: m.group("lead") + version + m.group("tail"), text),
                                     encoding="utf-8", newline="")
            rewritten += 1
        else:
            errors.append(f"{path}: required_version {found}, the contract's {OPENTOFU} is {version}")
    return errors, rewritten


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    parser.add_argument("--root", default=str(ROOT))
    args = parser.parse_args()
    root = pathlib.Path(args.root).resolve()
    images = load_images(root)

    errors, used, rewritten = [], set(), 0
    for path, text, refs in scan(root):
        new_text = text
        for ref, digest in refs:
            if ref not in images:
                errors.append(f"{path}: {ref} is not in {CONTRACT} container_images")
                continue
            used.add(ref)
            if digest != images[ref]:
                if args.write:
                    new_text = new_text.replace(f"{ref}@{digest}", f"{ref}@{images[ref]}")
                else:
                    errors.append(f"{path}: {ref} pins {digest[:19]}..., the contract says {images[ref][:19]}...")
        if args.write and new_text != text:
            (root / path).write_text(new_text, encoding="utf-8", newline="")
            rewritten += 1
    for ref in sorted(set(images) - used):
        errors.append(f"{CONTRACT}: {ref} is listed but no tracked file uses it")
    version_errors, version_rewritten = stack_versions(root, opentofu_version(images), args.write)
    errors.extend(version_errors)
    rewritten += version_rewritten

    if errors:
        for error in errors:
            print(f"FAIL container-images: {error}", file=sys.stderr)
        sys.exit(1)
    if args.write:
        print(f"OK container-images: {rewritten} file(s) rewritten to the contract")
    else:
        print(f"OK container-images: {len(used)} image(s), every reference matches the contract")


if __name__ == "__main__":
    main()
