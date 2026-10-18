#!/usr/bin/env python3
"""The release's manifest and bundle as one small OCI image, written as an OCI layout (root ADR-0276 §5).

    release-manifest-image.py <release-dir> <layout-dir>

The layer is a gzip tar of manifest.json and manifest.sigstore.json with fixed times and owners,
so the same two files always make the same digests. scripts/ci/release-push.sh copies the layout's
image `release` to release-<commit>; nothing is built with docker.
"""

import gzip
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path

FILES = ("manifest.json", "manifest.sigstore.json")


def blob(layout: Path, data: bytes) -> str:
    digest = hashlib.sha256(data).hexdigest()
    (layout / "blobs" / "sha256" / digest).write_bytes(data)
    return f"sha256:{digest}"


def main(release: Path, layout: Path) -> int:
    (layout / "blobs" / "sha256").mkdir(parents=True)
    plain = io.BytesIO()
    with tarfile.open(fileobj=plain, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name in FILES:
            data = (release / name).read_bytes()
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime = len(data), 0o444, 0
            tar.addfile(info, io.BytesIO(data))
    plain_bytes = plain.getvalue()
    layer = gzip.compress(plain_bytes, mtime=0)
    config = json.dumps({"architecture": "amd64", "os": "linux", "config": {},
                         "rootfs": {"type": "layers", "diff_ids": [f"sha256:{hashlib.sha256(plain_bytes).hexdigest()}"]}},
                        sort_keys=True).encode()
    manifest = json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": blob(layout, config), "size": len(config)},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": blob(layout, layer), "size": len(layer)}],
    }, sort_keys=True).encode()
    (layout / "oci-layout").write_text('{"imageLayoutVersion": "1.0.0"}\n', encoding="utf-8")
    (layout / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": [{
        "mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": blob(layout, manifest), "size": len(manifest),
        "annotations": {"org.opencontainers.image.ref.name": "release"}}]}) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2])))
