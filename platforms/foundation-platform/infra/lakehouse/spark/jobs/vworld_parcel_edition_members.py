#!/usr/bin/env python3
"""The members of each collected parcel ZIP, read from R2 by ranged request (root ADR-0148).

Runs inside the GDAL image (`config/tile-bake-containers.contract.json` `images.gdal`): GDAL's
`/vsis3/` reads a ZIP's central directory with a few kilobytes of range requests, so a 13 GB
edition is measured without downloading it. One JSON line per object: its key, its size, the
member names and their dates. `vworld_parcel_editions.py propose` turns them into the contract's
edition entry. This is how the 202609 edition was verified on 2026-10-05.

Reads object keys (one per line) on stdin and the bucket from `B`; the S3 credentials and endpoint
are GDAL's own `AWS_*` variables, set by the caller and never printed.
"""

from __future__ import annotations

import io
import json
import os
import sys
import zipfile


class _RangedFile(io.RawIOBase):
    """A seekable file over GDAL's VSI layer, so `zipfile` reads only what it needs."""

    def __init__(self, gdal, path: str) -> None:
        self._gdal = gdal
        self._handle = gdal.VSIFOpenL(path, "rb")
        if self._handle is None:
            raise OSError(f"cannot open {path}")
        gdal.VSIFSeekL(self._handle, 0, 2)
        self.size = gdal.VSIFTellL(self._handle)
        gdal.VSIFSeekL(self._handle, 0, 0)

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = 0) -> int:
        self._gdal.VSIFSeekL(self._handle, offset, whence)
        return self._gdal.VSIFTellL(self._handle)

    def tell(self) -> int:
        return self._gdal.VSIFTellL(self._handle)

    def readinto(self, buffer) -> int:
        data = self._gdal.VSIFReadL(1, len(buffer), self._handle) or b""
        buffer[: len(data)] = data
        return len(data)

    def close(self) -> None:
        if not self.closed:
            self._gdal.VSIFCloseL(self._handle)
        super().close()


def main() -> int:
    from osgeo import gdal  # noqa: PLC0415 - only the GDAL image has it

    gdal.UseExceptions()
    bucket = os.environ["B"]
    for key in (line.strip() for line in sys.stdin):
        if not key:
            continue
        handle = _RangedFile(gdal, f"/vsis3/{bucket}/{key}")
        try:
            with zipfile.ZipFile(io.BufferedReader(handle, buffer_size=65536)) as archive:
                infos = archive.infolist()
            print(json.dumps({
                "object_key": key, "bytes": handle.size,
                "members": sorted(info.filename for info in infos),
                "member_dates": sorted({"%04d-%02d-%02d" % info.date_time[:3] for info in infos}),
            }), flush=True)
        finally:
            handle.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
