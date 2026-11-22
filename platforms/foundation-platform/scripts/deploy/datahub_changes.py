#!/usr/bin/env python3
"""Which DataHub services a release must recreate (foundation-release.sh datahub).

    datahub_changes.py <config-hashes-file> <running-file>

<config-hashes-file> is `docker compose config --hash '*'` of the release's infra/datahub/compose.yml
("<service> <hash>" per line); <running-file> is what the host runs, from every container of the
`datahub` project: "<service> <config-hash label>" per line. Prints the services whose rendered
config differs from their container's, or that have no container, one per line, in compose order.
These are the hashes `docker compose up` itself compares; a deploy where none differs touches
no container.
"""

from __future__ import annotations

import pathlib
import sys


def pairs(text: str) -> list[tuple[str, str]]:
    rows = []
    for line in text.splitlines():
        if line.strip():
            service, digest = line.split()
            rows.append((service, digest))
    return rows


def changed_services(config_hashes: str, running: str) -> list[str]:
    deployed = dict(pairs(running))
    return [service for service, digest in pairs(config_hashes) if deployed.get(service) != digest]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__.splitlines()[2].strip(), file=sys.stderr)
        return 64
    config, running = (pathlib.Path(path).read_text(encoding="utf-8") for path in argv)
    for service in changed_services(config, running):
        print(service)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
