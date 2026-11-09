#!/usr/bin/env python3
"""Measure where Korean visitors enter, and how fast, for the same reads through Cloudflare and through
the Seoul front door (root ADR-0461).

    korea-edge-measure.py --sample SAMPLE.json [--cloudfront DOOR=dXXXX.cloudfront.net ...]
                          [--reads 5] [--globalping 12] [--json OUT.jsonl]

SAMPLE.json names the reads, never committed (a by-PNU path carries a real PNU):
    {"tiles": ["/<source id>/<z>/<x>/<y>", ...], "catalog": ["/parcels/by-pnu/<pnu>", ...]}
W43 (2026-11-09) measured a parcels z14 tile of 683,030 B, a zoning z11 tile of 1,138,669 B and one
by-PNU document of 883 B gzip; a re-measurement uses those same three.

Every read of every door runs through two routes:
- cloudflare: the Worker straight from Cloudflare, on the door's origin hostname
  (config/cdn-front-door.contract.json) or, while the door still answers on Cloudflare, its public one;
- cloudfront: the public hostname through CloudFront. Before the switch the public name still resolves
  to Cloudflare, so curl connects to the distribution by name (--connect-to, TLS for the public name:
  the distribution holds its certificate); the distribution's name comes from --cloudfront, which the
  edge apply logs (journalctl -u foundation-edge-settings.service: "front door <door>: <name>"). After
  the switch the public name is the distribution and needs no --cloudfront.

Each answer gives its entry point (Cloudflare: the cf-ray suffix; CloudFront: x-amz-cf-pop), whether
the edge held it, its encoding, the connect time, the time to first byte, the total and the
throughput, from this machine (the laptop is KT, AS4766). --globalping adds that many Korean probes
of Globalping's (no account) for the first read of each door and route: their network, entry point
and first byte, so SK Broadband and LG U+ are seen too. Read-only, standard library and curl only,
no credentials.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time
import urllib.request

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "config/cdn-front-door.contract.json"
R2 = PLATFORM / "config/r2-connections.contract.json"
GLOBALPING = "https://api.globalping.io/v1/measurements"
# curl's own timings; the headers come from --dump-header.
WRITE_OUT = ('{"status":%{http_code},"connect":%{time_connect},"tls":%{time_appconnect},'
             '"ttfb":%{time_starttransfer},"total":%{time_total},"bytes":%{size_download},'
             '"bytes_per_second":%{speed_download},"http":"%{http_version}"}')


def doors(contract: dict, r2: dict) -> dict[str, dict[str, str]]:
    """Each door with a gateway (the ones the sample reads): its public and its Cloudflare hostname."""

    found = {}
    for name, door in contract["doors"].items():
        if "gateway" not in door:
            continue
        public = r2[door["gateway"]]["public_hostname"]
        found[name] = {"public": public,
                       "cloudflare": public if door["answered_by"] == "cloudflare" else door["origin_hostname"],
                       "answered_by": door["answered_by"]}
    return found


def entry_point(headers: dict[str, str]) -> str:
    """Where the answer entered: CloudFront's PoP (ICN80-P1), Cloudflare's colo (the ray's suffix)."""

    if "x-amz-cf-pop" in headers:
        return headers["x-amz-cf-pop"]
    ray = headers.get("cf-ray", "")
    return ray.rsplit("-", 1)[1] if "-" in ray else "?"


def edge_hit(headers: dict[str, str]) -> str:
    if "x-cache" in headers:
        return headers["x-cache"].split(" ", 1)[0]
    return headers.get("cf-cache-status", "?")


def parse_headers(text: str) -> dict[str, str]:
    """The last response's headers (curl writes every response it followed), lower-cased."""

    blocks = [block for block in text.replace("\r\n", "\n").split("\n\n") if block.strip()]
    headers = {}
    for line in (blocks[-1] if blocks else "").splitlines()[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
    return headers


def curl(url: str, connect_to: str | None) -> dict:
    # Asked for as a browser does and not decoded: the bytes and the throughput are what crossed the wire.
    command = ["curl", "-sS", "-o", "-", "-D", "-", "-w", "\n" + WRITE_OUT,
               "-H", "Accept-Encoding: br, gzip", "--max-time", "60", url]
    if connect_to:
        command[1:1] = ["--connect-to", connect_to]
    result = subprocess.run(command, capture_output=True)
    if result.returncode != 0:
        return {"status": 0, "error": result.stderr.decode(errors="replace").strip()[-300:]}
    output = result.stdout
    timings = json.loads(output[output.rindex(b"\n") + 1:])
    head = output.split(b"\r\n\r\n")[0].decode("latin-1") if b"\r\n\r\n" in output else ""
    headers = parse_headers(head)
    return {**timings, "entry": entry_point(headers), "edge": edge_hit(headers),
            "encoding": headers.get("content-encoding", "none")}


def globalping(host: str, path: str, probes: int, connect_host: str | None) -> list[dict]:
    """One HTTP GET from `probes` Korean probes; connect_host, when given, is where they connect (the
    distribution), with the public name as the Host header."""

    body = {"type": "http", "target": connect_host or host, "limit": probes,
            "locations": [{"country": "KR"}],
            "measurementOptions": {"protocol": "HTTPS", "request": {"path": path, "method": "GET",
                                                                   **({"host": host} if connect_host else {})}}}
    request = urllib.request.Request(GLOBALPING, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        measurement = json.load(response)["id"]
    for _ in range(60):
        time.sleep(2)
        with urllib.request.urlopen(f"{GLOBALPING}/{measurement}", timeout=30) as response:
            result = json.load(response)
        if result["status"] != "in-progress":
            break
    rows = []
    for probe in result.get("results", []):
        answer = probe["result"]
        headers = {name.lower(): value if isinstance(value, str) else ", ".join(value)
                   for name, value in (answer.get("headers") or {}).items()}
        rows.append({"network": probe["probe"].get("network"), "asn": probe["probe"].get("asn"),
                     "status": answer.get("statusCode"), "entry": entry_point(headers),
                     "ttfb_ms": (answer.get("timings") or {}).get("firstByte")})
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sample", type=pathlib.Path, required=True)
    parser.add_argument("--cloudfront", action="append", default=[], metavar="DOOR=NAME")
    parser.add_argument("--reads", type=int, default=5)
    parser.add_argument("--globalping", type=int, default=0, metavar="PROBES")
    parser.add_argument("--json", type=pathlib.Path)
    args = parser.parse_args(argv)
    known = doors(json.loads(CONTRACT.read_text(encoding="utf-8")), json.loads(R2.read_text(encoding="utf-8")))
    distributions = dict(item.split("=", 1) for item in args.cloudfront)
    sample = json.loads(args.sample.read_text(encoding="utf-8"))
    rows = []
    print("| door | route | path | entry | edge | enc | status | connect s | TTFB s | total s | bytes | MB/s |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for name, paths in sample.items():
        door = known[name]
        routes = [("cloudflare", door["cloudflare"], None)]
        if door["answered_by"] == "cloudfront":
            routes.append(("cloudfront", door["public"], None))
        elif name in distributions:
            routes.append(("cloudfront", door["public"], f"{door['public']}:443:{distributions[name]}:443"))
        else:
            print(f"<!-- {name}: no --cloudfront {name}=<distribution>, so only Cloudflare is measured -->")
        for route, host, connect_to in routes:
            for path in paths:
                for _ in range(args.reads):
                    row = {"door": name, "route": route, "path": path, **curl(f"https://{host}{path}", connect_to)}
                    rows.append(row)
                    print(f"| {name} | {route} | {path[:24]}... | {row.get('entry', '-')} | {row.get('edge', '-')} | "
                          f"{row.get('encoding', '-')} | {row['status']} | {row.get('connect', 0):.3f} | "
                          f"{row.get('ttfb', 0):.3f} | {row.get('total', 0):.3f} | {row.get('bytes', 0):,} | "
                          f"{row.get('bytes_per_second', 0) / 1e6:.2f} |")
                if args.globalping:
                    connect_host = distributions.get(name) if connect_to else None
                    for probe in globalping(host, path, args.globalping, connect_host):
                        rows.append({"door": name, "route": route, "path": path, "probe": probe})
                        print(f"<!-- globalping {name} {route} {probe['network']} AS{probe['asn']}: "
                              f"{probe['status']} {probe['entry']} TTFB {probe['ttfb_ms']} ms -->")
    if args.json:
        args.json.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return 0 if rows and all(row.get("status", row.get("probe", {}).get("status")) == 200 for row in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
