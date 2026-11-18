#!/usr/bin/env python3
"""Read the live by-PNU edges the way a user does (root ADR-0327): black-box, GET, twice.

    by-pnu-edge-probe.py                         every by-PNU Worker config/worker-deploys.contract.json
                                                 lists, on its production hostname (the quarter-hourly
                                                 timer, foundation-by-pnu-edge-probe.timer)
    by-pnu-edge-probe.py --base-url URL LANE     the Worker of LANE on one hostname (the Worker
                                                 deploy's smoke, scripts/deploy/worker_autodeploy.py)
    by-pnu-edge-probe.py --screen                the weekly synthetic screen (root ADR-0480,
                                                 foundation-by-pnu-screen-probe.timer): for real PNUs,
                                                 every section the screen shows, against each lane's
                                                 freshness budget (`screen` below)

For each lane the Worker serves (its own and the kind lanes it hosts, root ADR-0193 §4) it reads the
lane's gate sample (the file FOUNDATION_PLATFORM_<LANE>_BY_PNU_SERVING_MONITOR_SAMPLE_PATH names,
written on the host at the lane's cut-over: no PNU enters the repository), takes the first entry the
edge does not answer 404 (within contract by_pnu_section_packs.edge_probe.scan_documents) and GETs its
document `reads` more times. Every one of those answers must be 200, of the lane's content type, a
non-empty JSON object. On the Worker that serves parcel_media it then takes the first blob the media
documents lane's sample documents name and GETs it `reads` times: 200, the content type of its
extension, bytes whose sha256 is the one in its name.

2026-10-29: every onbid photo GET after the first answered 500, while a HEAD check passed. The first
read of a version may already come from the edge copy, which every version shares; the second always
does.

A lane whose sample variable is not set is not cut over and is skipped, said so; a run that probed no
lane fails. Read-only, standard library only, no credentials. Exits 1 on any failure: the unit's
OnFailure reports it to Slack, and a Worker deploy rolls the version back.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import mimetypes
import os
import pathlib
import re
import sys
import urllib.error
import urllib.request
from typing import Any, Callable, Iterator

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "config/r2-connections.contract.json"
DEPLOYS = PLATFORM / "config/worker-deploys.contract.json"
ONBID = PLATFORM / "infra/lakehouse/contracts/data-go-kr-onbid.contract.json"
# The PNU's digits; a longer key carries the lane's key suffix (root ADR-0204, request_path_of).
PNU_DIGITS = 19
SHA256 = re.compile(r"[0-9a-f]{64}")
# The standard table only: the module-level functions would read the Windows registry.
MEDIA_TYPES = mimetypes.MimeTypes()

Answer = tuple[int, dict[str, str], bytes]


def load(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def lane_block(contract: dict[str, Any], lane: str) -> dict[str, Any]:
    """The gateway block of a lane with a Worker of its own (parcel, building, a whole by_pnu_lanes lane)."""

    if lane in ("parcel", "building"):
        return contract[f"{lane}_by_pnu_gateway"]
    return contract["by_pnu_lanes"][lane]


def with_kind(path: str, segment: str, kind: str) -> str:
    """`path` with `<kind>/` before its last `segment` segment (the Worker's withKind, lane.ts)."""

    parts = path.split("/")
    at = len(parts) - 1 - parts[::-1].index(segment)
    return "/".join([*parts[:at], kind, *parts[at:]])


def served_lanes(contract: dict[str, Any], lane: str) -> list[tuple[str, str, str]]:
    """(lane, request prefix, content type) of every lane the Worker of `lane` serves, its own first."""

    block = lane_block(contract, lane)
    lanes = [(lane, block["request_path"]["prefix"], block["content_type"])]
    for name, entry in contract.get("by_pnu_lanes", {}).items():
        if entry.get("host") == f"{lane}_by_pnu_gateway":
            lanes.append((name, with_kind(block["request_path"]["prefix"], "by-pnu", entry["kind"]),
                          block["content_type"]))
    return lanes


def request_path_of(key: str) -> str:
    """The publisher's request_path_of: `<pnu>/<suffix>` for a key longer than a PNU."""

    return f"{key[:PNU_DIGITS]}/{key[PNU_DIGITS:]}" if len(key) > PNU_DIGITS else key


def sample_variable(lane: str) -> str:
    return f"FOUNDATION_PLATFORM_{lane.upper().replace('-', '_')}_BY_PNU_SERVING_MONITOR_SAMPLE_PATH"


def media_keys(document: Any, prefix: str) -> Iterator[str]:
    """The content-addressed keys a served document names (rounds[].photos[], root ADR-0296 §5):
    every object in it with a sha256 and a media_format."""

    if isinstance(document, dict):
        sha, extension = document.get("sha256"), document.get("media_format")
        if isinstance(sha, str) and SHA256.fullmatch(sha) and isinstance(extension, str):
            yield f"{prefix}/sha256/{sha}.{extension}"
        for value in document.values():
            yield from media_keys(value, prefix)
    elif isinstance(document, list):
        for value in document:
            yield from media_keys(value, prefix)


def json_document(answer: Answer) -> tuple[Any, str | None]:
    """The answer's JSON object, or why it is not one."""

    _, headers, body = answer
    if not body:
        return None, "an empty body"
    try:
        value = json.loads(body)
    except (OSError, ValueError) as error:
        return None, f"not a JSON document ({error})"
    if not isinstance(value, dict) or not value:
        return None, "not a non-empty JSON object"
    return value, None


def decoded(answer: Answer) -> Answer:
    """The answer with its gzip content coding removed, or a 0 answer saying it does not decode."""

    status, headers, body = answer
    if headers.get("content-encoding") != "gzip":
        return answer
    try:
        return status, headers, gzip.decompress(body)
    except (OSError, EOFError) as error:
        return 0, {}, f"a gzip-encoded body that does not decode ({error})".encode()


class Probe:
    def __init__(self, contract: dict[str, Any], base_url: str):
        self.contract = contract
        self.policy = contract["by_pnu_section_packs"]["edge_probe"]
        self.base_url = base_url.rstrip("/")
        self.failures: list[str] = []
        self.probed: list[str] = []

    def log(self, message: str) -> None:
        print(f"by-pnu-edge-probe {self.base_url}: {message}", flush=True)

    def fail(self, message: str) -> None:
        self.failures.append(message)
        self.log(f"FAILED {message}")

    def get(self, path: str) -> Answer:
        """One answer, its body as a user agent reads it: a gzip-encoded body is decoded, as a browser
        decodes it. 2026-11-10 the catalog moved behind CloudFront (root ADR-0470), which gzips a PDF
        for a client that accepts it; the bytes held to the hash in the blob's name were the encoded
        ones, and every quarter-hourly probe failed from then on while the PDF itself was whole.

        No answer at all (a timeout, a reset connection) is asked again, at most the policy's
        transport_tries times: it says nothing about the edge, and 2026-11-11 one 20 s timeout rolled
        a Worker deploy back. An answer of any status is the edge's and is returned as it is."""
        request = urllib.request.Request(f"{self.base_url}{path}", headers={
            "Accept-Encoding": "gzip", "User-Agent": self.policy["user_agent"]})
        tries = self.policy["transport_tries"]
        for attempt in range(1, tries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.policy["read_timeout_seconds"]) as response:
                    return decoded((response.status, {k.lower(): v for k, v in response.headers.items()},
                                    response.read()))
            except urllib.error.HTTPError as error:
                return decoded((error.code, {k.lower(): v for k, v in error.headers.items()}, error.read()))
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                if attempt == tries:
                    return 0, {}, f"{error} ({tries} tries)".encode()
                self.log(f"GET {path}: no answer ({error}), asked again ({attempt} of {tries})")
        raise AssertionError("unreachable")

    def hold(self, path: str, content_type: str, judge: Callable[[Answer], str | None]) -> None:
        """`reads` GETs of `path`, each held to 200, `content_type` and `judge`."""

        reads = self.policy["reads"]
        for read in range(1, reads + 1):
            answer = self.get(path)
            status, headers, body = answer
            what = f"GET {path} (read {read} of {reads})"
            if status != 200:
                self.fail(f"{what} answered {status or body.decode(errors='replace')}")
            elif headers.get("content-type") != content_type:
                self.fail(f"{what} answered content type {headers.get('content-type')!r}, not {content_type!r}")
            elif problem := judge(answer):
                self.fail(f"{what}: {problem}")

    def lane(self, lane: str, prefix: str, content_type: str) -> list[str]:
        """Holds the first sample entry the edge serves; the lane's sample ([] when not cut over)."""

        variable = sample_variable(lane)
        if not os.environ.get(variable):
            self.log(f"{lane}: {variable} is not set, the lane is not cut over; skipped")
            return []
        try:
            sample = list(load(pathlib.Path(os.environ[variable]))["sample"])
        except (OSError, ValueError, KeyError, TypeError) as error:
            self.fail(f"{lane}: cannot read its gate sample: {error}")
            return []
        self.probed.append(lane)
        for key in sample[: self.policy["scan_documents"]]:
            path = f"{prefix}{request_path_of(key)}"
            if self.get(path)[0] != 404:
                self.hold(path, content_type, lambda answer: json_document(answer)[1])
                return sample
        self.fail(f"{lane}: none of the first {self.policy['scan_documents']} entries of its gate sample is served")
        return sample

    def media(self, prefix: str, sample: list[str]) -> None:
        """The first blob the media documents lane's sample names, held to the hash in its name."""

        pattern = re.compile(self.contract["parcel_media"]["path_pattern"])
        object_prefix = load(ONBID)["media"]["object_prefix"].rstrip("/")
        key = None
        for entry in sample[: self.policy["scan_documents"]]:
            answer = self.get(f"{prefix}{request_path_of(entry)}")
            document = json_document(answer)[0] if answer[0] == 200 else None
            key = next((k for k in media_keys(document, object_prefix) if pattern.fullmatch(k)), None)
            if key:
                break
        if key is None:
            self.log(f"media: none of the first {self.policy['scan_documents']} served "
                     f"{self.policy['media']['documents_lane']} sample documents names a blob; nothing to read")
            return
        sha = key.rsplit("/", 1)[1].split(".", 1)[0]

        def judge(answer: Answer) -> str | None:
            if not answer[2]:
                return "an empty body"
            digest = hashlib.sha256(answer[2]).hexdigest()
            return None if digest == sha else f"the bytes hash to {digest}, not the {sha} in the name"

        self.probed.append("media")
        self.hold(f"/{key}", MEDIA_TYPES.guess_type(key)[0] or "application/octet-stream", judge)

    def worker(self, lane: str) -> None:
        """Every lane the Worker of `lane` serves, then its media when it serves them."""

        documents_lane = self.policy["media"]["documents_lane"]
        media_sample: list[str] = []
        media_prefix = ""
        for name, prefix, content_type in served_lanes(self.contract, lane):
            sample = self.lane(name, prefix, content_type)
            if name == documents_lane:
                media_sample, media_prefix = sample, prefix
        if lane == self.contract["parcel_media"]["lane"] and media_sample:
            self.media(media_prefix, media_sample)


def production_workers(contract: dict[str, Any]) -> list[tuple[str, str]]:
    """(lane, production hostname) of every by-PNU Worker the host deploys."""

    return [(entry["smoke"]["lane"], lane_block(contract, entry["smoke"]["lane"])["public_hostname"])
            for entry in load(DEPLOYS)["workers"] if entry["smoke"]["kind"] == "by-pnu"]


SCREEN_METRICS = "screen-probe.prom"
SAMPLE_LINE = re.compile(r'^(\w+)\{[^}]*lane="([^"]+)"[^}]*\} (\S+)$')
FRESHNESS = ("foundation_by_pnu_served_manifest_age_seconds", "foundation_by_pnu_lane_behind_seconds",
             "foundation_by_pnu_serving_freshness_budget_seconds")


def lane_freshness(textfiles: str) -> dict[str, str]:
    """Each lane's freshness, from the hourly serving monitor's and the reconciler's series (root ADR-0306,
    ADR-0444): `stale` when it is behind its inputs (or, unmeasured, its manifest is older) by more than
    its budget, `unmeasured` without a manifest age, else `fresh`."""

    found: dict[tuple[str, str], float] = {}
    for line in textfiles.splitlines():
        match = SAMPLE_LINE.match(line.strip())
        if match and match[1] in FRESHNESS:
            found[(match[1], match[2])] = float(match[3])
    verdicts = {}
    for lane in {lane for _, lane in found}:
        age, behind, budget = (found.get((name, lane)) for name in FRESHNESS)
        if age is None or budget is None:
            verdicts[lane] = "unmeasured"
        else:
            verdicts[lane] = "stale" if (behind if behind is not None else age) > budget else "fresh"
    return verdicts


def screen(contract: dict[str, Any], workers: list[tuple[str, str]], textfiles: str) -> tuple[list[dict], list[str]]:
    """The weekly synthetic screen (root ADR-0480): every section a parcel's screen shows, for real PNUs.

    The PNUs are the first entry each cut-over lane's gate sample serves (read on the host, never in the
    repository), so each has at least one section that must show. For every PNU every lane any by-PNU
    Worker serves is read once: `shown` (200, the lane's content type, a JSON object), `absent` (404: the
    parcel has none of it), else `error`. The PNU's own lane answering 404 is `missing`. Each lane's
    freshness is held to its budget. (rows, failures): a failure is an error, a missing section, or a
    stale lane that shows."""

    probes = {base_url: Probe(contract, base_url) for _, base_url in workers}
    lanes = [(name, prefix, content_type, base_url) for lane, base_url in workers
             for name, prefix, content_type in served_lanes(contract, lane)]
    picked: dict[str, tuple[str, str]] = {}
    for name, prefix, _, base_url in lanes:
        variable = sample_variable(name)
        if not os.environ.get(variable):
            continue
        try:
            sample = list(load(pathlib.Path(os.environ[variable]))["sample"])
        except (OSError, ValueError, KeyError, TypeError):
            continue  # the edge probe and the lane's monitor report an unreadable sample
        for key in sample[: probes[base_url].policy["scan_documents"]]:
            if key[:PNU_DIGITS] not in picked and probes[base_url].get(f"{prefix}{request_path_of(key)}")[0] != 404:
                picked[key[:PNU_DIGITS]] = (name, key)
                break
    fresh = lane_freshness(textfiles)
    rows, failures = [], []
    for pnu, (origin, key) in sorted(picked.items()):
        for name, prefix, content_type, base_url in lanes:
            status, headers, body = probes[base_url].get(f"{prefix}{request_path_of(key if name == origin else pnu)}")
            if status == 404:
                verdict = "missing" if name == origin else "absent"
            elif status == 200 and headers.get("content-type") == content_type and json_document((status, headers, body))[1] is None:
                verdict = "shown"
            else:
                verdict = "error"
            rows.append({"pnu": pnu, "lane": name, "verdict": verdict, "status": status, "freshness": fresh.get(name, "unmeasured")})
            if verdict in ("missing", "error"):
                failures.append(f"{name} for a sample PNU of {origin}: {verdict} (HTTP {status or body.decode(errors='replace')[:80]})")
    for name in sorted({row["lane"] for row in rows if row["verdict"] == "shown" and row["freshness"] == "stale"}):
        failures.append(f"{name} shows data older than its freshness budget")
    return rows, failures


def screen_textfile(rows: list[dict], now: float) -> str:
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        counts[(row["lane"], row["verdict"])] = counts.get((row["lane"], row["verdict"]), 0) + 1
    lines = ["# HELP foundation_screen_probe_sections Sections the weekly synthetic screen read, by lane and verdict (root ADR-0480).",
             "# TYPE foundation_screen_probe_sections gauge"]
    lines += [f'foundation_screen_probe_sections{{lane="{lane}",verdict="{verdict}"}} {count}' for (lane, verdict), count in sorted(counts.items())]
    stale = {row["lane"] for row in rows if row["verdict"] == "shown" and row["freshness"] == "stale"}
    lines += ["# HELP foundation_screen_probe_stale_lanes Lanes that showed data older than their freshness budget.",
              "# TYPE foundation_screen_probe_stale_lanes gauge", f"foundation_screen_probe_stale_lanes {len(stale)}",
              "# HELP foundation_screen_probe_pnus The real PNUs the screen was read for.",
              "# TYPE foundation_screen_probe_pnus gauge", f"foundation_screen_probe_pnus {len({row['pnu'] for row in rows})}",
              "# HELP foundation_screen_probe_timestamp_seconds When the weekly synthetic screen last finished.",
              "# TYPE foundation_screen_probe_timestamp_seconds gauge", f"foundation_screen_probe_timestamp_seconds {now:.0f}"]
    return "\n".join(lines) + "\n"


def run_screen(contract: dict[str, Any]) -> int:
    import time  # noqa: PLC0415

    metrics_dir = pathlib.Path(os.environ.get("FOUNDATION_SCREEN_PROBE_METRICS_DIR", "/var/lib/foundation-jobs/metrics"))
    textfiles = ""
    for path in sorted(metrics_dir.glob("*.prom")):
        try:
            textfiles += path.read_text(encoding="utf-8") + "\n"
        except OSError:
            continue
    workers = [(lane, f"https://{hostname}") for lane, hostname in production_workers(contract)]
    rows, failures = screen(contract, workers, textfiles)
    if rows:
        target = metrics_dir / SCREEN_METRICS
        temporary = target.with_name(f".{SCREEN_METRICS}.next")
        temporary.write_text(screen_textfile(rows, time.time()), encoding="utf-8")
        temporary.chmod(0o644)
        temporary.replace(target)
    shown = sum(row["verdict"] == "shown" for row in rows)
    print(f"by-pnu-edge-probe --screen: {len({row['pnu'] for row in rows})} PNUs, {len(rows)} sections read, {shown} shown", flush=True)
    if failures:
        print(f"by-pnu-edge-probe --screen: {len(failures)} failures (root ADR-0480): {'; '.join(failures)}", file=sys.stderr)
        return 1
    if not rows:
        print("by-pnu-edge-probe --screen: no PNU was read (no gate sample is set): nothing shows the screen works", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", help="one hostname, e.g. https://catalog-preview.perfectory.io")
    parser.add_argument("lane", nargs="?", help="with --base-url: the lane of the Worker answering there")
    parser.add_argument("--screen", action="store_true",
                        help="the weekly synthetic screen: every section for real PNUs, against each lane's freshness")
    args = parser.parse_args(argv)
    contract = load(CONTRACT)
    if args.screen:
        if args.base_url:
            parser.error("--screen reads the production hostnames")
        return run_screen(contract)
    if bool(args.base_url) != bool(args.lane):
        parser.error("name both --base-url and the lane, or neither")
    targets = [(args.lane, args.base_url)] if args.base_url else [
        (lane, f"https://{hostname}") for lane, hostname in production_workers(contract)]
    failures, probed = [], []
    for lane, base_url in targets:
        probe = Probe(contract, base_url)
        probe.worker(lane)
        failures += probe.failures
        probed += [f"{base_url} {name}" for name in probe.probed]
    if failures:
        print(f"by-pnu-edge-probe: {len(failures)} failures (root ADR-0327): {'; '.join(failures)}", file=sys.stderr)
        return 1
    if not probed:
        print("by-pnu-edge-probe: no lane was probed (no gate sample is set): nothing shows the edges serve",
              file=sys.stderr)
        return 1
    print(f"by-pnu-edge-probe: passed: {', '.join(probed)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
