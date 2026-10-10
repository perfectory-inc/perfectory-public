#!/usr/bin/env python3
"""The data host deploys the Cloudflare Workers by itself (root ADR-0175).

foundation-autodeploy.sh starts foundation-worker-autodeploy.service (this program, as root) after it
deployed a commit and on every tick that finds the host already on main's head. Each run:

- does nothing while /etc/foundation-platform/autodeploy.off or worker-autodeploy.off exists, or
  while the account token's file (the runtime-secrets contract's cloudflare-deploy group) does not
  exist: until the owner places it, Workers are deployed by hand as before;
- deploys only from the control checkout of the commit the host runs (root ADR-0159 §3), and waits
  while the two differ (a deploy is under way or failed);
- deploys exactly the Workers config/worker-deploys.contract.json lists, and of those only the ones
  whose inputs changed since their last deploy here (the first run deploys all); a Worker whose
  Wrangler config names a Worker other than its gateway block's is refused before anything runs;
- refuses with exit 78 before any side effect when the host is half configured (the token file
  lacks a name or is open to others, a by-PNU lane's monitor file is missing);
- per Worker: dependencies from the lockfile, D1 migrations when the entry has them, then for the
  preview Worker (when the gateway has one) and then production: upload a version, refuse it unless
  it carries every variable and secret the version serving now has (keep_vars inherits them from the
  latest upload, which need not be the one serving), move 100% to it, apply the config's routes,
  and hold it to the smoke; a smoke that fails moves 100% back to the version that served before.

A Worker that fails is tried again on later ticks while it is behind, at most the contract's
attempts_per_change times for the same inputs; each failure exits 1 once so the unit's OnFailure
reports it. A refusal is reported once until its reason changes. Every version id is logged.
"""

from __future__ import annotations

import dataclasses
import fcntl
import hashlib
import json
import os
import pathlib
import re
import shutil
import stat
import subprocess
import sys
import time
from typing import Any

PLATFORM = "platforms/foundation-platform"
CREDENTIAL_GROUP = "cloudflare-deploy"
CREDENTIAL_NAMES = ("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID")
VERSION_ID = re.compile(r"Version ID:\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")
VARIABLE_TYPES = ("plain_text", "json")
SECRET_TYPES = ("secret_text", "secret_key")
WRANGLER = pathlib.Path(__file__).resolve().parent / "worker-wrangler.sh"


@dataclasses.dataclass(frozen=True)
class Host:
    """Where things are. Fixed on the host (main() takes no paths); the tests point it at a fixture."""

    control_root: pathlib.Path = pathlib.Path("/opt/perfectory-control/current")
    release_root: pathlib.Path = pathlib.Path("/opt/foundation-platform")
    state: pathlib.Path = pathlib.Path("/data/foundation-platform/worker-autodeploy")
    off_switches: tuple[pathlib.Path, ...] = (
        pathlib.Path("/etc/foundation-platform/autodeploy.off"),
        pathlib.Path("/etc/foundation-platform/worker-autodeploy.off"),
    )
    credential_uid: int = 0
    workspace_owner: str = "65534:65534"
    poll_tries: int = 24
    poll_seconds: float = 5.0


class Refused(Exception):
    """The host or the tree cannot be deployed from; nothing was done. Carries the exit status."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class Failed(Exception):
    """One Worker's deploy did not complete."""


def log(message: str) -> None:
    print(f"worker-autodeploy: {message}", flush=True)


# --- contracts ---------------------------------------------------------------------------------


@dataclasses.dataclass
class Target:
    """The production Worker or its preview."""

    label: str  # "production" | "preview"
    env: str | None  # the Wrangler env, None for production
    name: str
    hostname: str
    declared_vars: frozenset[str]
    has_routes: bool


@dataclasses.dataclass
class Worker:
    id: str
    gateway: dict[str, Any]
    service_dir: str
    wrangler_config: str
    inputs: list[str]
    smoke: dict[str, Any]
    d1_database: str | None
    targets: list[Target]  # preview first when there is one


def _pointer(block: dict[str, Any], dotted: str) -> Any:
    value: Any = block
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            raise Refused(65, f"the gateway block has no {dotted}")
        value = value[part]
    return value


def _target(label: str, env: str | None, section: dict[str, Any], name: str, hostname: str) -> Target:
    vars_ = section.get("vars") or {}
    return Target(label=label, env=env, name=name, hostname=hostname, declared_vars=frozenset(vars_),
                  has_routes=bool(section.get("routes") or section.get("route")))


def load_workers(tree: pathlib.Path) -> tuple[list[Worker], int]:
    """The listed Workers, each checked against its gateway block and its Wrangler config."""

    platform = tree / PLATFORM
    contract = json.loads((platform / "config/worker-deploys.contract.json").read_text(encoding="utf-8"))
    gateways = json.loads((platform / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
    workers = []
    for entry in contract["workers"]:
        block = gateways.get(entry["gateway"])
        if not isinstance(block, dict) or not block.get("worker_name"):
            raise Refused(65, f"{entry['id']}: no gateway block {entry['gateway']!r} with a worker_name")
        config_path = platform / entry["service_dir"] / entry["wrangler_config"]
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise Refused(65, f"{entry['id']}: cannot read {entry['service_dir']}/{entry['wrangler_config']}: {error}") from error
        # The one rule that keeps the account's other scripts out of reach: a config deploys the
        # Worker its gateway block names, and nothing else.
        if config.get("name") != block["worker_name"]:
            raise Refused(65, f"{entry['id']}: {entry['wrangler_config']} deploys {config.get('name')!r}, "
                              f"not the contract's {block['worker_name']!r}")
        hostname = block.get("public_hostname") or entry["smoke"].get("hostname")
        if not hostname:
            raise Refused(65, f"{entry['id']}: neither the gateway block nor the smoke names a hostname")
        targets = []
        if entry.get("preview"):
            preview = _pointer(block, entry["preview"])
            env = preview["wrangler_env"]
            section = (config.get("env") or {}).get(env)
            if not isinstance(section, dict) or section.get("name") != preview["worker_name"]:
                raise Refused(65, f"{entry['id']}: {entry['wrangler_config']} env {env!r} does not deploy "
                                  f"{preview['worker_name']!r}")
            targets.append(_target("preview", env, section, preview["worker_name"], preview["public_hostname"]))
        targets.append(_target("production", None, config, block["worker_name"], hostname))
        d1 = None
        if entry.get("d1_migrations"):
            d1 = block.get("d1_database_name")
            if not d1:
                raise Refused(65, f"{entry['id']}: d1_migrations without the gateway's d1_database_name")
        workers.append(Worker(id=entry["id"], gateway=block, service_dir=entry["service_dir"],
                              wrangler_config=entry["wrangler_config"], inputs=list(entry["inputs"]),
                              smoke=entry["smoke"], d1_database=d1, targets=targets))
    return workers, int(contract["attempts_per_change"])


def fingerprint(tree: pathlib.Path, inputs: list[str]) -> str:
    """The bytes a Worker is made of: every file under its inputs, by path. Markdown is prose about
    the Worker, not the Worker, so a README edit does not deploy it."""

    digest = hashlib.sha256()
    platform = tree / PLATFORM
    for item in sorted(inputs):
        root = platform / item
        files = [root] if root.is_file() else sorted(
            path for path in root.rglob("*") if path.is_file() and path.suffix != ".md")
        if not files or not root.exists():
            raise Refused(65, f"input {item} does not exist in the control checkout")
        for path in files:
            digest.update(path.relative_to(platform).as_posix().encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def credential_path(tree: pathlib.Path) -> pathlib.Path:
    secrets = json.loads((tree / PLATFORM / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
    return pathlib.Path(next(g["path"] for g in secrets["groups"] if g["name"] == CREDENTIAL_GROUP))


def monitor_file(tree: pathlib.Path, lane: str) -> pathlib.Path:
    secrets = json.loads((tree / PLATFORM / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
    return pathlib.Path(next(g["path"] for g in secrets["groups"] if g["name"] == f"{lane}-serving-monitor"))


# --- the host's state --------------------------------------------------------------------------


def read_json(path: pathlib.Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def write_json(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def names_in(path: pathlib.Path) -> set[str]:
    return set(re.findall(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=", path.read_text(encoding="utf-8"), re.MULTILINE))


def check_credential(host: Host, path: pathlib.Path) -> None:
    """The token file is complete, root's alone, and loaded by the unit. Names only."""

    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != host.credential_uid or stat.S_IMODE(info.st_mode) & 0o077:
        raise Refused(78, f"{path} must be a regular file owned by root with mode 0600 "
                          f"(it is {stat.S_IMODE(info.st_mode):04o}, uid {info.st_uid})")
    missing = [name for name in CREDENTIAL_NAMES if name not in names_in(path)]
    if missing:
        raise Refused(78, f"{path} does not hold {', '.join(missing)}")
    unloaded = [name for name in CREDENTIAL_NAMES if not os.environ.get(name)]
    if unloaded:
        raise Refused(78, f"{', '.join(unloaded)} are not in this process's environment; run it as "
                          f"foundation-worker-autodeploy.service, whose EnvironmentFile loads {path}")


# --- commands ----------------------------------------------------------------------------------


def command(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        tail = (result.stdout + result.stderr).strip().splitlines()[-20:]
        raise Failed(f"{' '.join(args[:6])}... exited {result.returncode}:\n  " + "\n  ".join(tail))
    return result


class Deploy:
    """One Worker's deploy from one workspace."""

    def __init__(self, host: Host, worker: Worker, commit: str, workspace: pathlib.Path):
        self.host, self.worker, self.commit, self.workspace = host, worker, commit, workspace
        self.service = f"{PLATFORM}/{worker.service_dir}"
        # The config Wrangler is given; resolve_d1_ids() swaps in a derived copy in the workspace.
        self.config = worker.wrangler_config

    def wrangler(self, *args: str, target: Target | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
        env = ["--env", target.env] if target is not None and target.env else []
        return command(["bash", str(WRANGLER), str(self.workspace), self.service, "wrangler", *args,
                        "--config", self.config, *env], check=check)

    def resolve_d1_ids(self) -> None:
        """The repository's config names its D1 databases without a database_id (Wrangler created
        them; the rendered config and its test keep the id out of the repository), and remote D1
        operations need it (2026-10-10, the first run: "missing a database_id"). Each id is looked up
        by database_name in the account and written into a derived config in this workspace only,
        after `config:check` has held the rendered copy to the contract."""

        rendered = self.workspace / self.service / self.worker.wrangler_config
        config = json.loads(rendered.read_text(encoding="utf-8"))
        sections = [config, *[s for s in (config.get("env") or {}).values() if isinstance(s, dict)]]
        missing = [d for s in sections for d in s.get("d1_databases") or [] if not d.get("database_id")]
        if not missing:
            return
        out = self.wrangler("d1", "list", "--json").stdout
        try:
            listed = json.loads(out[out.index("["):])
        except ValueError as error:
            raise Failed(f"cannot read the account's D1 databases: {error}") from error
        for database in missing:
            name = database.get("database_name")
            ids = [d.get("uuid") for d in listed if isinstance(d, dict) and d.get("name") == name]
            if len(ids) != 1 or not ids[0]:
                raise Failed(f"D1 database {name!r}: the account has {len(ids)} databases of that name, "
                             f"not one; nothing was migrated or uploaded")
            database["database_id"] = ids[0]
            log(f"{self.worker.id}: D1 database {name} resolved by name for this deploy")
        derived = rendered.with_name(f"autodeploy.{self.worker.wrangler_config}")
        derived.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        self.config = derived.name

    def serving(self, target: Target) -> str:
        """The one version at 100% now. A split (an operator's canary) is not ours to move."""

        out = self.wrangler("deployments", "status", "--name", target.name, "--json", target=target).stdout
        try:
            versions = json.loads(out[out.index("{"):])["versions"]
        except (ValueError, KeyError) as error:
            raise Failed(f"{target.name}: cannot read its deployment: {error}") from error
        full = [v["version_id"] for v in versions if v.get("percentage") == 100]
        if len(versions) != 1 or len(full) != 1:
            raise Failed(f"{target.name}: traffic is split across {len(versions)} versions (a canary under way?); "
                         f"not deploying over it")
        return full[0]

    def bindings(self, target: Target, version: str) -> dict[str, tuple[str, str]]:
        out = self.wrangler("versions", "view", version, "--name", target.name, "--json", target=target).stdout
        try:
            found = json.loads(out[out.index("{"):])["resources"]["bindings"]
        except (ValueError, KeyError) as error:
            raise Failed(f"{target.name}: cannot read the bindings of {version}: {error}") from error
        return {b["name"]: (b["type"], json.dumps(b.get("text", b.get("json")), sort_keys=True)) for b in found}

    def same_settings(self, target: Target, before: str, after: str) -> None:
        """Every variable and secret `before` serves with is on `after`, unchanged, unless the config
        sets it. Names only in what it says."""

        old, new = self.bindings(target, before), self.bindings(target, after)
        problems = []
        for name, (kind, value) in sorted(old.items()):
            if kind in SECRET_TYPES and new.get(name, ("",))[0] != kind:
                problems.append(f"secret {name} is missing")
            elif kind in VARIABLE_TYPES and name not in target.declared_vars and new.get(name) != (kind, value):
                problems.append(f"variable {name} is {'missing' if name not in new else 'different'}")
        for name, (kind, _) in sorted(new.items()):
            if kind in VARIABLE_TYPES and name not in old and name not in target.declared_vars:
                problems.append(f"variable {name} is new and not in the config")
        if problems:
            raise Failed(f"{target.name}: the uploaded version {after} does not keep what {before} serves with "
                         f"({'; '.join(problems)}); it was not deployed")

    def upload(self, target: Target) -> str:
        out = self.wrangler("versions", "upload", "--message", f"autodeploy {self.commit}",
                            "--tag", self.commit[:12], target=target)
        found = VERSION_ID.findall(out.stdout + out.stderr)
        if not found:
            raise Failed(f"{target.name}: the upload printed no version id; nothing was deployed")
        return found[-1]

    def move(self, target: Target, version: str, message: str) -> None:
        self.wrangler("versions", "deploy", f"{version}@100%", "--name", target.name, "--yes",
                      "--message", message, target=target)

    # -- smoke --

    def answer(self, url: str) -> tuple[int, dict[str, str]]:
        result = command(["curl", "-sS", "-o", "/dev/null", "-D", "-", "--max-time", "20",
                          "-H", "Cache-Control: no-cache", url], check=False)
        status, headers = 0, {}
        for line in result.stdout.splitlines():
            match = re.match(r"^HTTP/\S+\s+(\d{3})", line)
            if match:
                status, headers = int(match.group(1)), {}
            elif ":" in line:
                key, _, value = line.partition(":")
                headers[key.strip().lower()] = value.strip()
        return status, headers

    def wait_for(self, url: str, accept, what: str) -> None:
        seen = None
        for attempt in range(self.host.poll_tries):
            status, headers = self.answer(url)
            seen = (status, headers)
            if accept(status, headers):
                return
            if attempt + 1 < self.host.poll_tries:
                time.sleep(self.host.poll_seconds)
        raise Failed(f"{url} never answered {what} (last: {seen[0] if seen else 'nothing'})")

    def smoke(self, target: Target, version: str) -> None:
        if self.serving(target) != version:
            raise Failed(f"{target.name}: {version} is not the version at 100% after the deploy")
        smoke = self.worker.smoke
        if smoke["kind"] == "by-pnu":
            header = self.worker.gateway["version_header"].lower()
            url = f"https://{target.hostname}{self.worker.gateway['request_path']['capabilities']}"
            self.wait_for(url, lambda status, headers: status == 200 and headers.get(header) == version,
                          f"200 from {version}")
            self.monitor(smoke["lane"], target.hostname)
        else:
            url = f"https://{target.hostname}{smoke['path']}"
            self.wait_for(url, lambda status, _: status == smoke["status"], str(smoke["status"]))
        log(f"{self.worker.id}: {target.label} {target.name} passed its smoke on {version}")

    def monitor(self, lane: str, hostname: str) -> None:
        """The lane's hourly serving monitor, pointed at this hostname, with its unit's files."""

        unit = f"foundation-by-pnu-serving-monitor@{lane}.service"
        shown = command(["systemctl", "show", "-p", "EnvironmentFiles", "--value", unit]).stdout
        files = [("" if ignore == "no" else "-") + path
                 for path, ignore in re.findall(r"(/\S+) \(ignore_errors=(yes|no)\)", shown)]
        if not files:
            raise Failed(f"{unit} names no environment files; is it installed?")
        prefix = f"FOUNDATION_PLATFORM_{lane.upper()}_BY_PNU_SERVING"
        command(["systemd-run", "--wait", "--collect", "--pipe", "--quiet", "-p", "User=foundation-platform",
                 "-p", f"WorkingDirectory={self.host.release_root / 'current'}",
                 *[item for path in files for item in ("-p", f"EnvironmentFile={path}")],
                 "-E", f"{prefix}_MONITOR_BASE_URL=https://{hostname}",
                 str(self.host.release_root / "current/scripts/ops/by-pnu-serving-monitor.sh"), lane])

    # -- the whole deploy --

    def run(self) -> dict[str, Any]:
        command(["bash", str(WRANGLER), str(self.workspace), self.service, "install"])
        self.resolve_d1_ids()
        before = {target.label: self.serving(target) for target in self.worker.targets}
        if self.worker.d1_database:
            self.wrangler("d1", "migrations", "apply", self.worker.d1_database, "--remote")
            log(f"{self.worker.id}: D1 migrations applied to {self.worker.d1_database}")
        record = {}
        for target in self.worker.targets:
            old = before[target.label]
            new = self.upload(target)
            log(f"{self.worker.id}: {target.label} {target.name} uploaded {new} (serving {old})")
            self.same_settings(target, old, new)
            try:
                # A deployment that failed may still have moved some traffic: it is rolled back too.
                self.move(target, new, f"autodeploy {self.commit}")
                log(f"{self.worker.id}: {target.label} {target.name} moved to {new}")
                if target.has_routes:
                    self.wrangler("triggers", "deploy", target=target)
                self.smoke(target, new)
            except Failed as failure:
                self.roll_back(target, old, new)
                raise Failed(f"{target.label} {target.name} on {new}: {failure}") from failure
            record[target.label] = {"name": target.name, "from": old, "to": new}
        return record

    def roll_back(self, target: Target, old: str, new: str) -> None:
        try:
            self.move(target, old, f"autodeploy rollback from {new}")
            log(f"{self.worker.id}: {target.label} {target.name} rolled back to {old}")
        except Failed as failure:
            log(f"{self.worker.id}: THE ROLLBACK OF {target.name} TO {old} FAILED; {new} may still serve. "
                f"Move it back by hand (wrangler versions deploy {old}@100% --name {target.name}): {failure}")


# --- one run -----------------------------------------------------------------------------------


def stage(host: Host, tree: pathlib.Path, worker: Worker) -> pathlib.Path:
    """A fresh copy of the Worker's inputs, owned by the account the container runs as."""

    workspace = host.state / "work" / worker.id
    if workspace.exists():
        shutil.rmtree(workspace)
    for item in worker.inputs:
        source, target = tree / PLATFORM / item, workspace / PLATFORM / item
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target, ignore=shutil.ignore_patterns("node_modules", ".wrangler"))
        else:
            shutil.copy2(source, target)
    command(["chown", "-R", host.workspace_owner, str(workspace)])
    return workspace


def refuse_once(host: Host, refusal: Refused) -> int:
    marker = host.state / "refused"
    message = str(refusal)
    if marker.exists() and marker.read_text(encoding="utf-8").strip() == message:
        log(f"still refused, reported before: {message}")
        return 0
    marker.write_text(message + "\n", encoding="utf-8")
    log(f"refused: {message}")
    return refusal.status


def run(host: Host) -> int:
    for switch in host.off_switches:
        if switch.exists():
            log(f"off ({switch} exists)")
            return 0
    tree = host.control_root.resolve()
    credential = credential_path(tree)
    if not credential.exists():
        log(f"off: {credential} does not exist (root ADR-0175); Workers are deployed by hand")
        return 0
    host.state.mkdir(parents=True, exist_ok=True)
    host.state.chmod(0o700)
    with open(host.state / "lock", "w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log("a Worker deploy is already running")
            return 0
        return run_locked(host, tree, credential)


def run_locked(host: Host, tree: pathlib.Path, credential: pathlib.Path) -> int:
    commit = (tree / ".perfectory-control-commit").read_text(encoding="utf-8").strip()
    running = (host.release_root / "current").resolve().name
    if commit != running:
        log(f"the host runs {running} and the control checkout is {commit}; waiting for the host deploy")
        return 0
    try:
        workers, attempts_allowed = load_workers(tree)
        behind = []
        for worker in workers:
            digest = fingerprint(tree, worker.inputs)
            done = read_json(host.state / "workers" / f"{worker.id}.json")
            if done and done.get("fingerprint") == digest:
                continue
            failed = read_json(host.state / "failed" / f"{worker.id}.json")
            if failed and failed.get("fingerprint") == digest and failed.get("attempts", 0) >= attempts_allowed:
                log(f"{worker.id}: gave up on these inputs after {failed['attempts']} attempts; waiting for a change "
                    f"(or remove {host.state / 'failed' / (worker.id + '.json')} to try again)")
                continue
            behind.append((worker, digest, failed if failed and failed.get("fingerprint") == digest else None))
        if not behind:
            return 0
        check_credential(host, credential)
        for worker, _, _ in behind:
            if worker.smoke["kind"] == "by-pnu" and not monitor_file(tree, worker.smoke["lane"]).exists():
                raise Refused(78, f"{worker.id}: its smoke runs the {worker.smoke['lane']} serving monitor, whose file "
                                  f"{monitor_file(tree, worker.smoke['lane'])} does not exist")
    except Refused as refusal:
        return refuse_once(host, refusal)
    (host.state / "refused").unlink(missing_ok=True)

    log(f"deploying {', '.join(worker.id for worker, _, _ in behind)} from {commit}")
    failures = 0
    for worker, digest, failed in behind:
        try:
            record = Deploy(host, worker, commit, stage(host, tree, worker)).run()
        except Failed as failure:
            failures += 1
            attempts = (failed or {}).get("attempts", 0) + 1
            write_json(host.state / "failed" / f"{worker.id}.json",
                       {"fingerprint": digest, "commit": commit, "attempts": attempts, "error": str(failure)[:2000]})
            log(f"{worker.id}: the deploy of {commit} failed (attempt {attempts}): {failure}")
            continue
        write_json(host.state / "workers" / f"{worker.id}.json",
                   {"fingerprint": digest, "commit": commit,
                    "deployed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "targets": record})
        (host.state / "failed" / f"{worker.id}.json").unlink(missing_ok=True)
        log(f"{worker.id}: deployed {commit}: " + ", ".join(f"{label} {r['from']} -> {r['to']}" for label, r in record.items()))
    return 1 if failures else 0


def main() -> int:
    return run(Host())


if __name__ == "__main__":
    sys.exit(main())
