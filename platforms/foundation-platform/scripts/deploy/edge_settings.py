#!/usr/bin/env python3
"""The data host applies the console's edge settings by itself (root ADR-0436).

foundation-autodeploy.sh starts foundation-edge-settings.service (this program, as root) beside the
Worker deploy, on every tick that finds the host on main's head. Each run:

- does nothing while /etc/foundation-platform/autodeploy.off or edge-settings.off exists, while the
  account token's file (runtime-secrets group cloudflare-deploy) does not exist, or while the control
  checkout is not the commit the host runs (a deploy is under way);
- reads the allowlist from the identity platform's staff list (identity.staff with at least one role)
  and refuses an empty one: the edge would then let nobody in, the owner included;
- applies infra/edge with the pinned OpenTofu image only when what it applies changed (the module, the
  files it reads, the allowlist, the account) or a day has passed since the last apply, which is how
  a dashboard edit (drift) is put back within a day;
- keeps the state in the R2 bucket STATE_BUCKET (made here when missing), locked by OpenTofu's
  conditional PUT and encrypted with a passphrase made once on this host (runtime-secrets group
  edge-settings); its S3 key pair is derived from the account token (Cloudflare: the token's id and
  the SHA-256 of its value), so no second credential exists;
- writes the tunnel token OpenTofu read back into the runtime-secrets group console-tunnel, and starts
  the console (foundation-release.sh console) when that token changed. Nobody copies it;
- then applies the production shape ledger's bucket, infra/shape-ledger (root ADR-0476), the same way
  under its own records: the bucket, its lock rules and its public read domain, with the account token
  alone, the bucket and domain read from the R2 connection contract;
- then applies the Seoul front door, infra/cdn (root ADR-0461), the same way under its own records,
  once the runtime-secrets group aws-cdn holds the separate AWS account's apply key: CloudFront, its
  certificates and the budget with that key, the zone's records with the account token, the state in
  the same bucket under its own key with the same passphrase. Without that key it does nothing.

A failure exits 1 so the unit's OnFailure reports it, at most ATTEMPTS times for the same inputs and
the same deployer files; then it waits for a change to either. Names and counts only; never a value
or an address.
"""

from __future__ import annotations

import dataclasses
import fcntl
import hashlib
import json
import os
import pathlib
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import runtime_secrets  # noqa: E402

PLATFORM = "platforms/foundation-platform"
MODULE = "infra/edge"
# What the module reads and what applies it, under the repository root: a change to the first applies
# again; a change to the second gives a failed apply fresh attempts.
INPUTS = (f"{PLATFORM}/{MODULE}", f"{PLATFORM}/compose.console.yml",
          "platforms/identity-platform/config/staff-console-applications.v1.json")
TOOL_FILES = (f"{PLATFORM}/scripts/deploy/edge_settings.py", "tools/container-images.env")
STATE_BUCKET = "foundation-platform-edge-settings-state-prod"
STATE_KEY = "edge/terraform.tfstate"
FRONT_DOOR_MODULE = "infra/cdn"
FRONT_DOOR_CONTRACT = f"{PLATFORM}/config/cdn-front-door.contract.json"
FRONT_DOOR_INPUTS = (f"{PLATFORM}/{FRONT_DOOR_MODULE}", FRONT_DOOR_CONTRACT)
FRONT_DOOR_STATE_KEY = "cdn/terraform.tfstate"
SHAPE_LEDGER_MODULE = "infra/shape-ledger"
SHAPE_LEDGER_INPUTS = (f"{PLATFORM}/{SHAPE_LEDGER_MODULE}",)
SHAPE_LEDGER_STATE_KEY = "shape-ledger/terraform.tfstate"
FRONT_DOOR_GROUP = "aws-cdn"
FRONT_DOOR_KEY_NAMES = ("FOUNDATION_PLATFORM_AWS_CDN_ACCESS_KEY_ID", "FOUNDATION_PLATFORM_AWS_CDN_SECRET_ACCESS_KEY")
TOKEN_NAME = "FOUNDATION_PLATFORM_WORKERS_WRITER_TOKEN"
PASSPHRASE_NAME = "FOUNDATION_PLATFORM_EDGE_STATE_PASSPHRASE"
DRIFT_SECONDS = 24 * 3600
ATTEMPTS = 3
STAFF_SQL = ("SELECT DISTINCT lower(s.email) FROM identity.staff s "
             "JOIN identity.staff_role r ON r.staff_id = s.id ORDER BY 1")
API = "https://api.cloudflare.com/client/v4"


@dataclasses.dataclass(frozen=True)
class Host:
    """Where things are. Fixed on the host (main() takes no paths); the tests point it at a fixture."""

    control_root: pathlib.Path = pathlib.Path("/opt/perfectory-control/current")
    release_root: pathlib.Path = pathlib.Path("/opt/foundation-platform")
    state: pathlib.Path = pathlib.Path("/data/foundation-platform/edge-settings")
    secrets_root: pathlib.Path = pathlib.Path("/")
    off_switches: tuple[pathlib.Path, ...] = (
        pathlib.Path("/etc/foundation-platform/autodeploy.off"),
        pathlib.Path("/etc/foundation-platform/edge-settings.off"),
    )


class Refused(Exception):
    """Nothing was applied; carries the exit status."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def log(message: str) -> None:
    print(f"edge-settings: {message}", flush=True)


def group_path(tree: pathlib.Path, name: str) -> pathlib.Path:
    contract = json.loads((tree / PLATFORM / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
    return pathlib.Path(next(g["path"] for g in contract["groups"] if g["name"] == name))


def on_host(host: Host, path: pathlib.Path) -> pathlib.Path:
    return host.secrets_root / str(path).lstrip("/")


def values_in(path: pathlib.Path) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line and not line.startswith("#"))


def set_group(host: Host, tree: pathlib.Path, group: str, values: dict[str, str]) -> None:
    """Through the contract (root ADR-0247): the file keeps the group's owner, group and mode."""

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=host.state, delete=False) as source:
        os.chmod(source.name, 0o600)
        source.write("".join(f"{name}={value}\n" for name, value in values.items()))
    try:
        runtime_secrets.set_names(tree / PLATFORM, group, pathlib.Path(source.name), host.secrets_root)
    finally:
        os.unlink(source.name)


def s3_key_pair(token: str, token_id: str) -> tuple[str, str]:
    """R2's S3 credentials of an account API token: its id, and the SHA-256 of its value."""

    return token_id, hashlib.sha256(token.encode()).hexdigest()


def cloudflare(token: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(f"{API}{path}", method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def staff_emails() -> list[str]:
    """The identity platform's staff list, read from its database container (no network, no port)."""

    container = subprocess.run(
        ["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=identity-platform-runtime",
         "--filter", "label=com.docker.compose.service=identity-db"],
        check=True, capture_output=True, text=True).stdout.split()
    if len(container) != 1:
        raise Refused(69, f"expected one running identity-db, found {len(container)}")
    rows = subprocess.run(["docker", "exec", container[0], "sh", "-c",
                           'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"', "_", STAFF_SQL],
                          check=True, capture_output=True, text=True).stdout.split()
    return sorted({row for row in rows if "@" in row})


def image(tree: pathlib.Path) -> str:
    pins = values_in(tree / "tools/container-images.env")
    return pins["EDGE_SETTINGS_IMAGE"]


def tofu(host: Host, tree: pathlib.Path, env: dict[str, str], *args: str, module: str = MODULE,
         work: str = "work") -> str:
    """One OpenTofu command in its pinned image: the checkout read-only, the module's work directory the
    only place it writes. Values pass by name from the environment, never on a command line."""

    names = sorted(env)
    memory = json.loads((tree / PLATFORM / "config/worker-deploys.contract.json").read_text(encoding="utf-8"))[
        "containers"]["edge_settings"]["memory_limit"]
    command = ["docker", "run", "--rm", "--init", "--memory", memory, "--memory-swap", memory, "--pids-limit", "512",
               "--read-only", "--tmpfs", "/tmp", "-v", f"{tree}:/src:ro", "-v", f"{host.state / work}:/work",
               "-w", f"/src/{PLATFORM}/{module}", "-e", "HOME=/tmp", "-e", "TF_DATA_DIR=/work/data", "-e", "TF_IN_AUTOMATION=1",
               "-e", "TF_INPUT=0", *[item for name in names for item in ("-e", name)], image(tree), *args]
    result = subprocess.run(command, env={**os.environ, **env}, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"tofu {args[0]} exited {result.returncode}: {result.stderr[-2000:]}")
    return result.stdout


def read_json(path: pathlib.Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def write_json(path: pathlib.Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def run(host: Host) -> int:
    for switch in host.off_switches:
        if switch.exists():
            log(f"off ({switch} exists)")
            return 0
    tree = host.control_root.resolve()
    credential = group_path(tree, "cloudflare-deploy")
    if not on_host(host, credential).exists():
        log(f"off: {credential} does not exist (root ADR-0175)")
        return 0
    commit = (tree / ".perfectory-control-commit").read_text(encoding="utf-8").strip()
    running = (host.release_root / "current").resolve().name
    if commit != running:
        log(f"the host runs {running} and the control checkout is {commit}; waiting for the host deploy")
        return 0
    host.state.mkdir(parents=True, exist_ok=True)
    host.state.chmod(0o700)
    with open(host.state / "lock", "w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log("an apply is already running")
            return 0
        try:
            return run_locked(host, tree, commit)
        except Refused as refusal:
            log(f"refused: {refusal}")
            return refusal.status


def run_locked(host: Host, tree: pathlib.Path, commit: str) -> int:
    token, account = os.environ.get(TOKEN_NAME), os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    if not token or not account:
        raise Refused(78, f"{TOKEN_NAME} and CLOUDFLARE_ACCOUNT_ID are not in this process's environment; "
                          "run it as foundation-edge-settings.service")
    emails = staff_emails()
    if not emails:
        raise Refused(78, "the identity platform lists no staff with a role; the edge would let nobody in")
    tool = fingerprint(tree, TOOL_FILES)
    tunnel_file = on_host(host, group_path(tree, "console-tunnel"))
    digest = hashlib.sha256("\0".join([fingerprint(tree, INPUTS), account, *emails]).encode()).hexdigest()
    console = converge(host.state, digest, tool, commit, bool(values_in(tunnel_file).get("TUNNEL_TOKEN")),
                       lambda: apply(host, tree, token, account, emails, tunnel_file),
                       f"{len(emails)} staff on the allowlist")
    # The ledger bucket and the front door are each their own apply: a console failure holds neither
    # back, nor the reverse.
    ledger_values = shape_ledger_values(tree)
    digest = hashlib.sha256("\0".join([fingerprint(tree, SHAPE_LEDGER_INPUTS), json.dumps(ledger_values, sort_keys=True),
                                       account]).encode()).hexdigest()
    ledger = converge(host.state / "shape-ledger", digest, tool, commit, True,
                      lambda: apply_shape_ledger(host, tree, token, account, ledger_values), "shape ledger")
    keys = values_in(on_host(host, group_path(tree, FRONT_DOOR_GROUP)))
    if not all(keys.get(name) for name in FRONT_DOOR_KEY_NAMES):
        log(f"front door off: {group_path(tree, FRONT_DOOR_GROUP)} holds no AWS apply key (root ADR-0461)")
        return max(console, ledger)
    digest = hashlib.sha256("\0".join([
        fingerprint(tree, FRONT_DOOR_INPUTS), json.dumps(front_door_hostnames(tree), sort_keys=True), account,
        hashlib.sha256(keys[FRONT_DOOR_KEY_NAMES[0]].encode()).hexdigest(), *emails]).encode()).hexdigest()
    front = converge(host.state / "cdn", digest, tool, commit, True,
                     lambda: apply_front_door(host, tree, token, account, emails, keys), "front door")
    return max(console, ledger, front)


def converge(records: pathlib.Path, digest: str, tool: str, commit: str, fresh: bool, apply_inputs,
             applied: str) -> int:
    """Applies when the inputs changed, a day has passed (drift) or fresh is false, at most ATTEMPTS
    times for the same inputs and the same deployer files; then waits for a change to either."""

    records.mkdir(mode=0o700, exist_ok=True)
    done = read_json(records / "applied.json") or {}
    if done.get("digest") == digest and time.time() - done.get("at", 0) < DRIFT_SECONDS and fresh:
        return 0
    failed = read_json(records / "failed.json") or {}
    if failed.get("digest") == digest and failed.get("tool") == tool and failed.get("attempts", 0) >= ATTEMPTS:
        log(f"gave up on these inputs after {failed['attempts']} attempts with this deployer; waiting for a "
            f"change to either (or remove {records / 'failed.json'})")
        return 0
    try:
        apply_inputs()
    except Exception as failure:  # noqa: BLE001 (any failure counts as an attempt and is reported)
        attempts = failed.get("attempts", 0) + 1 if failed.get("digest") == digest and failed.get("tool") == tool else 1
        write_json(records / "failed.json", {"digest": digest, "tool": tool, "commit": commit,
                                             "attempts": attempts, "error": str(failure)[:2000]})
        log(f"the apply of {commit} failed (attempt {attempts}): {failure}")
        return 1
    write_json(records / "applied.json", {"digest": digest, "commit": commit, "at": int(time.time())})
    (records / "failed.json").unlink(missing_ok=True)
    log(f"applied {commit} ({applied})")
    return 0


def fingerprint(tree: pathlib.Path, inputs: tuple[str, ...]) -> str:
    """Every file under the inputs, by its path in the repository, with its bytes."""

    digest = hashlib.sha256()
    for item in sorted(inputs):
        root = tree / item
        files = [root] if root.is_file() else sorted(path for path in root.rglob("*") if path.is_file())
        if not files:
            raise Refused(65, f"input {item} does not exist in {tree}")
        for path in files:
            digest.update(path.relative_to(tree).as_posix().encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def account_state(host: Host, tree: pathlib.Path, token: str, account: str) -> tuple[str, str, str]:
    """The state bucket (made when missing), its S3 key pair and the state passphrase (made once)."""

    status, verified = cloudflare(token, "GET", "/user/tokens/verify")
    if status != 200:
        raise RuntimeError(f"the account token does not verify (HTTP {status})")
    status, _ = cloudflare(token, "GET", f"/accounts/{account}/r2/buckets/{STATE_BUCKET}")
    if status == 404:
        status, _ = cloudflare(token, "POST", f"/accounts/{account}/r2/buckets", {"name": STATE_BUCKET})
        if status != 200:
            raise RuntimeError(f"creating the state bucket {STATE_BUCKET} answered HTTP {status}")
        log(f"created the state bucket {STATE_BUCKET}")
    elif status != 200:
        raise RuntimeError(f"reading the state bucket {STATE_BUCKET} answered HTTP {status}")
    passphrase_file = on_host(host, group_path(tree, "edge-settings"))
    passphrase = values_in(passphrase_file).get(PASSPHRASE_NAME)
    if not passphrase:
        # Made once here; lost, the state is read again from the account (docs/runbooks/edge-settings.md).
        set_group(host, tree, "edge-settings", {PASSPHRASE_NAME: secrets.token_urlsafe(48)})
        passphrase = values_in(passphrase_file)[PASSPHRASE_NAME]
        log(f"made the state passphrase in {passphrase_file}")
    key_id, key_secret = s3_key_pair(token, verified["result"]["id"])
    return key_id, key_secret, passphrase


def init(host: Host, tree: pathlib.Path, env: dict[str, str], account: str, key: str, module: str,
         work: str) -> None:
    directory = host.state / work
    directory.mkdir(mode=0o700, exist_ok=True)
    (directory / "backend.hcl").write_text(
        f'bucket = "{STATE_BUCKET}"\nkey = "{key}"\n'
        f'endpoints = {{ s3 = "https://{account}.r2.cloudflarestorage.com" }}\n', encoding="utf-8")
    tofu(host, tree, env, "init", "-no-color", "-lockfile=readonly", "-reconfigure",
         "-backend-config=/work/backend.hcl", module=module, work=work)


def apply(host: Host, tree: pathlib.Path, token: str, account: str, emails: list[str],
          tunnel_file: pathlib.Path) -> None:
    """Plans and applies the console's module; starts the console when the tunnel token changed."""

    key_id, key_secret, passphrase = account_state(host, tree, token, account)
    env = {"CLOUDFLARE_API_TOKEN": token, "AWS_ACCESS_KEY_ID": key_id, "AWS_SECRET_ACCESS_KEY": key_secret,
           "TF_VAR_account_id": account, "TF_VAR_staff_emails": json.dumps(emails),
           "TF_VAR_state_passphrase": passphrase}
    init(host, tree, env, account, STATE_KEY, MODULE, "work")
    tofu(host, tree, env, "apply", "-no-color", "-auto-approve")
    tunnel = tofu(host, tree, env, "output", "-raw", "tunnel_token").strip()
    if not tunnel:
        raise RuntimeError("the module gave no tunnel token")
    if values_in(tunnel_file).get("TUNNEL_TOKEN") == tunnel:
        return
    set_group(host, tree, "console-tunnel", {"TUNNEL_TOKEN": tunnel})
    log(f"wrote the tunnel token into {tunnel_file}")
    console = host.control_root / PLATFORM / "scripts/deploy/foundation-release.sh"
    subprocess.run(["/bin/bash", str(console), "console"], check=False)


def shape_ledger_values(tree: pathlib.Path) -> dict[str, str]:
    """The ledger bucket and its public read origin, from the R2 connection contract (root ADR-0476):
    of that generated, often changing file only these are the module's input."""

    r2 = json.loads((tree / PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
    values = r2["connections"]["shape_ledger"]["expected_values"]
    return {"bucket": values["FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_BUCKET"],
            "public_base_url": values["FOUNDATION_PLATFORM_R2_SHAPE_LEDGER_PUBLIC_BASE_URL"]}


def apply_shape_ledger(host: Host, tree: pathlib.Path, token: str, account: str, values: dict[str, str]) -> None:
    """Plans and applies infra/shape-ledger with the account token: the bucket, its lock rules, its
    public read domain."""

    key_id, key_secret, passphrase = account_state(host, tree, token, account)
    env = {"CLOUDFLARE_API_TOKEN": token, "AWS_ACCESS_KEY_ID": key_id, "AWS_SECRET_ACCESS_KEY": key_secret,
           "TF_VAR_account_id": account, "TF_VAR_bucket": values["bucket"],
           "TF_VAR_public_base_url": values["public_base_url"], "TF_VAR_state_passphrase": passphrase}
    init(host, tree, env, account, SHAPE_LEDGER_STATE_KEY, SHAPE_LEDGER_MODULE, "work-shape-ledger")
    tofu(host, tree, env, "apply", "-no-color", "-auto-approve", module=SHAPE_LEDGER_MODULE, work="work-shape-ledger")


def front_door_hostnames(tree: pathlib.Path) -> dict[str, str]:
    """The public hostnames the module reads from the R2 connection contract, by door: of that
    generated, often changing file only these are the front door's input."""

    doors = json.loads((tree / FRONT_DOOR_CONTRACT).read_text(encoding="utf-8"))["doors"]
    r2 = json.loads((tree / PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
    return {name: door.get("public_hostname") or r2[door["gateway"]]["public_hostname"]
            for name, door in doors.items()}


def apply_front_door(host: Host, tree: pathlib.Path, token: str, account: str, emails: list[str],
                     keys: dict[str, str]) -> None:
    """Plans and applies infra/cdn: AWS with the separate account's key, the zone with the account token."""

    key_id, key_secret, passphrase = account_state(host, tree, token, account)
    status, subdomain = cloudflare(token, "GET", f"/accounts/{account}/workers/subdomain")
    if status != 200:
        raise RuntimeError(f"reading the workers.dev subdomain answered HTTP {status}")
    env = {"CLOUDFLARE_API_TOKEN": token, "AWS_ACCESS_KEY_ID": key_id, "AWS_SECRET_ACCESS_KEY": key_secret,
           "TF_VAR_aws_access_key_id": keys[FRONT_DOOR_KEY_NAMES[0]],
           "TF_VAR_aws_secret_access_key": keys[FRONT_DOOR_KEY_NAMES[1]],
           "TF_VAR_workers_subdomain": subdomain["result"]["subdomain"], "TF_VAR_alert_emails": json.dumps(emails),
           "TF_VAR_state_passphrase": passphrase}
    init(host, tree, env, account, FRONT_DOOR_STATE_KEY, FRONT_DOOR_MODULE, "work-cdn")
    tofu(host, tree, env, "apply", "-no-color", "-auto-approve", module=FRONT_DOOR_MODULE, work="work-cdn")
    distributions = json.loads(tofu(host, tree, env, "output", "-json", "distributions",
                                    module=FRONT_DOOR_MODULE, work="work-cdn") or "{}")
    for name, domain in sorted(distributions.items()):
        log(f"front door {name}: {domain}")


def main() -> int:
    return run(Host())


if __name__ == "__main__":
    sys.exit(main())
