"""The data host deploys the Cloudflare Workers by itself (scripts/deploy/worker_autodeploy.py, root
ADR-0175).

A fixture host holds a control checkout (the real contracts and Wrangler configs, a stub source per
gateway), the release it runs, and the token's and monitors' files where the fixture's copy of the
runtime-secrets contract says. Stand-ins answer for the rest: `docker` plays Wrangler against a small
fake account (cloud.json: each Worker's versions, their bindings, the one at 100%), which also holds
a script no contract lists; `curl` answers each hostname with the version serving it; `systemctl`
and `systemd-run` play the serving monitor. scripts/deploy/worker-wrangler.sh runs for real.
"""

import contextlib
import importlib.util
import io
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
ROOT = PLATFORM.parents[1]
SCRIPT = PLATFORM / "scripts/deploy/worker_autodeploy.py"
CONTRACT = json.loads((PLATFORM / "config/worker-deploys.contract.json").read_text(encoding="utf-8"))
GATEWAYS = json.loads((PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
COMMIT = "a" * 40
UNLISTED = "someone-elses-script"
OLD = "00000000-0000-4000-8000-{:012d}"

spec = importlib.util.spec_from_file_location("worker_autodeploy", SCRIPT)
autodeploy = importlib.util.module_from_spec(spec)
sys.modules["worker_autodeploy"] = autodeploy
spec.loader.exec_module(autodeploy)

# Wrangler, against cloud.json. Every call is logged with the Worker it acted on.
FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, pathlib, sys, uuid
args = sys.argv[1:]
root = pathlib.Path(os.environ["FIXTURE_ROOT"])
cloud_path = root / "cloud.json"
cloud = json.loads(cloud_path.read_text())
mount = args[args.index("-v") + 1].split(":")[0]
workdir = pathlib.Path(mount) / args[args.index("-w") + 1].removeprefix("/work/")
def log(entry):
    with open(root / "calls.log", "a") as out:
        out.write(json.dumps(entry) + "\n")
if "install" in " ".join(args[-1:]):
    log({"action": "install", "dir": workdir.name})
    sys.exit(0)
w = args[args.index("wrangler") + 1:]
def option(name):
    return w[w.index(name) + 1] if name in w else None
assert "CLOUDFLARE_API_TOKEN" in args and "CLOUDFLARE_ACCOUNT_ID" in args, "wrangler ran without the credential names"
config = json.loads((workdir / option("--config")).read_text())
env = option("--env")
section = config["env"][env] if env else config
name = option("--name") or section["name"]
assert name == section["name"], f"--name {name} is not the config's {section['name']}"
action = " ".join(w[:2])
log({"action": action, "name": name, "args": w})
worker = cloud[name]
fail = os.environ.get("FIXTURE_FAIL", "")
if f"{action}:{name}" in fail.split(","):
    sys.exit(1)
if action == "deployments status":
    if name in os.environ.get("FIXTURE_SPLIT", "").split():
        print(json.dumps({"versions": [{"version_id": worker["serving"], "percentage": 90},
                                       {"version_id": worker["latest"], "percentage": 10}]}))
    else:
        print(json.dumps({"versions": [{"version_id": worker["serving"], "percentage": 100}]}))
elif action == "versions view":
    print(json.dumps({"id": w[2], "resources": {"bindings": worker["versions"][w[2]]}}))
elif action == "versions upload":
    # keep_vars: the variables of the latest upload, then the config's own.
    bindings = [b for b in worker["versions"][worker["latest"]]
                if b["name"] not in os.environ.get("FIXTURE_DROP", "").split()]
    for key, value in (section.get("vars") or {}).items():
        bindings = [b for b in bindings if b["name"] != key] + [{"type": "plain_text", "name": key, "text": value}]
    version = str(uuid.uuid4())
    worker["versions"][version] = bindings
    worker["latest"] = version
    print(f"Uploaded {name}\nWorker Version ID: {version}")
elif action == "versions deploy":
    version = w[2].removesuffix("@100%")
    assert version in worker["versions"], version
    worker["serving"] = version
cloud_path.write_text(json.dumps(cloud))
'''

# Each URL answers with the version serving its hostname; FIXTURE_BROKEN hostnames answer 500 from
# any version uploaded in the test.
FAKE_CURL = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
root = pathlib.Path(os.environ["FIXTURE_ROOT"])
url = sys.argv[-1]
host, _, path = url.removeprefix("https://").partition("/")
with open(root / "curl.log", "a") as out:
    out.write(url + "\n")
cloud = json.loads((root / "cloud.json").read_text())
hosts = json.loads((root / "hosts.json").read_text())
serving = cloud[hosts[host]["worker"]]["serving"]
status = hosts[host]["paths"].get("/" + path, 404)
if host in os.environ.get("FIXTURE_BROKEN", "").split() and not serving.startswith("00000000-"):
    status = 500
print(f"HTTP/2 {status}\nfoundation-worker-version: {serving}\n")
'''

FAKE_SYSTEMCTL = r'''#!/usr/bin/env bash
printf '%s\n' "$*" >> "${FIXTURE_ROOT}/systemctl.log"
if [[ "$1" == show && "$*" == *EnvironmentFiles* ]]; then
  printf '%s (ignore_errors=no)\n%s (ignore_errors=yes)\n' "${FIXTURE_ROOT}/recovery.env" "${FIXTURE_ROOT}/monitor-${!#}.env"
fi
'''

FAKE_SYSTEMD_RUN = r'''#!/usr/bin/env bash
printf '%s\n' "$*" >> "${FIXTURE_ROOT}/monitor.log"
[[ "$*" != *"${FIXTURE_MONITOR_FAILS:-no such host}"* ]]
'''


class TheHostDeploysTheWorkers(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="worker-autodeploy-")
        self.addCleanup(temp.cleanup)
        root = self.root = pathlib.Path(temp.name)
        tree = self.tree = root / "control" / "releases" / COMMIT
        platform = tree / "platforms/foundation-platform"
        (platform / "config").mkdir(parents=True)
        for name in ("worker-deploys.contract.json", "r2-connections.contract.json"):
            shutil.copy(PLATFORM / "config" / name, platform / "config" / name)
        # The token's and the monitors' files live where the fixture's runtime-secrets contract says.
        secrets = json.loads((PLATFORM / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
        self.credential = root / "cloudflare-deploy.env"
        for group in secrets["groups"]:
            if group["name"] == "cloudflare-deploy":
                group["path"] = str(self.credential)
            elif group["name"].endswith("-serving-monitor"):
                group["path"] = str(root / f"{group['name']}.env")
                pathlib.Path(group["path"]).write_text("X=1\n")
        (platform / "config/runtime-secrets.contract.json").write_text(json.dumps(secrets))
        for entry in CONTRACT["workers"]:
            service = platform / entry["service_dir"]
            (service / "src").mkdir(parents=True, exist_ok=True)
            shutil.copy(PLATFORM / entry["service_dir"] / entry["wrangler_config"], service / entry["wrangler_config"])
            (service / "src/index.ts").write_text(f"// {entry['id']}\n")
        (tree / ".perfectory-control-commit").write_text(COMMIT + "\n")
        (root / "control/current").symlink_to(pathlib.Path("releases") / COMMIT)
        (root / "opt/releases" / COMMIT).mkdir(parents=True)
        (root / "opt/current").symlink_to(pathlib.Path("releases") / COMMIT)
        self.credential.write_text("CLOUDFLARE_API_TOKEN=x\nCLOUDFLARE_ACCOUNT_ID=y\n")
        self.credential.chmod(0o600)

        # The fake account: every listed Worker (and its preview) at one version with a CORS value
        # set in the dashboard, and a script no contract lists.
        cloud, hosts, self.names = {}, {}, {}
        for number, entry in enumerate(CONTRACT["workers"]):
            block = GATEWAYS[entry["gateway"]]
            names = [(block["worker_name"], block.get("public_hostname") or entry["smoke"]["hostname"])]
            if entry.get("preview"):
                preview = block["section_packs"]["preview_worker"]
                names.insert(0, (preview["worker_name"], preview["public_hostname"]))
            self.names[entry["id"]] = [name for name, _ in names]
            for index, (name, hostname) in enumerate(names):
                version = OLD.format(number * 10 + index)
                cloud[name] = {"serving": version, "latest": version, "versions": {version: [
                    {"type": "plain_text", "name": "FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS", "text": "https://app.example.test"},
                    {"type": "secret_text", "name": "A_SECRET"},
                    {"type": "r2_bucket", "name": "BUCKET", "bucket_name": "b"}]}}
                if entry["smoke"]["kind"] == "by-pnu":
                    paths = {block["request_path"]["capabilities"]: 200}
                else:
                    paths = {entry["smoke"]["path"]: entry["smoke"]["status"]}
                hosts[hostname] = {"worker": name, "paths": paths}
        cloud[UNLISTED] = {"serving": OLD.format(999), "latest": OLD.format(999), "versions": {OLD.format(999): []}}
        (root / "cloud.json").write_text(json.dumps(cloud))
        (root / "hosts.json").write_text(json.dumps(hosts))
        self.bin = root / "bin"
        self.bin.mkdir()
        for name, body in (("docker", FAKE_DOCKER), ("curl", FAKE_CURL), ("systemctl", FAKE_SYSTEMCTL),
                           ("systemd-run", FAKE_SYSTEMD_RUN), ("chown", "#!/bin/sh\nexit 0\n")):
            (self.bin / name).write_text(body)
            (self.bin / name).chmod(0o755)
        self.host = autodeploy.Host(
            control_root=root / "control/current", release_root=root / "opt", state=root / "state",
            off_switches=(root / "autodeploy.off", root / "worker-autodeploy.off"),
            credential_uid=os.getuid(), workspace_owner="0:0", poll_tries=2, poll_seconds=0)

    def tick(self, **env):
        saved = dict(os.environ)
        os.environ.update({"PATH": f"{self.bin}:{saved['PATH']}", "FIXTURE_ROOT": str(self.root),
                           "CLOUDFLARE_API_TOKEN": "x", "CLOUDFLARE_ACCOUNT_ID": "y", **env})
        self.output = io.StringIO()
        try:
            with contextlib.redirect_stdout(self.output):
                return autodeploy.run(self.host)
        finally:
            os.environ.clear()
            os.environ.update(saved)

    def calls(self):
        path = self.root / "calls.log"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def cloud(self):
        return json.loads((self.root / "cloud.json").read_text())

    def acted_on(self, action):
        return [call["name"] for call in self.calls() if call["action"] == action]

    def change(self, worker_id):
        entry = next(e for e in CONTRACT["workers"] if e["id"] == worker_id)
        path = self.tree / "platforms/foundation-platform" / entry["service_dir"] / "src/index.ts"
        path.write_text(path.read_text() + "// changed\n")

    # -- what is deployed --

    def test_the_first_run_deploys_every_listed_worker_and_nothing_else(self):
        self.assertEqual(self.tick(), 0)
        listed = {name for names in self.names.values() for name in names}
        self.assertEqual(set(self.acted_on("versions deploy")), listed)
        self.assertNotIn(UNLISTED, {call["name"] for call in self.calls() if "name" in call})
        cloud = self.cloud()
        self.assertEqual(cloud[UNLISTED]["serving"], OLD.format(999))
        for name in listed:
            self.assertFalse(cloud[name]["serving"].startswith("00000000-"), f"{name} was not deployed")
        record = json.loads((self.host.state / "workers/parcel-gateway.json").read_text())
        self.assertEqual(record["commit"], COMMIT)
        self.assertEqual(set(record["targets"]), {"preview", "production"})
        self.assertEqual(record["targets"]["production"]["to"], cloud["foundation-parcel-gateway"]["serving"])

    def test_nothing_changed_nothing_runs(self):
        self.assertEqual(self.tick(), 0)
        before = len(self.calls())
        self.assertEqual(self.tick(), 0)
        self.assertEqual(len(self.calls()), before)

    def test_only_the_worker_whose_sources_changed_is_deployed(self):
        self.assertEqual(self.tick(), 0)
        (self.root / "calls.log").unlink()
        self.change("tile-gateway")
        self.assertEqual(self.tick(), 0)
        self.assertEqual(self.acted_on("versions deploy"), ["foundation-tile-gateway"])

    def test_a_readme_edit_deploys_nothing(self):
        self.assertEqual(self.tick(), 0)
        entry = next(e for e in CONTRACT["workers"] if e["id"] == "tile-gateway")
        (self.tree / "platforms/foundation-platform" / entry["service_dir"] / "README.md").write_text("prose\n")
        before = len(self.calls())
        self.assertEqual(self.tick(), 0)
        self.assertEqual(len(self.calls()), before)

    def test_a_shared_input_deploys_every_worker_built_from_it(self):
        self.assertEqual(self.tick(), 0)
        (self.root / "calls.log").unlink()
        self.change("parcel-gateway")  # one source, two lanes (root ADR-0160)
        self.assertEqual(self.tick(), 0)
        self.assertEqual(sorted(self.acted_on("versions deploy")), sorted(
            self.names["parcel-gateway"] + self.names["building-gateway"]))

    def test_the_preview_goes_first_and_production_only_after_its_smoke(self):
        self.assertEqual(self.tick(), 0)
        preview, production = self.names["parcel-gateway"]
        steps = [(c["action"], c["name"]) for c in self.calls() if c.get("name") in (preview, production)]
        order = [step for step in steps if step[0] in ("versions upload", "versions deploy")]
        self.assertEqual(order, [("versions upload", preview), ("versions deploy", preview),
                                 ("versions upload", production), ("versions deploy", production)])
        monitors = (self.root / "monitor.log").read_text().splitlines()
        parcel = [line for line in monitors if line.endswith(" parcel")]
        self.assertEqual(len(parcel), 2)
        self.assertIn("MONITOR_BASE_URL=https://catalog-preview.perfectory.io", parcel[0])
        self.assertIn(f"MONITOR_BASE_URL=https://{GATEWAYS['parcel_by_pnu_gateway']['public_hostname']}", parcel[1])

    def test_map_edit_applies_its_d1_migrations_before_it_uploads(self):
        self.assertEqual(self.tick(), 0)
        actions = [c["action"] for c in self.calls() if c.get("name") == "foundation-map-edit-gateway"]
        self.assertLess(actions.index("d1 migrations"), actions.index("versions upload"))

    # -- what a failure does --

    def test_a_production_smoke_that_fails_rolls_production_back(self):
        hostname = GATEWAYS["vector_tile_gateway"]["public_hostname"]
        self.assertEqual(self.tick(FIXTURE_BROKEN=hostname), 1)
        cloud = self.cloud()
        self.assertTrue(cloud["foundation-tile-gateway"]["serving"].startswith("00000000-"), "not rolled back")
        deploys = [c["args"][2] for c in self.calls()
                   if c["action"] == "versions deploy" and c["name"] == "foundation-tile-gateway"]
        self.assertEqual(len(deploys), 2)
        self.assertTrue(deploys[1].startswith("00000000-"), "the second move is not back to the old version")
        failed = json.loads((self.host.state / "failed/tile-gateway.json").read_text())
        self.assertEqual(failed["attempts"], 1)
        self.assertFalse((self.host.state / "workers/tile-gateway.json").exists())
        # The others were deployed all the same.
        self.assertTrue((self.host.state / "workers/profile-gateway.json").exists())

    def test_a_preview_smoke_that_fails_never_reaches_production(self):
        preview, production = self.names["building-gateway"]
        self.assertEqual(self.tick(FIXTURE_MONITOR_FAILS="buildings-preview.perfectory.io"), 1)
        self.assertNotIn(production, self.acted_on("versions upload"))
        self.assertTrue(self.cloud()[preview]["serving"].startswith("00000000-"), "the preview was not rolled back")

    def test_a_version_that_lost_a_variable_is_never_deployed(self):
        self.assertEqual(self.tick(FIXTURE_DROP="FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS"), 1)
        self.assertEqual(self.acted_on("versions deploy"), [], "a version without its CORS value served")
        for names in self.names.values():
            self.assertTrue(self.cloud()[names[0]]["serving"].startswith("00000000-"))

    def test_a_split_deployment_is_left_to_its_canary(self):
        production = self.names["parcel-gateway"][1]
        self.assertEqual(self.tick(FIXTURE_SPLIT=production), 1)
        self.assertNotIn("foundation-parcel-gateway", self.acted_on("versions upload"))
        self.assertNotIn("foundation-parcel-gateway-preview", self.acted_on("versions upload"))

    def test_a_failed_worker_is_retried_while_behind_then_waits_for_a_change(self):
        fail = {"FIXTURE_FAIL": "versions upload:foundation-profile-gateway"}
        allowed = CONTRACT["attempts_per_change"]
        for _ in range(allowed):
            self.assertEqual(self.tick(**fail), 1)
        self.assertEqual(self.tick(**fail), 0, "a worker it gave up on alerts again")
        self.assertEqual(self.acted_on("versions upload").count("foundation-profile-gateway"), allowed)
        self.change("profile-gateway")
        self.assertEqual(self.tick(), 0)
        self.assertTrue((self.host.state / "workers/profile-gateway.json").exists())
        self.assertFalse((self.host.state / "failed/profile-gateway.json").exists())

    # -- when it does nothing --

    def test_without_the_token_file_it_is_off(self):
        self.credential.unlink()
        self.assertEqual(self.tick(), 0)
        self.assertEqual(self.calls(), [])

    def test_a_token_file_missing_a_name_is_refused_before_anything_runs_and_reported_once(self):
        self.credential.write_text("CLOUDFLARE_API_TOKEN=x\n")
        self.assertEqual(self.tick(), 78)
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.tick(), 0, "the same refusal alerted twice")
        self.credential.write_text("CLOUDFLARE_API_TOKEN=x\nCLOUDFLARE_ACCOUNT_ID=y\n")
        self.assertEqual(self.tick(), 0)
        self.assertFalse((self.host.state / "refused").exists())

    def test_a_token_file_others_can_read_is_refused(self):
        self.credential.chmod(0o644)
        self.assertEqual(self.tick(), 78)
        self.assertEqual(self.calls(), [])

    def test_a_token_not_loaded_by_the_unit_is_refused(self):
        self.assertEqual(self.tick(CLOUDFLARE_ACCOUNT_ID=""), 78)
        self.assertEqual(self.calls(), [])

    def test_a_missing_monitor_file_is_refused_before_anything_runs(self):
        (self.root / "parcel-serving-monitor.env").unlink()
        self.assertEqual(self.tick(), 78)
        self.assertEqual(self.calls(), [])

    def test_the_off_switches_stop_everything(self):
        for switch in self.host.off_switches:
            with self.subTest(switch.name):
                switch.touch()
                self.assertEqual(self.tick(), 0)
                self.assertEqual(self.calls(), [])
                switch.unlink()

    def test_a_control_checkout_the_host_does_not_run_waits(self):
        (self.root / "opt/current").unlink()
        (self.root / "opt/releases" / ("b" * 40)).mkdir()
        (self.root / "opt/current").symlink_to(pathlib.Path("releases") / ("b" * 40))
        self.assertEqual(self.tick(), 0)
        self.assertEqual(self.calls(), [])

    def test_a_config_that_names_another_worker_is_refused_before_anything_runs(self):
        entry = next(e for e in CONTRACT["workers"] if e["id"] == "tile-gateway")
        path = self.tree / "platforms/foundation-platform" / entry["service_dir"] / entry["wrangler_config"]
        config = json.loads(path.read_text())
        config["name"] = UNLISTED
        path.write_text(json.dumps(config))
        self.assertEqual(self.tick(), 65)
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.cloud()[UNLISTED]["serving"], OLD.format(999))


class TheContract(unittest.TestCase):
    def test_every_wrangler_config_in_the_platform_is_listed(self):
        listed = {(e["service_dir"], e["wrangler_config"]) for e in CONTRACT["workers"]}
        found = {(path.parent.relative_to(PLATFORM).as_posix(), path.name)
                 for path in (PLATFORM / "services").glob("*/wrangler*.jsonc")}
        self.assertEqual(found, listed, "a Worker the host would never deploy, or an entry with no config")

    def test_a_workers_inputs_hold_every_file_its_code_reads_outside_its_directory(self):
        # Asked of the code: every relative import or URL that leaves the service directory.
        reference = re.compile(r"""(?:from\s+|new URL\(\s*|import\(\s*)["'](\.\.?/[^"']+)["']""")
        for entry in CONTRACT["workers"]:
            service = PLATFORM / entry["service_dir"]
            inputs = [PLATFORM / item for item in entry["inputs"]]
            for path in [*service.glob("src/**/*.ts"), *service.glob("scripts/*.mjs")]:
                for target in reference.findall(path.read_text(encoding="utf-8")):
                    resolved = (path.parent / target).resolve()
                    with self.subTest(entry=entry["id"], file=path.name, target=target):
                        self.assertTrue(any(resolved == item or item in resolved.parents for item in inputs),
                                        f"{target} is not under {entry['inputs']}")

    def test_its_container_is_counted_in_the_hosts_memory_budget(self):
        budget = json.loads((ROOT / "tools/host-memory-budget.contract.json").read_text(encoding="utf-8"))
        contracts = [entry["contract"] for entry in budget["one_shot_contracts"]]
        self.assertIn("platforms/foundation-platform/config/worker-deploys.contract.json", contracts)

    def test_the_wrangler_step_runs_the_pinned_node_image(self):
        images = json.loads((ROOT / "tools/technology-versions.contract.json").read_text(encoding="utf-8"))["container_images"]
        line = next(line for line in (ROOT / "tools/container-images.env").read_text().splitlines()
                    if line.startswith("WORKER_DEPLOY_IMAGE="))
        reference = line.split("=", 1)[1]
        name, digest = reference.split("@")
        self.assertEqual(images[name], digest)
        engines = json.loads((PLATFORM / "services/foundation-tile-gateway/package.json").read_text(encoding="utf-8"))["engines"]
        self.assertEqual(name, f"node:{engines['node']}-bookworm")


if __name__ == "__main__":
    unittest.main()
