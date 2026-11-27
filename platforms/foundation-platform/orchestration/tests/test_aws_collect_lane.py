"""A collector on AWS runs as its lane says, from its state, and asks nothing when it is not ready
(root ADR-0616 A3 and §7-§8, ADR-0623; aws/collect_lane.py).

With the vendor, Secrets Manager, R2 and the publisher replaced:
- which lanes run here: every overlap lane, a handoff lane only while its job is off on the host (a provider
  quota is never spent by two copies);
- a lane's beat is the one its source contract declares, and every lane's pointer resolves;
- a run's tags carry the lane's role and size class and no time number (ADR-0623 §3);
- a run starts from the newest aws/ state, else the newest host/ state, and with neither asks nothing;
- the state goes back as a new object after every run that started its command, a failed one too, and never
  over an earlier key;
- the vendor inert or a source key not placed is NotReady and nothing is asked;
- the outcome is the command's last outcome line, as on the host.
"""

import io
import json
import pathlib
import sys
import tarfile
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster" / "aws"))
import collect_lane  # noqa: E402

CONTRACT = collect_lane.contract()
LANES = CONTRACT["collect"]["lanes"]


class Store:
    """The bucket as a dict, with the calls collect_lane makes."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.puts = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        store = self

        class Pages:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                yield {"Contents": [{"Key": key} for key in sorted(store.objects) if key.startswith(Prefix)]}

        return Pages()

    def get_object(self, Bucket, Key):  # noqa: N803
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType, IfNoneMatch):  # noqa: N803
        assert IfNoneMatch == "*" and Key not in self.objects, "a state object is never overwritten"
        self.objects[Key] = Body
        self.puts.append(Key)


def tarball(files):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def files_of(blob):
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as archive:
        return {member.name: archive.extractfile(member).read() for member in archive.getmembers() if member.isfile()}


class Vendor:
    def __init__(self, answer=None):
        self.answer = answer or {"account_id": "synthetic", "bucket": "foundation-platform-lakehouse-prod",
                                 "expiration": "2026-11-27T09:00:00Z",
                                 "writer": {"access_key_id": "k", "secret_access_key": "s", "session_token": "t"},
                                 "reader": None}
        self.calls = []

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803
        self.calls.append((FunctionName, json.loads(Payload)))
        return {"Payload": io.BytesIO(json.dumps(self.answer).encode())}


class Secrets:
    class exceptions:  # noqa: N801
        ResourceNotFoundException = KeyError

    def __init__(self, values):
        self.values = values

    def get_secret_value(self, SecretId):  # noqa: N803
        return {"SecretString": self.values[SecretId]}


def secrets_for(*placed):
    runtime = CONTRACT["database"]["foundation"]["runtime_secret"]
    values = {runtime: json.dumps({"username": "u", "password": "p@ss", "host": "h", "port": 5432, "dbname": "foundation"})}
    for name in placed:
        values[f"{CONTRACT['tags']['project']}/source/{CONTRACT['collect']['source_secrets'][name]}"] = "a-key"
    return Secrets(values)


FAKE = """
import json, os, pathlib, sys
prefix = os.environ["FAKE_PREFIX"]
state = pathlib.Path(os.environ[prefix + "_STATE_DIR"])
seen = (state / "state.json").read_text() if (state / "state.json").exists() else ""
(state / "state.json").write_text(json.dumps({"seen": seen, "run": os.environ.get("FAKE_WORD")}))
print("asked the source")
print("foundation-job-outcome " + os.environ["FAKE_WORD"])
sys.exit(int(os.environ.get("FAKE_EXIT", "0")))
"""


class Run(unittest.TestCase):
    LANE = "pension_workplace_collect"

    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.fake = pathlib.Path(self.scratch.name) / "fake_publisher.py"
        self.fake.write_text(FAKE, encoding="utf-8")
        self.prefix = collect_lane.state_prefix(CONTRACT, self.LANE)

    def tearDown(self):
        self.scratch.cleanup()

    def collect(self, store, word="unchanged", exit_code=0, vendor=None, secrets=None, lane=None):
        import os
        lane = lane or self.LANE
        env = {"FAKE_PREFIX": LANES[lane]["env_prefix"], "FAKE_WORD": word, "FAKE_EXIT": str(exit_code)}
        old = {key: os.environ.get(key) for key in env}
        os.environ.update(env)
        lines = []
        try:
            return collect_lane.collect(lane, "run-1", secrets or secrets_for(*LANES[lane]["secrets"]),
                                        vendor or Vendor(), lines.append, program=[sys.executable, str(self.fake)],
                                        store_factory=lambda credentials: store), lines
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_with_no_state_at_all_it_asks_nothing(self):
        store = Store()
        with self.assertRaises(collect_lane.NotReady):
            self.collect(store)
        self.assertEqual(store.puts, [])

    def test_the_first_run_starts_from_the_newest_host_state_and_writes_its_own(self):
        store = Store({f"{self.prefix}host/20261126T000000Z.tar.gz": tarball({"state.json": b"old"}),
                       f"{self.prefix}host/20261127T000000Z.tar.gz": tarball({"state.json": b"newest"})})
        result, lines = self.collect(store)
        self.assertEqual(result["state_read"], f"{self.prefix}host/20261127T000000Z.tar.gz")
        self.assertIn("asked the source", lines)
        (written,) = store.puts
        self.assertTrue(written.startswith(f"{self.prefix}aws/") and written.endswith("-run-1.tar.gz"))
        self.assertEqual(json.loads(files_of(store.objects[written])["state.json"])["seen"], "newest")

    def test_its_own_state_wins_over_the_host_s(self):
        store = Store({f"{self.prefix}host/20261127T090000Z.tar.gz": tarball({"state.json": b"host"}),
                       f"{self.prefix}aws/20261127T000000Z-r0.tar.gz": tarball({"state.json": b"aws"})})
        result, _ = self.collect(store)
        self.assertEqual(result["state_read"], f"{self.prefix}aws/20261127T000000Z-r0.tar.gz")

    def test_a_failed_command_still_saves_its_state_then_fails(self):
        store = Store({f"{self.prefix}aws/20261127T000000Z-r0.tar.gz": tarball({"state.json": b"aws"})})
        with self.assertRaises(collect_lane.RunFailed):
            self.collect(store, word="failed", exit_code=1)
        self.assertEqual(len(store.puts), 1)

    def test_the_outcome_is_the_last_outcome_line(self):
        store = Store({f"{self.prefix}aws/20261127T000000Z-r0.tar.gz": tarball({"state.json": b"aws"})})
        result, _ = self.collect(store, word="changed")
        self.assertEqual(result["outcome"], "changed")
        self.assertEqual(collect_lane.outcome(["x"]), "changed", "no outcome line counts as changed, as on the host")

    def test_inert_vendor_or_an_unplaced_key_asks_nothing(self):
        store = Store({f"{self.prefix}host/20261127T000000Z.tar.gz": tarball({"state.json": b"host"})})
        with self.assertRaises(collect_lane.NotReady):
            self.collect(store, vendor=Vendor({"inert": "r2-mint-token"}))
        lane = "juso_electronic_map"
        prefix = collect_lane.state_prefix(CONTRACT, lane)
        store = Store({f"{prefix}host/20261127T000000Z.tar.gz": tarball({"cursor.json": b"{}"})})
        with self.assertRaises(collect_lane.NotReady):
            self.collect(store, secrets=secrets_for(), lane=lane)
        self.assertEqual(store.puts, [])

    def test_a_vendor_refusal_fails_the_run(self):
        with self.assertRaises(collect_lane.RunFailed):
            self.collect(Store(), vendor=Vendor({"refused": "no write prefix"}))


class TheLaneAsDeclared(unittest.TestCase):
    def test_a_handoff_lane_runs_here_only_while_its_job_is_off_on_the_host(self):
        jobs = collect_lane.jobs()
        hosted = collect_lane.on_host(jobs)
        here = collect_lane.lanes_here(CONTRACT, jobs)
        for job_id, lane in LANES.items():
            if lane["handover"] == collect_lane.OVERLAP:
                self.assertIn(job_id, here)
            else:
                self.assertEqual(job_id in here, job_id not in hosted, job_id)
        switched = json.loads(json.dumps(jobs))
        for entry in switched["jobs"]:
            if entry["id"] in LANES and LANES[entry["id"]]["handover"] == collect_lane.HANDOFF:
                entry["enabled"] = False
        self.assertEqual(set(collect_lane.lanes_here(CONTRACT, switched)), set(LANES))

    def test_every_lane_s_beat_resolves_in_its_source_contract(self):
        for job_id, lane in LANES.items():
            cron, timezone = collect_lane.beat(lane)
            self.assertEqual(len(cron.split()), 5, job_id)
            self.assertEqual(timezone, "UTC", job_id)

    def test_every_beat_is_daily_onbid_s_with_the_loss_the_owner_accepted(self):
        for job_id, lane in LANES.items():
            minute, hour, *rest = collect_lane.beat(lane)[0].split()
            self.assertTrue(minute.isdigit() and hour.isdigit() and rest == ["*", "*", "*"], job_id)
        onbid = json.loads((collect_lane.LAKEHOUSE_CONTRACTS / "data-go-kr-onbid.contract.json").read_text(encoding="utf-8"))
        self.assertIn("12 of 2,227 rounds (0.54 %)", onbid["beat"]["reason"])
        self.assertIn("owner decision 2026-11-27", onbid["beat"]["reason"])

    def test_rt_molit_spends_the_day_s_budget_in_its_one_daily_run(self):
        rt = json.loads((collect_lane.LAKEHOUSE_CONTRACTS / "rt-molit-real-transaction.contract.json").read_text(encoding="utf-8"))
        self.assertEqual(rt["manners"]["max_downloads_per_run"], rt["manners"]["daily_download_limit"])

    def test_the_run_tags_are_the_lane_s_role_and_size_and_no_time_number(self):
        for job_id, lane in LANES.items():
            tags = collect_lane.run_tags(CONTRACT, job_id, lane)
            size = CONTRACT["collect"]["size_classes"][lane["size"]]
            self.assertEqual((tags["ecs/cpu"], tags["ecs/memory"]), (str(size["cpu"]), str(size["memory"])))
            role = json.loads(tags["ecs/task_overrides"])["taskRoleArn"]
            self.assertIn(f":role{CONTRACT['ecs']['run_role_path']}{CONTRACT['tags']['project']}-{lane['role']}", role)
            self.assertEqual(tags[collect_lane.KIND_TAG], "collect")
            self.assertFalse([tag for tag in tags if "runtime" in tag or "timeout" in tag], job_id)

    def test_the_environment_carries_the_session_token_and_no_host_path(self):
        lane = LANES["onbid_public_auction_collect"]
        work = pathlib.Path(tempfile.gettempdir()) / "w"
        env = collect_lane.environment(lane, Vendor().answer, "postgres://x", {"DATA_GO_KR_SERVICE_KEY": "k"}, work)
        self.assertEqual(env["FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SESSION_TOKEN"], "t")
        self.assertEqual(env[f"{lane['env_prefix']}_LIVE_WRITE"], "1")
        self.assertEqual(env[f"{lane['env_prefix']}_METRICS_DIR"], str(work / "metrics_dir"))
        self.assertFalse([value for value in env.values() if value.startswith(("/var/lib", "/etc/foundation", "/data/"))])


if __name__ == "__main__":
    unittest.main()
