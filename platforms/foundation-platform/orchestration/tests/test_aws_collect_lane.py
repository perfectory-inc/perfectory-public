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
import shlex
import sys
import tarfile
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster" / "aws"))
import collect_lane  # noqa: E402

CONTRACT = collect_lane.contract()
LANES = CONTRACT["collect"]["lanes"]
# The lanes that start at a beat (collectors); a load starts on its inputs (eager).
BEATS = {job_id: lane for job_id, lane in LANES.items() if not collect_lane.started_by_inputs(lane)}


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
                                 "writer": {"access_key_id": "k", "secret_access_key": "s", "session_token": "t",
                                            "expiration": "2026-11-27T09:00:00Z"},
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

    def collect(self, store, word="unchanged", exit_code=0, vendor=None, secrets=None, lane=None, store_factory=None):
        import os
        lane = lane or self.LANE
        env = {"FAKE_PREFIX": LANES[lane]["env_prefix"], "FAKE_WORD": word, "FAKE_EXIT": str(exit_code)}
        old = {key: os.environ.get(key) for key in env}
        os.environ.update(env)
        lines = []
        try:
            return collect_lane.collect(lane, "run-1", secrets or secrets_for(*LANES[lane]["secrets"]),
                                        vendor or Vendor(), lines.append, program=[sys.executable, str(self.fake)],
                                        store_factory=store_factory or (lambda credentials: store)), lines
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

    def test_a_lane_with_no_host_copy_runs_here_and_starts_from_nothing(self):
        lane = LANES["data_go_kr_rtms_collect"]
        self.assertEqual(lane["handover"], collect_lane.NONE)
        unpaused = {**CONTRACT, "collect": {**CONTRACT["collect"], "lanes": {
            **LANES, "data_go_kr_rtms_collect": {**lane, "paused": False}}}}
        self.assertIn("data_go_kr_rtms_collect", collect_lane.lanes_here(unpaused, collect_lane.jobs()))
        prefix = collect_lane.state_prefix(CONTRACT, "data_go_kr_rtms_collect")
        with tempfile.TemporaryDirectory() as into:
            self.assertEqual(collect_lane.restore(Store(), "b", prefix, pathlib.Path(into), collect_lane.NONE),
                             (None, "empty"))
        with self.assertRaises(collect_lane.NotReady), tempfile.TemporaryDirectory() as into:
            collect_lane.restore(Store(), "b", prefix, pathlib.Path(into), collect_lane.OVERLAP)
        self.assertTrue(collect_lane.description("data_go_kr_rtms_collect", lane))

    def test_the_rtms_lane_writes_its_contract_s_kinds_and_reads_the_legal_dong_table(self):
        rtms = json.loads((collect_lane.LAKEHOUSE_CONTRACTS / "data-go-kr-rtms.contract.json").read_text(encoding="utf-8"))
        lane = LANES["data_go_kr_rtms_collect"]
        self.assertEqual(sorted(lane["write_prefixes"]),
                         sorted(f"bronze/source=datagokr__{name}/" for name in rtms["kinds"]))
        self.assertEqual(lane["read_prefixes"], [rtms["districts"]["source_prefix"]])
        self.assertEqual(lane["secrets"], ["DATA_GO_KR_SERVICE_KEY"])

    def test_a_lane_with_read_prefixes_gets_the_read_keys_and_others_none(self):
        reader = {"access_key_id": "rk", "secret_access_key": "rs", "session_token": "rt"}
        self.assertEqual(collect_lane.reader_environment(reader)["FOUNDATION_PLATFORM_LAKEHOUSE_READER_SESSION_TOKEN"],
                         "rt")
        self.assertEqual(collect_lane.reader_environment(None), {})

    def test_a_run_longer_than_its_keys_saves_its_state_with_fresh_ones(self):
        # 2026-11-27 onbid ran 2 h 12 min on a 1 h key (W101c): the state goes back with keys asked after the run.
        store = Store({f"{self.prefix}host/20261127T000000Z.tar.gz": tarball({"state.json": b"host"})})
        vendor, used = Vendor(), []

        def store_factory(credentials):
            used.append(len(vendor.calls))
            return store

        self.collect(store, vendor=vendor, store_factory=store_factory)
        self.assertEqual(used, [1, 2], "the state is saved with the second vend, made after the run")
        self.assertEqual(len(store.puts), 1)

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


SALE = "industrial_land_sale_notice_kicox_collect"
DECLARATION = {"tables": ["silver.t"], "lanes": {SALE: {"select": ["silver.t"]}}}
CATALOG = {"name": "perfectory-lakekeeper-prod"}
CLIENT = {"client_id": "c", "client_secret": "s", "token_uri": "https://issuer/oauth2/token",
          "scope": "lakekeeper-prod/catalog", "catalog_uri": "http://catalog.internal:8181/catalog",
          "warehouse": "foundation-platform-lakehouse-prod"}

NOT_READY = """
import os
print("foundation-job-not-ready silver.t holds no snapshot yet: its load's first commit comes first")
print("foundation-job-outcome waiting")
"""


class ATableStateLane(unittest.TestCase):
    """W109e (the first Lakekeeper switch): a collector whose state is its table in the catalog."""

    def test_its_catalog_client_comes_from_its_own_slot_with_the_catalog_s_address(self):
        slot = f"{CATALOG['name']}/principal-{SALE}"
        env = collect_lane.catalog_client(Secrets({slot: json.dumps(CLIENT)}), SALE, DECLARATION, CATALOG)
        self.assertEqual(env["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER"], "iceberg_rest")
        self.assertEqual(env["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"], CLIENT["catalog_uri"])
        self.assertEqual(env["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_SCOPE"], CLIENT["scope"])
        # A lane the declaration grants nothing holds no client, so the publisher stays on its own catalog.
        self.assertEqual(collect_lane.catalog_client(Secrets({}), "pension_workplace_collect", DECLARATION, CATALOG), {})
        # A slot that lacks a field (an apply that has not written it) asks nothing and names it.
        with self.assertRaises(collect_lane.NotReady):
            collect_lane.catalog_client(Secrets({slot: json.dumps({"client_id": "c"})}), SALE, DECLARATION, CATALOG)

    def test_the_generated_declaration_grants_the_sale_notice_lanes_as_decided(self):
        # Coordinator 2026-11-28: collectors select on fetch_attempt, the load writes both tables.
        declared = collect_lane.lakekeeper_declaration()
        attempt = "silver.industrial_land_sale_notice_fetch_attempt"
        both = sorted([attempt, "silver.industrial_land_sale_notice_attachment"])
        self.assertEqual(declared["tables"], both)
        for source in ("factoryon", "ilis", "kicox"):
            self.assertEqual(declared["lanes"][f"industrial_land_sale_notice_{source}_collect"], {"select": [attempt]})
        self.assertEqual(declared["lanes"]["industrial_land_sale_notice_silver"], {"write": both})
        for job_id in declared["lanes"]:
            self.assertEqual(LANES[job_id].get("state"), collect_lane.TABLE_STATE, job_id)

    def test_it_keeps_no_state_object_and_a_command_that_waits_asks_nothing(self):
        with tempfile.TemporaryDirectory() as scratch:
            fake = pathlib.Path(scratch) / "fake.py"
            fake.write_text(NOT_READY, encoding="utf-8")
            store = Store()
            old = collect_lane.catalog_client
            collect_lane.catalog_client = lambda client, job_id, *rest: {}
            try:
                with self.assertRaises(collect_lane.NotReady) as raised:
                    collect_lane.collect(SALE, "run-1", secrets_for(), Vendor(), [].append,
                                         program=[sys.executable, str(fake)], store_factory=lambda credentials: store)
            finally:
                collect_lane.catalog_client = old
            self.assertIn("silver.t holds no snapshot yet", str(raised.exception))
            self.assertEqual(store.puts, [], "no state object is read or written for a table-state lane")
        self.assertIsNone(collect_lane.not_ready(["foundation-job-outcome changed"]))


FAKE_IMPORT = """
import os, sys
with open(SEEN, "a", encoding="utf-8") as seen:
    seen.write(" ".join([sys.argv[1], os.environ["FOUNDATION_PLATFORM_LEDGER_IMPORT_OBJECT"],
                         os.environ["FOUNDATION_PLATFORM_LAKEHOUSE_READER_SESSION_TOKEN"]]) + chr(10))
"""


class TheLedgerImport(unittest.TestCase):
    def test_every_export_is_imported_once_with_the_reader_keys_and_the_ledger(self):
        exports = {f"{collect_lane.LEDGER_EXPORTS}2026-11-28-a__b.jsonl.gz": b"x",
                   f"{collect_lane.LEDGER_EXPORTS}2026-11-28-c__d.jsonl.gz": b"y",
                   f"{collect_lane.LEDGER_EXPORTS}2026-11-28-report.json": b"z"}
        reader = {"access_key_id": "rk", "secret_access_key": "rs", "session_token": "rt"}
        vendor = Vendor({**Vendor().answer, "reader": reader})
        with tempfile.TemporaryDirectory() as scratch:
            seen = pathlib.Path(scratch) / "seen.txt"
            fake = pathlib.Path(scratch) / "fake.py"
            fake.write_text(FAKE_IMPORT.replace("SEEN", repr(str(seen))), encoding="utf-8")
            imported = collect_lane.import_ledger("run-1", secrets_for(), vendor, [].append,
                                                  program=[sys.executable, str(fake)],
                                                  store_factory=lambda credentials: Store(exports))
            lines = seen.read_text(encoding="utf-8").splitlines()
        self.assertEqual(imported, sorted(key for key in exports if key.endswith(".jsonl.gz")))
        self.assertEqual([line.split()[1] for line in lines], imported)
        self.assertTrue(all(line.startswith("import-bronze-ledger-rows ") and line.endswith(" rt") for line in lines))
        self.assertEqual(vendor.calls[0][0].rsplit(":", 1)[1], collect_lane.LEDGER_IMPORT)


class TheLaneAsDeclared(unittest.TestCase):
    def test_a_handoff_lane_runs_here_only_while_its_job_is_off_on_the_host(self):
        jobs = collect_lane.jobs()
        hosted = collect_lane.on_host(jobs)
        here = collect_lane.lanes_here(CONTRACT, jobs)
        for job_id, lane in LANES.items():
            if lane.get("paused"):
                self.assertNotIn(job_id, here, job_id)
            elif lane["handover"] == collect_lane.OVERLAP:
                self.assertIn(job_id, here)
            else:
                self.assertEqual(job_id in here, job_id not in hosted, job_id)
        switched = json.loads(json.dumps(jobs))
        for entry in switched["jobs"]:
            if entry["id"] in LANES and LANES[entry["id"]]["handover"] == collect_lane.HANDOFF:
                entry["enabled"] = False
        paused = {job_id for job_id, lane in LANES.items() if lane.get("paused")}
        self.assertEqual(set(collect_lane.lanes_here(CONTRACT, switched)), set(LANES) - paused)

    def test_a_paused_lane_runs_nowhere_and_says_why(self):
        """Root ADR-0626 §8 N2: the Bronze epoch pauses onbid's handoff lane while its host job stays off."""
        for job_id, lane in LANES.items():
            self.assertIs(type(lane.get("paused", False)), bool, job_id)
            self.assertEqual(bool(lane.get("paused")), bool(lane.get("paused_reason")), job_id)
        jobs = collect_lane.jobs()
        platform = json.loads(json.dumps(CONTRACT))
        first = sorted(platform["collect"]["lanes"])[0]
        platform["collect"]["lanes"][first]["paused"] = True
        self.assertNotIn(first, collect_lane.lanes_here(platform, jobs))

    def test_every_lane_s_beat_resolves_in_its_source_contract(self):
        for job_id, lane in BEATS.items():
            cron, timezone = collect_lane.beat(lane)
            self.assertEqual(len(cron.split()), 5, job_id)
            self.assertEqual(timezone, "UTC", job_id)

    def test_every_beat_is_daily_onbid_s_with_the_loss_the_owner_accepted(self):
        for job_id, lane in BEATS.items():
            minute, hour, *rest = collect_lane.beat(lane)[0].split()
            self.assertTrue(minute.isdigit() and hour.isdigit() and rest == ["*", "*", "*"], job_id)
        onbid = json.loads((collect_lane.LAKEHOUSE_CONTRACTS / "data-go-kr-onbid.contract.json").read_text(encoding="utf-8"))
        self.assertIn("12 of 2,227 rounds (0.54 %)", onbid["beat"]["reason"])
        self.assertIn("owner decision 2026-11-27", onbid["beat"]["reason"])

    def test_rt_molit_runs_hourly_on_the_host_because_its_provider_drops_aws(self):
        rt = json.loads((collect_lane.LAKEHOUSE_CONTRACTS / "rt-molit-real-transaction.contract.json").read_text(encoding="utf-8"))
        self.assertEqual(rt["manners"]["max_downloads_per_run"], 30, "a run fits the host unit's 30 minutes")
        self.assertIn("drops TCP connections from AWS addresses", rt["beat"]["reason"])
        self.assertIn("rt_molit_real_transaction_collect", collect_lane.on_host(collect_lane.jobs()))

    def test_the_run_tags_are_the_lane_s_role_and_size_and_no_time_number(self):
        for job_id, lane in LANES.items():
            tags = collect_lane.run_tags(CONTRACT, job_id, lane)
            size = CONTRACT["collect"]["size_classes"][lane["size"]]
            self.assertEqual((tags["ecs/cpu"], tags["ecs/memory"]), (str(size["cpu"]), str(size["memory"])))
            role = json.loads(tags["ecs/task_overrides"])["taskRoleArn"]
            self.assertIn(f":role{CONTRACT['ecs']['run_role_path']}{CONTRACT['tags']['project']}-{lane['role']}", role)
            # The run queue limits collectors and loads apart (dagster.yaml): a lane's kind is its job's group.
            expected = "silver" if job_id == "industrial_land_sale_notice_silver" else "collect"
            self.assertEqual(tags[collect_lane.KIND_TAG], expected, job_id)
            self.assertFalse([tag for tag in tags if "runtime" in tag or "timeout" in tag], job_id)

    def test_a_load_starts_on_the_lanes_that_produce_its_inputs_and_has_no_beat(self):
        # ADR-0623 section 1-2: Silver and Gold are eager(); the producers are read from the graph, never listed.
        lane = LANES["industrial_land_sale_notice_silver"]
        self.assertTrue(collect_lane.started_by_inputs(lane))
        self.assertNotIn("beat", lane)
        self.assertEqual(collect_lane.producers("industrial_land_sale_notice_silver"),
                         ["industrial_land_sale_notice_factoryon_collect", "industrial_land_sale_notice_ilis_collect",
                          "industrial_land_sale_notice_kicox_collect"])

    def test_every_lane_refreshes_its_keys_through_the_sdk_whatever_its_run_time(self):
        # One platform time limit for every lane (root ADR-0623 §1): the publisher asks the vendor again before the
        # keys expire, so no lane's run time can outlive them (the SDK side: r2/credential_tests.rs).
        work = pathlib.Path(tempfile.gettempdir()) / "w"
        keys = {name: "k" for name in CONTRACT["collect"]["source_secrets"]}
        for job_id, lane in LANES.items():
            command = collect_lane.credential_process(job_id, "run-1")
            env = collect_lane.environment(lane, Vendor().answer, "postgres://x", keys, work, command)
            self.assertEqual(env[collect_lane.CREDENTIAL_PROCESS_ENV], command, job_id)
            self.assertEqual(shlex.split(command)[-3:], [collect_lane.CREDENTIAL_PROCESS, job_id, "run-1"])
        self.assertTrue(collect_lane.CREDENTIAL_PROCESS_ENV.startswith("FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_"))

    def test_the_command_prints_the_writer_keys_as_credential_process_json(self):
        self.assertEqual(collect_lane.credential_process_answer(Vendor().answer),
                         {"Version": 1, "AccessKeyId": "k", "SecretAccessKey": "s", "SessionToken": "t",
                          "Expiration": "2026-11-27T09:00:00Z"})

    def test_the_environment_carries_the_session_token_and_no_host_path(self):
        lane = LANES["onbid_public_auction_collect"]
        work = pathlib.Path(tempfile.gettempdir()) / "w"
        env = collect_lane.environment(lane, Vendor().answer, "postgres://x", {"DATA_GO_KR_SERVICE_KEY": "k"}, work,
                                       "refresh")
        self.assertEqual(env["FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SESSION_TOKEN"], "t")
        self.assertEqual(env[f"{lane['env_prefix']}_LIVE_WRITE"], "1")
        self.assertEqual(env["FOUNDATION_PLATFORM_RUN_USAGE"], "1", "measured as the host copy is measured")
        self.assertEqual(env[f"{lane['env_prefix']}_METRICS_DIR"], str(work / "metrics_dir"))
        self.assertFalse([value for value in env.values() if value.startswith(("/var/lib", "/etc/foundation", "/data/"))])


if __name__ == "__main__":
    unittest.main()
