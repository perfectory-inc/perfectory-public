"""The deploy and an operator's run-now reach a job Dagster decides through Dagster (root ADR-0565).

scripts/deploy/job_orchestrator.py stops and starts the job's sensor and launches its Dagster job, where
`airflow dags pause|unpause|trigger` did the same to a job's DAG. The first class holds its rules with a
stand-in for Dagster's GraphQL; the second runs the same requests against a real Dagster instance and
code location (the CI `dagster-observe` lane, where a skip fails the lane), so a request Dagster's schema
does not take cannot pass here.
"""

import hashlib
import importlib.util
import io
import os
import socket
import threading
import json
import pathlib
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "scripts" / "deploy"))

import dagster_writes  # noqa: E402
import job_orchestrator  # noqa: E402
import job_specs  # noqa: E402

HAS_DAGSTER = importlib.util.find_spec("dagster_graphql") is not None
NEEDS_DAGSTER = "requires dagster-graphql (the CI dagster-observe lane runs it)"
REQUEST = "11111111-1111-4111-8111-111111111111"


def listed():
    return {
        "observers": {"dagster": {"enabled": True, "ui": "http://127.0.0.1:19085"}},
        "priority_classes": {"default": {"value": 100}, "publish": {"value": 300}},
        "pools": {"spark": {"slots": 3, "description": "d"}},
        "jobs": [
            {"id": "nightly", "enabled": True, "dagster_stage": "dagster", "pool": "spark", "priority_class": "publish",
             "wait_limit_minutes": 10, "time_limit_minutes": 40, "stop_grace_minutes": 5},
            {"id": "hourly", "enabled": True, "dagster_stage": "dagster", "pool": "default_pool"},
            {"id": "proxied", "enabled": True, "dagster_stage": "proxied", "pool": "default_pool"},
            {"id": "old", "enabled": True, "pool": "default_pool"},
            {"id": "off", "enabled": False, "pool": "default_pool"},
        ],
    }


def state_id(name):
    """A sensor state's id as Dagster makes one: a hex digest."""
    return hashlib.sha1(name.encode()).hexdigest()


class Dagster:
    """Dagster's GraphQL as job_orchestrator asks it: sensor name -> status, and what it was told. Every
    request is first one the writable webserver's gate lets the host send (dagster_writes.HOST)."""

    def __init__(self, sensors, waiting=None):
        # `waiting`: Dagster job name -> the id of its run that has not started, or an answer to give as is.
        self.sensors, self.told, self.waiting = dict(sensors), [], dict(waiting or {})

    def __call__(self, query, variables):
        body = json.dumps({"query": query, "variables": variables}).encode()
        reason = dagster_writes.refused(dagster_writes.HOST, "POST", "/graphql", body)
        assert reason is None, f"the gate refuses it: {reason}"
        if "sensorsOrError" in query:
            return {"sensorsOrError": {"__typename": "Sensors", "results": [
                {"name": name, "sensorState": {"id": state_id(name), "status": status}}
                for name, status in self.sensors.items()]}}
        if "stopSensor" in query:
            name = next(name for name in self.sensors if state_id(name) == variables["id"])
            self.told.append(("stop", name))
            self.sensors[name] = "STOPPED"
            return {"stopSensor": {"__typename": "StopSensorMutationResult"}}
        if "startSensor" in query:
            name = variables["sensor"]["sensorName"]
            self.told.append(("start", name))
            self.sensors[name] = "RUNNING"
            return {"startSensor": {"__typename": "Sensor"}}
        if "runsOrError" in query:
            found = self.waiting.get(variables["job"])
            if isinstance(found, dict):
                return {"runsOrError": found}
            return {"runsOrError": {"__typename": "Runs", "results": [{"runId": found}] if found else []}}
        self.told.append(("launch", variables["params"]))
        return {"launchRun": {"__typename": "LaunchRunSuccess", "run": {"runId": "run-1"}}}


class TheJobsDagsterDecides(unittest.TestCase):
    def dagster(self, **status):
        return Dagster({"foundation_decide__nightly": "RUNNING", "foundation_decide__hourly": "RUNNING", **status})

    def test_only_an_enabled_job_at_dagster_is_dagsters(self):
        self.assertEqual(job_orchestrator.decided(listed()), ["nightly", "hourly"])

    def test_the_names_are_the_ones_the_code_location_makes(self):
        # decide.py and run_jobs.py name the sensor and the job by the same job_specs constants.
        self.assertEqual(job_orchestrator.REPOSITORY, {"repositoryLocationName": job_specs.DAGSTER_LOCATION,
                                                       "repositoryName": job_specs.DAGSTER_REPOSITORY})
        decide = (PLATFORM / "orchestration/dagster/decide.py").read_text(encoding="utf-8")
        self.assertIn("SENSOR_PREFIX = job_specs.DAGSTER_SENSOR_PREFIX", decide)

    def test_pause_stops_the_sensor_and_a_second_pause_does_nothing(self):
        dagster = self.dagster()
        self.assertEqual(job_orchestrator.pause(listed(), ["nightly"], dagster), ["nightly"])
        self.assertEqual(dagster.told, [("stop", "foundation_decide__nightly")])
        self.assertEqual(job_orchestrator.pause(listed(), ["nightly"], dagster), [])
        self.assertEqual(len(dagster.told), 1)
        self.assertEqual(dagster.sensors["foundation_decide__hourly"], "RUNNING", "no other job is held")

    def test_unpause_starts_only_the_stopped_ones(self):
        dagster = self.dagster(foundation_decide__nightly="STOPPED")
        self.assertEqual(job_orchestrator.unpause(listed(), ["nightly", "hourly"], dagster), ["nightly"])
        self.assertEqual(dagster.told, [("start", "foundation_decide__nightly")])

    def test_a_sensor_dagster_does_not_hold_is_an_error_when_releasing_and_nothing_when_holding(self):
        # The code location has not loaded the release that moved the job: nothing decides it, so a
        # deploy that "unpaused" it would leave it silently undecided. Holding it has nothing to stop.
        dagster = Dagster({"foundation_decide__hourly": "STOPPED"})
        with self.assertRaises(job_orchestrator.Unanswered) as raised:
            job_orchestrator.unpause(listed(), ["nightly", "hourly"], dagster)
        self.assertIn("foundation_decide__nightly", str(raised.exception))
        self.assertEqual(dagster.sensors["foundation_decide__hourly"], "RUNNING", "the one it holds is started first")
        self.assertEqual(job_orchestrator.pause(listed(), ["nightly"], dagster), [])

    def test_a_job_airflow_decides_is_refused(self):
        for job_id in ("proxied", "old", "off", "nowhere"):
            with self.subTest(job_id), self.assertRaises(job_orchestrator.Refused):
                job_orchestrator.pause(listed(), [job_id], self.dagster())
        with self.assertRaises(job_orchestrator.Refused):
            job_orchestrator.start(listed(), "old", "triggered", self.dagster(), {})

    def test_start_launches_the_jobs_dagster_job_with_the_tags_every_run_of_it_carries(self):
        dagster = self.dagster()
        self.assertEqual(job_orchestrator.start(listed(), "nightly", f"requested {REQUEST}", dagster, job_specs.run_tags(300, f"requested {REQUEST}", 60 * 55)), "run-1")
        ((_, params),) = dagster.told
        self.assertEqual(params["selector"], {**job_orchestrator.REPOSITORY, "pipelineName": "foundation_job__nightly"})
        tags = {tag["key"]: tag["value"] for tag in params["executionMetadata"]["tags"]}
        self.assertEqual(tags, job_specs.run_tags(300, f"requested {REQUEST}", 60 * 55))
        self.assertEqual(tags["foundation/run-kind"], f"requested {REQUEST}")

    def test_a_start_carries_the_tags_its_sensor_runs_carry(self):
        # Root ADR-0605: the class and the queue weight, and the decided-by tag, read from the real list.
        jobs = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))
        spec = next(spec for spec in job_specs.load_specs(jobs) if spec.job_id == "gold_panel_rebuild")
        tags = job_orchestrator.launch_tags(jobs, "gold_panel_rebuild", "triggered")
        self.assertEqual(tags, spec.dagster_tags("triggered"))
        self.assertEqual(tags[job_specs.PRIORITY_TAG], str(spec.priority * job_specs.QUEUE_WEIGHT_SPAN + spec.priority_weight))
        self.assertEqual(tags[job_specs.DECIDED_TAG], "dagster")

    def test_a_deploys_start_joins_the_run_that_waits_to_start_and_an_operators_is_always_launched(self):
        # 2026-11-23: the spark pool's one slot was taken for hours, a run the deploy owes stays recorded
        # until its unit starts, and eleven deploys launched the same eight Gold rebuilds again (74 of 93
        # queued runs were copies).
        real = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))
        dagster = Dagster({}, waiting={"foundation_job__gold_panel_rebuild": "run-0"})
        status, out, _ = self.run_main("start", "gold_panel_rebuild", dagster=dagster, listing=real)
        self.assertEqual((status, out), (0, "job-orchestrator: gold_panel_rebuild already waits to start as Dagster "
                                            "run run-0; not started again\n"))
        self.assertEqual(dagster.told, [], "nothing is launched")
        status, out, _ = self.run_main("start", "gold_panel_rebuild", "requested", REQUEST, dagster=dagster, listing=real)
        self.assertEqual((status, out), (0, "job-orchestrator: started gold_panel_rebuild as Dagster run run-1\n"))
        self.assertEqual([what for what, _ in dagster.told], ["launch"])
        # Another job's waiting run holds nothing back.
        other = Dagster({}, waiting={"foundation_job__nightly": "run-0"})
        self.assertEqual(self.run_main("start", "gold_panel_rebuild", dagster=other, listing=real)[1],
                         "job-orchestrator: started gold_panel_rebuild as Dagster run run-1\n")

    def test_a_dagster_that_does_not_say_what_waits_still_gets_the_start(self):
        # A run the deploy owes is not dropped because the question failed: it is launched, as before.
        real = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))
        dagster = Dagster({}, waiting={"foundation_job__gold_panel_rebuild": {"__typename": "PythonError", "message": "busy"}})
        status, out, err = self.run_main("start", "gold_panel_rebuild", dagster=dagster, listing=real)
        self.assertEqual((status, out), (0, "job-orchestrator: started gold_panel_rebuild as Dagster run run-1\n"))
        self.assertIn("starting gold_panel_rebuild without knowing", err)
        self.assertIsNone(job_orchestrator.waiting("nightly", self.dagster()))

    def test_start_takes_only_a_kind_the_forced_command_reads(self):
        for kind in ("", "check", "triggered; reboot", "requested x"):
            with self.subTest(kind), self.assertRaises(job_orchestrator.Refused):
                job_orchestrator.start(listed(), "nightly", kind, self.dagster(), {})

    def test_an_answer_that_is_not_the_expected_one_fails(self):
        def refuses(query, variables):
            return {"sensorsOrError": {"__typename": "PythonError", "message": "the location is loading"}}
        with self.assertRaises(job_orchestrator.Unanswered) as raised:
            job_orchestrator.pause(listed(), ["nightly"], refuses)
        self.assertIn("the location is loading", str(raised.exception))

    @unittest.skipUnless(hasattr(socket, "AF_UNIX") and os.name == "posix", "a unix socket")
    def test_dagster_is_asked_over_the_writer_socket_and_nothing_answering_is_unanswered(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "writer.sock")
            with self.assertRaises(job_orchestrator.Unanswered):
                job_orchestrator.poster(path)("query { version }", {})
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            asked = []

            def answer():
                connection, _ = server.accept()
                with connection:
                    # The whole request before answering: closing on a partial read is a broken pipe to the client.
                    request = b""
                    while b"\r\n\r\n" not in request and (chunk := connection.recv(65536)):
                        request += chunk
                    head, _, body = request.partition(b"\r\n\r\n")
                    length = int(next(line.split(b":", 1)[1] for line in head.split(b"\r\n")
                                      if line.lower().startswith(b"content-length:")))
                    while len(body) < length and (chunk := connection.recv(65536)):
                        body += chunk
                    asked.append(request)
                    payload = json.dumps({"data": {"sensorsOrError": {"__typename": "Sensors", "results": []}}}).encode()
                    head = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n"
                            b"Connection: close\r\n\r\n")
                    connection.sendall(head % len(payload) + payload)

            thread = threading.Thread(target=answer)
            thread.start()
            self.assertEqual(job_orchestrator.sensor_states(job_orchestrator.poster(path)), {})
            thread.join()
            server.close()
            self.assertTrue(asked[0].startswith(b"POST /graphql HTTP/1.1"))

    def run_main(self, *argv, dagster=None, listing=None, health=None):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "jobs.v1.json"
            path.write_text(json.dumps(listing or listed()), encoding="utf-8")
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                status = job_orchestrator.main([argv[0], str(path), *argv[1:]], post=dagster or self.dagster(),
                                               health=health or (lambda jobs: self.fail("the daemon was asked")))
        return status, out.getvalue(), err.getvalue()

    def test_a_pause_that_finds_no_dagster_holds_and_says_so(self):
        # 2026-11-23: the daemon and both webservers restarted in a loop, and every deploy stopped at step 0
        # asking Dagster to stop its sensors, so the release that fixed it could not be deployed.
        def gone(*_):
            raise job_orchestrator.Unreachable("Dagster did not answer at /run/foundation-dagster/writer.sock: "
                                               "[Errno 2] No such file or directory")

        for health in (lambda jobs: None, lambda jobs: False):
            status, out, err = self.run_main("pause", "nightly", "hourly", dagster=gone, health=health)
            self.assertEqual((status, out), (0, ""))
            self.assertIn("HELD WITHOUT DAGSTER", err)
            self.assertIn("none of the 2 jobs it decides can start", err)
        # A sensor daemon still seen healthy may start them: they could not be held.
        status, _, err = self.run_main("pause", "nightly", dagster=gone, health=lambda jobs: True)
        self.assertEqual(status, 1)
        self.assertNotIn("HELD", err)
        # Releasing or starting needs Dagster; a Dagster that answers and refuses is not "no Dagster".
        self.assertEqual(self.run_main("unpause", "nightly", dagster=gone, health=lambda jobs: None)[0], 1)
        def refusing(*_):
            raise job_orchestrator.Unanswered("Dagster refused the request (500): []")

        self.assertEqual(self.run_main("pause", "nightly", dagster=refusing)[0], 1)

    def test_the_sensor_daemon_is_asked_of_the_read_only_webserver(self):
        healthy = {"data": {"instance": {"daemonHealth": {"allDaemonStatuses": [
            {"daemonType": "SCHEDULER", "healthy": True}, {"daemonType": "SENSOR", "healthy": True}]}}}}
        sick = {"data": {"instance": {"daemonHealth": {"allDaemonStatuses": [
            {"daemonType": "SENSOR", "healthy": False}]}}}}
        self.assertIs(job_orchestrator.sensor_daemon_healthy(listed(), ask=lambda query: healthy), True)
        self.assertIs(job_orchestrator.sensor_daemon_healthy(listed(), ask=lambda query: sick), False)

        def down(query):
            raise OSError("connection refused")

        self.assertIsNone(job_orchestrator.sensor_daemon_healthy(listed(), ask=down))
        self.assertIsNone(job_orchestrator.sensor_daemon_healthy({"jobs": []}), "no address: nothing to ask")

    def test_the_command_line_the_deploy_and_the_run_now_use(self):
        self.assertEqual(self.run_main("decided")[:2], (0, "nightly\nhourly\n"))
        dagster = self.dagster()
        status, out, _ = self.run_main("pause", "nightly", "hourly", dagster=dagster)
        self.assertEqual((status, out), (0, "job-orchestrator: paused 2 of 2 sensors: nightly hourly\n"))
        status, out, _ = self.run_main("unpause", "nightly", "hourly", dagster=dagster)
        self.assertEqual((status, out), (0, "job-orchestrator: unpaused 2 of 2 sensors: nightly hourly\n"))
        # Nothing named asks Dagster nothing: a release with no such job never needs it to answer.
        self.assertEqual(self.run_main("pause", dagster=lambda *_: self.fail("asked"))[0], 0)
        # A start reads its tags from the release's whole list (launch_tags), so it names a real job.
        real = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))
        status, out, _ = self.run_main("start", "gold_panel_rebuild", dagster=dagster, listing=real)
        self.assertEqual((status, out), (0, "job-orchestrator: started gold_panel_rebuild as Dagster run run-1\n"))
        self.assertEqual(dagster.told[-1][1]["executionMetadata"]["tags"][0], {"key": "foundation/run-kind", "value": "triggered"})
        status, _, _ = self.run_main("start", "gold_panel_rebuild", "requested", REQUEST, dagster=dagster, listing=real)
        self.assertEqual(status, 0)
        self.assertEqual(self.run_main("pause", "old")[0], 64)
        self.assertEqual(self.run_main("unpause", "nightly", dagster=Dagster({}))[0], 1)
        self.assertEqual(self.run_main("stop", "nightly")[0], 64)


CODE_LOCATION = textwrap.dedent('''
    from dagster import DefaultSensorStatus, Definitions, SkipReason, job, op, sensor

    @op
    def run():
        pass

    @job(name="foundation_job__nightly")
    def nightly():
        run()

    @sensor(name="foundation_decide__nightly", job_name="foundation_job__nightly",
            default_status=DefaultSensorStatus.RUNNING)
    def decide(context):
        return SkipReason("nothing is due")

    defs = Definitions(jobs=[nightly], sensors=[decide])
''')


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class AgainstDagster(unittest.TestCase):
    """The same requests against Dagster's own GraphQL: a sensor declared running is stopped and started
    again by the state id its listing gives, and a launched run carries the tags and waits in the queue."""

    def test_hold_release_and_start_a_job(self):
        from dagster import instance_for_test
        from dagster._core.workspace.context import WorkspaceProcessContext
        from dagster._core.workspace.load_target import PythonFileTarget
        from dagster_graphql.test.utils import execute_dagster_graphql

        with tempfile.TemporaryDirectory() as directory, instance_for_test() as instance:
            path = pathlib.Path(directory) / "location.py"
            path.write_text(CODE_LOCATION, encoding="utf-8")
            target = PythonFileTarget(python_file=str(path), attribute=None, working_directory=None,
                                      location_name=job_specs.DAGSTER_LOCATION)
            with WorkspaceProcessContext(instance, target) as process:
                def post(query, variables):
                    # Dagster's own ids and answers, through the gate's rules for the host.
                    body = json.dumps({"query": query, "variables": variables}).encode()
                    self.assertIsNone(dagster_writes.refused(dagster_writes.HOST, "POST", "/graphql", body))
                    result = execute_dagster_graphql(process.create_request_context(), query, variables)
                    self.assertFalse(result.errors, result.errors)
                    return result.data

                jobs = listed()
                sensor = "foundation_decide__nightly"
                self.assertEqual(job_orchestrator.sensor_states(post)[sensor][1], "RUNNING")
                self.assertEqual(job_orchestrator.pause(jobs, ["nightly"], post), ["nightly"])
                self.assertEqual(job_orchestrator.sensor_states(post)[sensor][1], "STOPPED")
                self.assertEqual(job_orchestrator.pause(jobs, ["nightly"], post), [])
                self.assertEqual(job_orchestrator.unpause(jobs, ["nightly"], post), ["nightly"])
                self.assertEqual(job_orchestrator.sensor_states(post)[sensor][1], "RUNNING")
                self.assertEqual(job_orchestrator.unpause(jobs, ["nightly"], post), [])
                # Dagster has no sensor for the other job: releasing it fails, loudly.
                with self.assertRaises(job_orchestrator.Unanswered):
                    job_orchestrator.unpause(jobs, ["hourly"], post)
                # A run that waits to start is the one a deploy's start joins; one in progress or ended is not.
                from dagster import DagsterRun, DagsterRunStatus
                from dagster._core.utils import make_new_run_id

                self.assertIsNone(job_orchestrator.waiting("nightly", post))
                for status in (DagsterRunStatus.STARTED, DagsterRunStatus.SUCCESS, DagsterRunStatus.CANCELED):
                    instance.add_run(DagsterRun(job_name="foundation_job__nightly", run_id=make_new_run_id(), status=status))
                instance.add_run(DagsterRun(job_name="foundation_job__other", run_id=make_new_run_id(),
                                            status=DagsterRunStatus.QUEUED))
                self.assertIsNone(job_orchestrator.waiting("nightly", post))
                queued = make_new_run_id()
                instance.add_run(DagsterRun(job_name="foundation_job__nightly", run_id=queued, status=DagsterRunStatus.QUEUED))
                self.assertEqual(job_orchestrator.waiting("nightly", post), queued)
                run_id = job_orchestrator.start(jobs, "nightly", f"requested {REQUEST}", post, job_specs.run_tags(300, f"requested {REQUEST}", 60 * 55))
                run = instance.get_run_by_id(run_id)
                self.assertEqual(run.job_name, "foundation_job__nightly")
                for key, value in job_specs.run_tags(300, f"requested {REQUEST}", 60 * 55).items():
                    self.assertEqual(run.tags[key], value)
                # What the deploy asks after a release (airflow-runtime.sh through writer_gate.py ask).
                self.assertEqual(post(dagster_writes.RELOAD_LOCATION, None)["reloadRepositoryLocation"]["__typename"],
                                 "WorkspaceLocationEntry")
                entries = post(dagster_writes.LOCATION_ERRORS, None)["workspaceOrError"]["locationEntries"]
                self.assertEqual([entry["locationOrLoadError"]["__typename"] for entry in entries], ["RepositoryLocation"])
                # Root ADR-0605 §6: the pool limit documents as Dagster 1.13 answers them, through the gate.
                self.assertTrue(post(dagster_writes.SET_POOL_LIMIT, {"concurrencyKey": "duckdb", "limit": 2})["setConcurrencyLimit"])
                limits = {entry["concurrencyKey"]: entry for entry in
                          post(dagster_writes.POOL_LIMITS, None)["instance"]["concurrencyLimits"]}
                self.assertEqual((limits["duckdb"]["limit"], limits["duckdb"]["usingDefaultLimit"]), (2, False))
                self.assertTrue(post(dagster_writes.DELETE_POOL_LIMIT, {"concurrencyKey": "duckdb"})["deleteConcurrencyLimit"])
                self.assertNotIn("duckdb", {entry["concurrencyKey"] for entry in
                                            post(dagster_writes.POOL_LIMITS, None)["instance"]["concurrencyLimits"]})


if __name__ == "__main__":
    unittest.main()
