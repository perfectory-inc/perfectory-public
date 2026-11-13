"""The deploy and an operator's run-now reach a job Dagster decides through Dagster (root ADR-0565).

scripts/deploy/job_orchestrator.py stops and starts the job's sensor and launches its Dagster job, where
`airflow dags pause|unpause|trigger` did the same to a job's DAG. The first class holds its rules with a
stand-in for Dagster's GraphQL; the second runs the same requests against a real Dagster instance and
code location (the CI `dagster-observe` lane, where a skip fails the lane), so a request Dagster's schema
does not take cannot pass here.
"""

import importlib.util
import io
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
            {"id": "nightly", "enabled": True, "dagster_stage": "dagster", "pool": "spark", "priority_class": "publish"},
            {"id": "hourly", "enabled": True, "dagster_stage": "dagster", "pool": "default_pool"},
            {"id": "proxied", "enabled": True, "dagster_stage": "proxied", "pool": "default_pool"},
            {"id": "old", "enabled": True, "pool": "default_pool"},
            {"id": "off", "enabled": False, "pool": "default_pool"},
        ],
    }


class Dagster:
    """Dagster's GraphQL as job_orchestrator asks it: sensor name -> status, and what it was told."""

    def __init__(self, sensors):
        self.sensors, self.told = dict(sensors), []

    def __call__(self, query, variables):
        if "sensorsOrError" in query:
            return {"sensorsOrError": {"__typename": "Sensors", "results": [
                {"name": name, "sensorState": {"id": f"state-{name}", "status": status}}
                for name, status in self.sensors.items()]}}
        if "stopSensor" in query:
            name = variables["id"].removeprefix("state-")
            self.told.append(("stop", name))
            self.sensors[name] = "STOPPED"
            return {"stopSensor": {"__typename": "StopSensorMutationResult"}}
        if "startSensor" in query:
            name = variables["sensor"]["sensorName"]
            self.told.append(("start", name))
            self.sensors[name] = "RUNNING"
            return {"startSensor": {"__typename": "Sensor"}}
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
        dagster = Dagster({"foundation_decide__hourly": "RUNNING"})
        with self.assertRaises(job_orchestrator.Unanswered) as raised:
            job_orchestrator.unpause(listed(), ["nightly"], dagster)
        self.assertIn("foundation_decide__nightly", str(raised.exception))
        self.assertEqual(job_orchestrator.pause(listed(), ["nightly"], dagster), [])

    def test_a_job_airflow_decides_is_refused(self):
        for job_id in ("proxied", "old", "off", "nowhere"):
            with self.subTest(job_id), self.assertRaises(job_orchestrator.Refused):
                job_orchestrator.pause(listed(), [job_id], self.dagster())
        with self.assertRaises(job_orchestrator.Refused):
            job_orchestrator.start(listed(), "old", "triggered", self.dagster())

    def test_start_launches_the_jobs_dagster_job_with_the_tags_every_run_of_it_carries(self):
        dagster = self.dagster()
        self.assertEqual(job_orchestrator.start(listed(), "nightly", f"requested {REQUEST}", dagster), "run-1")
        ((_, params),) = dagster.told
        self.assertEqual(params["selector"], {**job_orchestrator.REPOSITORY, "pipelineName": "foundation_job__nightly"})
        tags = {tag["key"]: tag["value"] for tag in params["executionMetadata"]["tags"]}
        self.assertEqual(tags, job_specs.run_tags("spark", 300, f"requested {REQUEST}"))
        self.assertEqual(tags["foundation/run-kind"], f"requested {REQUEST}")

    def test_start_takes_only_a_kind_the_forced_command_reads(self):
        for kind in ("", "check", "triggered; reboot", "requested x"):
            with self.subTest(kind), self.assertRaises(job_orchestrator.Refused):
                job_orchestrator.start(listed(), "nightly", kind, self.dagster())

    def test_an_answer_that_is_not_the_expected_one_fails(self):
        def refuses(query, variables):
            return {"sensorsOrError": {"__typename": "PythonError", "message": "the location is loading"}}
        with self.assertRaises(job_orchestrator.Unanswered) as raised:
            job_orchestrator.pause(listed(), ["nightly"], refuses)
        self.assertIn("the location is loading", str(raised.exception))

    def test_dagster_is_asked_only_on_the_hosts_loopback(self):
        jobs = listed()
        jobs["observers"]["dagster"]["ui"] = "http://dagster.example:3000"
        with self.assertRaises(job_orchestrator.Unanswered):
            job_orchestrator.poster(jobs)

    def run_main(self, *argv, dagster=None):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "jobs.v1.json"
            path.write_text(json.dumps(listed()), encoding="utf-8")
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                status = job_orchestrator.main([argv[0], str(path), *argv[1:]], post=dagster or self.dagster())
        return status, out.getvalue(), err.getvalue()

    def test_the_command_line_the_deploy_and_the_run_now_use(self):
        self.assertEqual(self.run_main("decided")[:2], (0, "nightly\nhourly\n"))
        dagster = self.dagster()
        status, out, _ = self.run_main("pause", "nightly", "hourly", dagster=dagster)
        self.assertEqual((status, out), (0, "job-orchestrator: paused 2 of 2 sensors: nightly hourly\n"))
        status, out, _ = self.run_main("unpause", "nightly", "hourly", dagster=dagster)
        self.assertEqual((status, out), (0, "job-orchestrator: unpaused 2 of 2 sensors: nightly hourly\n"))
        # Nothing named asks Dagster nothing: a release with no such job never needs it to answer.
        self.assertEqual(self.run_main("pause", dagster=lambda *_: self.fail("asked"))[0], 0)
        status, out, _ = self.run_main("start", "nightly", dagster=dagster)
        self.assertEqual((status, out), (0, "job-orchestrator: started nightly as Dagster run run-1\n"))
        self.assertEqual(dagster.told[-1][1]["executionMetadata"]["tags"][0], {"key": "foundation/run-kind", "value": "triggered"})
        status, _, _ = self.run_main("start", "nightly", "requested", REQUEST, dagster=dagster)
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
                run_id = job_orchestrator.start(jobs, "nightly", f"requested {REQUEST}", post)
                run = instance.get_run_by_id(run_id)
                self.assertEqual(run.job_name, "foundation_job__nightly")
                for key, value in job_specs.run_tags("spark", 300, f"requested {REQUEST}").items():
                    self.assertEqual(run.tags[key], value)


if __name__ == "__main__":
    unittest.main()
