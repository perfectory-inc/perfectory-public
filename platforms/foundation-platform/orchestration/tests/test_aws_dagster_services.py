"""The webserver and the daemon on AWS are each their own always-on service, sized in the contract (root
ADR-0616 §1, coordinator 2026-11-27).

Read from the stack and the contract as their parsers read them (aws_stack.py). Until 2026-11-27 (W101b) both
were the two essential containers of one task of 0.5 vCPU and 2 GB: neither could be sized or deployed alone, a
webserver that died stopped the daemon with it, and every deploy, which stops the one daemon first, took the UI
down too. Dagster 1.13 (deploying-dagster-as-a-service): a deployment may run several webservers and a single
daemon.

Every always-on service is named before it outgrows its task (root ADR-0574 §5, contract alarms.near_limit):
on ai-server foundation-api sat at its memory cap for two weeks and was killed 69 times (root ADR-0573), and the
daemon stood at 379 of its 384 MiB on 2026-11-19. On AWS the sizes are Fargate's smallest, and the daemon's
CPU grows with every lane's sensors.
"""

import unittest

import aws_stack
from aws_stack import body, references

PER_SERVICE = ("aws_ecs_task_definition.dagster", "aws_ecs_service.dagster", "aws_cloudwatch_metric_alarm.dagster_not_running")


class EveryProcessIsItsOwnService(unittest.TestCase):
    def setUp(self):
        self.services = aws_stack.contract()["dagster"]["services"]
        self.processes = body("local.dagster_processes")

    def test_the_contract_names_each_process_its_service_and_its_size(self):
        self.assertEqual(sorted(self.services), ["daemon", "webserver"])
        self.assertEqual(sorted(self.processes), sorted(self.services), "a process the contract does not size never runs")
        for address in PER_SERVICE:
            self.assertEqual(body(address)["for_each"], "${local.dagster.services}", address)
        task = body("aws_ecs_task_definition.dagster")
        self.assertEqual((task["cpu"], task["memory"]), ("${each.value.cpu}", "${each.value.memory}"))
        self.assertEqual(body("aws_ecs_service.dagster")["name"], "${each.value.name}")
        names = [service["name"] for service in self.services.values()]
        self.assertEqual(len(set(names)), len(names))

    def test_a_task_runs_its_one_process(self):
        task = body("aws_ecs_task_definition.dagster")
        self.assertIn("local.dagster_processes", references(task["container_definitions"]))
        self.assertIn("local.dagster_container", references(task["container_definitions"]))
        self.assertEqual(self.processes["webserver"]["command"][0], "dagster-webserver")
        self.assertEqual(self.processes["daemon"]["command"][:2], ["dagster-daemon", "run"])

    def test_one_daemon_at_a_time_and_the_webserver_starts_its_new_task_first(self):
        daemon, webserver = self.processes["daemon"], self.processes["webserver"]
        self.assertEqual((daemon["minimum_healthy_percent"], daemon["maximum_percent"]), (0, 100))
        self.assertEqual((webserver["minimum_healthy_percent"], webserver["maximum_percent"]), (100, 200))
        service = body("aws_ecs_service.dagster")
        self.assertEqual(service["desired_count"], 1)
        self.assertIn("local.dagster_processes", references(service["deployment_minimum_healthy_percent"]))
        self.assertIn("local.dagster_processes", references(service["deployment_maximum_percent"]))

    def test_each_service_has_its_not_running_alarm(self):
        alarm = body("aws_cloudwatch_metric_alarm.dagster_not_running")
        self.assertIn("aws_ecs_service.dagster", references(alarm["dimensions"]))
        self.assertIn("each.key", references(alarm["alarm_name"]))
        self.assertEqual(alarm["alarm_actions"], ["${aws_sns_topic.alarms.arn}"])


class EveryAlwaysOnServiceIsNamedNearItsLimit(unittest.TestCase):
    def test_every_always_on_service_has_a_memory_and_a_cpu_alarm(self):
        services = references(body("local.always_on_services"))
        self.assertIn("aws_ecs_service.dagster", services)
        self.assertIn("aws_ecs_service.code_server", services)
        self.assertIn("local.dagster.services", services)
        self.assertIn("local.code_locations", services)
        near = body("local.near_limit")
        self.assertEqual(sorted(near), ["cpu", "memory"])
        self.assertEqual((near["memory"]["metric"], near["memory"]["statistic"]), ("MemoryUtilization", "Maximum"))
        self.assertEqual((near["cpu"]["metric"], near["cpu"]["statistic"]), ("CPUUtilization", "Average"))
        alarm = body("aws_cloudwatch_metric_alarm.near_limit")
        self.assertIn("local.always_on_services", references(alarm["for_each"]))
        self.assertIn("local.near_limit", references(alarm["for_each"]))
        self.assertEqual(alarm["alarm_actions"], ["${aws_sns_topic.alarms.arn}"])

    def test_the_thresholds_are_the_contracts(self):
        near = body("local.near_limit")
        self.assertEqual(near["memory"]["percent"], "${local.contract.alarms.near_limit.memory_percent}")
        self.assertEqual(near["cpu"]["percent"], "${local.contract.alarms.near_limit.cpu_percent}")
        declared = aws_stack.contract()["alarms"]["near_limit"]
        for key in ("memory_percent", "cpu_percent"):
            self.assertTrue(0 < declared[key] < 100, key)
        self.assertIn("local.near_limit", references(body("aws_cloudwatch_metric_alarm.near_limit")["threshold"]))


if __name__ == "__main__":
    unittest.main()
