"""Every code location on AWS is its own gRPC code server, and a code release leaves Dagster running (root
ADR-0616 §1, §2-2, coordinator 2026-11-26).

Read from the stack, the workspace and the contract as their parsers read them (aws_stack.py). 2026-11-26 (W107
review of origin/main): the one code location was loaded in process by the webserver and the daemon of one task,
from the code location's image, so every code release stopped the daemon and its schedule ticks, and a second
image (the tile lane's GDAL and tippecanoe) had no place.
"""

import unittest

import aws_stack
from aws_stack import body, reaching, references, statements

AWS = aws_stack.PLATFORM / "orchestration" / "dagster"


class EveryLocationIsItsOwnCodeServer(unittest.TestCase):
    def setUp(self):
        self.dagster = aws_stack.contract()["dagster"]
        self.locations = self.dagster["code_locations"]
        self.resources = aws_stack.stack()["resource"]

    def test_every_location_has_a_service_a_name_and_its_port_open_inside_the_tasks_group(self):
        self.assertTrue(self.locations)
        for address in ("aws_ecs_task_definition.code_server", "aws_ecs_service.code_server",
                        "aws_service_discovery_service.code_location", "aws_vpc_security_group_ingress_rule.code_servers",
                        "aws_vpc_security_group_egress_rule.code_servers"):
            self.assertEqual(body(address)["for_each"], "${local.code_locations}", address)
        self.assertEqual(body("local.code_locations"), "${local.dagster.code_locations}")
        service = body("aws_ecs_service.code_server")
        self.assertEqual([reference for reference in references(service["service_registries"]) if reference.startswith("aws_")],
                         ["aws_service_discovery_service.code_location"])
        rule = body("aws_vpc_security_group_ingress_rule.code_servers")
        self.assertEqual(rule["referenced_security_group_id"], "${aws_security_group.tasks.id}", "nothing from outside the group")

    def test_the_workspace_lists_exactly_the_contracts_locations(self):
        servers = [entry["grpc_server"] for entry in aws_stack.yaml_file(AWS / "aws" / "workspace.yaml")["load_from"]]
        self.assertEqual(sorted((server["location_name"], server["host"], server["port"]) for server in servers),
                         sorted((name, f"{name}.{self.dagster['namespace']}", location["port"])
                                for name, location in self.locations.items()))

    def test_a_code_release_moves_the_code_servers_and_not_the_service(self):
        container = body("local.dagster_container")
        self.assertEqual(container["image"], "${var.dagster_service_image}")
        moved = reaching("var.dagster_image")
        self.assertIn("aws_ecs_task_definition.code_server", moved)
        self.assertNotIn("aws_ecs_task_definition.dagster", moved)
        self.assertNotIn("local.dagster_environment", moved, "the service does not report the code's image")
        current = [address for address, value in aws_stack.blocks() if "DAGSTER_CURRENT_IMAGE" in str(value)]
        self.assertEqual(current, ["aws_ecs_task_definition.code_server"], "only a code server reports its image")

    def test_the_dagster_image_holds_the_workspace_where_the_service_reads_it(self):
        dockerfile = (AWS / "Dockerfile.aws-dagster").read_text(encoding="utf-8")
        self.assertIn(f"COPY orchestration/dagster/aws/workspace.yaml {self.dagster['workspace']}\n", dockerfile)
        self.assertIn("COPY orchestration/dagster/aws/dagster.yaml /opt/dagster/home/dagster.yaml\n", dockerfile)
        self.assertNotIn("definitions", dockerfile.split("FROM", 1)[1].replace("definitions.py", ""),
                         "the Dagster image holds no location's code")

    def test_a_code_server_may_publish_to_the_alarm_topic_and_nothing_else(self):
        self.assertEqual([(statement["actions"], statement["resources"])
                          for statement in statements("data.aws_iam_policy_document.code_server").values()],
                         [(["sns:Publish"], ["${aws_sns_topic.alarms.arn}"])])
        path = aws_stack.contract()["ecs"]["run_role_path"]
        self.assertNotEqual(self.resources["aws_iam_role"]["code_server"].get("path"), path, "no run may carry it")


if __name__ == "__main__":
    unittest.main()
