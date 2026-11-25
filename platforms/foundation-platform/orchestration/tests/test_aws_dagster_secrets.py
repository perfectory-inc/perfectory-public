"""The Dagster service and its runs on AWS never carry the RDS master password (root ADR-0616 A1).

EcsRunLauncher copies the daemon's container definition into every run task, so whatever secret the
service's task definition names, every job would carry. infra/aws/dagster.tf therefore gives Dagster the
database role's own secret, and only the one-off bootstrap reads the master user's. A later edit that
names the master secret anywhere else (the service's secrets, its execution role, a shared local) fails here.
Nor may Dagster start the bootstrap: its role starts only the family EcsRunLauncher registers runs in.
"""

import json
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
STACK = PLATFORM / "infra" / "aws" / "dagster.tf"
CONTRACT = PLATFORM / "config" / "aws-platform.contract.json"
INSTANCE = PLATFORM / "orchestration" / "dagster" / "aws" / "dagster.yaml"
HEADER = re.compile(r'^(resource|data|ephemeral|variable|output|locals)\b(?:\s+"([^"]+)")?(?:\s+"([^"]+)")?\s*\{\s*$')
MASTER = "master_user_secret"
APP = "aws_secretsmanager_secret.dagster_database"


def blocks(text: str) -> list[tuple[str, str]]:
    """(header, body) of each top-level block. The file is `tofu fmt`'s: a top-level block opens on a line
    of its own at column 0 and closes with a `}` alone at column 0."""
    found, header, body = [], None, []
    for line in text.splitlines():
        if header is None:
            match = HEADER.match(line)
            if match:
                header, body = " ".join(part for part in match.groups() if part), []
            continue
        if line == "}":
            found.append((header, "\n".join(body)))
            header = None
            continue
        body.append(line)
    if header is not None:
        raise AssertionError(f"{STACK.name}: block {header!r} never closes")
    return found


class DagsterNeverCarriesTheMasterSecret(unittest.TestCase):
    def setUp(self) -> None:
        self.blocks = blocks(STACK.read_text(encoding="utf-8"))
        self.by_header = dict(self.blocks)

    def test_the_parser_sees_the_stack(self) -> None:
        headers = [header for header, _ in self.blocks]
        self.assertIn("resource aws_ecs_task_definition dagster", headers)
        self.assertIn("resource aws_ecs_task_definition database_bootstrap", headers)
        self.assertGreater(len(headers), 20)

    def test_only_the_bootstrap_names_the_master_secret(self) -> None:
        naming = [header for header, body in self.blocks if MASTER in body]
        self.assertTrue(naming, "the bootstrap reads the master secret, so some block names it")
        for header in naming:
            self.assertTrue(header.split()[-1].startswith("database_bootstrap"), f"{header} names the master secret")

    def test_the_service_reads_the_role_secret_and_nothing_else(self) -> None:
        policy = self.by_header["data aws_iam_policy_document dagster_execution"]
        self.assertIn(f"{APP}.arn", policy)
        self.assertEqual(re.findall(r"resources\s*=\s*\[([^\]]*)\]", policy), [f"{APP}.arn"])
        shared = next(body for header, body in self.blocks if header == "locals" and "dagster_secrets" in body)
        secrets = re.search(r"dagster_secrets\s*=\s*\[(.*?)\n  \]", shared, re.S).group(1)
        self.assertEqual(re.findall(r'valueFrom\s*=\s*"\$\{([^}]+)\}', secrets), [f"{APP}.arn"])

    def test_the_service_task_uses_its_own_execution_role(self) -> None:
        task = self.by_header["resource aws_ecs_task_definition dagster"]
        self.assertIn("execution_role_arn       = aws_iam_role.dagster_execution.arn", task)
        self.assertNotIn("database_bootstrap", task)

    def test_dagster_starts_only_its_run_family(self) -> None:
        policy = self.by_header["data aws_iam_policy_document dagster"]
        self.assertNotIn("task-definition/*", policy)
        statement = re.search(r'sid\s*=\s*"LaunchRunsInThisCluster"(.*?)\n  \}', policy, re.S).group(1)
        self.assertEqual(re.findall(r'"([^"]*task-definition/[^"]*)"', statement),
                         ["arn:aws:ecs:${local.contract.region}:${local.contract.account.id}:task-definition/"
                          "${local.dagster.run_task_definition_prefix}_*"])
        prefix = json.loads(CONTRACT.read_text(encoding="utf-8"))["dagster"]["run_task_definition_prefix"]
        self.assertLess(len(prefix), 32, "dagster-aws asserts a prefix under 32 characters")
        bootstrap = re.search(r'family\s*=\s*"([^"]+)"', self.by_header["resource aws_ecs_task_definition database_bootstrap"]).group(1)
        self.assertFalse(bootstrap.startswith(f"{prefix}_"), "the run family pattern would match the bootstrap")
        # The launcher registers runs under the prefix the role allows: the instance reads it from the stack.
        self.assertIn("task_definition_prefix:\n      env: DAGSTER_RUN_TASK_DEFINITION_PREFIX", INSTANCE.read_text(encoding="utf-8"))
        shared = next(body for header, body in self.blocks if header == "locals" and "dagster_environment" in body)
        self.assertIn('{ name = "DAGSTER_RUN_TASK_DEFINITION_PREFIX", value = local.dagster.run_task_definition_prefix }', shared)


if __name__ == "__main__":
    unittest.main()
