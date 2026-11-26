"""Dagster on AWS holds only its own rights, and its runs hold none of them (root ADR-0616 §2-3, §2-8, A1).

Read from the stack and the instance as their parsers read them (aws_stack.py: conftest's hcl2 and yaml
parsers), never from the files' text layout.

- The RDS master password is named only by the one-off tasks that run as the master user (bootstrap, and
  finalize after the migrations), and the migrator's only by the rollout's tasks.
- The service reads the database role's password and nothing else; it runs under its own two roles.
- A run task carries none of Dagster's roles. Its task definition is the instance's own (dagster.yaml's
  run_launcher), whose task role is the default run role with no policy, so a run reaches only what its
  lane's role gives it. Every role Dagster may pass is under the contract's run path, and none of its own is.
  2026-11-26 (W107 review of origin/main d4816f74d): every run task copied the service's task definition and
  with it Dagster's role, which may register and start tasks, pass roles and use ECS Exec.
- Dagster may start only the family its launcher registers runs in, and that prefix is one dagster-aws
  accepts. 2026-11-26: the live prefix `perfectory-platform-run` (23 characters) made dagster-aws 0.29.27
  refuse to build the launcher ("no more than 16 characters"), so the first run on AWS could not have started.
- The rollout migrates before the service moves, and an apply waits for the service's new task.
"""

import unittest

import aws_stack
from aws_stack import body, naming, references, statements

MASTER = "aws_db_instance.platform.master_user_secret"
APP = "aws_secretsmanager_secret.dagster_database"
MIGRATOR = "aws_secretsmanager_secret.foundation_migrator"
# The one-off tasks that run as the master user, and the role that injects its password into them.
MASTER_READERS = ("aws_ecs_task_definition.database_bootstrap", "aws_ecs_task_definition.database_finalize",
                  "data.aws_iam_policy_document.database_bootstrap_execution")
# dagster-aws 0.29.27 EcsRunLauncher.__init__: "Task definition prefix must be no more than 16 characters".
# The release's image check builds the launcher from dagster.yaml with the library itself (aws-images-build.py).
LAUNCHER_PREFIX_LIMIT = 16


def environment(container_locals: str) -> dict[str, str]:
    return {entry["name"]: entry["value"] for entry in body(container_locals)}


class DagsterNeverCarriesTheMasterSecret(unittest.TestCase):
    def test_the_parser_sees_the_stack(self) -> None:
        resources = aws_stack.stack()["resource"]
        for type_, name in (("aws_ecs_task_definition", "dagster"), ("aws_ecs_task_definition", "database_bootstrap"),
                            ("aws_ecs_task_definition", "foundation_migrate"), ("aws_ecs_service", "dagster"),
                            ("aws_iam_role", "run_default"), ("terraform_data", "pre_deploy")):
            self.assertIn(name, resources.get(type_, {}), f"{type_}.{name}")
        self.assertGreater(len(aws_stack.blocks()), 40)
        self.assertIn(MASTER, {reference.split("[")[0] for reference in references(body("aws_ecs_task_definition.database_bootstrap"))})

    def test_only_the_master_readers_name_the_master_secret(self) -> None:
        readers = naming(MASTER)
        self.assertIn("aws_ecs_task_definition.database_bootstrap", readers)
        self.assertEqual(sorted(set(readers) - set(MASTER_READERS)), [])

    def test_only_the_rollout_tasks_name_the_migrator_secret(self) -> None:
        readers = naming(MIGRATOR)
        self.assertIn("aws_ecs_task_definition.foundation_migrate", readers)
        for address in readers:
            self.assertTrue(address.split(".")[-1].startswith(("foundation_migrator", "foundation_migrate", "database_bootstrap",
                                                               "database_finalize")), f"{address} names the migrator secret")

    def test_the_service_reads_the_role_secret_and_nothing_else(self) -> None:
        self.assertEqual([statement["resources"] for statement in statements("data.aws_iam_policy_document.dagster_execution").values()],
                         [[f"${{{APP}.arn}}"]])
        self.assertEqual([secret["valueFrom"] for secret in body("local.dagster_secrets")], ["${local.dagster_password}"])
        self.assertEqual(references(body("local.dagster_password")), [f"{APP}.arn"])

    def test_the_service_runs_under_its_own_roles(self) -> None:
        task = body("aws_ecs_task_definition.dagster")
        self.assertEqual(task["execution_role_arn"], "${aws_iam_role.dagster_execution.arn}")
        self.assertEqual(task["task_role_arn"], "${aws_iam_role.dagster.arn}")
        self.assertNotIn("aws_ecs_task_definition.dagster", naming("aws_ecs_task_definition.database_bootstrap"))


class RunsCarryNoneOfDagstersRights(unittest.TestCase):
    def setUp(self) -> None:
        self.path = aws_stack.contract()["ecs"]["run_role_path"]
        self.launcher = aws_stack.instance()["run_launcher"]["config"]
        self.environment = environment("local.dagster_environment")

    def test_dagster_passes_only_roles_under_the_run_path(self) -> None:
        passes = [statement for statement in statements("data.aws_iam_policy_document.dagster").values()
                  if "iam:PassRole" in statement["actions"]]
        self.assertEqual([statement["resources"] for statement in passes],
                         [["arn:aws:iam::${local.contract.account.id}:role${local.contract.ecs.run_role_path}*"]])
        self.assertTrue(self.path.startswith("/") and self.path.endswith("/run/"), self.path)
        under_path = sorted(name for name, role in aws_stack.stack()["resource"]["aws_iam_role"].items()
                            if role.get("path") == "${local.contract.ecs.run_role_path}")
        self.assertIn("run_default", under_path)
        self.assertIn("run_execution", under_path)
        for own in ("dagster", "dagster_execution", "database_bootstrap_execution", "foundation_migrate_execution"):
            self.assertNotIn(own, under_path, f"aws_iam_role.{own} is Dagster's or a one-off's, never a run's")

    def test_the_launcher_registers_its_own_run_task_definition(self) -> None:
        # A `task_definition` mapping: dagster-aws 0.29.27 builds the run's definition from it instead of
        # copying the service's container (with its role, its secrets and its ECS Exec).
        definition = self.launcher["task_definition"]
        self.assertEqual(definition["task_role_arn"], {"env": "DAGSTER_RUN_TASK_ROLE_ARN"})
        self.assertEqual(definition["execution_role_arn"], {"env": "DAGSTER_RUN_EXECUTION_ROLE_ARN"})
        self.assertEqual(self.environment["DAGSTER_RUN_TASK_ROLE_ARN"], "${aws_iam_role.run_default.arn}")
        self.assertEqual(self.environment["DAGSTER_RUN_EXECUTION_ROLE_ARN"], "${aws_iam_role.run_execution.arn}")
        self.assertEqual(self.launcher["secrets"], [{"name": "DAGSTER_PG_PASSWORD", "valueFrom": {"env": "DAGSTER_PG_PASSWORD_SECRET"}}])
        self.assertEqual(self.environment["DAGSTER_PG_PASSWORD_SECRET"], "${local.dagster_password}")
        # No secret is found by tag: that lists the account's secrets, which Dagster's role may not.
        self.assertIsNone(self.launcher["secrets_tag"])

    def test_the_default_run_role_has_no_policy(self) -> None:
        self.assertEqual([address for address in naming("aws_iam_role.run_default") if address != "local.dagster_environment"], [])

    def test_the_run_execution_role_injects_only_the_role_password(self) -> None:
        run = statements("data.aws_iam_policy_document.run_execution")
        secrets = [statement for statement in run.values() if "secretsmanager:GetSecretValue" in statement["actions"]]
        self.assertEqual([statement["resources"] for statement in secrets], [[f"${{{APP}.arn}}"]])
        self.assertNotIn("data.aws_iam_policy_document.run_execution", naming(MASTER))

    def test_dagster_starts_only_its_run_family(self) -> None:
        launch = statements("data.aws_iam_policy_document.dagster")["LaunchRunsInThisCluster"]
        self.assertEqual(launch["resources"], ["arn:aws:ecs:${local.contract.region}:${local.contract.account.id}:task-definition/"
                                               "${local.dagster.run_task_definition_prefix}_*"])
        prefix = aws_stack.contract()["dagster"]["run_task_definition_prefix"]
        self.assertLessEqual(len(prefix), LAUNCHER_PREFIX_LIMIT)
        for name, task in aws_stack.stack()["resource"]["aws_ecs_task_definition"].items():
            self.assertFalse(task["family"].startswith(f"{prefix}_"), f"the run family pattern would match {name}")
        self.assertEqual(self.launcher["task_definition_prefix"], {"env": "DAGSTER_RUN_TASK_DEFINITION_PREFIX"})
        self.assertEqual(self.environment["DAGSTER_RUN_TASK_DEFINITION_PREFIX"], "${local.dagster.run_task_definition_prefix}")


class TheRolloutMovesTheServiceLast(unittest.TestCase):
    def test_the_rollout_migrates_before_the_service_moves(self) -> None:
        service = body("aws_ecs_service.dagster")
        self.assertIn("${terraform_data.pre_deploy}", service["depends_on"])
        step = body("terraform_data.pre_deploy")
        named = references(step["triggers_replace"])
        for trigger in ("var.dagster_image", "var.runtime_image"):
            self.assertIn(trigger, named, "a new image digest runs the chain")
        chain = [reference.split(".")[1] for reference in references(step["provisioner"]["local-exec"][0]["environment"]["TASK_DEFINITIONS"])
                 if reference.startswith("aws_ecs_task_definition.")]
        self.assertEqual(chain, ["database_bootstrap", "foundation_migrate", "database_finalize"])

    def test_an_apply_waits_for_the_new_task_and_a_new_password_restarts_it(self) -> None:
        service = body("aws_ecs_service.dagster")
        self.assertIs(service["wait_for_steady_state"], True)
        self.assertIs(service["force_new_deployment"], True)
        self.assertEqual(references(service["triggers"]), [f"{APP.replace('aws_secretsmanager_secret.', 'aws_secretsmanager_secret_version.')}.secret_string_wo_version"])


if __name__ == "__main__":
    unittest.main()
