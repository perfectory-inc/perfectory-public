"""Dagster on AWS holds only its own rights, and its runs hold none of them (root ADR-0616 §2-3, §2-8, A1).

Read from the stack and the instance as their parsers read them (aws_stack.py: conftest's hcl2 and yaml
parsers), never from the files' text layout.

- The RDS master password reaches only the one-off tasks that run as the master user (the bootstrap, and the
  migrate task's open and finalize steps) and their execution roles, and the migrator's only those tasks,
  however many locals it passes through.
- The service reads the database role's password and nothing else; it runs under its own two roles.
- A run task carries none of Dagster's roles. Its task definition is the instance's own (dagster.yaml's
  run_launcher), whose task role is the default run role with no policy, so a run reaches only what its
  lane's role gives it. Every role Dagster may pass is under the contract's run path, and none of its own is.
  2026-11-26 (W107 review of origin/main d4816f74d): every run task copied the service's task definition and
  with it Dagster's role, which may register and start tasks, pass roles and use ECS Exec.
- Dagster may start only the family its launcher registers runs in, and that prefix is one dagster-aws
  accepts. 2026-11-26: the live prefix `perfectory-platform-run` (23 characters) made dagster-aws 0.29.27
  refuse to build the launcher ("no more than 16 characters"), so the first run on AWS could not have started.
- The rollout migrates before the service moves, and an apply waits for the service's new task. The
  bootstrap runs as the master only when its own definition or a password changes; the migrate task runs
  when its definition (with the runtime image) does; a new code-location image runs neither (2026-11-26:
  every image change, a rollback too, ran the bootstrap as the master). Its steps run in order.
- No task pulls from Docker Hub: psql's postgres is the contract's mirror in the release package.
"""

import unittest

import aws_stack
from aws_stack import body, naming, reaching, references, statements

MASTER = "aws_db_instance.platform.master_user_secret"
APP = "aws_secretsmanager_secret.dagster_database"
MIGRATOR = "aws_secretsmanager_secret.foundation_migrator"
# The one-off tasks (foundation-database.tf), their execution roles and the locals that hold their secrets.
ONE_OFF = ("aws_ecs_task_definition.database_bootstrap", "aws_ecs_task_definition.database_migrate",
           "data.aws_iam_policy_document.one_off_execution", "aws_iam_role.one_off_execution",
           "aws_iam_role_policy.one_off_execution", "local.one_off_secrets", "local.master_password",
           "local.migrator_password")
# dagster-aws 0.29.27 EcsRunLauncher.__init__: "Task definition prefix must be no more than 16 characters".
# The release's image check builds the launcher from dagster.yaml with the library itself (aws-images-build.py).
LAUNCHER_PREFIX_LIMIT = 16


def environment(container_locals: str) -> dict[str, str]:
    return {entry["name"]: entry["value"] for entry in body(container_locals)}


class DagsterNeverCarriesTheMasterSecret(unittest.TestCase):
    def test_the_parser_sees_the_stack(self) -> None:
        resources = aws_stack.stack()["resource"]
        for type_, name in (("aws_ecs_task_definition", "dagster"), ("aws_ecs_task_definition", "database_bootstrap"),
                            ("aws_ecs_task_definition", "database_migrate"), ("aws_ecs_service", "dagster"),
                            ("aws_iam_role", "run_default"), ("terraform_data", "database_migrate")):
            self.assertIn(name, resources.get(type_, {}), f"{type_}.{name}")
        self.assertGreater(len(aws_stack.blocks()), 40)
        self.assertIn("local.master_password", references(body("aws_ecs_task_definition.database_bootstrap")))

    def test_only_the_one_off_tasks_reach_the_master_secret(self) -> None:
        readers = reaching(MASTER)
        for task in ("aws_ecs_task_definition.database_bootstrap", "aws_ecs_task_definition.database_migrate"):
            self.assertIn(task, readers)
        self.assertEqual(sorted(readers - set(ONE_OFF)), [])

    def test_only_the_one_off_tasks_reach_the_migrator_secret(self) -> None:
        readers = reaching(MIGRATOR)
        self.assertIn("aws_ecs_task_definition.database_migrate", readers)
        allowed = set(ONE_OFF) | {"aws_secretsmanager_secret_version.foundation_migrator"}
        self.assertEqual(sorted(readers - allowed), [])

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
        self.assertIn("${terraform_data.database_migrate}", body("aws_ecs_service.dagster")["depends_on"])
        self.assertIn("${terraform_data.database_bootstrap}", body("terraform_data.database_migrate")["depends_on"])
        self.assertEqual([step["name"] for step in body("local.migrate_steps")], ["open", "migrate", "grants", "finalize"])

    def test_the_bootstrap_runs_on_its_own_definition_and_the_passwords_only(self) -> None:
        bootstrap = references(body("terraform_data.database_bootstrap")["triggers_replace"])
        self.assertEqual(sorted(bootstrap), sorted([
            "aws_ecs_task_definition.database_bootstrap.revision",
            "aws_secretsmanager_secret_version.dagster_database.secret_string_wo_version",
            "aws_secretsmanager_secret_version.foundation_migrator.secret_string_wo_version",
            "aws_secretsmanager_secret_version.foundation_runtime.secret_string_wo_version"]))
        self.assertEqual(references(body("terraform_data.database_migrate")["triggers_replace"]),
                         ["aws_ecs_task_definition.database_migrate.revision"])
        self.assertNotIn("var.runtime_image", references(body("aws_ecs_task_definition.database_bootstrap")))
        self.assertIn("aws_ecs_task_definition.database_migrate", reaching("var.runtime_image"))
        # A code-location image moves the service and the run tasks, never a one-off step.
        self.assertEqual(sorted(address for address in reaching("var.dagster_image") if address.startswith(("terraform_data.", "aws_ecs_task_definition.database"))), [])

    def test_no_task_pulls_from_docker_hub(self) -> None:
        self.assertEqual(references(body("local.postgres_image")), ["local.contract.images.repository", "local.versions.container_images"])
        [key] = [text.split('container_images["', 1)[1].split('"', 1)[0] for text in aws_stack.expressions(body("local.postgres_image"))]
        self.assertIn(key, aws_stack.contract()["images"]["mirrors"], "the image is mirrored by the release workflow")
        for name, task in aws_stack.stack()["resource"]["aws_ecs_task_definition"].items():
            for text in aws_stack.expressions(task["container_definitions"]):
                self.assertNotIn('image      = "', text, f"{name} names an image by a literal, not the contract's")

    def test_an_apply_waits_for_the_new_task_and_a_new_password_restarts_it(self) -> None:
        service = body("aws_ecs_service.dagster")
        self.assertIs(service["wait_for_steady_state"], True)
        self.assertIs(service["force_new_deployment"], True)
        self.assertEqual(references(service["triggers"]), [f"{APP.replace('aws_secretsmanager_secret.', 'aws_secretsmanager_secret_version.')}.secret_string_wo_version"])


if __name__ == "__main__":
    unittest.main()
