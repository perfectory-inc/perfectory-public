from __future__ import annotations

import copy
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

import foundation_ci_scope as scope


REPO_ROOT = Path(__file__).resolve().parents[2]
FOUNDATION_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "foundation-ci.yml"


class FoundationCiScopeTest(unittest.TestCase):
    def test_each_gate_covers_every_independent_witness(self) -> None:
        witnesses = {
            "boundary-slice": (
                "scripts/tiles/boundary-slice-proof.sh",
                "platforms/foundation-platform/migrations/20260819030646_project_industrial_complex_boundaries_into_postgis.sql",
                "platforms/foundation-platform/services/foundation-outbox-publisher/src/industrial_complex_boundary_static_release_publish.rs",
                "platforms/foundation-platform/crates/foundation-outbox/src/object_storage/file.rs",
                "platforms/foundation-platform/crates/catalog/catalog-domain/src/vector_tile.rs",
            ),
            "kafka-integration": (
                "scripts/verify/foundation-kafka-live.sh",
                "platforms/intelligence-platform/docker/c2-event-backbone.compose.yml",
                "platforms/foundation-platform/crates/foundation-outbox/src/kafka_broadcaster.rs",
                "platforms/foundation-platform/schemas/foundation-platform.catalog.collection-raw-written.v1.avsc",
            ),
            "compose-smoke": (
                "platforms/foundation-platform/.dockerignore",
                "platforms/foundation-platform/docker-compose.yml",
                "platforms/foundation-platform/infra/compose/bootstrap-foundation.sql",
                "platforms/foundation-platform/migrations/20260719000001_foundation_platform_schema.sql",
                "platforms/foundation-platform/services/foundation-api/Dockerfile",
            ),
            "static-release-toolchain-windows": (
                "platforms/foundation-platform/config/static-release-toolchain.contract.json",
                "scripts/tiles/static_release_toolchain_contract.py",
                "platforms/foundation-platform/services/foundation-outbox-publisher/src/static_release_toolchain.rs",
                "platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs",
            ),
        }

        self.assertEqual(set(witnesses), set(scope.GATES))
        for gate, paths in witnesses.items():
            with self.subTest(gate=gate):
                self.assertTrue(all(gate in scope.classify_paths([path]) for path in paths))

    def test_unrelated_product_change_selects_no_heavy_gate(self) -> None:
        self.assertEqual(
            scope.classify_paths(["products/gongzzang/apps/web/src/app/page.tsx"]),
            set(),
        )

    def test_any_lockfile_and_the_workflow_select_every_gate(self) -> None:
        for path in (
            "Cargo.lock",
            "platforms/foundation-platform/Cargo.lock",
            "services/example/pnpm-lock.yaml",
            ".github/workflows/foundation-ci.yml",
        ):
            with self.subTest(path=path):
                self.assertEqual(scope.classify_paths([path]), set(scope.GATES))

    def test_manual_dispatch_and_uncomparable_push_fail_open_to_all_gates(self) -> None:
        self.assertEqual(
            scope.selected_gates(event_name="workflow_dispatch", base="", head=""),
            set(scope.GATES),
        )
        self.assertEqual(
            scope.selected_gates(event_name="push", base="0" * 40, head="deadbeef"),
            set(scope.GATES),
        )

    def test_removing_a_required_route_is_detected(self) -> None:
        mutations = (
            ("boundary-slice", "prefixes", "scripts/tiles/"),
            (
                "boundary-slice",
                "prefixes",
                "platforms/foundation-platform/crates/foundation-outbox/",
            ),
            (
                "kafka-integration",
                "exact",
                "platforms/intelligence-platform/docker/c2-event-backbone.compose.yml",
            ),
            (
                "compose-smoke",
                "prefixes",
                "platforms/foundation-platform/infra/compose/",
            ),
            (
                "compose-smoke",
                "exact",
                "platforms/foundation-platform/.dockerignore",
            ),
            (
                "static-release-toolchain-windows",
                "exact",
                "platforms/foundation-platform/config/static-release-toolchain.contract.json",
            ),
        )
        for gate, kind, route in mutations:
            rules = copy.deepcopy(scope.RULES)
            rules[gate][kind].remove(route)
            with self.subTest(gate=gate), self.assertRaisesRegex(
                ValueError, "required witness is not covered"
            ):
                scope.validate_rules(rules)

    def test_git_diff_reports_both_sides_of_a_rename(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory)
            subprocess.run(["git", "init", "-q", repository], check=True)
            subprocess.run(
                ["git", "-C", repository, "config", "user.email", "ci@example.test"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", repository, "config", "user.name", "CI test"], check=True
            )
            old_path = repository / "scripts" / "tiles" / "proof.sh"
            old_path.parent.mkdir(parents=True)
            old_path.write_text("old\n", encoding="utf-8")
            subprocess.run(["git", "-C", repository, "add", "scripts/tiles/proof.sh"], check=True)
            subprocess.run(["git", "-C", repository, "commit", "-qm", "old"], check=True)
            base = subprocess.run(
                ["git", "-C", repository, "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            new_path = repository / "docs" / "proof.sh"
            new_path.parent.mkdir()
            old_path.rename(new_path)
            subprocess.run(["git", "-C", repository, "add", "scripts/tiles/proof.sh", "docs/proof.sh"], check=True)
            subprocess.run(["git", "-C", repository, "commit", "-qm", "rename"], check=True)
            head = subprocess.run(
                ["git", "-C", repository, "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

            self.assertEqual(
                scope.git_changed_paths(repository, base, head),
                {"scripts/tiles/proof.sh", "docs/proof.sh"},
            )


class FoundationWorkflowScopeContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = FOUNDATION_WORKFLOW.read_text(encoding="utf-8")

    def test_required_foundation_still_aggregates_exactly_seven_jobs(self) -> None:
        self.assertIn(
            "needs: [rust-quality, static-release-toolchain-windows, supply-chain, "
            "postgres-integration, kafka-integration, compose-smoke, boundary-slice]",
            self.workflow,
        )
        self.assertEqual(self.workflow.count("REQUIRED_RESULT_"), 7)

    def test_each_heavy_job_uses_the_scope_selector_and_success_guard(self) -> None:
        for gate in scope.GATES:
            with self.subTest(gate=gate):
                section = self._job_section(gate)
                self.assertIn("scripts/ci/foundation_ci_scope.py", section)
                self.assertIn(f"--gate {gate}", section)
                self._assert_all_post_selector_steps_are_guarded(gate, section)

    def test_unguarded_post_selector_step_is_rejected(self) -> None:
        gate = "compose-smoke"
        section = self._job_section(gate)
        guard = 'if [ "${FOUNDATION_CI_GATE_SELECTED:-false}" != true ]; then'
        mutated = section.replace(guard, "if false; then", 1)
        self.assertNotEqual(section, mutated)
        with self.assertRaises(AssertionError):
            self._assert_all_post_selector_steps_are_guarded(gate, mutated)

    def test_post_selector_action_step_is_rejected(self) -> None:
        gate = "compose-smoke"
        section = self._job_section(gate)
        mutated = section + "      - uses: example/action@" + "1" * 40 + "\n"
        with self.assertRaises(AssertionError):
            self._assert_all_post_selector_steps_are_guarded(gate, mutated)

    def test_work_before_the_guard_is_rejected(self) -> None:
        gate = "compose-smoke"
        section = self._job_section(gate)
        guard = 'if [ "${FOUNDATION_CI_GATE_SELECTED:-false}" != true ]; then'
        mutated = section.replace(guard, f"echo UNGUARDED_WORK\n          {guard}", 1)
        self.assertNotEqual(section, mutated)
        with self.assertRaises(AssertionError):
            self._assert_all_post_selector_steps_are_guarded(gate, mutated)

    def _job_section(self, gate: str) -> str:
        start = self.workflow.index(f"  {gate}:\n")
        following = re.search(r"(?m)^  [a-z0-9-]+:\s*$", self.workflow[start + 3 :])
        end = start + 3 + following.start() if following else None
        return self.workflow[start:end]

    def _assert_all_post_selector_steps_are_guarded(
        self, gate: str, section: str
    ) -> None:
        steps = re.split(r"(?m)(?=^      - )", section)
        selector = next(
            index
            for index, step in enumerate(steps)
            if "scripts/ci/foundation_ci_scope.py" in step
        )
        for step in steps[selector + 1 :]:
            if not step.strip():
                continue
            self.assertNotRegex(
                step,
                r"(?m)^(?:      - |        )uses:",
                "post-selector action steps cannot be bypassed by the gate guard",
            )
            if not re.search(r"(?m)^        run:", step):
                continue
            guard = (
                'if ($env:FOUNDATION_CI_GATE_SELECTED -ne "true") {'
                if gate == "static-release-toolchain-windows"
                else 'if [ "${FOUNDATION_CI_GATE_SELECTED:-false}" != true ]; then'
            )
            run_body = step.split("        run:", 1)[1]
            commands = [
                line.strip()
                for line in run_body.splitlines()[1:]
                if line.strip() and not line.strip().startswith("#")
            ]
            self.assertTrue(commands, "post-selector run step must have a command")
            self.assertEqual(
                commands[0],
                guard,
                "the unaffected guard must be the first executable statement",
            )
            self.assertIn(
                guard,
                step,
                "every run step after the selector must guard unaffected inputs",
            )
            self.assertRegex(
                step,
                rf"(?s){re.escape(guard)}.*?exit 0",
                "the unaffected guard must terminate the step successfully",
            )


if __name__ == "__main__":
    unittest.main()
