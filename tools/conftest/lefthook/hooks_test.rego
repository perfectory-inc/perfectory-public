# `conftest verify --policy tools/conftest/lefthook`: each test plants one fallback in a parsed
# lefthook.yml or shows an accepted form.
package lefthook

import rego.v1

strict := {"pre-push": {"commands": {"cargo-check": {"run": "cargo check --workspace --all-features"}}}}

with_run(hook, run) := {hook: {"commands": {"step": {"run": run}}}}

test_strict_commands_are_accepted if count(deny) == 0 with input as strict

test_echo_fallback_is_refused if {
	count(deny) > 0 with input as with_run("pre-push", "command -v cargo >/dev/null 2>&1 && cargo check || echo \"cargo not installed - CI enforces\"")
}

test_ci_enforces_wording_is_refused if {
	count(deny) > 0 with input as with_run("pre-commit", "gitleaks protect --staged --redact -v || echo \"gitleaks not installed locally - CI enforces\"")
}

test_fallback_named_in_a_shell_comment_is_accepted if {
	count(deny) == 0 with input as with_run("pre-push", "# never `|| true` here\npnpm lint")
}

test_true_fallback_is_refused if count(deny) > 0 with input as with_run("pre-push", "pnpm lint || true")

test_jobs_form_is_refused if {
	count(deny) > 0 with input as {"pre-push": {"jobs": [{"name": "lint", "run": "pnpm lint || true"}]}}
}

test_skip_condition_is_accepted if {
	count(deny) == 0 with input as {"pre-push": {"commands": {"docs": {
		"run": "python3 scripts/catalog/audit-documentation.py --check",
		"skip": ["merge", {"run": "! command -v python3 >/dev/null 2>&1"}],
	}}}}
}

test_tool_absence_skip_wording_is_refused if {
	count(deny) > 0 with input as {"pre-push": {"commands": {"docs": {
		"run": "python3 x.py",
		"skip": [{"run": "echo python3 not installed; exit 0"}],
	}}}}
}
