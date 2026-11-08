# `conftest verify --policy tools/conftest/workflow`: each test plants one incident in a parsed
# workflow (as `conftest test --combine` hands it: a list of {path, contents}) or shows an accepted
# form. The end-to-end cases through actionlint and the parser are scripts/guard/workflow-policy-self-test.sh.
package workflow

import rego.v1

ctx := {
	"area_dirs": ["products/example"],
	"required_contexts": ["required/example"],
	"selected_actions": ["example/tool@1111111111111111111111111111111111111111"],
}

checkout := {"uses": "actions/checkout@2222222222222222222222222222222222222222", "with": {"persist-credentials": false}}

terminal_result_step := {
	"name": "Require every gate",
	"env": {"REQUIRED_RESULT_BUILD": "${{ needs.build.result }}", "REQUIRED_RESULT_TEST": "${{ needs.test.result }}"},
	"working-directory": ".",
	"run": "bash scripts/ci/require-successful-needs.sh",
}

# Conftest's YAML parser reads the bare `on` key as `true`; the fixture keeps that shape.
valid := {
	"name": "synthetic",
	"true": {"pull_request": {"branches": ["main"]}, "merge_group": null, "push": {"branches": ["main"]}},
	"permissions": {"contents": "read"},
	"defaults": {"run": {"working-directory": "products/example"}},
	"jobs": {
		"build": {"name": "Build", "runs-on": "ubuntu-24.04", "steps": [
			object.union(checkout, {"name": "Checkout"}),
			{"name": "Tool", "uses": "example/tool@1111111111111111111111111111111111111111"},
		]},
		"test": {"name": "Test", "runs-on": "windows-2022", "steps": [{"run": "echo synthetic"}]},
		"required": {
			"name": "required/example", "if": "${{ always() }}", "needs": ["build", "test"],
			"runs-on": "ubuntu-24.04", "steps": [checkout, terminal_result_step],
		},
	},
}

one(document) := [{"path": ".github/workflows/example.yml", "contents": document}]

named(name, document) := [{"path": concat("", [".github/workflows/", name]), "contents": document}]

with_job(id, field, value) := json.patch(valid, [{"op": "add", "path": sprintf("/jobs/%s/%s", [id, field]), "value": value}])

refused(inputs) if count(deny) > 0 with input as inputs with data.context as ctx

accepted(inputs) if count(deny) == 0 with input as inputs with data.context as ctx

test_valid_workflow_is_accepted if accepted(one(valid))

test_untrusted_runner_is_refused if refused(one(with_job("test", "runs-on", "macos-15")))

test_self_hosted_list_runner_is_refused if refused(one(with_job("test", "runs-on", ["self-hosted"])))

test_write_all_is_refused if refused(one(object.union(valid, {"permissions": "write-all"})))

test_contents_write_is_refused if refused(one(object.union(valid, {"permissions": {"contents": "write"}})))

test_job_write_permission_is_refused if {
	refused(one(with_job("build", "permissions", {"contents": "read", "pull-requests": "write"})))
}

test_missing_top_level_permissions_is_refused if refused(one(object.remove(valid, ["permissions"])))

test_continue_on_error_is_refused if refused(one(with_job("test", "continue-on-error", true)))

test_echo_fallback_is_refused if {
	refused(one(with_job("test", "steps", [{"run": "cargo check --workspace || echo \"cargo not installed locally\""}])))
}

test_true_fallback_is_refused if refused(one(with_job("test", "steps", [{"run": "cargo check --workspace || true"}])))

test_fallback_named_in_a_shell_comment_is_accepted if {
	accepted(one(with_job("test", "steps", [{"run": "# `|| true` would hide a real failure\ncargo check || exit 1"}])))
}

test_strict_or_is_accepted if accepted(one(with_job("test", "steps", [{"run": "cargo check --workspace || exit 1"}])))

test_secret_reference_is_refused if {
	refused(one(with_job("test", "steps", [{"run": "echo ${{ secrets.TOKEN }}"}])))
}

test_bracket_secret_reference_is_refused if {
	refused(one(with_job("test", "steps", [{"run": "echo ${{ secrets['TOKEN'] }}"}])))
}

test_repository_variable_is_refused if {
	refused(one(with_job("test", "steps", [{"run": "echo ${{ vars.REGION }}"}])))
}

test_oidc_channel_is_refused if {
	refused(one(with_job("test", "steps", [{"run": "curl \"$ACTIONS_ID_TOKEN_REQUEST_URL\""}])))
}

test_sccache_secrets_are_admitted_only_in_foundation_ci if {
	step := [{"env": {"KEY": "${{ secrets.SCCACHE_R2_ACCESS_KEY_ID }}"}, "run": "echo"}]
	accepted(named("foundation-ci.yml", with_job("test", "steps", step)))
	refused(named("other.yml", with_job("test", "steps", step)))
}

test_another_secret_in_foundation_ci_is_refused if {
	step := [{"env": {"KEY": "${{ secrets.SCCACHE_R2_ACCESS_KEY_ID || secrets.OTHER }}"}, "run": "echo"}]
	refused(named("foundation-ci.yml", with_job("test", "steps", step)))
}

test_missing_pull_request_is_refused if {
	refused(one(json.patch(valid, [{"op": "remove", "path": "/true/pull_request"}])))
}

test_pull_request_path_filter_is_refused if {
	triggers := {"pull_request": {"branches": ["main"], "paths": ["src/**"]}, "merge_group": null}
	refused(one(object.union(valid, {"true": triggers})))
}

test_missing_merge_group_is_refused if {
	refused(one(json.patch(valid, [{"op": "remove", "path": "/true/merge_group"}])))
}

test_merge_group_filter_is_refused if {
	triggers := {"pull_request": {"branches": ["main"]}, "merge_group": {"types": ["checks_requested"]}}
	refused(one(object.union(valid, {"true": triggers})))
}

test_unlisted_conditional_step_is_refused if {
	refused(one(with_job("test", "steps", [{"name": "Gate", "if": "false", "run": "echo"}])))
}

test_listed_conditional_step_is_accepted if {
	accepted(one(with_job("test", "steps", [{"name": "Dump API log on failure", "if": "failure()", "run": "echo"}])))
}

test_unsafe_defaults_are_refused if {
	refused(one(object.union(valid, {"defaults": {"run": {"working-directory": "products/example", "shell": "bash"}}})))
	refused(one(object.union(valid, {"defaults": {"run": {"working-directory": "/tmp"}}})))
}

test_required_step_name_spoof_is_refused if {
	document := json.patch(valid, [
		{"op": "replace", "path": "/jobs/required/name", "value": "Aggregate"},
		{"op": "replace", "path": "/jobs/required/steps/1/name", "value": "required/example"},
	])
	refused(one(document))
}

test_incomplete_terminal_needs_is_refused if refused(one(with_job("required", "needs", ["build"])))

test_unchecked_terminal_result_is_refused if {
	refused(one(json.patch(valid, [{"op": "remove", "path": "/jobs/required/steps/1/env/REQUIRED_RESULT_TEST"}])))
}

test_terminal_extra_step_is_refused if {
	refused(one(with_job("required", "steps", [checkout, terminal_result_step, {"run": "echo"}])))
}

test_terminal_non_root_working_directory_is_refused if {
	refused(one(json.patch(valid, [{"op": "replace", "path": "/jobs/required/steps/1/working-directory", "value": "products"}])))
}

test_terminal_step_condition_is_refused if {
	refused(one(json.patch(valid, [{"op": "add", "path": "/jobs/required/steps/1/if", "value": "${{ always() }}"}])))
}

test_terminal_with_plain_always_in_scoped_workflow_is_refused if {
	scoped := json.patch(valid, [
		{"op": "add", "path": "/jobs/scope", "value": {"name": "Change scope", "runs-on": "ubuntu-24.04", "steps": [{"run": "echo"}]}},
		{"op": "replace", "path": "/jobs/required/needs", "value": ["scope", "build", "test"]},
		{"op": "add", "path": "/jobs/required/steps/1/env/REQUIRED_RESULT_SCOPE", "value": "${{ needs.scope.result }}"},
		{"op": "add", "path": "/jobs/required/steps/1/env/CHANGE_SCOPE_RUN", "value": "${{ needs.scope.outputs.affected }}"},
	])
	refused(one(scoped))
	accepted(one(json.patch(scoped, [{"op": "replace", "path": "/jobs/required/if", "value": scoped_terminal_if}])))
}

test_scope_answer_without_scope_job_is_refused if {
	refused(one(json.patch(valid, [{"op": "add", "path": "/jobs/required/steps/1/env/CHANGE_SCOPE_RUN", "value": "${{ needs.scope.outputs.affected }}"}])))
}

test_single_conditional_required_job_is_refused if {
	single := object.union(valid, {"jobs": {"required": {"name": "required/example", "if": "github.event_name == 'push'", "runs-on": "ubuntu-24.04", "steps": [{"run": "echo"}]}}})
	refused(one(single))
}

test_scheduled_report_has_no_required_context if {
	report := {
		"true": {"schedule": [{"cron": "0 0 * * *"}], "workflow_dispatch": null},
		"permissions": {"contents": "read"},
		"jobs": {"report": {"name": "Report", "runs-on": "ubuntu-24.04", "steps": [{"run": "echo"}]}},
	}
	accepted(array.concat(one(valid), named("report.yml", report)))
	refused(array.concat(one(valid), named("report.yml", json.patch(report, [{"op": "replace", "path": "/jobs/report/name", "value": "required/report"}]))))
}

advisory := {
	"true": {"schedule": [{"cron": "0 0 * * *"}], "workflow_dispatch": null},
	"permissions": {"contents": "read"},
	"jobs": {"issue": {"name": "Issue", "runs-on": "ubuntu-24.04", "permissions": {"contents": "read", "issues": "write"}, "steps": [{"run": "echo"}]}},
}

test_issue_write_only_in_the_advisory_workflow_on_schedule if {
	accepted(array.concat(one(valid), named("dependency-advisories.yml", advisory)))
	refused(array.concat(one(valid), named("other.yml", advisory)))
	refused(array.concat(one(valid), named("dependency-advisories.yml", object.union(advisory, {"true": {"push": {"branches": ["main"]}}}))))
}

release := {
	"true": {"push": {"branches": ["main"]}, "workflow_dispatch": null},
	"permissions": {"contents": "read"},
	"jobs": {"sign": {"name": "Sign", "runs-on": "ubuntu-24.04", "permissions": {"contents": "read", "id-token": "write", "attestations": "write", "packages": "write"}, "steps": [{"run": "echo"}]}},
}

test_one_signing_job_only_in_the_release_build if {
	accepted(array.concat(one(valid), named("foundation-release-build.yml", release)))
	refused(array.concat(one(valid), named("other.yml", release)))
	refused(array.concat(one(valid), named("foundation-release-build.yml", object.union(release, {"true": {"push": {"branches": ["dev"]}}}))))
	refused(array.concat(one(valid), named("foundation-release-build.yml", json.patch(release, [{"op": "remove", "path": "/jobs/sign/permissions/attestations"}]))))
	refused(array.concat(one(valid), named("foundation-release-build.yml", json.patch(release, [{"op": "add", "path": "/jobs/second", "value": release.jobs.sign}]))))
}

test_packages_write_without_signing_is_refused if {
	alone := json.patch(release, [{"op": "replace", "path": "/jobs/sign/permissions", "value": {"contents": "read", "packages": "write"}}])
	refused(array.concat(one(valid), named("foundation-release-build.yml", alone)))
}

test_actions_write_only_in_the_two_schedule_only_files if {
	pruner := json.patch(advisory, [{"op": "replace", "path": "/jobs/issue/permissions", "value": {"contents": "read", "actions": "write"}}])
	accepted(array.concat(one(valid), named("ci-cache-budget.yml", pruner)))
	accepted(array.concat(one(valid), named("rerun-runner-failures.yml", pruner)))
	refused(array.concat(one(valid), named("other.yml", pruner)))
	refused(array.concat(one(valid), named("ci-cache-budget.yml", object.union(pruner, {"true": {"push": {"branches": ["main"]}}}))))
}

test_local_action_is_refused if {
	refused(one(with_job("test", "steps", [{"uses": "./.github/actions/setup"}])))
}

test_unlisted_third_party_action_is_refused if {
	refused(one(with_job("test", "steps", [{"uses": "other/tool@3333333333333333333333333333333333333333"}])))
}

test_allowlisted_action_that_no_workflow_uses_is_refused if {
	unused := object.union(ctx, {"selected_actions": array.concat(ctx.selected_actions, ["unused/tool@4444444444444444444444444444444444444444"])})
	count(deny) > 0 with input as one(valid) with data.context as unused
}

test_ruleset_context_no_workflow_reports_is_refused if {
	drift := object.union(ctx, {"required_contexts": ["required/example", "required/other"]})
	count(deny) > 0 with input as one(valid) with data.context as drift
}

test_duplicate_ruleset_context_is_refused if {
	twice := object.union(ctx, {"required_contexts": ["required/example", "required/example"]})
	count(deny) > 0 with input as one(valid) with data.context as twice
}
