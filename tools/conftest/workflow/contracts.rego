# Repository contracts for the GitHub Actions workflows under .github/workflows, evaluated by
# `conftest test --combine` (scripts/guard/check-workflow-policy.sh) over the documents Conftest's
# YAML parser reads (root ADR-0422). Every rule reads parsed values, so quoting, flow maps, anchors
# and key spacing cannot hide a key from it. General Actions security (pinned SHAs and digests,
# persisted credentials, dangerous triggers) is zizmor's (scripts/guard/zizmor-workflows.sh).
#
# What each family prevents:
# - permissions: a public workflow's token writing to the repository. Every map is exactly
#   `contents: read`; four reviewed jobs add one named write (root ADR-0269, ADR-0271, ADR-0276,
#   ADR-0421), each only in its own file and on its own triggers.
# - secrets/vars/OIDC: a public workflow reading a secret a fork pull request could exfiltrate. The
#   one exception is foundation-ci.yml's three R2 compiler-cache names (root ADR-0294).
# - triggers and the terminal job: a required check that never starts in the merge queue (root
#   ADR-0155), or a gate that reports green without its jobs (identity-ci once named one test inside
#   one lane target, so `live_provisioning` never ran — root ADR-0011).
# - contexts and the Action allowlist: the ruleset requiring a check no workflow reports, or a
#   third-party Action outside the audited list (tools/github/selected-actions.json).
#
# `data.context`: `area_dirs` (tools/areas.contract.json), `required_contexts` (the main ruleset's
# required/* contexts) and `selected_actions` (the audited third-party Actions).
package workflow

import rego.v1

trusted_runners := {"ubuntu-24.04", "windows-2022"}

terminal_run := "bash scripts/ci/require-successful-needs.sh"

scoped_terminal_if := "${{ always() && (needs.scope.result != 'success' || needs.scope.outputs.affected == 'true') }}"

sccache_secrets := {"SCCACHE_R2_ENDPOINT", "SCCACHE_R2_ACCESS_KEY_ID", "SCCACHE_R2_SECRET_ACCESS_KEY"}

foundation_gate_if := "${{ env.FOUNDATION_CI_GATE_SELECTED == 'true' }}"

main_only_if := "${{ github.ref == 'refs/heads/main' }}"

cache_writer_if := "github.event_name == 'push' || github.event_name == 'schedule'"

# Conditional steps are reviewed diagnostic, cleanup or cache steps; a gate step cannot be changed
# to `if: false` and still report green. Each pair is a step name and its exact condition.
conditional_steps := {
	["Upload supply-chain artifacts", "always()"],
	["Clean Compose resources", "always()"],
	["Clean Kafka compose resources", "always()"],
	["Dump API log on failure", "failure()"],
	["Prepare npm advisory fix", "failure()"],
	["Upload Playwright report (on failure)", "failure()"],
	["Check generated public-data catalog", "${{ !cancelled() }}"],
	["Check generated pipeline map and API example", "${{ !cancelled() }}"],
	["Check documents", "${{ !cancelled() }}"],
	["Check ADR names, headings and numbers", "${{ !cancelled() }}"],
	["Publish document reports to the job summary", "${{ !cancelled() }}"],
	["Set up Rust", foundation_gate_if],
	["Rust cache", foundation_gate_if],
	["Set up pnpm", foundation_gate_if],
	["Set up Node", foundation_gate_if],
	["Locate pnpm store", foundation_gate_if],
	["Restore pnpm store", foundation_gate_if],
	["Save pnpm store", main_only_if],
	["Save sqlx-cli", main_only_if],
	["Fill pinned release binaries", cache_writer_if],
	["Save pinned release binaries", cache_writer_if],
	["Restore apt package cache", foundation_gate_if],
	["Save apt package cache", apt_cache_save_if],
}

# scripts/ci/apt-install.sh asks for a save only when it downloaded a set the restored cache lacked.
apt_cache_save_if := sprintf("${{ always() && env.PERFECTORY_APT_CACHE_SAVE != '' && (%s) }}", [cache_writer_if])

# --- the parsed workflows -----------------------------------------------------------------------

workflows contains [path, document] if {
	some entry in input
	path := entry.path
	document := entry.contents
}

file_name(path) := name if {
	parts := split(replace(path, "\\", "/"), "/")
	name := parts[count(parts) - 1]
}

# A YAML 1.1 parser reads the bare key `on` as the boolean true.
triggers(document) := object.get(document, "on", object.get(document, "true", null))

trigger_names(document) := {name | some name, _ in triggers(document)}

jobs(document) := object.get(document, "jobs", {})

steps(job) := object.get(job, "steps", [])

schedule_only(document) if {
	names := trigger_names(document)
	"schedule" in names
	count(names - {"schedule", "workflow_dispatch"}) == 0
}

release_trigger(document) if {
	trigger_names(document) - {"workflow_dispatch"} == {"push"}
	triggers(document).push == {"branches": ["main"]}
	object.get(triggers(document), "workflow_dispatch", null) == null
}

extra_writes(path, document) := {"issues"} if {
	file_name(path) == "dependency-advisories.yml"
	schedule_only(document)
} else := {"id-token", "attestations", "packages"} if {
	file_name(path) == "foundation-release-build.yml"
	release_trigger(document)
} else := {"actions"} if {
	file_name(path) in {"ci-cache-budget.yml", "rerun-runner-failures.yml"}
	schedule_only(document)
} else := set()

release_signer(path, document) if "id-token" in extra_writes(path, document)

# A report workflow (schedule and workflow_dispatch only, root ADR-0203) and the release build gate
# no pull request, so they carry no PR trigger and no required/* context.
scheduled(path, document) if schedule_only(document)

scheduled(path, document) if release_signer(path, document)

# --- permissions ----------------------------------------------------------------------------------

deny contains msg if {
	some [path, document] in workflows
	object.get(document, "permissions", null) != {"contents": "read"}
	msg := sprintf("%s: the top-level permissions must be exactly {contents: read}", [path])
}

deny contains msg if {
	some [path, document] in workflows
	some id, job in jobs(document)
	permissions := job.permissions
	not job_permissions_allowed(permissions, extra_writes(path, document))
	msg := sprintf("%s: job %s permissions must be contents: read plus only this file's reviewed write", [path, id])
}

job_permissions_allowed(permissions, extra) if {
	is_object(permissions)
	permissions.contents == "read"
	every key, value in object.remove(permissions, ["contents"]) {
		key in extra
		value == "write"
	}
}

deny contains msg if {
	some [path, document] in workflows
	some id, job in jobs(document)
	permissions := object.get(job, "permissions", {})
	is_object(permissions)
	("id-token" in object.keys(permissions)) != ("attestations" in object.keys(permissions))
	msg := sprintf("%s: job %s: a signing job takes id-token: write and attestations: write together", [path, id])
}

deny contains msg if {
	some [path, document] in workflows
	some id, job in jobs(document)
	permissions := object.get(job, "permissions", {})
	is_object(permissions)
	"packages" in object.keys(permissions)
	not "id-token" in object.keys(permissions)
	msg := sprintf("%s: job %s: only the signing job may write the release package", [path, id])
}

deny contains msg if {
	some [path, document] in workflows
	signers := {id |
		some id, job in jobs(document)
		permissions := object.get(job, "permissions", {})
		is_object(permissions)
		"id-token" in object.keys(permissions)
	}
	count(signers) > 1
	msg := sprintf("%s: only one job may sign the release (%v)", [path, signers])
}

# --- continue-on-error, fake-pass fallbacks, defaults ---------------------------------------------

# A command that swallows its own failure (`cmd || echo "not installed"`, `cmd || true`) turns a red
# step green; the two Gongzzang hooks that carried it passed every push without running. The same
# fallbacks are refused in lefthook.yml (tools/conftest/lefthook).
fake_pass_fallbacks := ["|| true", "|| echo", "CI enforces"]

deny contains msg if {
	some [path, document] in workflows
	walk(document, [location, value])
	count(location) > 0
	location[count(location) - 1] == "run"
	is_string(value)
	some line in split(value, "\n")
	not startswith(trim_space(line), "#")
	some fallback in fake_pass_fallbacks
	contains(line, fallback)
	msg := sprintf("%s: a run hides its own failure with `%s` (%v)", [path, fallback, location])
}

# Debian packages come through scripts/ci/apt-install.sh, which skips what the runner has, installs
# from the package cache, probes mirrors and retries. On 2026-11-11 the runners could not resolve any
# ubuntu.com mirror and every bare `apt-get` in CI ran past 20 minutes and failed (#1267, #1313,
# #1323, #1329).
deny contains msg if {
	some [path, document] in workflows
	walk(document, [location, value])
	count(location) > 0
	location[count(location) - 1] == "run"
	is_string(value)
	some line in split(value, "\n")
	not startswith(trim_space(line), "#")
	regex.match(`(^|[^A-Za-z0-9_./-])apt(-get|-fast)?[ \t]`, line)
	msg := sprintf("%s: install Debian packages with bash \"$GITHUB_WORKSPACE/scripts/ci/apt-install.sh\", not apt (%v)", [path, location])
}

# Playwright's `--with-deps` and `install-deps` run their own `apt-get update`; its `--dry-run` only
# names the packages, for apt-install.sh to install.
deny contains msg if {
	some [path, document] in workflows
	walk(document, [location, value])
	count(location) > 0
	location[count(location) - 1] == "run"
	is_string(value)
	some line in split(value, "\n")
	not startswith(trim_space(line), "#")
	playwright_runs_apt(line)
	msg := sprintf("%s: Playwright installs no system packages itself; pass `install-deps --dry-run`'s list to scripts/ci/apt-install.sh (%v)", [path, location])
}

playwright_runs_apt(line) if contains(line, "--with-deps")

playwright_runs_apt(line) if {
	contains(line, "install-deps")
	not contains(line, "--dry-run")
}

deny contains msg if {
	some [path, document] in workflows
	walk(document, [location, _])
	count(location) > 0
	location[count(location) - 1] == "continue-on-error"
	msg := sprintf("%s: required workflows must not continue on error (%v)", [path, location])
}

deny contains msg if {
	some [path, document] in workflows
	some [where, defaults] in defaults_of(document)
	not admitted_defaults(defaults)
	msg := sprintf("%s: %s defaults must be exactly run.working-directory set to an area directory", [path, where])
}

defaults_of(document) := {entry |
	some entry in array.concat(
		[["top-level", document.defaults] | document.defaults],
		[[sprintf("job %s", [id]), job.defaults] | some id, job in jobs(document); job.defaults],
	)
}

admitted_defaults(defaults) if {
	object.keys(defaults) == {"run"}
	object.keys(defaults.run) == {"working-directory"}
	defaults.run["working-directory"] in {dir | some dir in data.context.area_dirs}
}

# --- secrets, variables and the OIDC channel --------------------------------------------------------

expressions(text) := [match[1] | some match in regex.find_all_string_submatch_n(`(?s)\$\{\{(.*?)\}\}`, text, -1)]

deny contains msg if {
	some [path, document] in workflows
	walk(document, [_, value])
	is_string(value)
	contains(value, "ACTIONS_ID_TOKEN")
	msg := sprintf("%s: public workflows must not reach the OIDC token channel", [path])
}

deny contains msg if {
	some [path, document] in workflows
	walk(document, [_, value])
	is_string(value)
	some expression in expressions(value)
	regex.match(`(^|[^A-Za-z0-9_])vars([^A-Za-z0-9_]|$)`, expression)
	msg := sprintf("%s: public workflows must not reference repository variables (%s)", [path, expression])
}

deny contains msg if {
	some [path, document] in workflows
	walk(document, [_, value])
	is_string(value)
	some expression in expressions(value)
	regex.match(`(^|[^A-Za-z0-9_])secrets([^A-Za-z0-9_]|$)`, expression)
	not admitted_secret_expression(path, expression)
	msg := sprintf("%s: public workflows must not reference secrets (%s)", [path, expression])
}

# Every mention of `secrets` in the expression is one of the three cache names, in this one file.
admitted_secret_expression(path, expression) if {
	file_name(path) == "foundation-ci.yml"
	mentions := regex.find_all_string_submatch_n(`(^|[^A-Za-z0-9_])secrets([^A-Za-z0-9_]|$)`, expression, -1)
	names := regex.find_all_string_submatch_n(`secrets\.([A-Za-z0-9_]+)`, expression, -1)
	count(mentions) == count(names)
	every name in names {
		name[1] in sccache_secrets
	}
}

# --- triggers -------------------------------------------------------------------------------------

deny contains msg if {
	some [path, document] in workflows
	not is_object(triggers(document))
	msg := sprintf("%s: on must be a mapping of events", [path])
}

deny contains msg if {
	some [path, document] in workflows
	"pull_request" in trigger_names(document)
	triggers(document).pull_request != {"branches": ["main"]}
	msg := sprintf("%s: pull_request allows only branches: [main], so no filter can suppress a required check", [path])
}

deny contains msg if {
	some [path, document] in workflows
	"merge_group" in trigger_names(document)
	triggers(document).merge_group != null
	msg := sprintf("%s: merge_group admits no filter; it must run for every queued group", [path])
}

deny contains msg if {
	some [path, document] in workflows
	not scheduled(path, document)
	some event in ["pull_request", "merge_group"]
	not event in trigger_names(document)
	msg := sprintf("%s: a required workflow must run on %s (root ADR-0155)", [path, event])
}

# --- steps and runners ----------------------------------------------------------------------------

deny contains msg if {
	some [path, document] in workflows
	some id, job in jobs(document)
	some step in steps(job)
	condition := step["if"]
	not [object.get(step, "name", ""), condition] in conditional_steps
	msg := sprintf("%s: job %s: conditional step is not allowlisted: %v / %v", [path, id, object.get(step, "name", ""), condition])
}

deny contains msg if {
	some [path, document] in workflows
	some id, job in jobs(document)
	not object.get(job, "runs-on", null) in trusted_runners
	msg := sprintf("%s: job %s must use a literal trusted hosted runner (%v)", [path, id, object.get(job, "runs-on", null)])
}

# --- the required/* context and the terminal job ----------------------------------------------------

required_jobs(document) := {id |
	some id, job in jobs(document)
	is_string(job.name)
	startswith(job.name, "required/")
}

deny contains msg if {
	some [path, document] in workflows
	count(jobs(document)) == 0
	msg := sprintf("%s: has no jobs", [path])
}

deny contains msg if {
	some [path, document] in workflows
	scheduled(path, document)
	count(required_jobs(document)) != 0
	msg := sprintf("%s: a workflow that gates no pull request must not define a required/* context", [path])
}

deny contains msg if {
	some [path, document] in workflows
	not scheduled(path, document)
	count(required_jobs(document)) != 1
	msg := sprintf("%s: must define exactly one job-level required/* context", [path])
}

deny contains msg if {
	some [path, document] in workflows
	count(jobs(document)) == 1
	some id in required_jobs(document)
	jobs(document)[id]["if"]
	msg := sprintf("%s: the single required job must not be conditional", [path])
}

# The terminal job of a multi-job workflow: the only required/* job, it needs every other job and
# fails unless each one succeeded (scripts/ci/require-successful-needs.sh).
terminal(path, document) := [id, jobs(document)[id]] if {
	not scheduled(path, document)
	count(jobs(document)) > 1
	count(required_jobs(document)) == 1
	some id in required_jobs(document)
}

others(document, terminal_id) := {id | some id, _ in jobs(document); id != terminal_id}

scoped(document) if jobs(document).scope

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	scoped(document)
	object.get(job, "if", null) != scoped_terminal_if
	msg := sprintf("%s: a scoped terminal job must use the exact if: of root ADR-0324", [path])
}

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	not scoped(document)
	object.get(job, "if", null) != "${{ always() }}"
	msg := sprintf("%s: the terminal job must use exactly if: ${{ always() }}", [path])
}

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	some key in ["strategy", "env"]
	job[key]
	msg := sprintf("%s: the terminal job must not define %s", [path, key])
}

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	not terminal_steps(steps(job))
	msg := sprintf("%s: the terminal job must have one checkout step and one result step {name, env, working-directory: ., run: %s}", [path, terminal_run])
}

terminal_steps(all) if {
	count(all) == 2
	count([step | some step in all; checkout_step(step)]) == 1
	count([step | some step in all; result_step(step)]) == 1
}

checkout_step(step) if {
	object.keys(step) == {"uses", "with"}
	regex.match(`^actions/checkout@[0-9a-f]+$`, step.uses)
}

result_step(step) if {
	object.keys(step) == {"name", "env", "working-directory", "run"}
	step.run == terminal_run
	step["working-directory"] == "."
}

needs_list(job) := job.needs if is_array(job.needs)

needs_list(job) := [job.needs] if is_string(job.needs)

needs_list(job) := [] if not job.needs

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	needs := needs_list(job)
	not exact_needs(needs, others(document, id))
	msg := sprintf("%s: the terminal job must need every other job exactly once (needs %v, jobs %v)", [path, needs, others(document, id)])
}

exact_needs(needs, expected) if {
	count(needs) == count({need | some need in needs})
	{need | some need in needs} == expected
}

result_variable(id) := concat("", ["REQUIRED_RESULT_", upper(replace(id, "-", "_"))])

result_value(env, id) if regex.match(sprintf(`^\$\{\{\s*needs\.%s\.result\s*\}\}$`, [id]), env[result_variable(id)])

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	some step in steps(job)
	result_step(step)
	some other in others(document, id)
	not result_value(step.env, other)
	msg := sprintf("%s: terminal result mapping drift for %s (expected %s: ${{ needs.%s.result }})", [path, other, result_variable(other), other])
}

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	some step in steps(job)
	result_step(step)
	some key, _ in step.env
	not admitted_terminal_variable(document, id, key)
	msg := sprintf("%s: the terminal step passes %s, which is no result mapping or scope answer", [path, key])
}

admitted_terminal_variable(document, terminal_id, key) if {
	some other in others(document, terminal_id)
	key == result_variable(other)
}

admitted_terminal_variable(document, _, "CHANGE_SCOPE_RUN") if scoped(document)

admitted_terminal_variable(document, _, "CHANGE_SCOPE_SKIPPED") if scoped(document)

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	scoped(document)
	some step in steps(job)
	result_step(step)
	not regex.match(`^\$\{\{\s*needs\.scope\.outputs\.affected\s*\}\}$`, object.get(step.env, "CHANGE_SCOPE_RUN", ""))
	msg := sprintf("%s: the terminal step must pass CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.affected }} (root ADR-0202)", [path])
}

deny contains msg if {
	some [path, document] in workflows
	[id, job] := terminal(path, document)
	some step in steps(job)
	result_step(step)
	skipped := step.env.CHANGE_SCOPE_SKIPPED
	not regex.match(`^\$\{\{\s*needs\.scope\.outputs\.skipped\s*\}\}$`, skipped)
	msg := sprintf("%s: CHANGE_SCOPE_SKIPPED must be ${{ needs.scope.outputs.skipped }} (root ADR-0324)", [path])
}

# --- contexts and Actions, across every workflow ------------------------------------------------------

reported_contexts contains name if {
	some [_, document] in workflows
	some id in required_jobs(document)
	name := jobs(document)[id].name
}

required_contexts := {context | some context in data.context.required_contexts}

deny contains msg if {
	count(data.context.required_contexts) == 0
	msg := "the main ruleset requires no required/* context"
}

deny contains msg if {
	count(data.context.required_contexts) != count(required_contexts)
	msg := "the main ruleset lists a required/* context twice"
}

deny contains msg if {
	missing := required_contexts - reported_contexts
	count(missing) > 0
	msg := sprintf("the ruleset requires contexts no workflow job reports: %v", [missing])
}

deny contains msg if {
	extra := reported_contexts - required_contexts
	count(extra) > 0
	msg := sprintf("workflow jobs report required/* contexts the ruleset does not require: %v", [extra])
}

uses_of(document) := {action |
	some _, job in jobs(document)
	action := job.uses
} | {action |
	some _, job in jobs(document)
	some step in steps(job)
	action := step.uses
}

deny contains msg if {
	some [path, document] in workflows
	some action in uses_of(document)
	not is_string(action)
	msg := sprintf("%s: uses must be a string (%v)", [path, action])
}

deny contains msg if {
	some [path, document] in workflows
	some action in uses_of(document)
	action == ""
	msg := sprintf("%s: empty uses value", [path])
}

deny contains msg if {
	some [path, document] in workflows
	some action in uses_of(document)
	startswith(action, "./")
	msg := sprintf("%s: local Actions are not admitted by the audited allowlist (%s)", [path, action])
}

# Pinning every Action and docker:// image is zizmor's `unpinned-uses`; a docker:// action is not an
# allowlisted Action, and GitHub-owned Actions are admitted by the repository's Actions policy.
third_party_actions contains action if {
	some [_, document] in workflows
	some action in uses_of(document)
	is_string(action)
	action != ""
	not startswith(action, "./")
	not startswith(action, "docker://")
	not startswith(action, "actions/")
	not startswith(action, "github/")
}

selected_actions := {action | some action in data.context.selected_actions}

deny contains msg if {
	unlisted := third_party_actions - selected_actions
	count(unlisted) > 0
	msg := sprintf("workflows use third-party Actions outside tools/github/selected-actions.json: %v", [unlisted])
}

deny contains msg if {
	unused := selected_actions - third_party_actions
	count(unused) > 0
	msg := sprintf("tools/github/selected-actions.json admits Actions no workflow uses: %v", [unused])
}
