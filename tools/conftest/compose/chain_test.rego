package compose_chain

import rego.v1

needs(names) := {"depends_on": {name: {"condition": "service_completed_successfully"} | some name in names}}

project := {"services": {
	"postgres": {},
	"foundation-bootstrap": needs(["postgres"]),
	"foundation-migrate": needs(["foundation-bootstrap"]),
	"foundation-runtime-grants": needs(["foundation-migrate"]),
	"foundation-finalize": needs(["foundation-runtime-grants"]),
	"foundation-api": needs(["foundation-finalize", "postgres"]),
	"prometheus": needs(["foundation-api"]),
}}

good := ["docker compose up -d --wait foundation-api", "docker compose up -d postgres"]

refused(commands) if count(deny) > 0 with input as project with data.context.commands as commands

accepted(commands) if count(deny) == 0 with input as project with data.context.commands as commands

test_chain_is_read_from_depends_on if {
	chain == {"postgres", "foundation-bootstrap", "foundation-migrate", "foundation-runtime-grants", "foundation-finalize"} with input as project
}

test_waiting_for_the_terminal_is_accepted if accepted(good)

test_wait_after_the_terminal_is_accepted if accepted(["docker compose up -d foundation-api --wait"])

test_the_2026_09_02_deploy_is_refused if {
	refused(["docker compose up -d --no-deps postgres", "docker compose run --rm foundation-migrate", "docker compose up -d --wait foundation-api"])
}

test_no_deps_alone_is_refused if refused(array.concat(good, ["docker compose up -d --no-deps valkey"]))

test_any_intermediate_link_is_refused if {
	every link in ["foundation-bootstrap", "foundation-runtime-grants", "foundation-finalize"] {
		refused(array.concat(good, [sprintf("docker compose up %s", [link])]))
	}
}

test_a_new_link_is_covered_without_editing_the_policy if {
	longer := json.patch(project, [
		{"op": "add", "path": "/services/foundation-seed", "value": needs(["foundation-finalize"])},
		{"op": "replace", "path": "/services/foundation-api/depends_on", "value": needs(["foundation-seed"]).depends_on},
	])
	count(deny) > 0 with input as longer with data.context.commands as array.concat(good, ["docker compose run foundation-seed"])
}

test_no_wait_is_refused if refused(["docker compose up -d foundation-api"])

test_a_short_chain_is_refused if {
	count(deny) > 0 with input as {"services": {"postgres": {}, "foundation-api": needs(["postgres"])}} with data.context.commands as good
}
