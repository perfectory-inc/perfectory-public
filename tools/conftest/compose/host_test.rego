package compose_host

import rego.v1

ctx := {"parameters": ["TRINO_MEMORY_GB"], "on_host": true, "active_profiles": ["lakehouse-batch"]}

log_cap := {"driver": "json-file", "options": {"max-size": "50m", "max-file": "3"}}

service(fields) := object.union({"image": "example:local", "mem_limit": "512m", "logging": log_cap, "restart": "unless-stopped"}, fields)

project(services) := {"services": services}

refused(document) if count(deny) > 0 with input as document with data.context as ctx

accepted(document) if count(deny) == 0 with input as document with data.context as ctx

test_capped_logged_restarting_service_is_accepted if accepted(project({"api": service({})}))

test_contract_bound_cap_is_accepted if {
	accepted(project({"trino": service({"mem_limit": "${TRINO_MEMORY_GB:?bind the contract}g"})}))
}

test_missing_cap_is_refused if refused(project({"api": object.remove(service({}), ["mem_limit"])}))

test_cap_bound_to_an_unknown_parameter_is_refused if {
	refused(project({"api": service({"mem_limit": "${OTHER_GB:?set it}g"})}))
}

test_cap_with_a_default_is_refused if {
	refused(project({"api": service({"mem_limit": "${TRINO_MEMORY_GB:-8}g"})}))
}

test_uncapped_log_is_refused if {
	refused(project({"api": object.remove(service({}), ["logging"])}))
	refused(project({"api": json.patch(service({}), [{"op": "replace", "path": "/logging", "value": {"driver": "json-file", "options": {"max-file": "3"}}}])}))
	refused(project({"api": json.patch(service({}), [{"op": "replace", "path": "/logging/driver", "value": "local"}])}))
}

test_standing_service_without_restart_is_refused if {
	refused(project({"broker": object.remove(service({}), ["restart"])}))
}

test_one_shot_job_needs_no_restart if {
	accepted(project({"migrate": service({"restart": "no"})}))
	waited := {
		"init": object.remove(service({}), ["restart"]),
		"api": service({"depends_on": {"init": {"condition": "service_completed_successfully", "required": true}}}),
	}
	accepted(project(waited))
}

test_inactive_profile_service_needs_no_restart if {
	accepted(project({"smoke": object.remove(service({"profiles": ["compose-smoke"]}), ["restart"])}))
	refused(project({"batch": object.remove(service({"profiles": ["lakehouse-batch"]}), ["restart"])}))
}

test_fixed_name_on_a_batch_service_is_refused if {
	refused(project({"target-init": service({"restart": "no", "container_name": "foundation-platform-lakehouse-target-init"})}))
}

test_fixed_name_on_a_standing_service_is_accepted if {
	accepted(project({"trino": service({"container_name": "foundation-platform-trino"})}))
}

test_not_on_host_skips_restart_and_name_rules if {
	off_host := object.union(ctx, {"on_host": false})
	count(deny) == 0 with input as project({"job": service({"restart": "no", "container_name": "fixed"})}) with data.context as off_host
}
