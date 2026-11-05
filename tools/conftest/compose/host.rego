# What every service of a Compose project placed by tools/host-memory-budget.contract.json states,
# judged on the project as the pinned `docker compose config` merges it (every profile enabled;
# scripts/guard/every-container-has-a-memory-cap.sh runs it, root ADR-0422). The budget arithmetic
# over these caps stays in that guard.
#
# Prevents:
# - a container with no memory cap: on 2026-10-01 Trino sized its heap from the whole host and held
#   27.7GB of ai-server's 62GB while idle, beside the Spark loads and the data catalog (root
#   ADR-0118 §6);
# - an uncapped json-file log: on 2026-10-02 one reached 32GB and filled the root disk under the
#   production database twice;
# - a service that stays up but does not restart: the data catalog's Kafka broker stopped on that
#   full disk and stayed stopped;
# - a fixed container_name on a batch service: on 2026-10-29 the by-PNU bake and the deploy's
#   lakehouse migration both started `foundation-platform-lakehouse-target-init`, and Docker
#   refused the second (scripts/guard/batch-containers-are-run-scoped.sh, retired).
#
# `data.context`: `parameters` (the contract-bound memory parameter names), `on_host` (the project
# runs on the shared host, not in `not_on_host`) and `active_profiles` (the profiles the host
# enables for it).
package compose_host

import rego.v1

restarts := {"unless-stopped", "always", "on-failure"}

standing := {"unless-stopped", "always"}

literal_cap := `^[0-9]+[kmgKMG]$`

bound_cap := `^\$\{([A-Z_][A-Z0-9_]*):\?[^${}\r\n]+\}[kmg]$`

deny contains msg if {
	some name, service in input.services
	not valid_cap(object.get(service, "mem_limit", null))
	msg := sprintf("service %s states no valid literal or contract-bound required mem_limit (%v)", [name, object.get(service, "mem_limit", null)])
}

valid_cap(cap) if {
	is_string(cap)
	regex.match(literal_cap, cap)
}

valid_cap(cap) if {
	is_string(cap)
	[_, parameter] := regex.find_all_string_submatch_n(bound_cap, cap, 1)[0]
	parameter in data.context.parameters
}

deny contains msg if {
	some name, service in input.services
	not log_capped(object.get(service, "logging", null))
	msg := sprintf("service %s does not cap its log (logging: json-file with options max-size: <n>m and max-file: <n>)", [name])
}

log_capped(logging) if {
	logging.driver == "json-file"
	object.keys(logging.options) == {"max-size", "max-file"}
	regex.match(`^[0-9]+m$`, logging.options["max-size"])
	regex.match(`^[0-9]+$`, logging.options["max-file"])
}

# Services another service waits on to complete are one-shot jobs, whatever their restart says.
jobs contains dependency if {
	some _, service in input.services
	some dependency, condition in object.get(service, "depends_on", {})
	condition.condition == "service_completed_successfully"
}

active(service) if object.get(service, "profiles", null) == null

active(service) if count(service.profiles) == 0

active(service) if {
	some profile in service.profiles
	profile in data.context.active_profiles
}

stays_up(name, service) if {
	object.get(service, "restart", null) != "no"
	not name in jobs
}

deny contains msg if {
	data.context.on_host
	some name, service in input.services
	active(service)
	stays_up(name, service)
	not object.get(service, "restart", null) in restarts
	msg := sprintf("service %s stays up on the host but does not restart (restart: unless-stopped)", [name])
}

deny contains msg if {
	data.context.on_host
	some name, service in input.services
	service.container_name
	not object.get(service, "restart", null) in standing
	msg := sprintf("service %s has container_name %s but does not stay up; a batch container takes the run's one-off name", [name, service.container_name])
}
