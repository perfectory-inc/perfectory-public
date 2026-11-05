# Container runtime policy for one Compose file, as `docker compose config --no-interpolate
# --format json` normalizes it (scripts/guard/check-container-runtime-policy.sh runs it). The
# engine resolves anchors, aliases, `extends`, `include` and `!reset` first, so every rule reads
# the final service and none of them needs the YAML-syntax bans the hand parser carried.
#
# Prevents: a host-root escape or a moved image. Mounting the engine socket or sharing the host's
# PID/network namespace gives a container root on the host (ADR-0385 kept node-exporter and
# cAdvisor off both for that reason); a published port without a loopback bind serves a dev
# database to the network; a literal secret lands in a public repository; and a tag without a
# digest runs whatever the registry serves next.
#
# The guard runs compose with `--no-env-resolution` (env files are never read, so no local secret
# reaches the document) and `--no-path-resolution` (paths stay as written).
#
# `data.context`: `contract_images` (names the static-release contract projects) and
# `contract_owner_valid` (the guard checked the file's `x-perfectory-contract-image-owner` script).
package compose

import rego.v1

namespaces := {"network_mode", "ipc", "pid", "uts", "userns_mode"}

secret_key := `(?:^|_)(?:PASSWORD|MASTERKEY|SECRET|TOKEN|KEY)(?:$|_)`

required_env := `^\$\{[A-Z][A-Z0-9_]*:\?[^}]+\}$`

safe_bind := `^(?:127\.0\.0\.1|\$\{[A-Z_][A-Z0-9_]*:-127\.0\.0\.1\}):(?:[0-9]+|\$\{[A-Z_][A-Z0-9_]*:-[0-9]+\}):[0-9]+(?:/(?:tcp|udp))?$`

engine_socket := `(?:^|/)(?:var/)?run/docker\.sock(?:$|[:/])|//\./pipe/docker_engine`

contract_image := `^\$\{([A-Z][A-Z0-9_]*IMAGE[A-Z0-9_]*):\?[^}]+\}$`

slashed(path) := replace(path, "\\", "/")

deny contains msg if {
	some name, service in input.services
	some key in namespaces
	mode := service[key]
	contains(sprintf("%v", [mode]), "$")
	msg := sprintf("service %s: %s must be literal, not %v", [name, key, mode])
}

deny contains msg if {
	some name, service in input.services
	some key in namespaces
	service[key] == "host"
	msg := sprintf("service %s: %s: host shares the host's namespace", [name, key])
}

deny contains msg if {
	some name, service in input.services
	object.get(service, "privileged", false) != false
	msg := sprintf("service %s: privileged services are forbidden", [name])
}

deny contains msg if {
	some name, service in input.services
	walk(service, [_, value])
	is_string(value)
	regex.match(engine_socket, slashed(value))
	msg := sprintf("service %s: mounting a container-engine control socket is forbidden (%s)", [name, value])
}

deny contains msg if {
	some name, service in input.services
	some key, value in environment(object.get(service, "environment", {}))
	regex.match(secret_key, key)
	value != null
	not regex.match(required_env, value)
	msg := sprintf("service %s: secret-like environment %s requires ${NAME:?message}, not a value", [name, key])
}

# Compose keeps the form it was written in: a mapping, or a list of `KEY=value` / bare `KEY`.
environment(env) := env if is_object(env)

environment(env) := {key: value |
	some entry in env
	parts := split(entry, "=")
	key := parts[0]
	value := entry_value(entry, parts)
} if is_array(env)

entry_value(entry, parts) := null if count(parts) == 1

entry_value(entry, parts) := substring(entry, count(parts[0]) + 1, -1) if count(parts) > 1

deny contains msg if {
	some name, service in input.services
	some port in object.get(service, "ports", [])
	not loopback(port)
	msg := sprintf("service %s: published port %v must bind 127.0.0.1 (literal, or ${NAME:-127.0.0.1})", [name, port])
}

loopback(port) if {
	is_string(port)
	regex.match(safe_bind, port)
}

# A literal short-syntax port (`"127.0.0.1:5432:5432"`) is normalized to its long form.
loopback(port) if {
	is_object(port)
	port.host_ip == "127.0.0.1"
}

deny contains msg if {
	some name, service in input.services
	image := service.image
	not image_is_immutable(image)
	not contract_image_allowed(image)
	msg := sprintf("service %s: image %s needs tag@sha256 (only *:local may omit a digest)", [name, image])
}

image_is_immutable(reference) if {
	endswith(reference, ":local")
	not contains(reference, "@")
	not regex.match(`\s`, reference)
}

image_is_immutable(reference) if {
	parts := split(reference, "@sha256:")
	count(parts) == 2
	regex.match(`^[0-9a-f]{64}$`, parts[1])
	regex.match(`^[^\s${}]+:[^\s/:${}]+$`, parts[0])
}

contract_image_allowed(reference) if {
	[_, variable] := regex.find_all_string_submatch_n(contract_image, reference, 1)[0]
	variable in data.context.contract_images
	data.context.contract_owner_valid == true
}

deny contains msg if {
	some name, service in input.services
	build := service.build
	not local_context(object.get(build, "context", null))
	msg := sprintf("service %s: build context %v must be . or a literal ./ path below the Compose file", [name, object.get(build, "context", null)])
}

deny contains msg if {
	some name, service in input.services
	some key in {"additional_contexts", "dockerfile_inline"}
	service.build[key]
	msg := sprintf("service %s: build %s is unsupported (it reads outside the declared context)", [name, key])
}

local_context(".")

local_context(context) if {
	is_string(context)
	startswith(context, "./")
	not regex.match(`[$\\{}]|(^|/)\.\.(/|$)`, context)
}
