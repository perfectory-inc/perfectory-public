# `conftest verify --policy tools/conftest/compose`: each test plants one incident in a normalized
# Compose document (as `docker compose config --format json` emits it) or shows an accepted form.
package compose

import rego.v1

ctx := {"contract_images": ["EXAMPLE_TOOL_IMAGE"], "contract_owner_valid": true}

digest := "@sha256:0000000000000000000000000000000000000000000000000000000000000000"

good := {"services": {
	"api": {
		"image": concat("", ["example/api:1.2", digest]),
		"ports": ["127.0.0.1:${API_PORT:-18080}:8080", "${API_BIND:-127.0.0.1}:18081:8081/tcp"],
		"environment": {"API_TOKEN": "${API_TOKEN:?set the token}", "RUST_LOG": "info", "PASSTHROUGH_SECRET": null},
		"network_mode": "service:netns",
		"build": {"context": "./service", "dockerfile": "Dockerfile"},
		"privileged": false,
	},
	"local": {"image": "example-runtime:local", "build": {"context": "."}},
}}

with_service(service) := {"services": {"api": service}}

test_clean_document_is_accepted if {
	count(deny) == 0 with input as good with data.context as ctx
}

test_host_network_is_refused if {
	count(deny) == 1 with input as with_service({"network_mode": "host"}) with data.context as ctx
}

test_host_pid_is_refused if {
	count(deny) == 1 with input as with_service({"pid": "host"}) with data.context as ctx
}

test_dynamic_namespace_is_refused if {
	count(deny) == 1 with input as with_service({"network_mode": "${MODE:-bridge}"}) with data.context as ctx
}

test_privileged_is_refused if {
	count(deny) == 1 with input as with_service({"privileged": true}) with data.context as ctx
}

test_engine_socket_mount_is_refused if {
	service := {"volumes": [{"type": "bind", "source": "/var/run/docker.sock", "target": "/var/run/docker.sock"}]}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_windows_engine_pipe_is_refused if {
	service := {"volumes": [{"type": "npipe", "source": "//./pipe/docker_engine", "target": "/pipe"}]}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_literal_secret_is_refused if {
	service := {"environment": {"POSTGRES_PASSWORD": "postgres"}}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_wildcard_port_is_refused if {
	count(deny) == 1 with input as with_service({"ports": ["5432:5432"]}) with data.context as ctx
}

test_non_loopback_default_is_refused if {
	count(deny) == 1 with input as with_service({"ports": ["${BIND:-0.0.0.0}:5432:5432"]}) with data.context as ctx
}

test_long_syntax_port_is_refused if {
	port := {"target": 5432, "published": "5432", "host_ip": "0.0.0.0"}
	count(deny) == 1 with input as with_service({"ports": [port]}) with data.context as ctx
}

test_mutable_image_is_refused if {
	count(deny) == 1 with input as with_service({"image": "example/application:latest"}) with data.context as ctx
}

test_digest_without_tag_is_refused if {
	service := {"image": concat("", ["example/application", digest])}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_contract_image_with_valid_owner_is_accepted if {
	service := {"image": "${EXAMPLE_TOOL_IMAGE:?load the contract}"}
	count(deny) == 0 with input as with_service(service) with data.context as ctx
}

test_contract_image_with_false_owner_is_refused if {
	service := {"image": "${EXAMPLE_TOOL_IMAGE:?load the contract}"}
	false_owner := object.union(ctx, {"contract_owner_valid": false})
	count(deny) == 1 with input as with_service(service) with data.context as false_owner
}

test_unknown_image_variable_is_refused if {
	service := {"image": "${OTHER_IMAGE:?set it}"}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_remote_build_context_is_refused if {
	service := {"build": {"context": "https://github.com/example/app.git#main"}}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_build_context_above_the_directory_is_refused if {
	count(deny) == 1 with input as with_service({"build": {"context": "../other"}}) with data.context as ctx
}

test_additional_build_contexts_are_refused if {
	service := {"build": {"context": ".", "additional_contexts": {"x": "docker-image://alpine"}}}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_interpolated_build_context_is_refused if {
	count(deny) == 1 with input as with_service({"build": {"context": "${CONTEXT:-.}"}}) with data.context as ctx
}

test_backslash_build_context_is_refused if {
	count(deny) == 1 with input as with_service({"build": {"context": "./a\\b"}}) with data.context as ctx
}

test_normalized_loopback_port_is_accepted if {
	port := {"host_ip": "127.0.0.1", "mode": "ingress", "protocol": "tcp", "published": "19080", "target": 8080}
	count(deny) == 0 with input as with_service({"ports": [port]}) with data.context as ctx
}

test_normalized_port_without_host_ip_is_refused if {
	port := {"mode": "ingress", "protocol": "tcp", "published": "5432", "target": 5432}
	count(deny) == 1 with input as with_service({"ports": [port]}) with data.context as ctx
}

test_literal_secret_in_list_form_is_refused if {
	service := {"environment": ["RUST_LOG=info", "POSTGRES_PASSWORD=postgres"]}
	count(deny) == 1 with input as with_service(service) with data.context as ctx
}

test_list_form_required_and_passthrough_secrets_are_accepted if {
	service := {"environment": ["API_TOKEN=${API_TOKEN:?set it}", "PASSTHROUGH_SECRET", "URL=a=b"]}
	count(deny) == 0 with input as with_service(service) with data.context as ctx
}
