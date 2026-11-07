package dockerfile

import rego.v1

pinned := "example/runtime:1@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

from(reference) := {"Cmd": "from", "Flags": [], "Value": [reference]}

from_as(reference, alias) := {"Cmd": "from", "Flags": [], "Value": [reference, "AS", alias]}

copy_from(source) := {"Cmd": "copy", "Flags": [sprintf("--from=%s", [source])], "Value": ["/a", "/b"]}

run_mount(source) := {"Cmd": "run", "Flags": [sprintf("--mount=type=cache,target=/x,from=%s", [source])], "Value": ["true"]}

refused(file) if count(deny) > 0 with input as file with data.context as {}

accepted(file) if count(deny) == 0 with input as file with data.context as {}

test_pinned_local_and_scratch_are_accepted if {
	accepted([from(pinned), from("foundation-api:local"), from("scratch")])
}

test_tag_without_digest_is_refused if refused([from("example/runtime:latest")])

test_digest_without_tag_is_refused if refused([from(replace(pinned, ":1@", "@"))])

test_variable_image_is_refused if refused([from("${BASE_IMAGE}")])

test_earlier_stage_is_accepted if {
	accepted([from_as(pinned, "Build"), from("build"), copy_from("build"), copy_from("0"), run_mount("build")])
}

test_stage_named_only_later_is_refused if refused([from("build"), from_as(pinned, "build")])

test_external_copy_from_is_refused if refused([from(pinned), copy_from("ghcr.io/example/tool:1")])

test_external_mount_from_is_refused if refused([from(pinned), run_mount("alpine:3")])

test_pinned_external_copy_from_is_accepted if accepted([from(pinned), copy_from(pinned)])

test_mutable_syntax_frontend_is_refused if {
	count(deny) > 0 with input as [from(pinned)] with data.context as {"syntax": "docker/dockerfile:1"}
}

test_pinned_syntax_frontend_is_accepted if {
	count(deny) == 0 with input as [from(pinned)] with data.context as {"syntax": pinned}
}
