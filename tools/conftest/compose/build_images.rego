# Image pinning for one Dockerfile, as the pinned Conftest's Dockerfile parser (BuildKit's) reads
# it: continuations joined, quotes removed, flags split (scripts/guard/check-container-runtime-policy.sh
# runs it, root ADR-0422). The immutability rule is the Compose one (`compose.image_is_immutable`).
#
# Prevents: a moved image. A tag without a digest builds whatever the registry serves next, in a
# FROM, a `COPY --from=<image>`, a `RUN --mount=...,from=<image>` or the `# syntax=` frontend.
#
# `data.context.syntax`: the `# syntax=` parser directive, which the parser does not return.
package dockerfile

import data.compose.image_is_immutable
import rego.v1

stage_alias(index) := lower(input[index].Value[2]) if {
	input[index].Cmd == "from"
	count(input[index].Value) == 3
	lower(input[index].Value[1]) == "as"
}

# A name an earlier FROM gave its stage, or a stage's number.
earlier_stage(name, index) if {
	some earlier, instruction in input
	instruction.Cmd == "from"
	earlier < index
	stage_alias(earlier) == lower(name)
}

earlier_stage(name, _) if regex.match(`^[0-9]+$`, name)

deny contains msg if {
	some index, instruction in input
	instruction.Cmd == "from"
	reference := instruction.Value[0]
	lower(reference) != "scratch"
	not earlier_stage(reference, index)
	not image_is_immutable(reference)
	msg := sprintf("FROM %s needs tag@sha256; only *:local may omit a digest", [reference])
}

stage_source(instruction) := trim_prefix(flag, "--from=") if {
	instruction.Cmd == "copy"
	some flag in instruction.Flags
	startswith(flag, "--from=")
}

stage_source(instruction) := trim_prefix(option, "from=") if {
	instruction.Cmd == "run"
	some flag in instruction.Flags
	startswith(flag, "--mount=")
	some option in split(trim_prefix(flag, "--mount="), ",")
	startswith(option, "from=")
}

deny contains msg if {
	some index, instruction in input
	source := stage_source(instruction)
	not earlier_stage(source, index)
	not image_is_immutable(source)
	msg := sprintf("%s --from %s: an external stage image needs tag@sha256", [upper(instruction.Cmd), source])
}

deny contains msg if {
	syntax := data.context.syntax
	is_string(syntax)
	not image_is_immutable(syntax)
	msg := sprintf("# syntax=%s: the frontend needs tag@sha256", [syntax])
}
