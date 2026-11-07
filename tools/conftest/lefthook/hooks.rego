# Repository contracts for the root lefthook.yml, evaluated by `conftest test`
# (scripts/guard/lefthook-policy.sh) over the document Conftest's YAML parser reads (root ADR-0422).
#
# Prevents: a hook command that swallows its own failure. `cargo check || echo "cargo not installed
# - CI enforces"` and `gitleaks protect --staged || echo ...` once made two Gongzzang hooks pass
# every push without running, and "CI enforces" was the wording that excused it. A hook that cannot
# run is skipped by its `skip:` condition, never by a fallback that reports success. Workflows get
# the same refusal in tools/conftest/workflow.
package lefthook

import rego.v1

fake_pass_fallbacks := ["|| true", "|| echo", "CI enforces", "not installed"]

deny contains msg if {
	walk(input, [location, value])
	count(location) > 0
	location[count(location) - 1] == "run"
	is_string(value)
	some line in split(value, "\n")
	not startswith(trim_space(line), "#")
	some fallback in fake_pass_fallbacks
	contains(line, fallback)
	msg := sprintf("lefthook.yml: a run hides its own failure with `%s` (%v)", [fallback, location])
}
