# The one thing normalization hides: where an `include:` came from. `docker compose config`
# fetches a remote include (git, OCI) and merges it as if it were local, so the raw file is checked
# for it before the normalized document is (scripts/guard/check-container-runtime-policy.sh).
#
# Prevents: a service pulled in from a repository or registry no review here ever saw — the
# runtime policy would then judge a file whose content can change under us.
package compose_source

import rego.v1

deny contains msg if {
	some entry in object.get(input, "include", [])
	some path in include_paths(entry)
	not local_path(path)
	msg := sprintf("include %v must be a literal relative path to a local Compose file", [path])
}

include_paths(entry) := [entry] if is_string(entry)

include_paths(entry) := paths if {
	is_object(entry)
	raw := object.get(entry, "path", null)
	paths := as_list(raw)
}

as_list(value) := value if is_array(value)

as_list(value) := [value] if not is_array(value)

local_path(path) if {
	is_string(path)
	not startswith(path, "/")
	not regex.match(`[$\\{}]|://|^[A-Za-z]:|(^|/)\.\.(/|$)`, path)
	regex.match(`(^|/)((docker-)?compose([.-][a-z0-9_.-]+)?|[a-z0-9_.-]+\.compose)\.ya?ml$`, path)
}
