package compose_source

import rego.v1

test_local_includes_are_accepted if {
	count(deny) == 0 with input as {"include": ["compose.lakehouse.yml", {"path": "infra/compose.yml"}, {"path": ["a/compose.extra.yml"]}]}
}

test_remote_git_include_is_refused if {
	count(deny) == 1 with input as {"include": ["https://github.com/example/stack.git#main:compose.yml"]}
}

test_oci_include_is_refused if {
	count(deny) == 1 with input as {"include": [{"path": "oci://registry.example/stack:1"}]}
}

test_parent_directory_include_is_refused if {
	count(deny) == 1 with input as {"include": [{"path": "../other/compose.yml"}]}
}

test_interpolated_include_is_refused if {
	count(deny) == 1 with input as {"include": ["${STACK}/compose.yml"]}
}

test_non_compose_include_is_refused if {
	count(deny) == 1 with input as {"include": ["notes.yml"]}
}
