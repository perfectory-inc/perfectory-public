# The deploy asks compose for an end state; it does not walk the chain compose already declares.
# Judged on platforms/foundation-platform/docker-compose.yml as the pinned `docker compose config`
# normalizes it (every profile enabled), with the deploy's commands as data
# (scripts/guard/the-deploy-does-not-restate-the-compose-chain.sh runs it, root ADR-0422).
#
# Prevents: on 2026-09-02 the runtime database held all 33 migrations and `GET /catalog/v1/complexes`
# answered 500 with `permission denied for schema catalog`. `foundation-release.sh migrate` had run
# `up --no-deps postgres` and then `run foundation-migrate`: the compose chain written out a second
# time and stopped two links early, so foundation-runtime-grants and foundation-finalize never ran.
# `/health` does not touch the database, so the stack looked up for a day.
#
# The chain is read from the services' depends_on, never listed here, so a link added to compose is
# covered without editing this. postgres is its root and may be addressed on its own (a database can
# start without the migration chain); an intermediate link named in a command at all is the order
# written twice, whatever the invocation shape.
#
# `data.context.commands`: foundation-release.sh's lines that are not comments.
package compose_chain

import rego.v1

terminal := "foundation-api"

edges[name] := object.keys(object.get(service, "depends_on", {})) if some name, service in input.services

chain := graph.reachable(edges, {terminal}) - {terminal}

deny contains msg if {
	count(chain) < 3
	msg := sprintf("read only %d links of the compose chain to %s, which cannot be right", [count(chain), terminal])
}

deny contains "the deploy passes --no-deps, which tells compose to ignore the order it owns" if {
	some line in data.context.commands
	contains(line, "--no-deps")
}

deny contains msg if {
	some link in chain
	link != "postgres"
	some line in data.context.commands
	contains(line, link)
	msg := sprintf("the deploy runs %s directly instead of requiring %s; compose already orders it", [link, terminal])
}

deny contains msg if {
	not waits_for_terminal
	msg := sprintf("the deploy never waits for %s to be up; without --wait it returns before the chain finishes", [terminal])
}

waits_for_terminal if {
	some line in data.context.commands
	regex.match(sprintf(`up[^|;&]*--wait[^|;&]*%s|up[^|;&]*%s[^|;&]*--wait`, [terminal, terminal]), line)
}
