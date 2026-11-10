"""The asset checks, generated from the declared definitions (root ADR-0415, ADR-0333 §8).

A definition's `asset_checks` section (platforms/foundation-platform/definitions/<group>.definition.json)
says which of its own assets carries which check, whether the check blocks, and which other nodes it
reads. This module only turns each declaration into a Dagster asset check over the kind of check it
names; the kinds are the code (unit_price_checks.KINDS, publish_checks.KINDS, place_identity_checks.KINDS). No asset key, table or node id is written
here, so a check moves, is added or turns blocking by editing the asset's own definition.

A blocking check (`blocking: true`) fails with severity ERROR and, in a run that materializes its
asset, stops everything downstream of it (Dagster's blocking asset checks); a non-blocking one reports
with severity WARN only.
"""

from dataclasses import dataclass

from dagster import AssetCheckExecutionContext, AssetCheckSeverity, asset_check

import observe_assets


@dataclass(frozen=True)
class Kind:
    """A check the code location implements: what it holds, and how it decides one declared check."""

    description: str
    evaluate: object  # (context, Declared) -> AssetCheckResult


@dataclass(frozen=True)
class Declared:
    """One declared check, resolved against the pipeline graph."""

    group: str
    check: str
    node: dict
    reads: tuple
    blocking: bool
    keys: dict

    @property
    def severity(self):
        return AssetCheckSeverity.ERROR if self.blocking else AssetCheckSeverity.WARN

    def key(self, node_id=None):
        return self.keys[node_id or self.node["id"]]


def kinds():
    """Check name -> its kind (the modules imported at the end of this one)."""
    return {**unit_price_checks.KINDS, **publish_checks.KINDS, **place_identity_checks.KINDS}


def definitions():
    """group -> the definition's `asset_checks`."""
    return {group: definition.get("asset_checks", [])
            for group, definition in observe_assets.declared_definitions().items()}


def declared(graph, declarations=None):
    """Every declared check, resolved: its node and the nodes it reads are the graph's, by id."""
    declarations = definitions() if declarations is None else declarations
    nodes = {node["id"]: node for node in graph["nodes"]}
    keys = observe_assets.node_keys(graph)
    known = kinds()
    found = []
    for group, checks in sorted(declarations.items()):
        for check in checks:
            missing = sorted({check["asset"], *check.get("reads", [])} - set(nodes))
            if missing:
                raise observe_assets.job_specs.JobListError(f"{group}: asset check {check['check']} names nodes {missing} the pipeline graph has not")
            if check["check"] not in known:
                raise observe_assets.job_specs.JobListError(f"{group}: asset check {check['check']} is not one the code location implements: {sorted(known)}")
            found.append(Declared(group, check["check"], nodes[check["asset"]],
                                  tuple(nodes[node_id] for node_id in check.get("reads", [])),
                                  check["blocking"], keys))
    return found


def build(graph, declarations=None):
    """The Dagster asset checks of every declaration."""
    known = kinds()
    checks = []
    for item in declared(graph, declarations):
        kind = known[item.check]
        checks.append(asset_check(
            asset=item.key(), name=item.check, blocking=item.blocking, description=kind.description,
            additional_deps=[item.key(node["id"]) for node in item.reads],
        )(_evaluator(kind, item)))
    return checks


def _evaluator(kind, item):
    # A closure, not default arguments: Dagster reads every parameter after the context as an input.
    def evaluate(context: AssetCheckExecutionContext):
        return kind.evaluate(context, item)

    return evaluate


# At import, after Kind (the kinds' modules import it from here): Dagster's code server imports
# definitions.py with this directory on sys.path, restores sys.path, and only then calls defs(), so an
# import first made there finds nothing (every load failed on `import publish_checks` from 2026-11-06
# 21:16Z to 11-09, and the dual run recorded no comparison).
import place_identity_checks  # noqa: E402
import publish_checks  # noqa: E402
import unit_price_checks  # noqa: E402
