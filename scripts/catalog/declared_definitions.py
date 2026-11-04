#!/usr/bin/env python3
"""One declared definition per dataset group; every listed artifact is generated from them (root ADR-0415).

A definition file (`<group>.definition.json`, schema `scripts/catalog/definition.v1.schema.json`) holds every fact
of one group of datasets: its pipeline-graph nodes (the Dagster asset keys), the graph edges into
them (their `deps`), the jobs that materialize them (the systemd unit Dagster launches), and its
by-PNU lane, published schemas and host memory entries. `platform.definition.json` holds what no
group owns: each artifact's prose and settings, and until the flip (ADR-0415 §5 phase 2) the order
the committed files list their items in.

Phase 1 (now) commits no definition: the committed artifacts stay the single source while open pull
requests keep editing them. `check` splits the committed artifacts into definitions, writes and
reads them back as files, generates every artifact from them and holds the result equal to what is
committed: the same items, fields, values and order, and the byte-for-byte files for the artifacts
that were generated already. So the flip is proven on every change before it is made. `split`
writes the definitions the flip will commit; `generate` renders the artifacts from a definition
directory; `measure` prints the counts ADR-0415 §7 reports.

Standard library only, as every guard here: definition files are JSON, the YAML 1.2 subset.
"""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import pathlib
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEMA = "scripts/catalog/definition.v1.schema.json"
DEFINITION = "foundation-platform.definition.v1"
PLATFORM_FILE = "platform.definition.json"
SUFFIX = ".definition.json"

JOBS = "platforms/foundation-platform/orchestration/jobs.v1.json"
GRAPH = "platforms/foundation-platform/docs/catalog/pipeline-graph.v1.json"
R2 = "platforms/foundation-platform/config/r2-connections.contract.json"
MEMORY = "tools/host-memory-budget.contract.json"
REGISTRY = "platforms/foundation-platform/contracts/schema-registry/registry.json"
DEPENDENCIES = "platforms/foundation-platform/orchestration/job-dependencies.v1.json"
MAP = "docs/data-pipeline-map.md"
ENDPOINTS = "platforms/foundation-platform/docs/catalog/public-source-endpoint-catalog.v1.json"

# Every slot: the artifact, the path of the item collection in it, its shape (a list of objects
# keyed by `key`, or an object keyed by name) and the definition section that holds its items.
SLOTS = {
    "jobs": (JOBS, ("jobs",), "list", "id", "jobs"),
    "nodes": (GRAPH, ("nodes",), "list", "id", "assets"),
    "edges": (GRAPH, ("edges",), "list", "id", "dependencies"),
    "by_pnu_lanes": (R2, ("by_pnu_lanes",), "map", None, "by_pnu_lanes"),
    "host_memory": (MEMORY, ("scheduled_jobs", "memory"), "map", None, "host_memory"),
    "subjects": (REGISTRY, ("subjects",), "list", "subject", "published_schemas"),
}
SECTIONS = {slot[4]: name for name, slot in SLOTS.items()}
SOURCE_ARTIFACTS = tuple(dict.fromkeys(slot[0] for slot in SLOTS.values()))
# Artifacts that were generated before this ADR: rendered from the generated sources by their own
# renderers, never a second implementation, and held to the committed bytes.
DERIVED_ARTIFACTS = (DEPENDENCIES, MAP)


class DefinitionError(Exception):
    pass


def edge_id(edge):
    return f"{edge['from']}-to-{edge['to']}"


def kebab(name):
    return name.replace("_", "-")


def read_json(path):
    return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))


def dump(document):
    """The one spelling of a generated JSON file."""
    return json.dumps(document, ensure_ascii=False, indent=2) + "\n"


def at(document, path):
    for key in path:
        document = document[key]
    return document


def put(document, path, value):
    at(document, path[:-1])[path[-1]] = value


# --- split: committed artifacts -> definitions -------------------------------------------------


def items_of(documents, name):
    path, shape, key = SLOTS[name][1], SLOTS[name][2], SLOTS[name][3]
    found = at(documents[SLOTS[name][0]], path)
    if shape == "list":
        if not isinstance(found, list) or not all(isinstance(item, dict) and key in item for item in found):
            raise DefinitionError(f"{SLOTS[name][0]} {'.'.join(path)}: a list of objects each with `{key}`")
        ids = [item[key] for item in found]
        return ids, dict(zip(ids, found)), len(ids) != len(set(ids))
    if not isinstance(found, dict):
        raise DefinitionError(f"{SLOTS[name][0]} {'.'.join(path)}: an object keyed by name")
    return list(found), dict(found), False


def groups_of(documents):
    """Group name per item, from what the items already say (ADR-0415 §2: the bootstrap grouping).

    A by-PNU lane's group holds its lane line, its serving node, its Gold node and its published
    schema. A job joins the group of the first node it writes that has one, else its own. A node is
    in the group of the first job that writes it, else its own. An edge is in the group of the
    first job that carries it, else the group of the node it ends at. A memory entry is its job's.
    """
    lanes, _, _ = items_of(documents, "by_pnu_lanes")
    node_ids, nodes, _ = items_of(documents, "nodes")
    edge_ids, edges, _ = items_of(documents, "edges")
    job_ids, jobs, _ = items_of(documents, "jobs")
    subject_ids, _, _ = items_of(documents, "subjects")
    memory_ids, _, _ = items_of(documents, "host_memory")

    node_group = {}
    for lane in lanes:
        for node_id in (f"{lane}-by-pnu-section-packs", f"gold-{lane}"):
            if node_id in nodes:
                node_group.setdefault(node_id, lane)
    writes = {job_id: [edges[e]["to"] for e in jobs[job_id].get("pipeline_graph_edges", []) if e in edges]
              for job_id in job_ids}
    job_group = {}
    for job_id in job_ids:
        job_group[job_id] = next((node_group[n] for n in writes[job_id] if n in node_group), kebab(job_id))
    for job_id in job_ids:
        for node_id in writes[job_id]:
            node_group.setdefault(node_id, job_group[job_id])
    for node_id in node_ids:
        node_group.setdefault(node_id, node_id)
    edge_group = {}
    for job_id in job_ids:
        for e in jobs[job_id].get("pipeline_graph_edges", []):
            if e in edges:
                edge_group.setdefault(e, job_group[job_id])
    for e in edge_ids:
        edge_group.setdefault(e, node_group.get(edges[e]["to"], edges[e]["to"]))
    by_subject = {f"foundation-platform.{lane.replace('-', '_')}_by_pnu": lane for lane in lanes}
    return {
        "jobs": job_group,
        "nodes": node_group,
        "edges": edge_group,
        "by_pnu_lanes": {lane: lane for lane in lanes},
        "host_memory": {job_id: job_group.get(job_id, kebab(job_id)) for job_id in memory_ids},
        "subjects": {s: by_subject.get(s, s.removeprefix("foundation-platform.").replace("_", "-"))
                     for s in subject_ids},
    }


def split(documents):
    """The definitions that generate `documents` (artifact path -> parsed JSON)."""
    groups = groups_of(documents)
    definitions = {}
    platform = {"definition": DEFINITION, "artifacts": {}, "order": {}}
    for path in SOURCE_ARTIFACTS:
        platform["artifacts"][path] = copy.deepcopy(documents[path])
    for name, (path, where, shape, key, section) in SLOTS.items():
        ids, items, duplicated = items_of(documents, name)
        if duplicated:
            raise DefinitionError(f"{path} {'.'.join(where)}: an id is listed twice; one definition owns one id")
        put(platform["artifacts"][path], where, None)
        platform["order"][name] = ids
        for item_id in ids:
            item = copy.deepcopy(items[item_id])
            if name == "edges":
                if item["id"] != edge_id(item):
                    raise DefinitionError(f"{path}: edge {item['id']} is not named `<from>-to-<to>` ({edge_id(item)})")
                del item["id"]
            group = groups[name][item_id]
            definition = definitions.setdefault(group, {"definition": DEFINITION, "group": group})
            if shape == "list":
                definition.setdefault(section, []).append(item)
            else:
                definition.setdefault(section, {})[item_id] = item
    return platform, definitions


# --- generate: definitions -> artifacts --------------------------------------------------------


def generate(platform, definitions, ordered=True):
    """Artifact path -> parsed JSON, from the definitions. Without `ordered`, items sort by id."""
    if platform.get("definition") != DEFINITION:
        raise DefinitionError(f"{PLATFORM_FILE}: definition must be {DEFINITION}")
    owner = {name: {} for name in SLOTS}
    items = {name: {} for name in SLOTS}
    for group, definition in sorted(definitions.items()):
        if definition.get("definition") != DEFINITION or definition.get("group") != group:
            raise DefinitionError(f"{group}{SUFFIX}: definition must be {DEFINITION} and group {group}")
        unknown = set(definition) - {"definition", "group", "$schema", *SECTIONS}
        if unknown:
            raise DefinitionError(f"{group}{SUFFIX}: unknown sections {sorted(unknown)}")
        for section, name in SECTIONS.items():
            shape, key = SLOTS[name][2], SLOTS[name][3]
            held = definition.get(section, [] if shape == "list" else {})
            pairs = []
            if shape == "list":
                for item in held:
                    item = copy.deepcopy(item)
                    if name == "edges":
                        if "id" in item:
                            raise DefinitionError(f"{group}{SUFFIX}: an edge's id is `<from>-to-<to>`; do not write it")
                        item = {"id": edge_id(item), **item}
                    pairs.append((item[key], item))
            else:
                pairs = [(item_id, copy.deepcopy(item)) for item_id, item in held.items()]
            for item_id, item in pairs:
                if item_id in owner[name]:
                    raise DefinitionError(f"{name} {item_id}: in both {owner[name][item_id]} and {group}")
                owner[name][item_id] = group
                items[name][item_id] = item
    documents = copy.deepcopy(platform["artifacts"])
    for name, (path, where, shape, _key, _section) in SLOTS.items():
        order = platform.get("order", {}).get(name) if ordered else None
        if order is not None:
            if sorted(order) != sorted(items[name]):
                missing = sorted(set(items[name]) - set(order))
                stale = sorted(set(order) - set(items[name]))
                raise DefinitionError(f"{name}: order lists {stale} no definition holds and misses {missing}")
            ids = order
        else:
            ids = sorted(items[name])
        if shape == "list":
            put(documents[path], where, [items[name][i] for i in ids])
        else:
            put(documents[path], where, {i: items[name][i] for i in ids})
    return documents


def load_module(name, path, search):
    sys.path.insert(0, str(search))
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(search))


def derived(root, documents):
    """The artifacts generated before this ADR, from the generated sources, by their own renderers."""
    dags = root / "platforms/foundation-platform/orchestration/dags"
    catalog = root / "scripts/catalog"
    job_specs = load_module("job_specs", dags / "job_specs.py", dags)
    pipeline_map = load_module("render_pipeline_map", catalog / "render-pipeline-map.py", catalog)
    return {
        DEPENDENCIES: job_specs.rendered_dependencies(documents[JOBS], documents[GRAPH]),
        MAP: pipeline_map.render(documents[GRAPH], read_json(root / ENDPOINTS)),
    }


# --- files -------------------------------------------------------------------------------------


def write_definitions(directory, platform, definitions):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / PLATFORM_FILE).write_text(dump(platform), encoding="utf-8")
    for group, definition in definitions.items():
        (directory / f"{group}{SUFFIX}").write_text(
            dump({"$schema": SCHEMA, **definition}), encoding="utf-8")


def read_definitions(directory):
    platform = read_json(directory / PLATFORM_FILE)
    definitions = {}
    for path in sorted(directory.glob(f"*{SUFFIX}")):
        if path.name == PLATFORM_FILE:
            continue
        definition = read_json(path)
        definition.pop("$schema", None)
        definitions[path.name[: -len(SUFFIX)]] = definition
    return platform, definitions


def committed(root):
    return {path: read_json(root / path) for path in SOURCE_ARTIFACTS}


def ordered_equal(left, right):
    """Equal values, with object keys in the same order (json.loads keeps file order)."""
    if isinstance(left, dict) and isinstance(right, dict):
        return list(left) == list(right) and all(ordered_equal(left[k], right[k]) for k in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(ordered_equal(a, b) for a, b in zip(left, right))
    return type(left) is type(right) and left == right


def check(root):
    """Problems; none when the definitions split from the committed artifacts generate them again."""
    documents = committed(root)
    platform, definitions = split(documents)
    with tempfile.TemporaryDirectory() as scratch:
        # Through files, so what the flip commits is what was proven.
        write_definitions(pathlib.Path(scratch), platform, definitions)
        platform, definitions = read_definitions(pathlib.Path(scratch))
    problems = []
    generated = generate(platform, definitions)
    for path in SOURCE_ARTIFACTS:
        if not ordered_equal(generated[path], documents[path]):
            problems.append(f"{path}: the definitions do not generate the committed items, fields and order")
    for path, text in derived(root, generated).items():
        if text.encode("utf-8") != (root / path).read_bytes():
            problems.append(f"{path}: the definitions do not generate the committed bytes")
    # Phase 2 sorts by id (no order list): the same items, each id once.
    canonical = generate(platform, definitions, ordered=False)
    for name, (path, where, shape, key, _section) in SLOTS.items():
        def ids(document):
            found = at(document, where)
            return sorted(item[key] for item in found) if shape == "list" else sorted(found)
        if ids(canonical[path]) != ids(documents[path]):
            problems.append(f"{path} {'.'.join(where)}: the id-sorted generation drops or adds items")
    return problems


def measure(root):
    platform, definitions = split(committed(root))
    lanes = sorted(at(committed(root)[R2], ("by_pnu_lanes",)))
    report = {
        "definitions": len(definitions),
        "items": {name: len(platform["order"][name]) for name in SLOTS},
        "by_pnu_lanes": len(lanes),
        # Committed files one lane's items are spread over today (sources and derived), and after the flip.
        "files_one_lane_touches": {},
    }
    for lane in lanes:
        definition = definitions[lane]
        touched = {SLOTS[SECTIONS[s]][0] for s in SECTIONS if definition.get(s)}
        if touched & {JOBS, GRAPH}:
            touched |= set(DERIVED_ARTIFACTS)
        report["files_one_lane_touches"][lane] = {"today": len(touched), "after_flip": 1}
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check", "split", "generate", "measure"))
    parser.add_argument("--root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--definitions", type=pathlib.Path, help="split: write here; generate: read here")
    parser.add_argument("--out", type=pathlib.Path, help="generate: write the artifacts under this root")
    parser.add_argument("--sorted", action="store_true", help="generate: sort by id, ignoring the order list")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        if args.command == "check":
            problems = check(root)
            for problem in problems:
                print(f"FAIL declared-definitions: {problem}", file=sys.stderr)
            return 1 if problems else 0
        if args.command == "measure":
            print(json.dumps(measure(root), ensure_ascii=False, indent=2))
            return 0
        if args.definitions is None:
            parser.error(f"{args.command} needs --definitions")
        if args.command == "split":
            write_definitions(args.definitions, *split(committed(root)))
            return 0
        if args.out is None:
            parser.error("generate needs --out")
        documents = generate(*read_definitions(args.definitions), ordered=not args.sorted)
        files = {path: dump(document) for path, document in documents.items()}
        files.update(derived(root, documents))
        for path, text in files.items():
            target = args.out / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8", newline="\n")
        return 0
    except DefinitionError as error:
        print(f"FAIL declared-definitions: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
