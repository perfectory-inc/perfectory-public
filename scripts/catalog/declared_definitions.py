#!/usr/bin/env python3
"""One declared definition per dataset group; every listed artifact is generated from them (root ADR-0415).

A definition file (`platforms/foundation-platform/definitions/<group>.definition.json`, schema
`scripts/catalog/definition.v1.schema.json`) holds every fact of one group of datasets: its
pipeline-graph nodes (the Dagster asset keys), the graph edges into them (their `deps`), the jobs that
materialize them (the systemd unit Dagster launches), and its by-PNU lane, published schemas and host
memory entries. `platform.definition.json` holds what no group owns: each artifact's prose and
settings, and the order the artifacts listed their items in before the flip (`order`): those keep
their places and every later item follows sorted by id, so adding one never edits that file.

The generated artifacts (GENERATED) are not committed (root ADR-0418): `generate` writes them into the
tree where every reader has always found them. CI runs it after every checkout, the local hooks after
every checkout and merge, and the host's admission computes them from the canonical commit with the
control checkout's own renderers (`--renderers-root`). `check` holds that they generate and that git
tracks none of them; `show` gives an artifact at any commit, for the readers of history. `split` is
the bootstrap that made the definitions from the committed artifacts (root ADR-0415).

Standard library only, as every guard here: definition files are JSON, the YAML 1.2 subset.
"""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import pathlib
import subprocess
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
EXAMPLE = "platforms/foundation-platform/docs/catalog/pipeline-graph.v1.example.json"
DEFINITIONS = "platforms/foundation-platform/definitions"
LANE_INDEX = "platforms/foundation-platform/config/by-pnu-lane-index.v1.json"
LANE_INDEX_SCHEMA = "foundation-platform.by_pnu_lane_index.v1"

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
DERIVED_ARTIFACTS = (EXAMPLE, DEPENDENCIES, MAP, LANE_INDEX)


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
    # The coordinates here are the artifacts' runtime extents, each with its `*_review` line; the file
    # marker says so to scripts/guard/public-fixture-safety.py, whose line marker a JSON array splits.
    platform = {"definition": DEFINITION,
                "$comment": "public-repository-safety: reviewed-runtime-coordinate-inputs (the configured extents of the artifacts below, not places)",
                "artifacts": {}, "order": {}}
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
        order = platform.get("order", {}).get(name, []) if ordered else []
        stale = sorted(set(order) - set(items[name]))
        if stale:
            raise DefinitionError(f"{PLATFORM_FILE}: order.{name} lists {stale}, which no definition holds; remove them")
        ids = [*order, *sorted(set(items[name]) - set(order))]
        if shape == "list":
            put(documents[path], where, [items[name][i] for i in ids])
        else:
            put(documents[path], where, {i: items[name][i] for i in ids})
    return documents


def lane_index(documents):
    """Every by-PNU lane and what serves it, read from the definitions once (inventory B-F02).

    The parcel and building lanes and every `by_pnu_lanes` line. Each lane's serving nodes are its
    `<lane>-by-pnu-section-packs` and `<lane>-by-pnu-serving` graph nodes; its Gold tables are the
    Gold nodes with an edge into them; its bake and rebuild jobs are the jobs that carry an edge into
    the serving nodes and into the Gold nodes. No name is derived from another by a spelling rule but
    the environment prefix, which the publisher reads (FOUNDATION_PLATFORM_<LANE>_BY_PNU_SERVING); a
    reader takes all of it from here instead of re-deriving it.
    """
    r2, graph, jobs = documents[R2], documents[GRAPH], documents[JOBS]["jobs"]
    kinds = r2.get("by_pnu_lanes", {})
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}

    def writers(node_ids):
        return [job for job in jobs if any(edges[e]["to"] in node_ids for e in job["pipeline_graph_edges"] if e in edges)]

    lanes = []
    for lane in ("parcel", "building", *kinds):
        serving = [n for n in (f"{lane}-by-pnu-section-packs", f"{lane}-by-pnu-serving") if n in nodes]
        gold = sorted({edges[e]["from"] for e in edges if edges[e]["to"] in serving
                       and nodes[edges[e]["from"]]["type"] == "gold_table"})
        line = kinds.get(lane, {})
        lanes.append({
            "lane": lane,
            # Where the lane's gateway block is: the host lanes' own key, a kind's host, or a lane line
            # that holds its own block (unit-price, root ADR-0190).
            "gateway_block": (f"{lane}_by_pnu_gateway" if lane in ("parcel", "building")
                              else line["host"] if "host" in line else f"by_pnu_lanes.{lane}"),
            "kind": line.get("kind"),
            "section": line.get("section"),
            "env_prefix": "FOUNDATION_PLATFORM_" + lane.upper().replace("-", "_") + "_BY_PNU_SERVING",
            "serving_nodes": serving,
            "gold_tables": [nodes[n]["table_name"] for n in gold],
            "bake_jobs": [{"id": job["id"], "systemd_service": job["systemd_service"]} for job in writers(set(serving))],
            "rebuild_jobs": [{"id": job["id"], "systemd_service": job["systemd_service"]} for job in writers(set(gold))],
        })
    return {"schema_version": LANE_INDEX_SCHEMA,
            "generated_by": "scripts/catalog/declared_definitions.py (root ADR-0418); never edited, never committed",
            "lanes": lanes}


def load_module(name, path, search):
    sys.path.insert(0, str(search))
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(search))


def derived(root, documents, renderers_root=None):
    """The artifacts generated before root ADR-0415, from the generated sources, by their own renderers
    (those under `renderers_root`; the tree's own when None)."""
    renderers_root = renderers_root or root
    dags = renderers_root / "platforms/foundation-platform/orchestration/dags"
    catalog = renderers_root / "scripts/catalog"
    job_specs = load_module("job_specs", dags / "job_specs.py", dags)
    pipeline_map = load_module("render_pipeline_map", catalog / "render-pipeline-map.py", catalog)
    return {
        # The API example is the graph itself (render-pipeline-map.py `EXAMPLE`).
        EXAMPLE: dump(documents[GRAPH]),
        DEPENDENCIES: job_specs.rendered_dependencies(documents[JOBS], documents[GRAPH]),
        MAP: pipeline_map.render(documents[GRAPH], read_json(root / ENDPOINTS)),
        LANE_INDEX: dump(lane_index(documents)),
    }


# --- files -------------------------------------------------------------------------------------


def write_definitions(directory, platform, definitions):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / PLATFORM_FILE).write_text(dump(platform), encoding="utf-8", newline="\n")
    for group, definition in definitions.items():
        (directory / f"{group}{SUFFIX}").write_text(
            dump({"$schema": SCHEMA, **definition}), encoding="utf-8", newline="\n")


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


GENERATED = (*SOURCE_ARTIFACTS, *DERIVED_ARTIFACTS)


def artifacts(root, renderers_root=None):
    """Path -> text of every generated artifact, from the definitions under `root`."""
    documents = generate(*read_definitions(root / DEFINITIONS))
    files = {path: dump(document) for path, document in documents.items()}
    files.update(derived(root, documents, renderers_root))
    return {path: files[path] for path in GENERATED}


def write_artifacts(out, files):
    for path, text in files.items():
        target = out / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8", newline="\n")


def with_generated(paths):
    """`paths` (a change's files) and, when it changes a definition or the generator, every generated
    artifact: a reader of an artifact is changed by what generates it (the CI selectors ask this)."""
    paths = set(paths)
    generator = ("scripts/catalog/declared_definitions.py", "scripts/catalog/definition.v1.schema.json")
    if any(path.startswith(DEFINITIONS + "/") or path in generator for path in paths):
        paths |= set(GENERATED)
    return paths


def tracked(root, paths):
    listed = subprocess.run(["git", "-C", str(root), "ls-files", "--", *paths], capture_output=True,
                            text=True, check=True).stdout.split()
    return sorted(listed)


def check(root):
    """Problems; none when the definitions generate, git tracks no generated artifact, and the tree's
    copies (when present) are what the definitions generate now."""
    files = artifacts(root)
    problems = [f"{path}: generated from {DEFINITIONS} (root ADR-0418); remove it from git: git rm --cached {path}"
                for path in tracked(root, GENERATED)]
    for path, text in files.items():
        target = root / path
        if target.exists() and target.read_bytes() != text.encode("utf-8"):
            problems.append(f"{path}: differs from what {DEFINITIONS} generate; edit the definition, then run "
                            "python3 scripts/catalog/declared_definitions.py generate")
    return problems


def _git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, check=True).stdout


def show(root, rev, path):
    """The text of generated artifact `path` at commit `rev`: generated from that commit's definitions,
    or that commit's file before the definitions existed. Derived artifacts use this tree's renderers."""
    has_definitions = subprocess.run(["git", "-C", str(root), "cat-file", "-e", f"{rev}:{DEFINITIONS}"],
                                     capture_output=True).returncode == 0
    if not has_definitions:
        return _git(root, "show", f"{rev}:{path}").decode("utf-8")
    with tempfile.TemporaryDirectory() as scratch:
        tree = pathlib.Path(scratch)
        directory = tree / DEFINITIONS
        directory.mkdir(parents=True)
        for name in _git(root, "ls-tree", "--name-only", f"{rev}:{DEFINITIONS}").decode().split():
            (directory / name).write_bytes(_git(root, "show", f"{rev}:{DEFINITIONS}/{name}"))
        documents = generate(*read_definitions(directory))
        if path in documents:
            return dump(documents[path])
        (tree / ENDPOINTS).parent.mkdir(parents=True, exist_ok=True)
        (tree / ENDPOINTS).write_bytes(_git(root, "show", f"{rev}:{ENDPOINTS}"))
        return derived(tree, documents, renderers_root=root)[path]


def measure(root):
    platform, definitions = read_definitions(root / DEFINITIONS)
    documents = generate(platform, definitions)
    lanes = sorted(at(documents[R2], ("by_pnu_lanes",)))
    return {
        "definitions": len(definitions),
        "items": {name: len(at(documents[SLOTS[name][0]], SLOTS[name][1])) for name in SLOTS},
        "by_pnu_lanes": len(lanes),
        # The definition files a lane's facts are in: one, so one edit adds a lane.
        "files_one_lane_touches": {lane: sum(1 for d in definitions.values() if lane in d.get("by_pnu_lanes", {}))
                                   for lane in lanes},
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("generate", "check", "show", "split", "measure"))
    parser.add_argument("--root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--out", type=pathlib.Path, help="generate: write under this root instead of --root")
    parser.add_argument("--renderers-root", type=pathlib.Path,
                        help="generate: run the renderers of this tree (the host's control checkout)")
    parser.add_argument("--rev", help="show: the commit")
    parser.add_argument("--path", help="show: the generated artifact")
    parser.add_argument("--definitions", type=pathlib.Path, help="split: write here")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        if args.command == "generate":
            renderers = args.renderers_root.resolve() if args.renderers_root else None
            write_artifacts((args.out or root).resolve(), artifacts(root, renderers))
            return 0
        if args.command == "check":
            problems = check(root)
            for problem in problems:
                print(f"FAIL declared-definitions: {problem}", file=sys.stderr)
            return 1 if problems else 0
        if args.command == "show":
            if not args.rev or args.path not in GENERATED:
                parser.error(f"show needs --rev and --path, one of {GENERATED}")
            sys.stdout.buffer.write(show(root, args.rev, args.path).encode("utf-8"))
            return 0
        if args.command == "measure":
            print(json.dumps(measure(root), ensure_ascii=False, indent=2))
            return 0
        if args.definitions is None:
            parser.error("split needs --definitions")
        write_definitions(args.definitions, *split(committed(root)))
        return 0
    except DefinitionError as error:
        print(f"FAIL declared-definitions: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
