"""The by-PNU data kinds (root ADR-0193): the lines of `config/r2-connections.contract.json`
`by_pnu_lanes` that name a host.

A kind line says which host panel it splits, which of that panel's sections it is and the
section's content columns. A line that also names a `producer` is a kind built from its own
Silver by that Spark job (root ADR-0287) rather than from a panel section: it is served on the
host's lane the same way, but is not pinned to or checked against the host panel. Everything a
kind needs follows from that line: its Gold table
(`gold.<lane with - as _>`, lakehouse-domain `by_pnu_kind.rs`), its rebuild
(`gold-panel-rebuild.contract.json` `kind_tables`, `gold_rebuild.load_contract`), its producer run
(`by_pnu_kind_silver_to_gold.py --lane <lane>`) and its serving lane. Pure Python: the planner
and the host scripts read it without Spark.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

CONTRACT_PATH = Path(__file__).resolve().parents[4] / "config" / "r2-connections.contract.json"
# Each by-PNU document's fields per schema version (the publisher's by_pnu_document_records.rs).
RECORDS_PATH = Path(__file__).resolve().parents[4] / "contracts" / "by-pnu-documents"
# The panel each host's kinds are sections of, and which the kinds follow while it is served.
HOST_PANELS = {"parcel_by_pnu_gateway": "gold.parcel_panel", "building_by_pnu_gateway": "gold.building_panel"}


@dataclass(frozen=True)
class Kind:
    lane: str
    host: str
    section: str
    # Content column names in the Gold's order (a `<name>:long` spec names `<name>`).
    columns: tuple[str, ...]
    # The Spark job (a module under jobs/) that builds this kind from its own Silver, or None for a
    # section of the host panel (built by by_pnu_kind_silver_to_gold, root ADR-0193 §9).
    producer: str | None = None

    @property
    def gold_table(self) -> str:
        return "gold." + self.lane.replace("-", "_")

    @property
    def host_panel(self) -> str:
        return HOST_PANELS[self.host]

    @property
    def follows_host_panel(self) -> bool:
        """Whether the kind is a section of its host panel, pinned to it and checked against it."""
        return self.producer is None


def kinds(path: Path = CONTRACT_PATH) -> dict[str, Kind]:
    """Every kind line of the contract, by lane, in the contract's order."""
    lanes = json.loads(path.read_text(encoding="utf-8")).get("by_pnu_lanes", {})
    found = {}
    for lane, entry in lanes.items():
        if "host" not in entry:
            continue
        if entry["host"] not in HOST_PANELS or not entry.get("section") or not entry.get("columns"):
            raise ValueError(f"by_pnu_lanes.{lane} is a kind line without a known host, a section or its columns")
        producer = entry.get("producer")
        if producer is not None and not (isinstance(producer, str) and producer.isidentifier()):
            raise ValueError(f"by_pnu_lanes.{lane}.producer must name a Spark job module")
        found[lane] = Kind(lane, entry["host"], entry["section"],
                           tuple(spec.split(":", 1)[0] for spec in entry["columns"]), producer)
    return found


def lane_fields(lane: str, path: Path = CONTRACT_PATH) -> dict[str, str]:
    """What the operator's and bake's scripts read of a `by_pnu_lanes` lane, whether it is written
    whole (root ADR-0190) or as a kind line (root ADR-0193 §4, §9): its Gold table, the lane whose
    preview its latency is measured against, the lane whose documents it follows (`""` for none),
    the runtime-secrets group its serving monitor reads, and its Spark `producer` (`""` for none):
    an own-producer kind bakes in its own by-PNU bake job, not in `all` (root ADR-0303)."""
    entry = json.loads(path.read_text(encoding="utf-8"))["by_pnu_lanes"][lane]
    if "host" not in entry:
        return {"gold_table": entry["gold_table"], "latency_baseline_lane": entry["latency_baseline_lane"],
                "documents_follow_lane": entry.get("documents_follow_lane", ""),
                "monitor_group": f"{lane}-serving-monitor", "own_producer": ""}
    found = kind(lane, path)
    host_lane = found.host.removesuffix("_by_pnu_gateway")
    return {"gold_table": found.gold_table, "latency_baseline_lane": host_lane,
            "documents_follow_lane": entry.get("documents_follow_lane", host_lane),
            "monitor_group": "by-pnu-kind-serving-monitor", "own_producer": found.producer or ""}


def document_record(document: str, path: Path = RECORDS_PATH) -> set[str]:
    """The field paths of the document the publisher bakes now, so a Gold JSON column the document
    carries as written is held to it here: a subject the schema registry lists (root ADR-0336) by its
    current revision, any other document by the newest committed record
    (`foundation-platform.<document>.v<n>.json`, which the publisher's test holds to be the version
    it bakes)."""
    import importlib.util  # noqa: PLC0415 - the registry tool is read by tests, never by a Spark run

    tool = path.parents[1] / "scripts" / "schema_registry.py"
    spec = importlib.util.spec_from_file_location("schema_registry", tool)
    registry_tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(registry_tool)
    platform = path.parents[1]
    for entry in registry_tool.load(platform)["subjects"]:
        if entry["subject"] == f"foundation-platform.{document}":
            schema = registry_tool.schema_of(platform, entry["subject"], registry_tool.current(entry))
            return {field["path"] for field in registry_tool.record_of(schema)}
    versions = {int(p.name.removesuffix(".json").rsplit(".v", 1)[1]): p
                for p in path.glob(f"foundation-platform.{document}.v*.json")}
    if not versions:
        raise ValueError(f"no record of the {document} document under {path}")
    return {entry["path"] for entry in json.loads(versions[max(versions)].read_text(encoding="utf-8"))}


def json_paths(value: object, at: str) -> set[str]:
    """Every field path of a JSON value under `at`, in the records' grammar (`a.b`, `a[].b`)."""
    if isinstance(value, dict):
        return {p for key, inner in value.items() for p in {f"{at}.{key}"} | json_paths(inner, f"{at}.{key}")}
    if isinstance(value, list):
        return {p for item in value for p in json_paths(item, f"{at}[]")}
    return set()


def kind(lane: str, path: Path = CONTRACT_PATH) -> Kind:
    found = kinds(path).get(lane)
    if found is None:
        raise ValueError(f"the R2 connection contract names no kind lane {lane}")
    return found


if __name__ == "__main__":
    # `by_pnu_kinds.py <lane> <field>`: one lane_fields value, for the host scripts.
    import sys
    print(lane_fields(sys.argv[1])[sys.argv[2]])
