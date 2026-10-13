"""The by-PNU data kinds (root ADR-0193): the lines of `config/r2-connections.contract.json`
`by_pnu_lanes` that name a host.

A kind line says which host panel it splits, which of that panel's sections it is and the
section's content columns. Everything a kind needs follows from that line: its Gold table
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
# The panel each host's kinds are sections of, and which the kinds follow while it is served.
HOST_PANELS = {"parcel_by_pnu_gateway": "gold.parcel_panel", "building_by_pnu_gateway": "gold.building_panel"}


@dataclass(frozen=True)
class Kind:
    lane: str
    host: str
    section: str
    # Content column names in the Gold's order (a `<name>:long` spec names `<name>`).
    columns: tuple[str, ...]
    # Columns the kind's Gold once carried and no longer does: the producer drops them from the live table.
    retired_columns: tuple[str, ...] = ()

    @property
    def gold_table(self) -> str:
        return "gold." + self.lane.replace("-", "_")

    @property
    def host_panel(self) -> str:
        return HOST_PANELS[self.host]


def kinds(path: Path = CONTRACT_PATH) -> dict[str, Kind]:
    """Every kind line of the contract, by lane, in the contract's order."""
    lanes = json.loads(path.read_text(encoding="utf-8")).get("by_pnu_lanes", {})
    found = {}
    for lane, entry in lanes.items():
        if "host" not in entry:
            continue
        if entry["host"] not in HOST_PANELS or not entry.get("section") or not entry.get("columns"):
            raise ValueError(f"by_pnu_lanes.{lane} is a kind line without a known host, a section or its columns")
        retired = tuple(entry.get("retired_columns", ()))
        if retired and not entry.get("retired_columns_reason"):
            raise ValueError(f"by_pnu_lanes.{lane}.retired_columns needs a retired_columns_reason")
        found[lane] = Kind(lane, entry["host"], entry["section"],
                           tuple(spec.split(":", 1)[0] for spec in entry["columns"]), retired)
    return found


def lane_fields(lane: str, path: Path = CONTRACT_PATH) -> dict[str, str]:
    """What the operator's and bake's scripts read of a `by_pnu_lanes` lane, whether it is written
    whole (root ADR-0190) or as a kind line (root ADR-0193 §4, §9): its Gold table, the lane whose
    preview its latency is measured against, the lane whose documents it follows (`""` for none),
    and the runtime-secrets group its serving monitor reads."""
    entry = json.loads(path.read_text(encoding="utf-8"))["by_pnu_lanes"][lane]
    if "host" not in entry:
        return {"gold_table": entry["gold_table"], "latency_baseline_lane": entry["latency_baseline_lane"],
                "documents_follow_lane": entry.get("documents_follow_lane", ""),
                "monitor_group": f"{lane}-serving-monitor"}
    found = kind(lane, path)
    host_lane = found.host.removesuffix("_by_pnu_gateway")
    return {"gold_table": found.gold_table, "latency_baseline_lane": host_lane,
            "documents_follow_lane": entry.get("documents_follow_lane", host_lane),
            "monitor_group": "by-pnu-kind-serving-monitor"}


def kind(lane: str, path: Path = CONTRACT_PATH) -> Kind:
    found = kinds(path).get(lane)
    if found is None:
        raise ValueError(f"the R2 connection contract names no kind lane {lane}")
    return found


if __name__ == "__main__":
    # `by_pnu_kinds.py <lane> <field>`: one lane_fields value, for the host scripts.
    import sys
    print(lane_fields(sys.argv[1])[sys.argv[2]])
