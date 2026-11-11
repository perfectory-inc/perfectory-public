#!/usr/bin/env python3
"""The cadastral parcel editions the source contract names (root ADR-0067, ADR-0148).

`vworld-parcel-source-objects.json` lists, per provider edition (`YYYYMM`), the Bronze objects
that hold it, when the provider extracted it, and where its converted handoffs live. Everything
else about an edition is derived here, once, for every reader — the converter and the batch
loader (through the command line below), the legal-dong pairing, the parcel panel and the
lineage migration (by import):

- its `source_snapshot_id` in `silver.parcel_boundaries` (`snapshot_id_prefix` + edition), and
  its `valid_from_utc` (the first instant of its base month). Both used to be typed per run, and
  the same June data was written as `vworldkr__parcel-202606` while the September data was written
  as `vworldkr__parcel:202609` with the collection time as its validity;
- which edition the map and the catalog are built from (`served_edition`);
- which two editions bracket a code change (`bracketing`), for the legal-dong pairing's 지번 step.

The check (`validate`) refuses a contract whose editions could be mistaken for one another: an
object named in two editions, a member whose edition is not its edition's, a granularity whose
coverage disagrees with the other, or two editions sharing a handoff prefix.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = 2
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "vworld-parcel-source-objects.json"
CONTRACT_ENV = "VWORLD_PARCEL_SOURCE_CONTRACT"
EDITION = re.compile(r"^(\d{4})(0[1-9]|1[0-2])$")
GRANULARITIES = ("sido", "sigungu")


class EditionError(ValueError):
    """The contract, or the edition asked of it, cannot be used as asked."""


def contract_path() -> Path:
    return Path(os.environ.get(CONTRACT_ENV) or CONTRACT_PATH)


def load(path: Path | None = None, ledger: Path | None = None) -> dict[str, Any]:
    """The contract with the host's promotions ledger laid over it (root ADR-0490 §5), checked.

    `ledger` defaults to `host_ledger_path`: on the host and in its Spark containers the file the
    readiness judgement appends a granted promotion to; elsewhere (CI, a laptop) none, and the
    contract's own ledger is the whole ledger."""

    contract = json.loads((path or contract_path()).read_text(encoding="utf-8"))
    found = ledger if ledger is not None else host_ledger_path(contract)
    if found is not None and found.is_file():
        contract = overlay(contract, read_ledger(found))
    validate(contract)
    return contract


# The host's promotions ledger (root ADR-0490 §5): one promotion per line, appended by
# `parcel_edition_readiness.py grant`, never rewritten. The variable names another file (the tests').
LEDGER_ENV = "VWORLD_PARCEL_PROMOTIONS_LEDGER"


def host_ledger_path(contract: Mapping[str, Any]) -> Path | None:
    """The first of `$VWORLD_PARCEL_PROMOTIONS_LEDGER`, the contract's host path and its path inside the
    Spark containers (compose.lakehouse.yml mounts it read-only) whose directory exists. A file not
    made yet is no promotion yet; a directory that is absent is not the host."""

    if os.environ.get(LEDGER_ENV):
        return Path(os.environ[LEDGER_ENV])
    declared = contract.get("host_promotions_ledger") or {}
    for key in ("path", "container_path"):
        if declared.get(key) and Path(declared[key]).parent.is_dir():
            return Path(declared[key])
    return None


def read_ledger(path: Path) -> list[dict[str, Any]]:
    entries = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as error:
            raise EditionError(f"{path}:{number} is not a promotion: {error}") from error
        if not isinstance(entry, dict):
            raise EditionError(f"{path}:{number} is not a promotion")
        entries.append(entry)
    return entries


def overlay(contract: Mapping[str, Any], entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The contract with the host ledger's promotions after its own, and `served_edition` the last.

    A host promotion a reviewed change has since copied into the contract (same edition and instant) is
    the contract's already and is not added twice. The rest must continue the contract's ledger;
    `validate_promotions` refuses one that does not, so a host ledger and a contract that disagree stop
    every reader instead of serving two editions."""

    held = {(str(entry.get("edition")), str(entry.get("promoted_at_utc"))) for entry in contract.get("promotions") or []}
    added = [dict(entry) for entry in entries if (str(entry.get("edition")), str(entry.get("promoted_at_utc"))) not in held]
    if not added:
        return dict(contract)
    merged = json.loads(json.dumps(contract))
    merged["promotions"] = list(merged.get("promotions") or []) + added
    merged["served_edition"] = added[-1]["edition"]
    return merged


def _day(value: str, what: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise EditionError(f"{what} is not a YYYY-MM-DD date: {value!r}") from error


def validate(contract: Mapping[str, Any]) -> None:
    if contract.get("schema_version") != SCHEMA_VERSION:
        raise EditionError(f"source object contract schema_version {contract.get('schema_version')!r} is not {SCHEMA_VERSION}")
    editions = contract.get("editions")
    if not isinstance(editions, Mapping) or not editions:
        raise EditionError("the source contract names no editions")
    if contract.get("load_granularity") not in GRANULARITIES:
        raise EditionError(f"load_granularity must be one of {GRANULARITIES}")
    if contract.get("served_edition") not in editions:
        raise EditionError(f"served_edition {contract.get('served_edition')!r} is not one of the editions {sorted(editions)}")
    if not contract.get("snapshot_id_prefix") or not contract.get("handoff_suffix"):
        raise EditionError("the source contract must name snapshot_id_prefix and handoff_suffix")
    seen_keys: dict[str, str] = {}
    seen_names: dict[str, str] = {}
    prefixes: dict[str, str] = {}
    for name, edition in editions.items():
        if not EDITION.fullmatch(name):
            raise EditionError(f"edition {name!r} is not YYYYMM")
        if edition.get("provider_base_month") != f"{name[:4]}-{name[4:]}":
            raise EditionError(f"edition {name} names provider_base_month {edition.get('provider_base_month')!r}")
        extracted = edition.get("extracted_on") or {}
        if _day(extracted.get("earliest"), f"{name} extracted_on.earliest") > _day(extracted.get("latest"), f"{name} extracted_on.latest"):
            raise EditionError(f"edition {name} was extracted from {extracted['earliest']} to an earlier {extracted['latest']}")
        prefix = edition.get("handoff_prefix")
        if not prefix:
            raise EditionError(f"edition {name} names no handoff_prefix")
        if prefix in prefixes:
            raise EditionError(f"editions {prefixes[prefix]} and {name} share the handoff prefix {prefix}; one would read the other's handoffs")
        prefixes[prefix] = name
        objects = edition.get("objects") or []
        covered: dict[str, set[str]] = {granularity: set() for granularity in GRANULARITIES}
        for obj in objects:
            key = obj["object_key"]
            base = key.rsplit("/", 1)[-1]
            if key in seen_keys or base in seen_names:
                raise EditionError(f"{key} is named by editions {seen_keys.get(key) or seen_names.get(base)} and {name}")
            seen_keys[key], seen_names[base] = name, name
            if not base.endswith(".zip"):
                raise EditionError(f"{key} is not a .zip")
            if obj["granularity"] not in GRANULARITIES:
                raise EditionError(f"{key} has granularity {obj['granularity']!r}")
            if obj["dataset_name"] != f"LSMD_CONT_LDREG_{obj['region_code']}_{name}":
                raise EditionError(f"{key} holds {obj['dataset_name']}, not edition {name} of {obj['region_code']}")
            covered[obj["granularity"]].add(obj["region_code"][:2])
        counts = edition.get("granularity_counts") or {}
        for granularity in GRANULARITIES:
            held = sum(1 for obj in objects if obj["granularity"] == granularity)
            if counts.get(granularity) != held:
                raise EditionError(f"edition {name} counts {counts.get(granularity)} {granularity} objects and lists {held}")
        # Every 시도 a province object covers must have its 시군구 objects: a province without them is
        # a gap in the covering that loads. The reverse need not hold: the provider serves its
        # largest province files only through its download agent (RAON), which the scheduled
        # collection does not drive, and the province covering is never loaded (root ADR-0148).
        if not covered["sigungu"] or not covered["sido"] <= covered["sigungu"]:
            raise EditionError(f"edition {name}: its sido objects cover {sorted(covered['sido'])} and its sigungu objects "
                               f"{sorted(covered['sigungu'])}; every province needs its districts")
    validate_promotions(contract)


# The day root ADR-0291 gated promotion: a promotion before it rested on a reviewed pull request alone.
READINESS_GATED_FROM = "2026-10-21"
PROMOTION_STAGES = ("source", "silver", "rehearsal")


def validate_promotions(contract: Mapping[str, Any]) -> None:
    """The served edition is the last promotion of an append-only ledger, and each promotion after root
    ADR-0291 carries the readiness it passed (`parcel_edition_readiness.promote` is its one writer).
    A hand edit of `served_edition` fails here, in every reader and every test that loads the contract."""

    gate = contract.get("readiness") or {}
    if not 0 < float(gate.get("max_row_loss_fraction", 0)) < 1 or float(gate.get("max_report_age_hours", 0)) <= 0:
        raise EditionError("the source contract's readiness names max_row_loss_fraction (0..1) and max_report_age_hours")
    if float(gate.get("max_served_lag_days", 0)) <= 0 or not gate.get("max_served_lag_days_reason"):
        raise EditionError("the source contract's readiness names max_served_lag_days and why (root ADR-0490 §7)")
    ledger = contract.get("promotions")
    if not isinstance(ledger, list) or not ledger:
        raise EditionError("the source contract holds no promotions ledger; served_edition is set only by a promotion")
    previous: Mapping[str, Any] | None = None
    for index, entry in enumerate(ledger):
        name = entry.get("edition")
        if name not in contract["editions"]:
            raise EditionError(f"promotion {index} names edition {name!r}, which the contract does not hold")
        if not str(entry.get("by") or "").strip() or not str(entry.get("reason") or "").strip():
            raise EditionError(f"promotion {index} ({name}) names no one who asked or no reason")
        at = str(entry.get("promoted_at_utc") or "")
        if previous is not None:
            if at <= str(previous["promoted_at_utc"]) or entry.get("from_edition") != previous["edition"]:
                raise EditionError(f"promotion {index} ({name}) does not follow promotion {index - 1}: the ledger only grows")
        readiness = entry.get("readiness")
        if entry.get("rollback_of") is not None:
            # A rollback (root ADR-0490 §7) returns to the edition served before the promotion whose first
            # publish failed; it carries why, not a readiness, since that edition was served already.
            undone = previous or {}
            if (index == 0 or entry.get("rollback_of") != undone.get("promoted_at_utc")
                    or name != undone.get("from_edition") or not str(entry.get("cause") or "").strip()):
                raise EditionError(f"promotion {index} ({name}) is no rollback of promotion {index - 1}: it returns to "
                                   "the edition that promotion moved from, names it by promoted_at_utc, and says why")
        elif readiness is None:
            if index != 0 or at[:10] >= READINESS_GATED_FROM:
                raise EditionError(f"promotion {index} ({name}) carries no readiness; only the first, before "
                                   f"{READINESS_GATED_FROM}, may (root ADR-0291 §4)")
        elif sorted((readiness.get("stages") or {})) != sorted(PROMOTION_STAGES) or not readiness.get("contract_sha256"):
            raise EditionError(f"promotion {index} ({name}) does not carry the readiness it passed")
        previous = entry
    if ledger[-1]["edition"] != contract["served_edition"]:
        raise EditionError(f"served_edition {contract['served_edition']} is not the last promotion's "
                           f"{ledger[-1]['edition']}; move it with parcel_edition_readiness.py promote")


def edition(contract: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    found = contract["editions"].get(name)
    if found is None:
        raise EditionError(f"the source contract holds no edition {name!r}; it holds {sorted(contract['editions'])}")
    return found


def names(contract: Mapping[str, Any]) -> list[str]:
    return sorted(contract["editions"])


def served(contract: Mapping[str, Any]) -> str:
    return str(contract["served_edition"])


def snapshot_id(contract: Mapping[str, Any], name: str) -> str:
    edition(contract, name)
    return f"{contract['snapshot_id_prefix']}{name}"


ALLOW_OTHER_EDITION = "--allow-non-served-edition"


def add_reader_flag(parser: argparse.ArgumentParser) -> None:
    """The one override every parcel reader takes (`served_reader_id`, `edition_reader_id`)."""

    parser.add_argument(ALLOW_OTHER_EDITION, dest="allow_non_served_edition", action="store_true",
                        help="Read a parcel snapshot other than the source contract's served edition (or, for an "
                             "earlier endpoint, one the contract does not hold). Without it such an id is refused.")


def served_reader_id(given: str | None, allow_other: bool, flag: str, contract: Mapping[str, Any] | None = None) -> str:
    """The `source_snapshot_id` a parcel reader reads (ADR-0148 §1): the served edition's when none is
    given. A typed id that is not the served edition's is refused unless `allow_other`: a typo, the other
    spelling or last month's edition would otherwise build the map's tables from parcels the map does
    not serve."""

    if given is not None and allow_other:
        return given
    want = snapshot_id(contract := contract or load(), served(contract))
    if given is not None and given != want:
        raise EditionError(f"{flag} {given!r} is not the served edition's {want!r} (served_edition of the source "
                           f"contract); pass {ALLOW_OTHER_EDITION} to read another")
    return want


# The previous pins of an incremental Gold rebuild name the edition its current Gold was built from
# under this key (root ADR-0381): the comparison side of a promotion reads that edition, not the one
# the contract serves now. Only previous pins carry it; a current read is always the served edition.
PARCEL_TABLE = "silver.parcel_boundaries"
SERVED_EDITION_PIN = f"{PARCEL_TABLE}@served_edition"


def served_rows(frame: Any, contract: Mapping[str, Any] | None = None, snapshot: str | None = None) -> Any:
    """The served parcel universe of a `silver.parcel_boundaries` frame: its current rows of the served
    edition only (root ADR-0291 §5). Every edition's rows are current by the predicate, so a reader that
    takes `current_rows` of the whole table puts each parcel in once per edition and attaches its
    records to parcels the map does not serve (the 632 공매 orphans of 2026-10-21). The edition is
    filtered before the predicate is applied; the Semgrep rule
    tools/semgrep/rules/parcel-reads.yml refuses any other served-universe read."""

    from platform_contracts import current_row_predicate, load_lakehouse_contract, pinned_edition  # noqa: PLC0415

    parcels = load_lakehouse_contract("silver.parcel_boundaries")
    if current_row_predicate(parcels) is None:
        raise EditionError("silver.parcel_boundaries declares no current_row_predicate; the served parcels cannot be named")
    contract = contract or load()
    if snapshot is None:
        snapshot = snapshot_id(contract, served(contract))
    else:
        edition_of_snapshot_id(contract, snapshot)  # only an edition the contract holds, in its one spelling
    # The table's declared read of one edition (root ADR-0286 §3, ADR-0377): the edition, then the predicate.
    return pinned_edition(frame, parcels, snapshot)


def served_at(contract: Mapping[str, Any], instant: str) -> str:
    """The edition the promotions ledger served at `instant` (ISO-8601 UTC): the last promotion at or
    before it, or the first promotion's edition before any. Instants compare as their UTC strings."""

    ledger = contract["promotions"]
    held = [entry["edition"] for entry in ledger if str(entry["promoted_at_utc"]) <= instant.replace("+00:00", "Z")]
    return held[-1] if held else ledger[0]["edition"]


def edition_reader_id(given: str | None, allow_other: bool, flag: str, contract: Mapping[str, Any] | None = None) -> str | None:
    """An earlier endpoint (a lineage's `from`): any edition the contract holds, in its one spelling,
    unless `allow_other`. None stays None."""

    if given is None or allow_other:
        return given
    edition_of_snapshot_id(contract or load(), given)
    return given


def edition_of_snapshot_id(contract: Mapping[str, Any], value: str) -> str:
    """The edition a `source_snapshot_id` names; anything else is refused, including the other spelling."""

    prefix = contract["snapshot_id_prefix"]
    name = value[len(prefix):] if value.startswith(prefix) else ""
    if name not in contract["editions"]:
        raise EditionError(f"{value!r} is not the snapshot id of an edition; they are "
                           f"{[snapshot_id(contract, n) for n in names(contract)]}")
    return name


def valid_from_utc(name: str) -> str:
    match = EDITION.fullmatch(name)
    if match is None:
        raise EditionError(f"edition {name!r} is not YYYYMM")
    return f"{match.group(1)}-{match.group(2)}-01T00:00:00Z"


def load_objects(contract: Mapping[str, Any], name: str) -> list[Mapping[str, Any]]:
    """The objects of one edition at the load granularity (the other covering would double it)."""

    want = contract["load_granularity"]
    return [obj for obj in edition(contract, name)["objects"] if obj["granularity"] == want]


def handoff_key(contract: Mapping[str, Any], name: str, object_key: str) -> str:
    base = object_key.rsplit("/", 1)[-1][: -len(".zip")]
    return f"{edition(contract, name)['handoff_prefix']}/{base}{contract['handoff_suffix']}"


def all_objects(contract: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [obj for name in names(contract) for obj in contract["editions"][name]["objects"]]


def cadastral_sido(contract: Mapping[str, Any]) -> list[str]:
    """The 시도 the served parcel set carries: its sido objects."""

    return sorted({obj["region_code"][:2] for obj in edition(contract, served(contract))["objects"]
                   if obj["granularity"] == "sido"})


def bracketing(contract: Mapping[str, Any], effective: str) -> tuple[str | None, str | None]:
    """(the latest edition wholly extracted before `effective`, the earliest wholly extracted after it).

    `effective` is the change's day (`YYYYMMDD` or `YYYY-MM-DD`; a code abolished that day stops
    on it). An edition extracted on the day itself, or across it, could hold either side and is
    neither. None where the contract holds no such edition yet.
    """

    day = _day(effective if "-" in effective else f"{effective[:4]}-{effective[4:6]}-{effective[6:]}", "change date")
    before = after = None
    for name in names(contract):
        extracted = contract["editions"][name]["extracted_on"]
        if _day(extracted["latest"], name) < day:
            before = name
        elif after is None and _day(extracted["earliest"], name) > day:
            after = name
    return before, after


# --- a new provider edition (the monthly collection, scripts/ops/vworld-parcel-edition-collect.sh) -


PROVIDER_ENDPOINT = "vworld-dataset-parcel"
BASE_MONTH = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
MEMBER = re.compile(r"^LSMD_CONT_LDREG_(\d{2}|\d{5})_(\d{6})\.shp$")


def endpoint_catalog(catalog: Mapping[str, Any]) -> dict[str, Any]:
    """The endpoint catalog cut to the parcel dataset, so the plan and inventory read one dataset's
    pages a day instead of every VWorld dataset's."""

    kept = [e for e in catalog.get("endpoints", []) if e.get("endpoint_slug") == PROVIDER_ENDPOINT]
    if len(kept) != 1:
        raise EditionError(f"the endpoint catalog lists {len(kept)} {PROVIDER_ENDPOINT} endpoints, expected one")
    return {**catalog, "endpoints": kept}


def inventory_summary_csv(contract: Mapping[str, Any], catalog: Mapping[str, Any]) -> str:
    """The one-dataset summary `plan-vworld-dataset-collection` reads beside the catalog.

    The plan matches the endpoint to it by selector and carries its counts into the inventory as
    the expected ones; a difference is a warning there, not a refusal, because the provider's page
    decides. The expected counts are the latest edition's, which is what a new one should resemble.
    """

    selector = endpoint_catalog(catalog)["endpoints"][0]["provider_dataset_selector"]
    latest = contract["editions"][names(contract)[-1]]
    count = len(latest["objects"])
    gib = sum(obj["bytes"] for obj in latest["objects"]) / 2**30
    header = "module,svc_cde,ds_id,file_pages,file_count,large_file_count,listed_gib"
    return f"{header}\nvworld_dataset,{selector['svc_cde']},{selector['ds_id']},{-(-count // 10)},{count},0,{gib:.2f}\n"


def _parcel_files(inventory: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    jobs = [job for job in inventory.get("jobs", []) if job.get("endpoint_slug") == PROVIDER_ENDPOINT]
    if len(jobs) != 1:
        raise EditionError(f"the inventory holds {len(jobs)} {PROVIDER_ENDPOINT} jobs, expected one")
    files = list(jobs[0].get("files") or [])
    if not files:
        raise EditionError("the provider lists no parcel files: an empty page is not 'no new edition'")
    return files


def provider_edition(contract: Mapping[str, Any], inventory: Mapping[str, Any]) -> tuple[str, bool]:
    """(the newest edition the provider lists, whether the contract holds it).

    The provider keeps files of an abolished 시군구 at their last edition beside the current ones
    (2026-10: three 2026-06 files of Incheon's old 구 among the 2026-09 edition), so its edition is
    the newest base month it lists, not every file's. A file without a base month (the column
    definition document) says nothing either way. An edition older than one the contract holds is
    refused: the provider went back, or the page is not what it was.
    """

    months = sorted({f"{m.group(1)}{m.group(2)}" for f in _parcel_files(inventory)
                     if (m := BASE_MONTH.fullmatch(str(f.get("base_ym", "")).strip()))})
    if not months:
        raise EditionError("no parcel file lists a base month (YYYY-MM)")
    newest = months[-1]
    held = names(contract)
    if newest < held[-1]:
        raise EditionError(f"the provider's newest edition {newest} is older than the contract's {held[-1]}")
    return newest, newest in contract["editions"]


def held_reuploads(contract: Mapping[str, Any], inventory: Mapping[str, Any], name: str) -> list[str]:
    """Files of a held edition the provider updated after the edition was extracted.

    The provider can re-upload a file under the same base month; the contract still describes the
    earlier upload, and the daily check would otherwise say only "held". Each is
    `<file name> <갱신일>`, for the job's summary to warn about.
    """

    month = f"{name[:4]}-{name[4:]}"
    latest = edition(contract, name)["extracted_on"]["latest"]
    return sorted(f"{f.get('provider_file_name')} {str(f.get('updated_at'))[:10]}" for f in _parcel_files(inventory)
                  if str(f.get("base_ym", "")).strip() == month and str(f.get("updated_at") or "")[:10] > latest)


def select_inventory(inventory: Mapping[str, Any], name: str) -> dict[str, Any]:
    """The inventory cut to the directly downloadable files of one edition, for the ingest to
    collect. Which covering loads is decided by measuring the ZIPs, not by the provider's file
    names. The files served only through the provider's download agent (`selection_archive`: in
    2026-10 the six largest province files, never a district) are left out; every other file of the
    edition must then be collected, or nothing is proposed."""

    month = f"{name[:4]}-{name[4:]}"
    cut = json.loads(json.dumps(inventory))
    for job in cut["jobs"]:
        if job.get("endpoint_slug") == PROVIDER_ENDPOINT:
            job["files"] = [f for f in job["files"] if str(f.get("base_ym", "")).strip() == month
                            and f.get("download_kind") == "single_resource_file"]
            job["discovered_file_count"] = len(job["files"])
        else:
            job["files"] = []
    cut["jobs"] = [job for job in cut["jobs"] if job["files"]]
    if not cut["jobs"]:
        raise EditionError(f"the inventory lists no file of edition {name}")
    return cut


def check_not_partial(contract: Mapping[str, Any], name: str, objects: Sequence[Mapping[str, Any]]) -> None:
    """Refuses a proposal that covers less of the country than the edition before it.

    A provider listing caught half-updated (some files already the new month, the rest still the
    old one) yields an edition of half the 시군구, which the contract checks alone would accept.
    Against the previous edition (`new_edition_bounds`): the 시도 its 시군구 objects cover may change
    only by a merger — a 시도 leaves only while another appears, at most `max_sido_set_difference`
    codes in all — and its 시군구 count by at most `max_sigungu_count_change_share`.
    """

    earlier = [held for held in names(contract) if held < name]
    if not earlier:
        return
    bounds = contract.get("new_edition_bounds")
    if not isinstance(bounds, Mapping):
        raise EditionError("the source contract names no new_edition_bounds; a proposal cannot be checked against its predecessor")
    previous = earlier[-1]

    def districts(objs: Sequence[Mapping[str, Any]]) -> tuple[set[str], int]:
        codes = [obj["region_code"] for obj in objs if obj["granularity"] == "sigungu"]
        return {code[:2] for code in codes}, len(codes)

    was_sido, was_count = districts(contract["editions"][previous]["objects"])
    now_sido, now_count = districts(objects)
    left, appeared = was_sido - now_sido, now_sido - was_sido
    if (left and not appeared) or len(left | appeared) > int(bounds["max_sido_set_difference"]):
        raise EditionError(
            f"edition {name} covers the 시도 {sorted(now_sido)} where {previous} covered {sorted(was_sido)} "
            f"(left {sorted(left)}, appeared {sorted(appeared)}): a partial collection, not a new edition")
    if abs(now_count - was_count) > was_count * float(bounds["max_sigungu_count_change_share"]):
        raise EditionError(
            f"edition {name} holds {now_count} 시군구 objects where {previous} held {was_count} (bound "
            f"{bounds['max_sigungu_count_change_share']}): a partial collection, not a new edition")


def propose(contract: Mapping[str, Any], name: str, measured: Sequence[Mapping[str, Any]], prefix: str,
            measured_on: date | None = None) -> dict[str, Any]:
    """The contract entry for edition `name`, from the ZIPs as they were measured.

    `measured` holds one row per collected object: its key, bytes, the members of its central
    directory and their dates (`vworld_parcel_edition_members.py`). Each object must hold exactly one
    shapefile of this edition; the entry is checked against the contract as it would stand with it,
    so a proposal that could not be merged is refused here rather than in review.
    """

    objects, dates = [], []
    for row in measured:
        shapes = [m for m in row["members"] if m.endswith(".shp")]
        found = MEMBER.fullmatch(shapes[0]) if len(shapes) == 1 else None
        if found is None or found.group(2) != name:
            raise EditionError(f"{row['object_key']} holds {shapes}, not one shapefile of edition {name}")
        code = found.group(1)
        objects.append({"object_key": row["object_key"], "bytes": row["bytes"],
                        "dataset_name": shapes[0][: -len(".shp")], "region_code": code,
                        "granularity": "sido" if len(code) == 2 else "sigungu"})
        dates.extend(row["member_dates"])
    if not objects:
        raise EditionError(f"nothing was measured for edition {name}")
    objects.sort(key=lambda obj: (obj["granularity"] != "sido", obj["region_code"]))
    entry = {
        "provider_base_month": f"{name[:4]}-{name[4:]}",
        # Every member's date, not each file's newest: `bracketing` calls an edition wholly after a
        # change only when its earliest member is, so a file whose members straddle the change
        # (part written before it) must pull `earliest` back to its oldest member.
        "extracted_on": {"earliest": min(dates), "latest": max(dates)},
        "handoff_prefix": prefix,
        # The proposal is the measurement: the readiness check's source stage reads this date, and an
        # entry without it never reached ready (202610, collected 2026-10-15, root ADR-0380).
        "measured_at_utc": (measured_on or datetime.now(timezone.utc).date()).isoformat(),
        "how_measured": ("Each object's ZIP central directory was read by ranged request "
                         "(vworld_parcel_edition_members.py); the region code is the .shp member's, the "
                         "extraction dates every member's timestamp."),
        "granularity_counts": {g: sum(1 for obj in objects if obj["granularity"] == g) for g in GRANULARITIES},
        "objects": objects,
    }
    merged = json.loads(json.dumps(contract))
    merged["editions"][name] = entry
    validate(merged)
    check_not_partial(contract, name, objects)
    return entry


# --- command line (the shell loaders' only way into the contract) -----------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check", "editions", "served", "snapshot-id", "valid-from",
                                            "handoff-prefix", "handoff-suffix", "source-keys", "handoff-keys",
                                            "endpoint-catalog", "provider-edition", "held-reuploads", "select-inventory",
                                            "propose"))
    parser.add_argument("--edition", help="YYYYMM; required by every command naming one edition")
    parser.add_argument("--contract", help=f"defaults to ${CONTRACT_ENV}, then the contract beside this job")
    parser.add_argument("--inventory", help="provider-edition, select-inventory: the inventory report")
    parser.add_argument("--catalog", help="endpoint-catalog: public-source-endpoint-catalog.v1.json")
    parser.add_argument("--summary-output", help="endpoint-catalog: the plan's inventory summary CSV")
    parser.add_argument("--measured", help="propose: JSON lines from vworld_parcel_edition_members.py")
    parser.add_argument("--output", help="endpoint-catalog, select-inventory, propose: where to write")
    args = parser.parse_args(argv)
    try:
        contract = load(Path(args.contract) if args.contract else None)
        if args.command == "endpoint-catalog":
            catalog = json.loads(Path(args.catalog).read_text(encoding="utf-8"))
            Path(args.output).write_text(json.dumps(endpoint_catalog(catalog), ensure_ascii=False) + "\n", encoding="utf-8")
            if args.summary_output:
                Path(args.summary_output).write_text(inventory_summary_csv(contract, catalog), encoding="utf-8")
            return 0
        if args.command == "provider-edition":
            newest, held = provider_edition(contract, json.loads(Path(args.inventory).read_text(encoding="utf-8")))
            print(f"{newest} {'held' if held else 'new'}")
            return 0
        if args.command == "held-reuploads":
            found = held_reuploads(contract, json.loads(Path(args.inventory).read_text(encoding="utf-8")), args.edition)
            print("\n".join(found))
            return 0
        if args.command == "select-inventory":
            cut = select_inventory(json.loads(Path(args.inventory).read_text(encoding="utf-8")), args.edition)
            Path(args.output).write_text(json.dumps(cut, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            print(sum(len(job["files"]) for job in cut["jobs"]))
            return 0
        if args.command == "propose":
            rows = [json.loads(line) for line in Path(args.measured).read_text(encoding="utf-8").splitlines() if line.strip()]
            prefix = f"{edition(contract, served(contract))['handoff_prefix'].split('/edition=')[0]}/edition={args.edition}"
            entry = propose(contract, args.edition, rows, prefix)
            Path(args.output).write_text(json.dumps({args.edition: entry}, ensure_ascii=False, indent=2) + "\n",
                                         encoding="utf-8")
            print(f"edition={args.edition} {entry['granularity_counts']} extracted={entry['extracted_on']}")
            return 0
        if args.command == "check":
            print(f"ok editions={','.join(names(contract))} served={served(contract)}")
            return 0
        if args.command == "editions":
            print("\n".join(names(contract)))
            return 0
        if args.command == "served":
            print(served(contract))
            return 0
        if args.command == "handoff-suffix":
            print(contract["handoff_suffix"])
            return 0
        if not args.edition:
            raise EditionError(f"{args.command} needs --edition (one of {names(contract)})")
        if args.command == "snapshot-id":
            print(snapshot_id(contract, args.edition))
        elif args.command == "valid-from":
            edition(contract, args.edition)
            print(valid_from_utc(args.edition))
        elif args.command == "handoff-prefix":
            print(edition(contract, args.edition)["handoff_prefix"])
        elif args.command == "source-keys":
            print("\n".join(obj["object_key"] for obj in load_objects(contract, args.edition)))
        else:
            print("\n".join(handoff_key(contract, args.edition, obj["object_key"]) for obj in load_objects(contract, args.edition)))
        return 0
    except EditionError as error:
        print(f"vworld-parcel-editions: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
