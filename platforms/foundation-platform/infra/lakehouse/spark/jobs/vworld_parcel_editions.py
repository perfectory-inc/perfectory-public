#!/usr/bin/env python3
"""The cadastral parcel editions (root ADR-0067, ADR-0148, ADR-0224).

`vworld-parcel-source-objects.json` holds the rules; which Bronze objects hold an edition, when
the provider extracted it and where its converted handoffs live are the Bronze ledger's, derived
at run time by the publisher's `vworld-parcel-editions` command (`--editions` here, or
`$VWORLD_PARCEL_EDITIONS`). The contract alone answers the served edition, its 시도 and every
`source_snapshot_id`; a reader that needs an edition's objects or dates needs the ledger's
document, and is refused without it. Everything else about an edition is derived here, once, for
every reader — the converter and the batch loader (through the command line below), the
legal-dong pairing, the parcel panel and the lineage migration (by import):

- its `source_snapshot_id` in `silver.parcel_boundaries` (`snapshot_id_prefix` + edition), and
  its `valid_from_utc` (the first instant of its base month). Both used to be typed per run, and
  the same June data was written as `vworldkr__parcel-202606` while the September data was written
  as `vworldkr__parcel:202609` with the collection time as its validity;
- which edition the map and the catalog are built from (`served_edition`);
- which two editions bracket a code change (`bracketing`), for the legal-dong pairing's 지번 step.

The check (`validate`) refuses a contract that lists objects again, and editions that could be
mistaken for one another: an object named in two editions, a member whose edition is not its
edition's, a granularity whose coverage disagrees with the other, or two editions sharing a
handoff prefix.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = 3
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "vworld-parcel-source-objects.json"
CONTRACT_ENV = "VWORLD_PARCEL_SOURCE_CONTRACT"
EDITIONS_ENV = "VWORLD_PARCEL_EDITIONS"
RETIRED_KEYS = ("editions", "objects")
LEDGER_COMMAND = "foundation-outbox-publisher vworld-parcel-editions"
EDITION = re.compile(r"^(\d{4})(0[1-9]|1[0-2])$")
GRANULARITIES = ("sido", "sigungu")


class EditionError(ValueError):
    """The contract, or the edition asked of it, cannot be used as asked."""


def contract_path() -> Path:
    return Path(os.environ.get(CONTRACT_ENV) or CONTRACT_PATH)


def _retired(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for key, inner in value.items():
            if key in RETIRED_KEYS:
                return key
            if (found := _retired(inner)) is not None:
                return found
    if isinstance(value, list):
        return next((found for item in value if (found := _retired(item)) is not None), None)
    return None


def load(path: Path | None = None, editions_path: str | Path | None = None) -> dict[str, Any]:
    """The contract, with the ledger's editions when a document is given (or named by
    `$VWORLD_PARCEL_EDITIONS`)."""

    contract = json.loads((path or contract_path()).read_text(encoding="utf-8"))
    if (key := _retired(contract)) is not None:
        raise EditionError(f"the source contract holds `{key}`: an edition's objects are the Bronze ledger's, "
                           f"read at run time ({LEDGER_COMMAND}, root ADR-0224), not a list in the contract")
    editions_path = editions_path or os.environ.get(EDITIONS_ENV)
    if editions_path:
        document = json.loads(Path(editions_path).read_text(encoding="utf-8"))
        contract = {**contract, "editions": document["editions"]}
    validate(contract)
    return contract


def _day(value: str, what: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise EditionError(f"{what} is not a YYYY-MM-DD date: {value!r}") from error


def validate(contract: Mapping[str, Any]) -> None:
    if contract.get("schema_version") != SCHEMA_VERSION:
        raise EditionError(f"source object contract schema_version {contract.get('schema_version')!r} is not {SCHEMA_VERSION}")
    if contract.get("load_granularity") not in GRANULARITIES:
        raise EditionError(f"load_granularity must be one of {GRANULARITIES}")
    if not EDITION.fullmatch(str(contract.get("served_edition"))):
        raise EditionError(f"served_edition {contract.get('served_edition')!r} is not YYYYMM")
    if not contract.get("served_edition_sido"):
        raise EditionError("the source contract must name served_edition_sido")
    if not contract.get("snapshot_id_prefix") or not contract.get("handoff_suffix"):
        raise EditionError("the source contract must name snapshot_id_prefix and handoff_suffix")
    editions = contract.get("editions")
    if editions is None:
        return
    if contract["served_edition"] not in editions:
        raise EditionError(f"served_edition {contract['served_edition']!r} is not one of the ledger's editions {sorted(editions)}")
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


def _ledger(contract: Mapping[str, Any]) -> Mapping[str, Any]:
    if "editions" not in contract:
        raise EditionError(f"which objects hold an edition is the Bronze ledger's: pass --editions <the output of "
                           f"{LEDGER_COMMAND}> (or set ${EDITIONS_ENV}) (root ADR-0224)")
    return contract["editions"]


def edition(contract: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    found = _ledger(contract).get(name)
    if found is None:
        raise EditionError(f"the source contract holds no edition {name!r}; it holds {sorted(contract['editions'])}")
    return found


def names(contract: Mapping[str, Any]) -> list[str]:
    return sorted(_ledger(contract))


def served(contract: Mapping[str, Any]) -> str:
    return str(contract["served_edition"])


def snapshot_id(contract: Mapping[str, Any], name: str) -> str:
    """`snapshot_id_prefix` + edition. Without the ledger's editions any well-formed edition is
    accepted; with them, only one the ledger holds."""

    if "editions" in contract:
        edition(contract, name)
    elif not EDITION.fullmatch(str(name)):
        raise EditionError(f"edition {name!r} is not YYYYMM")
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
    held = contract["editions"] if "editions" in contract else None
    if not EDITION.fullmatch(name) or (held is not None and name not in held):
        known = [snapshot_id(contract, n) for n in names(contract)] if held is not None else f"{prefix}<YYYYMM>"
        raise EditionError(f"{value!r} is not the snapshot id of an edition; they are {known}")
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
    return [obj for name in names(contract) for obj in _ledger(contract)[name]["objects"]]


def cadastral_sido(contract: Mapping[str, Any]) -> list[str]:
    """The 시도 the served parcel set carries, as the contract states them (the ledger's editions
    refuse a served edition covering others)."""

    return sorted(contract["served_edition_sido"])


def bracketing(contract: Mapping[str, Any], effective: str) -> tuple[str | None, str | None]:
    """(the latest edition wholly extracted before `effective`, the earliest wholly extracted after it).

    `effective` is the change's day (`YYYYMMDD` or `YYYY-MM-DD`; a code abolished that day stops
    on it). An edition extracted on the day itself, or across it, could hold either side and is
    neither. None where the contract holds no such edition yet.
    """

    day = _day(effective if "-" in effective else f"{effective[:4]}-{effective[4:6]}-{effective[6:]}", "change date")
    before = after = None
    for name in names(contract):
        extracted = _ledger(contract)[name]["extracted_on"]
        if _day(extracted["latest"], name) < day:
            before = name
        elif after is None and _day(extracted["earliest"], name) > day:
            after = name
    return before, after


# --- a new provider edition (the monthly collection, scripts/ops/vworld-parcel-edition-collect.sh) -


PROVIDER_ENDPOINT = "vworld-dataset-parcel"
BASE_MONTH = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
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
    latest = _ledger(contract)[names(contract)[-1]]
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
    return newest, newest in _ledger(contract)


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


# --- command line (the shell loaders' only way into the contract) -----------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check", "editions", "served", "snapshot-id", "valid-from",
                                            "handoff-prefix", "handoff-suffix", "source-keys", "handoff-keys",
                                            "endpoint-catalog", "provider-edition", "held-reuploads", "select-inventory"))
    parser.add_argument("--edition", help="YYYYMM; required by every command naming one edition")
    parser.add_argument("--contract", help=f"defaults to ${CONTRACT_ENV}, then the contract beside this job")
    parser.add_argument("--inventory", help="provider-edition, select-inventory: the inventory report")
    parser.add_argument("--catalog", help="endpoint-catalog: public-source-endpoint-catalog.v1.json")
    parser.add_argument("--summary-output", help="endpoint-catalog: the plan's inventory summary CSV")
    parser.add_argument("--editions", help=f"the Bronze ledger's editions ({LEDGER_COMMAND}); defaults to ${EDITIONS_ENV}. "
                                           "Required by every command that reads an edition's objects or dates")
    parser.add_argument("--output", help="endpoint-catalog, select-inventory: where to write")
    args = parser.parse_args(argv)
    try:
        contract = load(Path(args.contract) if args.contract else None, args.editions)
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
        if args.command == "check":
            held = ",".join(names(contract)) if "editions" in contract else "(contract only)"
            print(f"ok editions={held} served={served(contract)}")
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
