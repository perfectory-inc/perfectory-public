#!/usr/bin/env python3
"""Are two engines' outputs for the same pinned inputs the same rows? (root ADR-0557 §3)

A transform moves from one engine to another only with this verdict. Both engines read the same
pinned input snapshots and write their output rows (the key columns and `row_digest`); this module
compares them by key, never by file layout or row order:

- `equal`: every key is on both sides with the same `row_digest`;
- otherwise the counts of keys only the reference has, only the candidate has, and on both with a
  different digest, with a bounded sample of each.

`row_digest` is the one the producers already write (root ADR-0099) and the fingerprint is
`gold_fingerprint`'s (root ADR-0274 §1), so a candidate that passes writes a Gold whose recorded
fingerprint equals the reference's and the bake republishes nothing. A digest is over the content's
JSON text, so "equal" here is byte equality of what is served; rows that differ only in how a
number or a null is written are still different, and `--content` says which they are.

The comparison is one SQL statement of the common subset (FULL OUTER JOIN, COUNT, CASE), run by
DuckDB when this file is a command; the same text runs on Spark SQL and Trino, and the tests run it
on SQLite, so the verdict does not belong to either engine under comparison.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Sequence

import gold_fingerprint

REPORT_SCHEMA_VERSION = 1
SAMPLE_LIMIT = 20
# The fingerprint's digest part (`gold_fingerprint.HEX_DIGITS` hex digits of `row_digest` as a
# number) in each engine's words; the sum of it and the row count are `gold_fingerprint.value`.
DIGEST_NUMBER = {
    "spark": "CAST(CONV(SUBSTRING({digest}, 1, {digits}), 16, 10) AS DECIMAL(38,0))",
    "duckdb": "CAST(('0x' || SUBSTR({digest}, 1, {digits})) AS HUGEINT)",
    "trino": "CAST(FROM_BASE(SUBSTR({digest}, 1, {digits}), 16) AS DECIMAL(38,0))",
}


def fingerprint_sql(relation: str, engine: str, digest: str = gold_fingerprint.DIGEST) -> str:
    """One row (`rows`, `digest_sum`): `relation`'s content fingerprint, asked in `engine`'s SQL."""
    number = DIGEST_NUMBER[engine].format(digest=digest, digits=gold_fingerprint.HEX_DIGITS)
    return f"SELECT COUNT(*) AS rows, COALESCE(SUM({number}), 0) AS digest_sum FROM {relation}"


def fingerprint_of(digests: Iterable[str]) -> str:
    """`gold_fingerprint.value` of the digests, computed with no engine (the tests' reference)."""
    rows = total = 0
    for digest in digests:
        rows += 1
        total += int(digest[:gold_fingerprint.HEX_DIGITS], 16)
    return gold_fingerprint.value(rows, total)


def _on(keys: Sequence[str]) -> str:
    return " AND ".join(f"r.{key} = c.{key}" for key in keys)


def keyed_diff_sql(reference: str, candidate: str, keys: Sequence[str], digest: str = gold_fingerprint.DIGEST) -> str:
    """One row of counts comparing `candidate` with `reference` by `keys`. A key is never null and
    names one row on each side (`duplicate_*` counts the keys that do not, which is a refusal)."""
    if not keys:
        raise ValueError("an engine comparison needs the output's key columns")
    first = keys[0]
    listed = ", ".join(keys)
    return f"""
SELECT
  (SELECT COUNT(*) FROM {reference}) AS reference_rows,
  (SELECT COUNT(*) FROM {candidate}) AS candidate_rows,
  (SELECT COUNT(*) FROM (SELECT {listed} FROM {reference} GROUP BY {listed} HAVING COUNT(*) > 1) d) AS duplicate_reference_keys,
  (SELECT COUNT(*) FROM (SELECT {listed} FROM {candidate} GROUP BY {listed} HAVING COUNT(*) > 1) d) AS duplicate_candidate_keys,
  COALESCE(SUM(CASE WHEN c.{first} IS NULL THEN 1 ELSE 0 END), 0) AS only_reference,
  COALESCE(SUM(CASE WHEN r.{first} IS NULL THEN 1 ELSE 0 END), 0) AS only_candidate,
  COALESCE(SUM(CASE WHEN r.{first} IS NOT NULL AND c.{first} IS NOT NULL AND r.{digest} = c.{digest} THEN 1 ELSE 0 END), 0) AS same,
  COALESCE(SUM(CASE WHEN r.{first} IS NOT NULL AND c.{first} IS NOT NULL
                     AND (r.{digest} <> c.{digest} OR r.{digest} IS NULL OR c.{digest} IS NULL) THEN 1 ELSE 0 END), 0) AS different
FROM {reference} r FULL OUTER JOIN {candidate} c ON {_on(keys)}
""".strip()


def sample_sql(reference: str, candidate: str, keys: Sequence[str], side: str, content: Sequence[str] = (),
               digest: str = gold_fingerprint.DIGEST, limit: int = SAMPLE_LIMIT) -> str:
    """At most `limit` keys of one kind of difference (`only_reference`, `only_candidate`,
    `different`), lowest key first; a `different` row also carries both sides' `content` columns.
    The keys are picked from the key and digest columns alone and the content is read for those
    keys only: a document column carried through the whole join is the whole output in memory
    (2026-11-12, 5.2M rows of `nearby_json`: 5 GiB and no answer in ten minutes)."""
    first = keys[0]
    where = {"only_reference": f"c.{first} IS NULL", "only_candidate": f"r.{first} IS NULL",
             "different": f"r.{first} IS NOT NULL AND c.{first} IS NOT NULL "
                          f"AND (r.{digest} <> c.{digest} OR r.{digest} IS NULL OR c.{digest} IS NULL)"}[side]
    key_columns = ", ".join(f"COALESCE(r.{key}, c.{key}) AS {key}" for key in keys)
    order = ", ".join(str(index + 1) for index in range(len(keys)))
    picked = (f"SELECT {key_columns} FROM {reference} r FULL OUTER JOIN {candidate} c ON {_on(keys)} "
              f"WHERE {where} ORDER BY {order} LIMIT {int(limit)}")
    if side != "different" or not content:
        return picked
    shown = ", ".join(f"p.{key} AS {key}" for key in keys)
    extra = "".join(f", r.{name} AS reference_{name}, c.{name} AS candidate_{name}" for name in content)
    on = lambda side_alias: " AND ".join(f"{side_alias}.{key} = p.{key}" for key in keys)  # noqa: E731
    return (f"WITH p AS ({picked}) SELECT {shown}{extra} FROM p JOIN {reference} r ON {on('r')} "
            f"JOIN {candidate} c ON {on('c')} ORDER BY {order}")


def written_alike(reference: Any, candidate: Any) -> bool:
    """Whether two content values differ only in how they are written: equal once every JSON text
    in them is parsed (key order, number spelling, an absent key against a null one)."""

    def parsed(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return parsed(json.loads(value))
            except ValueError:
                return value
        if isinstance(value, dict):
            return {key: parsed(item) for key, item in value.items() if item is not None}
        if isinstance(value, (list, tuple)):
            return [parsed(item) for item in value]
        if isinstance(value, float) and value == int(value):
            return int(value)
        return value

    return parsed(reference) == parsed(candidate)


@dataclass(frozen=True)
class Verdict:
    """What a comparison found. `equal` is the only state a migration may proceed on."""

    reference_rows: int
    candidate_rows: int
    same: int
    different: int
    only_reference: int
    only_candidate: int
    duplicate_reference_keys: int = 0
    duplicate_candidate_keys: int = 0
    reference_fingerprint: str | None = None
    candidate_fingerprint: str | None = None
    written_differently: int | None = None
    samples: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    @property
    def equal(self) -> bool:
        return (not (self.different or self.only_reference or self.only_candidate
                     or self.duplicate_reference_keys or self.duplicate_candidate_keys)
                and self.reference_rows == self.candidate_rows == self.same
                and self.reference_fingerprint == self.candidate_fingerprint)

    def report(self, **named: Any) -> dict[str, Any]:
        """The record a migration keeps: who was compared, on which pins, and the verdict."""
        return {"schema_version": REPORT_SCHEMA_VERSION, **named, "equal": self.equal, **asdict(self)}


def verdict_of(counts: dict[str, Any], **more: Any) -> Verdict:
    return Verdict(**{key: int(value) for key, value in counts.items()}, **more)


def compare(run: Any, reference: str, candidate: str, keys: Sequence[str], engine: str,
            content: Sequence[str] = ()) -> Verdict:
    """Compare two relations through `run(sql) -> list[dict]`, the caller's engine connection."""
    counts = run(keyed_diff_sql(reference, candidate, keys))[0]
    prints = [gold_fingerprint.value(row["rows"], row["digest_sum"])
              for row in (run(fingerprint_sql(name, engine))[0] for name in (reference, candidate))]
    samples = {side: run(sample_sql(reference, candidate, keys, side, content))
               for side in ("only_reference", "only_candidate", "different") if int(counts[side])}
    written = None
    if content and samples.get("different"):
        written = sum(all(written_alike(row[f"reference_{name}"], row[f"candidate_{name}"]) for name in content)
                      for row in samples["different"])
    return verdict_of(counts, reference_fingerprint=prints[0], candidate_fingerprint=prints[1],
                      written_differently=written, samples=samples)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, help="Parquet path or glob of the engine in production.")
    parser.add_argument("--candidate", required=True, help="Parquet path or glob of the engine being proven.")
    parser.add_argument("--key", action="append", required=True, help="An output key column; repeat for a composite key.")
    parser.add_argument("--content", action="append", default=[], help="A content column to show for differing rows.")
    parser.add_argument("--job", required=True, help="The transform compared, as the engine contract names it.")
    parser.add_argument("--reference-engine", required=True)
    parser.add_argument("--candidate-engine", required=True)
    parser.add_argument("--pins", help="JSON file of the input snapshot pins both engines read.")
    parser.add_argument("--report", help="Write the verdict here as JSON.")
    parser.add_argument("--memory-limit", help="DuckDB memory_limit for the comparison (for example 4GB); past it the join spills.")
    parser.add_argument("--temp-directory", help="Where the comparison spills.")
    args = parser.parse_args(argv)

    import duckdb  # noqa: PLC0415  (only the command needs an engine; the module does not)

    connection = duckdb.connect()
    for setting, value in (("memory_limit", args.memory_limit), ("temp_directory", args.temp_directory)):
        if value:
            connection.execute(f"SET {setting} = '{value.replace(chr(39), chr(39) * 2)}'")

    def run(sql: str) -> list[dict[str, Any]]:
        cursor = connection.execute(sql)
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    relations = []
    for name, path in (("reference", args.reference), ("candidate", args.candidate)):
        quoted = path.replace("\\", "/").replace("'", "''")
        connection.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{quoted}')")
        relations.append(name)
    found = compare(run, *relations, args.key, "duckdb", args.content)
    pins = json.load(open(args.pins, encoding="utf-8")) if args.pins else None
    report = found.report(job=args.job, reference_engine=args.reference_engine, candidate_engine=args.candidate_engine,
                          keys=list(args.key), input_pins=pins)
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as out:
            out.write(text + "\n")
    print(text)
    return 0 if found.equal else 1


if __name__ == "__main__":
    sys.exit(main())
