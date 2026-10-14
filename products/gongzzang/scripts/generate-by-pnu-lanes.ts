// scripts/generate-by-pnu-lanes.ts
//
// Writes apps/web/lib/api/by-pnu-lanes.generated.ts from the foundation's connection contract: every
// `by_pnu_lanes` entry the web can read, as the path its host serves it under and the document it
// answers with (root ADR-0193 §4). A lane naming a `host` and a `kind` is resolved by the same rule
// the publisher (`resolve_lane_entry`) and the Worker (`lane.ts` `resolveLane`) apply, held to the
// contract's `by_pnu_lane_rule.test_vectors`, its Gold table being `gold.<lane>` (ADR-0193 §9); a
// lane without a host is its own Worker (ADR-0190).
//
// Run `node scripts/generate-by-pnu-lanes.ts` from products/gongzzang after editing the contract.
// A unit test renders it again and fails while the committed file differs.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export const CONTRACT_PATH = fileURLToPath(
  new URL(
    "../../../platforms/foundation-platform/config/r2-connections.contract.json",
    import.meta.url,
  ),
);
export const OUTPUT_PATH = fileURLToPath(
  new URL("../apps/web/lib/api/by-pnu-lanes.generated.ts", import.meta.url),
);

interface Gateway {
  request_path: { prefix: string };
}

interface LaneEntry {
  host?: string;
  kind?: string;
  section?: string;
  columns?: string[];
  gold_table?: string;
  document_schema_version?: string;
  request_path?: { prefix: string };
}

export interface WebLane {
  /** `parcel` or `building` for a kind line; the lane itself when it is its own Worker. */
  host: string;
  /** The kind's path segment on its host; the lane itself when it is its own Worker. */
  kind: string;
  path: string;
  /** The host panel's section a kind line's document carries under the same name; none for a lane
   * that is its own Worker. */
  section: string | null;
  /** The document fields a kind line's document carries its section under (its content columns
   * without `_json` or a `:type`); none for a lane that is its own Worker. */
  fields: readonly string[] | null;
  documentSchemaVersion: string;
  goldTable: string;
}

/** `path` with `{kind}/` inserted before its last `segment` segment (the Worker's `withKind`). */
export function withKind(path: string, segment: string, kind: string): string {
  const parts = path.split("/");
  const at = parts.lastIndexOf(segment);
  if (at < 0) throw new Error(`${path} has no ${segment} segment to put a kind before`);
  parts.splice(at, 0, kind);
  return parts.join("/");
}

export function resolveWebLane(contract: Record<string, unknown>, name: string): WebLane {
  const entry = (contract.by_pnu_lanes as Record<string, LaneEntry>)[name];
  if (entry === undefined) throw new Error(`the contract names no by-PNU lane ${name}`);
  if (entry.host === undefined) {
    const { request_path, document_schema_version, gold_table } = entry;
    if (!request_path || !document_schema_version || !gold_table) {
      throw new Error(`lane ${name} is neither a kind line nor a Worker of its own`);
    }
    return {
      host: name,
      kind: name,
      path: request_path.prefix,
      section: null,
      fields: null,
      documentSchemaVersion: document_schema_version,
      goldTable: gold_table,
    };
  }
  const host = entry.host.match(/^(.+)_by_pnu_gateway$/)?.[1];
  const block = contract[entry.host] as Gateway | undefined;
  if (
    host === undefined ||
    block === undefined ||
    !entry.kind ||
    !entry.section ||
    !entry.columns?.length
  ) {
    throw new Error(
      `lane ${name} names host ${entry.host} and kind ${entry.kind}, not a kind line`,
    );
  }
  const snake = name.replaceAll("-", "_");
  return {
    host,
    kind: entry.kind,
    path: withKind(block.request_path.prefix, "by-pnu", entry.kind),
    section: entry.section,
    fields: entry.columns.map((column) => column.split(":")[0]?.replace(/_json$/, "") ?? column),
    documentSchemaVersion: `foundation-platform.${snake}_by_pnu.v1`,
    goldTable: `gold.${snake}`,
  };
}

export function renderByPnuLanes(contract: Record<string, unknown>): string {
  const names = Object.keys(contract.by_pnu_lanes as object).sort();
  const lanes = names.map((name) => {
    const lane = resolveWebLane(contract, name);
    return [
      `  ${JSON.stringify(name)}: {`,
      `    host: ${JSON.stringify(lane.host)},`,
      `    kind: ${JSON.stringify(lane.kind)},`,
      `    path: ${JSON.stringify(lane.path)},`,
      `    section: ${JSON.stringify(lane.section)},`,
      `    fields: ${JSON.stringify(lane.fields)?.replaceAll(`","`, `", "`)},`,
      `    documentSchemaVersion: ${JSON.stringify(lane.documentSchemaVersion)},`,
      `    goldTable: ${JSON.stringify(lane.goldTable)},`,
      "  },",
    ].join("\n");
  });
  return `// Generated from platforms/foundation-platform/config/r2-connections.contract.json (by_pnu_lanes).
// Run \`node scripts/generate-by-pnu-lanes.ts\` from products/gongzzang after editing the contract.

/** Every by-PNU lane the web reads: the host serving it, its path there and its document. */
export const BY_PNU_LANES = {
${lanes.join("\n")}
} as const;

export type ByPnuLaneName = keyof typeof BY_PNU_LANES;
export type ByPnuHost = (typeof BY_PNU_LANES)[ByPnuLaneName]["host"];
`;
}

export function readContract(): Record<string, unknown> {
  return JSON.parse(readFileSync(CONTRACT_PATH, "utf8"));
}

if (process.argv[1] !== undefined && fileURLToPath(import.meta.url) === process.argv[1]) {
  writeFileSync(OUTPUT_PATH, renderByPnuLanes(readContract()));
}
