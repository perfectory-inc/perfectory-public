// scripts/generate-by-pnu-lanes.ts
//
// Writes apps/web/lib/api/by-pnu-lanes.generated.ts from the foundation's connection contract: every
// `by_pnu_lanes` entry the web can read, as the path its host serves it under and the document it
// answers with (root ADR-0193 §4). A lane naming a `host` and a `kind` is resolved by the same rule
// the publisher (`resolve_lane_entry`) and the Worker (`lane.ts` `resolveLane`) apply, held to the
// contract's `by_pnu_lane_rule.test_vectors`; a lane without a host is its own Worker (ADR-0190).
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
  gold_table: string;
  document_schema_version?: string;
  request_path?: { prefix: string };
}

export interface WebLane {
  /** `parcel` or `building` for a lane of a shared host; the lane itself when it is its own Worker. */
  host: string;
  path: string;
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
    if (entry.request_path === undefined || entry.document_schema_version === undefined) {
      throw new Error(`lane ${name} has neither a host nor a request path of its own`);
    }
    return {
      host: name,
      path: entry.request_path.prefix,
      documentSchemaVersion: entry.document_schema_version,
      goldTable: entry.gold_table,
    };
  }
  const host = entry.host.match(/^(.+)_by_pnu_gateway$/)?.[1];
  const block = contract[entry.host] as Gateway | undefined;
  if (host === undefined || block === undefined || entry.kind === undefined) {
    throw new Error(`lane ${name} names host ${entry.host} and kind ${entry.kind}, not a gateway`);
  }
  return {
    host,
    path: withKind(block.request_path.prefix, "by-pnu", entry.kind),
    documentSchemaVersion: `foundation-platform.${name.replaceAll("-", "_")}_by_pnu.v1`,
    goldTable: entry.gold_table,
  };
}

export function renderByPnuLanes(contract: Record<string, unknown>): string {
  const names = Object.keys(contract.by_pnu_lanes as object).sort();
  const lanes = names.map((name) => {
    const lane = resolveWebLane(contract, name);
    return [
      `  ${JSON.stringify(name)}: {`,
      `    host: ${JSON.stringify(lane.host)},`,
      `    path: ${JSON.stringify(lane.path)},`,
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
