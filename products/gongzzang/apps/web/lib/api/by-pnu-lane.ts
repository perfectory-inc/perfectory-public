// apps/web/lib/api/by-pnu-lane.ts
//
// Each data kind is its own by-PNU document on its own lane (root ADR-0193): one generic read per
// lane. The lanes, their hosts, paths and document sections come from the generated registry, never
// from a hand list: a view names the sections it draws, the registry names the lanes serving them.
import { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { BY_PNU_LANES, type ByPnuHost, type ByPnuLaneName } from "./by-pnu-lanes.generated";

/** A lane's answer for one PNU: its document, or the lane's word that it has none (404). */
export type LaneDocument =
  | { status: "found"; document: Readonly<Record<string, unknown>> }
  | { status: "absent" };

/** The edge base URL of each host. A lane on a host missing here fails the typecheck. */
function hostBaseUrl(host: ByPnuHost): string {
  const bases: Record<string, string> & Record<ByPnuHost, string> = {
    parcel: env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL,
    building: env.NEXT_PUBLIC_BUILDING_EDGE_BASE_URL,
    "unit-price": env.NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL,
  };
  return bases[host].replace(/\/$/, "");
}

/** The lane of `host` whose document carries each section, if the contract names one. */
export function lanesForSections<S extends string>(
  host: ByPnuHost,
  sections: readonly S[],
): Partial<Record<S, ByPnuLaneName>> {
  const lanes: Partial<Record<S, ByPnuLaneName>> = {};
  for (const [lane, entry] of Object.entries(BY_PNU_LANES) as [
    ByPnuLaneName,
    (typeof BY_PNU_LANES)[ByPnuLaneName],
  ][]) {
    if (entry.host !== host) continue;
    for (const section of entry.sections) {
      if ((sections as readonly string[]).includes(section)) lanes[section as S] = lane;
    }
  }
  return lanes;
}

export function laneUrl(lane: ByPnuLaneName, pnu: string): string {
  const { host, path } = BY_PNU_LANES[lane];
  return `${hostBaseUrl(host)}${path}${pnu}`;
}

/** Every lane document is `{schema_version, pnu, source: {table, iceberg_snapshot_id}, <body>}`. */
function envelopeSchema(lane: ByPnuLaneName) {
  const { documentSchemaVersion, goldTable } = BY_PNU_LANES[lane];
  return z.looseObject({
    schema_version: z.literal(documentSchemaVersion),
    pnu: PnuSchema,
    source: z.object({ table: z.literal(goldTable), iceberg_snapshot_id: z.string().min(1) }),
  });
}

export async function fetchLaneDocument(
  lane: ByPnuLaneName,
  pnu: string,
  signal?: AbortSignal,
): Promise<LaneDocument> {
  PnuSchema.parse(pnu);
  const response = await fetch(laneUrl(lane, pnu), {
    signal,
    headers: { accept: "application/json" },
  });
  // The lane's pack index proves the PNU has no document of this kind: "none", not a failure.
  if (response.status === 404) return { status: "absent" };
  // 503 is a lane whose first generation is not published yet (or an outage): a failure.
  if (!response.ok) throw new Error(`${lane} edge fetch failed: ${response.status}`);
  const document = envelopeSchema(lane).parse(await response.json());
  if (document.pnu !== pnu) throw new Error(`${lane} edge response PNU disagrees with request`);
  return { status: "found", document };
}

/** What a view draws for one kind of one PNU. */
export type KindState<T> =
  | { state: "loading" }
  | { state: "ok"; value: T }
  | { state: "empty" }
  | { state: "error" };

const isEmpty = (value: unknown) =>
  value === null || value === undefined || (Array.isArray(value) && value.length === 0);

/**
 * One kind's section of one PNU. Its lane's document wins when the lane serves one. While the
 * bundle document still carries the section (migration, root ADR-0193 §6-4) the bundle answers
 * for everything else: a lane not yet published (503), not yet routed by the deployed Worker
 * (404), slow or failing. Without a bundle, the lane's own answer decides.
 */
export function kindSection(
  lane: { data?: LaneDocument; isError: boolean } | undefined,
  section: string,
  bundle: Readonly<Record<string, unknown>> | undefined,
): KindState<unknown> {
  const settled = (value: unknown): KindState<unknown> =>
    isEmpty(value) ? { state: "empty" } : { state: "ok", value };
  if (lane?.data?.status === "found") return settled(lane.data.document[section]);
  if (bundle !== undefined) return settled(bundle[section]);
  if (lane?.data?.status === "absent") return { state: "empty" };
  // No bundle and no lane for this kind is a registry gap, not something to wait for.
  return lane === undefined || lane.isError ? { state: "error" } : { state: "loading" };
}
