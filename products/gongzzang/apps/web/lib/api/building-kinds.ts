// apps/web/lib/api/building-kinds.ts
//
// A parcel's buildings are three data kinds, each its own by-PNU lane (root ADR-0193): the titles
// (each building's own fields), each building's floors, and each building's units with the units no
// building carries. The building views read the three lanes and the bundle document side by side,
// each once, and join them by building id into the shape the views already take. A lane's document
// wins; while the bundle still carries the kind (migration, ADR-0193 §6-4) it answers for a lane
// that has no document yet (503 before its first publish, 404 before the Worker routes it, a
// failure or an unparsable body), so nothing is retried and nothing waits on an unpublished lane.
import { keyBy } from "es-toolkit";
import { z } from "zod";
import {
  BuildingNotServedError,
  type EdgeBuildingProfile,
  EdgeFloorSchema,
  EdgeUnitSchema,
  fetchBuildingBundle,
} from "./building-edge";
import { fetchLaneDocument, type LaneDocument, lanesForSections } from "./by-pnu-lane";
import { BY_PNU_LANES } from "./by-pnu-lanes.generated";

/** The building panel's sections, by the names the contract's kind lines give them. */
const SECTIONS = ["titles", "floors", "units"] as const;
type Section = (typeof SECTIONS)[number];

const TitleSchema = z.object({
  id: z.string().uuid(),
  parcel_id: z.string().uuid(),
  register_pk: z.string(),
  purpose_code: z.string().nullable(),
  structure_code: z.string().nullable(),
  floor_area_m2: z.number().positive().nullable(),
  stories: z.number().int().nonnegative().nullable(),
  below_ground_floors: z.number().int().nonnegative(),
  has_rooftop: z.boolean(),
  rooftop_area_m2: z.number().nonnegative().nullish(),
  // The source's values as the bundle carries them (building-edge.ts).
  rooftop_usage: z.union([z.array(z.string()), z.string()]),
  built_year: z.number().int().nullable(),
});
const PerBuilding = <T extends z.ZodTypeAny>(field: string, item: T) =>
  z.array(z.object({ id: z.string().uuid(), [field]: z.array(item) }));
/** Each section's document fields, as the registry names them, and how they are read. */
const BODIES: Record<Section, (fields: readonly string[]) => z.ZodTypeAny> = {
  titles: ([buildings = "buildings"]) => z.object({ [buildings]: z.array(TitleSchema) }),
  floors: ([floors = "floors"]) => z.object({ [floors]: PerBuilding("floors", EdgeFloorSchema) }),
  units: ([units = "units", unlinked = "unlinked_units"]) =>
    z.object({
      [units]: PerBuilding("units", EdgeUnitSchema),
      [unlinked]: z.array(EdgeUnitSchema),
    }),
};

/** The fields of a lane's document parsed as `section`, or undefined when the lane has none to give. */
function laneBody(section: Section, lane: string | undefined, answer: LaneDocument | undefined) {
  if (lane === undefined || answer?.status !== "found") return undefined;
  const fields = BY_PNU_LANES[lane as keyof typeof BY_PNU_LANES].fields ?? [];
  const parsed = BODIES[section](fields).safeParse(answer.document);
  return parsed.success
    ? fields.map((field) => (parsed.data as Record<string, unknown>)[field])
    : undefined;
}

type Title = z.infer<typeof TitleSchema>;
type Floor = z.infer<typeof EdgeFloorSchema>;
type Unit = z.infer<typeof EdgeUnitSchema>;
export type BuildingProfile = Pick<EdgeBuildingProfile, "buildings" | "unlinked_units">;

/**
 * The parcel's buildings from the three kinds and the bundle: each kind from its lane when the lane
 * has a document, else from the bundle. `null` when neither the bundle nor any lane knows the parcel.
 */
export function joinBuildingKinds(
  bundle: EdgeBuildingProfile | null,
  lanes: Partial<Record<Section, unknown[]>>,
  absent: ReadonlySet<Section>,
): BuildingProfile | null {
  const titles: Title[] | undefined =
    (lanes.titles?.[0] as Title[] | undefined) ?? bundle?.buildings;
  const floorsById = keyBy(
    (lanes.floors?.[0] as { id: string; floors: Floor[] }[] | undefined) ??
      (bundle?.buildings ?? []).map(({ id, floors }) => ({ id, floors })),
    (building) => building.id,
  );
  const unitsById = keyBy(
    (lanes.units?.[0] as { id: string; units: Unit[] }[] | undefined) ??
      (bundle?.buildings ?? []).map(({ id, units }) => ({ id, units })),
    (building) => building.id,
  );
  const unlinked = (lanes.units?.[1] as Unit[] | undefined) ?? bundle?.unlinked_units;
  // Not served: no bundle document and every lane answered "none".
  if (bundle === null && SECTIONS.every((section) => absent.has(section))) return null;
  return {
    buildings: (titles ?? []).map((title) => ({
      ...title,
      floors: floorsById[title.id]?.floors ?? [],
      units: unitsById[title.id]?.units ?? [],
    })),
    unlinked_units: unlinked ?? [],
  };
}

/** The parcel's buildings for the building views: the bundle and the three lanes in one round. */
export async function fetchBuildingProfile(
  pnu: string,
  signal?: AbortSignal,
): Promise<BuildingProfile> {
  const lanes = lanesForSections("building", SECTIONS);
  const [bundle, ...answers] = await Promise.all([
    fetchBuildingBundle(pnu, signal).catch((error: unknown) =>
      error instanceof BuildingNotServedError
        ? null
        : error instanceof Error
          ? error
          : new Error(String(error)),
    ),
    ...SECTIONS.map((section) => {
      const lane = lanes[section];
      return lane === undefined
        ? undefined
        : fetchLaneDocument(lane, pnu, signal).catch(() => undefined);
    }),
  ]);
  const bodies: Partial<Record<Section, unknown[]>> = {};
  const absent = new Set<Section>();
  SECTIONS.forEach((section, index) => {
    const answer = answers[index];
    if (answer?.status === "absent") absent.add(section);
    const body = laneBody(section, lanes[section], answer);
    if (body !== undefined) bodies[section] = body;
  });
  // A bundle that failed (not "not served") is an outage unless every kind's lane answered.
  if (bundle instanceof Error) {
    if (SECTIONS.some((section) => bodies[section] === undefined && !absent.has(section)))
      throw bundle;
  }
  const joined = joinBuildingKinds(bundle instanceof Error ? null : bundle, bodies, absent);
  if (joined === null) throw new BuildingNotServedError(pnu);
  return joined;
}
