// apps/web/lib/api/unit-floor.ts
//
// A unit's floor as the screen shows it, composed from the values the data gives: the source's
// floor kind and the number within it (root ADR-0193 §9). The data side does not format floors for
// a screen, and the web does not read floors back out of formatted text — no regex over labels.
// A bundle document (v2/v3) still carries only the server's `floor_label`; it is drawn as given,
// in the order the document lists the units, until the bundle retires.
import type { BuildingUnit } from "./building-units";

/** The source floor kinds a unit's floor can be, as the normalization's wire names give them. */
export const UNIT_FLOOR_KINDS = [
  "above_ground",
  "basement",
  "rooftop",
  "all_floors",
  "multi_floor_lower",
  "multi_floor_upper",
] as const;
export type UnitFloorKind = (typeof UNIT_FLOOR_KINDS)[number];

/** The `panels.parcel.floors` messages a floor is written with. */
export type FloorMessages = (
  key: `unitFloor.${UnitFloorKind}`,
  values: { number: number },
) => string;

type UnitFloor = Pick<BuildingUnit, "floor_kind" | "floor_number" | "floor_label">;

const isKind = (kind: string | null | undefined): kind is UnitFloorKind =>
  (UNIT_FLOOR_KINDS as readonly (string | null | undefined)[]).includes(kind);
const NUMBERED: ReadonlySet<UnitFloorKind> = new Set([
  "above_ground",
  "basement",
  "multi_floor_lower",
  "multi_floor_upper",
]);

/** The floor's text, or null when the data does not say which floor (shown as "unknown"). */
export function unitFloorText(unit: UnitFloor, t: FloorMessages): string | null {
  const { floor_kind: kind, floor_number: number } = unit;
  if (kind == null) return unit.floor_label?.trim() || null;
  if (!isKind(kind) || (NUMBERED.has(kind) && number == null)) return null;
  return t(`unitFloor.${kind}`, { number: number ?? 0 });
}

/**
 * Where the floor stands, bottom to top, for ordering: basements below ground, the rooftop above
 * every numbered floor, and a floor the data does not place last. Units without a floor kind (a
 * bundle document) all rank the same, so a stable sort keeps the document's own order.
 */
export function unitFloorRank(unit: UnitFloor): number {
  const { floor_kind: kind, floor_number: number } = unit;
  if (kind === "rooftop") return 1_000_000;
  if (kind === "all_floors") return -1_000_000;
  if (!isKind(kind) || number == null) return Number.POSITIVE_INFINITY;
  return kind === "basement" ? -number : number;
}

const hoCollator = new Intl.Collator("ko", { numeric: true });
type Ho = Pick<BuildingUnit, "ho_name" | "ho_number">;
/**
 * Units in number order: by the data's `ho_number` when both have one, else by the names as read
 * (101호 before 1001호) — a name is compared as given, never parsed.
 */
export const compareHoName = (a: Ho, b: Ho) =>
  a.ho_number != null && b.ho_number != null && a.ho_number !== b.ho_number
    ? a.ho_number - b.ho_number
    : hoCollator.compare(a.ho_name, b.ho_name);
