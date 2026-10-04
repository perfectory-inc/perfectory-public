import connectionContract from "../../../config/r2-connections.contract.json";

/// Section packs (root ADR-0147): one R2 object per (section, legal dong), head + index + body.
/// The byte layout, the resolution order and the join are the publisher's
/// (`foundation-outbox-publisher/src/by_pnu_pack.rs`, `.../section_packs/{read,sections}.rs`);
/// the golden packs in `test/fixtures/section-packs/` hold both sides to the same bytes.

const packPolicy = connectionContract.by_pnu_section_packs;
const lanePacks = connectionContract.building_by_pnu_gateway.section_packs;
const PREFIX_BYTES = 20;
const INDEX_ENTRY_BYTES = 28;
const PNU_BYTES = 19;
const STATE_DOCUMENT = 1;
const STATE_TOMBSTONE = 2;
/// Parsed heads kept per isolate; a head is immutable (packs are create-only), so a cached one is
/// never stale.
const HEAD_MEMORY_ENTRIES = 512;
const headCacheOrigin = "https://foundation-building-gateway.invalid/pack-head/";
const magic = new TextEncoder().encode(packPolicy.magic);

/// The sections the join knows, in the publisher's order. The contract must name exactly these.
export const JOINED_SECTIONS = ["buildings", "floors", "units", "unit_prices"] as const;

export interface PackSection {
  name: string;
  generation: number;
  /// Patches up to this number are already in the generation.
  patchFloor: number;
}

export interface PackPatch {
  patch: number;
  units: ReadonlySet<string>;
}

export interface PackPlan {
  sections: readonly PackSection[];
  /// Newest first.
  patches: readonly PackPatch[];
  unitLength: number;
  fingerprint: string;
}

function isPositive(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1;
}

function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

/// Reads a manifest's `section_packs` block; `null` when it is not one this Worker can trust,
/// which the caller answers as an outage.
export function parseSectionPacks(raw: unknown, patchCeiling: number): PackPlan | null {
  if (typeof raw !== "object" || raw === null) return null;
  const block = raw as Record<string, unknown>;
  if (
    block.schema_version !== packPolicy.manifest_section_packs_schema_version ||
    block.format_version !== packPolicy.format_version
  ) {
    return null;
  }
  const unitLength = block.unit_prefix_length;
  if (!isPositive(unitLength) || unitLength > 19) return null;
  if (!Array.isArray(block.sections) || block.sections.length !== lanePacks.sections.length) return null;
  const sections: PackSection[] = [];
  for (const [index, entry] of (block.sections as unknown[]).entries()) {
    if (typeof entry !== "object" || entry === null) return null;
    const { name, generation, patch_floor: patchFloor } = entry as Record<string, unknown>;
    if (typeof name !== "string" || name !== lanePacks.sections[index] || !isPositive(generation) || !isCount(patchFloor)) {
      return null;
    }
    sections.push({ name, generation, patchFloor });
  }
  if (!Array.isArray(block.patches) || block.patches.length > patchCeiling) return null;
  const unitPattern = new RegExp(`^[0-9]{${unitLength}}$`);
  const patches: PackPatch[] = [];
  for (const entry of block.patches as unknown[]) {
    if (typeof entry !== "object" || entry === null) return null;
    const { patch, units } = entry as Record<string, unknown>;
    if (!isPositive(patch)) return null;
    const previous = patches.at(-1);
    if (previous !== undefined && previous.patch <= patch) return null;
    if (!Array.isArray(units) || units.length === 0) return null;
    if (!units.every((unit) => typeof unit === "string" && unitPattern.test(unit))) return null;
    patches.push({ patch, units: new Set(units as string[]) });
  }
  const newest = Math.max(patches[0]?.patch ?? 0, ...sections.map((section) => section.patchFloor));
  return {
    sections,
    patches,
    unitLength,
    fingerprint: `packs-g${sections.map((section) => section.generation).join(".")}-p${newest}`,
  };
}

/// Every contract section at one unpublished generation with no patches: the preview a cut-over
/// gate probes.
export function previewPlan(generation: number): PackPlan {
  return {
    sections: lanePacks.sections.map((name) => ({ name, generation, patchFloor: 0 })),
    patches: [],
    unitLength: packPolicy.unit_prefix_length,
    fingerprint: `preview-g${generation}`,
  };
}

interface PackHead {
  index: Uint8Array;
  entryCount: number;
  bodyStart: number;
  bodyLength: number;
  etag: string;
}

const headMemory = new Map<string, PackHead>();

function remember(key: string, head: PackHead): void {
  headMemory.delete(key);
  headMemory.set(key, head);
  if (headMemory.size > HEAD_MEMORY_ENTRIES) {
    const oldest = headMemory.keys().next().value;
    if (oldest !== undefined) headMemory.delete(oldest);
  }
}

class PackFormatError extends Error {}

/// Parses the head; `null` when `bytes` stop before it ends (then the caller reads further).
export function parseHead(bytes: Uint8Array, etag: string): PackHead | null {
  if (bytes.length < PREFIX_BYTES) throw new PackFormatError("short pack");
  for (let i = 0; i < magic.length; i += 1) {
    if (bytes[i] !== magic[i]) throw new PackFormatError("not a section pack");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint16(8, true) !== packPolicy.format_version || view.getUint16(10, true) !== 0) {
    throw new PackFormatError("unknown pack format");
  }
  const headerLength = view.getUint32(12, true);
  const indexLength = view.getUint32(16, true);
  if (indexLength % INDEX_ENTRY_BYTES !== 0) throw new PackFormatError("torn index");
  const headLength = PREFIX_BYTES + headerLength + indexLength;
  if (bytes.length < headLength) return null;
  const header = JSON.parse(
    new TextDecoder().decode(bytes.subarray(PREFIX_BYTES, PREFIX_BYTES + headerLength)),
  ) as Record<string, unknown>;
  const entryCount = indexLength / INDEX_ENTRY_BYTES;
  if (header.compression !== "gzip" || header.entry_count !== entryCount || !isCount(header.body_length)) {
    throw new PackFormatError("pack header disagrees with its index");
  }
  return {
    index: bytes.slice(PREFIX_BYTES + headerLength, headLength),
    entryCount,
    bodyStart: headLength,
    bodyLength: header.body_length,
    etag,
  };
}

function headCacheResponse(head: PackHead): Response {
  const prefix = new ArrayBuffer(16);
  const view = new DataView(prefix);
  view.setUint32(0, head.entryCount, true);
  view.setUint32(4, head.bodyStart, true);
  view.setUint32(8, head.bodyLength, true);
  return new Response(new Blob([prefix, head.index]), {
    headers: { "Cache-Control": packPolicy.cache_control, ETag: head.etag },
  });
}

async function headFromCache(key: string): Promise<PackHead | null> {
  const cached = await caches.default.match(`${headCacheOrigin}${key}`);
  if (cached === undefined) return null;
  const bytes = new Uint8Array(await cached.arrayBuffer());
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    entryCount: view.getUint32(0, true),
    bodyStart: view.getUint32(4, true),
    bodyLength: view.getUint32(8, true),
    index: bytes.slice(16),
    etag: cached.headers.get("ETag") ?? "",
  };
}

/// One pack's head: isolate memory, then the edge cache, then one range read of R2 (two when the
/// head is larger than the contract's `head_read_bytes`). `null` when the pack does not exist.
export async function readHead(
  bucket: Pick<R2Bucket, "get">,
  key: string,
  ctx: ExecutionContext,
): Promise<PackHead | null> {
  const remembered = headMemory.get(key);
  if (remembered !== undefined) return remembered;
  const cached = await headFromCache(key);
  if (cached !== null) {
    remember(key, cached);
    return cached;
  }
  const first = await bucket.get(key, { range: { offset: 0, length: packPolicy.head_read_bytes } });
  if (first === null) return null;
  if (!("body" in first)) throw new PackFormatError("pack head read returned no body");
  let bytes = new Uint8Array(await first.arrayBuffer());
  let head = parseHead(bytes, first.etag);
  if (head === null) {
    const headerLength = new DataView(bytes.buffer, bytes.byteOffset).getUint32(12, true);
    const indexLength = new DataView(bytes.buffer, bytes.byteOffset).getUint32(16, true);
    const whole = await bucket.get(key, {
      range: { offset: 0, length: PREFIX_BYTES + headerLength + indexLength },
    });
    if (whole === null || !("body" in whole) || whole.etag !== first.etag) {
      throw new PackFormatError("pack changed between reads");
    }
    bytes = new Uint8Array(await whole.arrayBuffer());
    head = parseHead(bytes, first.etag);
    if (head === null) throw new PackFormatError("pack head is shorter than declared");
  }
  remember(key, head);
  ctx.waitUntil(caches.default.put(`${headCacheOrigin}${key}`, headCacheResponse(head)));
  return head;
}

interface Entry {
  state: typeof STATE_DOCUMENT | typeof STATE_TOMBSTONE;
  offset: number;
  length: number;
}

function comparePnu(index: Uint8Array, at: number, pnu: Uint8Array): number {
  for (let i = 0; i < PNU_BYTES; i += 1) {
    const difference = (index[at + i] ?? 0) - (pnu[i] ?? 0);
    if (difference !== 0) return difference;
  }
  return 0;
}

/// Binary search over the sorted index.
export function findEntry(head: PackHead, pnu: string): Entry | null {
  const target = new TextEncoder().encode(pnu);
  const view = new DataView(head.index.buffer, head.index.byteOffset, head.index.byteLength);
  let low = 0;
  let high = head.entryCount - 1;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    const at = middle * INDEX_ENTRY_BYTES;
    const order = comparePnu(head.index, at, target);
    if (order === 0) {
      const state = head.index[at + 27];
      if (state !== STATE_DOCUMENT && state !== STATE_TOMBSTONE) {
        throw new PackFormatError("unknown entry state");
      }
      const entry = { state, offset: view.getUint32(at + 19, true), length: view.getUint32(at + 23, true) } as Entry;
      if (entry.offset + entry.length > head.bodyLength) throw new PackFormatError("entry past the body");
      return entry;
    }
    if (order < 0) low = middle + 1;
    else high = middle - 1;
  }
  return null;
}

/// One document's gzip member, by one range read, decoded.
async function readDocument(
  bucket: Pick<R2Bucket, "get">,
  key: string,
  head: PackHead,
  entry: Entry,
): Promise<unknown> {
  const object = await bucket.get(key, { range: { offset: head.bodyStart + entry.offset, length: entry.length } });
  if (object === null || !("body" in object) || (head.etag !== "" && object.etag !== head.etag)) {
    // Pack keys are create-only: never combine a cached head with other bytes.
    throw new PackFormatError("pack changed or vanished under its cached head");
  }
  const decoded = new Response(
    new Blob([await object.arrayBuffer()]).stream().pipeThrough(new DecompressionStream("gzip")),
  );
  return JSON.parse(await decoded.text()) as unknown;
}

export type Fragment =
  | { kind: "document"; value: unknown }
  | { kind: "tombstone" }
  | { kind: "absent" };

function packKey(section: PackSection, patch: number | null, unit: string): string {
  const patchDir = patch === null ? "" : `p${patch}/`;
  return `${lanePacks.root}/${section.name}/g${section.generation}/${patchDir}${unit}${packPolicy.suffix}`;
}

/// One section's fragment of `pnu`: the newest patch naming its dong and holding it, else the
/// base. A patch the manifest lists but R2 lacks is an outage, not an absence.
async function findFragment(
  bucket: Pick<R2Bucket, "get">,
  ctx: ExecutionContext,
  plan: PackPlan,
  section: PackSection,
  pnu: string,
): Promise<Fragment> {
  const unit = pnu.slice(0, plan.unitLength);
  for (const patch of plan.patches) {
    if (patch.patch <= section.patchFloor || !patch.units.has(unit)) continue;
    const key = packKey(section, patch.patch, unit);
    const head = await readHead(bucket, key, ctx);
    if (head === null) throw new PackFormatError(`listed patch pack ${key} is missing`);
    const entry = findEntry(head, pnu);
    if (entry === null) continue;
    if (entry.state === STATE_TOMBSTONE) return { kind: "tombstone" };
    return { kind: "document", value: await readDocument(bucket, key, head, entry) };
  }
  const key = packKey(section, null, unit);
  const head = await readHead(bucket, key, ctx);
  if (head === null) return { kind: "absent" };
  const entry = findEntry(head, pnu);
  if (entry === null) return { kind: "absent" };
  if (entry.state === STATE_TOMBSTONE) throw new PackFormatError("tombstone in a base pack");
  return { kind: "document", value: await readDocument(bucket, key, head, entry) };
}

export class InconsistentSections extends Error {}

type Json = Record<string, unknown>;

function asObject(value: unknown): Json {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InconsistentSections("a fragment is not an object");
  }
  return value as Json;
}

function asArray(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new InconsistentSections("a fragment is not an array");
  return value;
}

/// The publisher's join (`sections.rs`): fills the anchor's empty places, refusing ids that are
/// not exactly the anchor's sequence.
export function joinFragments(fragments: readonly unknown[]): Json {
  const [anchorRaw, floorsRaw, unitsRaw, pricesRaw] = fragments;
  const anchor = asObject(anchorRaw);
  const buildings = asArray(anchor.buildings).map(asObject);
  const floors = asArray(floorsRaw).map(asObject);
  const units = asObject(unitsRaw);
  const unitBuildings = asArray(units.buildings).map(asObject);
  const prices = asArray(pricesRaw).map(asObject);
  if (
    floors.length !== buildings.length ||
    unitBuildings.length !== buildings.length ||
    asArray(anchor.unlinked_units).length !== 0
  ) {
    throw new InconsistentSections("sections name other buildings");
  }
  buildings.forEach((building, index) => {
    const floorsOf = floors[index];
    const unitsOf = unitBuildings[index];
    if (
      floorsOf === undefined ||
      unitsOf === undefined ||
      floorsOf.building_id !== building.id ||
      unitsOf.building_id !== building.id ||
      asArray(building.floors).length !== 0 ||
      asArray(building.units).length !== 0
    ) {
      throw new InconsistentSections("sections name other buildings");
    }
    building.floors = asArray(floorsOf.floors);
    building.units = asArray(unitsOf.units);
  });
  anchor.unlinked_units = asArray(units.unlinked_units);
  const allUnits = [
    ...buildings.flatMap((building) => asArray(building.units).map(asObject)),
    ...asArray(anchor.unlinked_units).map(asObject),
  ];
  if (allUnits.length !== prices.length) throw new InconsistentSections("unit prices disagree");
  allUnits.forEach((unit, index) => {
    const price = prices[index];
    if (price === undefined || price.unit_id !== unit.id || asArray(unit.official_price_history).length !== 0) {
      throw new InconsistentSections("unit prices disagree");
    }
    unit.official_price_history = asArray(price.official_price_history);
  });
  return anchor;
}

export type Resolved = { kind: "document"; document: Json } | { kind: "tombstone" } | { kind: "absent" };

/// The PNU's answer: every section read at once, the anchor deciding, the rest agreeing.
export async function resolvePacks(
  bucket: Pick<R2Bucket, "get">,
  ctx: ExecutionContext,
  plan: PackPlan,
  pnu: string,
): Promise<Resolved> {
  const found = await Promise.all(plan.sections.map((section) => findFragment(bucket, ctx, plan, section, pnu)));
  const anchorIndex = plan.sections.findIndex((section) => section.name === lanePacks.anchor_section);
  const anchor = found[anchorIndex];
  if (anchor === undefined) throw new InconsistentSections("no anchor section");
  if (anchor.kind !== "document") {
    if (found.some((fragment) => fragment.kind === "document")) {
      throw new InconsistentSections("a section answers where the anchor does not");
    }
    return anchor;
  }
  const values = found.map((fragment) => {
    if (fragment.kind !== "document") throw new InconsistentSections("a section is missing");
    return fragment.value;
  });
  return { kind: "document", document: joinFragments(values) };
}
