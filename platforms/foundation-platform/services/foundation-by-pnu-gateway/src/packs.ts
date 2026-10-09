import connectionContract from "../../../config/r2-connections.contract.json";
import { cacheOrigin, lanePacks, UNIT } from "./lane";

/// Section packs (root ADR-0147, ADR-0151): one R2 object per (section, legal dong), head + index +
/// body, or one per part of a large dong (ADR-0163). Each lane has one section, `documents`, whose
/// entry for a PNU is its served document as one gzip member; the Worker answers with that member as it is
/// (`Content-Encoding: gzip`), without decompressing or parsing it. The byte layout and the
/// resolution order are the publisher's (`foundation-outbox-publisher/src/by_pnu_pack.rs`,
/// `.../section_packs/{read,sections}.rs`); the golden packs in `test/fixtures/section-packs/` hold
/// both sides to the same bytes.

const packPolicy = connectionContract.by_pnu_section_packs;
const PREFIX_BYTES = 20;
const INDEX_ENTRY_BYTES = 28;
const PNU_BYTES = 19;
const STATE_DOCUMENT = 1;
const STATE_TOMBSTONE = 2;
/// The read path's bounds (root ADR-0147 Revision): which packs are read whole, how much an
/// isolate keeps, and how R2 is retried.
const readPolicy = packPolicy.read_path;
const magic = new TextEncoder().encode(packPolicy.magic);
/// Parts (root ADR-0163): a large dong is cut into packs of `{dong}-{part}` by `fnv1a32`, and its
/// part count is read from the section generation's one parts index.
const partsPolicy = packPolicy.parts;
const partedUnitPattern = new RegExp(`^(?:${partsPolicy.unit_pattern})$`);
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

/// The parts index a section generation is read under: the one the manifest names by key and
/// sha256, or, for a preview (which has no manifest block), the index at its conventional key
/// when the bake wrote one.
export type PackParts =
  | { kind: "named"; key: string; sha256: string; partedUnits: number }
  | { kind: "conventional"; key: string };

export interface PackSection {
  name: string;
  generation: number;
  /// Patches up to this number are already in the generation.
  patchFloor: number;
  /// Absent for a section whose every dong is one pack.
  parts?: PackParts;
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

/// The conventional key of a section generation's parts index (root ADR-0163):
/// `{root}/{section}/g{n}/parts.json`.
export function partsKey(section: Pick<PackSection, "name" | "generation">): string {
  return `${lanePacks.root}/${section.name}/g${section.generation}/${partsPolicy.index_file_name}`;
}

/// A section entry's `parts`: the index's key (which must be the generation's conventional one),
/// the sha256 of its bytes and the number of dongs it names; `null` when it is malformed.
function parseParts(raw: unknown, section: Pick<PackSection, "name" | "generation">): PackParts | null {
  if (typeof raw !== "object" || raw === null) return null;
  const { key, sha256, parted_units: partedUnits } = raw as Record<string, unknown>;
  if (key !== partsKey(section) || typeof sha256 !== "string" || !SHA256_PATTERN.test(sha256) || !isCount(partedUnits)) {
    return null;
  }
  return { kind: "named", key, sha256, partedUnits };
}

/// Reads a manifest's `section_packs` block; `null` when it is not one this Worker can trust,
/// which the caller answers as an outage. Version 3 names no parts; version 4 (root ADR-0163) is
/// the block whose sections name a parts index, and one that names none is not a version 4 block.
export function parseSectionPacks(raw: unknown, patchCeiling: number): PackPlan | null {
  if (typeof raw !== "object" || raw === null) return null;
  const block = raw as Record<string, unknown>;
  const parted = block.schema_version === packPolicy.manifest_section_packs_parted_schema_version;
  if (
    (!parted && block.schema_version !== packPolicy.manifest_section_packs_schema_version) ||
    block.format_version !== packPolicy.format_version
  ) {
    return null;
  }
  const unitLength = block.unit_prefix_length;
  if (!isPositive(unitLength) || unitLength > 19) return null;
  // A part unit is a dong of the contract's unit length and a part (`parts.unit_pattern`).
  if (parted && unitLength !== packPolicy.unit_prefix_length) return null;
  if (!Array.isArray(block.sections) || block.sections.length !== lanePacks.sections.length) return null;
  const sections: PackSection[] = [];
  for (const [index, entry] of (block.sections as unknown[]).entries()) {
    if (typeof entry !== "object" || entry === null) return null;
    const { name, generation, patch_floor: patchFloor, parts: rawParts } = entry as Record<string, unknown>;
    if (typeof name !== "string" || name !== lanePacks.sections[index] || !isPositive(generation) || !isCount(patchFloor)) {
      return null;
    }
    if (rawParts === undefined) {
      sections.push({ name, generation, patchFloor });
      continue;
    }
    if (!parted) return null;
    const parts = parseParts(rawParts, { name, generation });
    if (parts === null) return null;
    sections.push({ name, generation, patchFloor, parts });
  }
  if (parted && sections.every((section) => section.parts === undefined)) return null;
  if (!Array.isArray(block.patches) || block.patches.length > patchCeiling) return null;
  const unitPattern = parted ? partedUnitPattern : new RegExp(`^[0-9]{${unitLength}}$`);
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
/// gate probes. The fingerprint names the preview's Worker version, so a fresh upload answers its
/// first reads from R2 instead of from edge copies an earlier probe left (2026-10-07: 9,952 of
/// 10,000 cold reads of gate (b) were such copies; contract `cold_reads_not_from_r2_max_share`).
/// A preview has no manifest block to name a parts index, so each section reads the one at the
/// generation's conventional key, and a generation without one is unparted (root ADR-0163).
export function previewPlan(generation: number, version: string | null): PackPlan {
  return {
    sections: lanePacks.sections.map((name) => ({
      name,
      generation,
      patchFloor: 0,
      parts: { kind: "conventional", key: partsKey({ name, generation }) },
    })),
    patches: [],
    unitLength: packPolicy.unit_prefix_length,
    fingerprint: version === null ? `preview-g${generation}` : `preview-g${generation}-${version}`,
  };
}

export interface PackHead {
  index: Uint8Array;
  entryCount: number;
  bodyStart: number;
  bodyLength: number;
  /// The pack's R2 `etag`, unquoted: what every later read of the same key must return.
  etag: string;
}

/// What one pack read leaves behind: its head, and the whole pack when it is small enough to read
/// in the same GET (the contract's `read_path.whole_pack_max_bytes`).
export interface PackCopy {
  head: PackHead;
  /// The whole pack, or `null` when only the head was read (a large pack: documents by range).
  bytes: Uint8Array | null;
}

/// One request's reads: what the read path did, in the words `Server-Timing` and the structured
/// logs use. Wall time in a Worker advances only across I/O, so the milliseconds here are time
/// spent waiting on R2.
export class ReadTrace {
  r2Gets = 0;
  retries = 0;
  /// Per section: `{memory|r2}-{whole|head|absent}`, and `+range` when a document needed a second
  /// R2 read; `edge-answer` when the PNU's answer came from its edge copy.
  readonly sections = new Map<string, string>();
  /// Per section: milliseconds waiting on R2, every attempt included.
  readonly sectionR2Ms = new Map<string, number>();
  readonly failures: string[] = [];
  /// The parts index reads (root ADR-0163), apart from the pack reads above: an isolate reads a
  /// section's index once, so they are not part of what a first read of a PNU costs.
  partsGets = 0;
  /// Per section: where its parts index came from, `r2`, `memory` or `absent` (a preview's
  /// generation without one).
  readonly parts = new Map<string, string>();
  /// Per section: milliseconds waiting on R2 for its parts index.
  readonly partsR2Ms = new Map<string, number>();

  note(section: string, label: string): void {
    const previous = this.sections.get(section);
    this.sections.set(section, previous === undefined ? label : `${previous}+${label}`);
  }

  waited(section: string, milliseconds: number): void {
    this.sectionR2Ms.set(section, (this.sectionR2Ms.get(section) ?? 0) + milliseconds);
  }

  /// The R2 wait on the request's critical path: sections are read at once, so the longest.
  get r2Ms(): number {
    return Math.max(0, ...this.sectionR2Ms.values());
  }
}

/// The policy a read runs under and what it may wait on. `sleep` and `random` are the clock and
/// the jitter, injectable so a test can plant transient errors without real delays.
export interface PackReads {
  bucket: Pick<R2Bucket, "get">;
  ctx: ExecutionContext;
  trace: ReadTrace;
  /// `Date.now()` past which no further R2 attempt starts.
  deadline: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

/// Starts the reads of one request under the contract's R2 deadline.
export function packReads(bucket: Pick<R2Bucket, "get">, ctx: ExecutionContext): PackReads {
  return { bucket, ctx, trace: new ReadTrace(), deadline: Date.now() + readPolicy.r2_deadline_ms };
}

/// Packs this isolate holds, least recently used first, within a byte budget: a pack key is
/// create-only, so a copy is never stale (Haystack: the metadata a read needs stays in memory).
const packMemory = new Map<string, PackCopy>();
let packMemoryBytes = 0;

function copyBytes(copy: PackCopy): number {
  return copy.head.index.byteLength + (copy.bytes?.byteLength ?? 0);
}

/// Forgets every pack and parts index this isolate remembers (tests, to stand for a new isolate).
export function forgetPacks(): void {
  packMemory.clear();
  packMemoryBytes = 0;
  partsMemory.clear();
}

function remember(key: string, copy: PackCopy): void {
  const previous = packMemory.get(key);
  if (previous !== undefined) {
    packMemory.delete(key);
    packMemoryBytes -= copyBytes(previous);
  }
  const size = copyBytes(copy);
  if (size > readPolicy.memory_budget_bytes) return;
  packMemory.set(key, copy);
  packMemoryBytes += size;
  for (const [oldest, held] of packMemory) {
    if (packMemoryBytes <= readPolicy.memory_budget_bytes) break;
    packMemory.delete(oldest);
    packMemoryBytes -= copyBytes(held);
  }
}

function recall(key: string): PackCopy | undefined {
  const copy = packMemory.get(key);
  if (copy !== undefined) {
    packMemory.delete(key);
    packMemory.set(key, copy);
  }
  return copy;
}

/// The pack is not what its key and the contract say: never retried, always an outage.
export class PackFormatError extends Error {}

/// R2 did not answer within the read policy (attempts or deadline spent): the only 503 that is
/// not an inconsistency, and only after the bounded retries.
export class PackReadUnavailable extends Error {}

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
    // A view, not a copy: the caller decides which bytes behind it are kept.
    index: bytes.subarray(PREFIX_BYTES + headerLength, headLength),
    entryCount,
    bodyStart: headLength,
    bodyLength: header.body_length,
    etag,
  };
}

/// A whole pack's bytes, checked: the head parses and the body it declares ends at the last byte.
function wholePack(bytes: Uint8Array, etag: string): PackCopy {
  const head = parseHead(bytes, etag);
  if (head === null || head.bodyStart + head.bodyLength !== bytes.byteLength) {
    throw new PackFormatError("pack length disagrees with its header");
  }
  return { head, bytes };
}

/// The length of a pack's head (prefix, header JSON and index) from its first 20 bytes.
function headLength(prefix: Uint8Array): number {
  for (let i = 0; i < magic.length; i += 1) {
    if (prefix[i] !== magic[i]) throw new PackFormatError("not a section pack");
  }
  const view = new DataView(prefix.buffer, prefix.byteOffset, prefix.byteLength);
  return PREFIX_BYTES + view.getUint32(12, true) + view.getUint32(16, true);
}

function joined(chunks: readonly Uint8Array[], length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  let at = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, at);
    at += chunk.byteLength;
  }
  return bytes;
}

class AttemptTimeout extends Error {}

/// Why an R2 attempt failed, in the words the logs use: `timeout`, or R2's five-digit error code.
function failureClass(error: unknown): string {
  if (error instanceof AttemptTimeout) return "timeout";
  const message = error instanceof Error ? error.message : String(error);
  const code = message.match(/\((\d{5})\)/)?.[1];
  return code === undefined ? `r2:${message.slice(0, 80)}` : `r2:${code}`;
}

function within<T>(work: Promise<T>, milliseconds: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new AttemptTimeout("R2 attempt ran past the read deadline")), milliseconds);
  });
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

/// One R2 read with its body, retried while it fails transiently: at most `r2_attempts` attempts,
/// each bounded by `r2_attempt_timeout_ms` and by what is left of the request's deadline, with
/// full-jitter backoff between them (AWS Architecture Blog, "Exponential Backoff And Jitter"). An
/// attempt that stalls is abandoned at its own bound and asked again, so one stalled read cannot
/// spend the whole deadline (the Tail at Scale: a retry of a slow request usually lands on a fast
/// path). A pack that is not what it should be (`PackFormatError`) is never retried: retrying cannot
/// make it right.
async function withRetry<T>(
  reads: PackReads,
  what: { section: string; key: string; phase: "pack" | "range" | "parts" },
  attempt: () => Promise<T>,
): Promise<T> {
  const sleep = reads.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const random = reads.random ?? Math.random;
  const parts = what.phase === "parts";
  let lastClass = "deadline";
  for (let tried = 0; tried < readPolicy.r2_attempts; tried += 1) {
    const left = reads.deadline - Date.now();
    if (left <= 0) break;
    const started = Date.now();
    if (parts) reads.trace.partsGets += 1;
    else reads.trace.r2Gets += 1;
    try {
      return await within(attempt(), Math.min(left, readPolicy.r2_attempt_timeout_ms));
    } catch (error) {
      if (error instanceof PackFormatError) throw error;
      lastClass = failureClass(error);
      reads.trace.failures.push(`${what.section}:${what.phase}:${lastClass}`);
      console.log(
        JSON.stringify({
          event: "pack_read_retry",
          section: what.section,
          key: what.key,
          phase: what.phase,
          attempt: tried + 1,
          error_class: lastClass,
          elapsed_ms: Date.now() - started,
        }),
      );
    } finally {
      const waited = Date.now() - started;
      if (parts) {
        reads.trace.partsR2Ms.set(what.section, (reads.trace.partsR2Ms.get(what.section) ?? 0) + waited);
      } else {
        reads.trace.waited(what.section, waited);
      }
    }
    if (tried + 1 < readPolicy.r2_attempts) {
      const backoff = random() * readPolicy.r2_retry_base_ms * 2 ** tried;
      if (Date.now() + backoff >= reads.deadline) break;
      reads.trace.retries += 1;
      await sleep(backoff);
    }
  }
  throw new PackReadUnavailable(`${what.section} ${what.phase}: R2 unavailable (${lastClass})`);
}

/// One pack: isolate memory, then one GET of R2. The GET names no range, so a pack shorter than
/// any fixed first read is never asked past its end (no reliance on R2 clamping a range). A pack
/// within `whole_pack_max_bytes` is read whole by that GET, so its head and its documents come from
/// one R2 hop; a larger one is read only as far as its head reaches and the rest of its body is
/// cancelled unread. `null` when the pack does not exist.
///
/// No pack goes to the edge cache (ADR-0154): with the Worker placed beside the bucket an R2 read
/// of a pack read before costs about what an edge lookup does (2026-10-06, Tokyo: 35-45 ms), while
/// writing the copy cost more CPU than the rest of the read (cold CPU p99 8.2 ms with it, 5.7 ms
/// without). What a reader reads again is its own PNU, and that is the answer's edge copy.
export async function readPack(reads: PackReads, section: string, key: string): Promise<PackCopy | null> {
  const remembered = recall(key);
  if (remembered !== undefined) {
    reads.trace.note(section, `memory-${remembered.bytes === null ? "head" : "whole"}`);
    return remembered;
  }
  const copy = await withRetry(reads, { section, key, phase: "pack" }, () => fetchPack(reads.bucket, key));
  if (copy === null) {
    reads.trace.note(section, "r2-absent");
    return null;
  }
  remember(key, copy);
  reads.trace.note(section, `r2-${copy.bytes === null ? "head" : "whole"}`);
  return copy;
}

async function fetchPack(bucket: Pick<R2Bucket, "get">, key: string): Promise<PackCopy | null> {
  const object = await bucket.get(key);
  if (object === null) return null;
  if (!("body" in object)) throw new PackFormatError("pack read returned no body");
  if (object.size <= readPolicy.whole_pack_max_bytes) {
    return wholePack(new Uint8Array(await object.arrayBuffer()), object.etag);
  }
  const reader = object.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  let wanted: number | null = null;
  try {
    while (wanted === null || length < wanted) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      length += value.byteLength;
      if (wanted === null && length >= PREFIX_BYTES) wanted = headLength(joined(chunks, length));
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
  if (wanted === null || length < wanted) throw new PackFormatError("pack head is shorter than declared");
  // A copy of the head alone: the body streamed past it is not kept.
  const head = parseHead(joined(chunks, length).slice(0, wanted), object.etag);
  if (head === null) throw new PackFormatError("pack head is shorter than declared");
  return { head, bytes: null };
}

/// FNV-1a, 32 bit, over the ASCII bytes of `text` (the contract's `parts.hash_definition`): the
/// hash that places a PNU in its dong's part. The publisher's twin is `r2_layout::by_pnu_packs`;
/// both are held to the contract's `parts.hash_test_vectors`.
export function fnv1a32(text: string): number {
  let hash = 0x811c9dc5;
  for (const byte of new TextEncoder().encode(text)) {
    hash = Math.imul(hash ^ byte, 0x01000193);
  }
  return hash >>> 0;
}

/// The part counts of one section generation: the dongs of more than one part, by dong.
export type PartCounts = ReadonlyMap<string, number>;

const NO_PARTS: PartCounts = new Map();
/// Parts indexes this isolate holds, least recently used first, by identity (`sha256:<hex>` for a
/// manifest-named index, `key:<key>` for a preview's): an index is immutable, and a lane reads one
/// generation per section (two across a publish), so a handful is enough.
const PARTS_MEMORY_ENTRIES = 4;
const partsMemory = new Map<string, PartCounts>();

function rememberParts(identity: string, counts: PartCounts): void {
  partsMemory.delete(identity);
  partsMemory.set(identity, counts);
  for (const oldest of partsMemory.keys()) {
    if (partsMemory.size <= PARTS_MEMORY_ENTRIES) break;
    partsMemory.delete(oldest);
  }
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

/// A parts index's bytes, checked against the section generation it is read for: the contract's
/// index schema, this lane's unit, the section and generation, the contract's hash, and every
/// entry a dong of the unit length with a count of at least 2 whose last part is still a unit the
/// contract allows. A manifest-named index must also name exactly as many dongs as the manifest says.
function parsePartsIndex(bytes: Uint8Array, section: PackSection, partedUnits: number | null): PartCounts {
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes));
  } catch {
    throw new PackFormatError("parts index is not JSON");
  }
  if (typeof raw !== "object" || raw === null) throw new PackFormatError("parts index is not an object");
  const index = raw as Record<string, unknown>;
  if (
    index.schema_version !== partsPolicy.index_schema_version ||
    index.unit !== UNIT ||
    index.section !== section.name ||
    index.generation !== section.generation ||
    index.hash !== partsPolicy.hash ||
    typeof index.parts !== "object" ||
    index.parts === null ||
    Array.isArray(index.parts)
  ) {
    throw new PackFormatError("parts index is not this section generation's");
  }
  const dongPattern = new RegExp(`^[0-9]{${packPolicy.unit_prefix_length}}$`);
  const counts = new Map<string, number>();
  for (const [dong, count] of Object.entries(index.parts as Record<string, unknown>)) {
    if (
      !dongPattern.test(dong) ||
      typeof count !== "number" ||
      !Number.isSafeInteger(count) ||
      count < 2 ||
      !partedUnitPattern.test(`${dong}-${count - 1}`)
    ) {
      throw new PackFormatError("parts index entry is not a dong of more than one part");
    }
    counts.set(dong, count);
  }
  if (partedUnits !== null && counts.size !== partedUnits) {
    throw new PackFormatError("parts index names another number of dongs than the manifest");
  }
  return counts;
}

/// The part counts a section generation is read under: none for an unparted section; else its
/// index from isolate memory, or read once from R2. A manifest-named index that is missing, or
/// whose bytes do not hash to the manifest's sha256, is an outage, never a guess; a preview's
/// conventional index that is absent leaves the generation unparted.
async function partCounts(reads: PackReads, section: PackSection): Promise<PartCounts> {
  const parts = section.parts;
  if (parts === undefined) return NO_PARTS;
  const identity = parts.kind === "named" ? `sha256:${parts.sha256}` : `key:${parts.key}`;
  const remembered = partsMemory.get(identity);
  if (remembered !== undefined) {
    rememberParts(identity, remembered);
    reads.trace.parts.set(section.name, "memory");
    return remembered;
  }
  const bytes = await withRetry(reads, { section: section.name, key: parts.key, phase: "parts" }, async () => {
    const object = await reads.bucket.get(parts.key);
    if (object === null) return null;
    if (!("body" in object)) throw new PackFormatError("parts index read returned no body");
    return new Uint8Array(await object.arrayBuffer());
  });
  let counts: PartCounts;
  if (bytes === null) {
    if (parts.kind === "named") throw new PackFormatError(`listed parts index ${parts.key} is missing`);
    reads.trace.parts.set(section.name, "absent");
    counts = NO_PARTS;
  } else {
    if (parts.kind === "named" && (await sha256Hex(bytes)) !== parts.sha256) {
      throw new PackFormatError(`parts index ${parts.key} does not hash to the manifest's sha256`);
    }
    counts = parsePartsIndex(bytes, section, parts.kind === "named" ? parts.partedUnits : null);
    reads.trace.parts.set(section.name, "r2");
  }
  rememberParts(identity, counts);
  return counts;
}

/// The unit of `pnu` (root ADR-0163): its dong, or `{dong}-{fnv1a32(pnu) mod K}` when the
/// generation's index gives the dong K > 1 parts.
export function partUnit(counts: PartCounts, unitLength: number, pnu: string): string {
  const dong = pnu.slice(0, unitLength);
  const count = counts.get(dong) ?? 1;
  return count === 1 ? dong : `${dong}-${fnv1a32(pnu) % count}`;
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

/// A served document: its gzip member as the pack holds it, and a strong entity tag. A pack key is
/// create-only, so the pack's own tag and the member's place in it name these bytes exactly.
export interface ServedDocument {
  member: Uint8Array;
  etag: string;
}

/// One document's gzip member: from the whole pack's bytes when they were read, else by one range
/// read whose entity tag must be the head's. The tag is compared on the answer rather than sent as
/// `onlyIf.etagMatches`: a pack key is create-only, so the condition never decides anything, and
/// it cost CPU on every large-pack read (2026-10-06, 590 reads: p99 8.4 ms with it, 6.7 ms
/// without). A rewritten key would still be refused here, only after its bytes arrived.
async function readDocument(
  reads: PackReads,
  section: string,
  key: string,
  copy: PackCopy,
  entry: Entry,
): Promise<ServedDocument> {
  const start = copy.head.bodyStart + entry.offset;
  const etag = `${copy.head.etag}-${entry.offset}`;
  if (copy.bytes !== null) return { member: copy.bytes.subarray(start, start + entry.length), etag };
  reads.trace.note(section, "range");
  const member = await withRetry(reads, { section, key, phase: "range" }, async () => {
    const object = await reads.bucket.get(key, { range: { offset: start, length: entry.length } });
    if (object === null || !("body" in object) || object.etag !== copy.head.etag) {
      // Pack keys are create-only: never combine a remembered head with other bytes.
      throw new PackFormatError("pack changed or vanished under its remembered head");
    }
    return new Uint8Array(await object.arrayBuffer());
  });
  return { member, etag };
}

const answerCacheOrigin = `${cacheOrigin}/answer/`;
const ANSWER_ETAG_HEADER = "X-Member-Etag";

/// The edge cache identity of one PNU's served member under one served state: the plan's
/// fingerprint names the generations and patches, so a new publish never answers from a copy of
/// the previous one, and the synthetic origin keeps any client URL from naming or shaping it.
export function answerCacheUrl(fingerprint: string, pnu: string): string {
  return `${answerCacheOrigin}${encodeURIComponent(fingerprint)}/${pnu}`;
}

/// The edge copy of a served member: its bytes as the pack holds them, its entity tag beside them.
/// Stored without `Content-Encoding`, so the Cache API keeps the bytes as they are.
export function answerCacheResponse(document: ServedDocument): Response {
  return new Response(document.member, {
    headers: { "Cache-Control": packPolicy.cache_control, [ANSWER_ETAG_HEADER]: document.etag },
  });
}

/// A PNU's served member from the edge cache, or `null` when there is none (or it carries no tag).
/// A warm read of the same PNU is then one cache lookup, as on the object path, with no pack read.
export async function answerFromCache(fingerprint: string, pnu: string): Promise<ServedDocument | null> {
  const cached = await caches.default.match(answerCacheUrl(fingerprint, pnu));
  if (cached === undefined) return null;
  const etag = cached.headers.get(ANSWER_ETAG_HEADER);
  const member = new Uint8Array(await cached.arrayBuffer());
  return etag === null || etag === "" || member.byteLength === 0 ? null : { member, etag };
}

export type Resolved =
  | ({ kind: "document" } & ServedDocument)
  | { kind: "tombstone" }
  | { kind: "absent" };

export function packKey(section: Pick<PackSection, "name" | "generation">, patch: number | null, unit: string): string {
  const patchDir = patch === null ? "" : `p${patch}/`;
  return `${lanePacks.root}/${section.name}/g${section.generation}/${patchDir}${unit}${packPolicy.suffix}`;
}

/// The document of `pnu` in one section: the newest patch naming its unit and holding it, else the
/// base. A patch the manifest lists but R2 lacks is an outage, not an absence. The base and the
/// patches of a parted generation share its parts (root ADR-0163 §4), so a patch naming a parted
/// dong by its bare dong was written under other part counts: an outage too.
async function findDocument(reads: PackReads, plan: PackPlan, section: PackSection, pnu: string): Promise<Resolved> {
  const unit = partUnit(await partCounts(reads, section), plan.unitLength, pnu);
  const dong = pnu.slice(0, plan.unitLength);
  for (const patch of plan.patches) {
    if (patch.patch <= section.patchFloor) continue;
    if (unit !== dong && patch.units.has(dong)) {
      throw new PackFormatError(`patch p${patch.patch} names parted dong ${dong} unparted`);
    }
    if (!patch.units.has(unit)) continue;
    const key = packKey(section, patch.patch, unit);
    const copy = await readPack(reads, section.name, key);
    if (copy === null) throw new PackFormatError(`listed patch pack ${key} is missing`);
    const entry = findEntry(copy.head, pnu);
    if (entry === null) continue;
    if (entry.state === STATE_TOMBSTONE) return { kind: "tombstone" };
    return { kind: "document", ...(await readDocument(reads, section.name, key, copy, entry)) };
  }
  const key = packKey(section, null, unit);
  const copy = await readPack(reads, section.name, key);
  if (copy === null) return { kind: "absent" };
  const entry = findEntry(copy.head, pnu);
  if (entry === null) return { kind: "absent" };
  if (entry.state === STATE_TOMBSTONE) throw new PackFormatError("tombstone in a base pack");
  return { kind: "document", ...(await readDocument(reads, section.name, key, copy, entry)) };
}

/// The PNU's answer, from the lane's one section (the contract's anchor, `documents`).
export async function resolvePacks(reads: PackReads, plan: PackPlan, pnu: string): Promise<Resolved> {
  const [section, ...others] = plan.sections;
  if (section === undefined || others.length > 0 || section.name !== lanePacks.anchor_section) {
    throw new PackFormatError("a plan serves exactly the contract's one documents section");
  }
  return findDocument(reads, plan, section, pnu);
}
