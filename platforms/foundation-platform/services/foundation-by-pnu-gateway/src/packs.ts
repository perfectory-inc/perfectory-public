import connectionContract from "../../../config/r2-connections.contract.json";
import { cacheOrigin, lanePacks } from "./lane";

/// Section packs (root ADR-0147, ADR-0151): one R2 object per (section, legal dong), head + index +
/// body. Each lane has one section, `documents`, whose entry for a PNU is its served
/// document as one gzip member; the Worker answers with that member as it is
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
/// gate probes. The fingerprint names the preview's Worker version, so a fresh upload answers its
/// first reads from R2 instead of from edge copies an earlier probe left (2026-10-07: 9,952 of
/// 10,000 cold reads of gate (b) were such copies; contract `cold_reads_not_from_r2_max_share`).
export function previewPlan(generation: number, version: string | null): PackPlan {
  return {
    sections: lanePacks.sections.map((name) => ({ name, generation, patchFloor: 0 })),
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

/// Forgets every pack this isolate remembers (tests, to stand for a new isolate).
export function forgetPacks(): void {
  packMemory.clear();
  packMemoryBytes = 0;
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
  what: { section: string; key: string; phase: "pack" | "range" },
  attempt: () => Promise<T>,
): Promise<T> {
  const sleep = reads.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const random = reads.random ?? Math.random;
  let lastClass = "deadline";
  for (let tried = 0; tried < readPolicy.r2_attempts; tried += 1) {
    const left = reads.deadline - Date.now();
    if (left <= 0) break;
    const started = Date.now();
    reads.trace.r2Gets += 1;
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
      reads.trace.waited(what.section, Date.now() - started);
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

/// The document of `pnu` in one section: the newest patch naming its dong and holding it, else the
/// base. A patch the manifest lists but R2 lacks is an outage, not an absence.
async function findDocument(reads: PackReads, plan: PackPlan, section: PackSection, pnu: string): Promise<Resolved> {
  const unit = pnu.slice(0, plan.unitLength);
  for (const patch of plan.patches) {
    if (patch.patch <= section.patchFloor || !patch.units.has(unit)) continue;
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
