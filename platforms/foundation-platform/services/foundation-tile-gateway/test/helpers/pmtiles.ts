import { gzipSync } from "node:zlib";

// Entirely synthetic: Hilbert tile ids 0 and 1, no geographic feature data.
export const TILE_BYTES = [new Uint8Array([0x1a, 0x01, 0x00]), new Uint8Array([0x1a, 0x01, 0x01])];

function varint(value: number): number[] {
  const bytes: number[] = [];
  do {
    const remainder = value % 128;
    value = Math.floor(value / 128);
    bytes.push(remainder | (value > 0 ? 128 : 0));
  } while (value > 0);
  return bytes;
}

export function syntheticArchive({ tileType = 1, tileCompression = 1, internalCompression = 1 } = {}): Uint8Array {
  const payloads = TILE_BYTES.map((bytes) => tileCompression === 2 ? gzipSync(bytes) : bytes);
  const directory = new Uint8Array([
    ...varint(2), // entry count
    ...varint(0), ...varint(1), // delta-coded Hilbert ids
    ...varint(1), ...varint(1), // run lengths
    ...payloads.flatMap((bytes) => varint(bytes.length)),
    ...varint(1), ...varint(0), // offset+1, then contiguous offset sentinel
  ]);
  const root = internalCompression === 2 ? gzipSync(directory) : directory;
  const metadata = internalCompression === 2 ? gzipSync("{}") : new TextEncoder().encode("{}");
  const tileOffset = 127 + root.length + metadata.length;
  const tileLength = payloads.reduce((sum, bytes) => sum + bytes.length, 0);
  const archive = new Uint8Array(tileOffset + tileLength);
  archive.set(new TextEncoder().encode("PMTiles"));
  archive[7] = 3;
  const header = new DataView(archive.buffer);
  for (const [offset, value] of [
    [8, 127], [16, root.length], [24, 127 + root.length], [32, metadata.length],
    [40, tileOffset], [48, 0], [56, tileOffset], [64, tileLength], [72, 2], [80, 2], [88, 2],
  ] as const) header.setBigUint64(offset, BigInt(value), true);
  archive[96] = 1; // clustered
  archive[97] = internalCompression;
  archive[98] = tileCompression;
  archive[99] = tileType;
  archive[100] = 0;
  archive[101] = 1;
  // Bounds/center bytes stay zero; they do not describe real parcels.
  archive.set(root, 127);
  archive.set(metadata, 127 + root.length);
  let offset = tileOffset;
  for (const bytes of payloads) {
    archive.set(bytes, offset);
    offset += bytes.length;
  }
  return archive;
}

export class FakeBucket {
  readonly reads: { key: string; offset: number; length: number }[] = [];
  readonly objects = new Map<string, Uint8Array>();
  readonly etag = "synthetic-object-etag";
  failure: Error | undefined;

  async get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null> {
    if (this.failure !== undefined) throw this.failure;
    const range = options?.range;
    if (range === undefined || !("offset" in range) || range.offset === undefined ||
      !("length" in range) || range.length === undefined) {
      throw new Error("The gateway must use bounded R2 range reads");
    }
    this.reads.push({ key, offset: range.offset, length: range.length });
    const object = this.objects.get(key);
    if (object === undefined) return null;
    const bytes = object.slice(range.offset, range.offset + range.length);
    return {
      etag: this.etag,
      httpEtag: `"${this.etag}"`,
      body: new Response(bytes).body,
      arrayBuffer: async () => bytes.buffer,
    } as R2ObjectBody;
  }

  binding(): Pick<R2Bucket, "get"> {
    return this as unknown as Pick<R2Bucket, "get">;
  }
}
