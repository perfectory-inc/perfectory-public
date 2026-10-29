import { readFile } from "node:fs/promises";

import type { Miniflare } from "miniflare";
import { afterEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import registry from "../../../contracts/schema-registry/registry.json";
import { LANES } from "../src/lane";
import { documentSchemas, recordsReadableDocuments, subjectOfLane } from "../src/schemas";
import { startGateway } from "./miniflare";

// Root ADR-0336. 2026-10-27..29: the public auction document went from v1 to v2 (an optional media
// index per round) and every reader that held its own copy of the version string broke on its own.
// This Worker takes the subject's majors from the bundled registry: any revision of a major it
// reads is served, and a major it does not read is an outage rather than a document no reader can
// parse. The publisher asks its capabilities before it publishes a new major (can-i-deploy). PNUs
// sit in the repository-reserved synthetic namespace.
const PNU = "9999900000100000000";
const UNIT = "9999900000";
const PACK_POLICY = connectionContract.by_pnu_section_packs;
const AUCTION = LANES.find((lane) => lane.name === "parcel-public-auction");
const SUBJECT = registry.subjects.find((subject) => subject.owner.lane === "parcel-public-auction")!.subject;
const fixtures = new URL("fixtures/section-packs/", import.meta.url);

function manifest(documentSchema: string, patchSchema?: string): string {
  return `${JSON.stringify({
    schema_version: 2,
    unit: "parcel-public-auction-by-pnu",
    base_generation: 1,
    base_object_count: 0,
    document_schema_version: documentSchema,
    gold_table: "gold.parcel_public_auction",
    gold_iceberg_snapshot_id: "999990000000000001",
    reflected_gold_iceberg_snapshot_id: "999990000000000001",
    pnu_prefix_length: 5,
    patches: [],
    object_count: 0,
    published_at_utc: "2026-01-01T00:00:00Z",
    section_packs: {
      schema_version: PACK_POLICY.manifest_section_packs_schema_version,
      format_version: PACK_POLICY.format_version,
      unit_prefix_length: PACK_POLICY.unit_prefix_length,
      document_schema_version: documentSchema,
      gold_table: "gold.parcel_public_auction",
      reflected_gold_iceberg_snapshot_id: "999990000000000001",
      document_count: 2,
      sections: [{ name: "documents", generation: 1, gold_iceberg_snapshot_id: "999990000000000001", document_count: 2, pack_count: 1, patch_floor: 0 }],
      patches:
        patchSchema === undefined
          ? []
          : [{ patch: 1, units: ["9999999999"], upserted: 0, deleted: 0, document_schema_version: patchSchema }],
    },
  })}\n`;
}

describe("the public auction subject's majors come from the registry (root ADR-0336)", () => {
  let runtime: Miniflare | undefined;

  afterEach(async () => {
    await runtime?.dispose();
    runtime = undefined;
  });

  it("reads every string of major 1, the legacy v2 among them, and nothing else", () => {
    const subject = subjectOfLane("parcel-public-auction");
    expect(subject?.subject).toBe(SUBJECT);
    expect(subject?.majors).toEqual([1]);
    expect([...(subject?.strings ?? [])]).toEqual([`${SUBJECT}.v1`, `${SUBJECT}.v2`]);
    // A later revision of major 1 carries `<subject>.v1`: no reader here changes for it.
    expect(recordsReadableDocuments("parcel-public-auction", { document_schema_version: `${SUBJECT}.v1` })).toBe(true);
    expect(recordsReadableDocuments("parcel-public-auction", { document_schema_version: `${SUBJECT}.v2` })).toBe(true);
    expect(
      recordsReadableDocuments("parcel-public-auction", {
        document_schema_version: `${SUBJECT}.v2`,
        patches: [{ document_schema_version: `${SUBJECT}.v3` }],
      }),
    ).toBe(false);
    expect(recordsReadableDocuments("parcel-public-auction", {})).toBe(false);
    // A lane the registry does not list keeps its earlier mechanism.
    expect(subjectOfLane("parcel-zoning")).toBeNull();
    expect(recordsReadableDocuments("parcel-zoning", { document_schema_version: "anything" })).toBe(true);
    expect(documentSchemas("parcel-public-auction")).toEqual({ [SUBJECT]: [1] });
    expect(documentSchemas("parcel")).toEqual({});
  });

  it("serves a major it reads, names it at the capabilities path, and refuses one it does not", async () => {
    expect(AUCTION).toBeDefined();
    const pack = new Uint8Array(await readFile(new URL("g1-documents.pack", fixtures)));
    const read = () =>
      runtime!.dispatchFetch(`https://catalog.example.test${AUCTION!.prefix}${PNU}`, { headers: { "Accept-Encoding": "gzip" } });
    for (const [documentSchema, patchSchema, status] of [
      [`${SUBJECT}.v2`, undefined, 200],
      [`${SUBJECT}.v1`, `${SUBJECT}.v1`, 200],
      [`${SUBJECT}.v2`, `${SUBJECT}.v3`, 503],
    ] as const) {
      runtime = await startGateway("parcel");
      const bucket = await runtime.getR2Bucket(connectionContract.parcel_by_pnu_gateway.r2_binding);
      await bucket.put(AUCTION!.manifestObject, manifest(documentSchema, patchSchema));
      await bucket.put(`${AUCTION!.packRoot}/documents/g1/${UNIT}${PACK_POLICY.suffix}`, pack);
      expect((await read()).status, `${documentSchema} ${patchSchema}`).toBe(status);
      const capabilities = (await (
        await runtime.dispatchFetch(`https://catalog.example.test${AUCTION!.capabilities}`)
      ).json()) as { unit: string; document_schemas: Record<string, number[]> };
      expect(capabilities.unit).toBe("parcel-public-auction-by-pnu");
      expect(capabilities.document_schemas).toEqual({ [SUBJECT]: [1] });
      await runtime.dispose();
      runtime = undefined;
    }
  });
});
