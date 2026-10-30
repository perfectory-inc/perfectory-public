// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import { generate } from "../../codegen/gen-schemas.mjs";
import { OwnershipDocumentSchema } from "./ownership";
import { PublicAuctionDocumentSchema } from "./public-auction";

// Root ADR-0336. 2026-10-29: an old build of this web refused the public auction's v2 documents —
// its own z.enum of version strings no longer matched what the foundation published. The web now
// parses what the foundation's schema registry says, by subject and major, and reads exactly the
// paths its pact in the registry lists, which the registry's CI check holds every revision to.
const REPOSITORY = resolve(__dirname, "../../../../../..");
const registry = JSON.parse(
  readFileSync(
    resolve(REPOSITORY, "platforms/foundation-platform/contracts/schema-registry/registry.json"),
    "utf8",
  ),
) as {
  subjects: {
    subject: string;
    owner: { lane: string };
    consumers: { name: string; at: string; reads: { path: string }[] }[];
  }[];
};

/// Every property path a JSON Schema reaches, an array's items under `<field>[]`.
function paths(schema: Record<string, unknown>, at = ""): string[] {
  const found: string[] = [];
  const items = schema.items as Record<string, unknown> | undefined;
  if (items !== undefined) found.push(...paths(items, `${at}[]`));
  for (const variant of (schema.anyOf as Record<string, unknown>[] | undefined) ?? []) {
    found.push(...paths(variant, at));
  }
  for (const [name, inner] of Object.entries(
    (schema.properties as Record<string, Record<string, unknown>> | undefined) ?? {},
  )) {
    const path = at === "" ? name : `${at}.${name}`;
    found.push(path, ...paths(inner, path));
  }
  return found;
}

describe("the web parses what the schema registry says (root ADR-0336)", () => {
  it("commits exactly what pnpm gen:schemas generates from the registry", () => {
    const files = generate(REPOSITORY);
    expect(files.length).toBeGreaterThan(0);
    for (const file of files) {
      expect(readFileSync(resolve(REPOSITORY, file.path), "utf8"), file.path).toBe(file.text);
    }
  });

  it("reads exactly the public auction paths its pact lists", () => {
    const entry = registry.subjects.find(
      (subject) => subject.owner.lane === "parcel-public-auction",
    );
    const pact = entry?.consumers.find((consumer) => consumer.name === "web");
    expect(pact?.at).toBe("products/gongzzang/apps/web/lib/api/public-auction.generated.ts");
    const read = paths(z.toJSONSchema(PublicAuctionDocumentSchema, { io: "input" }));
    expect([...new Set(read)].sort()).toEqual(pact?.reads.map((r) => r.path).sort());
  });

  it("reads exactly the ownership paths its pact lists", () => {
    const entry = registry.subjects.find((subject) => subject.owner.lane === "parcel-ownership");
    const pact = entry?.consumers.find((consumer) => consumer.name === "web");
    expect(pact?.at).toBe("products/gongzzang/apps/web/lib/api/ownership.generated.ts");
    const read = paths(z.toJSONSchema(OwnershipDocumentSchema, { io: "input" }));
    expect([...new Set(read)].sort()).toEqual(pact?.reads.map((r) => r.path).sort());
  });
});
