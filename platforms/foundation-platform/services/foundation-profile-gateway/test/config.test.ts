import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import connectionContract from "../../../config/r2-connections.contract.json";

describe("generated Wrangler configuration", () => {
  it("projects the single R2 contract without credentials or production routes", async () => {
    const text = await readFile(new URL("../wrangler.jsonc", import.meta.url), "utf8");
    const config = JSON.parse(text) as Record<string, unknown>;
    const gateway = connectionContract.profile_gateway;
    const lakehouse = connectionContract.connections.lakehouse;

    expect(gateway.r2_binding).toMatch(/^FOUNDATION_PLATFORM_(?!R2_)[A-Z0-9_]+$/);
    expect(config.name).toBe(gateway.worker_name);
    expect(config.main).toBe("src/index.ts");
    expect(config.compatibility_date).toBe(gateway.compatibility_date);
    expect(config.workers_dev).toBe(false);
    expect(config.keep_vars).toBe(true);
    expect(config.r2_buckets).toEqual([
      {
        binding: gateway.r2_binding,
        bucket_name: lakehouse.expected_values.FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET,
      },
    ]);
    expect(config.vars).toBeUndefined();
    expect(text).not.toMatch(/remote|routes|custom_domain|account_id|access_key|secret/i);
    expect(text).not.toContain('"*"');
  });
});
