import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import connectionContract from "../../../config/r2-connections.contract.json";

describe("generated Wrangler configuration", () => {
  it("projects the single R2 contract without credentials", async () => {
    const text = await readFile(new URL("../wrangler.jsonc", import.meta.url), "utf8");
    const config = JSON.parse(text) as Record<string, unknown>;
    const gateway = connectionContract.building_by_pnu_gateway;
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
    expect(text).not.toMatch(/remote|account_id|access_key|secret/i);
    expect(text).not.toContain('"*"');
  });

  it("serves exactly the hostnames the contract names, and nothing wildcarded", async () => {
    const text = await readFile(new URL("../wrangler.jsonc", import.meta.url), "utf8");
    const config = JSON.parse(text) as { routes?: { pattern: string; custom_domain: boolean }[] };
    const gateway = connectionContract.building_by_pnu_gateway;

    // 주소의 정본은 계약이다: 배포가 붙이는 도메인은 계약이 이름한 것과 정확히 같아야 하고,
    // 그래서 주소 이전은 계약 한 줄 변경이 된다.
    expect(config.routes).toEqual(
      [gateway.public_hostname, ...gateway.public_hostname_aliases].map((pattern) => ({
        pattern,
        custom_domain: true,
      })),
    );
    for (const route of config.routes ?? []) {
      expect(route.pattern).toMatch(/^[a-z0-9.-]+$/);
    }
  });

  it("manifest and objects live under one serving root the binding can reach", () => {
    const layout = connectionContract.building_by_pnu_gateway.object_key;
    expect(layout.manifest_object.startsWith(`${layout.root}/`)).toBe(true);
    expect(layout.manifest_object.slice(layout.root.length + 1)).not.toContain("/");
  });
});
