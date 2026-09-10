import { afterEach, expect, it, vi } from "vitest";
import contract from "../../../config/r2-connections.contract.json";
import { fetchBuilding } from "../src/index";

afterEach(() => vi.unstubAllGlobals());

it.each(["manifest", "object"])("%s read failure is a CORS-readable, uncacheable outage", async (failure) => {
  vi.stubGlobal("caches", { default: { match: async () => undefined, put: async () => {} } });
  const policy = contract.building_by_pnu_gateway;
  const bucket = { get: vi.fn(async (key: string) => {
    if (failure === "object" && key === policy.object_key.manifest_object) {
      return { body: null, text: async () => JSON.stringify({ schema_version: 1, unit: "building-by-pnu", current_generation: 1 }) };
    }
    throw new Error("synthetic R2 failure");
  }) };
  const response = await fetchBuilding(
    new Request(`https://buildings.example.test${policy.request_path.prefix}9999900000100000000`, { headers: { Origin: "http://localhost:3000" } }),
    { [policy.r2_binding]: bucket as unknown as Pick<R2Bucket, "get">, [policy.allowed_origins_binding]: "http://localhost:3000" },
    { waitUntil: vi.fn() } as unknown as ExecutionContext,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(response.headers.get("access-control-allow-origin")).toBe("http://localhost:3000");
});
