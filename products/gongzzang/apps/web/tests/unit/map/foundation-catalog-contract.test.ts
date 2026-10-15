// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import openapiTS, { astToString } from "openapi-typescript";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startFoundationVectorManifestPolling } from "@/lib/map/foundation-vector-source-refresh";

const webRoot = resolve(__dirname, "../../..");
// The reviewed snapshot that docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json
// pins by hash; the generated types must be exactly what it produces.
const snapshot = resolve(
  webRoot,
  "../../crates/foundation-platform-client/openapi/catalog.v1.json",
);
const generated = resolve(webRoot, "lib/api/foundation-catalog.generated.ts");
const normalize = (text: string) => text.replaceAll("\r\n", "\n").trim();

describe("Foundation Catalog contract types", () => {
  it("are what the pinned OpenAPI snapshot generates", async () => {
    const expected = normalize(
      astToString(await openapiTS(JSON.parse(readFileSync(snapshot, "utf8")))),
    );
    // The committed file carries openapi-typescript's banner; compare the generated body only.
    const current = normalize(readFileSync(generated, "utf8"));
    expect(
      current.endsWith(expected),
      "lib/api/foundation-catalog.generated.ts is stale: run `pnpm api:generate` and commit it",
    ).toBe(true);
  });
});

describe("Foundation manifest polling cadence", () => {
  afterEach(() => vi.useRealTimers());

  it("waits the interval the latest manifest names, not a client constant", async () => {
    vi.useFakeTimers();
    const fetchManifest = vi.fn(async () => ({ manifest: { refresh_after_seconds: 9 } }));
    const stop = startFoundationVectorManifestPolling({
      initialManifest: { refresh_after_seconds: 7 },
      intervalMs: (latest) => latest.refresh_after_seconds * 1_000,
      startImmediately: false,
      fetchManifest,
      onManifest: () => undefined,
    });
    await vi.advanceTimersByTimeAsync(6_999);
    expect(fetchManifest).toHaveBeenCalledTimes(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetchManifest).toHaveBeenCalledTimes(1);
    // The fetched manifest now says nine seconds.
    await vi.advanceTimersByTimeAsync(8_999);
    expect(fetchManifest).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetchManifest).toHaveBeenCalledTimes(2);
    stop();
  });
});
