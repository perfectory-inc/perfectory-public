// @vitest-environment node

import { NextRequest } from "next/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import { register } from "@/instrumentation";
import { proxy } from "@/proxy";

const sdkStarts = vi.hoisted(() => ({ count: 0 }));
vi.mock("@opentelemetry/sdk-node", () => ({
  NodeSDK: class {
    start() {
      sdkStarts.count += 1;
    }
  },
}));
// register() imports this beside the SDK. The real module's first import took 4 s on a cold disk
// (2026-10-19), which a full run's contention pushed past the 5 s test limit; the test only counts
// SDK starts.
vi.mock("@opentelemetry/instrumentation-fetch", () => ({
  FetchInstrumentation: class {},
}));

const FLAG = "NEXT_PUBLIC_GONGZZANG_READ_ONLY_PREVIEW";

async function answer(path: string) {
  return proxy(new NextRequest(`http://localhost:3000${path}`));
}

describe("read-only preview (root ADR-0249)", () => {
  afterEach(() => vi.unstubAllEnvs());

  it("keeps the login when the flag is not set", async () => {
    const res = await answer("/listings");
    expect(res.status).toBe(307);
    expect(res.headers.get("location")).toContain("/login?returnTo=%2Flistings");
  });

  it("keeps the login for any value other than 1", async () => {
    vi.stubEnv(FLAG, "true");
    expect((await answer("/listings")).status).toBe(307);
  });

  it("serves the map page without a session when the flag is set", async () => {
    vi.stubEnv(FLAG, "1");
    const res = await answer("/listings");
    expect(res.status).toBe(200);
    expect(res.headers.get("content-security-policy")).toContain("default-src 'self'");
  });

  it("answers 404 for every page and API that needs identity or the Rust API", async () => {
    vi.stubEnv(FLAG, "1");
    for (const path of [
      "/profile",
      "/listings/new",
      "/complexes",
      "/login",
      "/api/proxy/listings",
      "/api/auth/login",
    ]) {
      expect((await answer(path)).status, path).toBe(404);
    }
  });

  it("still sends the root to the map page", async () => {
    vi.stubEnv(FLAG, "1");
    const res = await answer("/");
    expect(res.status).toBe(307);
    expect(res.headers.get("location")).toBe("http://localhost:3000/listings");
  });

  it("starts telemetry only outside the preview", async () => {
    vi.stubEnv("NEXT_RUNTIME", "nodejs");
    sdkStarts.count = 0;
    vi.stubEnv(FLAG, "1");
    await register();
    expect(sdkStarts.count).toBe(0);
    vi.stubEnv(FLAG, "");
    await register();
    expect(sdkStarts.count).toBe(1);
  });
});
