// @vitest-environment node

import { NextRequest } from "next/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/ratelimit", () => ({ checkRate: vi.fn() }));
vi.mock("@/lib/session/store", () => ({ getSession: vi.fn() }));

const { proxy } = await import("@/proxy");
const { preconnectDataOrigins } = await import("@/lib/browser-data-origins");

describe("preconnect hints", () => {
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL", "https://foundation.example.test");
  });
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("name only origins the CSP lets the page fetch", async () => {
    const res = await proxy(new NextRequest("https://gongzzang.example.test/login"));
    const connectSrc =
      (res.headers.get("content-security-policy") ?? "")
        .split(";")
        .map((part) => part.trim())
        .find((part) => part.startsWith("connect-src"))
        ?.split(" ") ?? [];

    const origins = preconnectDataOrigins();
    expect(origins).toContain("https://foundation.example.test");
    for (const origin of origins) expect(connectSrc).toContain(origin);
  });

  it("stay within web.dev's few critical origins", () => {
    expect(preconnectDataOrigins().length).toBeLessThanOrEqual(4);
  });
});
