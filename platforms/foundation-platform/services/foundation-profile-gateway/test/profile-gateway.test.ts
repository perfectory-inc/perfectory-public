import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { parseAllowedOrigins } from "../src/index";

const PROFILE_ID = "00000000-0000-7000-8000-000000000001";
const PROFILE_KEY = `gold/industrial-complex/profiles/${PROFILE_ID}.json`;
const PROFILE_URL = `https://profiles.example.test/${PROFILE_KEY}`;
const ALLOWED_ORIGIN = "https://app.example.test";
const SECOND_ALLOWED_ORIGIN = "https://admin.example.test";
const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const R2_BINDING = connectionContract.profile_gateway.r2_binding;

describe("foundation profile gateway", () => {
  let runtime: Miniflare | undefined;
  let profileBody: string;

  beforeEach(async () => {
    profileBody = await readFile(new URL("fixtures/profile.json", import.meta.url), "utf8");
    const bundle = await build({
      entryPoints: [fileURLToPath(new URL("../src/index.ts", import.meta.url))],
      bundle: true,
      format: "esm",
      platform: "browser",
      write: false,
      absWorkingDir: packageRoot,
    });
    const output = bundle.outputFiles[0];
    if (output === undefined) throw new Error("esbuild emitted no Worker module");
    runtime = new Miniflare({
      compatibilityDate: "2026-04-26",
      modules: [{ type: "ESModule", path: "index.mjs", contents: output.text }],
      r2Buckets: [R2_BINDING],
      bindings: {
        FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS: `${ALLOWED_ORIGIN}, ${SECOND_ALLOWED_ORIGIN}`,
      },
      cache: true,
    });
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    await bucket.put(PROFILE_KEY, profileBody, {
      httpMetadata: { contentType: "application/json; charset=utf-8" },
    });
    await bucket.put("bronze/vworld/2026/raw.jsonl", "{}\n");
  });

  afterEach(async () => {
    await runtime?.dispose();
  });

  it("canonical GET returns the immutable profile", async () => {
    const response = await runtime?.dispatchFetch(PROFILE_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    if (response === undefined) throw new Error("Miniflare did not start");

    expect(response.status).toBe(200);
    expect(await response.text()).toBe(profileBody);
    expect(response.headers.get("content-type")).toBe("application/json; charset=utf-8");
    expect(response.headers.get("cache-control")).toBe(
      "public, max-age=31536000, immutable",
    );
    expect(response.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]+"$/);
  });

  it("missing canonical profile returns 404", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(
      "https://profiles.example.test/gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000002.json",
    );
    expect(response.status).toBe(404);
  });

  it.each([
    "https://profiles.example.test/bronze/vworld/2026/raw.jsonl",
    "https://profiles.example.test/silver/industrial-complexes/part-0.parquet",
    `https://profiles.example.test/gold/industrial-complex/profiles/nested/${PROFILE_ID}.json`,
    "https://profiles.example.test/gold/industrial-complex/profiles/latest.json",
    "https://profiles.example.test/gold/industrial-complex/profiles/not-a-uuid.json",
    `${PROFILE_URL}?download=1`,
  ])("non-profile paths return 404: %s", async (url) => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    expect((await runtime.dispatchFetch(url)).status).toBe(404);
  });

  it.each([
    `https://profiles.example.test/gold/industrial-complex/profiles/../../bronze/x`,
    `https://profiles.example.test/gold/industrial-complex/profiles/%2e%2e/%2e%2e/bronze/x`,
    `https://profiles.example.test/gold/industrial-complex/profiles/%252e%252e/%252e%252e/bronze/x`,
    `https://profiles.example.test/gold/industrial-complex/profiles/${PROFILE_ID}%2f..%2fbronze.json`,
    `https://profiles.example.test/gold/industrial-complex/profiles/${PROFILE_ID}%252f..%252fbronze.json`,
  ])("traversal forms return 404: %s", async (url) => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    expect((await runtime.dispatchFetch(url)).status).toBe(404);
  });

  it.each(["PUT", "POST", "DELETE", "PATCH"])(
    "write methods return 405 and preserve the object: %s",
    async (method) => {
      if (runtime === undefined) throw new Error("Miniflare did not start");
      const response = await runtime.dispatchFetch(PROFILE_URL, {
        method,
        ...(method === "DELETE" ? {} : { body: "mutated" }),
      });
      expect(response.status).toBe(405);
      expect(response.headers.get("allow")).toBe("GET, HEAD, OPTIONS");
      expect(await (await runtime.dispatchFetch(PROFILE_URL)).text()).toBe(profileBody);
    },
  );

  it("origin gate and cache isolation never emit wildcard CORS", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");

    const noOrigin = await runtime.dispatchFetch(PROFILE_URL);
    expect(noOrigin.status).toBe(200);
    expect(noOrigin.headers.get("access-control-allow-origin")).toBeNull();

    const first = await runtime.dispatchFetch(PROFILE_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    expect(first.status).toBe(200);
    expect(first.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(first.headers.get("vary")).toBe("Origin");

    const second = await runtime.dispatchFetch(PROFILE_URL, {
      headers: { Origin: SECOND_ALLOWED_ORIGIN },
    });
    expect(second.status).toBe(200);
    expect(second.headers.get("access-control-allow-origin")).toBe(SECOND_ALLOWED_ORIGIN);
    expect(second.headers.get("access-control-allow-origin")).not.toBe("*");

    const denied = await runtime.dispatchFetch(PROFILE_URL, {
      headers: { Origin: "https://attacker.example.test" },
    });
    expect(denied.status).toBe(403);
    expect(denied.headers.get("access-control-allow-origin")).toBeNull();
  });

  it("cached object is origin-neutral", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const response = await runtime.dispatchFetch(PROFILE_URL, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    await response.arrayBuffer();
    const cached = await (await runtime.getCaches()).default.match(PROFILE_URL);
    expect(cached).toBeDefined();
    expect(cached?.headers.get("access-control-allow-origin")).toBeNull();
    expect(cached?.headers.get("vary")).toBeNull();
  });

  it("CORS preflight allows only configured application origins", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const allowed = await runtime.dispatchFetch(PROFILE_URL, {
      method: "OPTIONS",
      headers: {
        Origin: ALLOWED_ORIGIN,
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "If-None-Match",
      },
    });
    expect(allowed.status).toBe(204);
    expect(allowed.headers.get("access-control-allow-origin")).toBe(ALLOWED_ORIGIN);
    expect(allowed.headers.get("access-control-allow-methods")).toBe("GET, HEAD, OPTIONS");
    expect(allowed.headers.get("access-control-allow-headers")).toBe("If-None-Match");
    expect(allowed.headers.get("access-control-expose-headers")).toBe("ETag");

    const denied = await runtime.dispatchFetch(PROFILE_URL, {
      method: "OPTIONS",
      headers: {
        Origin: "https://attacker.example.test",
        "Access-Control-Request-Method": "GET",
      },
    });
    expect(denied.status).toBe(403);
  });

  it("conditional GET returns 304 on cold R2 and warm Cache API matches", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const stored = await bucket.head(PROFILE_KEY);
    if (stored === null) throw new Error("profile fixture is missing");

    const cold = await runtime.dispatchFetch(PROFILE_URL, {
      headers: { "If-None-Match": stored.httpEtag, Origin: ALLOWED_ORIGIN },
    });
    expect(cold.status).toBe(304);
    expect(cold.headers.get("etag")).toBe(stored.httpEtag);
    expect(await cold.text()).toBe("");

    const nonMatch = await runtime.dispatchFetch(PROFILE_URL, {
      headers: { "If-None-Match": '"not-this-object"' },
    });
    expect(nonMatch.status).toBe(200);
    expect(await nonMatch.text()).toBe(profileBody);

    await bucket.delete(PROFILE_KEY);
    const warm = await runtime.dispatchFetch(PROFILE_URL, {
      headers: { "If-None-Match": stored.httpEtag, Origin: SECOND_ALLOWED_ORIGIN },
    });
    expect(warm.status).toBe(304);
    expect(warm.headers.get("access-control-allow-origin")).toBe(SECOND_ALLOWED_ORIGIN);
  });

  it("conditional HEAD returns headers without a body", async () => {
    if (runtime === undefined) throw new Error("Miniflare did not start");
    const bucket = await runtime.getR2Bucket(R2_BINDING);
    const stored = await bucket.head(PROFILE_KEY);
    if (stored === null) throw new Error("profile fixture is missing");

    const response = await runtime.dispatchFetch(PROFILE_URL, { method: "HEAD" });
    expect(response.status).toBe(200);
    expect(response.headers.get("etag")).toBe(stored.httpEtag);
    expect(response.headers.get("content-length")).toBe(String(stored.size));
    expect(await response.text()).toBe("");

    const conditional = await runtime.dispatchFetch(PROFILE_URL, {
      method: "HEAD",
      headers: { "If-None-Match": stored.httpEtag },
    });
    expect(conditional.status).toBe(304);
    expect(conditional.headers.get("etag")).toBe(stored.httpEtag);
    expect(await conditional.text()).toBe("");
  });

  it("CORS grammar matches the shared contract corpus", () => {
    for (const accepted of connectionContract.profile_gateway.cors.accepted) {
      expect(parseAllowedOrigins(accepted), accepted).not.toBeNull();
    }
    for (const rejected of connectionContract.profile_gateway.cors.rejected) {
      expect(parseAllowedOrigins(rejected), rejected).toBeNull();
    }
  });

  it("production source exposes no R2 list or write capability", async () => {
    const source = await readFile(new URL("../src/index.ts", import.meta.url), "utf8");
    expect(source).not.toMatch(/LAKEHOUSE\s*\.\s*(?:list|put|delete)\s*\(/);
    expect(source).not.toMatch(/(?:ACCESS_KEY|SECRET_ACCESS|ACCOUNT_ID)/);
    expect(source).not.toContain("foundation-platform-lakehouse-prod");
    expect(source).not.toMatch(/Access-Control-Allow-Origin[\s\S]{0,80}["']\*["']/);
    expect(source).toContain('Pick<R2Bucket, "get">');
  });
});
