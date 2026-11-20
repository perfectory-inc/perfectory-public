// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("__Host- sid cookie helpers (production)", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("NEXT_PUBLIC_API_BASE_URL", "https://api.gongzzang.test");
    vi.stubEnv(
      "NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL",
      "https://foundation-platform.gongzzang.test",
    );
    vi.stubEnv("ZITADEL_ISSUER", "https://auth.gongzzang.test");
    vi.stubEnv("ZITADEL_CLIENT_ID", "production-client");
    vi.stubEnv("ZITADEL_AUDIENCE", "production-audience");
    vi.stubEnv("ZITADEL_REDIRECT_URI", "https://gongzzang.test/api/auth/callback");
    vi.stubEnv("REDIS_URL", "rediss://redis.gongzzang.test:6379");
    vi.stubEnv("SESSION_SECRET", "production-session-secret-32-bytes-valid");
    vi.stubEnv("INTERNAL_AUTH_SECRET", "production-internal-auth-secret-32-valid");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("uses __Host- prefix and security flags", async () => {
    const { SID_COOKIE_NAME } = await import("@/lib/session/cookie");
    expect(SID_COOKIE_NAME).toBe("__Host-sid");
  });

  it("setSidCookie returns Set-Cookie with all required flags", async () => {
    const { setSidCookie } = await import("@/lib/session/cookie");
    const setCookie = setSidCookie("abc123", 86400);
    expect(setCookie).toContain("__Host-sid=abc123");
    expect(setCookie).toContain("Secure");
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("SameSite=Strict");
    expect(setCookie).toContain("Path=/");
    expect(setCookie).toContain("Max-Age=86400");
    expect(setCookie).toContain("Partitioned");
  });

  it("deleteSidCookie returns Set-Cookie with Max-Age=0", async () => {
    const { deleteSidCookie } = await import("@/lib/session/cookie");
    const setCookie = deleteSidCookie();
    expect(setCookie).toContain("__Host-sid=");
    expect(setCookie).toContain("Max-Age=0");
  });
});

describe("dev cookie helpers (no prefix, localhost HTTP 호환)", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.stubEnv("NODE_ENV", "development");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("uses 'sid' (no __Host- prefix) in dev", async () => {
    const { SID_COOKIE_NAME } = await import("@/lib/session/cookie");
    expect(SID_COOKIE_NAME).toBe("sid");
  });

  it("uses 'auth-state' (no __Secure- prefix) in dev", async () => {
    const { AUTH_STATE_COOKIE_NAME } = await import("@/lib/session/cookie");
    expect(AUTH_STATE_COOKIE_NAME).toBe("auth-state");
  });

  it("setSidCookie omits Secure + Partitioned in dev", async () => {
    const { setSidCookie } = await import("@/lib/session/cookie");
    const setCookie = setSidCookie("abc123", 86400);
    expect(setCookie).toContain("sid=abc123");
    expect(setCookie).not.toContain("Secure");
    expect(setCookie).not.toContain("Partitioned");
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("SameSite=Strict");
    expect(setCookie).toContain("Path=/");
  });
});

describe("sealAuthState / unsealAuthState", () => {
  const payload = {
    code_verifier: "v".repeat(43),
    state: "s1",
    nonce: "n1",
    return_to: "/listings",
  };

  it("roundtrips, and the seal does not show the verifier or nonce", async () => {
    const { sealAuthState, unsealAuthState } = await import("@/lib/session/cookie");
    const sealed = await sealAuthState(payload);
    expect(sealed).not.toContain(payload.code_verifier);
    expect(await unsealAuthState(sealed)).toEqual(payload);
  });

  it("returns null on a tampered seal", async () => {
    const { sealAuthState, unsealAuthState } = await import("@/lib/session/cookie");
    const sealed = await sealAuthState(payload);
    const i = Math.floor(sealed.length / 2);
    const tampered = `${sealed.slice(0, i)}${sealed[i] === "A" ? "B" : "A"}${sealed.slice(i + 1)}`;
    expect(await unsealAuthState(tampered)).toBeNull();
  });

  it("returns null on garbage and on the old HMAC format", async () => {
    const { unsealAuthState } = await import("@/lib/session/cookie");
    expect(await unsealAuthState("nodothere")).toBeNull();
    expect(await unsealAuthState("eyJ4IjoxfQ.bWFj")).toBeNull();
  });
});
