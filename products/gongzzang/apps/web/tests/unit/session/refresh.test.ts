// @vitest-environment node

import { beforeEach, describe, expect, it, vi } from "vitest";

const store = vi.hoisted(() => ({
  getSession: vi.fn(),
  deleteSession: vi.fn(),
  refreshSession: vi.fn(),
  REFRESH_TTL_SEC: 2_592_000,
}));
const refreshTokens = vi.hoisted(() => vi.fn());
const redisSet = vi.hoisted(() => vi.fn());
const emitAuthEvent = vi.hoisted(() => vi.fn());

vi.mock("@/lib/session/store", () => store);
vi.mock("@/lib/oidc", () => ({ refreshTokens }));
vi.mock("@/lib/session/redis", () => ({ getRedis: () => ({ set: redisSet }) }));
vi.mock("@/lib/auth/internal-event", () => ({ emitAuthEvent }));
vi.mock("@/lib/session/single-flight", () => ({
  withLock: async (_key: string, _ttl: number, run: () => Promise<unknown>) => run(),
}));
vi.mock("@/lib/env", () => ({
  env: { ZITADEL_ISSUER: "https://issuer.example.com", ZITADEL_CLIENT_ID: "client" },
}));

const { freshSession } = await import("@/lib/session/refresh");

const now = () => Math.floor(Date.now() / 1000);
const session = (exp: number) => ({
  sub: "user_1",
  jti: "jti_old",
  role: "Broker",
  access_token: "at_old",
  refresh_token: "rt_old",
  id_token: "it_old",
  exp,
});

describe("fresh session", () => {
  beforeEach(() => {
    for (const fn of [store.getSession, store.deleteSession, store.refreshSession]) fn.mockReset();
    refreshTokens.mockReset();
    redisSet.mockReset();
    emitAuthEvent.mockReset();
  });

  it("uses a live access token as it is, without asking the issuer", async () => {
    const live = session(now() + 300);
    expect(await freshSession("sid_1", live)).toBe(live);
    expect(refreshTokens).not.toHaveBeenCalled();
  });

  it("refreshes an expired access token before it is forwarded, and denies the old jti", async () => {
    store.getSession.mockResolvedValue(session(now() - 5));
    refreshTokens.mockResolvedValue({
      sub: "user_1",
      jti: "jti_new",
      role: "Broker",
      access_token: "at_new",
      refresh_token: "rt_new",
      id_token: "it_new",
      expires_in: 300,
    });

    const fresh = await freshSession("sid_1", session(now() - 5));

    expect(refreshTokens).toHaveBeenCalledWith(expect.objectContaining({ refresh_token: "rt_old" }));
    expect(fresh?.access_token).toBe("at_new");
    expect(store.refreshSession).toHaveBeenCalledWith(
      "sid_1",
      expect.objectContaining({ access_token: "at_new", jti: "jti_new" }),
      store.REFRESH_TTL_SEC,
    );
    expect(redisSet).toHaveBeenCalledWith("jti:deny:jti_old", "1", "EX", 1);
  });

  it("refreshes a token that ends within the skew", async () => {
    store.getSession.mockResolvedValue(session(now() + 10));
    refreshTokens.mockRejectedValue(new Error("invalid_grant"));

    expect(await freshSession("sid_1", session(now() + 10))).toBeNull();
    expect(refreshTokens).toHaveBeenCalledOnce();
  });

  it("ends the session when the refresh token no longer works", async () => {
    store.getSession.mockResolvedValue(session(now() - 5));
    refreshTokens.mockRejectedValue(new Error("invalid_grant"));

    expect(await freshSession("sid_1", session(now() - 5))).toBeNull();
    expect(store.deleteSession).toHaveBeenCalledWith("sid_1");
    expect(emitAuthEvent).toHaveBeenCalledWith("RefreshFailed", expect.anything());
  });

  it("does not refresh twice when another request already refreshed while this one waited", async () => {
    const refreshed = session(now() + 300);
    store.getSession.mockResolvedValue(refreshed);

    expect(await freshSession("sid_1", session(now() - 5))).toBe(refreshed);
    expect(refreshTokens).not.toHaveBeenCalled();
  });
});
