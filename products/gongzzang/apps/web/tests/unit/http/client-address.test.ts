import { afterEach, describe, expect, it, vi } from "vitest";
import { clientAddress } from "@/lib/http/client-address";

const headers = (init: Record<string, string>) => new Headers(init);

describe("client address", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("ignores the caller-written first X-Forwarded-For entry and keeps the hop the nearest proxy appended", () => {
    expect(clientAddress(headers({ "x-forwarded-for": "6.6.6.6, 203.0.113.10" }))).toBe(
      "203.0.113.10",
    );
    expect(clientAddress(headers({ "x-forwarded-for": "7.7.7.7, 203.0.113.10" }))).toBe(
      "203.0.113.10",
    );
  });

  it("falls back to unknown without any forwarding header", () => {
    expect(clientAddress(headers({}))).toBe("unknown");
  });

  it("does not trust CF-Connecting-IP outside the Cloudflare Workers runtime", () => {
    expect(
      clientAddress(headers({ "cf-connecting-ip": "6.6.6.6", "x-forwarded-for": "203.0.113.10" })),
    ).toBe("203.0.113.10");
  });

  it("uses CF-Connecting-IP on Cloudflare Workers, whatever X-Forwarded-For says", () => {
    vi.stubGlobal("navigator", { userAgent: "Cloudflare-Workers" });
    expect(
      clientAddress(
        headers({ "cf-connecting-ip": "198.51.100.7", "x-forwarded-for": "6.6.6.6, 198.51.100.7" }),
      ),
    ).toBe("198.51.100.7");
  });
});
