import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError, DawneerClient, loadSession, NotSignedIn, type Fetch, type SessionInfo } from "./client";

const SESSION: SessionInfo = { sub: "1", name: "Synthetic", email: "s@example.test", csrf: "csrf-value" };
const ITEM = "00000000-0000-5000-8000-000000000001";

function recording(status = 200, body: unknown = {}) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fetchImpl: Fetch = async (url, init) => {
    calls.push({ url: String(url), init: init ?? {} });
    return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
  };
  return { calls, fetchImpl };
}

describe("DawneerClient", () => {
  it("reads without the CSRF value and only through the server's relay", async () => {
    const { calls, fetchImpl } = recording(200, { items: [], next_after: null });
    await new DawneerClient(SESSION, fetchImpl).listItems({ status: "pending", prefix: "99999", undecidedOnly: true });
    expect(calls[0]?.url).toBe("/api/foundation/lineage-review/items?status=pending&prefix=99999&undecided_only=true&limit=50");
    expect(new Headers(calls[0]?.init.headers).get("x-dawneer-csrf")).toBeNull();
  });

  it("sends the CSRF value and the idempotency key with a decision", async () => {
    const { calls, fetchImpl } = recording(200, { disposition: "recorded", requires_approval: false });
    await new DawneerClient(SESSION, fetchImpl).decide(
      ITEM,
      { outcome: "not_a_link", reason_code: "site_survey", note: "", evidence_etag: "e".repeat(64) },
      "decide-key-0001",
    );
    const headers = new Headers(calls[0]?.init.headers);
    expect(calls[0]?.init.method).toBe("POST");
    expect(headers.get("x-dawneer-csrf")).toBe("csrf-value");
    expect(headers.get("idempotency-key")).toBe("decide-key-0001");
  });

  it("a dry run carries no idempotency key and stores nothing", async () => {
    const { calls, fetchImpl } = recording(200, { disposition: "dry_run_passed", requires_approval: true });
    const result = await new DawneerClient(SESSION, fetchImpl).dryRun(ITEM, {
      outcome: "unsure",
      note: "",
      evidence_etag: "e".repeat(64),
    });
    expect(calls[0]?.url.endsWith("/decisions?dry_run=true")).toBe(true);
    expect(new Headers(calls[0]?.init.headers).get("idempotency-key")).toBeNull();
    expect(result.requires_approval).toBe(true);
  });

  it("turns 401 into NotSignedIn and other refusals into their message", async () => {
    await expect(new DawneerClient(SESSION, recording(401).fetchImpl).getItem(ITEM)).rejects.toBeInstanceOf(NotSignedIn);
    const conflict = recording(409, { error: "the evidence changed since it was read" });
    const refused = new DawneerClient(SESSION, conflict.fetchImpl).claim(ITEM);
    await expect(refused).rejects.toBeInstanceOf(ApiError);
    await expect(refused).rejects.toMatchObject({ status: 409, message: "the evidence changed since it was read" });
  });
});

describe("the default fetch", () => {
  afterEach(() => vi.unstubAllGlobals());

  // A browser's fetch throws "Illegal invocation" when `this` is anything but the window or
  // undefined; this stub does the same, so a default that is called as a method fails here.
  function strictFetch() {
    return vi.fn(function (this: unknown) {
      if (this !== undefined && this !== globalThis) throw new TypeError("Illegal invocation");
      return Promise.resolve(new Response(JSON.stringify({ items: [], next_after: null }), { status: 200 }));
    });
  }

  it("is called unbound by the client and by loadSession", async () => {
    const stub = strictFetch();
    vi.stubGlobal("fetch", stub);
    await new DawneerClient(SESSION).listItems({});
    await loadSession();
    expect(stub).toHaveBeenCalledTimes(2);
  });
});
