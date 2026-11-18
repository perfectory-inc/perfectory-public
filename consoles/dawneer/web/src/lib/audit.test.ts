import { describe, expect, it } from "vitest";

import { actorText, appliedViaHref, auditQuery, changedKeys } from "./audit";

describe("auditQuery", () => {
  it("leaves empty filters out and turns KST days into instants", () => {
    const query = auditQuery({ actor: "me", objectType: " job ", objectId: "", from: "2026-11-19", to: "2026-11-19" });
    expect(query.get("actor")).toBe("me");
    expect(query.get("object_type")).toBe("job");
    expect(query.has("object_id")).toBe(false);
    // 2026-11-19 00:00 KST is 11-18 15:00 UTC; the end of the day is the next day's start.
    expect(query.get("from")).toBe("2026-11-18T15:00:00.000Z");
    expect(query.get("to")).toBe("2026-11-19T15:00:00.000Z");
  });

  it("ignores a day that is not YYYY-MM-DD and carries extra fields", () => {
    const query = auditQuery({ from: "yesterday" }, { limit: "100" });
    expect(query.has("from")).toBe(false);
    expect(query.get("limit")).toBe("100");
  });
});

describe("changedKeys", () => {
  it("lists only keys whose values differ, in key order", () => {
    expect(changedKeys({ max: 2, same: "x" }, { max: 3, same: "x", added: true })).toEqual([
      { key: "added", before: null, after: "true" },
      { key: "max", before: "2", after: "3" },
    ]);
    expect(changedKeys(null, { lanes: "a" })).toEqual([{ key: "lanes", before: null, after: "a" }]);
  });
});

describe("actorText", () => {
  it("prefers the staff account, then the source's own id", () => {
    expect(actorText({ actor_kind: "staff", actor_staff_id: "00000000-0000-4000-8000-0000000000aa" })).toBe(
      "직원 00000000",
    );
    expect(actorText({ actor_kind: "unlinked", actor_external: "unix:1000@ai-server" })).toBe("unix:1000@ai-server");
    expect(actorText({ actor_kind: "system" })).toBe("시스템");
  });
});

describe("appliedViaHref", () => {
  it("links a pull request and a deployed commit, nothing else", () => {
    expect(appliedViaHref("pull_request", "1437")).toBe("https://github.com/perfectory-inc/perfectory-public/pull/1437");
    expect(appliedViaHref("deploy", "95b3e810")).toBe(
      "https://github.com/perfectory-inc/perfectory-public/commit/95b3e810",
    );
    expect(appliedViaHref("api", "x")).toBeNull();
    expect(appliedViaHref("pull_request", "1;evil")).toBeNull();
  });
});
