import { describe, expect, it } from "vitest";
import { grantedRoles } from "@/lib/oidc";
import { hasAnyRole } from "@/lib/session/roles";

const CLAIM = "urn:zitadel:iam:org:project:roles";

describe("granted roles", () => {
  it("keeps every role in the Zitadel claim, sorted, whatever the key order", () => {
    const a = grantedRoles({ [CLAIM]: { Operator: { o1: "d" }, Broker: { o1: "d" } } });
    const b = grantedRoles({ [CLAIM]: { Broker: { o1: "d" }, Operator: { o1: "d" } } });
    expect(a).toEqual(["Broker", "Operator"]);
    expect(b).toEqual(a);
  });

  it("invents no role when the claim is absent or malformed", () => {
    expect(grantedRoles({})).toEqual([]);
    expect(grantedRoles({ [CLAIM]: null })).toEqual([]);
    expect(grantedRoles({ [CLAIM]: "Broker" })).toEqual([]);
    expect(grantedRoles({ [CLAIM]: ["Broker"] })).toEqual([]);
  });
});

describe("hasAnyRole", () => {
  it("passes when any required role is granted, regardless of which came first", () => {
    expect(hasAnyRole({ roles: ["Admin", "Broker"] }, ["Broker"])).toBe(true);
    expect(hasAnyRole({ roles: ["Broker", "Operator"] }, ["Admin", "Operator"])).toBe(true);
  });

  it("refuses a session with no matching role, including no role at all", () => {
    expect(hasAnyRole({ roles: ["Operator"] }, ["Broker"])).toBe(false);
    expect(hasAnyRole({ roles: [] }, ["Broker"])).toBe(false);
  });

  it("asks nothing when the policy requires no role", () => {
    expect(hasAnyRole({ roles: [] }, [])).toBe(true);
  });
});
