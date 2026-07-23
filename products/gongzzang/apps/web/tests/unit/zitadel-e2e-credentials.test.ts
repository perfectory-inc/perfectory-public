import { describe, expect, it } from "vitest";
import { requireZitadelE2eCredentials } from "../e2e/zitadel-e2e-credentials";

describe("requireZitadelE2eCredentials", () => {
  it("returns explicitly injected credentials", () => {
    expect(
      requireZitadelE2eCredentials({
        ZITADEL_E2E_USERNAME: "synthetic-admin@example.invalid",
        ZITADEL_E2E_PASSWORD: "synthetic-password",
      }),
    ).toEqual({
      username: "synthetic-admin@example.invalid",
      password: "synthetic-password",
    });
  });

  it.each([
    "ZITADEL_E2E_USERNAME",
    "ZITADEL_E2E_PASSWORD",
  ])("rejects a missing %s instead of using a known default", (missing) => {
    const env: Record<string, string> = {
      ZITADEL_E2E_USERNAME: "synthetic-admin@example.invalid",
      ZITADEL_E2E_PASSWORD: "synthetic-password",
    };
    delete env[missing];

    expect(() => requireZitadelE2eCredentials(env)).toThrow(`${missing} is required`);
  });
});
