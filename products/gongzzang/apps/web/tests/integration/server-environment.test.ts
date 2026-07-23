import { describe, expect, it } from "vitest";
import { env } from "@/lib/env";

describe("server integration environment", () => {
  it("keeps server-only configuration available", () => {
    expect(typeof window).toBe("undefined");
    expect(env.REDIS_URL).toBe(process.env.REDIS_URL);
  });
});
