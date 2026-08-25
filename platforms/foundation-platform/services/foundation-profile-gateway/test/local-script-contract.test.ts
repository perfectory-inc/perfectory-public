import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("local Wrangler proof contract", () => {
  it("local proof is bounded and cannot touch remote R2", async () => {
    const source = await readFile(new URL("../scripts/verify-local.mjs", import.meta.url), "utf8");

    expect(source).toContain("mkdtemp(prefix)");
    expect(source).toContain('"r2",');
    expect(source).toContain('"object",');
    expect(source).toContain('"put",');
    expect(source).toContain('"--local",');
    expect(source).toContain('"--persist-to",');
    expect(source).toContain("gateway.object_key.root");
    expect(source).toContain("bronze/vworld/2026/raw.jsonl");
    expect(source).toMatch(/wranglerBin,[\s\S]{0,80}"dev",[\s\S]{0,80}"--local"/);
    expect(source).toContain('"--persist-to",');
    expect(source).toContain('"--var",');
    expect(source).toContain("gateway.allowed_origins_binding");
    expect(source).toContain("attempt <= 60");
    expect(source).toContain('server.kill("SIGTERM")');
    expect(source).toContain('spawnSync("taskkill"');
    expect(source).toContain("spawnSync");
    expect(source).toContain("timeout: 60_000");
    expect(source).toContain("If-None-Match");
    expect(source).toContain("https://attacker.example.test");
    expect(source).not.toContain("--remote");
    expect(source).not.toContain('"deploy"');
  });
});
