import { Miniflare } from "miniflare";
import { expect, it } from "vitest";
import { startMiniflare } from "./miniflare";

// The suites' one flaky failure (2026-10-18: 4 runs in 30 on a workstation) was a Miniflare that the
// OS had put on a port the fetch standard refuses. Pinned to one such port, it fails every time.
const worker = {
  compatibilityDate: "2026-04-26",
  modules: true,
  script: "export default { fetch() { return new Response('ok'); } };",
  port: 6000,
};

it("a Miniflare on a port the fetch standard refuses cannot be reached by dispatchFetch", async () => {
  const runtime = new Miniflare(worker);
  try {
    const failure = await runtime.dispatchFetch("https://example.test/").catch((error: unknown) => error);
    expect(String((failure as { cause?: unknown }).cause)).toContain("bad port");
  } finally {
    await runtime.dispose();
  }
});

it("startMiniflare never hands out a runtime on such a port", async () => {
  await expect(startMiniflare(worker)).rejects.toThrow("refuses 20 times");
}, 60_000);
