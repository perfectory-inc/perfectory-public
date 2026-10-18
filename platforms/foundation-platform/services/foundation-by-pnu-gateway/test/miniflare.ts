import { Miniflare, type MiniflareOptions } from "miniflare";

/// The ports the fetch standard refuses to connect to (https://fetch.spec.whatwg.org/#bad-port);
/// undici, which `dispatchFetch` uses, refuses exactly these with "bad port".
const BAD_PORTS = new Set([
  1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23, 25, 37, 42, 43, 53, 69, 77, 79, 87, 95, 101, 102, 103,
  104, 109, 110, 111, 113, 115, 117, 119, 123, 135, 137, 139, 143, 161, 179, 389, 427, 465, 512, 513,
  514, 515, 526, 530, 531, 532, 540, 548, 554, 556, 563, 587, 601, 636, 989, 990, 993, 995, 1719,
  1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061, 6000, 6566, 6665, 6666, 6667, 6668, 6669, 6679,
  6697, 10080,
]);

/// A Miniflare whose entry port `dispatchFetch` can reach. Miniflare listens on a port the OS picks,
/// and a Windows dynamic port range can start at 1024 (a workstation's: 1024-15000, 2026-10-18), so
/// 4 runs in 30 of the suite drew a bad port such as 6000 or 6665 and failed with "fetch failed:
/// bad port". A runtime on such a port is disposed and started again; Linux (32768-60999) never
/// draws one.
export async function startMiniflare(options: MiniflareOptions): Promise<Miniflare> {
  for (let attempt = 1; attempt <= 20; attempt += 1) {
    const runtime = new Miniflare(options);
    const entry = await runtime.ready;
    if (!BAD_PORTS.has(Number(entry.port))) return runtime;
    await runtime.dispose();
  }
  throw new Error(`Miniflare drew a port the fetch standard refuses 20 times in a row`);
}
