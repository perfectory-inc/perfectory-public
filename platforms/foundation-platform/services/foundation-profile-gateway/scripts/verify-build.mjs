import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const prefix = join(tmpdir(), "foundation-profile-gateway-build-");
const output = await mkdtemp(prefix);

try {
  const wranglerBin = fileURLToPath(
    new URL("../node_modules/wrangler/bin/wrangler.js", import.meta.url),
  );
  const result = spawnSync(
    process.execPath,
    [wranglerBin, "deploy", "--dry-run", "--outdir", output],
    {
      cwd: new URL("..", import.meta.url),
      encoding: "utf8",
      stdio: "inherit",
      env: { ...process.env, WRANGLER_SEND_METRICS: "false" },
    },
  );
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`wrangler dry-run exited ${String(result.status)}`);
} finally {
  const resolved = resolve(output);
  const safePrefix = `${resolve(tmpdir())}${sep}foundation-profile-gateway-build-`;
  if (!resolved.startsWith(safePrefix)) {
    throw new Error(`refusing to remove unexpected build path: ${resolved}`);
  }
  await rm(resolved, { recursive: true, force: true });
}
