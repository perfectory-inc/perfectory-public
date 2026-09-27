import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

// The same synthetic archive proof runs in local workerd with in-memory R2 and Cache API.
const vitestBin = fileURLToPath(new URL("../node_modules/vitest/vitest.mjs", import.meta.url));
const result = spawnSync(process.execPath, [vitestBin, "run", "test/tile-runtime.test.ts"], {
  cwd: new URL("..", import.meta.url),
  stdio: "inherit",
  env: { ...process.env, WRANGLER_SEND_METRICS: "false" },
});
if (result.error !== undefined) throw result.error;
if (result.status !== 0) throw new Error(`local workerd proof exited ${String(result.status)}`);
