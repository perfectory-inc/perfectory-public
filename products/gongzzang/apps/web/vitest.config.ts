import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// Two lanes, one declaration. `test.projects` is Vitest's own mechanism for this
// (it replaced the deprecated `test.workspace`), so the shared plugin, alias and
// setup are stated once instead of copied into a second config file that can
// drift from this one.
//
// The split is by directory and nothing else. It used to be by directory *plus* a
// list of five files named in both configs — an exclude here and a matching
// include there — because those five exercise a real Redis while living under
// `tests/unit/`. Mirrored lists are what ADR-0011 남은 부채 2 was about: drop a
// name from one side without adding it to the other and the file runs in no lane
// at all, with both lanes green. They now live under `tests/integration/`, so
// there is no list left to keep in step.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": new URL("./", import.meta.url).pathname,
    },
  },
  test: {
    globals: true,
    setupFiles: ["./tests/setup.ts"],
    projects: [
      {
        extends: true,
        test: {
          // The CI unit step (`pnpm turbo test` -> `vitest run --project unit`):
          // deterministic, mocked, no backing services.
          name: "unit",
          environment: "happy-dom",
          include: ["tests/unit/**/*.test.{ts,tsx}", "lib/**/*.test.{ts,tsx}"],
          // Keep filesystem-scanning contract tests deterministic on Windows and CI
          // runners. The default worker count can starve module imports long enough
          // to trip Vitest's 5s timeout.
          maxWorkers: 2,
        },
      },
      {
        extends: true,
        test: {
          // Redis-backed request/session flows. Run by `pnpm test:integration`
          // against the service container, never by the unit step.
          name: "integration",
          environment: "node",
          include: ["tests/integration/**/*.test.{ts,tsx}"],
        },
      },
    ],
  },
});
