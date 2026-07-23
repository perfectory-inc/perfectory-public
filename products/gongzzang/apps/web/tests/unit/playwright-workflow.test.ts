import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

// Workflows moved to the monorepo root .github/ and gained an area prefix
// (Task 1 of the governance overhaul). Repo root is six levels above this file.
const FRONTEND_WORKFLOW = resolve(
  __dirname,
  "../../../../../../.github/workflows/gongzzang-frontend.yml",
);
const GENERAL_CI_WORKFLOW = resolve(
  __dirname,
  "../../../../../../.github/workflows/gongzzang-ci.yml",
);
const DB_MIGRATIONS_WORKFLOW = resolve(
  __dirname,
  "../../../../../../.github/workflows/gongzzang-db-migrations.yml",
);
// The two-stage test contract moved out of the workflow YAML into the single
// verification SSOT (ADR-0004). This test now anchors that invariant to xtask.
const XTASK_MAIN = resolve(__dirname, "../../../../../../tools/xtask/src/main.rs");

const REQUIRED_BUILD_ENV = [
  "NEXT_PUBLIC_API_BASE_URL",
  "NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL",
  "NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID",
  "ZITADEL_ISSUER",
  "ZITADEL_CLIENT_ID",
  "ZITADEL_AUDIENCE",
  "ZITADEL_REDIRECT_URI",
  "REDIS_URL",
  "SESSION_SECRET",
  "INTERNAL_AUTH_SECRET",
  "FOUNDATION_PLATFORM_WEBHOOK_SECRET",
] as const;

describe("frontend workflow ownership", () => {
  it("keeps the production frontend gate in one workflow", () => {
    const frontendWorkflow = readFileSync(FRONTEND_WORKFLOW, "utf8");
    const generalCiWorkflow = readFileSync(GENERAL_CI_WORKFLOW, "utf8");

    expect(generalCiWorkflow).not.toMatch(/^ {2}frontend:\s*$/m);
    expect(frontendWorkflow).toContain("run: pnpm turbo build");
    for (const variable of REQUIRED_BUILD_ENV) {
      expect(frontendWorkflow).toContain(`${variable}:`);
    }
  });
});

describe("Rust test workflow ownership", () => {
  it("runs database integration tests only in the migrated PostGIS workflow", () => {
    const generalCiWorkflow = readFileSync(GENERAL_CI_WORKFLOW, "utf8");
    const dbMigrationsWorkflow = readFileSync(DB_MIGRATIONS_WORKFLOW, "utf8");
    const xtaskMain = readFileSync(XTASK_MAIN, "utf8");

    // General CI verifies through the single SSOT (ADR-0004), never raw cargo
    // test. The two-stage split (exclude gongzzang-persistence from the workspace
    // pass, run its non-DB suite separately) lives in xtask, not the YAML.
    expect(generalCiWorkflow).toContain("cargo xtask verify gongzzang");
    expect(generalCiWorkflow).not.toMatch(/^\s*-\s+run:\s+cargo test\b/m);

    // The DB-integration lane needs a live PostGIS service, so it must run ONLY
    // in the migrated PostGIS workflow — never in general CI.
    expect(generalCiWorkflow).not.toMatch(/--features integration/);
    expect(dbMigrationsWorkflow).toContain(
      "cargo test -p gongzzang-persistence --features integration -- --test-threads=1",
    );

    // The two-stage contract's SSOT home: xtask marks gongzzang two_stage_test.
    // If someone flips this off, gongzzang-persistence DB tests would run in the
    // workspace pass without a database — this assertion guards that regression.
    expect(xtaskMain).toMatch(/slug:\s*"gongzzang"[\s\S]*?two_stage_test:\s*true/);
  });
});

describe("frontend workflow Playwright runtime", () => {
  it("does not override the Playwright-owned local callback URL", () => {
    const workflow = readFileSync(FRONTEND_WORKFLOW, "utf8");

    expect(workflow).not.toContain("ZITADEL_REDIRECT_URI: http://localhost:3000/api/auth/callback");
    expect(workflow).toContain("Playwright runtime derives the local callback URL");
  });
});
