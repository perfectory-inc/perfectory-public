// Planted-failure tests for the npm override contract (root ADR-0158). Every check the tool
// makes is shown to refuse a tree that breaks it; the fixtures are throwaway git
// repositories, never this checkout.
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, describe, test } from "node:test";
import {
  CONTRACT_PATH,
  ContractError,
  TECHNOLOGY_CONTRACT_PATH,
  check,
  compareVersions,
  fixedVersionFor,
  main,
  proposeFromReport,
  readBaselineGroups,
  readLockfile,
  render,
  trackedTrees,
  validateContract,
} from "./security-overrides.mjs";
import { execFileSync } from "node:child_process";

// A hook hands its children a binding to the real repository; a fixture must never use it
// (scripts/guard/lib/fixture-repo.sh holds the incident).
for (const name of ["GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"]) delete process.env[name];

const TECHNOLOGY = {
  manifest_exact_pins: { node: "24.20.0", pnpm: "9.12.0", vite: "8.2.2" },
  container_images: { "node:24.20.0-bookworm": `sha256:${"a".repeat(64)}` },
};

const SECURITY_WS = {
  kind: "security",
  package: "ws",
  version: "8.21.0",
  advisories: ["GHSA-96hv-2xvq-fx4p"],
  reason: "fixture",
  date: "2026-10-01",
};
const PIN_VITE = { kind: "technology-pin", package: "vite", version_from: "vite", reason: "fixture", date: "2026-10-01" };

function lockfile({ overrides = {}, packages = [] }) {
  const lines = ["lockfileVersion: '9.0'", "", "settings:", "  autoInstallPeers: true", ""];
  if (Object.keys(overrides).length) {
    lines.push("overrides:");
    for (const [key, value] of Object.entries(overrides)) lines.push(`  '${key}': ${value}`);
    lines.push("");
  }
  lines.push("importers:", "", "  .:", "    dependencies: {}", "", "packages:", "");
  for (const id of packages) lines.push(`  ${id.startsWith("@") ? `'${id}'` : id}:`, "    resolution: {integrity: sha512-x}", "");
  lines.push("snapshots:", "");
  for (const id of packages) lines.push(`  ${id.startsWith("@") ? `'${id}'` : id}: {}`, "");
  return lines.join("\n");
}

const roots = [];
afterEach(() => {
  while (roots.length) rmSync(roots.pop(), { recursive: true, force: true });
});

function git(root, ...args) {
  return execFileSync("git", ["-C", root, ...args], { encoding: "utf8" });
}

function write(root, path, text) {
  mkdirSync(dirname(join(root, path)), { recursive: true });
  writeFileSync(join(root, path), typeof text === "string" ? text : `${JSON.stringify(text, null, 2)}\n`);
}

// trees: { "<dir>": { packages: [...], overrides: {...lock block}, manifest: {...} } }
function fixture({ entries, trees, track = Object.keys(trees) }) {
  const root = mkdtempSync(join(tmpdir(), "npm-overrides-"));
  roots.push(root);
  git(root, "init", "--quiet");
  write(root, CONTRACT_PATH, { schema_version: 1, purpose: "fixture", overrides: entries });
  write(root, TECHNOLOGY_CONTRACT_PATH, TECHNOLOGY);
  for (const [tree, spec] of Object.entries(trees)) {
    write(root, `${tree}/package.json`, spec.manifest ?? { name: tree.replaceAll("/", "-"), private: true });
    write(root, `${tree}/pnpm-lock.yaml`, lockfile(spec));
  }
  git(root, "add", "--", CONTRACT_PATH, TECHNOLOGY_CONTRACT_PATH, ...track.flatMap((tree) => [`${tree}/package.json`, `${tree}/pnpm-lock.yaml`]));
  return root;
}

const WS_RENDERED = { "ws@>=8.0.0 <9.0.0": "^8.21.0" };

// A tree as the refresh leaves it: manifest rendered, lock generated with the same block.
function consistent(extra = {}) {
  return {
    entries: [SECURITY_WS],
    trees: {
      "services/a": {
        packages: ["ws@8.21.3"],
        overrides: WS_RENDERED,
        manifest: { name: "a", private: true, pnpm: { overrides: WS_RENDERED } },
      },
      ...extra,
    },
  };
}

function errorsOf(root) {
  return check(root).errors.join("\n");
}

describe("check", () => {
  test("a rendered tree whose lock was refreshed passes", () => {
    assert.deepEqual(check(fixture(consistent())).errors, []);
  });

  test("a hand-edited override in package.json is refused", () => {
    const spec = consistent();
    spec.trees["services/a"].manifest.pnpm.overrides = { ...WS_RENDERED, katex: "0.19.0" };
    assert.match(errorsOf(fixture(spec)), /services\/a\/package\.json: pnpm\.overrides is not the rendering.*not from the contract: katex/);
  });

  test("a package.json missing a contract override is refused", () => {
    const spec = consistent();
    delete spec.trees["services/a"].manifest.pnpm;
    assert.match(errorsOf(fixture(spec)), /missing or different: ws@>=8\.0\.0 <9\.0\.0/);
  });

  test("a lockfile generated from other overrides is refused", () => {
    const spec = consistent();
    spec.trees["services/a"].overrides = { ws: "8.21.0" };
    assert.match(errorsOf(fixture(spec)), /services\/a\/pnpm-lock\.yaml: its overrides block is not the rendered overrides/);
  });

  test("a lockfile still resolving a version below the floor is refused", () => {
    const spec = consistent();
    spec.trees["services/a"].packages = ["ws@8.20.0", "ws@8.21.3"];
    assert.match(errorsOf(fixture(spec)), /resolves ws@8\.20\.0, below the floor 8\.21\.0 \(GHSA-96hv-2xvq-fx4p\)/);
  });

  test("a technology pin resolving another version is refused", () => {
    const root = fixture({
      entries: [PIN_VITE],
      trees: {
        web: { packages: ["vite@8.2.1"], overrides: { vite: "8.2.2" }, manifest: { name: "web", pnpm: { overrides: { vite: "8.2.2" } } } },
      },
    });
    assert.match(errorsOf(root), /resolves vite@8\.2\.1, not the pinned 8\.2\.2/);
  });

  test("a technology pin follows the technology contract, not a restated value", () => {
    const root = fixture({ entries: [PIN_VITE], trees: { web: { packages: ["vite@8.2.2"] } } });
    render(root);
    assert.deepEqual(JSON.parse(readFileSync(join(root, "web/package.json"), "utf8")).pnpm.overrides, { vite: "8.2.2" });
  });
});

describe("trees are derived from git", () => {
  test("an untracked lockfile is not a tree; tracking it makes it one", () => {
    const spec = consistent({ "services/b": { packages: ["ws@8.18.0"] } });
    const root = fixture({ ...spec, track: ["services/a"] });
    assert.deepEqual(trackedTrees(root), ["services/a"]);
    assert.deepEqual(check(root).errors, []);
    git(root, "add", "--", "services/b/package.json", "services/b/pnpm-lock.yaml");
    assert.deepEqual(trackedTrees(root), ["services/a", "services/b"]);
    assert.match(errorsOf(root), /services\/b\/package\.json: pnpm\.overrides is not the rendering/);
  });

  test("a tree that does not resolve the package gets no override", () => {
    const root = fixture(consistent({ "services/b": { packages: ["left-pad@1.3.0"] } }));
    assert.deepEqual(render(root), []);
    assert.deepEqual(check(root).errors, []);
  });

  test("a tree on another major of the package is left alone", () => {
    const root = fixture(consistent({ "services/b": { packages: ["ws@7.5.10"] } }));
    assert.deepEqual(render(root), []);
  });
});

describe("render", () => {
  test("writes the floor selector and is idempotent once the lock is lifted", () => {
    const root = fixture({
      entries: [SECURITY_WS, { ...SECURITY_WS, package: "katex", version: "0.19.0", from: "0.11.0" }],
      trees: { web: { packages: ["ws@8.18.0", "katex@0.16.21"], manifest: { name: "web", scripts: { test: "x" } } } },
    });
    assert.deepEqual(render(root), ["web"]);
    const manifest = JSON.parse(readFileSync(join(root, "web/package.json"), "utf8"));
    assert.deepEqual(Object.keys(manifest), ["name", "scripts", "pnpm"]);
    assert.deepEqual(manifest.pnpm.overrides, { "katex@>=0.11.0 <0.20.0": "^0.19.0", "ws@>=8.0.0 <9.0.0": "^8.21.0" });
    // What the refresh produces: lifted versions, the same block. Rendering must not move.
    write(root, "web/pnpm-lock.yaml", lockfile({ packages: ["ws@8.21.3", "katex@0.19.0"], overrides: manifest.pnpm.overrides }));
    assert.deepEqual(render(root), []);
    assert.deepEqual(check(root).errors, []);
  });

  test("removes the overrides key when nothing applies and keeps other pnpm settings", () => {
    const root = fixture({
      entries: [SECURITY_WS],
      trees: { web: { packages: ["left-pad@1.3.0"], manifest: { name: "web", pnpm: { onlyBuiltDependencies: ["esbuild"], overrides: { ws: "8.0.0" } } } } },
    });
    render(root);
    assert.deepEqual(JSON.parse(readFileSync(join(root, "web/package.json"), "utf8")).pnpm, { onlyBuiltDependencies: ["esbuild"] });
  });
});

describe("contract validation", () => {
  const trees = ["services/a"];
  const refuse = (entries, pattern) =>
    assert.throws(() => validateContract({ schema_version: 1, overrides: entries }, TECHNOLOGY, trees), (error) => error instanceof ContractError && pattern.test(error.message));

  test("accepts the fixture entries", () => {
    assert.equal(validateContract({ schema_version: 1, overrides: [SECURITY_WS, PIN_VITE] }, TECHNOLOGY, trees).length, 2);
  });
  test("unknown field", () => refuse([{ ...SECURITY_WS, pinned: true }], /unknown field 'pinned'/));
  test("security entry without advisories", () => refuse([{ ...SECURITY_WS, advisories: [] }], /names its advisories/));
  test("malformed advisory id", () => refuse([{ ...SECURITY_WS, advisories: ["GHSA-nope"] }], /not a GHSA or CVE id/));
  test("unsorted advisories", () => refuse([{ ...SECURITY_WS, advisories: ["GHSA-96hv-2xvq-fx4p", "GHSA-58qx-3vcg-4xpx"] }], /unique and sorted/));
  test("missing reason", () => refuse([{ ...SECURITY_WS, reason: " " }], /reason is required/));
  test("from restating the default", () => refuse([{ ...SECURITY_WS, from: "8.0.0" }], /is the default/));
  test("from above the version", () => refuse([{ ...SECURITY_WS, from: "8.22.0" }], /is above version/));
  test("overlapping ranges of one package", () => refuse([SECURITY_WS, { ...SECURITY_WS, version: "8.22.0" }], /ranges overlap/));
  test("a technology pin restating a version", () => refuse([{ ...PIN_VITE, version: "8.2.2" }], /takes its version from/));
  test("a technology pin naming an unknown key", () => refuse([{ ...PIN_VITE, version_from: "vitepress" }], /not a manifest_exact_pins key/));
  test("a tree with no tracked lockfile", () => refuse([{ ...SECURITY_WS, trees: ["services/z"] }], /holds no tracked pnpm-lock\.yaml/));
  test("schema version", () => assert.throws(() => validateContract({ schema_version: 2, overrides: [] }, TECHNOLOGY, trees), ContractError));
});

describe("lockfile reader", () => {
  test("reads quoted keys, scoped packages and the overrides block", () => {
    const lock = readLockfile(lockfile({ overrides: { "ws@>=8.0.0 <9.0.0": "^8.21.0" }, packages: ["@grpc/grpc-js@1.14.5", "ws@8.21.3"] }));
    assert.deepEqual([...lock.overrides], [["ws@>=8.0.0 <9.0.0", "^8.21.0"]]);
    assert.deepEqual([...lock.packages.get("@grpc/grpc-js")], ["1.14.5"]);
    assert.deepEqual([...lock.packages.get("ws")], ["8.21.3"]);
  });
  test("refuses a lockfile version it does not understand", () => {
    assert.throws(() => readLockfile("lockfileVersion: '6.0'\n"), ContractError);
  });
  test("orders prereleases below the release", () => {
    assert.equal(compareVersions("1.0.0-rc.1", "1.0.0"), -1);
    assert.equal(compareVersions("1.0.0-rc.2", "1.0.0-rc.10"), -1);
  });
});

describe("propose", () => {
  const vulnerability = (id, events) => ({ id, aliases: [], affected: [{ package: { ecosystem: "npm", name: "sharp" }, ranges: [{ type: "SEMVER", events }] }] });
  const report = (name, version, vulnerabilities) => ({
    results: [{
      source: { path: "/repo/services/a/pnpm-lock.yaml", type: "lockfile" },
      packages: [{ package: { name, version, ecosystem: "npm" }, vulnerabilities, groups: [{ ids: vulnerabilities.map((v) => v.id) }] }],
    }],
  });
  const contract = { schema_version: 1, purpose: "fixture", overrides: [{ ...SECURITY_WS, package: "sharp", version: "0.35.4", from: "0.0.0" }] };

  test("raises an existing floor to the first fixed release and records the advisory", () => {
    const { contract: next, changes } = proposeFromReport(contract, report("sharp", "0.35.4", [vulnerability("GHSA-wq5f-xc86-pv6w", [{ introduced: "0" }, { fixed: "0.35.5" }])]), new Set(), "2026-10-07");
    assert.equal(changes.length, 1);
    assert.deepEqual(next.overrides[0], { ...contract.overrides[0], version: "0.35.5", advisories: ["GHSA-96hv-2xvq-fx4p", "GHSA-wq5f-xc86-pv6w"], date: "2026-10-07" });
    assert.equal(contract.overrides[0].version, "0.35.4", "the input contract is not mutated");
  });

  test("adds a new floor on the affected line", () => {
    const vuln = { id: "GHSA-mwcw-c2x4-8c55", aliases: [], affected: [{ package: { ecosystem: "npm", name: "nanoid" }, ranges: [{ type: "SEMVER", events: [{ introduced: "4.0.0" }, { fixed: "5.0.9" }] }] }] };
    const { contract: next } = proposeFromReport(contract, report("nanoid", "5.0.8", [vuln]), new Set(), "2026-10-07");
    const entry = next.overrides.find((item) => item.package === "nanoid");
    assert.deepEqual(entry, { kind: "security", package: "nanoid", version: "5.0.9", advisories: ["GHSA-mwcw-c2x4-8c55"], reason: entry.reason, date: "2026-10-07" });
    assert.equal(validateContract(next, TECHNOLOGY, []).length, 2);
  });

  test("ignores an advisory group the OSV baseline already accepts", () => {
    const baseline = readBaselineGroups("# osv-vulnerability-baseline-v1\nnpm\tsharp\tGHSA-wq5f-xc86-pv6w\t1\treason\n");
    const { changes } = proposeFromReport(contract, report("sharp", "0.35.4", [vulnerability("GHSA-wq5f-xc86-pv6w", [{ introduced: "0" }, { fixed: "0.35.5" }])]), baseline, "2026-10-07");
    assert.deepEqual(changes, []);
  });

  test("reports an advisory without a fixed release instead of inventing a floor", () => {
    const { changes, unfixable } = proposeFromReport(contract, report("sharp", "0.35.4", [vulnerability("GHSA-wq5f-xc86-pv6w", [{ introduced: "0" }, { last_affected: "0.35.4" }])]), new Set(), "2026-10-07");
    assert.deepEqual(changes, []);
    assert.match(unfixable.join(), /sharp@0\.35\.4 GHSA-wq5f-xc86-pv6w/);
  });

  test("picks the fix of the range that contains the resolved version", () => {
    const vuln = { id: "GHSA-3wwx-pv8p-q78v", affected: [{ package: { ecosystem: "npm", name: "undici" }, ranges: [{ type: "SEMVER", events: [{ introduced: "6.25.0" }, { fixed: "6.28.1" }, { introduced: "7.28.0" }, { fixed: "7.29.1" }] }] }] };
    assert.equal(fixedVersionFor(vuln, "undici", "6.26.0"), "6.28.1");
    assert.equal(fixedVersionFor(vuln, "undici", "7.28.5"), "7.29.1");
    assert.equal(fixedVersionFor(vuln, "undici", "7.29.1"), null);
  });

  test("the command writes nothing and exits 3 when there is nothing to raise", () => {
    const root = fixture(consistent());
    write(root, "report.json", { results: [] });
    const before = readFileSync(join(root, CONTRACT_PATH), "utf8");
    const log = console.log;
    console.log = () => {};
    try {
      assert.equal(main(["propose", "--root", root, "--osv-report", join(root, "report.json"), "--today", "2026-10-07"]), 3);
    } finally {
      console.log = log;
    }
    assert.equal(readFileSync(join(root, CONTRACT_PATH), "utf8"), before);
  });
});
