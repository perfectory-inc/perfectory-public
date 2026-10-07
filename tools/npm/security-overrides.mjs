#!/usr/bin/env node
// One source for every pnpm override in the repository (root ADR-0158).
//
// tools/npm/security-overrides.contract.json is the only hand-written list. The trees it
// renders into are not listed anywhere: they are every directory holding a tracked
// pnpm-lock.yaml, asked of git. Each tree's package.json `pnpm.overrides` is generated from
// the contract (`render`) and `check` refuses a tree whose manifest, lockfile override block
// or resolved versions disagree with it. `propose` turns an OSV-Scanner report into a
// contract bump, which is how a new advisory becomes a change instead of a hand edit in
// seven manifests.
//
// Zero dependencies on purpose: it runs under any Node the trees' engines admit, in CI and
// beside the pinned toolchain container that regenerates the lockfiles (refresh-locks.sh).

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const CONTRACT_PATH = "tools/npm/security-overrides.contract.json";
export const TECHNOLOGY_CONTRACT_PATH = "tools/technology-versions.contract.json";
export const BASELINE_PATH = "tools/osv-vulnerability-baseline.tsv";
const KINDS = new Set(["security", "technology-pin"]);
const ADVISORY = /^(GHSA(-[23456789cfghjmpqrvwx]{4}){3}|CVE-\d{4}-\d{4,})$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const PACKAGE = /^(@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/;
const FIELDS = new Set(["kind", "package", "version", "version_from", "from", "advisories", "reason", "date", "trees"]);

export class ContractError extends Error {}

// ---------------------------------------------------------------- semver (the subset we need)

export function parseVersion(text) {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$/.exec(text);
  if (!match) return null;
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    pre: match[4] ? match[4].split(".") : [],
  };
}

function requireVersion(text, where) {
  const parsed = typeof text === "string" ? parseVersion(text) : null;
  if (!parsed) throw new ContractError(`${where}: '${text}' is not a semantic version`);
  return parsed;
}

export function compareVersions(a, b) {
  const left = typeof a === "string" ? parseVersion(a) : a;
  const right = typeof b === "string" ? parseVersion(b) : b;
  for (const key of ["major", "minor", "patch"]) {
    if (left[key] !== right[key]) return left[key] < right[key] ? -1 : 1;
  }
  if (left.pre.length === 0 || right.pre.length === 0) {
    if (left.pre.length === right.pre.length) return 0;
    return left.pre.length === 0 ? 1 : -1;
  }
  for (let index = 0; index < Math.max(left.pre.length, right.pre.length); index += 1) {
    const x = left.pre[index];
    const y = right.pre[index];
    if (x === undefined) return -1;
    if (y === undefined) return 1;
    if (x === y) continue;
    const xNumeric = /^\d+$/.test(x);
    const yNumeric = /^\d+$/.test(y);
    if (xNumeric && yNumeric) return Number(x) < Number(y) ? -1 : 1;
    if (xNumeric !== yNumeric) return xNumeric ? -1 : 1;
    return x < y ? -1 : 1;
  }
  return 0;
}

// A release line is what a caret range admits: one major, or one minor while the major is 0.
export function lineStart(version) {
  const v = requireVersion(version, "line");
  return v.major > 0 ? `${v.major}.0.0` : `0.${v.minor}.0`;
}

export function lineEnd(version) {
  const v = requireVersion(version, "line");
  return v.major > 0 ? `${v.major + 1}.0.0` : `0.${v.minor + 1}.0`;
}

function inRange(version, from, end) {
  return compareVersions(version, from) >= 0 && compareVersions(version, end) < 0;
}

// ---------------------------------------------------------------- contract

export function loadJson(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new ContractError(`cannot read ${path}: ${error.message}`);
  }
}

// Validates the contract and returns its entries with the derived selector and value.
//
// A security entry is a floor, not a pin: every dependency that declares a range inside
// [from, end of version's release line) is rewritten to ^version. pnpm matches an override
// selector against the *declared* range (it must be a subset), so the selector spans the
// whole line; a tree already above the floor keeps its newer release and other majors are
// left alone unless `from` reaches down into them.
export function validateContract(contract, technologyContract, knownTrees) {
  if (contract?.schema_version !== 1) throw new ContractError("schema_version must be 1");
  if (!Array.isArray(contract.overrides)) throw new ContractError("overrides must be a list");
  const pins = technologyContract?.manifest_exact_pins ?? {};
  const entries = [];
  contract.overrides.forEach((raw, index) => {
    const where = `overrides[${index}] (${raw?.package})`;
    for (const key of Object.keys(raw)) {
      if (!FIELDS.has(key)) throw new ContractError(`${where}: unknown field '${key}'`);
    }
    if (!KINDS.has(raw.kind)) throw new ContractError(`${where}: kind must be one of ${[...KINDS].join(", ")}`);
    if (typeof raw.package !== "string" || !PACKAGE.test(raw.package)) {
      throw new ContractError(`${where}: package is not an npm package name`);
    }
    if (typeof raw.reason !== "string" || raw.reason.trim() === "") throw new ContractError(`${where}: reason is required`);
    if (typeof raw.date !== "string" || !DATE.test(raw.date)) throw new ContractError(`${where}: date must be YYYY-MM-DD`);
    if (raw.trees !== undefined) {
      if (!Array.isArray(raw.trees) || raw.trees.length === 0) throw new ContractError(`${where}: trees must be a non-empty list when present`);
      for (const tree of raw.trees) {
        if (!knownTrees.includes(tree)) throw new ContractError(`${where}: tree '${tree}' holds no tracked pnpm-lock.yaml`);
      }
    }
    const entry = { ...raw, where };
    if (raw.kind === "security") {
      if (raw.version_from !== undefined) throw new ContractError(`${where}: a security override states its own version`);
      requireVersion(raw.version, `${where}.version`);
      if (!Array.isArray(raw.advisories) || raw.advisories.length === 0) {
        throw new ContractError(`${where}: a security override names its advisories`);
      }
      for (const id of raw.advisories) {
        if (typeof id !== "string" || !ADVISORY.test(id)) throw new ContractError(`${where}: '${id}' is not a GHSA or CVE id`);
      }
      if ([...new Set(raw.advisories)].sort().join() !== raw.advisories.join()) {
        throw new ContractError(`${where}: advisories must be unique and sorted`);
      }
      if (raw.from !== undefined && raw.from === lineStart(raw.version)) {
        throw new ContractError(`${where}: from ${raw.from} is the default (start of ${raw.version}'s line); omit it`);
      }
      entry.from = raw.from ?? lineStart(raw.version);
      requireVersion(entry.from, `${where}.from`);
      if (compareVersions(entry.from, raw.version) > 0) throw new ContractError(`${where}: from ${entry.from} is above version ${raw.version}`);
      entry.end = lineEnd(raw.version);
      entry.selector = `${raw.package}@>=${entry.from} <${entry.end}`;
      entry.value = `^${raw.version}`;
    } else {
      if (raw.version !== undefined || raw.from !== undefined || raw.advisories !== undefined) {
        throw new ContractError(`${where}: a technology pin takes its version from ${TECHNOLOGY_CONTRACT_PATH} (version_from) and nothing else`);
      }
      if (typeof raw.version_from !== "string" || typeof pins[raw.version_from] !== "string") {
        throw new ContractError(`${where}: version_from is not a manifest_exact_pins key in ${TECHNOLOGY_CONTRACT_PATH}`);
      }
      entry.version = pins[raw.version_from];
      requireVersion(entry.version, `${where}.version_from`);
      entry.selector = raw.package;
      entry.value = entry.version;
    }
    entries.push(entry);
  });
  // pnpm applies one override per declared dependency; two entries reaching the same declared
  // range would make the outcome depend on map order.
  for (let i = 0; i < entries.length; i += 1) {
    for (let j = i + 1; j < entries.length; j += 1) {
      const a = entries[i];
      const b = entries[j];
      if (a.package !== b.package) continue;
      if (a.kind !== "security" || b.kind !== "security") {
        throw new ContractError(`${a.where} and ${b.where}: a technology pin must be the only override of its package`);
      }
      if (compareVersions(a.from, b.end) < 0 && compareVersions(b.from, a.end) < 0) {
        throw new ContractError(`${a.where} and ${b.where}: ranges overlap`);
      }
    }
  }
  return entries;
}

// ---------------------------------------------------------------- pnpm lockfile (v9) reader

function unquote(scalar) {
  const text = scalar.trim();
  if (text.length >= 2 && text.startsWith("'") && text.endsWith("'")) return text.slice(1, -1).replaceAll("''", "'");
  if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) return JSON.parse(text);
  return text;
}

// Splits `key: value` where the key may be a quoted YAML scalar.
function splitMapping(line) {
  const body = line.trimStart();
  if (body.startsWith("'") || body.startsWith('"')) {
    const quote = body[0];
    let index = 1;
    while (index < body.length) {
      if (body[index] === quote) {
        if (quote === "'" && body[index + 1] === "'") {
          index += 2;
          continue;
        }
        break;
      }
      if (quote === '"' && body[index] === "\\") index += 1;
      index += 1;
    }
    const rest = body.slice(index + 1);
    if (!rest.startsWith(":")) return null;
    return [unquote(body.slice(0, index + 1)), rest.slice(1).trim()];
  }
  const colon = body.indexOf(": ");
  if (colon === -1) return body.endsWith(":") ? [body.slice(0, -1), ""] : null;
  return [body.slice(0, colon), body.slice(colon + 2).trim()];
}

// Reads the two things a pnpm v9 lockfile states that matter here: the overrides block it was
// generated with, and every resolved package version.
export function readLockfile(text) {
  if (!/^lockfileVersion: '9\.\d+'$/m.test(text)) throw new ContractError("only pnpm lockfile v9 is understood");
  const overrides = new Map();
  const packages = new Map();
  let section = null;
  for (const line of text.split(/\r?\n/)) {
    if (line === "" || line.startsWith("#")) continue;
    if (!line.startsWith(" ")) {
      section = line.endsWith(":") ? line.slice(0, -1) : null;
      continue;
    }
    if (!/^ {2}\S/.test(line)) continue;
    const mapping = splitMapping(line);
    if (!mapping) continue;
    const [key, value] = mapping;
    if (section === "overrides") {
      overrides.set(key, unquote(value));
    } else if (section === "packages") {
      const at = key.lastIndexOf("@");
      if (at <= 0) continue;
      const version = key.slice(at + 1);
      if (!parseVersion(version)) continue;
      const name = key.slice(0, at);
      if (!packages.has(name)) packages.set(name, new Set());
      packages.get(name).add(version);
    }
  }
  return { overrides, packages };
}

// ---------------------------------------------------------------- trees and rendering

// The trees are not a list anyone keeps: every directory with a tracked pnpm-lock.yaml.
export function trackedTrees(root) {
  const out = execFileSync("git", ["-C", root, "ls-files", "-z", "--", "*pnpm-lock.yaml"], { encoding: "utf8" });
  const trees = out
    .split("\0")
    .filter((path) => /(^|\/)pnpm-lock\.yaml$/.test(path))
    .map((path) => (path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "."));
  return [...new Set(trees)].sort();
}

// An entry reaches a tree that resolves its package inside the entry's range. The lifted
// version stays inside that range, so rendering is stable across a lock refresh.
export function entryApplies(entry, tree, lock) {
  if (entry.trees && !entry.trees.includes(tree)) return false;
  const versions = lock.packages.get(entry.package);
  if (!versions) return false;
  if (entry.kind !== "security") return true;
  return [...versions].some((version) => inRange(version, entry.from, entry.end));
}

export function renderOverrides(entries, tree, lock) {
  const applicable = entries
    .filter((entry) => entryApplies(entry, tree, lock))
    .sort((a, b) => {
      if (a.package !== b.package) return a.package < b.package ? -1 : 1;
      return compareVersions(a.version, b.version);
    });
  return Object.fromEntries(applicable.map((entry) => [entry.selector, entry.value]));
}

function renderManifest(manifest, overrides) {
  const pnpm = { ...(manifest.pnpm ?? {}) };
  if (Object.keys(overrides).length > 0) pnpm.overrides = overrides;
  else delete pnpm.overrides;
  const next = { ...manifest };
  if (Object.keys(pnpm).length > 0) next.pnpm = pnpm;
  else delete next.pnpm;
  return `${JSON.stringify(next, null, 2)}\n`;
}

export function load(root) {
  const trees = trackedTrees(root);
  if (trees.length === 0) throw new ContractError("no tracked pnpm-lock.yaml");
  const entries = validateContract(
    loadJson(join(root, CONTRACT_PATH)),
    loadJson(join(root, TECHNOLOGY_CONTRACT_PATH)),
    trees,
  );
  const states = trees.map((tree) => {
    const manifestPath = join(root, tree, "package.json");
    if (!existsSync(manifestPath)) throw new ContractError(`${tree}: pnpm-lock.yaml has no package.json beside it`);
    const manifestText = readFileSync(manifestPath, "utf8");
    let manifest;
    try {
      manifest = JSON.parse(manifestText);
    } catch (error) {
      throw new ContractError(`${tree}/package.json: ${error.message}`);
    }
    let lock;
    try {
      lock = readLockfile(readFileSync(join(root, tree, "pnpm-lock.yaml"), "utf8"));
    } catch (error) {
      throw new ContractError(`${tree}/pnpm-lock.yaml: ${error.message}`);
    }
    const overrides = renderOverrides(entries, tree, lock);
    return { tree, manifestPath, manifestText, manifest, lock, overrides, rendered: renderManifest(manifest, overrides) };
  });
  return { trees, entries, states };
}

export function render(root) {
  const changed = [];
  for (const state of load(root).states) {
    if (state.rendered === state.manifestText) continue;
    writeFileSync(state.manifestPath, state.rendered);
    changed.push(state.tree);
  }
  return changed;
}

function sameMap(left, right) {
  return JSON.stringify(Object.entries(left).sort()) === JSON.stringify(Object.entries(right).sort());
}

export function check(root) {
  const { trees, entries, states } = load(root);
  const errors = [];
  for (const { tree, lock, overrides, manifest, manifestText, rendered } of states) {
    if (rendered !== manifestText) {
      const current = manifest.pnpm?.overrides ?? {};
      const missing = Object.keys(overrides).filter((key) => current[key] !== overrides[key]);
      const extra = Object.keys(current).filter((key) => !(key in overrides));
      errors.push(
        `${tree}/package.json: pnpm.overrides is not the rendering of ${CONTRACT_PATH}` +
          (missing.length ? `; missing or different: ${missing.join(", ")}` : "") +
          (extra.length ? `; not from the contract: ${extra.join(", ")}` : "") +
          " (run: node tools/npm/security-overrides.mjs render && bash tools/npm/refresh-locks.sh)",
      );
    }
    if (!sameMap(Object.fromEntries(lock.overrides), overrides)) {
      errors.push(`${tree}/pnpm-lock.yaml: its overrides block is not the rendered overrides (run: bash tools/npm/refresh-locks.sh)`);
    }
    for (const entry of entries) {
      if (!entryApplies(entry, tree, lock)) continue;
      for (const version of [...(lock.packages.get(entry.package) ?? [])].sort(compareVersions)) {
        if (entry.kind === "security" && inRange(version, entry.from, entry.version)) {
          errors.push(`${tree}/pnpm-lock.yaml: resolves ${entry.package}@${version}, below the floor ${entry.version} (${entry.advisories.join(", ")})`);
        }
        if (entry.kind === "technology-pin" && version !== entry.version) {
          errors.push(`${tree}/pnpm-lock.yaml: resolves ${entry.package}@${version}, not the pinned ${entry.version}`);
        }
      }
    }
  }
  return { errors, trees, entries };
}

// ---------------------------------------------------------------- OSV report -> contract proposal

export function readBaselineGroups(text) {
  const groups = new Set();
  for (const line of text.split(/\r?\n/)) {
    if (!line || line.startsWith("#")) continue;
    const [ecosystem, name, group] = line.split("\t");
    groups.add(`${ecosystem}\t${name}\t${group}`);
  }
  return groups;
}

// The fixed release of the affected range that contains `version`; null when that range has
// no fix or the advisory does not cover the version.
export function fixedVersionFor(vulnerability, name, version) {
  let best = null;
  for (const affected of vulnerability.affected ?? []) {
    if (affected.package?.ecosystem !== "npm" || affected.package?.name !== name) continue;
    for (const range of affected.ranges ?? []) {
      if (range.type !== "SEMVER" && range.type !== "ECOSYSTEM") continue;
      let introduced = null;
      for (const event of range.events ?? []) {
        if (event.introduced !== undefined) {
          introduced = event.introduced === "0" ? "0.0.0" : event.introduced;
          continue;
        }
        const fixed = event.fixed ?? null;
        const last = event.last_affected ?? null;
        if (introduced === null || (fixed === null && last === null)) continue;
        if (!parseVersion(introduced) || !parseVersion(fixed ?? last)) {
          introduced = null;
          continue;
        }
        const covers = compareVersions(version, introduced) >= 0 &&
          (fixed ? compareVersions(version, fixed) < 0 : compareVersions(version, last) <= 0);
        if (covers && fixed === null) return null;
        if (covers && (best === null || compareVersions(fixed, best) > 0)) best = fixed;
        introduced = null;
      }
    }
  }
  return best;
}

// Returns { contract, changes, unfixable } without writing anything. Only npm findings in a
// pnpm lockfile whose advisory group is not already accepted in the OSV baseline count.
export function proposeFromReport(contract, report, baselineGroups, today) {
  const findings = new Map();
  const unfixable = [];
  for (const result of report.results ?? []) {
    if (!/(^|[\\/])pnpm-lock\.yaml$/.test(result.source?.path ?? "")) continue;
    for (const finding of result.packages ?? []) {
      const { name, version, ecosystem } = finding.package ?? {};
      if (ecosystem !== "npm" || !parseVersion(version ?? "")) continue;
      for (const group of finding.groups ?? []) {
        const ids = [...new Set(group.ids ?? [])].sort();
        if (baselineGroups.has(`npm\t${name}\t${ids.join(",")}`)) continue;
        let fixed = null;
        let blocked = false;
        for (const vulnerability of finding.vulnerabilities ?? []) {
          if (!ids.includes(vulnerability.id)) continue;
          const candidate = fixedVersionFor(vulnerability, name, version);
          if (candidate === null) blocked = true;
          else if (fixed === null || compareVersions(candidate, fixed) > 0) fixed = candidate;
        }
        if (blocked || fixed === null) {
          unfixable.push(`${name}@${version} ${ids.join(",")}`);
          continue;
        }
        const aliases = new Set(ids.filter((id) => ADVISORY.test(id)));
        for (const vulnerability of finding.vulnerabilities ?? []) {
          if (!ids.includes(vulnerability.id)) continue;
          for (const alias of vulnerability.aliases ?? []) if (ADVISORY.test(alias) && alias.startsWith("GHSA")) aliases.add(alias);
        }
        const key = `${name}@${version}`;
        const current = findings.get(key) ?? { name, version, ids: new Set(), fixed };
        for (const id of aliases) current.ids.add(id);
        if (compareVersions(fixed, current.fixed) > 0) current.fixed = fixed;
        findings.set(key, current);
      }
    }
  }
  const next = structuredClone(contract);
  const changes = [];
  const ordered = [...findings.values()].sort((a, b) => (a.name === b.name ? compareVersions(a.version, b.version) : a.name < b.name ? -1 : 1));
  for (const finding of ordered) {
    if (finding.ids.size === 0) {
      unfixable.push(`${finding.name}@${finding.version} (no GHSA or CVE id to record)`);
      continue;
    }
    const ids = [...finding.ids].sort();
    const existing = next.overrides.find(
      (entry) => entry.kind === "security" && entry.package === finding.name &&
        inRange(finding.version, entry.from ?? lineStart(entry.version), lineEnd(entry.version)),
    );
    if (existing) {
      const before = existing.version;
      if (compareVersions(finding.fixed, existing.version) > 0) {
        const from = existing.from ?? lineStart(existing.version);
        existing.version = finding.fixed;
        if (from === lineStart(existing.version)) delete existing.from;
        else existing.from = from;
      }
      existing.advisories = [...new Set([...existing.advisories, ...ids])].sort();
      existing.date = today;
      changes.push(`${finding.name}: floor ${before} -> ${existing.version} for ${ids.join(", ")} (a tree resolved ${finding.version})`);
    } else {
      const entry = {
        kind: "security",
        package: finding.name,
        version: finding.fixed,
        advisories: ids,
        reason: `OSV: ${finding.name} ${finding.version} is affected; ${finding.fixed} is the first fixed release.`,
        date: today,
      };
      const from = lineStart(finding.version);
      if (from !== lineStart(finding.fixed)) entry.from = from;
      next.overrides.push(entry);
      changes.push(`${finding.name}: new floor ${finding.fixed} for ${ids.join(", ")} (a tree resolved ${finding.version})`);
    }
  }
  next.overrides.sort((a, b) => {
    if (a.package !== b.package) return a.package < b.package ? -1 : 1;
    if (a.kind !== "security" || b.kind !== "security") return 0;
    return compareVersions(a.version, b.version);
  });
  return { contract: next, changes, unfixable };
}

// ---------------------------------------------------------------- CLI

// The lockfile toolchain is the one the trees' engines pin; both pins and the container
// digest are read from the technology contract, never restated.
export function toolchain(root) {
  const technology = loadJson(join(root, TECHNOLOGY_CONTRACT_PATH));
  const pins = technology.manifest_exact_pins ?? {};
  requireVersion(pins.node, "manifest_exact_pins.node");
  requireVersion(pins.pnpm, "manifest_exact_pins.pnpm");
  const image = `node:${pins.node}-bookworm`;
  const digest = technology.container_images?.[image];
  if (!/^sha256:[0-9a-f]{64}$/.test(digest ?? "")) {
    throw new ContractError(`${TECHNOLOGY_CONTRACT_PATH}: container_images has no digest for ${image}`);
  }
  return { node: pins.node, pnpm: pins.pnpm, image: `${image}@${digest}` };
}

function parseArgs(argv) {
  const [command, ...rest] = argv;
  const options = { command, root: resolve(dirname(fileURLToPath(import.meta.url)), "..", "..") };
  const flags = { "--root": "root", "--osv-report": "report", "--today": "today", "--summary": "summary" };
  for (let index = 0; index < rest.length; index += 2) {
    const name = flags[rest[index]];
    if (!name || rest[index + 1] === undefined) throw new ContractError(`unknown or incomplete argument ${rest[index]}`);
    options[name] = name === "root" ? resolve(rest[index + 1]) : rest[index + 1];
  }
  return options;
}

export function main(argv) {
  const options = parseArgs(argv);
  const { root } = options;
  switch (options.command) {
    case "trees":
      for (const tree of trackedTrees(root)) console.log(tree);
      return 0;
    case "toolchain": {
      const pinned = toolchain(root);
      console.log(`NODE_VERSION=${pinned.node}`);
      console.log(`PNPM_VERSION=${pinned.pnpm}`);
      console.log(`NODE_IMAGE=${pinned.image}`);
      return 0;
    }
    case "render": {
      const changed = render(root);
      console.log(changed.length ? `rendered: ${changed.join(", ")}` : "OK npm-security-overrides: every package.json already renders the contract");
      return 0;
    }
    case "check": {
      const { errors, trees, entries } = check(root);
      for (const error of errors) console.error(`FAIL npm-security-overrides: ${error}`);
      if (errors.length > 0) return 1;
      console.log(`OK npm-security-overrides (trees=${trees.length}, entries=${entries.length})`);
      return 0;
    }
    case "propose": {
      if (!options.report) throw new ContractError("propose needs --osv-report <osv-scanner json>");
      const today = options.today ?? new Date().toISOString().slice(0, 10);
      if (!DATE.test(today)) throw new ContractError("--today must be YYYY-MM-DD");
      const contractPath = join(root, CONTRACT_PATH);
      const baselinePath = join(root, BASELINE_PATH);
      const { contract, changes, unfixable } = proposeFromReport(
        loadJson(contractPath),
        loadJson(options.report),
        readBaselineGroups(existsSync(baselinePath) ? readFileSync(baselinePath, "utf8") : ""),
        today,
      );
      // A proposal that breaks the contract must not reach the disk.
      validateContract(contract, loadJson(join(root, TECHNOLOGY_CONTRACT_PATH)), trackedTrees(root));
      const lines = [
        ...changes.map((change) => `- ${change}`),
        ...unfixable.map((item) => `- not fixable by an override (no fixed release in the affected range): ${item}`),
      ];
      if (options.summary) writeFileSync(options.summary, lines.length ? `${lines.join("\n")}\n` : "");
      for (const line of lines) console.log(line);
      if (changes.length === 0) {
        console.log("no new npm advisory with a fixed release to raise a floor for");
        return 3;
      }
      writeFileSync(contractPath, `${JSON.stringify(contract, null, 2)}\n`);
      return 0;
    }
    default:
      console.error("usage: security-overrides.mjs trees|toolchain|render|check|propose [--osv-report FILE] [--summary FILE] [--today YYYY-MM-DD] [--root DIR]");
      return 2;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    process.exitCode = main(process.argv.slice(2));
  } catch (error) {
    if (!(error instanceof ContractError)) throw error;
    console.error(`FAIL npm-security-overrides: ${error.message}`);
    process.exitCode = 1;
  }
}
