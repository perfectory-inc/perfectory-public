// deploy.sh runs this before it asks for a token: node --test common.test.mjs
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const common = fileURLToPath(new URL("./common.sh", import.meta.url));

/** cf_token in a shell where wrangler prints `output` and exits with `status`. */
function cfToken(output, status, env = {}) {
  const script = `out=$2 rc=$3; source "$1"; wrangler() { printf '%s' "$out"; return "$rc"; }; cf_token`;
  const childEnv = { ...process.env, ...env };
  if (!("CLOUDFLARE_API_TOKEN" in env)) delete childEnv.CLOUDFLARE_API_TOKEN;
  return spawnSync("bash", ["-c", script, "cf_token", common, output, String(status)], {
    env: childEnv,
    encoding: "utf8",
  });
}

test("a wrangler without `auth token` fails by name", () => {
  const r = cfToken("✘ [ERROR] Unknown arguments: auth, token", 1);
  assert.equal(r.status, 1);
  assert.equal(r.stdout, "");
  assert.match(r.stderr, /no Cloudflare API token: set CLOUDFLARE_API_TOKEN/);
});

test("the login's token is printed", () => {
  const r = cfToken('{"token":"login-token"}', 0);
  assert.equal(r.status, 0);
  assert.equal(r.stdout, "login-token");
});

test("CLOUDFLARE_API_TOKEN wins over the login", () => {
  const r = cfToken("", 1, { CLOUDFLARE_API_TOKEN: "env-token" });
  assert.equal(r.status, 0);
  assert.equal(r.stdout, "env-token");
});

test("the preview opens on the front door's name from the contract", () => {
  const r = spawnSync(
    "bash",
    ["-c", 'source "$1"; front_door_origin', "front_door_origin", common],
    {
      encoding: "utf8",
    },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /^https:\/\/[a-z0-9.-]+$/);
  assert.doesNotMatch(r.stdout, /workers\.dev/);
});
