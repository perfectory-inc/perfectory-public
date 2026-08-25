import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, open, readFile, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const contract = JSON.parse(
  await readFile(new URL("../../../config/r2-connections.contract.json", import.meta.url), "utf8"),
);
const gateway = contract.profile_gateway;
const connection = contract.connections[gateway.connection];
const bucket = connection.expected_values.FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET;
const profileId = "00000000-0000-7000-8000-000000000001";
const profileKey = `${gateway.object_key.root}/${profileId}${gateway.object_key.suffix}`;
const bronzeKey = "bronze/vworld/2026/raw.jsonl";
const port = Number.parseInt(process.env.FOUNDATION_PROFILE_GATEWAY_LOCAL_PORT ?? "18787", 10);
const prefix = join(tmpdir(), "foundation-profile-gateway-");
const proofRoot = await mkdtemp(prefix);
const persistDir = join(proofRoot, "r2-state");
const profileFixture = join(proofRoot, "profile.json");
const bronzeFixture = join(proofRoot, "bronze.jsonl");
const wranglerLogPath = join(proofRoot, "wrangler.log");
const wranglerBin = fileURLToPath(new URL("../node_modules/wrangler/bin/wrangler.js", import.meta.url));
const childEnv = {
  ...process.env,
  CI: "true",
  WRANGLER_SEND_METRICS: "false",
};
let server;
let logHandle;

function runWrangler(args) {
  const result = spawnSync(process.execPath, [wranglerBin, ...args], {
    cwd: packageRoot,
    env: childEnv,
    encoding: "utf8",
    timeout: 60_000,
  });
  if (result.error !== undefined || result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw result.error ?? new Error(`wrangler ${args[0]} exited ${String(result.status)}`);
  }
}

function request(path, { method = "GET", headers = {}, body } = {}) {
  return new Promise((resolveRequest, rejectRequest) => {
    const client = http.request(
      { hostname: "127.0.0.1", port, path, method, headers },
      (response) => {
        const chunks = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => {
          resolveRequest({
            status: response.statusCode ?? 0,
            headers: response.headers,
            body: Buffer.concat(chunks),
          });
        });
      },
    );
    client.setTimeout(5_000, () => client.destroy(new Error("local request timed out")));
    client.on("error", rejectRequest);
    if (body !== undefined) client.write(body);
    client.end();
  });
}

function assertStatus(label, actual, expected) {
  process.stdout.write(`${label.padEnd(28)} ${String(actual)}\n`);
  if (actual !== expected) throw new Error(`${label}: expected ${expected}, got ${actual}`);
}

async function waitForReady() {
  for (let attempt = 1; attempt <= 60; attempt += 1) {
    if (server.exitCode !== null) throw new Error("wrangler dev exited before readiness");
    try {
      const response = await request(`/${profileKey}`);
      if (response.status === 200) return;
    } catch {
      // The listener is expected to refuse connections while workerd starts.
    }
    await new Promise((resolveTimer) => setTimeout(resolveTimer, 250));
  }
  throw new Error("wrangler dev readiness timed out");
}

async function stopServer() {
  if (server === undefined || server.exitCode !== null) return;
  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(server.pid), "/t", "/f"], {
      encoding: "utf8",
      timeout: 10_000,
    });
  } else {
    server.kill("SIGTERM");
  }
  await Promise.race([
    new Promise((resolveExit) => server.once("exit", resolveExit)),
    new Promise((resolveTimer) => setTimeout(resolveTimer, 5_000)),
  ]);
  if (server.exitCode === null) server.kill("SIGKILL");
}

async function removeProofRoot(path) {
  for (let attempt = 1; attempt <= 20; attempt += 1) {
    try {
      await rm(path, { recursive: true, force: true });
      return;
    } catch (error) {
      if (error?.code !== "EBUSY" || attempt === 20) throw error;
      await new Promise((resolveTimer) => setTimeout(resolveTimer, 250));
    }
  }
}

try {
  const profileBody = Buffer.from(`{"artifact_id":"${profileId}","name":"local proof"}\n`);
  await writeFile(profileFixture, profileBody);
  await writeFile(bronzeFixture, "{}\n");
  runWrangler([
    "r2",
    "object",
    "put",
    `${bucket}/${profileKey}`,
    "--file",
    profileFixture,
    "--content-type",
    gateway.content_type,
    "--local",
    "--persist-to",
    persistDir,
  ]);
  runWrangler([
    "r2",
    "object",
    "put",
    `${bucket}/${bronzeKey}`,
    "--file",
    bronzeFixture,
    "--local",
    "--persist-to",
    persistDir,
  ]);

  logHandle = await open(wranglerLogPath, "w");
  server = spawn(
    process.execPath,
    [
      wranglerBin,
      "dev",
      "--local",
      "--persist-to",
      persistDir,
      "--port",
      String(port),
      "--var",
      `${gateway.allowed_origins_binding}:http://localhost:3000`,
    ],
    { cwd: packageRoot, env: childEnv, stdio: ["ignore", logHandle.fd, logHandle.fd] },
  );
  await waitForReady();

  const canonical = await request(`/${profileKey}`);
  assertStatus("canonical GET", canonical.status, 200);
  if (!canonical.body.equals(profileBody)) throw new Error("canonical GET bytes drifted");
  const etag = canonical.headers.etag;
  if (typeof etag !== "string" || etag === "") throw new Error("canonical GET omitted ETag");

  assertStatus(
    "conditional GET",
    (await request(`/${profileKey}`, { headers: { "If-None-Match": etag } })).status,
    304,
  );
  assertStatus("Bronze allowlist", (await request(`/${bronzeKey}`)).status, 404);
  assertStatus(
    "raw traversal",
    (await request(`/${gateway.object_key.root}/../../bronze/x`)).status,
    404,
  );
  assertStatus(
    "encoded dot traversal",
    (await request(`/${gateway.object_key.root}/%2e%2e/%2e%2e/bronze/x`)).status,
    404,
  );
  assertStatus(
    "encoded slash traversal",
    (await request(`/${gateway.object_key.root}/${profileId}%2f..%2fbronze.json`)).status,
    404,
  );
  assertStatus(
    "PUT denied",
    (await request(`/${profileKey}`, { method: "PUT", body: "mutated" })).status,
    405,
  );
  assertStatus(
    "disallowed Origin",
    (
      await request(`/${profileKey}`, {
        headers: { Origin: "https://attacker.example.test" },
      })
    ).status,
    403,
  );
  process.stdout.write("OK foundation-profile-gateway local Wrangler/R2 proof\n");
} catch (error) {
  if (logHandle !== undefined) {
    await logHandle.sync();
    process.stderr.write(await readFile(wranglerLogPath, "utf8"));
  }
  throw error;
} finally {
  await stopServer();
  await logHandle?.close();
  const resolved = resolve(proofRoot);
  const safePrefix = `${resolve(tmpdir())}${sep}foundation-profile-gateway-`;
  if (!resolved.startsWith(safePrefix) || dirname(resolved) !== resolve(tmpdir())) {
    throw new Error(`refusing to remove unexpected proof path: ${resolved}`);
  }
  await removeProofRoot(resolved);
}
