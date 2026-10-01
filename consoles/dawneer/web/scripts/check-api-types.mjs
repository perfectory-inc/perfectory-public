// Fails when a committed type file in src/api is not exactly what its Foundation OpenAPI document
// generates. The documents themselves are pinned to the Rust code by Foundation tests, so together
// the two checks keep the screens' types equal to the API (root ADR-0116).
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import openapiTS, { astToString } from "openapi-typescript";

const here = dirname(fileURLToPath(import.meta.url));
const docs = resolve(here, "../../../../platforms/foundation-platform/docs/openapi");
// Each committed type file and the OpenAPI document it must equal.
const pairs = [
  ["lineage-review.v1.json", "foundation.d.ts"],
  ["catalog.v1.json", "catalog.d.ts"],
  ["data-catalog.v1.json", "data-catalog.d.ts"],
];

const normalize = (text) => text.replaceAll("\r\n", "\n").trim();
let stale = 0;
for (const [spec, types] of pairs) {
  const url = new URL(`file:///${resolve(docs, spec).replaceAll("\\", "/")}`);
  const generated = normalize(astToString(await openapiTS(url)));
  const current = normalize(readFileSync(resolve(here, "../src/api", types), "utf8"));
  // The committed file carries openapi-typescript's banner; compare the generated body only.
  if (!current.endsWith(generated)) {
    console.error(`src/api/${types} is stale against ${spec}: run \`pnpm api:generate\` and commit the result.`);
    stale += 1;
  }
}
if (stale) process.exit(1);
console.log(`OK dawneer api types match ${pairs.length} OpenAPI documents`);
