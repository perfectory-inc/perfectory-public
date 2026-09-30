// Fails when src/api/foundation.d.ts is not exactly what the committed Foundation OpenAPI document
// generates. The document itself is pinned to the Rust code by a Foundation test, so together the
// two checks keep the screens' types equal to the API (root ADR-0116).
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import openapiTS, { astToString } from "openapi-typescript";

const here = dirname(fileURLToPath(import.meta.url));
const spec = resolve(here, "../../../../platforms/foundation-platform/docs/openapi/lineage-review.v1.json");
const committed = resolve(here, "../src/api/foundation.d.ts");

const generated = astToString(await openapiTS(new URL(`file:///${spec.replaceAll("\\", "/")}`)));
const normalize = (text) => text.replaceAll("\r\n", "\n").trim();
const current = normalize(readFileSync(committed, "utf8"));
// The committed file carries openapi-typescript's banner; compare the generated body only.
if (!current.endsWith(normalize(generated))) {
  console.error("src/api/foundation.d.ts is stale: run `pnpm api:generate` and commit the result.");
  process.exit(1);
}
console.log("OK dawneer api types match lineage-review.v1.json");
