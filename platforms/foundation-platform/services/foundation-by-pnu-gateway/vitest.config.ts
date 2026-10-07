import { defineConfig } from "vitest/config";

/// One source, two lanes (root ADR-0160): every test runs under the lane its file is for, the way
/// `wrangler.<lane>.jsonc` bundles it. `parcel-*.test.ts` is the parcel lane; the rest is the
/// building lane, which the pack read path was built and measured on.
const shared = {
  testTimeout: 20_000,
  hookTimeout: 20_000,
  fileParallelism: false,
};

export default defineConfig({
  test: {
    projects: [
      {
        define: { __FOUNDATION_BY_PNU_LANE__: JSON.stringify("building") },
        test: { ...shared, name: "building", include: ["test/**/*.test.ts"], exclude: ["test/**/parcel-*.test.ts"] },
      },
      {
        define: { __FOUNDATION_BY_PNU_LANE__: JSON.stringify("parcel") },
        test: { ...shared, name: "parcel", include: ["test/**/parcel-*.test.ts"] },
      },
    ],
  },
});
