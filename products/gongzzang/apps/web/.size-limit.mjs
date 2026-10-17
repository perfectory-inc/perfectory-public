// Sizes only, through @size-limit/file. The app preset also measured run time in headless Chrome
// (estimo -> puppeteer -> extract-zip, which has two unfixed advisories) and every check disabled that.
const PRODUCTION_BUNDLE_LIMIT = "450 KB";
const TURBOPACK_RUNTIME_LIMIT = "20 KB";

const productionBundleCheck = {
  name: "production bundle (all JS chunks, gzipped)",
  path: ".next/static/chunks/*.js",
  limit: PRODUCTION_BUNDLE_LIMIT,
  gzip: true,
};

const turbopackRuntimeCheck = {
  name: "production bundle (turbopack runtime)",
  path: ".next/static/chunks/turbopack-*.js",
  limit: TURBOPACK_RUNTIME_LIMIT,
  gzip: true,
};

export default process.env.GONGZZANG_BAZEL_SIZE_LIMIT === "1"
  ? [productionBundleCheck]
  : [productionBundleCheck, turbopackRuntimeCheck];
