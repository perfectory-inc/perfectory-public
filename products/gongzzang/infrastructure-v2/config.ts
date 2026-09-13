import * as pulumi from "@pulumi/pulumi";

// All sizing is config-driven with safe defaults, so nothing sensitive lives in code.
// Defaults reflect the measured real load of the legacy stack (mostly idle) — start small,
// scale out on traffic via the edge + horizontal ECS, not via a big database.
const c = new pulumi.Config();

/** Deployment environment name; prefixes every resource. */
export const env = c.get("env") ?? "prod";
export const prefix = `${env}-gongzzang`;

/** Network: how many AZs to spread across (2 = Multi-AZ baseline). */
export const azCount = c.getNumber("azCount") ?? 2;

/** Product database — small on purpose. Heavy data lives in R2/lakehouse, not here. */
export const dbInstanceClass = c.get("dbInstanceClass") ?? "db.t4g.medium"; // measured: CPU ~2%, so small is plenty
export const dbStorageGb = c.getNumber("dbStorageGb") ?? 50; // product state only, not 40M-row geodata
export const dbMultiAz = c.getBoolean("dbMultiAz") ?? true; // reliability for live users
export const dbName = c.get("dbName") ?? "gongzzang";
export const dbUsername = c.get("dbUsername") ?? "gongzzang_app";

/** Backend service sizing (measured: api CPU ~0.4%, memory ~81% of 8GB → 1 vCPU / 8GB). */
export const backendCpu = c.getNumber("backendCpu") ?? 1024; // 1 vCPU
export const backendMemory = c.getNumber("backendMemory") ?? 8192; // 8 GB
export const backendPort = c.getNumber("backendPort") ?? 8080;
export const backendDesiredCount = c.getNumber("backendDesiredCount") ?? 1;

/**
 * Container image for the backend. Empty until the image is built and pushed; set at deploy time
 * (e.g. `pulumi config set backendImage <ecr-url>:<tag>`). Empty falls back to the ECR repo URL as
 * a placeholder so `preview` works before any image exists.
 */
export const backendImage = c.get("backendImage") ?? "";
