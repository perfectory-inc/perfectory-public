import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import * as random from "@pulumi/random";
import * as cfg from "./config.js";

// ────────────────────────────────────────────────────────────────────────────
// Greenfield platform infrastructure — built from zero, deployed only at launch.
//
// Everything here is code + `pulumi preview` (free). No AWS resource exists, and no charge
// begins, until `pulumi up`. The legacy console-created stack is left untouched.
//
// Shape (from the approved plan): Cloudflare stays the front door + R2 for bulk data; AWS runs
// the app. Heavy data lives in R2/lakehouse, so the DB and containers are deliberately small —
// traffic is absorbed by the edge + horizontal ECS, not by a big database.
// ────────────────────────────────────────────────────────────────────────────

// ── Network: a fresh VPC with public + private subnets across AZs, one NAT to hold cost down ──
const vpc = new awsx.ec2.Vpc(`${cfg.prefix}-vpc`, {
  numberOfAvailabilityZones: cfg.azCount,
  natGateways: { strategy: "Single" }, // one NAT (~$32/mo). HA (per-AZ) NAT is a later toggle.
  tags: { Env: cfg.env },
});

// ── Container registry for our own images (backend, and later web/admin) ──
const backendRepo = new aws.ecr.Repository(`${cfg.prefix}-backend`, {
  forceDelete: true,
  imageScanningConfiguration: { scanOnPush: true },
});

// ── Product database: small Postgres. Master password is AWS-managed (Secrets Manager), never in code ──
const dbSubnets = new aws.rds.SubnetGroup(`${cfg.prefix}-db-subnets`, {
  subnetIds: vpc.privateSubnetIds,
});
const dbSg = new aws.ec2.SecurityGroup(`${cfg.prefix}-db-sg`, {
  vpcId: vpc.vpcId,
  description: "Product DB — reachable only from inside the VPC",
  ingress: [{ protocol: "tcp", fromPort: 5432, toPort: 5432, cidrBlocks: [vpc.vpc.cidrBlock] }],
  egress: [{ protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] }],
});
const db = new aws.rds.Instance(`${cfg.prefix}-db`, {
  engine: "postgres",
  instanceClass: cfg.dbInstanceClass,
  allocatedStorage: cfg.dbStorageGb,
  multiAz: cfg.dbMultiAz,
  dbName: cfg.dbName,
  username: cfg.dbUsername,
  manageMasterUserPassword: true,
  dbSubnetGroupName: dbSubnets.name,
  vpcSecurityGroupIds: [dbSg.id],
  storageEncrypted: true,
  skipFinalSnapshot: true, // TODO: false + finalSnapshotIdentifier before real launch.
  applyImmediately: true,
  tags: { Env: cfg.env },
});

// ── ECS cluster + one shared load balancer (host/path routing → many services on one ALB) ──
const cluster = new aws.ecs.Cluster(`${cfg.prefix}-cluster`, { tags: { Env: cfg.env } });
const alb = new awsx.lb.ApplicationLoadBalancer(`${cfg.prefix}-alb`, {
  subnetIds: vpc.publicSubnetIds,
});

// App containers: reachable from the load balancer, may reach the DB and the internet (via NAT).
const appSg = new aws.ec2.SecurityGroup(`${cfg.prefix}-app-sg`, {
  vpcId: vpc.vpcId,
  description: "App containers",
  ingress: [
    {
      protocol: "tcp",
      fromPort: cfg.backendPort,
      toPort: cfg.backendPort,
      cidrBlocks: [vpc.vpc.cidrBlock],
    },
  ],
  egress: [{ protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] }],
});

// ── Backend service (the template every other service copies) ──
const backend = new awsx.ecs.FargateService(`${cfg.prefix}-backend`, {
  cluster: cluster.arn,
  desiredCount: cfg.backendDesiredCount,
  networkConfiguration: {
    subnets: vpc.privateSubnetIds,
    securityGroups: [appSg.id],
    assignPublicIp: false,
  },
  taskDefinitionArgs: {
    container: {
      name: "backend",
      // Placeholder until the image is built + pushed; set `backendImage` at deploy time.
      image: cfg.backendImage !== "" ? cfg.backendImage : backendRepo.repositoryUrl,
      cpu: cfg.backendCpu,
      memory: cfg.backendMemory,
      essential: true,
      portMappings: [{ containerPort: cfg.backendPort, targetGroup: alb.defaultTargetGroup }],
      environment: [
        { name: "DATABASE_HOST", value: db.address },
        { name: "DATABASE_NAME", value: cfg.dbName },
        // DB password: injected from the AWS-managed secret (db.masterUserSecrets) — TODO wire as a
        // container secret so it is never rendered into the task definition in plaintext.
      ],
    },
  },
  tags: { Env: cfg.env },
});

// ── Login (SSO) = Zitadel: our identity-platform runs on it (Go, no JVM). Behind the shared ALB. ──
// 32-char master key generated and kept (encrypted) in Pulumi state — no manual secret step.
const zitadelMasterkey = new random.RandomString(`${cfg.prefix}-zitadel-masterkey`, {
  length: 32,
  special: false,
});

const zitadelTg = new aws.lb.TargetGroup(`${cfg.prefix}-zitadel-tg`, {
  port: 8080,
  protocol: "HTTP",
  targetType: "ip",
  vpcId: vpc.vpcId,
  healthCheck: { path: "/debug/healthz", matcher: "200" },
});

// Route auth.<domain> on the shared ALB to Zitadel; the backend keeps the default route.
new aws.lb.ListenerRule(`${cfg.prefix}-zitadel-rule`, {
  listenerArn: alb.listeners.apply((ls) => {
    const listener = ls?.[0];
    if (!listener) {
      throw new Error("ALB has no listener to attach the Zitadel host rule to");
    }
    return listener.arn;
  }),
  priority: 10,
  conditions: [{ hostHeader: { values: [cfg.zitadelHost] } }],
  actions: [{ type: "forward", targetGroupArn: zitadelTg.arn }],
});

const zitadel = new awsx.ecs.FargateService(`${cfg.prefix}-zitadel`, {
  cluster: cluster.arn,
  desiredCount: 1,
  networkConfiguration: {
    subnets: vpc.privateSubnetIds,
    securityGroups: [appSg.id],
    assignPublicIp: false,
  },
  taskDefinitionArgs: {
    container: {
      name: "zitadel",
      image: cfg.zitadelImage,
      cpu: cfg.zitadelCpu,
      memory: cfg.zitadelMemory,
      essential: true,
      command: ["start-from-init", "--masterkeyFromEnv", "--tlsMode", "external"],
      portMappings: [{ containerPort: 8080, targetGroup: zitadelTg }],
      environment: [
        { name: "ZITADEL_MASTERKEY", value: zitadelMasterkey.result },
        { name: "ZITADEL_EXTERNALDOMAIN", value: cfg.zitadelHost },
        { name: "ZITADEL_EXTERNALSECURE", value: "true" },
        { name: "ZITADEL_DATABASE_POSTGRES_HOST", value: db.address },
        { name: "ZITADEL_DATABASE_POSTGRES_PORT", value: "5432" },
        { name: "ZITADEL_DATABASE_POSTGRES_DATABASE", value: cfg.zitadelDbName },
        { name: "ZITADEL_DATABASE_POSTGRES_USER_USERNAME", value: cfg.zitadelDbUser },
        { name: "ZITADEL_DATABASE_POSTGRES_USER_SSL_MODE", value: "require" },
        // TODO: DB user password + admin master creds from the RDS-managed secret
        // (db.masterUserSecrets) injected as container secrets, and a dedicated `zitadel` DB user.
      ],
    },
  },
  tags: { Env: cfg.env },
});

// ── Still to add (same pattern), tracked so the skeleton names the whole target ──
// TODO: web / admin-web services (프론트·관리자) behind the shared ALB (host/path rules).
// TODO: osrm routing service (measured tiny: 1 vCPU / 4GB).
// TODO: llm proxy service.
// TODO: data collection + real-time alerts = Lambda + EventBridge + SNS/SES (separate module).
// TODO: ALB listener rules per host (api. / admin. / auth. …); restrict appSg ingress to the ALB SG.
// TODO: wire the DB master secret into containers as a secret, not an env value.

export const vpcId = vpc.vpcId;
export const clusterName = cluster.name;
export const dbEndpoint = db.address;
export const albDnsName = alb.loadBalancer.dnsName;
export const backendRepoUrl = backendRepo.repositoryUrl;
export const backendServiceName = backend.service.name;
export const zitadelServiceName = zitadel.service.name;
export const zitadelUrl = `https://${cfg.zitadelHost}`;
