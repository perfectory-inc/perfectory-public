import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
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

// ── Still to add (same pattern), tracked so the skeleton names the whole target ──
// TODO: Zitadel service (로그인) + its schema on this RDS — replaces the legacy Keycloak.
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
