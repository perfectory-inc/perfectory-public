// Fixture for tools/semgrep/rules/r2-read-paths.yml (`opengrep scan --test`). Not compiled.
// ruleid: workers-read-r2-through-the-binding
import { S3Client } from "@aws-sdk/client-s3";
// ruleid: workers-read-r2-through-the-binding
import { AwsClient } from "aws4fetch";
// ok: workers-read-r2-through-the-binding
import { Hono } from "hono";

// ruleid: workers-read-r2-through-the-binding
const client = new S3Client({ region: "auto" });
// ruleid: workers-read-r2-through-the-binding
const lazy = await import("aws4fetch");
// ok: workers-read-r2-through-the-binding
const object = await env.BUCKET.get(key);
