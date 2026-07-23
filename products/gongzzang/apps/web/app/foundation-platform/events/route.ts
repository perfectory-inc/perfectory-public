import type { NextRequest } from "next/server";
import { handleWebhookPost } from "@/lib/foundation-platform/webhook-handler";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export function POST(req: NextRequest) {
  return handleWebhookPost(req);
}
