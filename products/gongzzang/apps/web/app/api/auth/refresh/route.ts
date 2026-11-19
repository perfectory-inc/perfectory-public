import { type NextRequest, NextResponse } from "next/server";
import { getTranslations } from "next-intl/server";
import { problem } from "@/lib/http/problem";
import { SID_COOKIE_NAME } from "@/lib/session/cookie";
import { refreshSessionTokens } from "@/lib/session/refresh";

/**
 * 명시적 갱신(브라우저가 꼭 지금 새 토큰을 원할 때). 평소의 갱신은 API 중계가 토큰을 쓰기 직전에
 * `freshSession` 으로 한다 — 같은 함수, 같은 잠금.
 */
export async function POST(req: NextRequest) {
  const t = await getTranslations("server.auth.refresh");
  const sid = req.cookies.get(SID_COOKIE_NAME)?.value;
  const refreshed = sid ? await refreshSessionTokens(sid) : null;
  if (!refreshed) {
    return problem({
      type: "auth/session-expired",
      title: t("expiredTitle"),
      status: 401,
      instance: req.url,
    }).toResponse();
  }
  return NextResponse.json({ ok: true });
}
