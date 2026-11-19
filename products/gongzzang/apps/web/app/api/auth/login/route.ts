import { type NextRequest, NextResponse } from "next/server";
import { env } from "@/lib/env";
import { buildAuthorizationUrl, generatePkceParams } from "@/lib/oidc";
import { AUTH_STATE_TTL_SEC, sealAuthState, setAuthStateCookie } from "@/lib/session/cookie";
import { sanitizeReturnTo } from "@/lib/url";

export async function POST(req: NextRequest) {
  const formData = await req.formData().catch(() => null);
  // C1: sanitize returnTo — same-origin path only (open redirect prevention)
  const returnTo = sanitizeReturnTo(formData?.get("returnTo") as string | null);

  const pkce = await generatePkceParams();
  const authUrl = buildAuthorizationUrl({
    issuer: env.ZITADEL_ISSUER,
    clientId: env.ZITADEL_CLIENT_ID,
    redirectUri: env.ZITADEL_REDIRECT_URI,
    scope: "openid profile email offline_access",
    code_challenge: pkce.code_challenge,
    state: pkce.state,
    nonce: pkce.nonce,
  });

  // C2: 암호화+서명(iron-session) — 쿠키에서 verifier·nonce 가 읽히지 않는다.
  const sealed = await sealAuthState({
    code_verifier: pkce.code_verifier,
    state: pkce.state,
    nonce: pkce.nonce,
    return_to: returnTo,
  });

  return new NextResponse(null, {
    status: 302,
    headers: {
      Location: authUrl,
      "Set-Cookie": setAuthStateCookie(sealed, AUTH_STATE_TTL_SEC),
    },
  });
}

// M3: GET removed — <img src="/api/auth/login"> CSRF 시도 차단.
// 로그인 버튼은 반드시 <form action="/api/auth/login" method="POST"> 사용.
