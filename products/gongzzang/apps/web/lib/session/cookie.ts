import { sealData, unsealData } from "iron-session";
import { env } from "@/lib/env";

const isProd = process.env.NODE_ENV === "production";

// Production: __Host-/__Secure- prefix + Secure flag (HTTPS strict).
// Dev runs on localhost HTTP, so the Secure-only prefixes are disabled there.
export const SID_COOKIE_NAME = isProd ? "__Host-sid" : "sid";
export const AUTH_STATE_COOKIE_NAME = isProd ? "__Secure-auth-state" : "auth-state";

function buildSidCookie(value: string, maxAgeSec: number): string {
  const parts = [`${SID_COOKIE_NAME}=${value}`];
  if (isProd) parts.push("Secure", "Partitioned");
  parts.push("HttpOnly", "SameSite=Strict", "Path=/", `Max-Age=${maxAgeSec}`);
  return parts.join("; ");
}

export function setSidCookie(sid: string, maxAgeSec: number): string {
  return buildSidCookie(sid, maxAgeSec);
}

export function deleteSidCookie(): string {
  return buildSidCookie("", 0);
}

export interface AuthStateCookiePayload {
  code_verifier: string;
  state: string;
  nonce: string;
  return_to: string;
}

/** 로그인 시작부터 콜백까지(초). 쿠키 Max-Age 와 봉인의 ttl 이 같은 값이다. */
export const AUTH_STATE_TTL_SEC = 600;

/**
 * PKCE verifier·state·nonce·return_to 를 iron-session 9 `sealData` 로 **암호화+서명**한다
 * (README: "signed and encrypted"; 비밀번호 32자 이상 = `SESSION_SECRET` 스키마). 전에는 HMAC 서명만이라
 * verifier 와 nonce 가 쿠키에서 읽혔다(W-6).
 */
export function sealAuthState(payload: AuthStateCookiePayload): Promise<string> {
  return sealData(payload, { password: env.SESSION_SECRET, ttl: AUTH_STATE_TTL_SEC });
}

/** 봉인을 연다. 변조·만료·다른 비밀번호면 iron-session 이 `{}` 를 돌려주고, 여기서는 `null`. */
export async function unsealAuthState(seal: string): Promise<AuthStateCookiePayload | null> {
  let data: Partial<AuthStateCookiePayload>;
  try {
    data = await unsealData<AuthStateCookiePayload>(seal, {
      password: env.SESSION_SECRET,
      ttl: AUTH_STATE_TTL_SEC,
    });
  } catch {
    return null;
  }
  const { code_verifier, state, nonce, return_to } = data;
  if (
    typeof code_verifier !== "string" ||
    typeof state !== "string" ||
    typeof nonce !== "string" ||
    typeof return_to !== "string"
  ) {
    return null;
  }
  return { code_verifier, state, nonce, return_to };
}

function buildAuthStateCookie(value: string, maxAgeSec: number): string {
  const parts = [`${AUTH_STATE_COOKIE_NAME}=${value}`];
  if (isProd) parts.push("Secure");
  parts.push("HttpOnly", "SameSite=Lax", "Path=/api/auth/", `Max-Age=${maxAgeSec}`);
  return parts.join("; ");
}

export function setAuthStateCookie(payload: string, maxAgeSec: number): string {
  return buildAuthStateCookie(payload, maxAgeSec);
}

export function deleteAuthStateCookie(): string {
  return buildAuthStateCookie("", 0);
}
