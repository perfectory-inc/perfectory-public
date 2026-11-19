import { emitAuthEvent } from "@/lib/auth/internal-event";
import { env } from "@/lib/env";
import { refreshTokens, type TokenResult } from "@/lib/oidc";
import { getRedis } from "./redis";
import { withLock } from "./single-flight";
import {
  deleteSession,
  getSession,
  REFRESH_TTL_SEC,
  refreshSession,
  type SessionData,
} from "./store";

/** 이 초 안에 끝나는 access token 은 이미 끝난 것으로 본다(중계 도중 만료 방지). */
export const ACCESS_TOKEN_SKEW_SEC = 30;

const nowSec = () => Math.floor(Date.now() / 1000);

/**
 * 쓸 수 있는 세션을 돌려준다: access token 이 아직 살아 있으면 그대로, 끝났거나 곧 끝나면
 * 서버에서 refresh 해서. refresh 가 실패하면(refresh token 만료·폐기) 세션을 지우고 `null`.
 *
 * 브라우저는 refresh 를 부르지 않는다. 토큰을 쓰는 곳(API 중계)이 쓰기 직전에 부른다 —
 * BFF 가 토큰을 쥐고 있으니 갱신도 BFF 의 몫이다(W-4: 전에는 아무도 부르지 않아 만료된 토큰이 그대로 나갔다).
 */
export async function freshSession(sid: string, session: SessionData): Promise<SessionData | null> {
  if (session.exp > nowSec() + ACCESS_TOKEN_SKEW_SEC) return session;
  return refreshSessionTokens(sid);
}

/**
 * 세션 하나를 한 번만 refresh 한다(같은 sid 의 동시 요청은 잠금 하나를 기다려 갱신된 세션을 읽는다).
 * 옛 jti 는 남은 수명 동안 거부 목록에 둔다.
 */
export async function refreshSessionTokens(sid: string): Promise<SessionData | null> {
  return withLock(
    `refresh:${sid}`,
    10,
    async () => {
      const current = await getSession(sid);
      if (!current) return null;
      // 잠금을 기다리는 동안 다른 요청이 이미 갱신했으면 그 결과를 쓴다.
      if (current.exp > nowSec() + ACCESS_TOKEN_SKEW_SEC) return current;

      let next: TokenResult;
      try {
        next = await refreshTokens({
          issuer: env.ZITADEL_ISSUER,
          clientId: env.ZITADEL_CLIENT_ID,
          refresh_token: current.refresh_token,
        });
      } catch {
        await emitAuthEvent("RefreshFailed", { user_sub: current.sub, jti: current.jti });
        // refresh token 이 끝났으면 세션도 끝낸다(좀비 세션 방지).
        await deleteSession(sid);
        return null;
      }

      const remainingSec = Math.max(1, current.exp - nowSec());
      await getRedis().set(`jti:deny:${current.jti}`, "1", "EX", remainingSec);

      const refreshed: SessionData = {
        sub: next.sub,
        jti: next.jti,
        roles: next.roles,
        access_token: next.access_token,
        refresh_token: next.refresh_token,
        id_token: next.id_token,
        exp: nowSec() + next.expires_in,
      };
      await refreshSession(sid, refreshed, REFRESH_TTL_SEC);
      await emitAuthEvent("RefreshSucceeded", {
        user_sub: next.sub,
        prev_jti: current.jti,
        new_jti: next.jti,
        exp: refreshed.exp,
      });
      return refreshed;
    },
    {
      // 다른 요청이 잠금을 쥔 채 끝나지 않으면, 그사이 갱신된 세션이 있으면 그것을 쓴다.
      onLocked: async () => {
        const latest = await getSession(sid);
        return latest && latest.exp > nowSec() ? latest : null;
      },
      maxRetries: 3,
      retryDelayMs: 100,
    },
  );
}
